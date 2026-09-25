'''RIPE Atlas: read EXISTING public measurements from PK probes plus control probes.'''
from __future__ import annotations

import math
from typing import Any

from collector.collectors.base import RunContext, error_observation
from collector.models import Group, Observation, Status, from_epoch

DEFAULT_BASE = 'https://atlas.ripe.net/api/v2'
ISLAMABAD = (33.6844, 73.0479)  # lat, lon


class RipeAtlasCollector:
  name = 'ripe_atlas'

  def collect(self, ctx: RunContext) -> list[Observation]:
    cfg = ctx.settings(self.name)
    base = cfg.get('base_url', DEFAULT_BASE)
    out: list[Observation] = []
    probes = self._discover_probes(ctx, base, cfg, out)
    if not probes:
      return out
    start = int(ctx.lookback(int(cfg.get('lookback_minutes', 120))).timestamp())
    stop = int(ctx.started_at.timestamp())
    probe_ids = ','.join(map(str, sorted(probes)))

    for msm in ctx.config['ripe_atlas_measurements']:
      group = Group(msm.get('group', 'unknown'))
      try:
        results, meta = ctx.client.get_json(
          f"{base}/measurements/{msm['id']}/results/",
          {'start': start, 'stop': stop, 'probe_ids': probe_ids, 'format': 'json'})
      except Exception as exc:  # noqa: BLE001
        out.append(error_observation(ctx, self.name, f"atlas_{msm.get('type', '?')}",
                                     msm.get('target'), exc, group=group))
        continue
      for r in results if isinstance(results, list) else []:
        probe = probes.get(r.get('prb_id'), {})
        status, metrics, error = parse_result(r)
        asn = probe.get('asn')
        out.append(Observation(
          collector=self.name, measurement_type=f"atlas_{r.get('type', msm.get('type'))}",
          timestamp=from_epoch(r.get('timestamp', stop)),
          target=r.get('dst_name') or r.get('dst_addr') or msm.get('target'),
          status=status, group=group, asn=asn, network=ctx.network_for(asn),
          location={k: probe.get(k) for k in ('country', 'vantage', 'near_islamabad')},
          metrics=metrics, error=error,
          source_id=f"{r.get('msm_id')}:{r.get('prb_id')}:{r.get('timestamp')}",
          raw_metadata={'msm_id': r.get('msm_id'), 'prb_id': r.get('prb_id'),
                        'from': r.get('from'), 'af': r.get('af'), 'api_url': meta['url']},
        ))
    return out

  def _discover_probes(self, ctx: RunContext, base: str, cfg: dict,
                       out: list[Observation]) -> dict[int, dict[str, Any]]:
    '''Stateless: rediscover connected probes each run (cheap, a few requests).'''
    plan = [('PK', 'pakistan', int(cfg.get('max_pakistan_probes', 25)))]
    plan += [(cc, 'control', int(cfg.get('probes_per_control_country', 3)))
             for cc in cfg.get('control_countries', [])]
    probes: dict[int, dict[str, Any]] = {}
    for cc, vantage, limit in plan:
      try:
        data, _ = ctx.client.get_json(f'{base}/probes/', {
          'country_code': cc, 'status': 1, 'page_size': limit,
          'fields': 'id,asn_v4,country_code,geometry'})
      except Exception as exc:  # noqa: BLE001
        out.append(error_observation(ctx, self.name, 'atlas_probe_discovery', f'country:{cc}',
                                     exc))
        continue
      for p in (data.get('results') or [])[:limit]:
        lon, lat = ((p.get('geometry') or {}).get('coordinates') or [None, None])[:2]
        probes[p['id']] = {
          'asn': p.get('asn_v4'), 'country': p.get('country_code'), 'vantage': vantage,
          'near_islamabad': _km(lat, lon, *ISLAMABAD) < 40 if lat is not None else None,
        }
    for pid in cfg.get('extra_probe_ids', []):
      probes.setdefault(int(pid), {'vantage': 'control'})
    return probes


def parse_result(r: dict) -> tuple[Status, dict[str, Any], dict | None]:
  '''Map one Atlas result to (status, metrics, error). Unknown types are kept, not dropped.'''
  kind = r.get('type')
  if kind == 'ping':
    sent, rcvd = r.get('sent') or 0, r.get('rcvd') or 0
    metrics = {'sent': sent, 'rcvd': rcvd, 'loss': round(1 - rcvd / sent, 3) if sent else None,
               'rtt_avg_ms': _pos(r.get('avg')), 'rtt_min_ms': _pos(r.get('min')),
               'rtt_max_ms': _pos(r.get('max'))}
    if not sent:
      return Status.MEASUREMENT_FAILURE, metrics, {'kind': 'no_packets_sent'}
    return (Status.OK if rcvd else Status.TARGET_FAILURE), metrics, None
  if kind == 'dns':
    res = r.get('result') or ((r.get('resultset') or [{}])[0].get('result'))
    err = r.get('error') or ((r.get('resultset') or [{}])[0].get('error'))
    if res:
      return Status.OK, {'rtt_ms': res.get('rt'), 'answers': res.get('ANCOUNT'),
                         'size': res.get('size')}, None
    return Status.TARGET_FAILURE, {}, {'kind': 'dns', 'detail': err}
  if kind == 'http':
    res = (r.get('result') or [{}])[0]
    code, err = res.get('res'), res.get('err')
    status = Status.OK if code and 200 <= code < 400 else Status.TARGET_FAILURE
    return status, {'http_status': code, 'rtt_ms': res.get('rt')}, \
      ({'kind': 'http', 'detail': err} if err else None)
  if kind == 'sslcert':
    err = r.get('err') or r.get('alert')
    return (Status.TARGET_FAILURE if err else Status.OK), {'rtt_ms': r.get('rt')}, \
      ({'kind': 'tls', 'detail': err} if err else None)
  if kind == 'traceroute':
    hops = r.get('result') or []
    last = (hops[-1].get('result') or [{}]) if hops else [{}]
    reached = any(h.get('from') == r.get('dst_addr') for h in last)
    return (Status.OK if reached else Status.TARGET_FAILURE), \
      {'hop_count': len(hops), 'destination_reached': reached}, None
  return Status.NO_DATA, {'unparsed_type': kind}, None


def _pos(v: Any) -> float | None:
  return v if isinstance(v, (int, float)) and v >= 0 else None  # Atlas uses -1 for n/a


def _km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
  p1, p2 = math.radians(lat1), math.radians(lat2)
  dl = math.radians(lon2 - lon1)
  return 6371 * math.acos(min(1, math.sin(p1) * math.sin(p2)
                              + math.cos(p1) * math.cos(p2) * math.cos(dl)))
