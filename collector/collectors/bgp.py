'''BGP via RIPEstat (RIS-backed): visibility snapshots + withdrawal events.'''
from __future__ import annotations

from datetime import timedelta

from collector.collectors.base import RunContext, error_observation, parse_asn
from collector.models import Group, Observation, Status, iso

DEFAULT_BASE = 'https://stat.ripe.net/data'
STAT_TIME = '%Y-%m-%dT%H:%M:%S'


class BgpCollector:
  name = 'bgp'

  def collect(self, ctx: RunContext) -> list[Observation]:
    cfg = ctx.settings(self.name)
    base = cfg.get('base_url', DEFAULT_BASE)
    app = {'sourceapp': cfg.get('sourceapp', 'pk-connectivity-monitor')}
    out: list[Observation] = []
    for asn in ctx.asns:
      out.append(self._routing_status(ctx, base, app, f'AS{asn}', asn))
    for pfx in ctx.config['pakistan_prefixes']:
      out.append(self._routing_status(ctx, base, app, pfx['prefix'], pfx.get('origin_asn')))
      if cfg.get('fetch_updates', True):
        out += self._updates(ctx, base, app, pfx, int(cfg.get('updates_lookback_minutes', 120)))
    return out

  def _routing_status(self, ctx: RunContext, base: str, app: dict, resource: str,
                      asn: int | None) -> Observation:
    mtype = 'bgp_routing_status'
    try:
      data, meta = ctx.client.get_json(f'{base}/routing-status/data.json',
                                       {**app, 'resource': resource})
      d = data['data']
      vis = d.get('visibility') or {}
    except Exception as exc:  # noqa: BLE001
      return error_observation(ctx, self.name, mtype, resource, exc, group=Group.PAKISTAN, asn=asn)
    v4, v6 = vis.get('v4') or {}, vis.get('v6') or {}
    seeing = (v4.get('ris_peers_seeing') or 0) + (v6.get('ris_peers_seeing') or 0)
    metrics = {
      'v4_peers_seeing': v4.get('ris_peers_seeing'), 'v4_total_peers': v4.get('total_ris_peers'),
      'v6_peers_seeing': v6.get('ris_peers_seeing'), 'v6_total_peers': v6.get('total_ris_peers'),
      'announced_space': d.get('announced_space'),
      'last_seen': d.get('last_seen'),
      'origins': [parse_asn(o.get('origin')) for o in (d.get('origins') or [])
                  if isinstance(o, dict)],
    }
    return Observation(
      collector=self.name, measurement_type=mtype,
      timestamp=d.get('query_time', iso(ctx.started_at)).rstrip('Z') + 'Z',
      target=resource, status=Status.OK if seeing else Status.TARGET_FAILURE,
      group=Group.PAKISTAN, asn=asn, network=ctx.network_for(asn), metrics=metrics,
      raw_metadata={'api': meta, 'data_call_status': data.get('data_call_status')},
    )

  def _updates(self, ctx: RunContext, base: str, app: dict, pfx: dict,
               lookback: int) -> list[Observation]:
    prefix, asn = pfx['prefix'], pfx.get('origin_asn')
    end = ctx.started_at
    start = end - timedelta(minutes=lookback)
    try:
      data, meta = ctx.client.get_json(f'{base}/bgp-updates/data.json', {
        **app, 'resource': prefix, 'starttime': start.strftime(STAT_TIME),
        'endtime': end.strftime(STAT_TIME)})
      updates = data['data'].get('updates') or []
    except Exception as exc:  # noqa: BLE001
      return [error_observation(ctx, self.name, 'bgp_updates', prefix, exc, group=Group.PAKISTAN,
                                asn=asn)]
    n_a = sum(u.get('type') == 'A' for u in updates)
    n_w = sum(u.get('type') == 'W' for u in updates)
    # Window summary (windows overlap between runs; analysis should use the events below)
    out = [Observation(
      collector=self.name, measurement_type='bgp_updates_window', timestamp=iso(end),
      target=prefix, status=Status.OK, group=Group.PAKISTAN, asn=asn,
      network=ctx.network_for(asn),
      metrics={'window_start': iso(start), 'announcements': n_a, 'withdrawals': n_w},
      raw_metadata={'api': meta},
    )]
    for u in updates:
      if u.get('type') != 'W':
        continue  # individual withdrawals kept; announcements only counted
      attrs = u.get('attrs') or {}
      out.append(Observation(
        collector=self.name, measurement_type='bgp_withdrawal',
        timestamp=str(u.get('timestamp')).rstrip('Z') + 'Z', target=attrs.get('target_prefix'),
        status=Status.OK, group=Group.PAKISTAN, asn=asn, network=ctx.network_for(asn),
        metrics={'peer': attrs.get('source_id')},
        source_id=f"{u.get('timestamp')}:{attrs.get('source_id')}:{attrs.get('target_prefix')}",
      ))
    return out
