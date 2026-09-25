'''OONI: web_connectivity for a small target set, plus a country-level availability summary.'''
from __future__ import annotations

import json
from collections import Counter
from typing import Any

from collector.collectors.base import RunContext, error_observation, parse_asn
from collector.models import Group, Observation, Status

DEFAULT_BASE = 'https://api.ooni.io/api/v1'
OONI_TIME = '%Y-%m-%dT%H:%M:%S'


class OoniCollector:
  name = 'ooni'

  def collect(self, ctx: RunContext) -> list[Observation]:
    cfg = ctx.settings(self.name)
    base = cfg.get('base_url', DEFAULT_BASE)
    # OONI uploads lag; overlapping windows are fine because uid-based IDs dedupe
    window = {
      'since': ctx.lookback(int(cfg.get('lookback_minutes', 180))).strftime(OONI_TIME),
      'until': ctx.started_at.strftime(OONI_TIME),
      'probe_cc': cfg.get('probe_cc', 'PK'),
    }
    detail_budget = int(cfg.get('max_detail_fetches', 5))
    out: list[Observation] = []

    for test_name in cfg.get('test_names', ['web_connectivity']):
      out += self._country_summary(ctx, base, {**window, 'test_name': test_name},
                                   int(cfg.get('summary_limit', 500)))

    for target in ctx.config['ooni_targets']:
      group = Group(target.get('group', 'unknown'))
      params = {**window, 'test_name': target.get('test_name', 'web_connectivity'),
                'domain': target['domain'], 'limit': int(cfg.get('per_target_limit', 50)),
                'order_by': 'measurement_start_time', 'order': 'desc'}
      try:
        data, meta = ctx.client.get_json(f'{base}/measurements', params)
        results = data.get('results') or []
      except Exception as exc:  # noqa: BLE001
        out.append(error_observation(ctx, self.name, params['test_name'], target['domain'], exc,
                                     group=group))
        continue
      for m in results:
        obs = self._from_listing(ctx, m, group, meta)
        if obs.status is not Status.OK and detail_budget > 0:
          detail_budget -= 1
          obs.metrics['details'] = self._details(ctx, base, m.get('measurement_uid'))
        out.append(obs)
    return out

  def _from_listing(self, ctx: RunContext, m: dict, group: Group, meta: dict) -> Observation:
    asn, cc, uid = parse_asn(m.get('probe_asn')), m.get('probe_cc'), m.get('measurement_uid')
    if m.get('failure'):
      status = Status.MEASUREMENT_FAILURE
    elif m.get('confirmed') or m.get('anomaly'):
      status = Status.TARGET_FAILURE  # OONI 'anomaly' - a signal, not a verdict
    else:
      status = Status.OK
    return Observation(
      collector=self.name, measurement_type=m.get('test_name', 'unknown'),
      timestamp=m.get('measurement_start_time'), target=m.get('input'), status=status,
      group=group, asn=asn, network=ctx.network_for(asn),
      location={'country': cc, 'vantage': 'pakistan' if cc == 'PK' else 'other'},
      metrics={k: m.get(k) for k in ('anomaly', 'confirmed', 'failure', 'scores')},
      source_id=uid,
      raw_metadata={'measurement_url': m.get('measurement_url'), 'report_id': m.get('report_id'),
                    'explorer_url': f'https://explorer.ooni.org/m/{uid}' if uid else None,
                    'api_url': meta['url']},
    )

  def _country_summary(self, ctx: RunContext, base: str, params: dict,
                       limit: int) -> list[Observation]:
    '''Counts per ASN - gives a 'measurement availability' baseline without dumping payloads.'''
    target = f"country:{params['probe_cc']}"
    try:
      data, meta = ctx.client.get_json(f'{base}/measurements', {**params, 'limit': limit})
      results = data.get('results') or []
    except Exception as exc:  # noqa: BLE001
      return [error_observation(ctx, self.name, 'ooni_country_summary', target, exc,
                                group=Group.PAKISTAN)]
    by_asn: dict[str, Counter] = {}
    for m in results:
      c = by_asn.setdefault(str(parse_asn(m.get('probe_asn'))), Counter())
      c['total'] += 1
      c['anomaly'] += bool(m.get('anomaly'))
      c['confirmed'] += bool(m.get('confirmed'))
      c['failure'] += bool(m.get('failure'))
    return [Observation(
      collector=self.name, measurement_type='ooni_country_summary',
      timestamp=params['until'] + 'Z', target=target,
      status=Status.OK if results else Status.NO_DATA, group=Group.PAKISTAN,
      location={'country': params['probe_cc'], 'vantage': 'pakistan'},
      metrics={'test_name': params['test_name'], 'window_since': params['since'] + 'Z',
               'count': len(results), 'truncated': len(results) >= limit,
               'by_asn': {a: dict(c) for a, c in by_asn.items()},
               'by_access_type': _by_access_type(ctx, by_asn)},
      raw_metadata={'api': meta},
    )]

  def _details(self, ctx: RunContext, base: str, uid: str | None) -> dict[str, Any]:
    '''Fetch one full measurement and keep only analysis-relevant fields.'''
    if not uid:
      return {'error': 'no uid'}
    try:
      data, _ = ctx.client.get_json(f'{base}/measurement_meta',
                                    {'measurement_uid': uid, 'full': 'true'})
      raw = data.get('raw_measurement')
      return extract_details(json.loads(raw) if isinstance(raw, str) else raw or {})
    except Exception as exc:  # noqa: BLE001 - details are best-effort
      return {'error': repr(exc)[:300]}


def extract_details(raw: dict) -> dict[str, Any]:
  tk = raw.get('test_keys') or {}
  queries = tk.get('queries') or []
  return {
    'dns_experiment_failure': tk.get('dns_experiment_failure'),
    'http_experiment_failure': tk.get('http_experiment_failure'),
    'control_failure': tk.get('control_failure'),
    'blocking': tk.get('blocking'),
    'accessible': tk.get('accessible'),
    'resolver_asn': raw.get('resolver_asn'),
    'dns_failures': [q.get('failure') for q in queries if q.get('failure')][:10],
    'dns_answers': [
      {'type': a.get('answer_type'), 'value': a.get('ipv4') or a.get('ipv6') or a.get('hostname')}
      for q in queries for a in (q.get('answers') or [])
    ][:20],
    'tcp_connect': [
      {'ip': t.get('ip'), 'port': t.get('port'), 'success': (t.get('status') or {}).get('success'),
       'failure': (t.get('status') or {}).get('failure')}
      for t in (tk.get('tcp_connect') or [])
    ][:20],
    'http_status_codes': [(r.get('response') or {}).get('code')
                          for r in (tk.get('requests') or [])][:10],
  }


def _by_access_type(ctx: RunContext, by_asn: dict[str, Counter]) -> dict[str, dict]:
  totals: dict[str, Counter] = {}
  for asn, counts in by_asn.items():
    net = ctx.network_for(parse_asn(asn)) or {}
    totals.setdefault(net.get('access_type') or 'unlisted', Counter()).update(counts)
  return {k: dict(v) for k, v in totals.items()}
