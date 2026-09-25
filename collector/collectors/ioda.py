'''IODA: raw per-entity signals (BGP, active probing, telescope) and outage events.'''
from __future__ import annotations

from typing import Any

from collector.collectors.base import RunContext, error_observation
from collector.http import FetchError
from collector.models import Group, Observation, Status, from_epoch

DEFAULT_BASE = 'https://api.ioda.inetintel.cc.gatech.edu/v2'


class IodaCollector:
  name = 'ioda'

  def collect(self, ctx: RunContext) -> list[Observation]:
    cfg = ctx.settings(self.name)
    base = cfg.get('base_url', DEFAULT_BASE)
    until = int(ctx.started_at.timestamp())
    since = int(ctx.lookback(int(cfg.get('lookback_minutes', 120))).timestamp())
    out: list[Observation] = []
    for entity in self._entities(ctx, cfg):
      out += self._signals(ctx, base, entity, since, until)
      if cfg.get('fetch_outage_events', True):
        out += self._events(ctx, base, entity, since, until)
    return out

  def _entities(self, ctx: RunContext, cfg: dict) -> list[dict[str, Any]]:
    entities = [dict(e) for e in cfg.get('entities', [])]
    if cfg.get('include_pakistan_asns', True):
      entities += [{'type': 'asn', 'code': str(a), 'group': 'pakistan', 'asn': a}
                   for a in ctx.asns]
    return entities

  def _signals(self, ctx: RunContext, base: str, entity: dict, since: int,
               until: int) -> list[Observation]:
    target = f"{entity['type']}:{entity['code']}"
    group, asn = Group(entity.get('group', 'unknown')), entity.get('asn')
    try:
      data, meta = ctx.client.get_json(
        f"{base}/signals/raw/{entity['type']}/{entity['code']}", {'from': since, 'until': until})
      series_list = _flatten(data.get('data') if isinstance(data, dict) else None)
    except Exception as exc:  # noqa: BLE001 - each entity is its own failure boundary
      return [error_observation(ctx, self.name, 'ioda_signal', target, exc, group=group, asn=asn)]

    out: list[Observation] = []
    for s in series_list:
      ds, t0, step, values = s.get('datasource'), s.get('from'), s.get('step'), s.get('values')
      if not isinstance(values, list) or t0 is None or not step:
        continue
      for i, value in enumerate(values):
        if value is None:
          continue  # not yet available; a later run will pick it up
        ts = t0 + i * step
        out.append(Observation(
          collector=self.name, measurement_type='ioda_signal', timestamp=from_epoch(ts),
          target=target, status=Status.OK, group=group, asn=asn, network=ctx.network_for(asn),
          location={'country': 'PK' if group is Group.PAKISTAN else None},
          metrics={'datasource': ds, 'value': value, 'step_s': step,
                   'native_step_s': s.get('nativeStep')},
          # value in the key: upstream revisions are kept as new records (auditable)
          source_id=f'{target}:{ds}:{ts}:{value}',
          raw_metadata={'entity_name': s.get('entityName'), 'api': meta},
        ))
    if not out:
      out.append(Observation(
        collector=self.name, measurement_type='ioda_signal', timestamp=from_epoch(until),
        target=target, status=Status.NO_DATA, group=group, asn=asn,
        network=ctx.network_for(asn), raw_metadata={'api': meta},
      ))
    return out

  def _events(self, ctx: RunContext, base: str, entity: dict, since: int,
              until: int) -> list[Observation]:
    target = f"{entity['type']}:{entity['code']}"
    group, asn = Group(entity.get('group', 'unknown')), entity.get('asn')
    params = {'entityType': entity['type'], 'entityCode': entity['code'],
              'from': since, 'until': until}
    try:
      data, meta = ctx.client.get_json(f'{base}/outages/events', params)
      if not isinstance(data, dict):
        raise FetchError(Status.MALFORMED, 'outages/events: response is not an object', url=meta['url'])
      if data.get('error') is not None:
        raise FetchError(Status.MALFORMED, f"outages/events: {data['error']}", url=meta['url'])
      events = data.get('data')
      if not isinstance(events, list):
        raise FetchError(Status.MALFORMED, 'outages/events: data is not a list', url=meta['url'])
      if not isinstance(events, list):
        raise FetchError(Status.MALFORMED, 'outages/events: data is not a list', url=meta['url'])
    except Exception as exc:  # noqa: BLE001
      return [error_observation(ctx, self.name, 'ioda_outage_event', target, exc, group=group,
                                asn=asn)]
    out = []
    for ev in events:
      if not isinstance(ev, dict):
        continue
      scalars = {k: v for k, v in ev.items() if isinstance(v, (str, int, float, bool, type(None)))}
      start = ev.get('start') or ev.get('from') or since
      out.append(Observation(
        collector=self.name, measurement_type='ioda_outage_event', timestamp=from_epoch(start),
        # IODA's own label; we do not interpret it as a confirmed outage
        target=target, status=Status.OK, group=group, asn=asn, network=ctx.network_for(asn),
        metrics={k: ev.get(k) for k in ('score', 'duration', 'datasource', 'method')
                 if k in ev},
        source_id=f"{target}:{start}:{ev.get('datasource')}:{ev.get('method')}",
        raw_metadata={'event': scalars, 'api': meta},
      ))
    return out


def _flatten(node: Any) -> list[dict]:
  '''IODA wraps series in nested lists; return a flat list of series dicts.'''
  if isinstance(node, dict):
    return [node]
  if isinstance(node, list):
    return [s for child in node for s in _flatten(child)]
  return []
