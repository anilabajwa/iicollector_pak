'''Orchestrate one stateless collection run.'''
from __future__ import annotations

import argparse
import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from collector.collectors import REGISTRY
from collector.collectors.base import RunContext, error_observation
from collector.config import asn_index, load_config
from collector.http import PoliteClient
from collector.models import (COLLECTION_ERRORS, Observation, Status, floor_to_slot, iso, utcnow)
from collector.storage import JsonlStore

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class SourceResult:
  name: str
  observations: list[Observation]
  duration_s: float

  @property
  def errors(self) -> int:
    return sum(o.status in COLLECTION_ERRORS for o in self.observations)

  @property
  def label(self) -> str:
    if not self.observations:
      return 'EMPTY'
    if self.errors == 0:
      return 'OK'
    return 'FAILED' if self.errors == len(self.observations) else 'PARTIAL'


def run_collector(collector_cls: type, ctx: RunContext) -> SourceResult:
  '''Independent failure boundary per source (spec s14).'''
  t0 = time.monotonic()
  name = collector_cls.name
  try:
    observations = collector_cls().collect(ctx)
  except Exception as exc:  # noqa: BLE001 - errors are data
    observations = [error_observation(ctx, name, f'{name}_run', None, exc)]
  return SourceResult(name, observations, time.monotonic() - t0)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
  p = argparse.ArgumentParser(description='Pakistan connectivity monitor - one collection run')
  p.add_argument('--config', type=Path, default=ROOT / 'config' / 'targets.yaml')
  p.add_argument('--data-dir', type=Path, default=ROOT / 'data')
  p.add_argument('--only', help='comma-separated collector names')
  p.add_argument('--scheduled-at', help='override nominal slot (ISO UTC)')
  p.add_argument('--dry-run', action='store_true')
  return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
  args = parse_args(argv)
  started = utcnow()
  # GitHub does not expose the intended cron time; floor-of-start is a labelled approximation
  scheduled = (datetime.fromisoformat(args.scheduled_at.replace('Z', '+00:00'))
               if args.scheduled_at else floor_to_slot(started))
  run_id = f"{os.environ.get('GITHUB_RUN_ID', 'local')}.{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
  cfg = load_config(args.config)
  http = cfg.get('http', {})
  ctx = RunContext(
    run_id=run_id, scheduled_at=scheduled, started_at=started, config=cfg, asns=asn_index(cfg),
    client=PoliteClient(timeout_s=http.get('timeout_s', 20), max_retries=http.get('max_retries', 2),
                        min_interval_s=http.get('min_interval_s', 0.5)),
  )

  wanted = set(args.only.split(',')) if args.only else None
  enabled = [cls for name, cls in REGISTRY.items()
             if (wanted is None or name in wanted)
             and (ctx.settings(name).get('enabled', True))]

  with ThreadPoolExecutor(max_workers=max(1, len(enabled))) as pool:
    results = list(pool.map(lambda c: run_collector(c, ctx), enabled))

  stamp = {'run_id': run_id, 'scheduled_at': iso(scheduled), 'collected_at': iso(started)}
  all_obs = [o.finalise(**stamp) for r in results for o in r.observations]
  store = JsonlStore(args.data_dir)
  fresh = store.filter_new(all_obs, started)
  finished = utcnow()

  summary = Observation(
    collector='run', measurement_type='run_summary', timestamp=iso(finished), target=None,
    status=Status.OK, source_id=run_id,
    metrics={
      'sources': {r.name: {'status': r.label, 'observations': len(r.observations),
                           'errors': r.errors, 'duration_s': round(r.duration_s, 2)}
                  for r in results},
      'observations_total': len(all_obs), 'observations_new': len(fresh),
      'status_counts': dict(Counter(str(o.status) for o in all_obs)),
      'duration_s': round((finished - started).total_seconds(), 2),
      'scheduled_at_basis': 'cli' if args.scheduled_at else 'floor_of_start',
      'completed_at': iso(finished),
    },
  ).finalise(**stamp)

  meaningful = bool(fresh)
  if meaningful and not args.dry_run:
    store.append([*fresh, summary], started)
  report = render_summary(summary, results)
  print(report)
  _github_outputs(meaningful=meaningful, slot=iso(scheduled), report=report)
  return 0  # collection problems are recorded as data, not exit codes


def render_summary(summary: Observation, results: list[SourceResult]) -> str:
  m = summary.metrics
  lines = [summary.collected_at, '']
  lines += [f"{r.name + ':':<14}{r.label:<8} ({len(r.observations)} obs, {r.errors} err)"
            for r in results]
  lines += ['', f"observations: {m['observations_total']} ({m['observations_new']} new)",
            f"errors:       {sum(r.errors for r in results)}",
            f"duration:     {m['duration_s']}s"]
  return '\n'.join(lines)


def _github_outputs(*, meaningful: bool, slot: str, report: str) -> None:
  if out := os.environ.get('GITHUB_OUTPUT'):
    with open(out, 'a', encoding='utf-8') as fh:
      fh.write(f"meaningful={'true' if meaningful else 'false'}\nslot={slot}\n")
  if summ := os.environ.get('GITHUB_STEP_SUMMARY'):
    with open(summ, 'a', encoding='utf-8') as fh:
      fh.write(f'```\n{report}\n```\n')
