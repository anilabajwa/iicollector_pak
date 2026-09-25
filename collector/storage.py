'''Append-only, date-partitioned JSONL storage with observation_id de-duplication.'''
from __future__ import annotations

import json
import os
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from pathlib import Path

from collector.models import Observation


class JsonlStore:
  '''
  Files are partitioned by UTC *collection* date (data/YYYY-MM-DD.jsonl).
  Measurement time is in each record's `timestamp`. Dedupe looks back `dedupe_days`
  files so late or overlapping upstream records do not reappear across midnight.
  '''

  def __init__(self, root: Path, dedupe_days: int = 2) -> None:
    self.root, self.dedupe_days = root, dedupe_days
    self.root.mkdir(parents=True, exist_ok=True)

  def path_for(self, day: date) -> Path:
    return self.root / f'{day.isoformat()}.jsonl'

  def _ids_in(self, path: Path) -> set[str]:
    if not path.exists():
      return set()
    ids: set[str] = set()
    with path.open(encoding='utf-8') as fh:
      for line in fh:
        try:
          ids.add(json.loads(line)['observation_id'])
        except (ValueError, KeyError):
          continue  # tolerate a truncated line from an interrupted run
    return ids

  def existing_ids(self, now: datetime) -> set[str]:
    days = (now.date() - timedelta(days=d) for d in range(self.dedupe_days + 1))
    return set().union(*(self._ids_in(self.path_for(d)) for d in days))

  def filter_new(self, observations: Iterable[Observation], now: datetime) -> list[Observation]:
    seen = self.existing_ids(now)
    fresh: list[Observation] = []
    for obs in observations:
      if obs.observation_id not in seen:
        seen.add(obs.observation_id)
        fresh.append(obs)
    return fresh

  def append(self, observations: list[Observation], now: datetime) -> Path:
    path = self.path_for(now.date())
    with path.open('a', encoding='utf-8') as fh:
      fh.writelines(obs.to_json() + '\n' for obs in observations)
      fh.flush()
      os.fsync(fh.fileno())
    return path
