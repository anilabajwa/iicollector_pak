'''Common observation envelope, status taxonomy and UTC time helpers.'''
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta  # NOTE(3.11+): datetime.UTC
from enum import StrEnum  # NOTE(3.11+)
from typing import Any

SCHEMA_VERSION = 1
SLOT_MINUTES = 5


class Status(StrEnum):
  '''Outcome taxonomy - the distinction between these is deliberate (spec s8).'''
  OK = 'ok'
  TARGET_FAILURE = 'target_failure'            # measurement worked; target unreachable/anomalous
  MEASUREMENT_FAILURE = 'measurement_failure'  # source says its own measurement failed
  NO_DATA = 'no_data'                          # source answered, but nothing in window
  API_ERROR = 'api_error'                      # HTTP 4xx/5xx, connection refused, etc.
  TIMEOUT = 'timeout'                          # our request to the API timed out
  MALFORMED = 'malformed_response'             # unparseable / unexpected shape
  RATE_LIMITED = 'rate_limited'                # HTTP 429
  COLLECTOR_ERROR = 'collector_error'          # bug in our code


# Statuses meaning 'we failed to collect', as opposed to 'we observed a failure'
COLLECTION_ERRORS = frozenset({
  Status.API_ERROR, Status.TIMEOUT, Status.MALFORMED, Status.RATE_LIMITED, Status.COLLECTOR_ERROR,
})


class Group(StrEnum):
  '''Conceptual target group (spec s4). Vantage point lives in `location`.'''
  PAKISTAN = 'pakistan'
  INTERNATIONAL_CONTROL = 'international_control'
  CROSS_LOCATION = 'cross_location'
  UNKNOWN = 'unknown'


def utcnow() -> datetime:
  return datetime.now(UTC)


def iso(dt: datetime) -> str:
  return dt.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def from_epoch(ts: float | int) -> str:
  return iso(datetime.fromtimestamp(ts, UTC))


def floor_to_slot(dt: datetime, minutes: int = SLOT_MINUTES) -> datetime:
  dt = dt.astimezone(UTC).replace(second=0, microsecond=0)
  return dt - timedelta(minutes=dt.minute % minutes)


def stable_hash(*parts: Any) -> str:
  joined = '|'.join('' if p is None else str(p) for p in parts)
  return hashlib.sha256(joined.encode()).hexdigest()[:24]


@dataclass(slots=True)
class Observation:
  '''One line of JSONL. Collectors fill measurement fields; runner fills run fields.'''
  collector: str
  measurement_type: str
  timestamp: str                     # when the SOURCE says it was measured (UTC ISO)
  target: str | None
  status: Status
  group: Group = Group.UNKNOWN
  asn: int | None = None
  network: dict | None = None        # {name, access_type: mobile|fixed|mixed|...}
  location: dict | None = None       # {country, vantage, near_islamabad, ...}
  metrics: dict = field(default_factory=dict)
  error: dict | None = None
  source_id: str | None = None       # upstream identifier (OONI uid, Atlas msm:prb:ts ...)
  raw_metadata: dict | None = None   # small audit payload + API response meta
  # --- filled by runner.finalise ---
  observation_id: str = ''
  run_id: str = ''
  scheduled_at: str = ''
  collected_at: str = ''
  schema_version: int = SCHEMA_VERSION

  def identity_key(self) -> str:
    '''
    Deterministic ID:
    - with source_id: stable across runs, so re-fetching the same upstream record dedupes.
    - without: a per-slot snapshot; the ok/error class is included so that a successful
      rerun is not discarded as a duplicate of an earlier failed attempt.
    '''
    if self.source_id:
      return stable_hash(self.collector, self.measurement_type, self.source_id, self.target)
    outcome = 'err' if self.status in COLLECTION_ERRORS else 'ok'
    return stable_hash(
      self.collector, self.measurement_type, self.target, self.asn, self.scheduled_at, outcome,
    )

  def finalise(self, *, run_id: str, scheduled_at: str, collected_at: str) -> Observation:
    self.run_id, self.scheduled_at, self.collected_at = run_id, scheduled_at, collected_at
    self.observation_id = self.identity_key()
    return self

  def to_json(self) -> str:
    return json.dumps(asdict(self), sort_keys=True, separators=(',', ':'), default=str)
