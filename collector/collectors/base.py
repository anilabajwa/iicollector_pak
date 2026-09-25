'''Shared collector plumbing.'''
from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from collector.http import FetchError, PoliteClient
from collector.models import Group, Observation, Status, iso


@dataclass(frozen=True)
class RunContext:
  run_id: str
  scheduled_at: datetime
  started_at: datetime
  config: dict[str, Any]
  client: PoliteClient
  asns: dict[int, dict]

  def settings(self, collector: str) -> dict[str, Any]:
    return self.config['collectors'].get(collector, {}) or {}

  def lookback(self, minutes: int) -> datetime:
    return self.started_at - timedelta(minutes=minutes)

  def network_for(self, asn: int | None) -> dict | None:
    return self.asns.get(asn) if asn is not None else None


class Collector(Protocol):
  name: str

  def collect(self, ctx: RunContext) -> list[Observation]: ...


def error_observation(ctx: RunContext, collector: str, measurement_type: str, target: str | None,
                      exc: BaseException, *, group: Group = Group.UNKNOWN,
                      asn: int | None = None) -> Observation:
  '''Turn any exception into a data point (errors are data - spec s8).'''
  if isinstance(exc, FetchError):
    status, error = exc.status, exc.as_error()
  else:
    status = Status.COLLECTOR_ERROR
    tail = traceback.format_exception(type(exc), exc, exc.__traceback__)[-3:]
    error = {'kind': str(status), 'message': repr(exc)[:500], 'traceback_tail': ''.join(tail)}
  return Observation(
    collector=collector, measurement_type=measurement_type, timestamp=iso(ctx.started_at),
    target=target, status=status, group=group, asn=asn, network=ctx.network_for(asn), error=error,
  )


def parse_asn(value: Any) -> int | None:
  '''Accept 17557, '17557' or 'AS17557'.'''
  if value is None:
    return None
  try:
    return int(str(value).upper().removeprefix('AS'))
  except ValueError:
    return None
