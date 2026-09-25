'''Core invariants: deterministic IDs, dedupe, error classification.'''
from datetime import UTC, datetime

from collector.collectors.ripe_atlas import parse_result
from collector.models import Observation, Status
from collector.storage import JsonlStore

STAMP = {'run_id': 'r.1', 'scheduled_at': '2026-09-25T01:35:00Z',
         'collected_at': '2026-09-25T01:36:10Z'}


def make(**kw) -> Observation:
  base = dict(collector='ooni', measurement_type='web_connectivity',
              timestamp='2026-09-25T01:30:00Z', target='x', status=Status.OK)
  return Observation(**{**base, **kw}).finalise(**STAMP)


def test_source_id_is_stable_across_runs() -> None:
  a = make(source_id='uid1')
  b = make(source_id='uid1').finalise(run_id='r.2', scheduled_at='2026-09-25T01:40:00Z',
                                      collected_at='x')
  assert a.observation_id == b.observation_id


def test_success_rerun_not_dropped_after_error() -> None:
  err = make(status=Status.TIMEOUT)
  ok = make(status=Status.OK)
  assert err.observation_id != ok.observation_id


def test_store_dedupes(tmp_path) -> None:
  store = JsonlStore(tmp_path)
  now = datetime(2026, 9, 25, 1, 36, tzinfo=UTC)
  obs = [make(source_id='u1'), make(source_id='u2')]
  store.append(store.filter_new(obs, now), now)
  again = store.filter_new([make(source_id='u1'), make(source_id='u3')], now)
  assert [o.source_id for o in again] == ['u3']


def test_atlas_ping_total_loss_is_target_failure() -> None:
  status, metrics, _ = parse_result({'type': 'ping', 'sent': 3, 'rcvd': 0, 'avg': -1})
  assert status is Status.TARGET_FAILURE and metrics['loss'] == 1.0
