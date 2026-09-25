'''
Minimal active control from the CI runner (NOT in Pakistan - usually US/EU Azure).
Helps separate 'target/server down' from 'Pakistan-path problem'. One light
DNS lookup / TCP connect / HTTPS request per target per run; disable in config if unwanted.
Target-side timeouts are reported as TARGET_FAILURE with error.kind='timeout';
Status.TIMEOUT is reserved for API timeouts.
'''
from __future__ import annotations

import socket
import time
from collections.abc import Callable
from urllib.parse import urlparse

import requests

from collector.collectors.base import RunContext
from collector.http import USER_AGENT
from collector.models import Group, Observation, Status, iso

VANTAGE = {'vantage': 'ci_runner', 'country': None, 'note': 'GitHub-hosted runner, outside PK'}


class RunnerProbeCollector:
  name = 'runner_probe'

  def collect(self, ctx: RunContext) -> list[Observation]:
    timeout = float(ctx.settings(self.name).get('timeout_s', 8))
    targets = ctx.config['targets']
    controls = ctx.config['international_control_targets']
    out: list[Observation] = []
    for t in targets.get('domains', []) + controls.get('domains', []):
      out.append(self._run(ctx, t, 'dns', t['name'], lambda n=t['name']: _dns(n)))
    for t in targets.get('ips', []) + controls.get('ips', []):
      port = int(t.get('port', 443))
      out.append(self._run(ctx, t, 'tcp', f"{t['address']}:{port}",
                           lambda a=t['address'], p=port: _tcp(a, p, timeout)))
    for t in targets.get('urls', []) + controls.get('urls', []):
      out.append(self._run(ctx, t, 'https', t['url'], lambda u=t['url']: _https(u, timeout)))
    return out

  def _run(self, ctx: RunContext, t: dict, kind: str, target: str,
           fn: Callable[[], dict]) -> Observation:
    t0 = time.monotonic()
    status, metrics, error = Status.OK, {}, None
    try:
      metrics = fn()
    except (socket.timeout, TimeoutError, requests.Timeout) as exc:
      status, error = Status.TARGET_FAILURE, {'kind': 'timeout', 'detail': repr(exc)[:200]}
    except (OSError, requests.RequestException) as exc:
      status, error = Status.TARGET_FAILURE, {'kind': kind, 'detail': repr(exc)[:200]}
    metrics['duration_ms'] = round((time.monotonic() - t0) * 1000)
    return Observation(
      collector=self.name, measurement_type=f'runner_{kind}', timestamp=iso(ctx.started_at),
      target=target, status=status, group=Group(t.get('group', 'unknown')),
      location=VANTAGE, metrics=metrics, error=error,
      raw_metadata={'category': t.get('category'), 'description': t.get('description')},
    )


def _dns(name: str) -> dict:
  infos = socket.getaddrinfo(name, None, proto=socket.IPPROTO_TCP)
  return {'addresses': sorted({i[4][0] for i in infos})[:10]}


def _tcp(address: str, port: int, timeout: float) -> dict:
  with socket.create_connection((address, port), timeout=timeout):
    return {'connected': True}


def _https(url: str, timeout: float) -> dict:
  with requests.get(url, timeout=timeout, stream=True, allow_redirects=True,
                    headers={'User-Agent': USER_AGENT}) as resp:  # headers only; body not read
    return {'http_status': resp.status_code, 'final_host': urlparse(resp.url).netloc}
