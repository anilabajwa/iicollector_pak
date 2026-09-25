'''Polite JSON HTTP client: timeouts, per-host spacing, bounded exponential backoff.'''
from __future__ import annotations

import threading
import time
from typing import Any
from urllib.parse import urlparse

import requests

from collector.models import Status

USER_AGENT = 'pk-connectivity-monitor/0.1 (observational research; contact: repo owner)'


class FetchError(Exception):
  '''A classified failure to obtain data from an API.'''

  def __init__(self, status: Status, message: str, *, url: str | None = None,
               http_status: int | None = None, attempts: int = 0) -> None:
    super().__init__(message)
    self.status, self.url, self.http_status, self.attempts = status, url, http_status, attempts

  def as_error(self) -> dict[str, Any]:
    return {'kind': str(self.status), 'message': str(self)[:500], 'url': self.url,
            'http_status': self.http_status, 'attempts': self.attempts}


class PoliteClient:
  def __init__(self, *, timeout_s: float = 20, max_retries: int = 2, backoff_base_s: float = 2,
               min_interval_s: float = 0.5, max_retry_after_s: float = 30) -> None:
    self.timeout_s, self.max_retries = timeout_s, max_retries
    self.backoff_base_s, self.min_interval_s = backoff_base_s, min_interval_s
    self.max_retry_after_s = max_retry_after_s
    self._session = requests.Session()  # shared across threads; fine for plain GETs
    self._session.headers.update({'User-Agent': USER_AGENT, 'Accept': 'application/json'})
    self._next_slot: dict[str, float] = {}
    self._lock = threading.Lock()

  def _throttle(self, host: str) -> None:
    '''Space requests to the same host by at least min_interval_s.'''
    with self._lock:
      now = time.monotonic()
      start = max(now, self._next_slot.get(host, 0))
      self._next_slot[host] = start + self.min_interval_s
    if (wait := start - now) > 0:
      time.sleep(wait)

  def get_json(self, url: str, params: dict | None = None, *,
               timeout_s: float | None = None) -> tuple[Any, dict[str, Any]]:
    '''Return (parsed_json, response_meta) or raise FetchError.'''
    host = urlparse(url).netloc
    attempts = 0
    while True:
      attempts += 1
      self._throttle(host)
      t0 = time.monotonic()
      retry_after: float | None = None
      try:
        resp = self._session.get(url, params=params, timeout=timeout_s or self.timeout_s)
      except requests.Timeout as exc:
        err = FetchError(Status.TIMEOUT, repr(exc), url=url, attempts=attempts)
      except requests.RequestException as exc:
        err = FetchError(Status.API_ERROR, repr(exc), url=url, attempts=attempts)
      else:
        code = resp.status_code
        if code == 429:
          err = FetchError(Status.RATE_LIMITED, 'HTTP 429', url=resp.url, http_status=code,
                           attempts=attempts)
          retry_after = _parse_retry_after(resp.headers.get('Retry-After'))
        elif code >= 500:
          err = FetchError(Status.API_ERROR, f'HTTP {code}', url=resp.url, http_status=code,
                           attempts=attempts)
        elif code >= 400:  # client errors will not improve on retry
          raise FetchError(Status.API_ERROR, f'HTTP {code}: {resp.text[:200]}', url=resp.url,
                           http_status=code, attempts=attempts)
        else:
          try:
            data = resp.json()
          except ValueError as exc:
            raise FetchError(Status.MALFORMED, f'invalid JSON: {exc}', url=resp.url,
                             http_status=code, attempts=attempts) from exc
          meta = {'url': resp.url, 'http_status': code, 'attempts': attempts,
                  'elapsed_ms': round((time.monotonic() - t0) * 1000), 'bytes': len(resp.content)}
          return data, meta
      if attempts > self.max_retries:
        raise err
      delay = self.backoff_base_s * 2 ** (attempts - 1)
      if retry_after is not None:
        if retry_after > self.max_retry_after_s:
          raise err  # do not hammer, and do not stall the run
        delay = max(delay, retry_after)
      time.sleep(delay)


def _parse_retry_after(value: str | None) -> float | None:
  try:
    return float(value) if value else None
  except ValueError:
    return None  # HTTP-date form: ignore and fall back to exponential backoff
