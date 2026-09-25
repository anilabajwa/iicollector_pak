'''
Pakistan / Islamabad Internet Connectivity Monitor - scheduled observational collector.

PROJECT STRUCTURE
-----------------
  collector/
    __main__.py          entry point (this file): bootstraps deps, runs one collection
    bootstrap.py         silent pip install of missing dependencies
    config.py            YAML loading + light validation
    models.py            Observation envelope, Status/Group enums, time helpers
    http.py              polite HTTP client (timeouts, backoff, rate-limit handling)
    runner.py            orchestrates collectors, failure boundaries, persistence, summary
    storage.py           date-partitioned, de-duplicated JSONL store
    collectors/
      base.py            RunContext, Collector protocol, error-observation helper
      ioda.py            IODA signals + outage events
      ooni.py            OONI web_connectivity (+ optional small detail extraction)
      ripe_atlas.py      RIPE Atlas existing public measurements, PK + control probes
      bgp.py             RIPEstat routing-status + BGP updates for PK ASNs/prefixes
      runner_probe.py    tiny DNS/TCP/HTTPS control from the (non-PK) CI runner
  config/targets.yaml    ALL targets / ASNs / prefixes / measurement IDs live here
  data/YYYY-MM-DD.jsonl  output, partitioned by UTC collection date
  analysis/              later
  tests/                 pytest unit tests
  .github/workflows/collect.yml   GitHub requires workflows here (spec's 'workflow/')

SETUP
-----
  1. Create a PRIVATE GitHub repo, push this tree.
  2. Settings > Actions > General > Workflow permissions: 'Read and write'.
  3. Review config/targets.yaml (entries marked VERIFY). [DONE]
  4. Actions tab > 'collect' > 'Run workflow' to trigger manually; cron then runs every 5 min.

LOCAL RUN
---------
  python -m collector                       # deps auto-installed silently
  python -m collector --dry-run             # collect + print, write nothing
  python -m collector --only ooni,bgp       # subset of collectors
  python -m pytest tests                    # tests (pip install pytest)
'''
from collector.bootstrap import ensure_dependencies

ensure_dependencies()

from collector.runner import main  # noqa: E402 - must follow bootstrap

raise SystemExit(main())
