'''Load and lightly validate config/targets.yaml.'''
from pathlib import Path
from typing import Any

import yaml

REQUIRED_KEYS = ('targets', 'pakistan_asns', 'pakistan_prefixes', 'ooni_targets',
                 'ripe_atlas_measurements', 'international_control_targets', 'collectors')


class ConfigError(ValueError):
  pass


def load_config(path: Path) -> dict[str, Any]:
  with path.open(encoding='utf-8') as fh:
    cfg = yaml.safe_load(fh) or {}
  missing = [k for k in REQUIRED_KEYS if k not in cfg]
  if missing:
    raise ConfigError(f'config missing keys: {missing}')
  for entry in cfg['pakistan_asns']:
    if not {'asn', 'name', 'access_type'} <= entry.keys():
      raise ConfigError(f'pakistan_asns entry needs asn/name/access_type: {entry}')
  return cfg


def asn_index(cfg: dict[str, Any]) -> dict[int, dict]:
  '''ASN -> {name, access_type, description}; mobile vs fixed is first-class.'''
  return {
    int(e['asn']): {k: e.get(k) for k in ('name', 'access_type', 'description')}
    for e in cfg['pakistan_asns']
  }
