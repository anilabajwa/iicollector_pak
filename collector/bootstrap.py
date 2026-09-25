'''Silently install missing third-party dependencies (keeps setup to one command).'''
import importlib.util
import subprocess
import sys

# import name -> pip requirement
REQUIRED: dict[str, str] = {
  'requests': 'requests>=2.31',
  'yaml': 'PyYAML>=6.0',
}


def ensure_dependencies() -> None:
  missing = [spec for module, spec in REQUIRED.items() if importlib.util.find_spec(module) is None]
  if not missing:
    return
  subprocess.run(
    [sys.executable, '-m', 'pip', 'install', '--quiet', '--disable-pip-version-check', *missing],
    check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
  )
  