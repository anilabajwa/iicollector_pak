'''Collector registry. Add new sources here.'''
from collector.collectors.bgp import BgpCollector
from collector.collectors.ioda import IodaCollector
from collector.collectors.ooni import OoniCollector
from collector.collectors.ripe_atlas import RipeAtlasCollector
from collector.collectors.runner_probe import RunnerProbeCollector

REGISTRY = {c.name: c for c in (
  IodaCollector, OoniCollector, RipeAtlasCollector, BgpCollector, RunnerProbeCollector,
)}
