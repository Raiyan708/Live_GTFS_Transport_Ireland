"""The dashboard colours vehicles by delay band; the alert engine raises alerts by tier. They must agree, or the map
would show a 'moderate' dot for a trip that never alerted. The TS constants are parsed from source and compared."""
import re
from pathlib import Path

from gtfsrt.config import AlertConfig

TS = Path(__file__).resolve().parents[1] / "web" / "src" / "lib" / "delay.ts"


def test_ts_delay_thresholds_match_the_alert_engine_defaults():
    m = re.search(r"THRESHOLDS\s*=\s*\{\s*moderate:\s*(\d+),\s*severe:\s*(\d+),\s*critical:\s*(\d+)", TS.read_text())
    assert m, "THRESHOLDS constant not found in delay.ts (was it renamed?)"
    cfg = AlertConfig()
    assert tuple(int(x) for x in m.groups()) == (cfg.on_seconds, *cfg.escalation_seconds)
