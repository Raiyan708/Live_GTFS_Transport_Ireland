"""One definition of how an orchestrator run id becomes a file name.

Shared by the Glue job (writes serving/_manifests/<id>.json) and the Airflow DAG (reads it back).
Two copies would eventually drift and the DAG would look for a manifest that was never written.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone


def safe_run_id(run_id: str) -> str:
    """Airflow run ids look like 'scheduled__2026-11-06T16:00:00+00:00' (':' and '+' are unsafe in keys)."""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id) if run_id else ""
    return cleaned or datetime.now(timezone.utc).strftime("manual_%Y%m%dT%H%M%S")
