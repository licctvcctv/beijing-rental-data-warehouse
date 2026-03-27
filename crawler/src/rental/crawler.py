"""Beijing rental housing data collector.

This module provides the entry point that the CLI invokes via ``python -m main rental``.
In production it would scrape listing sites; here it delegates to the synthetic
data generator (``gen_rental_data.py``) so the pipeline can run offline.
"""
from __future__ import annotations

import csv
import logging
import subprocess
import sys
from pathlib import Path
from typing import Optional

from common.constants import EXPORT_DIR
from common.storage import write_csv

logger = logging.getLogger(__name__)

GEN_SCRIPT = Path(__file__).resolve().parents[2] / "gen_rental_data.py"
OUTPUT_FILE = "rental_raw.csv"


def run(*, sample: Optional[int] = None) -> None:
    target = EXPORT_DIR / OUTPUT_FILE
    if not target.exists():
        logger.info("rental_raw.csv not found, generating synthetic data …")
        subprocess.check_call([sys.executable, str(GEN_SCRIPT)])

    rows: list[dict[str, str]] = []
    with target.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
            if sample and len(rows) >= sample:
                break

    out = write_csv("rental", rows, OUTPUT_FILE)
    logger.info("Exported %d rental records → %s", len(rows), out)
