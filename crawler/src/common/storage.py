from __future__ import annotations

import csv
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping

from .constants import EXPORT_DIR, INTERIM_LINKS_DIR, NULL, SCHEMAS


def ensure_directories() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    INTERIM_LINKS_DIR.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_row(row: Mapping[str, object], schema_name: str) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for field in SCHEMAS[schema_name]:
        value = row.get(field, NULL)
        if value is None:
            normalized[field] = NULL
            continue
        text = str(value).strip()
        normalized[field] = text if text else NULL
    return normalized


def write_csv(schema_name: str, rows: Iterable[Mapping[str, object]], filename: str) -> Path:
    ensure_directories()
    output_path = EXPORT_DIR / filename
    schema = SCHEMAS[schema_name]
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=schema)
        writer.writeheader()
        for row in rows:
            writer.writerow(normalize_row(row, schema_name))
    return output_path


def save_links(filename: str, rows: Iterable[Mapping[str, object]]) -> Path:
    ensure_directories()
    output_path = INTERIM_LINKS_DIR / filename
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(list(rows), file, ensure_ascii=False, indent=2)
    return output_path


def as_row(item: object) -> dict[str, object]:
    if is_dataclass(item):
        return asdict(item)
    return dict(item)
