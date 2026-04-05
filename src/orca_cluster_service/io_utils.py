from __future__ import annotations

import csv
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def atomic_write_text(path: Path, content: str) -> None:
    ensure_directory(path.parent)
    with NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8", newline="") as handle:
        handle.write(content)
        temp_name = handle.name
    Path(temp_name).replace(path)


def atomic_write_json(path: Path, payload: object) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def write_csv(path: Path, rows: Iterable[dict[str, object]], fieldnames: list[str]) -> None:
    ensure_directory(path.parent)
    with NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        temp_name = handle.name
    Path(temp_name).replace(path)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
