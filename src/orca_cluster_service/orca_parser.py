from __future__ import annotations

import re
from pathlib import Path

from .models import AtomCoordinate, ParseResult


ENERGY_PATTERN = re.compile(r"FINAL SINGLE POINT ENERGY\s+(-?\d+\.\d+(?:[Ee][+-]?\d+)?)")
FREQUENCY_PATTERN = re.compile(r"^\s*\d+\s*:\s*(-?\d+\.\d+)\s*cm\*\*-1", re.MULTILINE)
ELEMENT_PATTERN = re.compile(r"^[A-Z][a-z]?$")


def parse_orca_output(path: Path) -> ParseResult:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    energies = [float(match.group(1)) for match in ENERGY_PATTERN.finditer(text)]
    frequencies = tuple(float(match.group(1)) for match in FREQUENCY_PATTERN.finditer(text))
    geometry = _extract_last_geometry(lines)
    terminated_normally = "ORCA TERMINATED NORMALLY" in text
    return ParseResult(
        terminated_normally=terminated_normally,
        energy_hartree=energies[-1] if energies else None,
        geometry=geometry,
        frequencies_cm1=frequencies,
    )


def tail_text(path: Path, limit: int = 30) -> str:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-limit:])


def _extract_last_geometry(lines: list[str]) -> list[AtomCoordinate]:
    last_geometry: list[AtomCoordinate] = []
    marker = "CARTESIAN COORDINATES (ANGSTROEM)"
    for index, line in enumerate(lines):
        if marker not in line.upper():
            continue
        candidate: list[AtomCoordinate] = []
        started = False
        for current in lines[index + 1 :]:
            stripped = current.strip()
            if not stripped:
                if started:
                    break
                continue
            parsed = _parse_coordinate_line(stripped)
            if parsed is None:
                if started:
                    break
                continue
            candidate.append(parsed)
            started = True
        if candidate:
            last_geometry = candidate
    return last_geometry


def _parse_coordinate_line(line: str) -> AtomCoordinate | None:
    parts = line.split()
    if not parts:
        return None

    if ELEMENT_PATTERN.match(parts[0]):
        floats = _extract_floats(parts[1:])
        if len(floats) >= 3:
            return AtomCoordinate(parts[0], floats[0], floats[1], floats[2])
        return None

    if parts[0].isdigit() and len(parts) >= 5 and ELEMENT_PATTERN.match(parts[1]):
        floats = _extract_floats(parts[2:])
        if len(floats) >= 3:
            x, y, z = floats[-3:]
            return AtomCoordinate(parts[1], x, y, z)
    return None


def _extract_floats(parts: list[str]) -> list[float]:
    values: list[float] = []
    for part in parts:
        try:
            values.append(float(part))
        except ValueError:
            continue
    return values
