from __future__ import annotations

import json
import math
import random
from pathlib import Path

from .models import AtomCoordinate


def build_geometry(
    *,
    num_atoms: int,
    element: str,
    template_name: str,
    distance: float,
    jitter: float,
    seed: int,
    coordinate_template_file: Path | None = None,
) -> list[AtomCoordinate]:
    template_points = _load_template_points(
        num_atoms=num_atoms,
        template_name=template_name,
        coordinate_template_file=coordinate_template_file,
    )
    scaled = _scale_points_to_distance(template_points, distance)
    perturbed = _apply_jitter(scaled, jitter=jitter, seed=seed)
    return [AtomCoordinate(element=element, x=x, y=y, z=z) for x, y, z in perturbed]


def write_xyz(path: Path, coordinates: list[AtomCoordinate], comment: str) -> None:
    lines = [str(len(coordinates)), comment]
    lines.extend(atom.to_xyz_line() for atom in coordinates)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_xyz(path: Path) -> list[AtomCoordinate]:
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(lines) < 3:
        raise ValueError(f"XYZ file is too short: {path}")
    coordinates: list[AtomCoordinate] = []
    for line in lines[2:]:
        parts = line.split()
        if len(parts) < 4:
            raise ValueError(f"Invalid XYZ line in {path}: {line}")
        coordinates.append(
            AtomCoordinate(
                element=parts[0],
                x=float(parts[1]),
                y=float(parts[2]),
                z=float(parts[3]),
            )
        )
    return coordinates


def _load_template_points(
    *,
    num_atoms: int,
    template_name: str,
    coordinate_template_file: Path | None,
) -> list[tuple[float, float, float]]:
    if coordinate_template_file:
        points = _read_points_from_file(coordinate_template_file)
        if len(points) != num_atoms:
            raise ValueError(
                f"Coordinate template has {len(points)} atoms, expected {num_atoms}."
            )
        return points

    normalized_name = template_name.strip().lower()
    if normalized_name == "icosahedron":
        return _icosahedron_points(num_atoms)
    if normalized_name == "ring":
        return _ring_points(num_atoms)
    if normalized_name == "cubic":
        return _cubic_points(num_atoms)
    raise ValueError(
        f"Unsupported geometry template '{template_name}'. Use icosahedron, ring, cubic, or COORDINATE_TEMPLATE_FILE."
    )


def _icosahedron_points(num_atoms: int) -> list[tuple[float, float, float]]:
    if num_atoms != 12:
        raise ValueError("The built-in icosahedron template requires NUM_ATOMS=12.")
    phi = (1 + math.sqrt(5)) / 2
    points = [
        (0.0, -1.0, -phi),
        (0.0, -1.0, phi),
        (0.0, 1.0, -phi),
        (0.0, 1.0, phi),
        (-1.0, -phi, 0.0),
        (-1.0, phi, 0.0),
        (1.0, -phi, 0.0),
        (1.0, phi, 0.0),
        (-phi, 0.0, -1.0),
        (phi, 0.0, -1.0),
        (-phi, 0.0, 1.0),
        (phi, 0.0, 1.0),
    ]
    return _center_points(points)


def _ring_points(num_atoms: int) -> list[tuple[float, float, float]]:
    radius = 1.0 / (2.0 * math.sin(math.pi / num_atoms))
    points: list[tuple[float, float, float]] = []
    for index in range(num_atoms):
        angle = 2.0 * math.pi * index / num_atoms
        points.append((radius * math.cos(angle), radius * math.sin(angle), 0.0))
    return _center_points(points)


def _cubic_points(num_atoms: int) -> list[tuple[float, float, float]]:
    edge = math.ceil(num_atoms ** (1.0 / 3.0))
    points: list[tuple[float, float, float]] = []
    for x in range(edge):
        for y in range(edge):
            for z in range(edge):
                if len(points) == num_atoms:
                    return _center_points(points)
                points.append((float(x), float(y), float(z)))
    return _center_points(points)


def _read_points_from_file(path: Path) -> list[tuple[float, float, float]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return [(float(item["x"]), float(item["y"]), float(item["z"])) for item in payload]
    if suffix == ".xyz":
        lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if len(lines) < 3:
            raise ValueError(f"XYZ template is too short: {path}")
        atom_lines = lines[2:]
        points = []
        for line in atom_lines:
            parts = line.split()
            if len(parts) < 4:
                raise ValueError(f"Invalid XYZ line: {line}")
            points.append((float(parts[1]), float(parts[2]), float(parts[3])))
        return points
    raise ValueError(f"Unsupported coordinate template file: {path.suffix}")


def _scale_points_to_distance(
    points: list[tuple[float, float, float]], distance: float
) -> list[tuple[float, float, float]]:
    if distance <= 0:
        raise ValueError("Distances must be positive.")
    reference_distance = _minimum_pair_distance(points)
    if reference_distance <= 0:
        raise ValueError("Geometry template contains overlapping coordinates.")
    scale = distance / reference_distance
    return [(x * scale, y * scale, z * scale) for x, y, z in points]


def _apply_jitter(
    points: list[tuple[float, float, float]], *, jitter: float, seed: int
) -> list[tuple[float, float, float]]:
    if jitter <= 0:
        return _center_points(points)
    rng = random.Random(seed)
    perturbed = [
        (
            x + rng.uniform(-jitter, jitter),
            y + rng.uniform(-jitter, jitter),
            z + rng.uniform(-jitter, jitter),
        )
        for x, y, z in points
    ]
    return _center_points(perturbed)


def _minimum_pair_distance(points: list[tuple[float, float, float]]) -> float:
    best: float | None = None
    for index, left in enumerate(points):
        for right in points[index + 1 :]:
            distance = math.dist(left, right)
            if distance == 0:
                continue
            if best is None or distance < best:
                best = distance
    if best is None:
        raise ValueError("At least two distinct points are required.")
    return best


def _center_points(points: list[tuple[float, float, float]]) -> list[tuple[float, float, float]]:
    center_x = sum(x for x, _, _ in points) / len(points)
    center_y = sum(y for _, y, _ in points) / len(points)
    center_z = sum(z for _, _, z in points) / len(points)
    return [(x - center_x, y - center_y, z - center_z) for x, y, z in points]
