from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from orca_cluster_service.geometry import build_geometry


class GeometryTests(unittest.TestCase):
    def test_icosahedron_scales_to_requested_distance(self) -> None:
        geometry = build_geometry(
            num_atoms=12,
            element="B",
            template_name="icosahedron",
            distance=2.5,
            jitter=0.0,
            seed=123,
        )
        minimum_distance = min(
            math.dist(
                (left.x, left.y, left.z),
                (right.x, right.y, right.z),
            )
            for index, left in enumerate(geometry)
            for right in geometry[index + 1 :]
        )
        self.assertAlmostEqual(minimum_distance, 2.5, places=6)


if __name__ == "__main__":
    unittest.main()
