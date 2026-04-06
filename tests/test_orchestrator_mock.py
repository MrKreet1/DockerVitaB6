from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from orca_cluster_service.models import (
    CampaignConfig,
    CleanupSettings,
    FrequencySettings,
    OrcaSettings,
    RefinementSettings,
    SinglePointSettings,
)
from orca_cluster_service.orchestrator import (
    CampaignOrchestrator,
    close_logging,
    configure_logging,
)


class MockCampaignTests(unittest.TestCase):
    def test_mock_campaign_creates_summary_and_best_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            results_root = Path(tmp_dir)
            config = CampaignConfig(
                campaign_name="test-campaign",
                results_root=results_root,
                num_atoms=12,
                element="B",
                geometry_template="icosahedron",
                coordinate_template_file=None,
                distances=(2.0, 2.25, 2.5, 2.75, 3.0),
                repeats_per_distance=2,
                coordinate_jitter=0.02,
                base_seed=17,
                resume=True,
                force_rerun=False,
                orca=OrcaSettings(
                    backend="mock",
                    binary="orca",
                    method="R2SCAN-3C Opt TightSCF",
                    charge=0,
                    multiplicities=(1, 3, 5),
                    processes=2,
                    maxcore_mb=512,
                    geom_max_iterations=100,
                    extra_blocks="",
                    mock_optimal_distance=2.45,
                    mock_optimal_multiplicity=3,
                    mock_base_energy=-24.0,
                    mock_noise_scale=0.0,
                ),
                refinement=RefinementSettings(enabled=True, step=0.1, points=1),
                frequency=FrequencySettings(
                    enabled=True,
                    top_n=2,
                    method="R2SCAN-3C NumFreq TightSCF",
                    extra_blocks="",
                    min_allowed_frequency_cm1=0.0,
                ),
                single_point=SinglePointSettings(
                    enabled=True,
                    top_n=2,
                    method="PBE0 D4 def2-TZVP def2/J RIJCOSX TightSCF",
                    extra_blocks="",
                ),
                cleanup=CleanupSettings(),
            )
            configure_logging(config.campaign_dir)
            try:
                orchestrator = CampaignOrchestrator(config)

                exit_code = orchestrator.run()
                self.assertEqual(exit_code, 0)

                campaign_dir = results_root / "test-campaign"
                summary_path = campaign_dir / "summary.csv"
                best_json_path = campaign_dir / "best.json"
                best_xyz_path = campaign_dir / "best.xyz"

                self.assertTrue(summary_path.exists())
                self.assertTrue(best_json_path.exists())
                self.assertTrue(best_xyz_path.exists())

                with summary_path.open("r", encoding="utf-8", newline="") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertEqual(len(rows), 46)
                self.assertIn("frequency", {row["calculation_type"] for row in rows})
                self.assertIn("single_point", {row["calculation_type"] for row in rows})

                payload = json.loads(best_json_path.read_text(encoding="utf-8"))
                self.assertEqual(payload["status"], "ok")
                self.assertEqual(payload["selection_basis"], "single_point_energy")
                self.assertEqual(payload["best_run"]["calculation_type"], "single_point")
                self.assertEqual(payload["best_run"]["multiplicity"], 3)
                self.assertTrue(payload["best_run"]["frequency_check_passed"])
                self.assertEqual(payload["best_run"]["distance"], 2.4)
            finally:
                close_logging()


if __name__ == "__main__":
    unittest.main()
