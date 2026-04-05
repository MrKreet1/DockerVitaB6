from __future__ import annotations

import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path

from .config import load_config
from .geometry import build_geometry
from .io_utils import atomic_write_json, ensure_directory, read_json, write_csv
from .models import CampaignConfig, RunDefinition
from .runner import build_runner


LOGGER = logging.getLogger("orca_cluster_service")


class CampaignOrchestrator:
    def __init__(self, config: CampaignConfig) -> None:
        self.config = config
        self.campaign_dir = ensure_directory(config.campaign_dir)
        self.runs_root = ensure_directory(self.campaign_dir / "runs")
        self.runner = build_runner(config.orca)

    def run(self) -> int:
        self._write_config_snapshot()
        records = self._load_existing_records()

        coarse_runs = self._build_run_definitions("coarse", self.config.distances, seed_offset=0)
        for run_definition in coarse_runs:
            self._execute_run(run_definition, records)

        best_record = self._select_best_record(records)
        if self.config.refinement.enabled and best_record is not None:
            refine_distances = self._build_refinement_distances(float(best_record["distance"]))
            refine_runs = self._build_run_definitions("refine", refine_distances, seed_offset=100000)
            for run_definition in refine_runs:
                self._execute_run(run_definition, records)

        self._persist_campaign_outputs(records)
        return 0 if self._select_best_record(records) else 1

    def _execute_run(self, run_definition: RunDefinition, records: dict[str, dict]) -> None:
        result_path = run_definition.run_dir / "result.json"
        if (
            run_definition.run_id in records
            and result_path.exists()
            and self.config.resume
            and not self.config.force_rerun
        ):
            LOGGER.info("Skipping completed run %s", run_definition.run_id)
            return

        ensure_directory(run_definition.run_dir)
        LOGGER.info(
            "Running %s | stage=%s distance=%.4f repeat=%s",
            run_definition.run_id,
            run_definition.stage,
            run_definition.distance,
            run_definition.repeat_index,
        )

        coordinates = build_geometry(
            num_atoms=self.config.num_atoms,
            element=self.config.element,
            template_name=self.config.geometry_template,
            distance=run_definition.distance,
            jitter=self.config.coordinate_jitter,
            seed=run_definition.seed,
            coordinate_template_file=self.config.coordinate_template_file,
        )
        execution = self.runner.run(run_definition, coordinates)
        record = {
            "run_id": run_definition.run_id,
            "stage": run_definition.stage,
            "distance": run_definition.distance,
            "repeat_index": run_definition.repeat_index,
            "seed": run_definition.seed,
            "status": execution.status,
            "exit_code": execution.exit_code,
            "energy_hartree": execution.energy_hartree,
            "terminated_normally": execution.terminated_normally,
            "runtime_seconds": round(execution.runtime_seconds, 6),
            "run_dir": str(run_definition.run_dir.relative_to(self.campaign_dir)),
            "input_file": str(execution.input_path.relative_to(self.campaign_dir)),
            "output_file": str(execution.output_path.relative_to(self.campaign_dir)),
            "initial_xyz": str(execution.initial_xyz_path.relative_to(self.campaign_dir)),
            "optimized_xyz": (
                str(execution.optimized_xyz_path.relative_to(self.campaign_dir))
                if execution.optimized_xyz_path
                else ""
            ),
            "error": execution.error or "",
            "updated_at": datetime.now(UTC).isoformat(),
        }
        atomic_write_json(result_path, record)
        records[run_definition.run_id] = record
        self._persist_campaign_outputs(records)
        LOGGER.info(
            "Completed %s | status=%s energy=%s",
            run_definition.run_id,
            execution.status,
            execution.energy_hartree,
        )

    def _persist_campaign_outputs(self, records: dict[str, dict]) -> None:
        rows = self._sorted_records(records)
        write_csv(
            self.campaign_dir / "summary.csv",
            rows,
            [
                "run_id",
                "stage",
                "distance",
                "repeat_index",
                "seed",
                "status",
                "exit_code",
                "energy_hartree",
                "terminated_normally",
                "runtime_seconds",
                "run_dir",
                "input_file",
                "output_file",
                "initial_xyz",
                "optimized_xyz",
                "updated_at",
                "error",
            ],
        )

        best_record = self._select_best_record(records)
        best_json_path = self.campaign_dir / "best.json"
        best_xyz_path = self.campaign_dir / "best.xyz"
        if best_record is None:
            atomic_write_json(
                best_json_path,
                {
                    "campaign_name": self.config.campaign_name,
                    "status": "not_found",
                    "updated_at": datetime.now(UTC).isoformat(),
                },
            )
            if best_xyz_path.exists():
                best_xyz_path.unlink()
            return

        geometry_relative = best_record["optimized_xyz"] or best_record["initial_xyz"]
        if geometry_relative:
            shutil.copyfile(self.campaign_dir / geometry_relative, best_xyz_path)
        atomic_write_json(
            best_json_path,
            {
                "campaign_name": self.config.campaign_name,
                "status": "ok",
                "updated_at": datetime.now(UTC).isoformat(),
                "best_run": best_record,
                "summary_csv": "summary.csv",
                "best_xyz": "best.xyz",
            },
        )

    def _build_run_definitions(
        self,
        stage: str,
        distances: tuple[float, ...],
        *,
        seed_offset: int,
    ) -> list[RunDefinition]:
        definitions: list[RunDefinition] = []
        for distance_index, distance in enumerate(distances):
            distance_slug = _distance_slug(distance)
            for repeat_index in range(1, self.config.repeats_per_distance + 1):
                run_id = f"{stage}-d{distance_slug}-r{repeat_index:02d}"
                run_dir = self.runs_root / stage / f"d_{distance_slug}" / f"r_{repeat_index:02d}"
                seed = self.config.base_seed + seed_offset + distance_index * 1000 + repeat_index
                definitions.append(
                    RunDefinition(
                        run_id=run_id,
                        stage=stage,
                        distance=distance,
                        repeat_index=repeat_index,
                        seed=seed,
                        run_dir=run_dir,
                    )
                )
        return definitions

    def _build_refinement_distances(self, center_distance: float) -> tuple[float, ...]:
        unique_base_distances = sorted(set(self.config.distances))
        if self.config.refinement.step is not None:
            step = self.config.refinement.step
        elif len(unique_base_distances) > 1:
            spacings = [
                right - left
                for left, right in zip(unique_base_distances, unique_base_distances[1:])
                if right > left
            ]
            step = min(spacings) / 2.0 if spacings else center_distance * 0.05
        else:
            step = center_distance * 0.05

        candidates = []
        for delta_index in range(-self.config.refinement.points, self.config.refinement.points + 1):
            if delta_index == 0:
                continue
            candidate = round(center_distance + delta_index * step, 6)
            if candidate <= 0:
                continue
            if candidate in self.config.distances:
                continue
            candidates.append(candidate)
        return tuple(sorted(dict.fromkeys(candidates)))

    def _load_existing_records(self) -> dict[str, dict]:
        records: dict[str, dict] = {}
        if not self.runs_root.exists():
            return records
        for result_file in self.runs_root.rglob("result.json"):
            record = read_json(result_file)
            records[record["run_id"]] = record
        return records

    def _sorted_records(self, records: dict[str, dict]) -> list[dict]:
        stage_order = {"coarse": 0, "refine": 1}
        return sorted(
            records.values(),
            key=lambda item: (
                stage_order.get(item["stage"], 99),
                float(item["distance"]),
                int(item["repeat_index"]),
            ),
        )

    def _select_best_record(self, records: dict[str, dict]) -> dict | None:
        successful = [
            record
            for record in records.values()
            if record["status"] == "success" and record["energy_hartree"] is not None
        ]
        if not successful:
            return None
        return min(successful, key=lambda item: float(item["energy_hartree"]))

    def _write_config_snapshot(self) -> None:
        atomic_write_json(self.campaign_dir / "campaign_config.json", self.config.to_dict())


def configure_logging(campaign_dir: Path) -> None:
    ensure_directory(campaign_dir)
    log_path = campaign_dir / "service.log"
    LOGGER.setLevel(logging.INFO)
    close_logging()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    LOGGER.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def close_logging() -> None:
    for handler in list(LOGGER.handlers):
        handler.flush()
        handler.close()
        LOGGER.removeHandler(handler)


def run_campaign(config_path: str | Path | None = None) -> int:
    config = load_config(config_path)
    configure_logging(config.campaign_dir)
    try:
        LOGGER.info("Starting campaign %s", config.campaign_name)
        orchestrator = CampaignOrchestrator(config)
        exit_code = orchestrator.run()
        LOGGER.info("Campaign %s finished with exit code %s", config.campaign_name, exit_code)
        return exit_code
    finally:
        close_logging()


def _distance_slug(distance: float) -> str:
    return f"{distance:.4f}".replace("-", "m").replace(".", "p")
