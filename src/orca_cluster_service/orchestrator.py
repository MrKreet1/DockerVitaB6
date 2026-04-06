from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
import shutil

from .config import load_config
from .geometry import build_geometry, read_xyz
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

        for run_definition in self._build_optimization_runs("coarse", self.config.distances, seed_offset=0):
            self._execute_run(run_definition, records)

        best_optimization = self._select_best_record(records, calculation_type="optimization")
        if self.config.refinement.enabled and best_optimization is not None:
            refine_distances = self._build_refinement_distances(float(best_optimization["distance"]))
            for run_definition in self._build_optimization_runs(
                "refine", refine_distances, seed_offset=100000
            ):
                self._execute_run(run_definition, records)

        if self.config.frequency.enabled:
            for run_definition in self._build_frequency_runs(records, seed_offset=200000):
                self._execute_run(run_definition, records)

        if self.config.single_point.enabled:
            single_point_seed_offset = 300000 if self.config.frequency.enabled else 200000
            for run_definition in self._build_single_point_runs(records, seed_offset=single_point_seed_offset):
                self._execute_run(run_definition, records)

        self._persist_campaign_outputs(records)
        best_record = self._select_best_record(records)
        if best_record is not None:
            self._cleanup_non_best_runs(records, best_record["run_id"])
            self._persist_campaign_outputs(records)

        return 0 if best_record else 1

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
            "Running %s | type=%s stage=%s multiplicity=%s distance=%.4f repeat=%s",
            run_definition.run_id,
            run_definition.calculation_type,
            run_definition.stage,
            run_definition.multiplicity,
            run_definition.distance,
            run_definition.repeat_index,
        )

        try:
            coordinates = self._resolve_coordinates(run_definition, records)
            execution = self.runner.run(run_definition, coordinates)
            record = {
                "run_id": run_definition.run_id,
                "stage": run_definition.stage,
                "calculation_type": run_definition.calculation_type,
                "method": run_definition.method,
                "distance": run_definition.distance,
                "multiplicity": run_definition.multiplicity,
                "repeat_index": run_definition.repeat_index,
                "seed": run_definition.seed,
                "source_run_id": run_definition.source_run_id or "",
                "status": execution.status,
                "exit_code": execution.exit_code,
                "energy_hartree": execution.energy_hartree,
                "minimum_frequency_cm1": (
                    min(execution.frequencies_cm1) if execution.frequencies_cm1 else None
                ),
                "imaginary_frequency_count": sum(
                    1 for frequency in execution.frequencies_cm1 if frequency < 0.0
                ),
                "frequency_check_passed": self._frequency_check_passed(
                    run_definition,
                    execution.frequencies_cm1,
                    records,
                ),
                "terminated_normally": execution.terminated_normally,
                "runtime_seconds": round(execution.runtime_seconds, 6),
                "run_dir": str(run_definition.run_dir.relative_to(self.campaign_dir)),
                "input_file": self._relative_path(execution.input_path),
                "output_file": self._relative_path(execution.output_path),
                "initial_xyz": self._relative_path(execution.initial_xyz_path),
                "optimized_xyz": self._relative_path(execution.optimized_xyz_path),
                "error": execution.error or "",
                "updated_at": datetime.now(UTC).isoformat(),
                "accuracy_level": self._accuracy_level(run_definition.calculation_type),
            }
            if execution.status == "success":
                self._cleanup_run_artifacts(
                    run_definition.run_dir, self.config.cleanup.delete_patterns_after_success
                )
        except Exception as exc:
            record = self._build_failed_record(run_definition, str(exc))

        atomic_write_json(result_path, record)
        records[run_definition.run_id] = record
        self._persist_campaign_outputs(records)
        LOGGER.info(
            "Completed %s | status=%s energy=%s",
            run_definition.run_id,
            record["status"],
            record["energy_hartree"],
        )

    def _resolve_coordinates(
        self, run_definition: RunDefinition, records: dict[str, dict]
    ) -> list:
        if run_definition.source_run_id:
            source_record = records.get(run_definition.source_run_id)
            if source_record is None:
                raise ValueError(
                    f"Source run '{run_definition.source_run_id}' was not found for {run_definition.run_id}."
                )
            geometry_relative = source_record.get("optimized_xyz") or source_record.get("initial_xyz")
            if not geometry_relative:
                raise ValueError(
                    f"Source run '{run_definition.source_run_id}' does not have geometry for {run_definition.run_id}."
                )
            return read_xyz(self.campaign_dir / geometry_relative)

        return build_geometry(
            num_atoms=self.config.num_atoms,
            element=self.config.element,
            template_name=self.config.geometry_template,
            distance=run_definition.distance,
            jitter=self.config.coordinate_jitter,
            seed=run_definition.seed,
            coordinate_template_file=self.config.coordinate_template_file,
        )

    def _persist_campaign_outputs(self, records: dict[str, dict]) -> None:
        rows = self._sorted_records(records)
        write_csv(
            self.campaign_dir / "summary.csv",
            rows,
            [
                "run_id",
                "stage",
                "calculation_type",
                "method",
                "distance",
                "multiplicity",
                "repeat_index",
                "seed",
                "source_run_id",
                "status",
                "exit_code",
                "energy_hartree",
                "minimum_frequency_cm1",
                "imaginary_frequency_count",
                "frequency_check_passed",
                "terminated_normally",
                "runtime_seconds",
                "accuracy_level",
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
                "selection_basis": (
                    "single_point_energy"
                    if best_record["calculation_type"] == "single_point"
                    else "frequency_verified_energy"
                    if best_record["calculation_type"] == "frequency"
                    else "optimization_energy"
                ),
                "best_run": best_record,
                "summary_csv": "summary.csv",
                "best_xyz": "best.xyz",
            },
        )

    def _build_optimization_runs(
        self,
        stage: str,
        distances: tuple[float, ...],
        *,
        seed_offset: int,
    ) -> list[RunDefinition]:
        definitions: list[RunDefinition] = []
        for multiplicity_index, multiplicity in enumerate(self.config.orca.multiplicities):
            for distance_index, distance in enumerate(distances):
                distance_slug = _distance_slug(distance)
                for repeat_index in range(1, self.config.repeats_per_distance + 1):
                    run_id = (
                        f"{stage}-m{multiplicity:02d}-d{distance_slug}-r{repeat_index:02d}"
                    )
                    run_dir = (
                        self.runs_root
                        / stage
                        / f"m_{multiplicity:02d}"
                        / f"d_{distance_slug}"
                        / f"r_{repeat_index:02d}"
                    )
                    seed = (
                        self.config.base_seed
                        + seed_offset
                        + multiplicity_index * 100000
                        + distance_index * 1000
                        + repeat_index
                    )
                    definitions.append(
                        RunDefinition(
                            run_id=run_id,
                            stage=stage,
                            calculation_type="optimization",
                            method=self.config.orca.method,
                            extra_blocks="",
                            distance=distance,
                            multiplicity=multiplicity,
                            repeat_index=repeat_index,
                            seed=seed,
                            run_dir=run_dir,
                        )
                    )
        return definitions

    def _build_single_point_runs(
        self, records: dict[str, dict], *, seed_offset: int
    ) -> list[RunDefinition]:
        source_records = self._select_top_records(
            records,
            calculation_type="frequency" if self.config.frequency.enabled else "optimization",
            top_n=self.config.single_point.top_n,
        )
        definitions: list[RunDefinition] = []
        for rank, source_record in enumerate(source_records, start=1):
            source_run_id = str(source_record["run_id"])
            run_id = f"single-point-r{rank:02d}-{source_run_id}"
            run_dir = self.runs_root / "single_point" / f"rank_{rank:02d}_{source_run_id}"
            definitions.append(
                RunDefinition(
                    run_id=run_id,
                    stage="single_point",
                    calculation_type="single_point",
                    method=self.config.single_point.method,
                    extra_blocks=self.config.single_point.extra_blocks,
                    distance=float(source_record["distance"]),
                    multiplicity=int(source_record["multiplicity"]),
                    repeat_index=int(source_record["repeat_index"]),
                    seed=self.config.base_seed + seed_offset + rank,
                    run_dir=run_dir,
                    source_run_id=source_run_id,
                )
            )
        return definitions

    def _build_frequency_runs(
        self, records: dict[str, dict], *, seed_offset: int
    ) -> list[RunDefinition]:
        source_records = self._select_top_records(
            records,
            calculation_type="optimization",
            top_n=self.config.frequency.top_n,
        )
        definitions: list[RunDefinition] = []
        for rank, source_record in enumerate(source_records, start=1):
            source_run_id = str(source_record["run_id"])
            run_id = f"frequency-r{rank:02d}-{source_run_id}"
            run_dir = self.runs_root / "frequency" / f"rank_{rank:02d}_{source_run_id}"
            definitions.append(
                RunDefinition(
                    run_id=run_id,
                    stage="frequency",
                    calculation_type="frequency",
                    method=self.config.frequency.method,
                    extra_blocks=self.config.frequency.extra_blocks,
                    distance=float(source_record["distance"]),
                    multiplicity=int(source_record["multiplicity"]),
                    repeat_index=int(source_record["repeat_index"]),
                    seed=self.config.base_seed + seed_offset + rank,
                    run_dir=run_dir,
                    source_run_id=source_run_id,
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
            record = self._normalize_record(read_json(result_file))
            records[record["run_id"]] = record
        return records

    def _normalize_record(self, record: dict) -> dict:
        calculation_type = record.get("calculation_type")
        if not calculation_type:
            calculation_type = "single_point" if record.get("stage") == "single_point" else "optimization"
            record["calculation_type"] = calculation_type
        if record.get("stage") == "frequency":
            record["calculation_type"] = "frequency"
            calculation_type = "frequency"
        default_method = self.config.orca.method
        if calculation_type == "frequency":
            default_method = self.config.frequency.method
        if calculation_type == "single_point":
            default_method = self.config.single_point.method
        record.setdefault("method", default_method)
        record.setdefault("multiplicity", self.config.orca.multiplicities[0])
        record.setdefault("source_run_id", "")
        record.setdefault("accuracy_level", self._accuracy_level(calculation_type))
        record.setdefault("minimum_frequency_cm1", None)
        record.setdefault("imaginary_frequency_count", None)
        record.setdefault("frequency_check_passed", None)
        return record

    def _sorted_records(self, records: dict[str, dict]) -> list[dict]:
        stage_order = {"coarse": 0, "refine": 1, "frequency": 2, "single_point": 3}
        return sorted(
            records.values(),
            key=lambda item: (
                stage_order.get(str(item["stage"]), 99),
                int(item.get("multiplicity", 1)),
                float(item["distance"]),
                int(item["repeat_index"]),
            ),
        )

    def _select_best_record(
        self, records: dict[str, dict], *, calculation_type: str | None = None
    ) -> dict | None:
        successful = [
            self._normalize_record(record.copy())
            for record in records.values()
            if record["status"] == "success" and record["energy_hartree"] is not None
        ]
        if calculation_type is not None:
            successful = [
                record for record in successful if record["calculation_type"] == calculation_type
            ]
            if calculation_type == "frequency":
                successful = [
                    record for record in successful if record.get("frequency_check_passed") is True
                ]
            if calculation_type == "single_point" and self.config.frequency.enabled:
                successful = [
                    record for record in successful if record.get("frequency_check_passed") is True
                ]
        elif successful:
            if self.config.frequency.enabled:
                has_frequency_records = any(
                    self._normalize_record(record.copy())["calculation_type"] == "frequency"
                    for record in records.values()
                )
                if has_frequency_records:
                    successful = [
                        record
                        for record in successful
                        if (
                            record["calculation_type"] == "single_point"
                            and record.get("frequency_check_passed") is True
                        )
                        or (
                            record["calculation_type"] == "frequency"
                            and record.get("frequency_check_passed") is True
                        )
                    ]
                    if not successful:
                        return None
            best_accuracy = max(int(record.get("accuracy_level", 1)) for record in successful)
            successful = [
                record for record in successful if int(record.get("accuracy_level", 1)) == best_accuracy
            ]

        if not successful:
            return None
        return min(successful, key=lambda item: float(item["energy_hartree"]))

    def _select_top_records(
        self, records: dict[str, dict], *, calculation_type: str, top_n: int
    ) -> list[dict]:
        successful = [
            self._normalize_record(record.copy())
            for record in records.values()
            if record["status"] == "success" and record["energy_hartree"] is not None
        ]
        successful = [record for record in successful if record["calculation_type"] == calculation_type]
        if calculation_type == "frequency":
            successful = [record for record in successful if record.get("frequency_check_passed") is True]
        successful.sort(key=lambda item: float(item["energy_hartree"]))
        return successful[:top_n]

    def _cleanup_non_best_runs(self, records: dict[str, dict], best_run_id: str) -> None:
        patterns = self.config.cleanup.delete_non_best_patterns
        if not patterns:
            return
        for record in records.values():
            if record["status"] != "success" or record["run_id"] == best_run_id:
                continue
            self._cleanup_run_artifacts(self.campaign_dir / str(record["run_dir"]), patterns)

    def _cleanup_run_artifacts(self, run_dir: Path, patterns: tuple[str, ...]) -> None:
        for pattern in patterns:
            for candidate in run_dir.glob(pattern):
                if candidate.is_file():
                    candidate.unlink(missing_ok=True)

    def _build_failed_record(self, run_definition: RunDefinition, error: str) -> dict:
        return {
            "run_id": run_definition.run_id,
            "stage": run_definition.stage,
            "calculation_type": run_definition.calculation_type,
            "method": run_definition.method,
            "distance": run_definition.distance,
            "multiplicity": run_definition.multiplicity,
            "repeat_index": run_definition.repeat_index,
            "seed": run_definition.seed,
            "source_run_id": run_definition.source_run_id or "",
            "status": "failed",
            "exit_code": 1,
            "energy_hartree": None,
            "minimum_frequency_cm1": None,
            "imaginary_frequency_count": None,
            "frequency_check_passed": None,
            "terminated_normally": False,
            "runtime_seconds": 0.0,
            "run_dir": str(run_definition.run_dir.relative_to(self.campaign_dir)),
            "input_file": "",
            "output_file": "",
            "initial_xyz": "",
            "optimized_xyz": "",
            "error": error,
            "updated_at": datetime.now(UTC).isoformat(),
            "accuracy_level": self._accuracy_level(run_definition.calculation_type),
        }

    def _write_config_snapshot(self) -> None:
        atomic_write_json(self.campaign_dir / "campaign_config.json", self.config.to_dict())

    def _relative_path(self, path: Path | None) -> str:
        if path is None:
            return ""
        return str(path.relative_to(self.campaign_dir))

    def _accuracy_level(self, calculation_type: str) -> int:
        if calculation_type == "single_point":
            return 3
        if calculation_type == "frequency":
            return 2
        return 1

    def _frequency_check_passed(
        self,
        run_definition: RunDefinition,
        frequencies_cm1: tuple[float, ...],
        records: dict[str, dict],
    ) -> bool | None:
        if run_definition.calculation_type == "frequency":
            if not frequencies_cm1:
                return False
            return min(frequencies_cm1) >= self.config.frequency.min_allowed_frequency_cm1
        if run_definition.calculation_type == "single_point" and run_definition.source_run_id:
            source_record = records.get(run_definition.source_run_id)
            if source_record is not None:
                return source_record.get("frequency_check_passed")
        return None


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
