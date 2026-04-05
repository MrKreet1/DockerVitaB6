from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AtomCoordinate:
    element: str
    x: float
    y: float
    z: float

    def to_xyz_line(self) -> str:
        return f"{self.element:<2} {self.x: .8f} {self.y: .8f} {self.z: .8f}"


@dataclass(frozen=True)
class RefinementSettings:
    enabled: bool = True
    step: float | None = None
    points: int = 2

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrcaSettings:
    backend: str = "real"
    binary: str = "orca"
    method: str = "R2SCAN-3C Opt TightSCF"
    charge: int = 0
    multiplicities: tuple[int, ...] = (1,)
    processes: int = 2
    maxcore_mb: int = 1500
    geom_max_iterations: int = 200
    extra_blocks: str = ""
    mock_optimal_distance: float = 2.45
    mock_optimal_multiplicity: int = 1
    mock_base_energy: float = -24.0
    mock_noise_scale: float = 0.0005

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SinglePointSettings:
    enabled: bool = True
    top_n: int = 3
    method: str = "PBE0 D4 def2-TZVP def2/J RIJCOSX TightSCF"
    extra_blocks: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CleanupSettings:
    delete_patterns_after_success: tuple[str, ...] = (
        "*.tmp",
        "*.gbw",
        "*.densities",
        "*.cis",
        "*.engrad",
        "*.hess",
        "*.ges",
        "*.prop",
        "*.property.txt",
        "*.trj",
    )
    delete_non_best_patterns: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CampaignConfig:
    campaign_name: str
    results_root: Path
    num_atoms: int
    element: str
    geometry_template: str
    coordinate_template_file: Path | None
    distances: tuple[float, ...]
    repeats_per_distance: int
    coordinate_jitter: float
    base_seed: int
    resume: bool
    force_rerun: bool
    orca: OrcaSettings
    refinement: RefinementSettings
    single_point: SinglePointSettings
    cleanup: CleanupSettings

    @property
    def campaign_dir(self) -> Path:
        return self.results_root / self.campaign_name

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["results_root"] = str(self.results_root)
        payload["coordinate_template_file"] = (
            str(self.coordinate_template_file) if self.coordinate_template_file else None
        )
        payload["distances"] = list(self.distances)
        return payload


@dataclass(frozen=True)
class RunDefinition:
    run_id: str
    stage: str
    calculation_type: str
    method: str
    extra_blocks: str
    distance: float
    multiplicity: int
    repeat_index: int
    seed: int
    run_dir: Path
    source_run_id: str | None = None


@dataclass(frozen=True)
class ParseResult:
    terminated_normally: bool
    energy_hartree: float | None
    geometry: list[AtomCoordinate]


@dataclass(frozen=True)
class RunExecution:
    status: str
    exit_code: int
    energy_hartree: float | None
    error: str | None
    output_path: Path
    input_path: Path
    initial_xyz_path: Path
    optimized_xyz_path: Path | None
    terminated_normally: bool
    runtime_seconds: float
