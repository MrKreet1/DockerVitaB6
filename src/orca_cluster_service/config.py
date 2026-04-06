from __future__ import annotations

import json
import os
import re
import tomllib
from pathlib import Path
from typing import Any, Callable

from .models import (
    CampaignConfig,
    CleanupSettings,
    OrcaSettings,
    RefinementSettings,
    SinglePointSettings,
)


CONFIG_ENV = "CONFIG_PATH"


def load_config(config_path: str | Path | None = None) -> CampaignConfig:
    resolved_config_path = _resolve_config_path(config_path)
    file_data = _load_config_file(resolved_config_path) if resolved_config_path else {}
    config_dir = resolved_config_path.parent if resolved_config_path else Path.cwd()
    cwd = Path.cwd()

    default_results_root = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "data")
    results_root_raw = _read_value("RESULTS_ROOT", file_data, ("results_root",), default_results_root, str)

    campaign_name = _sanitize_name(
        _read_value("CAMPAIGN_NAME", file_data, ("campaign_name",), "b12-search", str)
    )
    results_root = _resolve_path(results_root_raw, cwd)
    num_atoms = _read_value("NUM_ATOMS", file_data, ("cluster", "num_atoms"), 12, int)
    element = _read_value("ELEMENT", file_data, ("cluster", "element"), "B", str)
    geometry_template = _read_value(
        "GEOMETRY_TEMPLATE", file_data, ("cluster", "geometry_template"), "icosahedron", str
    )
    coordinate_template_raw = _read_value(
        "COORDINATE_TEMPLATE_FILE",
        file_data,
        ("cluster", "coordinate_template_file"),
        None,
        str,
        allow_none=True,
    )
    coordinate_template_file = (
        _resolve_path(
            coordinate_template_raw,
            cwd if os.getenv("COORDINATE_TEMPLATE_FILE") else config_dir,
        )
        if coordinate_template_raw
        else None
    )
    distances = tuple(
        _read_value(
            "DISTANCES",
            file_data,
            ("cluster", "distances"),
            (2.0, 2.25, 2.5, 2.75, 3.0, 3.5, 4.0),
            _parse_distances,
        )
    )
    repeats_per_distance = _read_value(
        "REPEATS_PER_DISTANCE", file_data, ("cluster", "repeats_per_distance"), 3, int
    )
    coordinate_jitter = _read_value(
        "COORDINATE_JITTER", file_data, ("cluster", "coordinate_jitter"), 0.08, float
    )
    base_seed = _read_value("BASE_RANDOM_SEED", file_data, ("cluster", "base_seed"), 20260406, int)
    resume = _read_value("RESUME", file_data, ("resume",), True, _parse_bool)
    force_rerun = _read_value("FORCE_RERUN", file_data, ("force_rerun",), False, _parse_bool)

    orca_settings = OrcaSettings(
        backend=_read_value("ORCA_BACKEND", file_data, ("orca", "backend"), "mock", str).lower(),
        binary=_read_value("ORCA_BINARY", file_data, ("orca", "binary"), "orca", str),
        method=_read_value(
            "ORCA_METHOD", file_data, ("orca", "method"), "R2SCAN-3C Opt TightSCF", str
        ),
        charge=_read_value("CHARGE", file_data, ("orca", "charge"), 0, int),
        multiplicities=tuple(
            _read_value(
                "MULTIPLICITIES",
                file_data,
                ("orca", "multiplicities"),
                None,
                _parse_int_list,
                allow_none=True,
            )
            or (
                _read_value(
                    "MULTIPLICITY",
                    file_data,
                    ("orca", "multiplicity"),
                    (1,),
                    _parse_int_list,
                )
            )
        ),
        processes=_read_value("ORCA_PROCESSES", file_data, ("orca", "processes"), 2, int),
        maxcore_mb=_read_value("ORCA_MAXCORE_MB", file_data, ("orca", "maxcore_mb"), 1500, int),
        geom_max_iterations=_read_value(
            "ORCA_GEOM_MAX_ITERATIONS", file_data, ("orca", "geom_max_iterations"), 200, int
        ),
        extra_blocks=_read_value(
            "ORCA_EXTRA_BLOCKS", file_data, ("orca", "extra_blocks"), "", str
        ),
        mock_optimal_distance=_read_value(
            "MOCK_OPTIMAL_DISTANCE", file_data, ("mock", "optimal_distance"), 2.45, float
        ),
        mock_optimal_multiplicity=_read_value(
            "MOCK_OPTIMAL_MULTIPLICITY", file_data, ("mock", "optimal_multiplicity"), 1, int
        ),
        mock_base_energy=_read_value(
            "MOCK_BASE_ENERGY", file_data, ("mock", "base_energy"), -24.0, float
        ),
        mock_noise_scale=_read_value(
            "MOCK_NOISE_SCALE", file_data, ("mock", "noise_scale"), 0.0005, float
        ),
    )
    refinement_settings = RefinementSettings(
        enabled=_read_value("REFINE_ENABLED", file_data, ("refinement", "enabled"), True, _parse_bool),
        step=_read_value(
            "REFINE_STEP",
            file_data,
            ("refinement", "step"),
            None,
            _parse_optional_float,
            allow_none=True,
        ),
        points=_read_value("REFINE_POINTS", file_data, ("refinement", "points"), 2, int),
    )
    single_point_settings = SinglePointSettings(
        enabled=_read_value(
            "SINGLE_POINT_ENABLED",
            file_data,
            ("single_point", "enabled"),
            True,
            _parse_bool,
        ),
        top_n=_read_value("SINGLE_POINT_TOP_N", file_data, ("single_point", "top_n"), 3, int),
        method=_read_value(
            "SINGLE_POINT_METHOD",
            file_data,
            ("single_point", "method"),
            "PBE0 D4 def2-TZVP def2/J RIJCOSX TightSCF",
            str,
        ),
        extra_blocks=_read_value(
            "SINGLE_POINT_EXTRA_BLOCKS",
            file_data,
            ("single_point", "extra_blocks"),
            "",
            str,
        ),
    )
    cleanup_settings = CleanupSettings(
        delete_patterns_after_success=tuple(
            _read_value(
                "DELETE_PATTERNS_AFTER_SUCCESS",
                file_data,
                ("cleanup", "delete_patterns_after_success"),
                CleanupSettings.delete_patterns_after_success,
                _parse_string_list,
            )
        ),
        delete_non_best_patterns=tuple(
            _read_value(
                "DELETE_NON_BEST_PATTERNS",
                file_data,
                ("cleanup", "delete_non_best_patterns"),
                (),
                _parse_string_list,
            )
        ),
    )

    if num_atoms <= 0:
        raise ValueError("NUM_ATOMS must be positive.")
    if repeats_per_distance <= 0:
        raise ValueError("REPEATS_PER_DISTANCE must be positive.")
    if not distances:
        raise ValueError("At least one distance must be provided.")
    if not orca_settings.multiplicities:
        raise ValueError("At least one multiplicity must be provided.")
    if single_point_settings.top_n <= 0:
        raise ValueError("SINGLE_POINT_TOP_N must be positive.")

    return CampaignConfig(
        campaign_name=campaign_name,
        results_root=results_root,
        num_atoms=num_atoms,
        element=element,
        geometry_template=geometry_template,
        coordinate_template_file=coordinate_template_file,
        distances=distances,
        repeats_per_distance=repeats_per_distance,
        coordinate_jitter=coordinate_jitter,
        base_seed=base_seed,
        resume=resume,
        force_rerun=force_rerun,
        orca=orca_settings,
        refinement=refinement_settings,
        single_point=single_point_settings,
        cleanup=cleanup_settings,
    )


def _resolve_config_path(config_path: str | Path | None) -> Path | None:
    candidate = config_path or os.getenv(CONFIG_ENV)
    if not candidate:
        return None
    return Path(candidate).expanduser().resolve()


def _load_config_file(config_path: Path) -> dict[str, Any]:
    suffix = config_path.suffix.lower()
    payload = config_path.read_text(encoding="utf-8")
    if suffix in {".toml", ".tml"}:
        return tomllib.loads(payload)
    if suffix == ".json":
        return json.loads(payload)
    raise ValueError(f"Unsupported config format: {config_path.suffix}")


def _read_value(
    env_name: str,
    payload: dict[str, Any],
    path: tuple[str, ...],
    default: Any,
    parser: Callable[[Any], Any],
    *,
    allow_none: bool = False,
) -> Any:
    env_value = os.getenv(env_name)
    if env_value is not None:
        return parser(env_value)

    current: Any = payload
    for key in path:
        if not isinstance(current, dict) or key not in current:
            current = default
            break
        current = current[key]

    if current is None and allow_none:
        return None
    return parser(current)


def _parse_distances(value: Any) -> tuple[float, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(float(item) for item in value)
    return tuple(float(part.strip()) for part in str(value).split(",") if part.strip())


def _parse_int_list(value: Any) -> tuple[int, ...]:
    if isinstance(value, int):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(int(item) for item in value)
    return tuple(int(part.strip()) for part in str(value).split(",") if part.strip())


def _parse_string_list(value: Any) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return tuple(part.strip() for part in str(value).split(",") if part.strip())


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Cannot parse boolean value: {value}")


def _parse_optional_float(value: Any) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    return float(value)


def _resolve_path(raw_value: str | Path, base_dir: Path) -> Path:
    path = Path(raw_value).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _sanitize_name(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return sanitized or "campaign"
