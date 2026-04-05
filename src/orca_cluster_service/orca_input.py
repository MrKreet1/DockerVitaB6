from __future__ import annotations

from .models import AtomCoordinate, OrcaSettings


def render_orca_input(
    *,
    run_id: str,
    coordinates: list[AtomCoordinate],
    orca_settings: OrcaSettings,
) -> str:
    lines = [
        f"# Auto-generated input for {run_id}",
        f"! {orca_settings.method.strip()}",
        f"%base \"{run_id}\"",
        f"%maxcore {orca_settings.maxcore_mb}",
        "%pal",
        f"  nprocs {orca_settings.processes}",
        "end",
        "%geom",
        f"  MaxIter {orca_settings.geom_max_iterations}",
        "end",
    ]

    if orca_settings.extra_blocks.strip():
        lines.append(orca_settings.extra_blocks.rstrip())

    lines.extend(
        [
            f"* xyz {orca_settings.charge} {orca_settings.multiplicity}",
            *(atom.to_xyz_line() for atom in coordinates),
            "*",
            "",
        ]
    )
    return "\n".join(lines)
