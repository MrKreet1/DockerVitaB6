from __future__ import annotations

from .models import AtomCoordinate, OrcaSettings


def render_orca_input(
    *,
    run_id: str,
    coordinates: list[AtomCoordinate],
    orca_settings: OrcaSettings,
    method: str,
    multiplicity: int,
    calculation_type: str,
    extra_blocks: str = "",
) -> str:
    lines = [
        f"# Auto-generated input for {run_id}",
        f"! {method.strip()}",
        f"%base \"{run_id}\"",
        f"%maxcore {orca_settings.maxcore_mb}",
        "%pal",
        f"  nprocs {orca_settings.processes}",
        "end",
    ]

    if calculation_type == "optimization":
        lines.extend(
            [
                "%geom",
                f"  MaxIter {orca_settings.geom_max_iterations}",
                "end",
            ]
        )

    merged_extra_blocks = "\n".join(
        block.rstrip()
        for block in (orca_settings.extra_blocks, extra_blocks)
        if block.strip()
    )
    if merged_extra_blocks:
        lines.append(merged_extra_blocks)

    lines.extend(
        [
            f"* xyz {orca_settings.charge} {multiplicity}",
            *(atom.to_xyz_line() for atom in coordinates),
            "*",
            "",
        ]
    )
    return "\n".join(lines)
