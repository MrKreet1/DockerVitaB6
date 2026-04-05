from __future__ import annotations

import os
import random
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from pathlib import Path

from .geometry import write_xyz
from .models import AtomCoordinate, OrcaSettings, RunDefinition, RunExecution
from .orca_input import render_orca_input
from .orca_parser import parse_orca_output, tail_text


class BaseRunner(ABC):
    def __init__(self, orca_settings: OrcaSettings) -> None:
        self.orca_settings = orca_settings

    @abstractmethod
    def run(self, run_definition: RunDefinition, coordinates: list[AtomCoordinate]) -> RunExecution:
        raise NotImplementedError


class RealOrcaRunner(BaseRunner):
    def run(self, run_definition: RunDefinition, coordinates: list[AtomCoordinate]) -> RunExecution:
        run_dir = run_definition.run_dir
        input_path = run_dir / f"{run_definition.run_id}.inp"
        output_path = run_dir / f"{run_definition.run_id}.out"
        initial_xyz_path = run_dir / "initial.xyz"
        optimized_xyz_path = run_dir / "optimized.xyz"

        input_path.write_text(
            render_orca_input(
                run_id=run_definition.run_id,
                coordinates=coordinates,
                orca_settings=self.orca_settings,
                method=run_definition.method,
                multiplicity=run_definition.multiplicity,
                calculation_type=run_definition.calculation_type,
                extra_blocks=run_definition.extra_blocks,
            ),
            encoding="utf-8",
        )
        write_xyz(initial_xyz_path, coordinates, f"Initial geometry for {run_definition.run_id}")

        started_at = time.perf_counter()
        try:
            binary = self._resolve_binary()
            environment = os.environ.copy()
            environment.setdefault("OMP_NUM_THREADS", "1")
            environment.setdefault("MKL_NUM_THREADS", "1")
            with output_path.open("w", encoding="utf-8") as output_handle:
                process = subprocess.run(
                    [binary, input_path.name],
                    cwd=run_dir,
                    stdout=output_handle,
                    stderr=subprocess.STDOUT,
                    check=False,
                    env=environment,
                )
            exit_code = process.returncode
        except FileNotFoundError as exc:
            exit_code = 127
            output_path.write_text(str(exc) + "\n", encoding="utf-8")

        runtime_seconds = time.perf_counter() - started_at
        parse_result = parse_orca_output(output_path)
        optimized_xyz: Path | None = None
        if parse_result.geometry:
            write_xyz(optimized_xyz_path, parse_result.geometry, f"Optimized geometry for {run_definition.run_id}")
            optimized_xyz = optimized_xyz_path

        error: str | None = None
        status = "success"
        if exit_code != 0:
            status = "failed"
            error = f"ORCA exited with code {exit_code}.\n{tail_text(output_path)}"
        elif not parse_result.terminated_normally:
            status = "failed"
            error = f"ORCA did not terminate normally.\n{tail_text(output_path)}"
        elif parse_result.energy_hartree is None:
            status = "failed"
            error = f"Final energy was not found in the output.\n{tail_text(output_path)}"

        return RunExecution(
            status=status,
            exit_code=exit_code,
            energy_hartree=parse_result.energy_hartree,
            error=error,
            output_path=output_path,
            input_path=input_path,
            initial_xyz_path=initial_xyz_path,
            optimized_xyz_path=optimized_xyz,
            terminated_normally=parse_result.terminated_normally,
            runtime_seconds=runtime_seconds,
        )

    def _resolve_binary(self) -> str:
        binary = self.orca_settings.binary
        if Path(binary).is_absolute():
            return binary
        resolved = shutil.which(binary)
        if not resolved:
            raise FileNotFoundError(
                f"ORCA binary '{binary}' was not found. Set ORCA_BINARY or use ORCA_BACKEND=mock for CI."
            )
        return resolved


class MockOrcaRunner(BaseRunner):
    def run(self, run_definition: RunDefinition, coordinates: list[AtomCoordinate]) -> RunExecution:
        run_dir = run_definition.run_dir
        input_path = run_dir / f"{run_definition.run_id}.inp"
        output_path = run_dir / f"{run_definition.run_id}.out"
        initial_xyz_path = run_dir / "initial.xyz"
        optimized_xyz_path = run_dir / "optimized.xyz"

        input_path.write_text(
            render_orca_input(
                run_id=run_definition.run_id,
                coordinates=coordinates,
                orca_settings=self.orca_settings,
                method=run_definition.method,
                multiplicity=run_definition.multiplicity,
                calculation_type=run_definition.calculation_type,
                extra_blocks=run_definition.extra_blocks,
            ),
            encoding="utf-8",
        )
        write_xyz(initial_xyz_path, coordinates, f"Initial geometry for {run_definition.run_id}")

        started_at = time.perf_counter()
        energy = self._mock_energy(run_definition)
        optimized_geometry = self._mock_relax_geometry(coordinates)
        output_path.write_text(
            self._render_mock_output(optimized_geometry, energy),
            encoding="utf-8",
        )
        write_xyz(optimized_xyz_path, optimized_geometry, f"Optimized geometry for {run_definition.run_id}")
        runtime_seconds = time.perf_counter() - started_at

        parse_result = parse_orca_output(output_path)
        return RunExecution(
            status="success",
            exit_code=0,
            energy_hartree=parse_result.energy_hartree,
            error=None,
            output_path=output_path,
            input_path=input_path,
            initial_xyz_path=initial_xyz_path,
            optimized_xyz_path=optimized_xyz_path,
            terminated_normally=parse_result.terminated_normally,
            runtime_seconds=runtime_seconds,
        )

    def _mock_energy(self, run_definition: RunDefinition) -> float:
        rng = random.Random(run_definition.seed)
        deviation = run_definition.distance - self.orca_settings.mock_optimal_distance
        multiplicity_penalty = 0.02 * abs(
            run_definition.multiplicity - self.orca_settings.mock_optimal_multiplicity
        )
        if run_definition.calculation_type == "single_point":
            stage_penalty = -0.012
        else:
            stage_penalty = 0.005 if run_definition.stage == "coarse" else 0.002
        noise_scale = self.orca_settings.mock_noise_scale
        return (
            self.orca_settings.mock_base_energy
            + 0.45 * deviation * deviation
            + multiplicity_penalty
            + stage_penalty
            + rng.uniform(-noise_scale, noise_scale)
        )

    def _mock_relax_geometry(self, coordinates: list[AtomCoordinate]) -> list[AtomCoordinate]:
        relaxed: list[AtomCoordinate] = []
        for atom in coordinates:
            relaxed.append(
                AtomCoordinate(
                    element=atom.element,
                    x=atom.x * 0.985,
                    y=atom.y * 0.985,
                    z=atom.z * 0.985,
                )
            )
        return relaxed

    def _render_mock_output(
        self, coordinates: list[AtomCoordinate], energy_hartree: float
    ) -> str:
        lines = [
            "-----------------------------",
            "CARTESIAN COORDINATES (ANGSTROEM)",
            "-----------------------------",
            *(atom.to_xyz_line() for atom in coordinates),
            "",
            f"FINAL SINGLE POINT ENERGY     {energy_hartree: .12f}",
            "",
            "****ORCA TERMINATED NORMALLY****",
            "",
        ]
        return "\n".join(lines)


def build_runner(orca_settings: OrcaSettings) -> BaseRunner:
    if orca_settings.backend == "mock":
        return MockOrcaRunner(orca_settings)
    if orca_settings.backend == "real":
        return RealOrcaRunner(orca_settings)
    raise ValueError(f"Unsupported ORCA_BACKEND '{orca_settings.backend}'.")
