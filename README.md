# ORCA Cluster Service

Containerized service for automated ORCA geometry optimization campaigns on a 12-atom boron cluster.

## What it does

- Generates starting coordinates for a B12 cluster from a named template or a custom coordinate file.
- Scans a list of inter-atomic distances.
- Runs repeated ORCA geometry optimizations for each distance.
- Runs each distance across multiple spin multiplicities.
- Stores per-run inputs, outputs, energies, and geometries in isolated folders.
- Writes `summary.csv`, `best.json`, and `best.xyz` incrementally so partial progress is not lost.
- Optionally performs a refinement sweep around the best coarse distance.
- Optionally performs higher-accuracy single-point calculations on the best optimized structures.

## Repository layout

- `src/orca_cluster_service/`: campaign orchestrator and ORCA integration.
- `config/b12.toml`: default campaign configuration.
- `tests/`: mock-backed smoke tests.
- `Dockerfile`: production and mock container targets.
- `railway.json`: Railway deployment settings.

## Configuration

The service accepts a TOML or JSON config file plus environment-variable overrides.

Main environment variables:

- `CAMPAIGN_NAME`
- `RESULTS_ROOT`
- `NUM_ATOMS`
- `ELEMENT`
- `GEOMETRY_TEMPLATE`
- `COORDINATE_TEMPLATE_FILE`
- `DISTANCES`
- `REPEATS_PER_DISTANCE`
- `COORDINATE_JITTER`
- `BASE_RANDOM_SEED`
- `CHARGE`
- `MULTIPLICITIES`
- `MULTIPLICITY` as a legacy single-value fallback
- `ORCA_BACKEND`
- `ORCA_BINARY`
- `ORCA_METHOD`
- `ORCA_PROCESSES`
- `ORCA_MAXCORE_MB`
- `REFINE_ENABLED`
- `REFINE_STEP`
- `REFINE_POINTS`
- `SINGLE_POINT_ENABLED`
- `SINGLE_POINT_TOP_N`
- `SINGLE_POINT_METHOD`
- `DELETE_PATTERNS_AFTER_SUCCESS`
- `DELETE_NON_BEST_PATTERNS`

Example local mock run:

```bash
export ORCA_BACKEND=mock
export RESULTS_ROOT=./data
PYTHONPATH=src python -m orca_cluster_service --config config/b12.toml
```

On Windows PowerShell:

```powershell
$env:ORCA_BACKEND = "mock"
$env:RESULTS_ROOT = ".\\data"
$env:PYTHONPATH = "src"
python -m orca_cluster_service --config config/b12.toml
```

## ORCA distribution

This repository does not ship ORCA binaries.

For a production image, unpack your licensed Linux ORCA distribution into `vendor/orca/` so that `vendor/orca/orca` exists before building the `runtime` target.

Example:

```bash
tar -xzf orca-linux.tar.gz -C vendor/orca
docker build --target runtime -t orca-b12:latest .
```

For CI and development without ORCA, use:

```bash
docker build --target mock-runtime -t orca-b12:mock .
```

## Outputs

Each campaign writes into `RESULTS_ROOT/CAMPAIGN_NAME/`:

- `summary.csv`
- `best.json`
- `best.xyz`
- `campaign_config.json`
- `service.log`
- `runs/<stage>/d_<distance>/r_<repeat>/`

Each run directory contains:

- ORCA input file
- ORCA output file
- `initial.xyz`
- `optimized.xyz` when available
- `result.json`

`summary.csv` also includes:

- `multiplicity`
- `calculation_type` (`optimization` or `single_point`)
- `method`
- `source_run_id` for single-point jobs

## GitHub Actions

Workflow file: `.github/workflows/ci.yml`

The workflow always:

- installs the package
- runs unit tests
- builds the mock container target

On `push` to `main`, it also publishes a runtime image to GHCR if the secret `ORCA_DIST_BASE64` is configured.

Expected secret format:

- Base64-encoded tar archive containing the licensed ORCA Linux distribution unpacked to `vendor/orca/`
- The extracted archive must provide an executable at `vendor/orca/orca`

## Railway deployment

Recommended production path:

1. Push the repository to GitHub.
2. Let GitHub Actions build the licensed `runtime` image and publish it to GHCR.
3. In Railway, create a service from the published image in GHCR, not from the repository Dockerfile.
4. Attach a persistent volume and mount it at `/app/data`.
5. Set service variables such as:
   - `RESULTS_ROOT=/app/data`
   - `CAMPAIGN_NAME=b12-production`
   - `ORCA_BINARY=/opt/orca/orca`
   - `MULTIPLICITIES=1,3,5`
6. Redeploy after changing configuration.

Why this path is preferred:

- the public repository does not contain ORCA binaries
- the `runtime` Docker target requires a licensed ORCA bundle in `vendor/orca/`
- GitHub Actions can restore that bundle from a secret and publish a ready-to-run image
- Railway can then consume the finished image directly without rebuilding ORCA-dependent layers

Repository-based Railway Docker builds are only appropriate if your private source already includes the licensed ORCA payload at build time.

## Storage policy

Cleanup is explicit and configurable.

- `delete_patterns_after_success` removes heavy ORCA scratch/wavefunction files immediately after each successful run.
- `delete_non_best_patterns` applies only after the best result is known and only to non-best runs.
- By default, reproducibility artifacts are kept: `.inp`, `.out`, `initial.xyz`, `optimized.xyz`, `result.json`, `summary.csv`, `best.json`, `best.xyz`.

If you need a more aggressive storage profile on Railway, set for example:

```toml
[cleanup]
delete_patterns_after_success = ["*.tmp", "*.gbw", "*.densities", "*.engrad", "*.hess"]
delete_non_best_patterns = ["optimized.xyz"]
```

The provided `railway.json` keeps the restart policy at `ON_FAILURE`, which allows the service to resume after crashes while avoiding a loop after a successful exit.

## Verification

Run the local test suite with:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```
