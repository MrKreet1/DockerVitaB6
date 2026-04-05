from __future__ import annotations

import argparse
import sys

from .orchestrator import run_campaign


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an ORCA geometry search campaign.")
    parser.add_argument(
        "--config",
        dest="config_path",
        help="Optional TOML or JSON config file. Environment variables override file values.",
    )
    args = parser.parse_args()
    return run_campaign(args.config_path)


if __name__ == "__main__":
    sys.exit(main())
