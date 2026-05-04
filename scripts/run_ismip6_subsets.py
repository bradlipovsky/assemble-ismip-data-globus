from __future__ import annotations

import argparse
from pathlib import Path

from subsetting.ismip6 import run_ismip6_subsets


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "globus" / "ismip6_subset_config.local.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Subset ISMIP6 Antarctic projection outputs at configured points.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--inventory-only", action="store_true", help="Write inventory metadata without transfers.")
    parser.add_argument("--smoke", action="store_true", help="Run only a representative one-run smoke test.")
    parser.add_argument("--max-runs", type=int, default=None, help="Limit the number of runs processed after inventory.")
    parser.add_argument("--group", default=None, help="Process only this ISMIP6 group.")
    parser.add_argument("--model", default=None, help="Process only this ISMIP6 model.")
    parser.add_argument("--experiment", default=None, help="Process only this ISMIP6 experiment.")
    parser.add_argument(
        "--summary-name",
        default=None,
        help="Write processing summary under this basename instead of replacing the default summary.",
    )
    args = parser.parse_args()

    run_ismip6_subsets(
        args.config,
        smoke=args.smoke,
        max_runs=args.max_runs,
        inventory_only=args.inventory_only,
        group=args.group,
        model=args.model,
        experiment=args.experiment,
        summary_name=args.summary_name,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
