from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import re

import h5py
import matplotlib.pyplot as plt
import numpy as np


TIME_UNITS_PATTERN = re.compile(r"^(?P<unit>\w+)\s+since\s+(?P<base>.+)$")


def maybe_decode(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, np.ndarray) and value.shape == (1,):
        return maybe_decode(value[0])
    return value


def parse_time_axis(time_values: np.ndarray, time_units: str | None) -> tuple[np.ndarray, str]:
    if not time_units:
        return time_values, "time"

    match = TIME_UNITS_PATTERN.match(time_units.strip())
    if not match or match.group("unit").lower() != "days":
        return time_values, f"time ({time_units})"

    try:
        base = datetime.fromisoformat(match.group("base").strip())
    except ValueError:
        return time_values, f"time ({time_units})"

    datetimes = np.array([base + timedelta(days=float(value)) for value in time_values], dtype=object)
    return datetimes, "time"


def load_subset(path: Path, variable_name: str | None) -> dict[str, object]:
    with h5py.File(path, "r") as ds:
        if variable_name is None:
            variable_name = next(
                name
                for name in ds.keys()
                if name
                not in {
                    "point",
                    "time",
                    "point_name",
                    "requested_latitude",
                    "requested_longitude",
                    "requested_x",
                    "requested_y",
                    "latitude",
                    "longitude",
                    "x",
                    "y",
                }
                and ds[name].ndim == 2
            )

        point_names = [maybe_decode(value) for value in ds["point_name"][:]]
        time_values = ds["time"][:]
        time_units = maybe_decode(ds["time"].attrs.get("units"))
        values = ds[variable_name][:]
        variable_units = maybe_decode(ds[variable_name].attrs.get("units")) or variable_name

    time_axis, time_label = parse_time_axis(time_values, time_units)
    return {
        "variable_name": variable_name,
        "variable_units": variable_units,
        "point_names": point_names,
        "time_axis": time_axis,
        "time_label": time_label,
        "values": values,
    }


def make_plot(path: Path, output_path: Path, variable_name: str | None) -> None:
    subset = load_subset(path, variable_name)
    values = subset["values"]
    point_names = subset["point_names"]
    time_axis = subset["time_axis"]

    fig, axes = plt.subplots(2, 1, figsize=(12, 8), constrained_layout=True)

    for index, point_name in enumerate(point_names):
        axes[0].plot(time_axis, values[:, index], label=point_name, linewidth=1.8)
    axes[0].set_title(f"{subset['variable_name']} point time series")
    axes[0].set_ylabel(str(subset["variable_units"]))
    axes[0].grid(True, alpha=0.3)
    axes[0].legend()

    point_means = np.nanmean(values, axis=0)
    axes[1].bar(point_names, point_means, color="#3a6ea5")
    axes[1].set_title("Mean value by point")
    axes[1].set_ylabel(str(subset["variable_units"]))
    axes[1].grid(True, axis="y", alpha=0.3)
    axes[1].tick_params(axis="x", rotation=20)
    axes[1].set_xlabel(str(subset["time_label"]))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plot a reduced point-subset NetCDF output.")
    parser.add_argument("input_path", type=Path, help="Reduced NetCDF file written by scripts/run_globus_subset.py")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("figures/point_subset_smoke_test.png"),
        help="Path to the output PNG.",
    )
    parser.add_argument(
        "--variable",
        default=None,
        help="Data variable to plot. If omitted, the script infers the first non-coordinate variable.",
    )
    return parser


def main() -> int:
    parser = make_parser()
    args = parser.parse_args()
    make_plot(args.input_path, args.output, args.variable)
    print(f"Wrote plot: {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
