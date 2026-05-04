from __future__ import annotations

import argparse
import json
from pathlib import Path


def md_cell(source: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source.splitlines(keepends=True),
    }


def code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def notebook(cells: list[dict]) -> dict:
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.12"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def build_coverage_notebook() -> dict:
    cells = [
        md_cell(
            "# ISMIP6 Coverage Summary\n\n"
            "Summarizes the Globus inventory, processing status, variable availability, failures, "
            "and basic output-file sanity checks for the ISMIP6 point-subset workflow."
        ),
        code_cell(
            """from __future__ import annotations

from collections import Counter
from pathlib import Path
import json

import matplotlib.pyplot as plt
import netCDF4
import numpy as np
import pandas as pd


RHO_ICE_KG_M3 = 917.0
SECONDS_PER_YEAR = 365.25 * 24 * 60 * 60

ROOT = Path.cwd()
if not (ROOT / "output" / "ismip6").exists():
    ROOT = ROOT.parent

OUTPUT_ROOT = ROOT / "output" / "ismip6"
SUMMARY_PATH = OUTPUT_ROOT / "ismip6_processing_summary.json"
INVENTORY_PATH = OUTPUT_ROOT / "ismip6_inventory.json"

assert SUMMARY_PATH.exists(), f"Missing summary: {SUMMARY_PATH}"
assert INVENTORY_PATH.exists(), f"Missing inventory: {INVENTORY_PATH}"

with SUMMARY_PATH.open() as f:
    summary = json.load(f)
with INVENTORY_PATH.open() as f:
    inventory = json.load(f)

records = pd.DataFrame(summary["records"])
runs = pd.DataFrame(inventory["runs"])

usable_statuses = {"processed", "skipped_existing"}
records["output_exists"] = records["output_netcdf"].apply(lambda p: Path(p).exists() if isinstance(p, str) else False)
records["usable_output"] = records["status"].isin(usable_statuses) & records["output_exists"]

print("Inventory runs:", inventory["run_count"])
print("Processing records:", summary["record_count"])
print("Created at:", summary["created_at"])
print("Status counts:")
print(records["status"].value_counts().to_string())
"""
        ),
        code_cell(
            """availability = []
for run in inventory["runs"]:
    for variable in inventory["variables"]:
        file_info = run["files"].get(variable)
        availability.append(
            {
                "group": run["group"],
                "model": run["model"],
                "experiment": run["experiment"],
                "standard_variable": variable,
                "available_in_inventory": file_info is not None,
                "remote_variable_hint": None if file_info is None else file_info.get("remote_variable_hint"),
                "filename": None if file_info is None else file_info.get("filename"),
            }
        )

availability = pd.DataFrame(availability)
availability_by_variable = (
    availability.groupby("standard_variable")["available_in_inventory"]
    .agg(["sum", "count"])
    .rename(columns={"sum": "available_files", "count": "runs"})
)
availability_by_variable["missing_files"] = availability_by_variable["runs"] - availability_by_variable["available_files"]
availability_by_variable
"""
        ),
        code_cell(
            """status_by_variable = (
    records.pivot_table(
        index="standard_variable",
        columns="status",
        values="group",
        aggfunc="count",
        fill_value=0,
    )
    .sort_index()
)
status_by_variable
"""
        ),
        code_cell(
            """fig, axes = plt.subplots(1, 2, figsize=(14, 4.8))

availability_by_variable[["available_files", "missing_files"]].plot.bar(
    stacked=True,
    ax=axes[0],
    color=["#2f7f5f", "#c86b4a"],
)
axes[0].set_title("Inventory availability by variable")
axes[0].set_xlabel("")
axes[0].set_ylabel("run-variable files")
axes[0].legend(frameon=False)

status_by_variable.plot.bar(stacked=True, ax=axes[1])
axes[1].set_title("Processing status by variable")
axes[1].set_xlabel("")
axes[1].set_ylabel("records")
axes[1].legend(frameon=False, bbox_to_anchor=(1.02, 1), loc="upper left")

for ax in axes:
    ax.grid(True, axis="y", alpha=0.25)
    ax.tick_params(axis="x", rotation=25)

plt.tight_layout()
plt.show()
"""
        ),
        code_cell(
            """run_keys = ["group", "model", "experiment"]
complete = (
    records[records["usable_output"]]
    .groupby(run_keys)["standard_variable"]
    .nunique()
    .reset_index(name="usable_variable_count")
)
complete["complete_four_variable_run"] = complete["usable_variable_count"] == 4

coverage_by_group_model = (
    records.assign(usable=records["usable_output"].astype(int))
    .pivot_table(
        index=["group", "model"],
        columns="standard_variable",
        values="usable",
        aggfunc="sum",
        fill_value=0,
    )
)
coverage_by_group_model["complete_runs"] = complete[complete["complete_four_variable_run"]].groupby(["group", "model"]).size()
coverage_by_group_model["complete_runs"] = coverage_by_group_model["complete_runs"].fillna(0).astype(int)
coverage_by_group_model.sort_index()
"""
        ),
        code_cell(
            """failed = records[records["status"].eq("failed")].copy()
if failed.empty:
    print("No failed records.")
else:
    display(
        failed[
            [
                "group",
                "model",
                "experiment",
                "standard_variable",
                "remote_variable",
                "remote_path",
                "message",
            ]
        ].sort_values(["group", "model", "experiment", "standard_variable"])
    )
"""
        ),
        code_cell(
            """missing = records[records["status"].eq("skipped_missing")].copy()
print("Skipped missing records:", len(missing))
if not missing.empty:
    display(
        missing.pivot_table(
            index=["group", "model"],
            columns="standard_variable",
            values="experiment",
            aggfunc="count",
            fill_value=0,
        )
    )
"""
        ),
        code_cell(
            """bad_outputs = records[records["status"].isin(usable_statuses) & ~records["output_exists"]]
assert bad_outputs.empty, "Some processed/skipped-existing records point to missing output files"

sample_path = Path(records.loc[records["usable_output"], "output_netcdf"].iloc[0])
with netCDF4.Dataset(sample_path) as ds:
    print("Sample file:", sample_path)
    print("dimensions:", {name: len(dim) for name, dim in ds.dimensions.items()})
    print("variables:", list(ds.variables))
    print("point names:", [str(v) for v in ds.variables["point_name"][:]])
    print("time units:", getattr(ds.variables["time"], "units", None))
    for name in ds.variables:
        if name not in {"time", "point", "point_name", "requested_latitude", "requested_longitude", "requested_x", "requested_y", "latitude", "longitude", "x", "y"}:
            print("data variable:", name, ds.variables[name].shape, getattr(ds.variables[name], "units", None))
            break
"""
        ),
    ]
    return notebook(cells)


def build_timeseries_notebook() -> dict:
    cells = [
        md_cell(
            "# ISMIP6 Example Time Series\n\n"
            "Loads one complete four-variable ISMIP6 run and plots the selected point time series. "
            "The notebook prefers `AWI/PISM1/exp01` when available, then falls back to the first complete run. "
            "Basal mass balance is also shown as a positive melt rate in `m ice yr-1`."
        ),
        code_cell(
            """from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json

import matplotlib.pyplot as plt
import netCDF4
import numpy as np
import pandas as pd


RHO_ICE_KG_M3 = 917.0
SECONDS_PER_YEAR = 365.25 * 24 * 60 * 60

ROOT = Path.cwd()
if not (ROOT / "output" / "ismip6").exists():
    ROOT = ROOT.parent

OUTPUT_ROOT = ROOT / "output" / "ismip6"
SUMMARY_PATH = OUTPUT_ROOT / "ismip6_processing_summary.json"

with SUMMARY_PATH.open() as f:
    summary = json.load(f)

records = pd.DataFrame(summary["records"])
usable = records[
    records["status"].isin({"processed", "skipped_existing"})
    & records["output_netcdf"].apply(lambda p: Path(p).exists() if isinstance(p, str) else False)
].copy()

required_variables = [
    "ice_thickness",
    "basal_melt_rate",
    "surface_mass_balance",
    "surface_temperature",
]

def choose_run() -> tuple[str, str, str]:
    for preferred in [("AWI", "PISM1", "exp01")]:
        subset = usable[
            (usable["group"] == preferred[0])
            & (usable["model"] == preferred[1])
            & (usable["experiment"] == preferred[2])
        ]
        if set(subset["standard_variable"]) >= set(required_variables):
            return preferred

    counts = (
        usable[usable["standard_variable"].isin(required_variables)]
        .groupby(["group", "model", "experiment"])["standard_variable"]
        .nunique()
        .reset_index(name="n_variables")
        .sort_values(["n_variables", "group", "model", "experiment"], ascending=[False, True, True, True])
    )
    complete = counts[counts["n_variables"] == len(required_variables)]
    assert not complete.empty, "No complete four-variable run found"
    row = complete.iloc[0]
    return str(row["group"]), str(row["model"]), str(row["experiment"])

GROUP, MODEL, EXPERIMENT = choose_run()
print("Selected run:", GROUP, MODEL, EXPERIMENT)
"""
        ),
        code_cell(
            """def decode(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def decimal_year(value, calendar="standard"):
    year = float(value.year)
    dayofyr = float(getattr(value, "dayofyr", 1))
    days_in_year = 360.0 if calendar == "360_day" else 365.0
    return year + (dayofyr - 1.0) / days_in_year


def parse_time(values, units, calendar="standard"):
    try:
        dates = netCDF4.num2date(values, units=units, calendar=calendar, only_use_cftime_datetimes=False)
    except Exception:
        return np.asarray(values), "time"

    converted = []
    for value in dates:
        if isinstance(value, datetime):
            converted.append(value)
            continue
        try:
            converted.append(
                datetime(
                    int(value.year),
                    int(value.month),
                    int(value.day),
                    int(getattr(value, "hour", 0)),
                    int(getattr(value, "minute", 0)),
                    int(getattr(value, "second", 0)),
                )
            )
        except Exception:
            return np.asarray([decimal_year(value, calendar=calendar) for value in dates], dtype=float), "model year"
    return np.asarray(converted, dtype=object), "time"


def flux_kg_m2_s_to_m_ice_yr(values):
    return np.asarray(values, dtype=float) * SECONDS_PER_YEAR / RHO_ICE_KG_M3


def plot_values(standard_variable, ds):
    data = ds["data"]
    units = ds["units"]
    ylabel = units
    title_suffix = ""

    if standard_variable == "basal_melt_rate" and "kg" in units and "s-1" in units:
        # ISMIP6 libmassbffl is positive for ice gain. Plot positive values as melt.
        data = -flux_kg_m2_s_to_m_ice_yr(data)
        ylabel = "basal melt rate (m ice / yr; positive = melt)"
        title_suffix = " converted from signed libmassbffl"
    elif standard_variable == "surface_mass_balance" and "kg" in units and "s-1" in units:
        data = flux_kg_m2_s_to_m_ice_yr(data)
        ylabel = "SMB (m ice / yr)"
        title_suffix = " converted from mass flux"

    return data, ylabel, title_suffix


def series_stats(series):
    finite = np.asarray(series, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return np.nan, np.nan, np.nan
    return float(np.min(finite)), float(np.mean(finite)), float(np.max(finite))


def read_subset(path: Path) -> dict:
    with netCDF4.Dataset(path) as ds:
        coord_names = {
            "time",
            "point",
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
        variable_name = next(name for name in ds.variables if name not in coord_names and len(ds.variables[name].dimensions) >= 2)
        variable = ds.variables[variable_name]
        time_var = ds.variables["time"]
        time, time_label = parse_time(time_var[:], getattr(time_var, "units", ""), getattr(time_var, "calendar", "standard"))
        return {
            "path": path,
            "variable_name": variable_name,
            "data": np.ma.filled(variable[:], np.nan).astype(float),
            "units": getattr(variable, "units", variable_name),
            "long_name": getattr(variable, "long_name", variable_name),
            "time": time,
            "time_label": time_label,
            "time_units": getattr(time_var, "units", ""),
            "point_names": [decode(v) for v in ds.variables["point_name"][:]],
            "requested_latitude": np.asarray(ds.variables["requested_latitude"][:], dtype=float),
            "requested_longitude": np.asarray(ds.variables["requested_longitude"][:], dtype=float),
            "actual_latitude": np.asarray(ds.variables["latitude"][:], dtype=float),
            "actual_longitude": np.asarray(ds.variables["longitude"][:], dtype=float),
            "attrs": {name: getattr(variable, name) for name in variable.ncattrs()},
        }


selected = usable[
    (usable["group"] == GROUP)
    & (usable["model"] == MODEL)
    & (usable["experiment"] == EXPERIMENT)
    & (usable["standard_variable"].isin(required_variables))
].set_index("standard_variable")

datasets = {name: read_subset(Path(selected.loc[name, "output_netcdf"])) for name in required_variables}
pd.DataFrame(
    [
        {
            "standard_variable": name,
            "netcdf_variable": ds["variable_name"],
            "shape": ds["data"].shape,
            "units": ds["units"],
            "time_start": ds["time"][0],
            "time_end": ds["time"][-1],
            "file": str(ds["path"].relative_to(ROOT)),
        }
        for name, ds in datasets.items()
    ]
)
"""
        ),
        code_cell(
            """first = next(iter(datasets.values()))
coord_check = pd.DataFrame(
    {
        "point": first["point_names"],
        "requested_latitude": first["requested_latitude"],
        "requested_longitude": first["requested_longitude"],
        "matched_latitude": first["actual_latitude"],
        "matched_longitude": first["actual_longitude"],
    }
)
coord_check["abs_latitude_delta"] = (coord_check["requested_latitude"] - coord_check["matched_latitude"]).abs()
coord_check["abs_longitude_delta"] = (coord_check["requested_longitude"] - coord_check["matched_longitude"]).abs()
coord_check
"""
        ),
        code_cell(
            """plt.rcParams.update(
    {
        "figure.figsize": (12, 5.2),
        "figure.dpi": 130,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "grid.alpha": 0.25,
        "font.size": 11,
    }
)

for standard_variable, ds in datasets.items():
    data, ylabel, title_suffix = plot_values(standard_variable, ds)
    fig, ax = plt.subplots()
    for point_index, point_name in enumerate(ds["point_names"]):
        ax.plot(ds["time"], data[:, point_index], linewidth=1.4, label=point_name)
    ax.set_title(f"{GROUP}/{MODEL}/{EXPERIMENT}: {standard_variable}{title_suffix}")
    ax.set_xlabel(ds["time_label"])
    ax.set_ylabel(ylabel)
    ax.grid(True, axis="y")
    ax.legend(frameon=False, ncol=2)
    fig.autofmt_xdate()
    plt.show()
"""
        ),
        code_cell(
            """stats = []
for standard_variable, ds in datasets.items():
    data, display_units, _ = plot_values(standard_variable, ds)
    for point_index, point_name in enumerate(ds["point_names"]):
        series = data[:, point_index]
        series_min, series_mean, series_max = series_stats(series)
        stats.append(
            {
                "standard_variable": standard_variable,
                "point": point_name,
                "display_units": display_units,
                "source_units": ds["units"],
                "min": series_min,
                "mean": series_mean,
                "max": series_max,
                "nan_count": int(np.isnan(series).sum()),
            }
        )
pd.DataFrame(stats)
"""
        ),
        code_cell(
            """def summarize_run(group, model, experiment, point_name="Thwaites"):
    subset = usable[
        (usable["group"] == group)
        & (usable["model"] == model)
        & (usable["experiment"] == experiment)
        & (usable["standard_variable"].isin(required_variables))
    ].set_index("standard_variable")
    if set(subset.index) < set(required_variables):
        return pd.DataFrame()

    rows = []
    for standard_variable in required_variables:
        ds = read_subset(Path(subset.loc[standard_variable, "output_netcdf"]))
        try:
            point_index = ds["point_names"].index(point_name)
        except ValueError:
            point_index = 0
            point_name = ds["point_names"][point_index]

        raw = ds["data"][:, point_index]
        display, display_units, _ = plot_values(standard_variable, ds)
        series = display[:, point_index]
        source_min, source_mean, source_max = series_stats(raw)
        display_min, display_mean, display_max = series_stats(series)
        rows.append(
            {
                "group": group,
                "model": model,
                "experiment": experiment,
                "point": point_name,
                "standard_variable": standard_variable,
                "source_units": ds["units"],
                "display_units": display_units,
                "source_min": source_min,
                "source_mean": source_mean,
                "source_max": source_max,
                "display_min": display_min,
                "display_mean": display_mean,
                "display_max": display_max,
            }
        )
    return pd.DataFrame(rows)


comparison = pd.concat(
    [
        summarize_run("AWI", "PISM1", "exp01"),
        summarize_run("PIK", "PISM1", "ctrl_proj_open"),
    ],
    ignore_index=True,
)
comparison
"""
        ),
    ]
    return notebook(cells)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate ISMIP6 exploratory notebooks.")
    parser.add_argument("--output-dir", type=Path, default=Path("notebooks"))
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        args.output_dir / "ismip6_coverage_summary.ipynb": build_coverage_notebook(),
        args.output_dir / "ismip6_example_timeseries.ipynb": build_timeseries_notebook(),
    }
    for path, nb in outputs.items():
        path.write_text(json.dumps(nb, indent=2) + "\n")
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
