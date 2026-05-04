import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "notebooks" / "ismip7_tas_point_subset_full_check.ipynb"
NETCDF_OUTPUT_PATH = ROOT / "output" / "ismip7_tas_point_subset_full.nc"
JSON_OUTPUT_PATH = ROOT / "output" / "ismip7_tas_point_subset_full.json"


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


PRELUDE = """from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import h5py
import numpy as np
import matplotlib.pyplot as plt


ROOT = Path(".")
DEFAULT_NETCDF_PATH = Path(__NETCDF_PATH__)
DEFAULT_JSON_PATH = Path(__JSON_PATH__)
REPO_ROOT = next(
    (
        candidate
        for candidate in [ROOT.resolve(), *ROOT.resolve().parents]
        if (candidate / "output").exists() and (candidate / "configs").exists()
    ),
    DEFAULT_NETCDF_PATH.parents[1],
)
NETCDF_PATH = DEFAULT_NETCDF_PATH if DEFAULT_NETCDF_PATH.exists() else REPO_ROOT / "output" / "ismip7_tas_point_subset_full.nc"
JSON_PATH = DEFAULT_JSON_PATH if DEFAULT_JSON_PATH.exists() else REPO_ROOT / "output" / "ismip7_tas_point_subset_full.json"

plt.rcParams.update(
    {
        "figure.figsize": (10, 5.5),
        "figure.dpi": 130,
        "savefig.dpi": 180,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.facecolor": "#f8f7f4",
        "grid.alpha": 0.3,
        "font.size": 11,
    }
)

print("NETCDF_PATH:", NETCDF_PATH)
print("JSON_PATH:", JSON_PATH)
print("NETCDF exists:", NETCDF_PATH.exists())
print("JSON exists:", JSON_PATH.exists())


def _decode(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, np.ndarray) and value.shape == (1,):
        return _decode(value[0])
    return value


def _parse_time_axis(time_values, time_units):
    if not time_units or not str(time_units).startswith("days since "):
        return np.asarray(time_values), "time"
    base = datetime.fromisoformat(str(time_units).replace("days since ", "").strip())
    dt = np.array([base + timedelta(days=float(day)) for day in time_values], dtype=object)
    return dt, "time"
"""


cells = [
    md_cell(
        """# ISMIP7 Full-Run Sanity Check

This notebook inspects the full point-subset outputs generated from the entire available source directory.

- `output/ismip7_tas_point_subset_full.nc`
- `output/ismip7_tas_point_subset_full.json`
"""
    ),
    code_cell(PRELUDE.replace("__NETCDF_PATH__", repr(str(NETCDF_OUTPUT_PATH))).replace("__JSON_PATH__", repr(str(JSON_OUTPUT_PATH)))),
    code_cell(
        """with h5py.File(NETCDF_PATH, "r") as ds:
    attrs = {k: _decode(v) for k, v in ds.attrs.items()}
    time_values = ds["time"][:]
    time_units = _decode(ds["time"].attrs.get("units"))
    time_axis, time_label = _parse_time_axis(time_values, time_units)
    point_names = [_decode(v) for v in ds["point_name"][:]]
    requested_latitude = ds["requested_latitude"][:]
    requested_longitude = ds["requested_longitude"][:]
    actual_latitude = ds["latitude"][:]
    actual_longitude = ds["longitude"][:]
    tas = ds["tas"][:]
    tas_units = _decode(ds["tas"].attrs.get("units"))

with JSON_PATH.open() as f:
    payload = json.load(f)

print("Points:", point_names)
print("Number of source files:", len(payload["source_files"]))
print("Number of time steps:", len(time_values))
print("Time span:", time_axis[0], "to", time_axis[-1])
print("tas shape:", tas.shape)
print("tas min/max:", float(np.nanmin(tas)), float(np.nanmax(tas)))
print("Duplicate time values:", len(time_values) - len(np.unique(time_values)))
print("Any NaNs:", bool(np.isnan(tas).any()))
"""
    ),
    code_cell(
        """print("Coordinate check:")
for i, name in enumerate(point_names):
    print(f"\\n{name}")
    print(f"  requested lat/lon: {requested_latitude[i]:.6f}, {requested_longitude[i]:.6f}")
    print(f"  matched   lat/lon: {actual_latitude[i]:.6f}, {actual_longitude[i]:.6f}")
"""
    ),
    code_cell(
        """fig, ax = plt.subplots(figsize=(12, 6))
for i, name in enumerate(point_names):
    ax.plot(time_axis, tas[:, i], linewidth=1.5, label=name)
ax.set_title("Full-run point time series")
ax.set_ylabel(f"tas ({tas_units})")
ax.set_xlabel(time_label)
ax.grid(True, axis="y")
ax.legend(frameon=False, ncol=2)
fig.autofmt_xdate()
plt.show()
"""
    ),
    code_cell(
        """annual_means = {}
years = np.array([t.year for t in time_axis])
for i, name in enumerate(point_names):
    year_mean = []
    year_labels = []
    for year in sorted(np.unique(years)):
        mask = years == year
        year_mean.append(np.nanmean(tas[mask, i]))
        year_labels.append(year)
    annual_means[name] = (np.array(year_labels), np.array(year_mean))

fig, ax = plt.subplots(figsize=(12, 6))
for name, (year_labels, year_mean) in annual_means.items():
    ax.plot(year_labels, year_mean, linewidth=1.7, label=name)
ax.set_title("Annual-mean tas by point")
ax.set_ylabel(f"tas ({tas_units})")
ax.set_xlabel("year")
ax.grid(True, axis="y")
ax.legend(frameon=False, ncol=2)
plt.show()
"""
    ),
    code_cell(
        """point_means = np.nanmean(tas, axis=0)
point_stds = np.nanstd(tas, axis=0)

fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), constrained_layout=True)
axes[0].bar(point_names, point_means, color="#3a6ea5")
axes[0].set_title("Mean tas by point")
axes[0].tick_params(axis="x", rotation=25)
axes[0].grid(True, axis="y")

axes[1].bar(point_names, point_stds, color="#d17a22")
axes[1].set_title("Temporal std by point")
axes[1].tick_params(axis="x", rotation=25)
axes[1].grid(True, axis="y")
plt.show()
"""
    ),
    code_cell(
        """preview = {
    "first_source_files": payload["source_files"][:5],
    "last_source_files": payload["source_files"][-5:],
    "first_timestamps": payload["time"]["iso8601"][:5],
    "last_timestamps": payload["time"]["iso8601"][-5:],
}
preview
"""
    ),
]


notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "version": "3.12",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
NOTEBOOK_PATH.write_text(json.dumps(notebook, indent=2) + "\n")
print(f"Wrote {NOTEBOOK_PATH}")
