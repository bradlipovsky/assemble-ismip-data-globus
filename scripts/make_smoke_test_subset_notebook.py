import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "notebooks" / "ismip7_tas_point_subset_smoke_check.ipynb"
NETCDF_OUTPUT_PATH = ROOT / "output" / "ismip7_tas_point_subset_smoke.nc"
JSON_OUTPUT_PATH = ROOT / "output" / "ismip7_tas_point_subset_smoke.json"


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
NETCDF_PATH = DEFAULT_NETCDF_PATH if DEFAULT_NETCDF_PATH.exists() else REPO_ROOT / "output" / "ismip7_tas_point_subset_smoke.nc"
JSON_PATH = DEFAULT_JSON_PATH if DEFAULT_JSON_PATH.exists() else REPO_ROOT / "output" / "ismip7_tas_point_subset_smoke.json"
FIG_DIR = REPO_ROOT / "figures"
FIG_DIR.mkdir(exist_ok=True)


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
        """# ISMIP7 Smoke-Test Sanity Check

This notebook inspects the reduced outputs from the 2-file smoke test:

- `output/ismip7_tas_point_subset_smoke.nc`
- `output/ismip7_tas_point_subset_smoke.json`

The goal is to do a quick sanity check before scaling beyond the 2015-2016 subset.
"""
    ),
    code_cell(PRELUDE.replace("__NETCDF_PATH__", repr(str(NETCDF_OUTPUT_PATH))).replace("__JSON_PATH__", repr(str(JSON_OUTPUT_PATH)))),
    code_cell(
        """with h5py.File(NETCDF_PATH, "r") as ds:
    dataset_summary = {name: {"shape": ds[name].shape, "dtype": str(ds[name].dtype)} for name in ds.keys()}
    attrs = {k: _decode(v) for k, v in ds.attrs.items()}
    time_values = ds["time"][:]
    time_units = _decode(ds["time"].attrs.get("units"))
    time_axis, time_label = _parse_time_axis(time_values, time_units)
    point_names = [_decode(v) for v in ds["point_name"][:]]
    requested_latitude = ds["requested_latitude"][:]
    requested_longitude = ds["requested_longitude"][:]
    actual_latitude = ds["latitude"][:]
    actual_longitude = ds["longitude"][:]
    actual_x = ds["x"][:]
    actual_y = ds["y"][:]
    tas = ds["tas"][:]
    tas_units = _decode(ds["tas"].attrs.get("units"))
    tas_attrs = {k: _decode(v) for k, v in ds["tas"].attrs.items()}

with JSON_PATH.open() as f:
    payload = json.load(f)

print("Dataset summary:")
for name, summary in dataset_summary.items():
    print(f"  {name}: shape={summary['shape']}, dtype={summary['dtype']}")

print()
print("Global attrs:")
for key in ["title", "institution", "source", "comment", "subset_method", "subset_source_files"]:
    if key in attrs:
        print(f"  {key}: {attrs[key]}")

print()
print("Variable attrs:")
for key, value in tas_attrs.items():
    print(f"  {key}: {value}")

print()
print("JSON keys:", list(payload.keys()))
print("Point names:", point_names)
print("Number of time steps:", len(time_values))
print("Time span:", time_axis[0], "to", time_axis[-1])
"""
    ),
    md_cell(
        """## Requested vs matched coordinates

The smoke test is nearest-neighbour extraction on the projected grid, so the matched grid cell should be close to the requested geographic target.
"""
    ),
    code_cell(
        """print("Point coordinate check:")
for i, name in enumerate(point_names):
    print(f"\\n{name}")
    print(f"  requested lat/lon: {requested_latitude[i]:.6f}, {requested_longitude[i]:.6f}")
    print(f"  matched   lat/lon: {actual_latitude[i]:.6f}, {actual_longitude[i]:.6f}")
    print(f"  matched   x/y (m): {actual_x[i]:.1f}, {actual_y[i]:.1f}")
"""
    ),
    code_cell(
        """duplicate_count = len(time_values) - len(np.unique(time_values))
print("Duplicate time values:", duplicate_count)
print("Any NaNs in tas:", bool(np.isnan(tas).any()))
print("tas shape:", tas.shape)
print("tas min/max:", float(np.nanmin(tas)), float(np.nanmax(tas)))
"""
    ),
    code_cell(
        """fig, ax = plt.subplots(figsize=(11, 5.5))

for i, name in enumerate(point_names):
    ax.plot(time_axis, tas[:, i], marker="o", linewidth=1.8, label=name)

ax.set_title("Smoke-test point time series")
ax.set_ylabel(f"tas ({tas_units})")
ax.set_xlabel(time_label)
ax.grid(True, axis="y")
ax.legend(frameon=False)
fig.autofmt_xdate()
plt.show()
"""
    ),
    code_cell(
        """point_means = np.nanmean(tas, axis=0)
point_stds = np.nanstd(tas, axis=0)

fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), constrained_layout=True)

axes[0].bar(point_names, point_means, color="#3a6ea5")
axes[0].set_title("Mean tas by point")
axes[0].set_ylabel(f"tas ({tas_units})")
axes[0].tick_params(axis="x", rotation=20)
axes[0].grid(True, axis="y")

axes[1].bar(point_names, point_stds, color="#d17a22")
axes[1].set_title("Temporal std by point")
axes[1].set_ylabel(f"tas ({tas_units})")
axes[1].tick_params(axis="x", rotation=20)
axes[1].grid(True, axis="y")

plt.show()
"""
    ),
    code_cell(
        """for i, name in enumerate(point_names):
    print(name)
    print("  first 6 values:", np.round(tas[:6, i], 3).tolist())
    print("  last  6 values:", np.round(tas[-6:, i], 3).tolist())
"""
    ),
    code_cell(
        """fig, ax = plt.subplots(figsize=(11, 4.5))
delta = tas[:, 1] - tas[:, 0]
ax.plot(time_axis, delta, color="#6c3f8d", marker="o", linewidth=1.8)
ax.axhline(0.0, color="black", linewidth=1.0, linestyle="--")
ax.set_title("Difference between site_2 and site_1")
ax.set_ylabel(f"tas difference ({tas_units})")
ax.set_xlabel(time_label)
ax.grid(True, axis="y")
fig.autofmt_xdate()
plt.show()
"""
    ),
    code_cell(
        """preview = {
    "source_files": payload["source_files"],
    "time_units": payload["time"]["attrs"].get("units"),
    "first_iso8601": payload["time"]["iso8601"][:3],
    "point_1_preview": {
        "name": payload["points"][0]["name"],
        "requested": payload["points"][0]["requested"],
        "actual": payload["points"][0]["actual"],
        "first_values": payload["points"][0]["values"][:5],
    },
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
