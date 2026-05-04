from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import globus_sdk
import netCDF4
import numpy as np

from subsetting.pipeline import (
    LON_LAT_TO_X_Y,
    X_Y_TO_LON_LAT,
    build_transfer_client,
    decode_attr,
    load_config,
    maybe_add_data_access_consents,
    nearest_index,
    normalize_longitude,
    parse_points,
    resolve_collection_id,
    resolve_path,
    submit_transfer_and_wait,
    write_output_json,
    write_output_netcdf,
)


DEFAULT_VARIABLES = {
    "ice_thickness": ["lithk"],
    "basal_melt_rate": ["libmassbffl"],
    "surface_mass_balance": ["acabf"],
    "surface_temperature": ["litemptop"],
    "vertical_velocity_surface": ["zvelsurf"],
    "vertical_velocity_base": ["zvelbase"],
    "ice_thickness_tendency": ["dlithkdt"],
    "horizontal_velocity_surface_x": ["xvelsurf"],
    "horizontal_velocity_surface_y": ["yvelsurf"],
    "horizontal_velocity_base_x": ["xvelbase"],
    "horizontal_velocity_base_y": ["yvelbase"],
}
COORDINATE_NAMES = {"time", "x", "y", "lat", "lon", "time_bnds", "bnds", "mapping"}
FILENAME_RE = re.compile(r"^(?P<prefix>.+?)_AIS_(?P<group>.+?)_(?P<model>.+?)_(?P<experiment>.+?)\.nc$")


class MissingDataVariableError(KeyError):
    """Raised when a file exists but does not contain the advertised ISMIP6 data variable."""


@dataclass(frozen=True)
class Ismip6Run:
    group: str
    model: str
    experiment: str
    path: str
    files: dict[str, dict[str, Any]]
    subdirs: list[str]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")


def list_dir(transfer_client: globus_sdk.TransferClient, collection_id: str, path: str) -> list[dict[str, Any]]:
    return list(transfer_client.operation_ls(collection_id, path=path))


def filename_prefix(filename: str) -> str:
    match = FILENAME_RE.match(filename)
    if match:
        return match.group("prefix")
    return filename.removesuffix(".nc").split("_")[0]


def variable_aliases(candidates: list[str], experiment: str) -> set[str]:
    aliases = set(candidates)
    for candidate in candidates:
        aliases.add(f"{experiment}{candidate}")
    return aliases


def build_file_index(files: list[dict[str, Any]], variables: dict[str, list[str]], experiment: str) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for standard_name, candidates in variables.items():
        aliases = variable_aliases(candidates, experiment)
        for item in files:
            name = item["name"]
            prefix = filename_prefix(name)
            if prefix in aliases:
                indexed[standard_name] = {
                    "filename": name,
                    "remote_variable_hint": prefix,
                    "size": item.get("size"),
                }
                break
    return indexed


def inventory_ismip6_runs(
    transfer_client: globus_sdk.TransferClient,
    source_collection_id: str,
    *,
    root_path: str,
    variables: dict[str, list[str]],
) -> list[Ismip6Run]:
    root = root_path.rstrip("/") or "/"
    groups = [item for item in list_dir(transfer_client, source_collection_id, root) if item.get("type") == "dir"]
    runs: list[Ismip6Run] = []

    for group_item in sorted(groups, key=lambda item: item["name"]):
        group = group_item["name"]
        group_path = f"/{group}/"
        models = [item for item in list_dir(transfer_client, source_collection_id, group_path) if item.get("type") == "dir"]
        print(f"Inventory: {group} ({len(models)} models)", flush=True)
        for model_item in sorted(models, key=lambda item: item["name"]):
            model = model_item["name"]
            model_path = f"{group_path}{model}/"
            experiments = [
                item for item in list_dir(transfer_client, source_collection_id, model_path) if item.get("type") == "dir"
            ]
            for exp_item in sorted(experiments, key=lambda item: item["name"]):
                experiment = exp_item["name"]
                exp_path = f"{model_path}{experiment}/"
                entries = list_dir(transfer_client, source_collection_id, exp_path)
                files = [item for item in entries if item.get("type") == "file" and item.get("name", "").endswith(".nc")]
                subdirs = sorted(item["name"] for item in entries if item.get("type") == "dir")
                runs.append(
                    Ismip6Run(
                        group=group,
                        model=model,
                        experiment=experiment,
                        path=exp_path,
                        files=build_file_index(files, variables, experiment),
                        subdirs=subdirs,
                    )
                )

    return runs


def run_to_json(run: Ismip6Run) -> dict[str, Any]:
    return {
        "group": run.group,
        "model": run.model,
        "experiment": run.experiment,
        "path": run.path,
        "files": run.files,
        "subdirs": run.subdirs,
    }


def nc_attrs(obj: Any) -> dict[str, Any]:
    return {name: decode_attr(getattr(obj, name)) for name in obj.ncattrs()}


def find_data_variable(ds: Any, candidates: list[str], experiment: str) -> str:
    aliases = variable_aliases(candidates, experiment)
    for name in aliases:
        if name in ds.variables:
            return name
    for name, item in ds.variables.items():
        if name not in COORDINATE_NAMES and len(item.dimensions) >= 2:
            if name in aliases or any(name.endswith(candidate) for candidate in candidates):
                return name
    raise MissingDataVariableError(f"No variable matching {sorted(aliases)} found in file")


def actual_lat_lon(ds: Any, iy: np.ndarray, ix: np.ndarray, actual_x: np.ndarray, actual_y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if "lat" in ds.variables and "lon" in ds.variables and len(ds.variables["lat"].dimensions) == 2 and len(ds.variables["lon"].dimensions) == 2:
        lat = ds.variables["lat"]
        lon = ds.variables["lon"]
        return (
            np.asarray([lat[y, x] for y, x in zip(iy, ix, strict=False)], dtype=np.float64),
            np.asarray([normalize_longitude(float(lon[y, x])) for y, x in zip(iy, ix, strict=False)], dtype=np.float64),
        )
    lon, lat = X_Y_TO_LON_LAT.transform(actual_x, actual_y)
    return np.asarray(lat, dtype=np.float64), np.asarray([normalize_longitude(v) for v in lon], dtype=np.float64)


def synthetic_standard_axis(length: int) -> np.ndarray:
    return (np.arange(length, dtype=np.float64) - (length - 1) / 2.0) * 8000.0


def horizontal_dimension_names(ds: Any) -> tuple[str | None, str | None]:
    x_name = "x" if "x" in ds.dimensions else ("nx" if "nx" in ds.dimensions else None)
    y_name = "y" if "y" in ds.dimensions else ("ny" if "ny" in ds.dimensions else None)
    return x_name, y_name


def point_indices_and_coordinates(ds: Any, points: list[Any]) -> dict[str, np.ndarray]:
    if "x" in ds.variables and "y" in ds.variables:
        x = np.asarray(ds.variables["x"][:], dtype=np.float64)
        y = np.asarray(ds.variables["y"][:], dtype=np.float64)
        ix = np.array([nearest_index(x, point.x) for point in points], dtype=np.int64)
        iy = np.array([nearest_index(y, point.y) for point in points], dtype=np.int64)
        actual_x = np.asarray(x[ix], dtype=np.float64)
        actual_y = np.asarray(y[iy], dtype=np.float64)
        actual_latitude, actual_longitude = actual_lat_lon(ds, iy, ix, actual_x, actual_y)
        return {
            "ix": ix,
            "iy": iy,
            "actual_x": actual_x,
            "actual_y": actual_y,
            "actual_latitude": actual_latitude,
            "actual_longitude": actual_longitude,
        }

    if "lat" not in ds.variables or "lon" not in ds.variables:
        x_dim, y_dim = horizontal_dimension_names(ds)
        if x_dim is None or y_dim is None:
            raise KeyError("file is missing x/y variables, 2D lat/lon variables, and recognizable horizontal dimensions")
        x = synthetic_standard_axis(len(ds.dimensions[x_dim]))
        y = synthetic_standard_axis(len(ds.dimensions[y_dim]))
        ix = np.array([nearest_index(x, point.x) for point in points], dtype=np.int64)
        iy = np.array([nearest_index(y, point.y) for point in points], dtype=np.int64)
        actual_x = np.asarray(x[ix], dtype=np.float64)
        actual_y = np.asarray(y[iy], dtype=np.float64)
        actual_longitude, actual_latitude = X_Y_TO_LON_LAT.transform(actual_x, actual_y)
        return {
            "ix": ix,
            "iy": iy,
            "actual_x": actual_x,
            "actual_y": actual_y,
            "actual_latitude": np.asarray(actual_latitude, dtype=np.float64),
            "actual_longitude": np.asarray([normalize_longitude(v) for v in actual_longitude], dtype=np.float64),
        }

    lat = np.asarray(ds.variables["lat"][:], dtype=np.float64)
    lon = np.asarray(ds.variables["lon"][:], dtype=np.float64)
    lon = ((lon + 180.0) % 360.0) - 180.0
    iy_values: list[int] = []
    ix_values: list[int] = []
    for point in points:
        dlon = ((lon - point.longitude + 180.0) % 360.0) - 180.0
        dlat = lat - point.latitude
        scale = np.cos(np.deg2rad(point.latitude))
        distance = dlat * dlat + (dlon * scale) * (dlon * scale)
        iy, ix = np.unravel_index(np.nanargmin(distance), distance.shape)
        iy_values.append(int(iy))
        ix_values.append(int(ix))

    iy = np.asarray(iy_values, dtype=np.int64)
    ix = np.asarray(ix_values, dtype=np.int64)
    actual_latitude = np.asarray([lat[y, x] for y, x in zip(iy, ix, strict=False)], dtype=np.float64)
    actual_longitude = np.asarray([normalize_longitude(float(lon[y, x])) for y, x in zip(iy, ix, strict=False)], dtype=np.float64)
    actual_x, actual_y = LON_LAT_TO_X_Y.transform(actual_longitude, actual_latitude)
    return {
        "ix": ix,
        "iy": iy,
        "actual_x": np.asarray(actual_x, dtype=np.float64),
        "actual_y": np.asarray(actual_y, dtype=np.float64),
        "actual_latitude": actual_latitude,
        "actual_longitude": actual_longitude,
    }


def extract_ismip6_points(path: Path, *, candidates: list[str], experiment: str, points: list[Any]) -> dict[str, Any]:
    with netCDF4.Dataset(path, "r") as ds:
        variable_name = find_data_variable(ds, candidates, experiment)
        if "time" not in ds.variables:
            raise KeyError(f"{path.name} is missing a time coordinate")

        time_values = np.asarray(ds.variables["time"][:])
        variable = ds.variables[variable_name]
        if len(variable.dimensions) != 3:
            raise NotImplementedError(f"{path.name}:{variable_name} has unsupported shape {variable.shape}")

        variable_attrs = nc_attrs(variable)
        time_attrs = nc_attrs(ds.variables["time"])
        global_attrs = nc_attrs(ds)

        point_match = point_indices_and_coordinates(ds, points)
        ix = point_match["ix"]
        iy = point_match["iy"]
        values = np.ma.filled(
            np.ma.stack([variable[:, yy, xx] for yy, xx in zip(iy, ix, strict=False)], axis=1),
            np.nan,
        ).astype(np.float64)

    return {
        "remote_variable": variable_name,
        "time": np.asarray(time_values),
        "values": values,
        "requested_x": np.array([p.x for p in points], dtype=np.float64),
        "requested_y": np.array([p.y for p in points], dtype=np.float64),
        "requested_latitude": np.array([p.latitude for p in points], dtype=np.float64),
        "requested_longitude": np.array([p.longitude for p in points], dtype=np.float64),
        "actual_x": point_match["actual_x"],
        "actual_y": point_match["actual_y"],
        "actual_latitude": point_match["actual_latitude"],
        "actual_longitude": point_match["actual_longitude"],
        "point_names": [p.name for p in points],
        "variable_attrs": variable_attrs,
        "time_attrs": time_attrs,
        "global_attrs": global_attrs,
    }


def horizontal_axes(ds: Any, x_dim: str, y_dim: str) -> tuple[np.ndarray, np.ndarray]:
    if "x" in ds.variables and "y" in ds.variables:
        return np.asarray(ds.variables["x"][:], dtype=np.float64), np.asarray(ds.variables["y"][:], dtype=np.float64)
    return synthetic_standard_axis(len(ds.dimensions[x_dim])), synthetic_standard_axis(len(ds.dimensions[y_dim]))


def extract_ismip6_stencils(
    path: Path,
    *,
    candidates: list[str],
    experiment: str,
    points: list[Any],
    radius: int,
) -> dict[str, Any]:
    with netCDF4.Dataset(path, "r") as ds:
        variable_name = find_data_variable(ds, candidates, experiment)
        if "time" not in ds.variables:
            raise KeyError(f"{path.name} is missing a time coordinate")

        time_values = np.asarray(ds.variables["time"][:])
        variable = ds.variables[variable_name]
        if len(variable.dimensions) != 3:
            raise NotImplementedError(f"{path.name}:{variable_name} has unsupported shape {variable.shape}")

        time_dim, y_dim, x_dim = variable.dimensions
        if time_dim != "time":
            raise NotImplementedError(f"{path.name}:{variable_name} has unsupported dimensions {variable.dimensions}")

        x, y = horizontal_axes(ds, x_dim, y_dim)
        point_match = point_indices_and_coordinates(ds, points)
        ix = point_match["ix"]
        iy = point_match["iy"]
        window = 2 * radius + 1

        values = np.full((len(time_values), len(points), window, window), np.nan, dtype=np.float64)
        stencil_x = np.full((len(points), window), np.nan, dtype=np.float64)
        stencil_y = np.full((len(points), window), np.nan, dtype=np.float64)
        for point_index, (yy, xx) in enumerate(zip(iy, ix, strict=False)):
            y_slice = slice(int(yy) - radius, int(yy) + radius + 1)
            x_slice = slice(int(xx) - radius, int(xx) + radius + 1)
            if y_slice.start < 0 or x_slice.start < 0 or y_slice.stop > len(y) or x_slice.stop > len(x):
                raise IndexError(f"Stencil radius {radius} around point {points[point_index].name!r} exceeds domain")
            values[:, point_index, :, :] = np.ma.filled(variable[:, y_slice, x_slice], np.nan).astype(np.float64)
            stencil_x[point_index, :] = x[x_slice]
            stencil_y[point_index, :] = y[y_slice]

        variable_attrs = nc_attrs(variable)
        time_attrs = nc_attrs(ds.variables["time"])
        global_attrs = nc_attrs(ds)

    return {
        "remote_variable": variable_name,
        "time": np.asarray(time_values),
        "values": values,
        "requested_x": np.array([p.x for p in points], dtype=np.float64),
        "requested_y": np.array([p.y for p in points], dtype=np.float64),
        "requested_latitude": np.array([p.latitude for p in points], dtype=np.float64),
        "requested_longitude": np.array([p.longitude for p in points], dtype=np.float64),
        "actual_x": point_match["actual_x"],
        "actual_y": point_match["actual_y"],
        "actual_latitude": point_match["actual_latitude"],
        "actual_longitude": point_match["actual_longitude"],
        "stencil_x": stencil_x,
        "stencil_y": stencil_y,
        "point_names": [p.name for p in points],
        "variable_attrs": variable_attrs,
        "time_attrs": time_attrs,
        "global_attrs": global_attrs,
    }


def write_stencil_netcdf(
    path: Path,
    *,
    variable_name: str,
    point_names: list[str],
    requested_x: np.ndarray,
    requested_y: np.ndarray,
    requested_latitude: np.ndarray,
    requested_longitude: np.ndarray,
    actual_x: np.ndarray,
    actual_y: np.ndarray,
    actual_latitude: np.ndarray,
    actual_longitude: np.ndarray,
    stencil_x: np.ndarray,
    stencil_y: np.ndarray,
    time_values: np.ndarray,
    values: np.ndarray,
    variable_attrs: dict[str, Any],
    time_attrs: dict[str, Any],
    global_attrs: dict[str, Any],
    source_files: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    with netCDF4.Dataset(path, "w") as ds:
        ds.createDimension("time", len(time_values))
        ds.createDimension("point", len(point_names))
        ds.createDimension("stencil_y", values.shape[2])
        ds.createDimension("stencil_x", values.shape[3])

        for key, value in global_attrs.items():
            try:
                ds.setncattr(key, value)
            except (TypeError, ValueError, AttributeError):
                pass
        ds.setncattr("subset_method", "nearest_neighbour_projected_grid_stencil")
        ds.setncattr("subset_grid_crs", "EPSG:3031")
        ds.setncattr("subset_input_crs", "EPSG:4326")
        ds.setncattr("subset_source_files", json.dumps(source_files))

        time_var = ds.createVariable("time", time_values.dtype, ("time",))
        time_var[:] = time_values
        for key, value in time_attrs.items():
            try:
                time_var.setncattr(key, value)
            except (TypeError, ValueError, AttributeError):
                pass

        string_dtype = str
        point_name_var = ds.createVariable("point_name", string_dtype, ("point",))
        point_name_var[:] = np.asarray(point_names, dtype=object)

        for name, data, units, standard_name in [
            ("requested_x", requested_x, "meter", "projection_x_coordinate"),
            ("requested_y", requested_y, "meter", "projection_y_coordinate"),
            ("requested_latitude", requested_latitude, "degrees_north", "latitude"),
            ("requested_longitude", requested_longitude, "degrees_east", "longitude"),
            ("x", actual_x, "meter", "projection_x_coordinate"),
            ("y", actual_y, "meter", "projection_y_coordinate"),
            ("latitude", actual_latitude, "degrees_north", "latitude"),
            ("longitude", actual_longitude, "degrees_east", "longitude"),
        ]:
            var = ds.createVariable(name, "f8", ("point",))
            var[:] = data
            var.units = units
            var.standard_name = standard_name

        x_stencil_var = ds.createVariable("stencil_x", "f8", ("point", "stencil_x"))
        x_stencil_var[:] = stencil_x
        x_stencil_var.units = "meter"
        x_stencil_var.standard_name = "projection_x_coordinate"
        y_stencil_var = ds.createVariable("stencil_y", "f8", ("point", "stencil_y"))
        y_stencil_var[:] = stencil_y
        y_stencil_var.units = "meter"
        y_stencil_var.standard_name = "projection_y_coordinate"

        data_var = ds.createVariable(variable_name, "f8", ("time", "point", "stencil_y", "stencil_x"), zlib=True)
        data_var[:] = values
        for key, value in variable_attrs.items():
            if key == "_FillValue":
                continue
            try:
                data_var.setncattr(key, value)
            except (TypeError, ValueError, AttributeError):
                pass
        data_var.coordinates = (
            "time point_name latitude longitude x y stencil_x stencil_y "
            "requested_latitude requested_longitude requested_x requested_y"
        )


def write_stencil_json(path: Path, *, variable_name: str, extracted: dict[str, Any], source_files: list[str]) -> None:
    def json_safe(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [json_safe(item) for item in value]
        if isinstance(value, np.ndarray):
            return json_safe(value.tolist())
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
        if isinstance(value, (np.bool_,)):
            return bool(value)
        return value

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at": utc_now(),
        "variable_name": variable_name,
        "remote_variable": extracted["remote_variable"],
        "source_files": source_files,
        "dimensions": {
            "time": int(extracted["values"].shape[0]),
            "point": int(extracted["values"].shape[1]),
            "stencil_y": int(extracted["values"].shape[2]),
            "stencil_x": int(extracted["values"].shape[3]),
        },
        "points": [
            {
                "name": name,
                "requested": {
                    "x_m": float(extracted["requested_x"][index]),
                    "y_m": float(extracted["requested_y"][index]),
                    "latitude_deg": float(extracted["requested_latitude"][index]),
                    "longitude_deg": float(extracted["requested_longitude"][index]),
                },
                "actual_center": {
                    "x_m": float(extracted["actual_x"][index]),
                    "y_m": float(extracted["actual_y"][index]),
                    "latitude_deg": float(extracted["actual_latitude"][index]),
                    "longitude_deg": float(extracted["actual_longitude"][index]),
                },
                "stencil_x_m": [float(value) for value in extracted["stencil_x"][index]],
                "stencil_y_m": [float(value) for value in extracted["stencil_y"][index]],
            }
            for index, name in enumerate(extracted["point_names"])
        ],
        "variable_attrs": json_safe(extracted["variable_attrs"]),
        "time_attrs": json_safe(extracted["time_attrs"]),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def output_stem(run: Ismip6Run, standard_variable: str, point_set_name: str) -> str:
    parts = ["ismip6", "AIS", run.group, run.model, run.experiment, standard_variable, point_set_name]
    return "_".join(slug(part) for part in parts)


def output_paths(output_dir: Path, run: Ismip6Run, standard_variable: str, point_set_name: str) -> tuple[Path, Path]:
    stem = output_stem(run, standard_variable, point_set_name)
    output_base = output_dir / "point_subsets" / run.group / run.model / run.experiment
    return output_base / f"{stem}.nc", output_base / f"{stem}.json"


def write_inventory(path: Path, *, collection_id: str, runs: list[Ismip6Run], variables: dict[str, list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at": utc_now(),
        "collection_id": collection_id,
        "variables": variables,
        "run_count": len(runs),
        "runs": [run_to_json(run) for run in runs],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_summary(summary_path: Path, csv_path: Path, records: list[dict[str, Any]], *, inventory_path: Path) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for record in records:
        counts[record["status"]] = counts.get(record["status"], 0) + 1
    payload = {
        "created_at": utc_now(),
        "inventory_path": str(inventory_path),
        "record_count": len(records),
        "status_counts": counts,
        "records": records,
    }
    summary_path.write_text(json.dumps(payload, indent=2) + "\n")

    fieldnames = [
        "status",
        "group",
        "model",
        "experiment",
        "standard_variable",
        "remote_variable",
        "remote_path",
        "output_netcdf",
        "output_json",
        "message",
    ]
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def submit_batch_transfer_and_wait(
    transfer_client: globus_sdk.TransferClient,
    source_collection_id: str,
    destination_collection_id: str,
    *,
    remote_stage_dir: str,
    remote_paths: list[str],
    label: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> str:
    data = globus_sdk.TransferData(
        source_collection_id,
        destination_collection_id,
        label=label,
        sync_level="checksum",
        verify_checksum=True,
        notify_on_failed=False,
        notify_on_succeeded=False,
        notify_on_inactive=False,
    )
    for remote_path in remote_paths:
        filename = Path(remote_path).name
        data.add_item(remote_path, remote_stage_dir.rstrip("/") + "/" + filename)

    response = transfer_client.submit_transfer(data)
    task_id = response["task_id"]
    print(f"Submitted transfer task {task_id} for {len(remote_paths)} ISMIP6 files", flush=True)
    ok = transfer_client.task_wait(task_id, timeout=timeout_seconds, polling_interval=poll_interval_seconds)
    if not ok:
        task_doc = transfer_client.get_task(task_id)
        raise RuntimeError(
            f"Transfer task {task_id} did not complete successfully within the timeout. "
            f"Current status: {task_doc.get('status')}"
        )
    return task_id


def subset_one_file(
    *,
    transfer_client: globus_sdk.TransferClient,
    source_collection_id: str,
    destination_collection_id: str,
    remote_path: str,
    local_stage_dir: Path,
    remote_stage_dir: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
    delete_after_extract: bool,
    run: Ismip6Run,
    standard_variable: str,
    candidates: list[str],
    points: list[Any],
    output_dir: Path,
    point_set_name: str,
    overwrite: bool,
) -> dict[str, Any]:
    filename = Path(remote_path).name
    local_path = local_stage_dir / filename
    if local_path.exists():
        local_path.unlink()

    stem = output_stem(run, standard_variable, point_set_name)
    output_netcdf = output_dir / "point_subsets" / run.group / run.model / run.experiment / f"{stem}.nc"
    output_json = output_dir / "point_subsets" / run.group / run.model / run.experiment / f"{stem}.json"

    base_record = {
        "group": run.group,
        "model": run.model,
        "experiment": run.experiment,
        "standard_variable": standard_variable,
        "remote_path": remote_path,
        "output_netcdf": str(output_netcdf),
        "output_json": str(output_json),
    }

    if output_netcdf.exists() and output_json.exists() and not overwrite:
        return {**base_record, "status": "skipped_existing", "remote_variable": None, "message": "outputs already exist"}

    submit_transfer_and_wait(
        transfer_client,
        source_collection_id,
        destination_collection_id,
        remote_path,
        remote_stage_dir.rstrip("/") + "/" + filename,
        timeout_seconds,
        poll_interval_seconds,
    )

    wait_seconds = 0
    while not local_path.exists():
        if wait_seconds > 60:
            raise RuntimeError(f"Transfer finished but local file did not appear: {local_path}")
        time.sleep(1)
        wait_seconds += 1

    extracted = extract_ismip6_points(local_path, candidates=candidates, experiment=run.experiment, points=points)
    remote_variable = extracted["remote_variable"]
    variable_attrs = dict(extracted["variable_attrs"])
    variable_attrs["ismip6_standard_variable"] = standard_variable
    variable_attrs["ismip6_remote_variable"] = remote_variable

    output_netcdf.parent.mkdir(parents=True, exist_ok=True)
    if output_netcdf.exists():
        output_netcdf.unlink()
    if output_json.exists():
        output_json.unlink()

    source_file_label = f"{run.path}{filename}"
    write_output_netcdf(
        output_netcdf,
        variable_name=standard_variable,
        point_names=extracted["point_names"],
        requested_x=extracted["requested_x"],
        requested_y=extracted["requested_y"],
        requested_latitude=extracted["requested_latitude"],
        requested_longitude=extracted["requested_longitude"],
        actual_x=extracted["actual_x"],
        actual_y=extracted["actual_y"],
        actual_latitude=extracted["actual_latitude"],
        actual_longitude=extracted["actual_longitude"],
        time_values=extracted["time"],
        values=extracted["values"],
        variable_attrs=variable_attrs,
        time_attrs=extracted["time_attrs"],
        global_attrs=extracted["global_attrs"],
        source_files=[source_file_label],
    )
    write_output_json(
        output_json,
        variable_name=standard_variable,
        point_names=extracted["point_names"],
        requested_x=extracted["requested_x"],
        requested_y=extracted["requested_y"],
        requested_latitude=extracted["requested_latitude"],
        requested_longitude=extracted["requested_longitude"],
        actual_x=extracted["actual_x"],
        actual_y=extracted["actual_y"],
        actual_latitude=extracted["actual_latitude"],
        actual_longitude=extracted["actual_longitude"],
        time_values=extracted["time"],
        values=extracted["values"],
        variable_attrs=variable_attrs,
        time_attrs=extracted["time_attrs"],
        global_attrs=extracted["global_attrs"],
        source_files=[source_file_label],
    )

    if delete_after_extract:
        local_path.unlink(missing_ok=True)

    return {
        **base_record,
        "status": "processed",
        "remote_variable": remote_variable,
        "message": f"shape={tuple(extracted['values'].shape)}",
    }


def select_runs(
    runs: list[Ismip6Run],
    *,
    smoke: bool,
    smoke_count: int | None,
    max_runs: int | None,
    group: str | None = None,
    model: str | None = None,
    experiment: str | None = None,
) -> list[Ismip6Run]:
    if group or model or experiment:
        runs = [
            run
            for run in runs
            if (group is None or run.group == group)
            and (model is None or run.model == model)
            and (experiment is None or run.experiment == experiment)
        ]
    if smoke:
        preferred = [
            run
            for run in runs
            if run.group == "AWI" and run.model == "PISM1" and run.experiment == "exp01"
        ]
        selected = preferred or runs[:1]
        return selected[: smoke_count or 1]
    if max_runs:
        return runs[:max_runs]
    return runs


def chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def run_ismip6_subsets(
    config_path: Path,
    *,
    smoke: bool = False,
    max_runs: int | None = None,
    inventory_only: bool = False,
    group: str | None = None,
    model: str | None = None,
    experiment: str | None = None,
    summary_name: str | None = None,
) -> None:
    config_path = config_path.resolve()
    config = load_config(config_path)
    variables = config.get("variables") or DEFAULT_VARIABLES
    points = parse_points(config, config_path=config_path)

    source_cfg = config["source"]
    dest_cfg = config["destination"]
    output_cfg = config.get("output", {})
    transfer_cfg = config.get("transfer", {})

    output_root = resolve_path(output_cfg.get("root", "../../output/ismip6"), config_path=config_path)
    output_root.mkdir(parents=True, exist_ok=True)
    log_dir = output_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    app, transfer_client = build_transfer_client(config)
    source_collection_id = resolve_collection_id(
        transfer_client,
        source_cfg.get("collection_name"),
        source_cfg.get("collection_id"),
    )
    destination_collection_id = dest_cfg["collection_id"]
    transfer_client = maybe_add_data_access_consents(app, transfer_client, source_collection_id, destination_collection_id)

    runs = inventory_ismip6_runs(
        transfer_client,
        source_collection_id,
        root_path=source_cfg.get("root_path", "/"),
        variables=variables,
    )
    inventory_path = output_root / "ismip6_inventory.json"
    write_inventory(inventory_path, collection_id=source_collection_id, runs=runs, variables=variables)
    print(f"Inventoried {len(runs)} ISMIP6 runs: {inventory_path}", flush=True)

    if inventory_only:
        return

    selected_runs = select_runs(
        runs,
        smoke=smoke,
        smoke_count=None,
        max_runs=max_runs,
        group=group,
        model=model,
        experiment=experiment,
    )
    if smoke:
        output_root = output_root / "smoke"
        output_root.mkdir(parents=True, exist_ok=True)

    local_stage_dir = resolve_path(dest_cfg["local_staging_dir"], config_path=config_path)
    local_stage_dir.mkdir(parents=True, exist_ok=True)
    remote_stage_dir = dest_cfg["collection_path"]
    timeout_seconds = int(transfer_cfg.get("task_timeout_seconds", 3600))
    poll_interval_seconds = int(transfer_cfg.get("poll_interval_seconds", 10))
    delete_after_extract = bool(transfer_cfg.get("delete_after_extract", True))
    batch_file_count = int(transfer_cfg.get("batch_file_count", 24))
    overwrite = bool(output_cfg.get("overwrite", False))
    point_set_name = str(config.get("subset", {}).get("point_set_name") or Path(config["subset"]["points_file"]).stem)

    records: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    log_path = log_dir / ("ismip6_smoke_processing_log.jsonl" if smoke else "ismip6_processing_log.jsonl")
    with log_path.open("w") as log_file:
        for run in selected_runs:
            for standard_variable, candidates in variables.items():
                file_info = run.files.get(standard_variable)
                if not file_info:
                    record = {
                        "status": "skipped_missing",
                        "group": run.group,
                        "model": run.model,
                        "experiment": run.experiment,
                        "standard_variable": standard_variable,
                        "remote_variable": None,
                        "remote_path": None,
                        "output_netcdf": None,
                        "output_json": None,
                        "message": f"none of {candidates} found",
                    }
                    records.append(record)
                    log_file.write(json.dumps(record) + "\n")
                    log_file.flush()
                    print(
                        f"{record['status']}: {run.group}/{run.model}/{run.experiment} "
                        f"{standard_variable} {record.get('message', '')}",
                        flush=True,
                    )
                    continue

                remote_path = run.path + file_info["filename"]
                output_netcdf, output_json = output_paths(output_root, run, standard_variable, point_set_name)
                if output_netcdf.exists() and output_json.exists() and not overwrite:
                    record = {
                        "status": "skipped_existing",
                        "group": run.group,
                        "model": run.model,
                        "experiment": run.experiment,
                        "standard_variable": standard_variable,
                        "remote_variable": file_info.get("remote_variable_hint"),
                        "remote_path": remote_path,
                        "output_netcdf": str(output_netcdf),
                        "output_json": str(output_json),
                        "message": "outputs already exist",
                    }
                    records.append(record)
                    log_file.write(json.dumps(record) + "\n")
                    log_file.flush()
                    continue

                pending.append(
                    {
                        "run": run,
                        "standard_variable": standard_variable,
                        "candidates": candidates,
                        "file_info": file_info,
                        "remote_path": remote_path,
                        "local_path": local_stage_dir / Path(remote_path).name,
                        "output_netcdf": output_netcdf,
                        "output_json": output_json,
                    }
                )

        print(f"Pending ISMIP6 files to transfer: {len(pending)}", flush=True)

        for batch_index, batch in enumerate(chunks(pending, batch_file_count), start=1):
            print(f"Processing batch {batch_index}: {len(batch)} files", flush=True)
            for item in batch:
                item["local_path"].unlink(missing_ok=True)

            batch_failed: str | None = None
            try:
                submit_batch_transfer_and_wait(
                    transfer_client,
                    source_collection_id,
                    destination_collection_id,
                    remote_stage_dir=remote_stage_dir,
                    remote_paths=[item["remote_path"] for item in batch],
                    label=f"ISMIP6 subset batch {batch_index}",
                    timeout_seconds=timeout_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                )
                for item in batch:
                    wait_seconds = 0
                    while not item["local_path"].exists():
                        if wait_seconds > 60:
                            raise RuntimeError(f"Transfer finished but local file did not appear: {item['local_path']}")
                        time.sleep(1)
                        wait_seconds += 1
            except Exception as exc:
                batch_failed = f"{type(exc).__name__}: {exc}"

            for item in batch:
                run = item["run"]
                try:
                    if batch_failed:
                        raise RuntimeError(batch_failed)

                    extracted = extract_ismip6_points(
                        item["local_path"],
                        candidates=item["candidates"],
                        experiment=item["run"].experiment,
                        points=points,
                    )
                    remote_variable = extracted["remote_variable"]
                    variable_attrs = dict(extracted["variable_attrs"])
                    variable_attrs["ismip6_standard_variable"] = item["standard_variable"]
                    variable_attrs["ismip6_remote_variable"] = remote_variable

                    item["output_netcdf"].parent.mkdir(parents=True, exist_ok=True)
                    item["output_netcdf"].unlink(missing_ok=True)
                    item["output_json"].unlink(missing_ok=True)
                    source_file_label = item["remote_path"]
                    write_output_netcdf(
                        item["output_netcdf"],
                        variable_name=item["standard_variable"],
                        point_names=extracted["point_names"],
                        requested_x=extracted["requested_x"],
                        requested_y=extracted["requested_y"],
                        requested_latitude=extracted["requested_latitude"],
                        requested_longitude=extracted["requested_longitude"],
                        actual_x=extracted["actual_x"],
                        actual_y=extracted["actual_y"],
                        actual_latitude=extracted["actual_latitude"],
                        actual_longitude=extracted["actual_longitude"],
                        time_values=extracted["time"],
                        values=extracted["values"],
                        variable_attrs=variable_attrs,
                        time_attrs=extracted["time_attrs"],
                        global_attrs=extracted["global_attrs"],
                        source_files=[source_file_label],
                    )
                    write_output_json(
                        item["output_json"],
                        variable_name=item["standard_variable"],
                        point_names=extracted["point_names"],
                        requested_x=extracted["requested_x"],
                        requested_y=extracted["requested_y"],
                        requested_latitude=extracted["requested_latitude"],
                        requested_longitude=extracted["requested_longitude"],
                        actual_x=extracted["actual_x"],
                        actual_y=extracted["actual_y"],
                        actual_latitude=extracted["actual_latitude"],
                        actual_longitude=extracted["actual_longitude"],
                        time_values=extracted["time"],
                        values=extracted["values"],
                        variable_attrs=variable_attrs,
                        time_attrs=extracted["time_attrs"],
                        global_attrs=extracted["global_attrs"],
                        source_files=[source_file_label],
                    )
                    record = {
                        "status": "processed",
                        "group": item["run"].group,
                        "model": item["run"].model,
                        "experiment": item["run"].experiment,
                        "standard_variable": item["standard_variable"],
                        "remote_variable": remote_variable,
                        "remote_path": item["remote_path"],
                        "output_netcdf": str(item["output_netcdf"]),
                        "output_json": str(item["output_json"]),
                        "message": f"shape={tuple(extracted['values'].shape)}",
                    }
                except MissingDataVariableError as exc:
                    record = {
                        "status": "skipped_missing",
                        "group": item["run"].group,
                        "model": item["run"].model,
                        "experiment": item["run"].experiment,
                        "standard_variable": item["standard_variable"],
                        "remote_variable": item["file_info"].get("remote_variable_hint"),
                        "remote_path": item["remote_path"],
                        "output_netcdf": None,
                        "output_json": None,
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                except Exception as exc:
                    record = {
                        "status": "failed",
                        "group": item["run"].group,
                        "model": item["run"].model,
                        "experiment": item["run"].experiment,
                        "standard_variable": item["standard_variable"],
                        "remote_variable": item["file_info"].get("remote_variable_hint"),
                        "remote_path": item["remote_path"],
                        "output_netcdf": None,
                        "output_json": None,
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                finally:
                    if delete_after_extract:
                        item["local_path"].unlink(missing_ok=True)
                records.append(record)
                log_file.write(json.dumps(record) + "\n")
                log_file.flush()
                print(
                    f"{record['status']}: {item['run'].group}/{item['run'].model}/{item['run'].experiment} "
                    f"{record['standard_variable']} {record.get('message', '')}",
                    flush=True,
                )

    summary_name = summary_name or ("ismip6_smoke_processing_summary" if smoke else "ismip6_processing_summary")
    write_summary(
        output_root / f"{summary_name}.json",
        output_root / f"{summary_name}.csv",
        records,
        inventory_path=inventory_path,
    )
    print(f"Wrote summary: {output_root / f'{summary_name}.json'}")
