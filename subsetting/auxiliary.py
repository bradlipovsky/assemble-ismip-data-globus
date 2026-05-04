from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

import h5py
import numpy as np

from subsetting.pipeline import (
    GEOGRAPHIC_CRS,
    PROJECTED_GRID_CRS,
    X_Y_TO_LON_LAT,
    build_transfer_client,
    decode_attr,
    load_config,
    nearest_index,
    normalize_longitude,
    parse_points,
    resolve_collection_id,
    maybe_add_data_access_consents,
    resolve_path,
    submit_transfer_and_wait,
)

try:
    import h5netcdf
except ImportError:  # pragma: no cover
    h5netcdf = None


FILENAME_PERIOD_PATTERN = re.compile(r"(?P<label>\d{4}-\d{4})")
STRING_DTYPE = h5py.string_dtype("utf-8")


def parse_sample_label(filename: str) -> str:
    match = FILENAME_PERIOD_PATTERN.search(filename)
    if match:
        return match.group("label")
    return Path(filename).stem


def _lat_lon_from_dataset(ds: h5py.File, iy: np.ndarray, ix: np.ndarray, actual_x: np.ndarray, actual_y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if "lat" in ds and "lon" in ds:
        lat = ds["lat"]
        lon = ds["lon"]
        if lat.ndim == 2 and lon.ndim == 2:
            actual_latitude = np.asarray([lat[yy, xx] for yy, xx in zip(iy, ix, strict=False)], dtype=np.float64)
            actual_longitude = np.asarray(
                [normalize_longitude(float(lon[yy, xx])) for yy, xx in zip(iy, ix, strict=False)],
                dtype=np.float64,
            )
            return actual_latitude, actual_longitude

    actual_longitude, actual_latitude = X_Y_TO_LON_LAT.transform(actual_x, actual_y)
    return (
        np.asarray(actual_latitude, dtype=np.float64),
        np.asarray([normalize_longitude(v) for v in actual_longitude], dtype=np.float64),
    )


def extract_auxiliary_points_from_file(path: Path, variable_name: str, points: list[Any]) -> dict[str, Any]:
    with h5py.File(path, "r") as ds:
        if variable_name not in ds:
            raise KeyError(f"Variable {variable_name!r} not found in {path.name}")

        x = ds["x"][:]
        y = ds["y"][:]
        var = ds[variable_name]
        fill_value = decode_attr(var.attrs.get("_FillValue"))
        variable_attrs = {k: decode_attr(v) for k, v in var.attrs.items()}
        global_attrs = {k: decode_attr(v) for k, v in ds.attrs.items()}

        ix = np.array([nearest_index(x, point.x) for point in points], dtype=np.int64)
        iy = np.array([nearest_index(y, point.y) for point in points], dtype=np.int64)

        actual_x = np.asarray(x[ix], dtype=np.float64)
        actual_y = np.asarray(y[iy], dtype=np.float64)
        actual_latitude, actual_longitude = _lat_lon_from_dataset(ds, iy, ix, actual_x, actual_y)

        if var.ndim == 3:
            values = np.stack([var[:, yy, xx] for yy, xx in zip(iy, ix, strict=False)], axis=1).astype(np.float64)
            if "z" in ds and len(ds["z"]) == var.shape[0]:
                lead_name = "z"
                lead_values = np.asarray(ds["z"][:], dtype=np.float64)
                lead_attrs = {k: decode_attr(v) for k, v in ds["z"].attrs.items()}
            else:
                lead_name = "level"
                lead_values = np.arange(var.shape[0], dtype=np.float64)
                lead_attrs = {}
        elif var.ndim == 2:
            values = np.asarray([var[yy, xx] for yy, xx in zip(iy, ix, strict=False)], dtype=np.float64)
            lead_name = None
            lead_values = None
            lead_attrs = {}
        else:
            raise NotImplementedError(f"Unsupported variable rank for {path.name}: {var.ndim}")

        if fill_value is not None:
            values = np.where(values >= float(fill_value) * 0.1, np.nan, values)

    return {
        "values": values,
        "lead_name": lead_name,
        "lead_values": lead_values,
        "lead_attrs": lead_attrs,
        "requested_x": np.array([p.x for p in points], dtype=np.float64),
        "requested_y": np.array([p.y for p in points], dtype=np.float64),
        "requested_latitude": np.array([p.latitude for p in points], dtype=np.float64),
        "requested_longitude": np.array([p.longitude for p in points], dtype=np.float64),
        "actual_x": actual_x,
        "actual_y": actual_y,
        "actual_latitude": actual_latitude,
        "actual_longitude": actual_longitude,
        "point_names": [p.name for p in points],
        "variable_attrs": variable_attrs,
        "global_attrs": global_attrs,
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return [_json_safe(item) for item in value.tolist()]
    if isinstance(value, h5py.Reference):
        return None
    if isinstance(value, np.void):
        if value.dtype.names:
            return {name: _json_safe(value[name]) for name in value.dtype.names}
        return _json_safe(value.tolist())
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, float):
        return None if np.isnan(value) else value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def write_auxiliary_output_netcdf(
    path: Path,
    variable_name: str,
    point_names: list[str],
    sample_labels: list[str],
    values: np.ndarray,
    lead_name: str | None,
    lead_values: np.ndarray | None,
    lead_attrs: dict[str, Any],
    requested_x: np.ndarray,
    requested_y: np.ndarray,
    requested_latitude: np.ndarray,
    requested_longitude: np.ndarray,
    actual_x: np.ndarray,
    actual_y: np.ndarray,
    actual_latitude: np.ndarray,
    actual_longitude: np.ndarray,
    variable_attrs: dict[str, Any],
    global_attrs: dict[str, Any],
    source_files: list[str],
) -> None:
    if h5netcdf is None:
        raise ModuleNotFoundError("h5netcdf is required to write NetCDF outputs")

    path.parent.mkdir(parents=True, exist_ok=True)
    dims = {"sample": len(sample_labels), "point": len(point_names)}
    if lead_name and lead_values is not None:
        dims[lead_name] = len(lead_values)

    with h5netcdf.File(path, "w") as ds:
        ds.dimensions = dims
        for key, value in global_attrs.items():
            try:
                ds.attrs[key] = value
            except (TypeError, ValueError, AttributeError):
                pass
        ds.attrs["subset_method"] = "nearest_neighbour_on_projected_grid"
        ds.attrs["subset_grid_crs"] = PROJECTED_GRID_CRS
        ds.attrs["subset_input_crs"] = GEOGRAPHIC_CRS
        ds.attrs["subset_source_files"] = json.dumps(source_files)

        sample_var = ds.create_variable("sample_name", ("sample",), dtype=STRING_DTYPE)
        sample_var[:] = np.asarray(sample_labels, dtype=object)

        point_name_var = ds.create_variable("point_name", ("point",), dtype=STRING_DTYPE)
        point_name_var[:] = np.asarray(point_names, dtype=object)

        if lead_name and lead_values is not None:
            lead_var = ds.create_variable(lead_name, (lead_name,), data=lead_values)
            for key, value in lead_attrs.items():
                try:
                    lead_var.attrs[key] = value
                except (TypeError, ValueError, AttributeError):
                    pass

        for name, data, units, standard_name in [
            ("requested_latitude", requested_latitude, "degrees_north", "latitude"),
            ("requested_longitude", requested_longitude, "degrees_east", "longitude"),
            ("requested_x", requested_x, "meter", "projection_x_coordinate"),
            ("requested_y", requested_y, "meter", "projection_y_coordinate"),
            ("latitude", actual_latitude, "degrees_north", "latitude"),
            ("longitude", actual_longitude, "degrees_east", "longitude"),
            ("x", actual_x, "meter", "projection_x_coordinate"),
            ("y", actual_y, "meter", "projection_y_coordinate"),
        ]:
            var = ds.create_variable(name, ("point",), data=data)
            var.attrs["units"] = units
            var.attrs["standard_name"] = standard_name

        data_dims = ("sample", lead_name, "point") if lead_name and lead_values is not None else ("sample", "point")
        data_var = ds.create_variable(variable_name, data_dims, data=values)
        for key, value in variable_attrs.items():
            if key == "_FillValue":
                continue
            try:
                data_var.attrs[key] = value
            except (TypeError, ValueError, AttributeError):
                pass


def write_auxiliary_output_json(
    path: Path,
    variable_name: str,
    point_names: list[str],
    sample_labels: list[str],
    values: np.ndarray,
    lead_name: str | None,
    lead_values: np.ndarray | None,
    lead_attrs: dict[str, Any],
    requested_x: np.ndarray,
    requested_y: np.ndarray,
    requested_latitude: np.ndarray,
    requested_longitude: np.ndarray,
    actual_x: np.ndarray,
    actual_y: np.ndarray,
    actual_latitude: np.ndarray,
    actual_longitude: np.ndarray,
    variable_attrs: dict[str, Any],
    global_attrs: dict[str, Any],
    source_files: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "variable": variable_name,
        "subset_method": "nearest_neighbour_on_projected_grid",
        "projected_grid_crs": PROJECTED_GRID_CRS,
        "input_coordinate_crs": GEOGRAPHIC_CRS,
        "source_files": source_files,
        "sample_labels": sample_labels,
        "variable_attrs": _json_safe(variable_attrs),
        "global_attrs": _json_safe(global_attrs),
        "points": [],
    }

    if lead_name and lead_values is not None:
        payload["lead_dimension"] = {
            "name": lead_name,
            "values": _json_safe(lead_values),
            "attrs": _json_safe(lead_attrs),
        }

    for index, name in enumerate(point_names):
        point_values = values[..., index] if values.ndim > 1 else values[index]
        payload["points"].append(
            {
                "name": name,
                "requested": {
                    "latitude_deg": _json_safe(requested_latitude[index]),
                    "longitude_deg": _json_safe(requested_longitude[index]),
                    "x_m": _json_safe(requested_x[index]),
                    "y_m": _json_safe(requested_y[index]),
                },
                "actual": {
                    "latitude_deg": _json_safe(actual_latitude[index]),
                    "longitude_deg": _json_safe(actual_longitude[index]),
                    "x_m": _json_safe(actual_x[index]),
                    "y_m": _json_safe(actual_y[index]),
                },
                "values": _json_safe(point_values),
            }
        )

    with path.open("w") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def run_auxiliary_subset(
    *,
    config_path: Path,
    remote_files: list[str],
    variable_name: str,
    output_netcdf: Path,
    output_json: Path,
) -> None:
    config = load_config(config_path)
    points = parse_points(config, config_path=config_path.resolve())
    app, transfer_client = build_transfer_client(config)

    source_cfg = config["source"]
    dest_cfg = config["destination"]
    source_collection_id = resolve_collection_id(
        transfer_client,
        source_cfg.get("collection_name"),
        source_cfg.get("collection_id"),
    )
    destination_collection_id = dest_cfg["collection_id"]
    transfer_client = maybe_add_data_access_consents(
        app,
        transfer_client,
        source_collection_id,
        destination_collection_id,
    )

    local_stage_dir = resolve_path(dest_cfg["local_staging_dir"], config_path=config_path)
    local_stage_dir.mkdir(parents=True, exist_ok=True)
    remote_stage_dir = dest_cfg["collection_path"]

    extracted_chunks: list[np.ndarray] = []
    sample_labels: list[str] = []
    source_files: list[str] = []
    metadata: dict[str, Any] | None = None

    for remote_source_path in remote_files:
        filename = Path(remote_source_path).name
        remote_destination_path = remote_stage_dir.rstrip("/") + "/" + filename
        local_path = local_stage_dir / filename
        if local_path.exists():
            local_path.unlink()

        submit_transfer_and_wait(
            transfer_client,
            source_collection_id,
            destination_collection_id,
            remote_source_path,
            remote_destination_path,
            timeout_seconds=3600,
            poll_interval_seconds=10,
        )

        wait_seconds = 0
        while not local_path.exists():
            if wait_seconds > 60:
                raise RuntimeError(f"Transfer finished but local file did not appear: {local_path}")
            time.sleep(1)
            wait_seconds += 1

        extracted = extract_auxiliary_points_from_file(local_path, variable_name=variable_name, points=points)
        extracted_chunks.append(extracted["values"])
        sample_labels.append(parse_sample_label(filename))
        source_files.append(filename)
        metadata = extracted
        print(f"Extracted {filename}: shape={np.shape(extracted['values'])}")
        local_path.unlink()

    if metadata is None:
        raise RuntimeError("No files were processed")

    if extracted_chunks[0].ndim == 2:
        values = np.stack(extracted_chunks, axis=0)
    else:
        values = np.stack(extracted_chunks, axis=0)

    resolved_output_netcdf = resolve_path(output_netcdf, config_path=config_path)
    resolved_output_json = resolve_path(output_json, config_path=config_path)

    write_auxiliary_output_netcdf(
        resolved_output_netcdf,
        variable_name=variable_name,
        point_names=metadata["point_names"],
        sample_labels=sample_labels,
        values=values,
        lead_name=metadata["lead_name"],
        lead_values=metadata["lead_values"],
        lead_attrs=metadata["lead_attrs"],
        requested_x=metadata["requested_x"],
        requested_y=metadata["requested_y"],
        requested_latitude=metadata["requested_latitude"],
        requested_longitude=metadata["requested_longitude"],
        actual_x=metadata["actual_x"],
        actual_y=metadata["actual_y"],
        actual_latitude=metadata["actual_latitude"],
        actual_longitude=metadata["actual_longitude"],
        variable_attrs=metadata["variable_attrs"],
        global_attrs=metadata["global_attrs"],
        source_files=source_files,
    )
    print(f"Wrote subset NetCDF: {resolved_output_netcdf}")

    write_auxiliary_output_json(
        resolved_output_json,
        variable_name=variable_name,
        point_names=metadata["point_names"],
        sample_labels=sample_labels,
        values=values,
        lead_name=metadata["lead_name"],
        lead_values=metadata["lead_values"],
        lead_attrs=metadata["lead_attrs"],
        requested_x=metadata["requested_x"],
        requested_y=metadata["requested_y"],
        requested_latitude=metadata["requested_latitude"],
        requested_longitude=metadata["requested_longitude"],
        actual_x=metadata["actual_x"],
        actual_y=metadata["actual_y"],
        actual_latitude=metadata["actual_latitude"],
        actual_longitude=metadata["actual_longitude"],
        variable_attrs=metadata["variable_attrs"],
        global_attrs=metadata["global_attrs"],
        source_files=source_files,
    )
    print(f"Wrote subset JSON: {resolved_output_json}")
