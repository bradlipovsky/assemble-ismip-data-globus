from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import globus_sdk
import h5py
import numpy as np
from globus_sdk import GlobusAppConfig
from globus_sdk.scopes import TransferScopes
from pyproj import Transformer

try:
    import h5netcdf
except ImportError:  # pragma: no cover - exercised only in lighter local setups
    h5netcdf = None


TUTORIAL_NATIVE_APP_CLIENT_ID = "61338d24-54d5-408f-a10d-66c06b59f6d2"
PROJECTED_GRID_CRS = "EPSG:3031"
GEOGRAPHIC_CRS = "EPSG:4326"
LON_LAT_TO_X_Y = Transformer.from_crs(GEOGRAPHIC_CRS, PROJECTED_GRID_CRS, always_xy=True)
X_Y_TO_LON_LAT = Transformer.from_crs(PROJECTED_GRID_CRS, GEOGRAPHIC_CRS, always_xy=True)
TIME_UNITS_PATTERN = re.compile(r"^(?P<unit>\w+)\s+since\s+(?P<base>.+)$")


@dataclass
class Point:
    name: str
    latitude: float
    longitude: float
    x: float
    y: float


def resolve_path(path_value: str | Path, *, config_path: Path) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (config_path.parent / path).resolve()
    return path


def load_config(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def build_app(config: dict[str, Any]) -> globus_sdk.UserApp:
    app_cfg = config.get("globus_app", {})
    client_id = app_cfg.get("client_id", TUTORIAL_NATIVE_APP_CLIENT_ID)
    app_name = app_cfg.get("app_name", "cesm2-ismip7-subsetter")
    return globus_sdk.UserApp(
        app_name,
        client_id=client_id,
        config=GlobusAppConfig(request_refresh_tokens=True),
    )


def build_transfer_client(config: dict[str, Any]) -> tuple[globus_sdk.UserApp, globus_sdk.TransferClient]:
    app = build_app(config)
    app.login()
    return app, globus_sdk.TransferClient(app=app)


def resolve_collection_id(
    transfer_client: globus_sdk.TransferClient,
    collection_name: str | None,
    collection_id: str | None,
) -> str:
    if collection_id:
        return collection_id
    if not collection_name:
        raise ValueError("Provide either source.collection_id or source.collection_name")

    results = list(transfer_client.endpoint_search(filter_fulltext=collection_name, limit=20))
    exact_matches = [item for item in results if item.get("display_name") == collection_name]
    matches = exact_matches or results
    if not matches:
        raise RuntimeError(f"No Globus collections matched {collection_name!r}")
    if len(exact_matches) > 1:
        ids = ", ".join(item["id"] for item in exact_matches)
        raise RuntimeError(
            f"Multiple exact matches found for {collection_name!r}. Set source.collection_id explicitly. Candidates: {ids}"
        )
    chosen = matches[0]
    print(f"Using source collection: {chosen['display_name']} ({chosen['id']})")
    return chosen["id"]


def collection_uses_data_access(doc: dict[str, Any]) -> bool:
    entity_type = str(doc.get("entity_type", ""))
    high_assurance = bool(doc.get("high_assurance", False))
    advertised_scopes = doc.get("mapped_collection_data_access_scope") or doc.get("data_access_scope")
    return entity_type.endswith("mapped_collection") and not high_assurance and bool(advertised_scopes)


def maybe_add_data_access_consents(
    app: globus_sdk.UserApp,
    transfer_client: globus_sdk.TransferClient,
    source_collection_id: str,
    destination_collection_id: str,
) -> globus_sdk.TransferClient:
    extra_scopes: list[str] = []
    for collection_id in (source_collection_id, destination_collection_id):
        doc = transfer_client.get_endpoint(collection_id)
        if collection_uses_data_access(doc):
            scope = doc.get("mapped_collection_data_access_scope") or doc.get("data_access_scope")
            if scope:
                extra_scopes.append(str(scope))

    if not extra_scopes:
        return transfer_client

    print("Adding collection-specific Globus consent scopes and refreshing login ...")
    app.add_scope_requirements({"transfer.api.globus.org": [TransferScopes.all, *extra_scopes]})
    app.login(force=True)
    return globus_sdk.TransferClient(app=app)


def list_remote_files(
    transfer_client: globus_sdk.TransferClient,
    source_collection_id: str,
    source_path: str,
    pattern: str,
) -> list[dict[str, Any]]:
    entries = list(transfer_client.operation_ls(source_collection_id, path=source_path))
    files = [
        item
        for item in entries
        if item.get("type") == "file" and fnmatch.fnmatch(item["name"], pattern)
    ]
    files.sort(key=lambda item: item["name"])
    return files


def list_local_files(source_cfg: dict[str, Any], config_path: Path) -> list[Path]:
    local_files = source_cfg.get("local_files")
    local_dir = source_cfg.get("local_dir")
    pattern = source_cfg.get("glob_pattern", "*.nc")

    if local_files and local_dir:
        raise ValueError("Use either source.local_files or source.local_dir, not both")

    if local_files:
        if not isinstance(local_files, list):
            raise ValueError("source.local_files must be a list when provided")
        paths = []
        for item in local_files:
            paths.append(resolve_path(item, config_path=config_path))
        return sorted(paths)

    if local_dir:
        local_dir_path = resolve_path(local_dir, config_path=config_path)
        return sorted(path for path in local_dir_path.iterdir() if path.is_file() and fnmatch.fnmatch(path.name, pattern))

    return []


def submit_transfer_and_wait(
    transfer_client: globus_sdk.TransferClient,
    source_collection_id: str,
    destination_collection_id: str,
    source_path: str,
    destination_path: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> str:
    label = f"ISMIP7 subset transfer {Path(source_path).name}"
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
    data.add_item(source_path, destination_path)
    response = transfer_client.submit_transfer(data)
    task_id = response["task_id"]
    print(f"Submitted transfer task {task_id} for {Path(source_path).name}")

    ok = transfer_client.task_wait(
        task_id,
        timeout=timeout_seconds,
        polling_interval=poll_interval_seconds,
    )
    if not ok:
        task_doc = transfer_client.get_task(task_id)
        raise RuntimeError(
            f"Transfer task {task_id} did not complete successfully within the timeout. "
            f"Current status: {task_doc.get('status')}"
        )
    return task_id


def decode_attr(value: Any) -> Any:
    if isinstance(value, h5py.Reference):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, np.ndarray) and value.shape == (1,):
        return decode_attr(value[0])
    return value


def normalize_longitude(longitude: float) -> float:
    normalized = ((longitude + 180.0) % 360.0) - 180.0
    if normalized == -180.0 and longitude > 0:
        return 180.0
    return normalized


def parse_angular_coordinate(value: Any, coordinate_name: str) -> float:
    if isinstance(value, (int, float, np.floating, np.integer)):
        numeric = float(value)
    elif isinstance(value, str):
        text = value.strip().upper().replace(" ", "")
        hemisphere = text[-1] if text and text[-1] in {"N", "S", "E", "W"} else None
        if hemisphere:
            text = text[:-1]
        if not text:
            raise ValueError(f"Invalid {coordinate_name} value: {value!r}")
        explicit_sign = -1.0 if text.startswith("-") else 1.0
        numeric = abs(float(text))
        if hemisphere and not text.startswith(("-", "+")):
            if hemisphere in {"S", "W"}:
                explicit_sign = -1.0
        numeric *= explicit_sign
    else:
        raise TypeError(f"Unsupported {coordinate_name} value type: {type(value)!r}")

    if coordinate_name == "latitude" and not -90.0 <= numeric <= 90.0:
        raise ValueError(f"Latitude must be between -90 and 90 degrees: {value!r}")
    if coordinate_name == "longitude":
        numeric = normalize_longitude(numeric)
    return numeric


def load_points_payload(points_file: Path) -> list[dict[str, Any]]:
    with points_file.open() as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        payload = payload.get("points", [])
    if not isinstance(payload, list):
        raise ValueError(f"Point file must contain a list or an object with a 'points' list: {points_file}")
    return payload


def parse_point_definition(item: dict[str, Any], index: int) -> Point:
    name = str(item.get("name") or f"point_{index}")

    has_geographic = any(key in item for key in ("latitude", "lat")) and any(
        key in item for key in ("longitude", "lon")
    )
    has_projected = "x" in item and "y" in item

    if has_geographic:
        latitude = parse_angular_coordinate(item.get("latitude", item.get("lat")), "latitude")
        longitude = parse_angular_coordinate(item.get("longitude", item.get("lon")), "longitude")
        x, y = LON_LAT_TO_X_Y.transform(longitude, latitude)
    elif has_projected:
        x = float(item["x"])
        y = float(item["y"])
        longitude, latitude = X_Y_TO_LON_LAT.transform(x, y)
        longitude = normalize_longitude(float(longitude))
        latitude = float(latitude)
    else:
        raise ValueError(
            f"Point {name!r} must define either latitude/longitude or x/y coordinates. "
            f"Received keys: {sorted(item.keys())}"
        )

    return Point(
        name=name,
        latitude=float(latitude),
        longitude=float(longitude),
        x=float(x),
        y=float(y),
    )


def parse_points(config: dict[str, Any], config_path: Path) -> list[Point]:
    subset_cfg = config["subset"]
    raw_points: list[dict[str, Any]] = []

    points_file = subset_cfg.get("points_file")
    if points_file:
        points_path = resolve_path(points_file, config_path=config_path)
        raw_points.extend(load_points_payload(points_path))

    inline_points = subset_cfg.get("points", [])
    if inline_points:
        if not isinstance(inline_points, list):
            raise ValueError("subset.points must be a list when provided")
        raw_points.extend(inline_points)

    points = [parse_point_definition(item, index + 1) for index, item in enumerate(raw_points)]
    if not points:
        raise ValueError("Provide at least one point via subset.points or subset.points_file")
    return points


def nearest_index(axis: np.ndarray, value: float) -> int:
    return int(np.abs(axis - value).argmin())


def extract_points_from_file(
    path: Path,
    variable_name: str,
    points: list[Point],
) -> dict[str, Any]:
    with h5py.File(path, "r") as ds:
        if variable_name not in ds:
            raise KeyError(f"Variable {variable_name!r} not found in {path.name}")
        x = ds["x"][:]
        y = ds["y"][:]
        time_values = ds["time"][:]
        var = ds[variable_name]
        fill_value = decode_attr(var.attrs.get("_FillValue"))
        variable_attrs = {k: decode_attr(v) for k, v in var.attrs.items()}
        time_attrs = {k: decode_attr(v) for k, v in ds["time"].attrs.items()}
        global_attrs = {k: decode_attr(v) for k, v in ds.attrs.items()}

        ix = np.array([nearest_index(x, point.x) for point in points], dtype=np.int64)
        iy = np.array([nearest_index(y, point.y) for point in points], dtype=np.int64)

        values = np.stack([var[:, yy, xx] for yy, xx in zip(iy, ix, strict=False)], axis=1).astype(np.float64)
        if fill_value is not None:
            values[values >= float(fill_value) * 0.1] = np.nan

        actual_x = x[ix].astype(np.float64)
        actual_y = y[iy].astype(np.float64)
        actual_longitude, actual_latitude = X_Y_TO_LON_LAT.transform(actual_x, actual_y)

    return {
        "time": np.asarray(time_values),
        "values": values,
        "requested_x": np.array([p.x for p in points], dtype=np.float64),
        "requested_y": np.array([p.y for p in points], dtype=np.float64),
        "requested_latitude": np.array([p.latitude for p in points], dtype=np.float64),
        "requested_longitude": np.array([p.longitude for p in points], dtype=np.float64),
        "actual_x": actual_x,
        "actual_y": actual_y,
        "actual_latitude": np.asarray(actual_latitude, dtype=np.float64),
        "actual_longitude": np.asarray([normalize_longitude(v) for v in actual_longitude], dtype=np.float64),
        "point_names": [p.name for p in points],
        "variable_attrs": variable_attrs,
        "time_attrs": time_attrs,
        "global_attrs": global_attrs,
    }


def write_output_netcdf(
    path: Path,
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
    time_values: np.ndarray,
    values: np.ndarray,
    variable_attrs: dict[str, Any],
    time_attrs: dict[str, Any],
    global_attrs: dict[str, Any],
    source_files: list[str],
) -> None:
    if h5netcdf is None:
        raise ModuleNotFoundError(
            "h5netcdf is required to write the compact NetCDF output. "
            "Install it or set output.path to null and rely on output.json_path."
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    string_dtype = h5py.string_dtype("utf-8")

    with h5netcdf.File(path, "w") as ds:
        ds.dimensions = {"time": len(time_values), "point": len(point_names)}

        for key, value in global_attrs.items():
            try:
                ds.attrs[key] = value
            except (TypeError, ValueError, AttributeError):
                pass
        ds.attrs["subset_method"] = "nearest_neighbour_on_projected_grid"
        ds.attrs["subset_grid_crs"] = PROJECTED_GRID_CRS
        ds.attrs["subset_input_crs"] = GEOGRAPHIC_CRS
        ds.attrs["subset_source_files"] = json.dumps(source_files)

        time_var = ds.create_variable("time", ("time",), data=time_values)
        for key, value in time_attrs.items():
            try:
                time_var.attrs[key] = value
            except (TypeError, ValueError, AttributeError):
                pass

        point_name_var = ds.create_variable("point_name", ("point",), dtype=string_dtype)
        point_name_var[:] = np.asarray(point_names, dtype=object)

        lat_req_var = ds.create_variable("requested_latitude", ("point",), data=requested_latitude)
        lat_req_var.attrs["units"] = "degrees_north"
        lat_req_var.attrs["standard_name"] = "latitude"
        lon_req_var = ds.create_variable("requested_longitude", ("point",), data=requested_longitude)
        lon_req_var.attrs["units"] = "degrees_east"
        lon_req_var.attrs["standard_name"] = "longitude"

        x_req_var = ds.create_variable("requested_x", ("point",), data=requested_x)
        x_req_var.attrs["units"] = "meter"
        x_req_var.attrs["standard_name"] = "projection_x_coordinate"
        y_req_var = ds.create_variable("requested_y", ("point",), data=requested_y)
        y_req_var.attrs["units"] = "meter"
        y_req_var.attrs["standard_name"] = "projection_y_coordinate"

        x_var = ds.create_variable("x", ("point",), data=actual_x)
        x_var.attrs["units"] = "meter"
        x_var.attrs["standard_name"] = "projection_x_coordinate"
        y_var = ds.create_variable("y", ("point",), data=actual_y)
        y_var.attrs["units"] = "meter"
        y_var.attrs["standard_name"] = "projection_y_coordinate"

        lat_var = ds.create_variable("latitude", ("point",), data=actual_latitude)
        lat_var.attrs["units"] = "degrees_north"
        lat_var.attrs["standard_name"] = "latitude"
        lon_var = ds.create_variable("longitude", ("point",), data=actual_longitude)
        lon_var.attrs["units"] = "degrees_east"
        lon_var.attrs["standard_name"] = "longitude"

        data_var = ds.create_variable(variable_name, ("time", "point"), data=values)
        for key, value in variable_attrs.items():
            if key == "_FillValue":
                continue
            try:
                data_var.attrs[key] = value
            except (TypeError, ValueError, AttributeError):
                pass
        data_var.attrs["coordinates"] = (
            "time point_name latitude longitude x y "
            "requested_latitude requested_longitude requested_x requested_y"
        )


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
        if np.isnan(value):
            return None
        return float(value)
    if isinstance(value, float):
        return None if np.isnan(value) else value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def maybe_format_time_values(time_values: np.ndarray, time_attrs: dict[str, Any]) -> list[str] | None:
    units = str(time_attrs.get("units", "")).strip()
    match = TIME_UNITS_PATTERN.match(units)
    if not match:
        return None
    if match.group("unit").lower() != "days":
        return None

    base_text = match.group("base").strip()
    try:
        base_time = datetime.fromisoformat(base_text)
    except ValueError:
        return None

    return [(base_time + timedelta(days=float(day))).isoformat() for day in time_values]


def write_output_json(
    path: Path,
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
    time_values: np.ndarray,
    values: np.ndarray,
    variable_attrs: dict[str, Any],
    time_attrs: dict[str, Any],
    global_attrs: dict[str, Any],
    source_files: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    time_iso8601 = maybe_format_time_values(time_values, time_attrs)
    payload = {
        "variable": variable_name,
        "subset_method": "nearest_neighbour_on_projected_grid",
        "projected_grid_crs": PROJECTED_GRID_CRS,
        "input_coordinate_crs": GEOGRAPHIC_CRS,
        "source_files": source_files,
        "time": {
            "values": _json_safe(time_values),
            "attrs": _json_safe(time_attrs),
            "iso8601": time_iso8601,
        },
        "variable_attrs": _json_safe(variable_attrs),
        "global_attrs": _json_safe(global_attrs),
        "points": [],
    }

    for index, name in enumerate(point_names):
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
                "values": _json_safe(values[:, index]),
            }
        )

    with path.open("w") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def run_pipeline(config_path: Path, list_only: bool = False) -> None:
    config = load_config(config_path)
    points = parse_points(config, config_path=config_path.resolve())

    source_cfg = config["source"]
    dest_cfg = config["destination"]
    output_cfg = config.get("output", {})
    transfer_cfg = config.get("transfer", {})
    subset_cfg = config["subset"]
    local_files = list_local_files(source_cfg, config_path=config_path.resolve())
    use_local_files = bool(local_files)

    source_collection_id: str | None = None
    destination_collection_id: str | None = None
    remote_dir = source_cfg.get("path", "")
    transfer_client: globus_sdk.TransferClient | None = None

    if not use_local_files:
        app, transfer_client = build_transfer_client(config)
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

        pattern = source_cfg.get("glob_pattern", "*.nc")
        remote_dir = source_cfg["path"]
        files: list[dict[str, Any]] | list[Path] = list_remote_files(transfer_client, source_collection_id, remote_dir, pattern)
    else:
        files = local_files

    file_limit = transfer_cfg.get("limit_files")
    if file_limit:
        files = files[: int(file_limit)]

    print(f"Configured {len(points)} target points")
    for point in points:
        print(
            f"  - {point.name}: lat={point.latitude:.6f}, lon={point.longitude:.6f}, "
            f"x={point.x:.1f}, y={point.y:.1f}"
        )

    if use_local_files:
        print(f"Found {len(files)} local files for processing")
        for item in files:
            print(f"  - {item}")
    else:
        print(f"Found {len(files)} files in {remote_dir!r} matching {pattern!r}")
        for item in files:
            print(f"  - {item['name']}")
    if list_only:
        return
    if not files:
        raise RuntimeError("No matching files found to process")

    if not use_local_files:
        local_stage_dir = resolve_path(dest_cfg["local_staging_dir"], config_path=config_path)
        local_stage_dir.mkdir(parents=True, exist_ok=True)
        remote_stage_dir = dest_cfg["collection_path"]

    timeout_seconds = int(transfer_cfg.get("task_timeout_seconds", 3600))
    poll_interval_seconds = int(transfer_cfg.get("poll_interval_seconds", 10))
    delete_after_extract = bool(transfer_cfg.get("delete_after_extract", True))
    variable_name = subset_cfg.get("variable", "tas")

    all_time_chunks: list[np.ndarray] = []
    all_value_chunks: list[np.ndarray] = []
    source_files: list[str] = []
    metadata: dict[str, Any] | None = None

    for item in files:
        if use_local_files:
            local_path = Path(item).resolve()
            filename = local_path.name
            if not local_path.exists():
                raise FileNotFoundError(f"Configured local file does not exist: {local_path}")
        else:
            assert transfer_client is not None
            assert source_collection_id is not None
            assert destination_collection_id is not None
            filename = item["name"]
            remote_source_path = remote_dir.rstrip("/") + "/" + filename
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
                timeout_seconds=timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
            )

            wait_seconds = 0
            while not local_path.exists():
                if wait_seconds > 30:
                    raise RuntimeError(f"Transfer finished but local file did not appear: {local_path}")
                time.sleep(1)
                wait_seconds += 1

        extracted = extract_points_from_file(local_path, variable_name=variable_name, points=points)
        all_time_chunks.append(extracted["time"])
        all_value_chunks.append(extracted["values"])
        source_files.append(filename)
        metadata = extracted

        print(f"Extracted {filename}: shape={extracted['values'].shape}")
        if not use_local_files and delete_after_extract:
            local_path.unlink()

    if metadata is None:
        raise RuntimeError("No files were processed")

    all_times = np.concatenate(all_time_chunks)
    all_values = np.vstack(all_value_chunks)
    order = np.argsort(all_times)
    all_times = all_times[order]
    all_values = all_values[order, :]

    duplicate_times = np.unique(all_times, return_counts=True)
    if np.any(duplicate_times[1] > 1):
        raise RuntimeError("Duplicate time values were detected across input files; refine file selection first.")

    output_path = output_cfg.get("path")
    if output_path:
        netcdf_output_path = resolve_path(output_path, config_path=config_path)
        if netcdf_output_path.exists() and not bool(output_cfg.get("overwrite", False)):
            raise FileExistsError(f"Output file already exists: {netcdf_output_path}")
        if netcdf_output_path.exists():
            netcdf_output_path.unlink()
        write_output_netcdf(
            netcdf_output_path,
            variable_name=variable_name,
            point_names=metadata["point_names"],
            requested_x=metadata["requested_x"],
            requested_y=metadata["requested_y"],
            requested_latitude=metadata["requested_latitude"],
            requested_longitude=metadata["requested_longitude"],
            actual_x=metadata["actual_x"],
            actual_y=metadata["actual_y"],
            actual_latitude=metadata["actual_latitude"],
            actual_longitude=metadata["actual_longitude"],
            time_values=all_times,
            values=all_values,
            variable_attrs=metadata["variable_attrs"],
            time_attrs=metadata["time_attrs"],
            global_attrs=metadata["global_attrs"],
            source_files=source_files,
        )
        print()
        print(f"Wrote subset NetCDF: {netcdf_output_path}")

    json_output_path = output_cfg.get("json_path")
    if json_output_path:
        reduced_json_path = resolve_path(json_output_path, config_path=config_path)
        if reduced_json_path.exists() and not bool(output_cfg.get("overwrite", False)):
            raise FileExistsError(f"Output file already exists: {reduced_json_path}")
        if reduced_json_path.exists():
            reduced_json_path.unlink()
        write_output_json(
            reduced_json_path,
            variable_name=variable_name,
            point_names=metadata["point_names"],
            requested_x=metadata["requested_x"],
            requested_y=metadata["requested_y"],
            requested_latitude=metadata["requested_latitude"],
            requested_longitude=metadata["requested_longitude"],
            actual_x=metadata["actual_x"],
            actual_y=metadata["actual_y"],
            actual_latitude=metadata["actual_latitude"],
            actual_longitude=metadata["actual_longitude"],
            time_values=all_times,
            values=all_values,
            variable_attrs=metadata["variable_attrs"],
            time_attrs=metadata["time_attrs"],
            global_attrs=metadata["global_attrs"],
            source_files=source_files,
        )
        print(f"Wrote subset JSON: {reduced_json_path}")

    if not output_path and not json_output_path:
        raise ValueError("Configure at least one of output.path or output.json_path")

    print(f"Processed {len(source_files)} source files")


def print_parser_help(parser: argparse.ArgumentParser) -> None:
    parser.print_help(sys.stderr)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Transfer ISMIP7 NetCDF files from Globus one at a time, extract point time series, "
            "and write compact local outputs."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "configs" / "globus" / "globus_subset_config.sample.json",
        help="Path to the JSON config file.",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Authenticate, resolve the collection, and list matching source files without transferring anything.",
    )
    return parser


def main() -> int:
    parser = make_parser()
    args = parser.parse_args()
    try:
        run_pipeline(args.config, list_only=args.list_only)
    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
