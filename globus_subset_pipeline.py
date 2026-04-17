from __future__ import annotations

import argparse
import fnmatch
import json
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import globus_sdk
import h5netcdf
import h5py
import numpy as np
from globus_sdk import GlobusAppConfig
from globus_sdk.scopes import TransferScopes


TUTORIAL_NATIVE_APP_CLIENT_ID = "61338d24-54d5-408f-a10d-66c06b59f6d2"


@dataclass
class Point:
    name: str
    x: float
    y: float


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
    return entity_type.endswith("mapped_collection") and not high_assurance


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
            extra_scopes.append(f"https://auth.globus.org/scopes/{collection_id}/data_access")

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
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, np.ndarray) and value.shape == (1,):
        return decode_attr(value[0])
    return value


def parse_points(config: dict[str, Any]) -> list[Point]:
    raw_points = config["subset"]["points"]
    points = [Point(name=item["name"], x=float(item["x"]), y=float(item["y"])) for item in raw_points]
    if not points:
        raise ValueError("subset.points must contain at least one point")
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

        actual_x = x[ix]
        actual_y = y[iy]

    return {
        "time": np.asarray(time_values),
        "values": values,
        "requested_x": np.array([p.x for p in points], dtype=np.float64),
        "requested_y": np.array([p.y for p in points], dtype=np.float64),
        "actual_x": actual_x.astype(np.float64),
        "actual_y": actual_y.astype(np.float64),
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
    actual_x: np.ndarray,
    actual_y: np.ndarray,
    time_values: np.ndarray,
    values: np.ndarray,
    variable_attrs: dict[str, Any],
    time_attrs: dict[str, Any],
    global_attrs: dict[str, Any],
    source_files: list[str],
) -> None:
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
        ds.attrs["subset_source_files"] = json.dumps(source_files)

        time_var = ds.create_variable("time", ("time",), data=time_values)
        for key, value in time_attrs.items():
            try:
                time_var.attrs[key] = value
            except (TypeError, ValueError, AttributeError):
                pass

        point_name_var = ds.create_variable("point_name", ("point",), dtype=string_dtype)
        point_name_var[:] = np.asarray(point_names, dtype=object)

        x_req_var = ds.create_variable("requested_x", ("point",), data=requested_x)
        x_req_var.attrs["units"] = "meter"
        y_req_var = ds.create_variable("requested_y", ("point",), data=requested_y)
        y_req_var.attrs["units"] = "meter"
        x_var = ds.create_variable("x", ("point",), data=actual_x)
        x_var.attrs["units"] = "meter"
        y_var = ds.create_variable("y", ("point",), data=actual_y)
        y_var.attrs["units"] = "meter"

        data_var = ds.create_variable(variable_name, ("time", "point"), data=values)
        for key, value in variable_attrs.items():
            if key == "_FillValue":
                continue
            try:
                data_var.attrs[key] = value
            except (TypeError, ValueError, AttributeError):
                pass
        data_var.attrs["coordinates"] = "time point_name x y requested_x requested_y"


def run_pipeline(config_path: Path, list_only: bool = False) -> None:
    config = load_config(config_path)
    points = parse_points(config)
    app, transfer_client = build_transfer_client(config)

    source_cfg = config["source"]
    dest_cfg = config["destination"]
    output_cfg = config["output"]
    transfer_cfg = config.get("transfer", {})
    subset_cfg = config["subset"]

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
    files = list_remote_files(transfer_client, source_collection_id, remote_dir, pattern)
    file_limit = transfer_cfg.get("limit_files")
    if file_limit:
        files = files[: int(file_limit)]

    print(f"Found {len(files)} files in {remote_dir!r} matching {pattern!r}")
    for item in files:
        print(f"  - {item['name']}")
    if list_only:
        return
    if not files:
        raise RuntimeError("No matching files found to process")

    local_stage_dir = Path(dest_cfg["local_staging_dir"]).expanduser().resolve()
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
        if delete_after_extract:
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

    output_path = Path(output_cfg["path"]).expanduser().resolve()
    if output_path.exists() and not bool(output_cfg.get("overwrite", False)):
        raise FileExistsError(f"Output file already exists: {output_path}")

    if output_path.exists():
        output_path.unlink()

    write_output_netcdf(
        output_path,
        variable_name=variable_name,
        point_names=metadata["point_names"],
        requested_x=metadata["requested_x"],
        requested_y=metadata["requested_y"],
        actual_x=metadata["actual_x"],
        actual_y=metadata["actual_y"],
        time_values=all_times,
        values=all_values,
        variable_attrs=metadata["variable_attrs"],
        time_attrs=metadata["time_attrs"],
        global_attrs=metadata["global_attrs"],
        source_files=source_files,
    )

    print()
    print(f"Wrote subset file: {output_path}")
    print(f"Processed {len(source_files)} source files")


def print_parser_help(parser: argparse.ArgumentParser) -> None:
    parser.print_help(sys.stderr)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transfer ISMIP7 NetCDF files from Globus one at a time, subset points, and write a compact NetCDF."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("globus_subset_config.sample.json"),
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
