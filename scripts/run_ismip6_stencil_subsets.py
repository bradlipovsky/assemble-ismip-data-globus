from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

from subsetting.ismip6 import (
    DEFAULT_VARIABLES,
    extract_ismip6_stencils,
    inventory_ismip6_runs,
    select_runs,
    slug,
    utc_now,
    write_inventory,
    write_stencil_json,
    write_stencil_netcdf,
)
from subsetting.pipeline import (
    build_transfer_client,
    load_config,
    maybe_add_data_access_consents,
    parse_points,
    resolve_collection_id,
    resolve_path,
    submit_transfer_and_wait,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "globus" / "ismip6_subset_config.local.json"


def output_stem(group: str, model: str, experiment: str, variable: str, point_set_name: str, radius: int) -> str:
    parts = ["ismip6", "AIS", group, model, experiment, variable, f"{point_set_name}_r{radius}_stencil"]
    return "_".join(slug(part) for part in parts)


def write_summary(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at": utc_now(),
        "record_count": len(records),
        "status_counts": {status: sum(1 for r in records if r["status"] == status) for status in sorted({r["status"] for r in records})},
        "records": records,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")
    csv_path = path.with_suffix(".csv")
    with csv_path.open("w", newline="") as f:
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
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def main() -> int:
    parser = argparse.ArgumentParser(description="Subset ISMIP6 projected-grid stencils around configured points.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--group", default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument("--experiment", default=None)
    parser.add_argument("--radius", type=int, default=1, help="Grid-cell radius; 1 gives a 3x3 stencil.")
    parser.add_argument(
        "--variables",
        nargs="+",
        default=["ice_thickness"],
        help="Standard variable names to subset as stencils.",
    )
    parser.add_argument("--summary-name", default="ismip6_stencil_processing_summary")
    args = parser.parse_args()

    config_path = args.config.resolve()
    config = load_config(config_path)
    variables = config.get("variables") or DEFAULT_VARIABLES
    stencil_variables = {name: variables[name] for name in args.variables}
    points = parse_points(config, config_path=config_path)

    source_cfg = config["source"]
    dest_cfg = config["destination"]
    output_cfg = config.get("output", {})
    transfer_cfg = config.get("transfer", {})
    output_root = resolve_path(output_cfg.get("root", "../../output/ismip6"), config_path=config_path)
    point_set_name = str(config.get("subset", {}).get("point_set_name") or Path(config["subset"]["points_file"]).stem)
    overwrite = bool(output_cfg.get("overwrite", False))

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
        variables=stencil_variables,
    )
    inventory_path = output_root / "ismip6_stencil_inventory.json"
    write_inventory(inventory_path, collection_id=source_collection_id, runs=runs, variables=stencil_variables)

    selected_runs = select_runs(
        runs,
        smoke=False,
        smoke_count=None,
        max_runs=None,
        group=args.group,
        model=args.model,
        experiment=args.experiment,
    )

    local_stage_dir = resolve_path(dest_cfg["local_staging_dir"], config_path=config_path)
    local_stage_dir.mkdir(parents=True, exist_ok=True)
    remote_stage_dir = dest_cfg["collection_path"]
    timeout_seconds = int(transfer_cfg.get("task_timeout_seconds", 3600))
    poll_interval_seconds = int(transfer_cfg.get("poll_interval_seconds", 10))
    delete_after_extract = bool(transfer_cfg.get("delete_after_extract", True))

    records: list[dict] = []
    for run in selected_runs:
        for standard_variable, candidates in stencil_variables.items():
            file_info = run.files.get(standard_variable)
            stem = output_stem(run.group, run.model, run.experiment, standard_variable, point_set_name, args.radius)
            output_base = output_root / "stencil_subsets" / run.group / run.model / run.experiment
            output_netcdf = output_base / f"{stem}.nc"
            output_json = output_base / f"{stem}.json"
            if not file_info:
                records.append(
                    {
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
                )
                continue
            remote_path = run.path + file_info["filename"]
            if output_netcdf.exists() and output_json.exists() and not overwrite:
                records.append(
                    {
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
                )
                continue

            local_path = local_stage_dir / Path(remote_path).name
            local_path.unlink(missing_ok=True)
            try:
                submit_transfer_and_wait(
                    transfer_client,
                    source_collection_id,
                    destination_collection_id,
                    remote_path,
                    remote_stage_dir.rstrip("/") + "/" + local_path.name,
                    timeout_seconds,
                    poll_interval_seconds,
                )
                wait_seconds = 0
                while not local_path.exists():
                    if wait_seconds > 60:
                        raise RuntimeError(f"Transfer finished but local file did not appear: {local_path}")
                    time.sleep(1)
                    wait_seconds += 1

                extracted = extract_ismip6_stencils(
                    local_path,
                    candidates=candidates,
                    experiment=run.experiment,
                    points=points,
                    radius=args.radius,
                )
                variable_attrs = dict(extracted["variable_attrs"])
                variable_attrs["ismip6_standard_variable"] = standard_variable
                variable_attrs["ismip6_remote_variable"] = extracted["remote_variable"]
                source_files = [remote_path]
                write_stencil_netcdf(
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
                    stencil_x=extracted["stencil_x"],
                    stencil_y=extracted["stencil_y"],
                    time_values=extracted["time"],
                    values=extracted["values"],
                    variable_attrs=variable_attrs,
                    time_attrs=extracted["time_attrs"],
                    global_attrs=extracted["global_attrs"],
                    source_files=source_files,
                )
                write_stencil_json(output_json, variable_name=standard_variable, extracted=extracted, source_files=source_files)
                record = {
                    "status": "processed",
                    "group": run.group,
                    "model": run.model,
                    "experiment": run.experiment,
                    "standard_variable": standard_variable,
                    "remote_variable": extracted["remote_variable"],
                    "remote_path": remote_path,
                    "output_netcdf": str(output_netcdf),
                    "output_json": str(output_json),
                    "message": f"shape={tuple(extracted['values'].shape)}",
                }
            except Exception as exc:
                record = {
                    "status": "failed",
                    "group": run.group,
                    "model": run.model,
                    "experiment": run.experiment,
                    "standard_variable": standard_variable,
                    "remote_variable": file_info.get("remote_variable_hint"),
                    "remote_path": remote_path,
                    "output_netcdf": None,
                    "output_json": None,
                    "message": f"{type(exc).__name__}: {exc}",
                }
            finally:
                if delete_after_extract:
                    local_path.unlink(missing_ok=True)

            records.append(record)
            print(
                f"{record['status']}: {run.group}/{run.model}/{run.experiment} "
                f"{standard_variable} {record.get('message', '')}",
                flush=True,
            )

    write_summary(output_root / f"{args.summary_name}.json", records)
    print(f"Wrote summary: {output_root / f'{args.summary_name}.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
