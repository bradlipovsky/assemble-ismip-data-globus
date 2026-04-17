# assemble-ismip-data-globus

This repository contains a Python pipeline for pulling large ISMIP7 NetCDF forcing files from Globus, extracting a very small subset of data, saving the reduced results locally, and deleting the large staged source files afterward.

The current implemented use case is:

- variable: `tas`
- extraction target: time series at a set of latitude/longitude points over Antarctica
- source format: large monthly NetCDF files on a Globus collection
- reduced outputs: one compact NetCDF file and one JSON file

This README is written as a handoff for another LLM or engineer who needs to continue the work, not as a polished GitHub landing page.

## Current Status

The main pipeline is implemented in [globus_subset_pipeline.py](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_pipeline.py).

What works now:

- authenticates to Globus via the Python SDK
- resolves a source collection by name or uses an explicit collection ID
- lists matching remote NetCDF files
- transfers one file at a time into a local Globus-connected staging directory
- opens the staged file locally
- converts requested geographic lat/lon points to the ISMIP projected grid (`EPSG:3031`)
- extracts nearest-gridpoint values for the configured variable
- concatenates results across all processed files in time order
- writes a reduced NetCDF output
- writes a reduced JSON output
- deletes each staged large file after extraction if configured

What is not yet implemented:

- interpolation beyond nearest-gridpoint extraction
- support for multiple variables in one run
- chunked or streaming extraction for more complex point/bounding-box products
- a formal schema/versioning system for the JSON outputs
- automated tests
- environment/dependency locking in this repo

## Repository Files

- [globus_subset_pipeline.py](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_pipeline.py): main implementation
- [globus_subset_config.json](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_config.json): current working config used locally
- [globus_subset_config.sample.json](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_config.sample.json): template config
- [points/temperature_points.json](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/points/temperature_points.json): current point list
- [GLOBUS_SUBSETTING.md](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/GLOBUS_SUBSETTING.md): shorter operational notes
- [output](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/output): reduced outputs land here by default

## Problem the Pipeline Solves

The forcing files are large, roughly hundreds of MB each. The actual downstream need is much smaller: only one physical quantity at a small set of locations. Downloading and keeping the full files locally is wasteful.

The intended workflow is:

1. Find remote NetCDF files on a Globus collection.
2. Transfer exactly one file at a time to a local staging folder.
3. Open that file locally.
4. Extract only the needed values.
5. Append those extracted values to a compact local product.
6. Delete the staged large file.

That pattern is already implemented.

## Data Assumptions

The current code assumes the source NetCDF files look like the sample `tas` files already inspected during development:

- the target variable exists, currently usually `tas`
- the file contains `x`, `y`, and `time` datasets
- the grid is Antarctic polar stereographic
- the file metadata indicates `EPSG:3031`
- the variable is on dimensions `(time, y, x)`
- missing data uses a large fill value rather than `NaN`

The inspected file characteristics that informed the current implementation were:

- `tas.shape == (12, 3041, 3041)`
- `x` and `y` are 1D projected coordinates in meters
- `time.units == "days since 1850-01-01 00:00:00"`
- the grid metadata says Antarctic polar stereographic south, EPSG3031

If future source files differ materially from those assumptions, the extraction code may need to branch by dataset type.

## Coordinate Handling

This is one of the main recent improvements.

Originally the code only supported directly specifying projected `x`/`y` coordinates in meters. That was not convenient because the user naturally wanted to specify latitude/longitude points.

The pipeline now supports both:

- geographic coordinates: `latitude` and `longitude`
- projected coordinates: `x` and `y`

Implementation details:

- geographic CRS is treated as `EPSG:4326`
- source grid CRS is treated as `EPSG:3031`
- conversion uses `pyproj.Transformer`
- nearest-gridpoint extraction is done in projected coordinates against the file’s `x` and `y` axes

The important implication is that geographic points are never matched directly in lat/lon space. They are first projected into the file’s native grid, then matched to the nearest `x`/`y` indices.

## Point Input Format

Point definitions can come from:

- `subset.points_file`
- inline `subset.points`

Both sources are supported in the same run. The implementation simply appends both lists together.

### Supported Point Fields

Each point must define either:

- `latitude` and `longitude`

or:

- `x` and `y`

Optional field:

- `name`

If `name` is omitted, the pipeline generates `point_1`, `point_2`, etc.

### Latitude/Longitude Parsing

The parser accepts numeric values or strings.

Examples that should work:

- `"82.375S"`
- `"71.497E"`
- `-168.626`
- `69.451`

Important parsing behavior:

- hemisphere suffixes `N`, `S`, `E`, `W` are supported
- explicit negative signs are respected
- longitudes are normalized into `[-180, 180]`

One subtle but important case:

- a value written as `-168.626E` is interpreted as `-168.626`, because the explicit negative sign wins

If you intend east-positive longitude there, use `168.626E` instead.

## Current Default Points

The current default point file is [points/temperature_points.json](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/points/temperature_points.json).

It currently contains two requested locations:

- `82.375S, -168.626`
- `69.451S, 71.497E`

Those are converted internally to projected coordinates before extraction.

## Config Structure

The main config file is JSON. The current local version is [globus_subset_config.json](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_config.json).

Top-level sections:

- `globus_app`
- `source`
- `destination`
- `subset`
- `output`
- `transfer`

### `globus_app`

Controls the Globus native app settings.

Fields:

- `app_name`
- `client_id`

Current default client ID:

- `61338d24-54d5-408f-a10d-66c06b59f6d2`

That is the Globus tutorial native-app client ID. It is acceptable for local experimentation, but a more durable workflow should register a dedicated app.

### `source`

Defines where the remote NetCDF files live.

Fields:

- `collection_name`
- `collection_id`
- `path`
- `glob_pattern`

Behavior:

- if `collection_id` is present, it is used directly
- otherwise the pipeline searches Globus for `collection_name`
- `glob_pattern` filters files after listing the remote directory

Current configured source path:

- `/ISMIP7/AIS/CESM2-WACCM/ssp126/SDBN1/tas/v2/`

### `destination`

Defines where Globus should transfer remote files before Python opens them locally.

Fields:

- `collection_id`
- `collection_path`
- `local_staging_dir`

This is an important concept:

- `collection_path` is the path as Globus sees it on the destination collection
- `local_staging_dir` is the path as the local Python process sees it on disk

Those must point to the same physical location.

### `subset`

Defines what to extract.

Fields currently used:

- `variable`
- `points_file`
- `points`

Current behavior:

- `variable` defaults to `"tas"` if omitted
- `points_file` is optional
- `points` is optional
- at least one point must be provided via one or both sources

### `output`

Defines where reduced products go.

Fields:

- `path`
- `json_path`
- `overwrite`

Behavior:

- if `path` is set, the pipeline writes reduced NetCDF
- if `json_path` is set, the pipeline writes JSON
- at least one of `path` or `json_path` must be configured
- if the file exists and `overwrite` is `false`, the run fails

### `transfer`

Controls operational behavior.

Fields:

- `poll_interval_seconds`
- `task_timeout_seconds`
- `delete_after_extract`
- `limit_files`

Behavior:

- `delete_after_extract=true` is what enforces the “do not keep the huge files” workflow
- `limit_files` is useful for smoke testing on a small subset of remote files

## Execution Flow

High-level execution order inside [globus_subset_pipeline.py](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_pipeline.py):

1. Load config JSON.
2. Parse points from `subset.points_file` and/or `subset.points`.
3. Build a Globus user app and transfer client.
4. Resolve source collection ID.
5. Optionally refresh login with extra data-access scopes if required by mapped collections.
6. List matching remote files.
7. For each file:
8. Submit a Globus transfer into the local staging destination.
9. Wait for task completion.
10. Wait briefly for the staged file to appear on the local filesystem.
11. Open the file with `h5py`.
12. Compute nearest indices in `x` and `y`.
13. Extract `var[:, y_idx, x_idx]` for each requested point.
14. Replace fill values with `NaN`.
15. Append this chunk to the in-memory combined result.
16. Delete the staged large file if configured.
17. After all files are processed, concatenate and sort by time.
18. Fail if duplicate time values are found.
19. Write reduced NetCDF and/or JSON outputs.

## Output Files

The current pipeline can write two reduced products.

### Reduced NetCDF

This is intended to be compact and analysis-friendly while keeping some metadata from the source files.

It contains:

- dimension `time`
- dimension `point`
- variable `time`
- variable `point_name`
- requested coordinates:
- `requested_latitude`
- `requested_longitude`
- `requested_x`
- `requested_y`
- actual matched coordinates:
- `latitude`
- `longitude`
- `x`
- `y`
- extracted data variable, for example `tas`

The main extracted variable has dimensions:

- `(time, point)`

Global attributes include:

- copied source metadata where possible
- `subset_method`
- `subset_grid_crs`
- `subset_input_crs`
- `subset_source_files`

### Reduced JSON

This exists mainly to make the reduced product easy to inspect, easy to pass into another tool, and easy to extend toward a “many points from a JSON file” workflow.

The JSON contains:

- `variable`
- `subset_method`
- `projected_grid_crs`
- `input_coordinate_crs`
- `source_files`
- `time`
- `variable_attrs`
- `global_attrs`
- `points`

Each point entry contains:

- `name`
- `requested`
- `actual`
- `values`

The `time` object includes:

- raw numeric time values
- copied time metadata
- `iso8601` timestamps when the units can be parsed

At the moment, ISO timestamps are only generated if time units match the implemented `"<unit> since <base>"` parser and the unit is `days`.

## Dependencies

The code currently imports:

- `globus_sdk`
- `h5py`
- `numpy`
- `pyproj`
- `h5netcdf`

Notes:

- `h5netcdf` is required only for writing the reduced NetCDF output
- if `h5netcdf` is unavailable, JSON output can still be a useful fallback if `output.path` is omitted

This repo does not yet include:

- `requirements.txt`
- `pyproject.toml`
- `environment.yml`

That is a good next improvement for reproducibility.

## Running the Pipeline

Typical usage:

```bash
python globus_subset_pipeline.py --config globus_subset_config.json --list-only
```

This should:

- trigger browser-based Globus login if needed
- resolve the source collection
- list matching files
- print the configured target points

Then run the full extraction:

```bash
python globus_subset_pipeline.py --config globus_subset_config.json
```

## Operational Notes

### Browser Login

The Globus flow is interactive the first time. The code requests refresh tokens, so later runs should usually be smoother.

### Data Access Consent

The pipeline includes logic for mapped collections that require extra collection-specific `data_access` consent scopes. If Globus indicates those are needed, the app refreshes login with the extra scopes.

### Local Staging Semantics

The transfer step and local file-open step are decoupled:

- Globus transfers into `destination.collection_path`
- the Python process reads from `destination.local_staging_dir`

If those are not aligned to the same folder, transfer may succeed but the file will never appear where Python is waiting for it.

### Deleting Large Files

The big temporary file is deleted only after successful extraction from that file in the current loop iteration.

This behavior is controlled by:

- `transfer.delete_after_extract`

If debugging requires inspecting full local files, set that to `false`.

## Known Limitations and Risks

### 1. Duplicate Time Values Cause a Hard Failure

After concatenating all extracted chunks, the pipeline sorts by time and explicitly fails if duplicate time values exist. This is conservative and reasonable for now, but future workflows may want a more flexible merge strategy.

### 2. Only Nearest-Gridpoint Extraction Is Implemented

No interpolation is done. For some use cases that is correct; for others, bilinear or conservative interpolation may be preferable.

### 3. Assumes 1D `x` and `y`

The code assumes coordinate axes are 1D and indexable independently. If a future dataset uses 2D curvilinear coordinates, this will need a different nearest-neighbor strategy.

### 4. No Formal Validation of Output Schema

The JSON format is pragmatic, not yet versioned. If other tools or agents start depending on it, add a schema version field.

### 5. No Tests Yet

The current work is functional implementation plus manual verification, not a tested package.

### 6. Environment Is Not Captured

The repo does not yet pin package versions. Another machine may need some setup work before the pipeline runs cleanly.

## Recommended Next Steps

If another LLM picks this up, these are the highest-value next improvements.

### Near-term

- add `requirements.txt` or `pyproject.toml`
- add a small test fixture and unit tests for point parsing and time formatting
- add a dry-run mode that exercises point parsing and config validation without Globus login
- add an option to write only JSON for minimal environments
- document the JSON output structure more formally

### Medium-term

- support multiple variables in one run
- support a separate manifest of requested variables and points
- add CSV output for easier inspection in spreadsheets or pandas
- add optional interpolation methods
- add provenance/version fields to outputs

### Longer-term

- support spatial subsets beyond points
- support remote catalogs with heterogeneous file naming
- build restart/checkpoint logic for long multi-file runs
- support parallel extraction once correctness is well covered

## Suggested Extension Points in Code

If modifying [globus_subset_pipeline.py](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_pipeline.py), these functions are the main places to extend:

- `parse_points`: extend accepted point manifests or validation rules
- `parse_point_definition`: add new coordinate conventions
- `extract_points_from_file`: change extraction method or support multiple variables
- `write_output_netcdf`: change reduced NetCDF schema
- `write_output_json`: change JSON schema
- `run_pipeline`: add orchestration features like checkpointing or batching

## Minimal Mental Model for a Future LLM

If you are another LLM continuing this repo, the most important thing to know is:

- the hard part of the workflow is not the data extraction math, it is the operational pattern of transferring one large file at a time from Globus, extracting a tiny subset, and deleting the staged file
- geographic points are now first-class inputs
- extraction is performed in the file’s projected grid, not directly in lat/lon space
- JSON point manifests are now part of the intended long-term interface
- the current code is a good base for scaling from “a few points” to “many points in a file,” but it is not yet a generalized subsetting framework

## Related Notes

See also [GLOBUS_SUBSETTING.md](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/GLOBUS_SUBSETTING.md) for the shorter operational summary.
