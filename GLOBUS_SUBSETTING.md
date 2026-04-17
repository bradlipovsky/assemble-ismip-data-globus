# Globus Subsetting Workflow

This repository now includes a first-pass pure Python Globus pipeline:

- [globus_subset_pipeline.py](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_pipeline.py)
- [globus_subset_config.sample.json](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/globus_subset_config.sample.json)
- [points/temperature_points.json](/Users/bradlipovsky/Documents/assemble-ismip-data-globus/points/temperature_points.json)

## What it does

1. Opens a browser-based Globus login flow and caches refresh tokens locally
2. Resolves the source collection named `GHub-upload`
3. Lists `.nc` files under `/ISMIP7/AIS/CESM2-WACCM/ssp126/SDBN1/tas/v2/`
4. Transfers one source file at a time into a local Globus-connected staging folder
5. Converts configured latitude/longitude points onto the native ISMIP Antarctic polar stereographic grid (`EPSG:3031`)
6. Extracts the nearest-gridpoint values for those target locations
7. Writes one compact output NetCDF plus a JSON time-series export for the selected points
8. Deletes the large temporary file after extraction

The default point file currently targets these two locations:

- `82.375S, -168.626`
- `69.451S, 71.497E`

## What you still need to fill in

### 1. Local destination collection ID

Put your local destination collection ID into `destination.collection_id`.

You can usually find this in the Globus web app URL when you open your local collection, or from the collection details page.

### 2. Local destination path

Set:

- `destination.collection_path` to the path Globus should transfer into on that collection
- `destination.local_staging_dir` to the matching local filesystem path your Python process can read

For Globus Connect Personal, the simplest setup is to choose a staging folder you can see locally and also expose to Globus.

### 3. Extraction points

The config now supports either:

- `subset.points_file` pointing at a JSON file of locations
- inline `subset.points`

Each point can be provided either as:

- geographic `latitude` / `longitude`
- projected ISMIP `x` / `y` coordinates in meters

That makes it easy to start with a short point list now and later scale up to a longer JSON file of target sites.

## Running it

Create a real config file from the sample and then run:

```bash
.venv/bin/python globus_subset_pipeline.py --config your_config.json --list-only
```

That will:

- trigger browser login
- resolve the collection
- list the matching files

Then run the full workflow:

```bash
.venv/bin/python globus_subset_pipeline.py --config your_config.json
```

## Notes on credentials

- The script uses the Globus Python SDK `UserApp` login flow
- The browser login is interactive the first time
- Refresh tokens are requested so later runs should usually reuse cached credentials
- Some collections require extra collection-specific `data_access` consent; the script checks for that and refreshes login if needed

## About the client ID

The sample config uses Globus's tutorial native-app client ID:

`61338d24-54d5-408f-a10d-66c06b59f6d2`

That is useful for getting started locally. For a longer-lived personal workflow, it is better to register your own native app and replace the client ID in the config.
