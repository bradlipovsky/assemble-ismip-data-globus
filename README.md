# assemble-ismip-data-globus

Utilities for subsetting large ISMIP/ISMIP7 NetCDF forcing products down to a small set of target points.

The current workflow supports:
- atmospheric forcing point time series such as `tas` and `acabf`
- auxiliary ocean/profile products such as `thetao`, `tf`, `so`, and `melt_rate`
- Globus transfer mode for remote source files
- local-file mode for smoke tests on already-downloaded files

## Layout

- [subsetting](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/subsetting): reusable Python package with the pipeline and auxiliary extractors
- [scripts](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/scripts): runnable entry points and notebook generators
- [configs/globus](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/configs/globus): Globus and local-file config templates
- [configs/points](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/configs/points): point definition files
- [notebooks](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/notebooks): inspection notebooks for smoke tests and full outputs
- [output](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/output): reduced NetCDF and JSON products
- [figures](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/figures): exported plots
- [scratch](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/scratch): temporary development artifacts

## Main Commands

List matching files without transferring:

```bash
python scripts/run_globus_subset.py --config configs/globus/globus_subset_config.local.json --list-only
```

Run the main point-subsetting pipeline:

```bash
python scripts/run_globus_subset.py --config configs/globus/globus_subset_config.local.json
```

Run the additional requested subsets (`acabf`, `so`, `thetao`, `tf`, `melt_rate`):

```bash
python scripts/run_requested_subsets.py
```

Generate a generic inspection notebook for an output file:

```bash
python scripts/make_generic_subset_notebook.py \
  output/ismip7_so_point_subset.nc \
  notebooks/ismip7_so_point_subset_check.ipynb \
  --title "ISMIP7 Salinity Check"
```

## Config Notes

The main config sections are:
- `globus_app`
- `source`
- `destination`
- `subset`
- `output`
- `transfer`

Config paths are now resolved relative to the config file itself, not the current shell directory. That means these work reliably from the `configs/globus` directory layout:
- `subset.points_file`
- `source.local_dir`
- `source.local_files`
- `destination.local_staging_dir`
- `output.path`
- `output.json_path`

The default point file is:
- [configs/points/temperature_points.json](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/configs/points/temperature_points.json)

The main local config is:
- [configs/globus/globus_subset_config.local.json](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/configs/globus/globus_subset_config.local.json)

## Outputs Already Produced

- `tas` full run: [output/ismip7_tas_point_subset_full.nc](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/output/ismip7_tas_point_subset_full.nc)
- `acabf` full run: [output/ismip7_acabf_point_subset_full.nc](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/output/ismip7_acabf_point_subset_full.nc)
- `thetao`: [output/ismip7_thetao_point_subset.nc](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/output/ismip7_thetao_point_subset.nc)
- `tf`: [output/ismip7_tf_point_subset.nc](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/output/ismip7_tf_point_subset.nc)
- `so`: [output/ismip7_so_point_subset.nc](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/output/ismip7_so_point_subset.nc)
- `melt_rate`: [output/ismip7_melt_rate_point_subset.nc](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/output/ismip7_melt_rate_point_subset.nc)

Inspection notebooks live alongside them in [notebooks](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/notebooks).

## Variable Notes

- `tas`: near-surface air temperature
- `acabf`: surface mass balance flux
- `thetao`: ocean potential temperature
- `tf`: thermal forcing
- `so`: ocean salinity
- `melt_rate`: annual-mean basal-melt-style product from the ocean parameterisation branch

For the processed ocean products, `thetao`, `tf`, and `so` are discrete sample periods rather than monthly time series. Their `z` coordinate is plotted as negative downward in the notebooks.

## Reference Links

- ISMIP7 protocol: https://www.ismip.org/research/ismip7-protocol
- AIS basal melt: https://www.ismip.org/participants/focus-groups/ais-basal-melt
- Surface mass balance: https://www.ismip.org/participants/focus-groups/surface-mass-balance
