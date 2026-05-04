# Globus Notes

Use [scripts/run_globus_subset.py](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/scripts/run_globus_subset.py) with a config from [configs/globus](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/configs/globus).

Typical commands:

```bash
python scripts/run_globus_subset.py --config configs/globus/globus_subset_config.local.json --list-only
python scripts/run_globus_subset.py --config configs/globus/globus_subset_config.local.json
```

Important IDs:
- `globus_app.client_id` is the OAuth/native-app client ID
- `destination.collection_id` is your local Globus collection ID

Important path behavior:
- config-relative paths are resolved relative to the config file
- local staging files are read from `destination.local_staging_dir`
- reduced outputs are written to the `output/` directory by the current configs

Useful configs:
- [configs/globus/globus_subset_config.sample.json](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/configs/globus/globus_subset_config.sample.json)
- [configs/globus/globus_subset_config.local.json](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/configs/globus/globus_subset_config.local.json)
- [configs/globus/globus_subset_config.local_files.sample.json](/home/bradlipovsky/notebooks/assemble-ismip-data-globus/configs/globus/globus_subset_config.local_files.sample.json)
