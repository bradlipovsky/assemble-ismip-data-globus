from __future__ import annotations

import json
from pathlib import Path

from subsetting.auxiliary import run_auxiliary_subset
from subsetting.pipeline import run_pipeline


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "globus" / "globus_subset_config.local.json"
TMP_DIR = ROOT / ".codex-generated-configs"
TMP_DIR.mkdir(parents=True, exist_ok=True)


def write_config(base_config: dict, *, variable: str, source_path: str, output_stem: str) -> Path:
    config = json.loads(json.dumps(base_config))
    config["source"]["path"] = source_path
    config["subset"]["variable"] = variable
    if config["subset"].get("points_file"):
        config["subset"]["points_file"] = str((CONFIG_PATH.parent / config["subset"]["points_file"]).resolve())
    config["output"]["path"] = str((ROOT / "output" / f"{output_stem}.nc").resolve())
    config["output"]["json_path"] = str((ROOT / "output" / f"{output_stem}.json").resolve())
    config["output"]["overwrite"] = True
    config["transfer"]["limit_files"] = None
    path = TMP_DIR / f"{output_stem}.json"
    path.write_text(json.dumps(config, indent=2) + "\n")
    return path


def main() -> int:
    base_config = json.loads(CONFIG_PATH.read_text())

    surface_jobs = [
        {
            "variable": "acabf",
            "source_path": "/ISMIP7/AIS/CESM2-WACCM/ssp126/SDBN1/acabf/v2/",
            "output_stem": "ismip7_acabf_point_subset_full",
        },
    ]

    for job in surface_jobs:
        cfg_path = write_config(base_config, **job)
        print(f"Running full subset for {job['variable']} ...")
        run_pipeline(cfg_path)

    auxiliary_jobs = [
        {
            "variable": "so",
            "remote_files": [
                "/ISMIP7/AIS/meltMIP/so_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2079-2099.nc",
                "/ISMIP7/AIS/meltMIP/so_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2179-2199.nc",
                "/ISMIP7/AIS/meltMIP/so_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2279-2299.nc",
            ],
            "netcdf": ROOT / "output" / "ismip7_so_point_subset.nc",
            "json": ROOT / "output" / "ismip7_so_point_subset.json",
        },
        {
            "variable": "thetao",
            "remote_files": [
                "/ISMIP7/AIS/meltMIP/thetao_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2079-2099.nc",
                "/ISMIP7/AIS/meltMIP/thetao_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2179-2199.nc",
                "/ISMIP7/AIS/meltMIP/thetao_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2279-2299.nc",
            ],
            "netcdf": ROOT / "output" / "ismip7_thetao_point_subset.nc",
            "json": ROOT / "output" / "ismip7_thetao_point_subset.json",
        },
        {
            "variable": "tf",
            "remote_files": [
                "/ISMIP7/AIS/meltMIP/tf_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2079-2099.nc",
                "/ISMIP7/AIS/meltMIP/tf_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2179-2199.nc",
                "/ISMIP7/AIS/meltMIP/tf_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2279-2299.nc",
            ],
            "netcdf": ROOT / "output" / "ismip7_tf_point_subset.nc",
            "json": ROOT / "output" / "ismip7_tf_point_subset.json",
        },
        {
            "variable": "melt_rate",
            "remote_files": [
                "/ISMIP7/AIS/parameterisations/ocean/ocean_modelling_data/Mathiot_NEMO_cold_m.nc",
                "/ISMIP7/AIS/parameterisations/ocean/ocean_modelling_data/Mathiot_NEMO_warm_m.nc",
                "/ISMIP7/AIS/parameterisations/ocean/ocean_modelling_data/Naughten_FESOM_ACCESS_cold_m.nc",
                "/ISMIP7/AIS/parameterisations/ocean/ocean_modelling_data/Naughten_FESOM_ACCESS_warm_m.nc",
                "/ISMIP7/AIS/parameterisations/ocean/ocean_modelling_data/Naughten_FESOM_MMM_cold_m.nc",
                "/ISMIP7/AIS/parameterisations/ocean/ocean_modelling_data/Naughten_FESOM_MMM_warm_m.nc",
                "/ISMIP7/AIS/parameterisations/ocean/ocean_modelling_data/Timmermann_FESOM_cold_m.nc",
                "/ISMIP7/AIS/parameterisations/ocean/ocean_modelling_data/Timmermann_FESOM_warm_m.nc",
            ],
            "netcdf": ROOT / "output" / "ismip7_melt_rate_point_subset.nc",
            "json": ROOT / "output" / "ismip7_melt_rate_point_subset.json",
        },
    ]

    for job in auxiliary_jobs:
        print(f"Running auxiliary subset for {job['variable']} ...")
        run_auxiliary_subset(
            config_path=CONFIG_PATH,
            remote_files=job["remote_files"],
            variable_name=job["variable"],
            output_netcdf=job["netcdf"],
            output_json=job["json"],
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
