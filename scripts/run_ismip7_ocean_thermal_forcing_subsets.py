from __future__ import annotations

from pathlib import Path

from subsetting.auxiliary import run_auxiliary_subset


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "globus" / "globus_subset_config.local.json"
OUTPUT_ROOT = ROOT / "output"


REMOTE_FILES = {
    "thetao": [
        "/ISMIP7/AIS/meltMIP/OI_Climatology_ismip8km_60m_thetao_extrap.nc",
        "/ISMIP7/AIS/meltMIP/thetao_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2079-2099.nc",
        "/ISMIP7/AIS/meltMIP/thetao_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2179-2199.nc",
        "/ISMIP7/AIS/meltMIP/thetao_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2279-2299.nc",
    ],
    "tf": [
        "/ISMIP7/AIS/meltMIP/OI_Climatology_ismip8km_60m_tf_extrap.nc",
        "/ISMIP7/AIS/meltMIP/tf_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2079-2099.nc",
        "/ISMIP7/AIS/meltMIP/tf_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2179-2199.nc",
        "/ISMIP7/AIS/meltMIP/tf_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2279-2299.nc",
    ],
    "so": [
        "/ISMIP7/AIS/meltMIP/OI_Climatology_ismip8km_60m_so_extrap.nc",
        "/ISMIP7/AIS/meltMIP/so_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2079-2099.nc",
        "/ISMIP7/AIS/meltMIP/so_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2179-2199.nc",
        "/ISMIP7/AIS/meltMIP/so_Oyr_CESM2-WACCM_ssp585_r1i1p1f1_ismip8km_60m_2279-2299.nc",
    ],
}


def main() -> int:
    for variable, remote_files in REMOTE_FILES.items():
        output_stem = f"ismip7_cesm2_waccm_ocean_{variable}_point_subset"
        print(f"Running ISMIP7 ocean subset for {variable} ...")
        run_auxiliary_subset(
            config_path=CONFIG_PATH,
            remote_files=remote_files,
            variable_name=variable,
            output_netcdf=OUTPUT_ROOT / f"{output_stem}.nc",
            output_json=OUTPUT_ROOT / f"{output_stem}.json",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
