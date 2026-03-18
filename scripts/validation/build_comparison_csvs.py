"""
build_comparison_csvs.py
========================
Merge GESLA observations with POM model time series to produce one
comparison CSV per station, ready for skill assessment.

The script performs a **left outer join** on ``datetime_utc``:
  - Left  : GESLA observations (hourly; index = datetime_utc)
  - Right : model time series (hourly; index = datetime_utc)

Both datasets are expected to be at the **same 1-hour resolution** (GESLA
typically provides hourly data; the model output is 1-hourly).  No
resampling is applied by default.  Use ``--resample <freq>`` to resample
the GESLA data to a different frequency before merging.

Merge rules
-----------
* The merge is performed on *exact* UTC timestamps (no fuzzy tolerance).
  Any GESLA observation without a matching model time step will have NaN
  model columns; any model step without a GESLA observation is dropped.
* Missing ``sea_level_obs_m`` (null-valued or flagged) is kept as NaN —
  it is the user's responsibility to filter on ``gesla_qc_flag`` /
  ``gesla_use_flag`` in downstream analysis.

Output columns
--------------
datetime_utc, sea_level_obs_m, gesla_qc_flag, gesla_use_flag,
model_eta_tide_m, model_eta_notide_m, model_tide_minus_notide_m,
station_file_name, station_name, site_code, country,
station_lon, station_lat, model_lon, model_lat, grid_i, grid_j, distance_km

Usage
-----
    # Build comparison CSVs for all stations:
    python scripts/validation/build_comparison_csvs.py

    # Single station:
    python scripts/validation/build_comparison_csvs.py \\
        --station san_francisco_ca-551a-usa-uhslc

    # Restrict comparison to the 2013-2018 model period:
    python scripts/validation/build_comparison_csvs.py \\
        --t-start 2013-01-01 --t-end 2019-01-01

    # Resample GESLA to hourly (nearest) before merging:
    python scripts/validation/build_comparison_csvs.py --resample 1h

    # Overwrite existing outputs:
    python scripts/validation/build_comparison_csvs.py --force
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys

import pandas as pd
import numpy as np

_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    GESLA_OBS_DIR,
    VALIDATION_DIR,
    STATION_MODEL_INDEX,
    GESLA_VS_MODEL_DIR,
    SURGEMIP_STNLIST,
)
from utils.gesla import load_station_list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL_TS_DIR = VALIDATION_DIR / "model_ts"

# Columns that carry station/model metadata (constant per row) —
# we fill them forward if they are present in either source.
_META_COLS = [
    "station_file_name", "station_name", "site_code", "country",
    "station_lon", "station_lat",
    "model_lon", "model_lat", "grid_i", "grid_j", "distance_km",
]

# Desired final column order
_FINAL_COLS = [
    "datetime_utc",
    "sea_level_obs_m",
    "gesla_qc_flag",
    "gesla_use_flag",
    "model_eta_tide_m",
    "model_eta_notide_m",
    "model_tide_minus_notide_m",
    "station_file_name",
    "station_name",
    "site_code",
    "country",
    "station_lon",
    "station_lat",
    "model_lon",
    "model_lat",
    "grid_i",
    "grid_j",
    "distance_km",
]


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--station-list",
        default=str(SURGEMIP_STNLIST),
        help="Path to SurgeMIP_stnlist.csv (used only to enumerate stations).",
    )
    p.add_argument(
        "--obs-dir",
        default=str(GESLA_OBS_DIR),
        help=f"Directory with per-station GESLA observation CSVs. (default: {GESLA_OBS_DIR})",
    )
    p.add_argument(
        "--model-dir",
        default=str(MODEL_TS_DIR),
        help=f"Directory with per-station model time series CSVs. (default: {MODEL_TS_DIR})",
    )
    p.add_argument(
        "--out-dir",
        default=str(GESLA_VS_MODEL_DIR),
        help=f"Output directory for comparison CSVs. (default: {GESLA_VS_MODEL_DIR})",
    )
    p.add_argument(
        "--station",
        default=None,
        help="Process only this one station (by FILE NAME).",
    )
    p.add_argument(
        "--t-start",
        default=None,
        help="Clip time series to this start date (ISO 8601).",
    )
    p.add_argument(
        "--t-end",
        default=None,
        help="Clip time series to this end date, exclusive (ISO 8601).",
    )
    p.add_argument(
        "--resample",
        default=None,
        metavar="FREQ",
        help=(
            "Resample GESLA observations to FREQ (e.g. '1h') before merging. "
            "Uses mean aggregation. Default: no resampling."
        ),
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output files.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Single-station merge
# ---------------------------------------------------------------------------

def merge_one_station(
    file_name: str,
    obs_dir: pathlib.Path,
    model_dir: pathlib.Path,
    out_dir: pathlib.Path,
    t_start: str | None,
    t_end: str | None,
    resample_freq: str | None,
    force: bool,
) -> str:
    """
    Merge GESLA obs + model for one station and save the result.

    Returns
    -------
    str : "written", "skipped", "obs_missing", "model_missing", "error"
    """
    out_file = out_dir / f"{file_name}.csv.gz"

    if out_file.exists() and not force:
        logger.debug("  SKIP (exists): %s", file_name)
        return "skipped"

    obs_file   = obs_dir   / f"{file_name}.csv.gz"
    model_file = model_dir / f"{file_name}.csv.gz"

    if not obs_file.exists():
        logger.debug("  OBS MISSING: %s", file_name)
        return "obs_missing"

    if not model_file.exists():
        logger.debug("  MODEL MISSING: %s", file_name)
        return "model_missing"

    try:
        obs   = pd.read_csv(obs_file,   compression="gzip", parse_dates=["datetime_utc"])
        model = pd.read_csv(model_file, compression="gzip", parse_dates=["datetime_utc"])
    except Exception as exc:
        logger.error("  READ ERROR for %s: %s", file_name, exc)
        return "error"

    # ---- Optional time clipping -------------------------------------------
    if t_start:
        ts = pd.Timestamp(t_start)
        obs   = obs[obs["datetime_utc"] >= ts]
        model = model[model["datetime_utc"] >= ts]
    if t_end:
        te = pd.Timestamp(t_end)
        obs   = obs[obs["datetime_utc"] < te]
        model = model[model["datetime_utc"] < te]

    if obs.empty:
        logger.debug("  OBS EMPTY after clip: %s", file_name)
        return "error"
    if model.empty:
        logger.debug("  MODEL EMPTY after clip: %s", file_name)
        return "error"

    # ---- Optional GESLA resampling ----------------------------------------
    if resample_freq:
        obs = obs.set_index("datetime_utc")
        numeric_cols = obs.select_dtypes(include="number").columns.tolist()
        obs_num  = obs[numeric_cols].resample(resample_freq).mean()
        # Carry forward string/bool metadata columns
        meta_obs = obs[[c for c in _META_COLS if c in obs.columns]].resample(resample_freq).first()
        obs = pd.concat([obs_num, meta_obs], axis=1).reset_index()

    # ---- Merge on datetime_utc (left = obs) --------------------------------
    obs_cols   = ["datetime_utc", "sea_level_obs_m", "gesla_qc_flag", "gesla_use_flag"]
    # Include any obs meta cols not already in model
    obs_extra  = [c for c in obs.columns if c in _META_COLS and c not in obs_cols]
    obs_cols  += obs_extra

    model_cols = [
        "datetime_utc",
        "model_eta_tide_m", "model_eta_notide_m", "model_tide_minus_notide_m",
    ] + [c for c in _META_COLS if c in model.columns]

    obs_sub   = obs[[c for c in obs_cols   if c in obs.columns]].copy()
    model_sub = model[[c for c in model_cols if c in model.columns]].copy()

    merged = pd.merge(obs_sub, model_sub, on="datetime_utc", how="left", suffixes=("", "_model"))

    # Resolve duplicate metadata columns (prefer obs version if both exist)
    for col in _META_COLS:
        col_dup = col + "_model"
        if col_dup in merged.columns:
            if col not in merged.columns:
                merged.rename(columns={col_dup: col}, inplace=True)
            else:
                merged.drop(columns=[col_dup], inplace=True)

    # ---- Column ordering ---------------------------------------------------
    final_cols = [c for c in _FINAL_COLS if c in merged.columns]
    extra_cols = [c for c in merged.columns if c not in final_cols]
    merged = merged[final_cols + extra_cols]

    # ---- Save --------------------------------------------------------------
    out_dir.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_file, index=False, compression="gzip")
    logger.debug(
        "  WRITTEN: %s  (%d rows, %d obs with model match)",
        file_name, len(merged),
        int(merged["model_eta_tide_m"].notna().sum()) if "model_eta_tide_m" in merged else 0,
    )
    return "written"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    obs_dir   = pathlib.Path(args.obs_dir)
    model_dir = pathlib.Path(args.model_dir)
    out_dir   = pathlib.Path(args.out_dir)

    # ---- Determine which stations to process --------------------------------
    # Use the union of obs and model directories (not limited to station list)
    # f.stem on "foo.csv.gz" returns "foo.csv" — use with_suffix("").stem to strip both
    obs_files   = {f.with_suffix("").stem: f for f in obs_dir.glob("*.csv.gz")}   if obs_dir.exists()   else {}
    model_files = {f.with_suffix("").stem: f for f in model_dir.glob("*.csv.gz")} if model_dir.exists() else {}

    if args.station:
        file_names = [args.station]
    else:
        # All stations that have at least an obs file
        file_names = sorted(obs_files.keys())

    if not file_names:
        logger.warning(
            "No observation files found in %s.\n"
            "  Run prepare_gesla.py first.",
            obs_dir,
        )
        sys.exit(0)

    logger.info(
        "Merging %d stations  |  obs_dir=%s  model_dir=%s",
        len(file_names), obs_dir, model_dir,
    )

    # ---- Process ------------------------------------------------------------
    counts = {
        "written": 0, "skipped": 0,
        "obs_missing": 0, "model_missing": 0, "error": 0,
    }
    total = len(file_names)

    for idx, file_name in enumerate(file_names, 1):
        status = merge_one_station(
            file_name    = file_name,
            obs_dir      = obs_dir,
            model_dir    = model_dir,
            out_dir      = out_dir,
            t_start      = args.t_start,
            t_end        = args.t_end,
            resample_freq= args.resample,
            force        = args.force,
        )
        counts[status] += 1

        if idx % 50 == 0 or idx == total:
            logger.info(
                "  Progress: %d/%d  written=%d  skipped=%d  "
                "obs_missing=%d  model_missing=%d  errors=%d",
                idx, total,
                counts["written"], counts["skipped"],
                counts["obs_missing"], counts["model_missing"], counts["error"],
            )

    # ---- Summary ------------------------------------------------------------
    logger.info(
        "\nDone.\n"
        "  Written        : %d\n"
        "  Skipped        : %d (already existed)\n"
        "  Obs missing    : %d\n"
        "  Model missing  : %d\n"
        "  Errors         : %d\n"
        "  Output         : %s",
        counts["written"], counts["skipped"],
        counts["obs_missing"], counts["model_missing"], counts["error"],
        out_dir,
    )


if __name__ == "__main__":
    main()
