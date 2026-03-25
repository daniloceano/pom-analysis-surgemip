"""
prepare_site_data.py
====================
Prepare data files for the interactive validation website.

Reads outputs produced by the validation pipeline and generates
lightweight JSON/GeoJSON files for static serving via Next.js / Vercel.

Outputs (written to site/public/data/)
---------------------------------------
  station_metrics.json   – unified per-station metrics for all modes
                           (used to populate the map and summary cards)
  ts/<mode>/<stn>.json   – per-station daily-mean time series (raw_tide, godin_notide, fes2022_notide)
                           (lazy-loaded when user clicks a station)

Station metrics JSON structure
------------------------------
  {
    "stations": [
      {
        "id": "santos-540a-bra-uhslc",
        "name": "Santos",
        "site_code": "540A",
        "country": "BRA",
        "lon": -46.30,
        "lat": -23.97,
        "model_lon": ...,
        "model_lat": ...,
        "distance_km": ...,
        "metrics": {
          "raw_tide": {
            "n_valid": 1234,
            "rmse_notide": 0.05,
            "bias_notide": 0.01,
            "pearson_r_notide": 0.95,
            "rmse_tide": 0.20,
            "bias_tide": -0.01,
            "pearson_r_tide": 0.98,
            "obs_mean_m": 0.12,
            "obs_max_m": 0.80
          },
          "godin_notide": { ... },
          "fes2022_notide": { ... }
        }
      },
      ...
    ]
  }

Time series JSON structure
--------------------------
  {
    "station_id": "santos-540a-bra-uhslc",
    "mode": "raw_tide",
    "dates": ["2013-01-01", ...],
    "obs": [0.12, ...],
    "notide": [0.05, ...],
    "tide": [0.15, ...]       <- only in raw_tide mode
  }

Usage
-----
    python scripts/pipeline/prepare_site_data.py
    python scripts/pipeline/prepare_site_data.py --force
    python scripts/pipeline/prepare_site_data.py --station santos-540a-bra-uhslc
"""
from __future__ import annotations

import argparse
import json
import logging
import pathlib
import sys

import numpy as np
import pandas as pd

_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    STATION_METRICS_CSV,
    STATION_METRICS_GODIN_CSV,
    STATION_METRICS_FES_CSV,
    GESLA_VS_MODEL_DIR,
    VALID_GODIN_DIR,
    VALID_FES_DIR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Output directory (Next.js public data folder)
SITE_DATA_DIR = _ROOT / "site" / "public" / "data"

# Model period (only serve this range in the time series)
MODEL_T_START = "2013-01-01"
MODEL_T_END   = "2019-01-01"

# Metric columns to include in station_metrics.json per mode
_METRIC_COLS_RAW = [
    "n_valid",
    "obs_mean_m", "obs_std_m", "obs_max_m",
    "model_notide_mean_m", "model_notide_std_m", "model_notide_max_m",
    "rmse_notide", "bias_notide", "pearson_r_notide",
    "model_tide_mean_m", "model_tide_std_m", "model_tide_max_m",
    "rmse_tide", "bias_tide", "pearson_r_tide",
]
_METRIC_COLS_DETIDED = [
    "n_valid",
    "obs_mean_m", "obs_std_m", "obs_max_m",
    "model_notide_mean_m", "model_notide_std_m", "model_notide_max_m",
    "rmse_notide", "bias_notide", "pearson_r_notide",
]

# ------------------------------------------------------------------
# Argument parsing
# ------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--station", default=None,
                   help="Process only one station (useful for testing).")
    p.add_argument("--force", action="store_true",
                   help="Overwrite existing output files.")
    p.add_argument("--skip-ts", action="store_true",
                   help="Skip time series JSON generation (only write metrics).")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _safe_float(val) -> float | None:
    """Convert numpy/pandas float to Python float; return None for NaN."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, 6)
    except (ValueError, TypeError):
        return None


def _load_metrics_df(csv_path: pathlib.Path) -> pd.DataFrame | None:
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    return df.set_index("station_file_name")


def _metrics_row_to_dict(row: pd.Series, cols: list[str]) -> dict:
    return {c: _safe_float(row.get(c)) for c in cols}


# ------------------------------------------------------------------
# Station metrics JSON
# ------------------------------------------------------------------

def build_station_metrics_json(
    raw_df: pd.DataFrame | None,
    godin_df: pd.DataFrame | None,
    fes_df: pd.DataFrame | None,
    station_filter: str | None,
) -> list[dict]:
    """Build unified station list with metrics for all available modes."""

    # Use the raw metrics as the primary source for station coordinates
    primary_df = raw_df if raw_df is not None else godin_df if godin_df is not None else fes_df
    if primary_df is None:
        logger.error("No metrics CSVs found — nothing to build.")
        return []

    stations = []
    ids = [station_filter] if station_filter else list(primary_df.index)

    for stn_id in ids:
        if stn_id not in primary_df.index:
            continue
        row = primary_df.loc[stn_id]

        stn = {
            "id":          str(stn_id),
            "name":        str(row.get("station_name", stn_id)),
            "site_code":   str(row.get("site_code", "")),
            "country":     str(row.get("country", "")),
            "lon":         _safe_float(row.get("station_lon")),
            "lat":         _safe_float(row.get("station_lat")),
            "model_lon":   _safe_float(row.get("model_lon")),
            "model_lat":   _safe_float(row.get("model_lat")),
            "distance_km": _safe_float(row.get("distance_km")),
            "metrics": {},
        }

        if raw_df is not None and stn_id in raw_df.index:
            stn["metrics"]["raw_tide"] = _metrics_row_to_dict(raw_df.loc[stn_id], _METRIC_COLS_RAW)

        if godin_df is not None and stn_id in godin_df.index:
            stn["metrics"]["godin_notide"] = _metrics_row_to_dict(godin_df.loc[stn_id], _METRIC_COLS_DETIDED)

        if fes_df is not None and stn_id in fes_df.index:
            stn["metrics"]["fes2022_notide"] = _metrics_row_to_dict(fes_df.loc[stn_id], _METRIC_COLS_DETIDED)

        # Only include stations that have at least one mode AND valid coordinates
        if stn["lon"] is not None and stn["lat"] is not None and stn["metrics"]:
            stations.append(stn)

    return stations


# ------------------------------------------------------------------
# Time series JSON
# ------------------------------------------------------------------

def _load_ts_for_mode(
    station_id: str,
    comp_dir: pathlib.Path,
    mode: str,
) -> dict | None:
    """
    Load comparison CSV for one station, resample to daily means,
    restrict to model period, and return a dict for JSON serialisation.
    """
    csv_path = comp_dir / f"{station_id}.csv.gz"
    if not csv_path.exists():
        return None

    try:
        df = pd.read_csv(csv_path, compression="gzip", parse_dates=["datetime_utc"],
                         index_col="datetime_utc", low_memory=False)
    except Exception as exc:
        logger.warning("  Cannot read %s: %s", csv_path.name, exc)
        return None

    # Restrict to model period
    df = df.loc[MODEL_T_START:MODEL_T_END]
    if df.empty:
        return None

    # Keep only numeric sea-level columns
    keep_cols = ["sea_level_obs_m"]
    if "model_eta_notide_m" in df.columns:
        keep_cols.append("model_eta_notide_m")
    if "model_eta_tide_m" in df.columns and mode == "raw_tide":
        keep_cols.append("model_eta_tide_m")

    df = df[keep_cols].copy()

    # Resample to daily means
    daily = df.resample("1D").mean()

    # Build output dict
    result: dict = {
        "station_id": station_id,
        "mode": mode,
        "dates": [d.strftime("%Y-%m-%d") for d in daily.index],
        "obs": [_safe_float(v) for v in daily["sea_level_obs_m"].values],
    }
    if "model_eta_notide_m" in daily.columns:
        result["notide"] = [_safe_float(v) for v in daily["model_eta_notide_m"].values]
    if "model_eta_tide_m" in daily.columns:
        result["tide"] = [_safe_float(v) for v in daily["model_eta_tide_m"].values]

    return result


def write_ts_json(
    station_id: str,
    mode: str,
    comp_dir: pathlib.Path,
    out_dir: pathlib.Path,
    force: bool,
) -> str:
    out_path = out_dir / f"{station_id}.json"
    if out_path.exists() and not force:
        return "skipped"

    ts = _load_ts_for_mode(station_id, comp_dir, mode)
    if ts is None:
        return "no_data"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as fh:
        json.dump(ts, fh, separators=(",", ":"))
    return "written"


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load metrics -------------------------------------------------------
    logger.info("Loading metrics CSVs…")
    raw_df   = _load_metrics_df(STATION_METRICS_CSV)
    godin_df = _load_metrics_df(STATION_METRICS_GODIN_CSV)
    fes_df   = _load_metrics_df(STATION_METRICS_FES_CSV)

    modes_available = []
    if raw_df is not None:
        logger.info("  raw_tide      : %d stations", len(raw_df))
        modes_available.append("raw_tide")
    if godin_df is not None:
        logger.info("  godin_notide  : %d stations", len(godin_df))
        modes_available.append("godin_notide")
    if fes_df is not None:
        logger.info("  fes2022_notide: %d stations", len(fes_df))
        modes_available.append("fes2022_notide")

    if not modes_available:
        logger.error("No metrics found. Run compute_station_metrics.py first.")
        sys.exit(1)

    # ---- Build station metrics JSON -----------------------------------------
    metrics_out = SITE_DATA_DIR / "station_metrics.json"
    if metrics_out.exists() and not args.force:
        logger.info("station_metrics.json exists — skipping (use --force).")
    else:
        logger.info("Building station_metrics.json…")
        stations = build_station_metrics_json(raw_df, godin_df, fes_df, args.station)
        with open(metrics_out, "w") as fh:
            json.dump({"stations": stations, "modes_available": modes_available}, fh,
                      separators=(",", ":"))
        logger.info("  Written: %s  (%d stations)", metrics_out, len(stations))

    # ---- Time series JSONs ---------------------------------------------------
    if args.skip_ts:
        logger.info("--skip-ts: skipping time series generation.")
        return

    mode_comps = {
        "raw_tide":      GESLA_VS_MODEL_DIR,
        "godin_notide":  VALID_GODIN_DIR,
        "fes2022_notide": VALID_FES_DIR,
    }

    # Determine station IDs to process
    primary_df = raw_df if raw_df is not None else godin_df if godin_df is not None else fes_df
    station_ids = [args.station] if args.station else list(primary_df.index)  # type: ignore[union-attr]

    total_written = 0
    total_skipped = 0
    total_nodata  = 0

    for mode, comp_dir in mode_comps.items():
        if not comp_dir.exists():
            logger.info("  Skipping mode '%s' — comp_dir missing: %s", mode, comp_dir)
            continue

        out_dir = SITE_DATA_DIR / "ts" / mode
        out_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Writing time series for mode '%s' → %s", mode, out_dir)

        n_written = n_skipped = n_nodata = 0
        for i, stn_id in enumerate(station_ids, 1):
            status = write_ts_json(stn_id, mode, comp_dir, out_dir, args.force)
            if status == "written":
                n_written += 1
            elif status == "skipped":
                n_skipped += 1
            else:
                n_nodata += 1
            if i % 100 == 0 or i == len(station_ids):
                logger.info("  %s  %d/%d  written=%d skipped=%d no_data=%d",
                            mode, i, len(station_ids), n_written, n_skipped, n_nodata)

        total_written += n_written
        total_skipped += n_skipped
        total_nodata  += n_nodata

    logger.info("Done.  written=%d  skipped=%d  no_data=%d", total_written, total_skipped, total_nodata)


if __name__ == "__main__":
    main()
