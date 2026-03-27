"""
prepare_site_data.py
====================
Prepare data files for the interactive validation website.

Reads outputs produced by the validation pipeline and generates
lightweight JSON files for static serving via Next.js / Vercel.

Outputs (written to site/public/data/)
---------------------------------------
  station_metrics.json   – unified per-station metrics for all modes
                           (used to populate the map and summary cards)
  ts/<stn>.json          – per-station daily-mean time series
                           Always contains raw obs + POM_tide + POM_notide.
                           Independent of validation mode: the chart shows
                           the same signals regardless of which mode is
                           selected in the UI.

Station metrics JSON structure
------------------------------
  {
    "stations": [
      {
        "id": "santos-540a-bra-uhslc",
        "name": "Santos",
        "site_code": "540A",
        "country": "BRA",
        "lon": -46.30, "lat": -23.97,
        "model_lon": ..., "model_lat": ..., "distance_km": ...,
        "metrics": {
          "godin_tide":    { "n_valid": ..., "rmse_notide": ..., ... },
          "fes2022_tide":  { ... },
          "godin_notide":  { ... },
          "fes2022_notide":{ ... }
        }
      },
      ...
    ],
    "modes_available": ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"]
  }

Time series JSON structure
--------------------------
  {
    "station_id": "santos-540a-bra-uhslc",
    "dates":  ["2013-01-01", ...],
    "obs":    [0.12, ...],       <- raw GESLA obs (with tidal signal, m)
    "tide":   [0.15, ...],       <- POM_tide daily mean (m)
    "notide": [0.05, ...]        <- POM_notide daily mean (surge reference, m)
  }

Why raw obs + POM_tide?
-----------------------
The primary time-series visualisation always shows the raw (un-detided)
observation alongside POM_tide.  Both signals include the astronomical tidal
component, making them visually comparable on the same y-axis.
Validation metrics (RMSE, bias, Pearson r) from the detided modes are shown
separately in the station card.

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
)

# Import new mode paths (may not exist in older settings.py versions)
try:
    from config.settings import (
        STATION_METRICS_GODIN_TIDE_CSV,
        STATION_METRICS_FES_TIDE_CSV,
    )
except ImportError:
    STATION_METRICS_GODIN_TIDE_CSV = None  # type: ignore[assignment]
    STATION_METRICS_FES_TIDE_CSV   = None  # type: ignore[assignment]

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

# Metric columns to expose per mode
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


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, 6)
    except (ValueError, TypeError):
        return None


def _load_metrics_df(csv_path: pathlib.Path | None) -> pd.DataFrame | None:
    if csv_path is None or not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    return df.set_index("station_file_name")


def _metrics_row_to_dict(row: pd.Series, cols: list[str]) -> dict:
    return {c: _safe_float(row.get(c)) for c in cols}


# ---------------------------------------------------------------------------
# Station metrics JSON
# ---------------------------------------------------------------------------

def build_station_metrics_json(
    dfs: dict[str, pd.DataFrame | None],
    station_filter: str | None,
) -> tuple[list[dict], list[str]]:
    """
    Build unified station list with metrics for all available modes.

    Parameters
    ----------
    dfs : dict[mode_name -> DataFrame | None]
        Mapping of validation mode name to metrics DataFrame (indexed by
        station_file_name).
    station_filter : str | None
        If set, only process this one station.

    Returns
    -------
    list[dict], list[str]
        Stations list and list of mode names that have data.
    """
    # Ordered preference for source of station coordinates
    mode_order = ["raw_tide", "godin_notide", "fes2022_notide",
                  "godin_tide", "fes2022_tide"]
    primary_df: pd.DataFrame | None = None
    for m in mode_order:
        if dfs.get(m) is not None:
            primary_df = dfs[m]
            break

    if primary_df is None:
        logger.error("No metrics CSVs found — nothing to build.")
        return [], []

    modes_available = [m for m in mode_order if dfs.get(m) is not None
                       and m != "raw_tide"]  # exclude raw descriptive from modes_available

    stations = []
    ids = [station_filter] if station_filter else list(primary_df.index)

    for stn_id in ids:
        if stn_id not in primary_df.index:
            continue
        row = primary_df.loc[stn_id]

        stn: dict = {
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

        # Validation modes (detided)
        for mode_name, metric_cols in [
            ("godin_tide",    _METRIC_COLS_DETIDED),
            ("fes2022_tide",  _METRIC_COLS_DETIDED),
            ("godin_notide",  _METRIC_COLS_DETIDED),
            ("fes2022_notide",_METRIC_COLS_DETIDED),
        ]:
            df = dfs.get(mode_name)
            if df is not None and stn_id in df.index:
                stn["metrics"][mode_name] = _metrics_row_to_dict(
                    df.loc[stn_id], metric_cols)

        # raw_tide (descriptive) — kept for backwards compatibility but
        # not listed in modes_available (not a surge validation mode)
        if dfs.get("raw_tide") is not None and stn_id in dfs["raw_tide"].index:
            stn["metrics"]["raw_tide"] = _metrics_row_to_dict(
                dfs["raw_tide"].loc[stn_id], _METRIC_COLS_RAW)

        if stn["lon"] is not None and stn["lat"] is not None and stn["metrics"]:
            stations.append(stn)

    return stations, modes_available


# ---------------------------------------------------------------------------
# Time series JSON  (always raw obs + POM_tide + POM_notide)
# ---------------------------------------------------------------------------

def _load_ts(
    station_id: str,
    raw_comp_dir: pathlib.Path,
) -> dict | None:
    """
    Load the raw_tide comparison CSV for one station and build the TS dict.

    The time series JSON ALWAYS uses the raw (un-detided) GESLA observation
    alongside POM_tide and POM_notide.  This makes the chart independent of
    the validation mode selector: users see the same raw comparison regardless
    of which detiding method they choose for the metric maps.

    Returns
    -------
    dict or None
    """
    csv_path = raw_comp_dir / f"{station_id}.csv.gz"
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

    # Select sea-level columns
    keep = ["sea_level_obs_m"]
    if "model_eta_tide_m"   in df.columns: keep.append("model_eta_tide_m")
    if "model_eta_notide_m" in df.columns: keep.append("model_eta_notide_m")
    df = df[[c for c in keep if c in df.columns]].copy()

    # Daily means
    daily = df.resample("1D").mean()

    result: dict = {
        "station_id": station_id,
        "dates": [d.strftime("%Y-%m-%d") for d in daily.index],
        "obs":   [_safe_float(v) for v in daily["sea_level_obs_m"].values],
    }
    if "model_eta_tide_m" in daily.columns:
        result["tide"]   = [_safe_float(v) for v in daily["model_eta_tide_m"].values]
    if "model_eta_notide_m" in daily.columns:
        result["notide"] = [_safe_float(v) for v in daily["model_eta_notide_m"].values]

    return result


def write_ts_json(
    station_id: str,
    raw_comp_dir: pathlib.Path,
    out_dir: pathlib.Path,
    force: bool,
) -> str:
    out_path = out_dir / f"{station_id}.json"
    if out_path.exists() and not force:
        return "skipped"

    ts = _load_ts(station_id, raw_comp_dir)
    if ts is None:
        return "no_data"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as fh:
        json.dump(ts, fh, separators=(",", ":"))
    return "written"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load all metrics CSVs -----------------------------------------------
    logger.info("Loading metrics CSVs…")
    dfs: dict[str, pd.DataFrame | None] = {
        "raw_tide":      _load_metrics_df(STATION_METRICS_CSV),
        "godin_notide":  _load_metrics_df(STATION_METRICS_GODIN_CSV),
        "fes2022_notide":_load_metrics_df(STATION_METRICS_FES_CSV),
        "godin_tide":    _load_metrics_df(STATION_METRICS_GODIN_TIDE_CSV),
        "fes2022_tide":  _load_metrics_df(STATION_METRICS_FES_TIDE_CSV),
    }
    for name, df in dfs.items():
        if df is not None:
            logger.info("  %-18s: %d stations", name, len(df))

    if all(v is None for v in dfs.values()):
        logger.error("No metrics found. Run compute_station_metrics.py first.")
        sys.exit(1)

    # ---- Build station metrics JSON ------------------------------------------
    metrics_out = SITE_DATA_DIR / "station_metrics.json"
    if metrics_out.exists() and not args.force:
        logger.info("station_metrics.json exists — skipping (use --force).")
    else:
        logger.info("Building station_metrics.json…")
        stations, modes_available = build_station_metrics_json(dfs, args.station)
        with open(metrics_out, "w") as fh:
            json.dump(
                {"stations": stations, "modes_available": modes_available},
                fh, separators=(",", ":"),
            )
        logger.info("  Written: %s  (%d stations, modes=%s)",
                    metrics_out, len(stations), modes_available)

    # ---- Time series JSONs  (one per station, always raw obs + POM_tide) -----
    if args.skip_ts:
        logger.info("--skip-ts: skipping time series generation.")
        return

    if not GESLA_VS_MODEL_DIR.exists():
        logger.warning(
            "raw_tide comparison CSV dir not found: %s\n"
            "  TS JSONs cannot be generated without raw_tide comparison CSVs.",
            GESLA_VS_MODEL_DIR,
        )
        return

    out_ts_dir = SITE_DATA_DIR / "ts"
    out_ts_dir.mkdir(parents=True, exist_ok=True)

    # Determine station IDs from primary metrics
    primary_df = next((v for v in dfs.values() if v is not None), None)
    station_ids = [args.station] if args.station else list(primary_df.index)  # type: ignore[union-attr]

    logger.info(
        "Writing time series JSONs → %s  (%d stations)", out_ts_dir, len(station_ids)
    )

    n_written = n_skipped = n_nodata = 0
    for i, stn_id in enumerate(station_ids, 1):
        status = write_ts_json(stn_id, GESLA_VS_MODEL_DIR, out_ts_dir, args.force)
        if status == "written":
            n_written += 1
        elif status == "skipped":
            n_skipped += 1
        else:
            n_nodata += 1
        if i % 100 == 0 or i == len(station_ids):
            logger.info("  %d/%d  written=%d skipped=%d no_data=%d",
                        i, len(station_ids), n_written, n_skipped, n_nodata)

    logger.info("Done.  written=%d  skipped=%d  no_data=%d",
                n_written, n_skipped, n_nodata)


if __name__ == "__main__":
    main()
