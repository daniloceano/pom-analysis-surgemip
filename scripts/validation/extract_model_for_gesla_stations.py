"""
extract_model_for_gesla_stations.py
====================================
Extract POM model time series for every station in the SurgeMIP/GESLA station
list and save per-station CSVs plus a summary index table.

For each station the script:
  1. Finds the nearest (wet) model grid point.
  2. Extracts the full (or period-filtered) ``eta_tide`` and ``eta_notide``
     time series from the memory-mapped GrADS binary files.
  3. Computes ``model_tide_minus_notide_m = eta_tide − eta_notide``.
  4. Saves the result as ``data/processed/validation/model_ts/<file_name>.csv.gz``.
  5. Appends a row to the station-model index table.

Grid-point matching
-------------------
Default method: **nearest grid point** (same as ``extract_point.py``).
The model uses ``GrADSReader.nearest_wet_ij()`` which searches expanding
shells to avoid returning a land-masked cell.

Distance is computed as the great-circle distance (Haversine formula) in km.

Usage
-----
    # Extract for all stations in the default station list:
    python scripts/validation/extract_model_for_gesla_stations.py

    # Restrict to a time window:
    python scripts/validation/extract_model_for_gesla_stations.py \\
        --t-start 2016-01-01 --t-end 2017-01-01

    # Process only one station (for testing):
    python scripts/validation/extract_model_for_gesla_stations.py \\
        --station san_francisco_ca-551a-usa-uhslc

    # Overwrite existing outputs:
    python scripts/validation/extract_model_for_gesla_stations.py --force

    # Use a custom station list:
    python scripts/validation/extract_model_for_gesla_stations.py \\
        --station-list data/processed/gesla/stations_manifest.csv
"""

from __future__ import annotations

import argparse
import logging
import math
import pathlib
import sys

import numpy as np
import pandas as pd

_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    TIDE_CTL, NOTIDE_CTL,
    SURGEMIP_STNLIST,
    VALIDATION_DIR,
    STATION_MODEL_INDEX,
)
from utils.grads_reader import GrADSReader
from utils.gesla import load_station_list

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL_TS_DIR = VALIDATION_DIR / "model_ts"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Return the great-circle distance in km between two (lon, lat) points."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi        = math.radians(lat2 - lat1)
    dlambda     = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _dt_to_idx(reader: GrADSReader, dt_str: str) -> int:
    """Convert an ISO datetime string to the nearest time-step index."""
    dt  = pd.Timestamp(dt_str).tz_localize(None)
    idx = reader.times.get_indexer([dt], method="nearest")[0]
    return int(idx)


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
        help="Path to SurgeMIP_stnlist.csv or the manifest CSV.",
    )
    p.add_argument(
        "--out-dir",
        default=str(MODEL_TS_DIR),
        help=f"Output directory for per-station model CSVs. (default: {MODEL_TS_DIR})",
    )
    p.add_argument(
        "--index-file",
        default=str(STATION_MODEL_INDEX),
        help=f"Output path for the station-model index table. (default: {STATION_MODEL_INDEX})",
    )
    p.add_argument(
        "--t-start",
        default=None,
        help="Start datetime ISO 8601 (default: beginning of dataset).",
    )
    p.add_argument(
        "--t-end",
        default=None,
        help="End datetime ISO 8601, exclusive (default: end of dataset).",
    )
    p.add_argument(
        "--station",
        default=None,
        help="Process only this one station (by FILE NAME).",
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
# Per-station extractor
# ---------------------------------------------------------------------------

def extract_one_station(
    row: dict,
    tide_reader: GrADSReader,
    notide_reader: GrADSReader,
    t_start_idx: int,
    t_end_idx: int,
    out_dir: pathlib.Path,
    force: bool = False,
) -> dict | None:
    """
    Extract model time series for one station.

    Returns a dict for the index table row, or None if skipped/failed.
    """
    file_name = str(row["file_name"]).strip()
    out_file  = out_dir / f"{file_name}.csv.gz"

    if out_file.exists() and not force:
        logger.debug("  SKIP (exists): %s", file_name)
        return None  # signal "skipped"

    lon = float(row["longitude"])
    lat = float(row["latitude"])

    # ---- Find nearest wet grid point ---------------------------------------
    try:
        j, i = tide_reader.nearest_wet_ij(lon, lat, t_idx=t_start_idx)
    except Exception as exc:
        logger.error("  nearest_wet_ij failed for %s: %s", file_name, exc)
        j, i = tide_reader.nearest_ij(lon, lat)

    grid_lon = float(tide_reader.lon[i])
    grid_lat = float(tide_reader.lat[j])
    dist_km  = haversine_km(lon, lat, grid_lon, grid_lat)

    # ---- Extract time series -----------------------------------------------
    times, ts_tide, _, _ = tide_reader.extract_point(
        lon, lat, t_start=t_start_idx, t_end=t_end_idx,
    )
    _, ts_notide, _, _ = notide_reader.extract_point(
        lon, lat, t_start=t_start_idx, t_end=t_end_idx,
    )

    # Convert masked arrays to float32 with NaN
    if hasattr(ts_tide, "filled"):
        ts_tide = ts_tide.filled(np.nan)
    if hasattr(ts_notide, "filled"):
        ts_notide = ts_notide.filled(np.nan)

    ts_tide   = ts_tide.astype(np.float32)
    ts_notide = ts_notide.astype(np.float32)
    ts_diff   = (ts_tide - ts_notide).astype(np.float32)

    # ---- Build DataFrame ---------------------------------------------------
    df = pd.DataFrame({
        "datetime_utc":              times,
        "model_eta_tide_m":          ts_tide,
        "model_eta_notide_m":        ts_notide,
        "model_tide_minus_notide_m": ts_diff,
        "station_file_name":         file_name,
        "station_name":              row.get("site_name", ""),
        "site_code":                 row.get("site_code", ""),
        "country":                   row.get("country", ""),
        "station_lon":               lon,
        "station_lat":               lat,
        "model_lon":                 grid_lon,
        "model_lat":                 grid_lat,
        "grid_i":                    i,
        "grid_j":                    j,
        "distance_km":               round(dist_km, 3),
    })

    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=False, compression="gzip")
    logger.debug(
        "  WRITTEN: %s  (dist=%.1f km,  n=%d)", file_name, dist_km, len(df)
    )

    # Return index row
    return {
        "station_file_name": file_name,
        "station_name":      row.get("site_name", ""),
        "site_code":         row.get("site_code", ""),
        "country":           row.get("country", ""),
        "station_lon":       lon,
        "station_lat":       lat,
        "model_lon":         grid_lon,
        "model_lat":         grid_lat,
        "grid_i":            i,
        "grid_j":            j,
        "distance_km":       round(dist_km, 3),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    out_dir    = pathlib.Path(args.out_dir)
    index_path = pathlib.Path(args.index_file)

    # ---- Load station list --------------------------------------------------
    station_list = load_station_list(args.station_list)
    logger.info("Station list: %d stations", len(station_list))

    if args.station:
        mask = station_list["file_name"] == args.station
        if not mask.any():
            logger.error("Station '%s' not found in station list.", args.station)
            sys.exit(1)
        station_list = station_list[mask]

    # ---- Open model datasets ------------------------------------------------
    logger.info("Opening tide dataset ...")
    tide_reader   = GrADSReader(TIDE_CTL,   verbose=False)
    logger.info("Opening notide dataset ...")
    notide_reader = GrADSReader(NOTIDE_CTL, verbose=False)

    # ---- Resolve time indices -----------------------------------------------
    t_start_idx = 0 if args.t_start is None else _dt_to_idx(tide_reader, args.t_start)
    t_end_idx   = tide_reader.nt if args.t_end is None else _dt_to_idx(tide_reader, args.t_end)

    logger.info(
        "Time range: %s → %s  (%d steps)",
        tide_reader.times[t_start_idx],
        tide_reader.times[t_end_idx - 1],
        t_end_idx - t_start_idx,
    )

    # ---- Process all stations -----------------------------------------------
    index_rows: list[dict] = []
    counts = {"written": 0, "skipped": 0, "error": 0}
    total  = len(station_list)

    for idx, (_, row_s) in enumerate(station_list.iterrows(), 1):
        row = row_s.to_dict()
        file_name = str(row.get("file_name", "?")).strip()

        try:
            result = extract_one_station(
                row=row,
                tide_reader=tide_reader,
                notide_reader=notide_reader,
                t_start_idx=t_start_idx,
                t_end_idx=t_end_idx,
                out_dir=out_dir,
                force=args.force,
            )
        except Exception as exc:
            logger.error("  ERROR for %s: %s", file_name, exc)
            counts["error"] += 1
            continue

        if result is None:
            counts["skipped"] += 1
        else:
            counts["written"] += 1
            index_rows.append(result)

        if idx % 50 == 0 or idx == total:
            logger.info(
                "  Progress: %d/%d  written=%d  skipped=%d  errors=%d",
                idx, total, counts["written"], counts["skipped"], counts["error"],
            )

    # ---- Save / update index table ------------------------------------------
    if index_rows:
        new_df = pd.DataFrame(index_rows)

        if index_path.exists() and not args.force:
            existing = pd.read_csv(index_path)
            # Merge: keep existing rows for stations we didn't re-process
            combined = pd.concat(
                [existing[~existing["station_file_name"].isin(new_df["station_file_name"])], new_df],
                ignore_index=True,
            )
        else:
            combined = new_df

        index_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(index_path, index=False)
        logger.info("Station-model index: %s  (%d rows)", index_path, len(combined))

    # ---- Summary ------------------------------------------------------------
    logger.info(
        "\nDone.\n"
        "  Written : %d\n"
        "  Skipped : %d (already existed)\n"
        "  Errors  : %d\n"
        "  Output  : %s\n"
        "  Index   : %s",
        counts["written"], counts["skipped"], counts["error"],
        out_dir, index_path,
    )


if __name__ == "__main__":
    main()
