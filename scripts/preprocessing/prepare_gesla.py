"""
prepare_gesla.py
================
Parse GESLA-4 station files and produce:
  1. One compressed CSV per station in ``data/processed/gesla/observations/``
  2. A manifest CSV at ``data/processed/gesla/stations_manifest.csv``

The script reads station files from either:
  - the extracted directory ``data/gesla/raw/stations/``  (default), or
  - directly from the ZIP archive (``--zip-file``) without full extraction.

Each output CSV contains the columns listed in the GESLA pipeline spec:
  datetime_utc, sea_level_obs_m, gesla_qc_flag, gesla_use_flag,
  station_file_name, station_name, site_code, country,
  station_lon, station_lat

Timezone rule
-------------
UTC = local_time − timedelta(hours=TIME_ZONE_HOURS)
If TIME_ZONE_HOURS is 0, data are treated as already in UTC (no shift).
If TIME_ZONE_HOURS is missing/NaN, UTC is assumed and logged as a warning.

Usage
-----
    # From extracted station files (default):
    python scripts/data/prepare_gesla.py

    # From a ZIP archive (no prior extraction needed):
    python scripts/data/prepare_gesla.py --zip-file data/gesla/raw/GESLA4.zip

    # Process a single station for testing:
    python scripts/data/prepare_gesla.py \\
        --station san_francisco_ca-551a-usa-uhslc

    # Overwrite existing outputs:
    python scripts/data/prepare_gesla.py --force

    # Only generate the manifest (skip file parsing):
    python scripts/data/prepare_gesla.py --manifest-only
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys
import zipfile

_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    SURGEMIP_STNLIST,
    GESLA_RAW_DIR,
    GESLA_OBS_DIR,
    GESLA_MANIFEST,
)
from utils.gesla import (
    load_station_list,
    parse_gesla_file,
    build_manifest,
    _find_stem_in_namelist,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


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
        help="Path to SurgeMIP_stnlist.csv.",
    )
    p.add_argument(
        "--stations-dir",
        default=str(GESLA_RAW_DIR / "stations"),
        help="Directory containing extracted GESLA station files.",
    )
    p.add_argument(
        "--zip-file",
        default=None,
        help=(
            "Read station files directly from this ZIP archive "
            "(alternative to --stations-dir)."
        ),
    )
    p.add_argument(
        "--out-dir",
        default=str(GESLA_OBS_DIR),
        help=f"Output directory for per-station CSVs. (default: {GESLA_OBS_DIR})",
    )
    p.add_argument(
        "--manifest",
        default=str(GESLA_MANIFEST),
        help=f"Path for the stations manifest CSV. (default: {GESLA_MANIFEST})",
    )
    p.add_argument(
        "--station",
        default=None,
        help="Process only this one station (by FILE NAME).",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite output files that already exist.",
    )
    p.add_argument(
        "--manifest-only",
        action="store_true",
        help="Only generate the manifest CSV; do not parse station files.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Source resolver
# ---------------------------------------------------------------------------

def _get_raw_bytes(
    file_name: str,
    stations_dir: pathlib.Path | None,
    zip_handle: zipfile.ZipFile | None,
) -> bytes | None:
    """
    Return the raw bytes of a GESLA station file from either a directory or
    a ZIP handle.  Returns None if the file cannot be found.
    """
    if stations_dir is not None:
        candidate = stations_dir / file_name
        if candidate.exists():
            return candidate.read_bytes()
        # Some contributors omit the extension; try as-is
        logger.debug("  File not found at %s", candidate)
        return None

    if zip_handle is not None:
        namelist  = zip_handle.namelist()
        zip_entry = _find_stem_in_namelist(
            pathlib.Path(file_name).stem.lower(), namelist
        )
        if zip_entry:
            return zip_handle.read(zip_entry)
        return None

    return None


# ---------------------------------------------------------------------------
# Single-station processor
# ---------------------------------------------------------------------------

def process_station(
    row: dict,
    stations_dir: pathlib.Path | None,
    zip_handle: zipfile.ZipFile | None,
    out_dir: pathlib.Path,
    force: bool = False,
) -> str:
    """
    Parse one station and save its CSV.gz.

    Returns
    -------
    str : one of "written", "skipped", "missing", "error"
    """
    file_name = str(row["file_name"]).strip()
    out_file  = out_dir / f"{file_name}.csv.gz"

    if out_file.exists() and not force:
        logger.debug("  SKIP (exists): %s", file_name)
        return "skipped"

    raw = _get_raw_bytes(file_name, stations_dir, zip_handle)
    if raw is None:
        logger.warning("  MISSING: %s", file_name)
        return "missing"

    try:
        df = parse_gesla_file(raw, station_meta=row)
    except Exception as exc:
        logger.error("  ERROR parsing %s: %s", file_name, exc)
        return "error"

    if df.empty:
        logger.warning("  EMPTY (no data): %s", file_name)
        return "error"

    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=False, compression="gzip")
    logger.debug("  WRITTEN: %s  (%d rows)", file_name, len(df))
    return "written"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    # ---- Load station list --------------------------------------------------
    station_list = load_station_list(args.station_list)
    logger.info("Station list loaded: %d stations", len(station_list))

    # ---- Filter to single station if requested ------------------------------
    if args.station:
        mask = station_list["file_name"] == args.station
        if not mask.any():
            logger.error("Station '%s' not found in station list.", args.station)
            sys.exit(1)
        station_list = station_list[mask]
        logger.info("Processing single station: %s", args.station)

    # ---- Build manifest -----------------------------------------------------
    manifest_path = pathlib.Path(args.manifest)
    build_manifest(station_list, manifest_path)
    logger.info("Manifest: %s", manifest_path)

    if args.manifest_only:
        logger.info("--manifest-only set; exiting after manifest generation.")
        return

    # ---- Set up data source -------------------------------------------------
    out_dir      = pathlib.Path(args.out_dir)
    stations_dir = pathlib.Path(args.stations_dir) if not args.zip_file else None
    zip_handle   = None

    if args.zip_file:
        zip_path = pathlib.Path(args.zip_file)
        if not zip_path.exists():
            logger.error("ZIP file not found: %s", zip_path)
            sys.exit(1)
        logger.info("Reading station files from ZIP: %s", zip_path)
        zip_handle = zipfile.ZipFile(zip_path, "r")
    else:
        if not stations_dir.exists():
            logger.error(
                "Station files directory not found: %s\n"
                "  Run download_gesla.py --extract first, or pass --zip-file.",
                stations_dir,
            )
            sys.exit(1)
        logger.info("Reading station files from: %s", stations_dir)

    # ---- Process all stations -----------------------------------------------
    counts = {"written": 0, "skipped": 0, "missing": 0, "error": 0}
    total  = len(station_list)

    try:
        for idx, (_, row_s) in enumerate(station_list.iterrows(), 1):
            row = row_s.to_dict()
            status = process_station(
                row=row,
                stations_dir=stations_dir,
                zip_handle=zip_handle,
                out_dir=out_dir,
                force=args.force,
            )
            counts[status] += 1

            if idx % 50 == 0 or idx == total:
                logger.info(
                    "  Progress: %d/%d  written=%d  skipped=%d  missing=%d  error=%d",
                    idx, total,
                    counts["written"], counts["skipped"],
                    counts["missing"], counts["error"],
                )
    finally:
        if zip_handle is not None:
            zip_handle.close()

    # ---- Summary ------------------------------------------------------------
    logger.info(
        "\nDone.\n"
        "  Written : %d\n"
        "  Skipped : %d (already existed)\n"
        "  Missing : %d (not found in source)\n"
        "  Errors  : %d\n"
        "  Output  : %s",
        counts["written"], counts["skipped"],
        counts["missing"], counts["error"],
        out_dir,
    )


if __name__ == "__main__":
    main()
