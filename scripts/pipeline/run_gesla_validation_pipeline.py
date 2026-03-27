"""
run_gesla_validation_pipeline.py
=================================
One-command orchestrator for the full GESLA-4 / POM validation workflow.

Stages
------
  Stage 1   – Check / download / extract GESLA-4 data
  Stage 2   – Prepare GESLA observation CSVs             (parallelised)
  Stage 3   – Extract model time series for all stations  (parallelised)
  Stage 3.5 – Apply tidal detiding to observations        (non-raw modes only)
  Stage 4   – Build observation-vs-model comparison CSVs  (parallelised, per mode)
  Stage 4b  – Build tide-derived comparison CSVs          (godin_tide / fes2022_tide)
  Stage 5   – Compute per-station validation metrics      (per mode)
  Stage 6   – Generate station-map figures                (per mode)

Validation modes (--mode)
-------------------------
  raw_tide       – Compare raw tidal observations vs model_eta_tide (and
                   model_eta_notide for reference).  No detiding applied.
                   Descriptive only — not a surge validation metric.
  godin_notide   – Godin (1972) low-pass filter (24 h + 24 h + 25 h running
                   means) applied to observations.  De-tided obs vs POM_notide.
  fes2022_notide – FES2022 tidal prediction subtracted from observations.
                   De-tided obs vs POM_notide (surge validation).
  godin_tide     – Godin filter applied to obs AND POM_tide.  Cross-check:
                   POM_tide_godin ≈ POM_notide (tidal component removed).
                   Requires godin_notide to be built first (Stage 4b).
  fes2022_tide   – FES2022 subtracted from obs AND POM_tide.  Cross-check:
                   POM_tide_fes ≈ POM_notide (POM was forced with FES2022).
                   Requires fes2022_notide to be built first (Stage 4b).
  all            – Run all five modes in sequence.

Stages 1–3 always run (shared inputs across modes).  Stage 3.5 and Stages
4–6 repeat for each selected mode.

The pipeline is **idempotent**: every stage skips files that already exist
unless the corresponding ``--force-*`` flag is passed.

Concurrency model
-----------------
``concurrent.futures.ThreadPoolExecutor`` is used for Stages 2–4:

* Stage 2 (prepare observations): reads individual extracted station files
  from disk — purely I/O-bound, no shared mutable state.
* Stage 3 (model extraction): uses two ``GrADSReader`` instances backed by
  ``numpy.memmap``.  ``numpy.memmap`` is read-only and the OS memory-mapping
  layer allows concurrent reads; no GIL-released C extensions are needed for
  safety here.  A single pair of readers is opened in the main thread and
  shared across all worker threads.
* Stage 4 (merge obs + model): reads and writes independent per-station files —
  fully I/O-bound.

``ProcessPoolExecutor`` is *not* used because:
  a) The model binary files (~138 GB) are memory-mapped; forking would
     duplicate the mapping handles unnecessarily and add start-up overhead.
  b) Python's GIL is not the bottleneck — the hot path is numpy indexing into
     a memory-mapped array, which releases the GIL.

Usage
-----
    # Raw mode only (default — skip completed stages):
    python scripts/pipeline/run_gesla_validation_pipeline.py

    # All five validation modes:
    python scripts/pipeline/run_gesla_validation_pipeline.py --mode all

    # Specific modes only:
    python scripts/pipeline/run_gesla_validation_pipeline.py \\
        --mode godin_notide fes2022_notide godin_tide fes2022_tide

    # Force all stages from scratch (all modes):
    python scripts/pipeline/run_gesla_validation_pipeline.py --mode all --force-all

    # Re-run detiding and downstream stages only:
    python scripts/pipeline/run_gesla_validation_pipeline.py \\
        --mode all --force-detide --force-build --force-metrics --force-maps

    # Dry-run (show what would be done, do nothing):
    python scripts/pipeline/run_gesla_validation_pipeline.py --mode all --dry-run

    # Restrict to a time window (passed to stages 3 & 4):
    python scripts/pipeline/run_gesla_validation_pipeline.py \\
        --t-start 2016-01-01 --t-end 2017-01-01

    # Restrict to a single station (useful for testing):
    python scripts/pipeline/run_gesla_validation_pipeline.py \\
        --mode all --station santos-540a-bra-uhslc

Environment variables
---------------------
    GESLA_ZIP_URL   – download URL for the GESLA-4 archive (alternative to --url)
    GESLA_ZIP_FILE  – local path for the ZIP (alternative to default in settings)
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

# ---------------------------------------------------------------------------
# Bootstrap: ensure project root is importable
# ---------------------------------------------------------------------------
_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    TIDE_CTL,
    NOTIDE_CTL,
    SURGEMIP_STNLIST,
    GESLA_RAW_DIR,
    GESLA_OBS_DIR,
    GESLA_OBS_GODIN_DIR,
    GESLA_OBS_FES_DIR,
    VALIDATION_DIR,
    GESLA_VS_MODEL_DIR,
    VALID_GODIN_DIR,
    VALID_FES_DIR,
    VALID_GODIN_TIDE_DIR,
    VALID_FES_TIDE_DIR,
    STATION_METRICS_CSV,
    STATION_METRICS_GODIN_CSV,
    STATION_METRICS_FES_CSV,
    STATION_METRICS_GODIN_TIDE_CSV,
    STATION_METRICS_FES_TIDE_CSV,
    FIG_VALID_DIR,
    FIG_VALID_RAW_DIR,
    FIG_VALID_GODIN_DIR,
    FIG_VALID_FES_DIR,
    FIG_VALID_GODIN_TIDE_DIR,
    FIG_VALID_FES_TIDE_DIR,
)
from utils.gesla import load_station_list

# Lazy imports — only imported when the stage actually runs
# (avoids loading cartopy/matplotlib at startup if maps are skipped)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL_TS_DIR       = VALIDATION_DIR / "model_ts"
GESLA_STATIONS_DIR = GESLA_RAW_DIR  / "stations"

# ---------------------------------------------------------------------------
# Validation mode configuration
# ---------------------------------------------------------------------------
# Each entry defines the data paths and metric targets for one validation mode.
#   obs_dir     – observation CSV directory (sea_level_obs_m = what is compared)
#   comp_dir    – comparison CSV output directory
#   metrics_csv – station metrics output file
#   fig_dir     – figure output directory
#   targets     – model targets for metrics; "notide" only for de-tided modes
#                 (comparing de-tided obs against model_eta_tide would be
#                  physically inconsistent)
# Mode naming: <obs_treatment>_<model_target>
#   raw_tide       – obs_raw (with tide)   vs  POM tide    — descriptive
#   godin_notide   – obs_godin (detided)   vs  POM notide  — surge validation
#   fes2022_notide – obs_fes2022 (detided) vs  POM notide  — surge validation
#   godin_tide     – obs_godin (detided)   vs  POM_tide_godin  — cross-check
#   fes2022_tide   – obs_fes (detided)     vs  POM_tide_fes    — cross-check
#
# For godin_tide and fes2022_tide, Stage 4 is replaced by stage4b_build_tide_modes
# which calls build_tide_mode_comparisons.py.  Stage 5 and 6 proceed normally.
VALIDATION_MODES: dict[str, dict] = {
    "raw_tide": {
        "obs_dir":     GESLA_OBS_DIR,
        "comp_dir":    GESLA_VS_MODEL_DIR,
        "metrics_csv": STATION_METRICS_CSV,
        "fig_dir":     FIG_VALID_RAW_DIR,
        "targets":     "notide,tide",   # both: notide (informational) and tide (descriptive)
        "tide_derived": False,
    },
    "godin_notide": {
        "obs_dir":     GESLA_OBS_GODIN_DIR,
        "comp_dir":    VALID_GODIN_DIR,
        "metrics_csv": STATION_METRICS_GODIN_CSV,
        "fig_dir":     FIG_VALID_GODIN_DIR,
        "targets":     "notide",        # surge validation: detided obs vs POM notide
        "tide_derived": False,
    },
    "fes2022_notide": {
        "obs_dir":     GESLA_OBS_FES_DIR,
        "comp_dir":    VALID_FES_DIR,
        "metrics_csv": STATION_METRICS_FES_CSV,
        "fig_dir":     FIG_VALID_FES_DIR,
        "targets":     "notide",        # surge validation: detided obs vs POM notide
        "tide_derived": False,
    },
    # ── Tide-derived modes: comparison CSVs built from existing godin_notide /
    #    fes2022_notide CSVs by applying Godin filter / FES2022 subtraction to
    #    model_eta_tide_m.  Stage 4 is replaced by stage4b_build_tide_modes.
    "godin_tide": {
        "comp_dir":    VALID_GODIN_TIDE_DIR,
        "metrics_csv": STATION_METRICS_GODIN_TIDE_CSV,
        "fig_dir":     FIG_VALID_GODIN_TIDE_DIR,
        "targets":     "notide",        # model_eta_notide_m holds POM_tide_godin
        "tide_derived": True,           # use stage4b instead of stage4
    },
    "fes2022_tide": {
        "comp_dir":    VALID_FES_TIDE_DIR,
        "metrics_csv": STATION_METRICS_FES_TIDE_CSV,
        "fig_dir":     FIG_VALID_FES_TIDE_DIR,
        "targets":     "notide",        # model_eta_notide_m holds POM_tide_fes
        "tide_derived": True,           # use stage4b instead of stage4
    },
}


# ---------------------------------------------------------------------------
# tqdm import (graceful fallback if somehow missing)
# ---------------------------------------------------------------------------
try:
    from tqdm import tqdm
    _TQDM_AVAILABLE = True
except ImportError:
    _TQDM_AVAILABLE = False
    logger.warning("tqdm not found — progress bars disabled.  Install with: pip install tqdm")


def _progress_bar(iterable, total: int, desc: str = "", unit: str = "stn"):
    """Wrap *iterable* in a tqdm bar when available, otherwise yield as-is."""
    if _TQDM_AVAILABLE:
        return tqdm(iterable, total=total, desc=desc, unit=unit, dynamic_ncols=True)
    return iterable


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ---- GESLA download options (Stage 1) -----------------------------------
    p.add_argument(
        "--url",
        default=None,
        help="Direct download URL for the GESLA-4 ZIP (Stage 1 only).",
    )
    p.add_argument(
        "--zip-file",
        default=None,
        help="Path to an existing GESLA-4 ZIP archive (skip download).",
    )

    # ---- Time-window options (Stages 3 & 4) ---------------------------------
    p.add_argument(
        "--t-start",
        default=None,
        help="Start date ISO 8601 passed to Stages 3 & 4 (default: beginning of dataset).",
    )
    p.add_argument(
        "--t-end",
        default=None,
        help="End date ISO 8601 passed to Stages 3 & 4 (default: end of dataset).",
    )

    # ---- Single-station mode ------------------------------------------------
    p.add_argument(
        "--station",
        default=None,
        help="Process only this one station file name (useful for testing).",
    )

    # ---- Parallelism --------------------------------------------------------
    p.add_argument(
        "--workers",
        type=int,
        default=100,
        help="Number of parallel threads for Stages 2–4 (default: 50).",
    )

    # ---- Force flags --------------------------------------------------------
    p.add_argument(
        "--force-all",
        action="store_true",
        help="Force re-run of all stages, ignoring existing outputs.",
    )
    p.add_argument("--force-download", action="store_true",
                   help="Force re-download/extract of GESLA archive (Stage 1).")
    p.add_argument("--force-prepare",  action="store_true",
                   help="Force re-preparation of observation CSVs (Stage 2).")
    p.add_argument("--force-extract",  action="store_true",
                   help="Force re-extraction of model time series (Stage 3).")
    p.add_argument("--force-build",    action="store_true",
                   help="Force rebuild of comparison CSVs (Stage 4).")
    p.add_argument("--force-metrics",  action="store_true",
                   help="Force recomputation of station metrics (Stage 5).")
    p.add_argument("--force-maps",     action="store_true",
                   help="Force regeneration of validation maps (Stage 6).")

    # ---- Validation mode ----------------------------------------------------
    p.add_argument(
        "--mode",
        nargs="+",
        default=["raw_tide"],
        choices=["raw_tide", "godin_notide", "fes2022_notide",
                 "godin_tide", "fes2022_tide", "all"],
        metavar="MODE",
        help=(
            "Which validation mode(s) to run for Stages 4–6.  "
            "Choices: raw_tide | godin_notide | fes2022_notide | "
            "godin_tide | fes2022_tide | all.  "
            "Multiple modes can be given: --mode godin_notide godin_tide.  "
            "'all' expands to all five modes.  "
            "godin_tide and fes2022_tide require godin_notide / fes2022_notide "
            "to be built first (Stage 4b derives their CSVs from existing ones).  "
            "Stages 1–3 always run (they produce data shared by all modes).  "
            "(default: raw_tide)"
        ),
    )

    # ---- Detiding force flags -----------------------------------------------
    p.add_argument(
        "--force-detide",
        action="store_true",
        help="Force re-application of tidal detiding (Stage 3.5).",
    )

    # ---- Dry-run ------------------------------------------------------------
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without executing any stage.",
    )

    # ---- Logging ------------------------------------------------------------
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    return p.parse_args()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _stage_header(n: int, label: str) -> None:
    """Print a prominent stage header."""
    bar = "─" * 60
    logger.info("")
    logger.info("┌%s┐", bar)
    logger.info("│  Stage %d — %s%s│", n, label, " " * max(0, 56 - len(label) - 9))
    logger.info("└%s┘", bar)


def _run_parallel(
    items: list,
    worker_fn: Callable,
    desc: str,
    n_workers: int,
    dry_run: bool,
) -> dict[str, int]:
    """
    Run *worker_fn(item)* over *items* in a ThreadPoolExecutor.

    Returns a counts dict with keys from the return values of *worker_fn*
    (each call must return a string status such as 'written', 'skipped', etc.)
    plus 'error' for unexpected exceptions.
    """
    counts: dict[str, int] = {}

    if dry_run:
        logger.info("[DRY-RUN] Would process %d items with %d workers", len(items), n_workers)
        return counts

    futures_map = {}
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        for item in items:
            fut = pool.submit(worker_fn, item)
            futures_map[fut] = item

        with _progress_bar(as_completed(futures_map), total=len(items), desc=desc) as pbar:
            for fut in pbar:
                try:
                    status = fut.result()
                except Exception as exc:
                    logger.error("Unhandled exception: %s", exc)
                    status = "error"
                counts[status] = counts.get(status, 0) + 1

    return counts


def _log_counts(counts: dict[str, int]) -> None:
    for status, n in sorted(counts.items()):
        logger.info("    %-20s : %d", status, n)


# ---------------------------------------------------------------------------
# Stage 1 — GESLA download / extraction check
# ---------------------------------------------------------------------------

def _find_gesla_zip(override: str | None) -> pathlib.Path | None:
    """
    Locate the GESLA-4 ZIP archive.

    Search order:
      1. Explicit ``--zip-file`` argument.
      2. ``GESLA_ZIP_FILE`` from settings / environment variable.
      3. Any ``*.zip`` file in ``GESLA_RAW_DIR`` (picks the largest one).

    Returns the path if found, else None.
    """
    from config.settings import GESLA_ZIP_FILE

    candidates: list[pathlib.Path] = []
    if override:
        candidates.append(pathlib.Path(override))
    candidates.append(pathlib.Path(GESLA_ZIP_FILE))
    # Also search the raw directory for any ZIP (handles GESLA4_ALL.zip etc.)
    if GESLA_RAW_DIR.exists():
        candidates += sorted(GESLA_RAW_DIR.glob("*.zip"), key=lambda p: p.stat().st_size, reverse=True)

    for p in candidates:
        if p.exists():
            return p
    return None


def stage1_check_gesla(args: argparse.Namespace) -> None:
    """
    Ensure GESLA source data are available on disk.

    Skip conditions (any one is sufficient, unless ``--force-download``):
      a) ``data/gesla/raw/stations/`` directory has at least one file
         (station files already extracted).
      b) ``data/processed/gesla/observations/`` directory has at least one
         file (Stage 2 was already completed — raw extraction not needed).

    If neither condition holds the script tries to extract from the ZIP
    (auto-detected in ``data/gesla/raw/``) or downloads if ``--url`` is given.
    """
    _stage_header(1, "Check / download / extract GESLA-4")

    stations_dir = GESLA_STATIONS_DIR
    n_stations   = sum(1 for _ in stations_dir.glob("*")) if stations_dir.exists() else 0
    n_obs        = sum(1 for _ in GESLA_OBS_DIR.glob("*.csv.gz")) if GESLA_OBS_DIR.exists() else 0

    if not args.force_download:
        if n_stations > 0:
            logger.info(
                "  Found %d extracted file(s) in %s — skipping.",
                n_stations, stations_dir,
            )
            return
        if n_obs > 0:
            logger.info(
                "  Found %d observation CSV(s) in %s — GESLA already prepared, skipping.",
                n_obs, GESLA_OBS_DIR,
            )
            return

    if args.dry_run:
        logger.info("[DRY-RUN] Would download/extract GESLA-4 to %s", stations_dir)
        return

    # Import and invoke the download script's logic
    from scripts.data.download_gesla import extract_stations as _extract_stations

    zip_path = _find_gesla_zip(args.zip_file)

    if zip_path is None:
        if args.url:
            from config.settings import GESLA_ZIP_FILE
            zip_path = pathlib.Path(GESLA_ZIP_FILE)
            from scripts.data.download_gesla import _download_with_progress
            _download_with_progress(args.url, zip_path)
        else:
            logger.error(
                "No GESLA-4 ZIP found in %s and no --url provided.\n"
                "  Options:\n"
                "    • Pass --url <download-link>  (free registration at "
                "gesla787883612.wordpress.com)\n"
                "    • Pass --zip-file /path/to/GESLA4.zip\n"
                "    • Set the GESLA_ZIP_URL environment variable",
                GESLA_RAW_DIR,
            )
            sys.exit(1)

    logger.info("  Using ZIP: %s", zip_path)
    n_extracted, n_skipped, missing = _extract_stations(
        zip_path,
        SURGEMIP_STNLIST,
        stations_dir,
    )
    logger.info(
        "  Extraction complete: extracted=%d  skipped=%d  missing=%d",
        n_extracted, n_skipped, len(missing),
    )


# ---------------------------------------------------------------------------
# Stage 2 — Prepare GESLA observation CSVs
# ---------------------------------------------------------------------------

def stage2_prepare_gesla(
    station_list,
    n_workers: int,
    force: bool,
    dry_run: bool,
    zip_file_override: str | None = None,
) -> None:
    """
    Parse GESLA station files → per-station observation CSVs (parallel).

    Source resolution order:
      1. Extracted ``data/gesla/raw/stations/`` directory (preferred).
      2. Any ZIP archive found in ``data/gesla/raw/`` (used directly if
         the stations directory does not exist or is empty).
    """
    _stage_header(2, "Prepare GESLA observation CSVs")

    from scripts.data.prepare_gesla import process_station as _process_station

    stations_dir = GESLA_STATIONS_DIR
    out_dir      = GESLA_OBS_DIR

    # ---- Determine data source ----------------------------------------------
    n_stations = sum(1 for _ in stations_dir.glob("*")) if stations_dir.exists() else 0

    if n_stations > 0:
        source_desc  = str(stations_dir)
        use_zip      = False
        zip_handle   = None
    else:
        # Fall back to ZIP
        zip_path = _find_gesla_zip(zip_file_override)
        if zip_path is None:
            if dry_run:
                logger.info(
                    "[DRY-RUN] Would prepare %d stations "
                    "(no extracted dir found; would need ZIP or Stage 1)",
                    len(station_list),
                )
                return
            logger.error(
                "Stations directory not found (%s) and no ZIP available.\n"
                "  Run Stage 1 first, or pass --zip-file.",
                stations_dir,
            )
            sys.exit(1)
        source_desc = str(zip_path)
        use_zip     = True
        # zip_handle opened below (after dry-run check)

    if dry_run:
        logger.info("[DRY-RUN] Would prepare %d stations from %s", len(station_list), source_desc)
        return

    import zipfile as _zipfile

    zip_handle = _zipfile.ZipFile(zip_path, "r") if use_zip else None  # type: ignore[possibly-undefined]

    n_total = len(station_list)
    logger.info("  Processing %d stations from %s", n_total, source_desc)

    try:
        if use_zip:
            # ZIP is NOT thread-safe → process sequentially when using ZIP source
            logger.info(
                "  Using ZIP source — processing sequentially (ZIP is not thread-safe)."
            )
            counts: dict[str, int] = {}
            for _, row_s in station_list.iterrows():
                status = _process_station(
                    row=row_s.to_dict(),
                    stations_dir=None,
                    zip_handle=zip_handle,
                    out_dir=out_dir,
                    force=force,
                )
                counts[status] = counts.get(status, 0) + 1
        else:
            def _worker(row_dict: dict) -> str:
                return _process_station(
                    row=row_dict,
                    stations_dir=stations_dir,
                    zip_handle=None,
                    out_dir=out_dir,
                    force=force,
                )
            rows = [row.to_dict() for _, row in station_list.iterrows()]
            counts = _run_parallel(rows, _worker, desc="Stage 2", n_workers=n_workers, dry_run=False)
    finally:
        if zip_handle is not None:
            zip_handle.close()

    _log_counts(counts)


# ---------------------------------------------------------------------------
# Stage 3 — Extract model time series
# ---------------------------------------------------------------------------

def stage3_extract_model(
    station_list,
    n_workers: int,
    force: bool,
    dry_run: bool,
    t_start: str | None,
    t_end: str | None,
) -> None:
    """Extract eta_tide / eta_notide at each GESLA station (parallel)."""
    _stage_header(3, "Extract model time series for GESLA stations")

    import pandas as pd
    from utils.grads_reader import GrADSReader
    from scripts.validation.extract_model_for_gesla_stations import (
        extract_one_station as _extract_one,
        _dt_to_idx,
        haversine_km,
    )
    from config.settings import STATION_MODEL_INDEX

    if dry_run:
        logger.info("[DRY-RUN] Would open GrADS readers and extract %d stations", len(station_list))
        return

    logger.info("  Opening tide dataset …")
    tide_reader = GrADSReader(TIDE_CTL, verbose=False)
    logger.info("  Opening notide dataset …")
    notide_reader = GrADSReader(NOTIDE_CTL, verbose=False)

    t_start_idx = 0 if t_start is None else _dt_to_idx(tide_reader, t_start)
    t_end_idx   = tide_reader.nt if t_end is None else _dt_to_idx(tide_reader, t_end)

    logger.info(
        "  Time range: %s → %s  (%d steps)  workers=%d",
        tide_reader.times[t_start_idx],
        tide_reader.times[t_end_idx - 1],
        t_end_idx - t_start_idx,
        n_workers,
    )

    MODEL_TS_DIR.mkdir(parents=True, exist_ok=True)
    index_rows: list[dict] = []
    counts: dict[str, int] = {}

    def _worker(row_dict: dict) -> str:
        result = _extract_one(
            row=row_dict,
            tide_reader=tide_reader,
            notide_reader=notide_reader,
            t_start_idx=t_start_idx,
            t_end_idx=t_end_idx,
            out_dir=MODEL_TS_DIR,
            force=force,
        )
        if result is None:
            return "skipped"
        # Collect for index — thread-safe append (list.append is GIL-protected)
        index_rows.append(result)
        return "written"

    rows = [row.to_dict() for _, row in station_list.iterrows()]

    futures_map = {}
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        for row_dict in rows:
            fut = pool.submit(_worker, row_dict)
            futures_map[fut] = row_dict

        with _progress_bar(as_completed(futures_map), total=len(rows), desc="Stage 3") as pbar:
            for fut in pbar:
                try:
                    status = fut.result()
                except Exception as exc:
                    logger.error("Unhandled exception in Stage 3: %s", exc)
                    status = "error"
                counts[status] = counts.get(status, 0) + 1

    _log_counts(counts)

    # ---- Persist / update station-model index --------------------------------
    if index_rows:
        new_df = pd.DataFrame(index_rows)
        index_path = pathlib.Path(STATION_MODEL_INDEX)
        if index_path.exists() and not force:
            existing = pd.read_csv(index_path)
            combined = pd.concat(
                [
                    existing[~existing["station_file_name"].isin(new_df["station_file_name"])],
                    new_df,
                ],
                ignore_index=True,
            )
        else:
            combined = new_df
        index_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(index_path, index=False)
        logger.info("  Station-model index: %s  (%d rows)", index_path, len(combined))


# ---------------------------------------------------------------------------
# Stage 3.5 — Apply tidal detiding (only for non-raw modes)
# ---------------------------------------------------------------------------

def stage35_apply_detiding(
    modes: list[str],
    force: bool,
    dry_run: bool,
    n_workers: int,
    station: str | None,
) -> None:
    """
    Apply Godin filter and/or FES2022 tidal subtraction to GESLA observations.

    Only runs when at least one non-raw mode is selected.  The two methods are
    independent and can both run in the same call:

      godin   → data/processed/gesla/observations_godin/
      fes2022 → data/processed/gesla/observations_fes/

    Godin is parallelised via ThreadPoolExecutor; FES is sequential (NetCDF
    file I/O is not guaranteed to be thread-safe).
    """
    needs_godin = "godin_notide"   in modes
    needs_fes   = "fes2022_notide" in modes
    if not needs_godin and not needs_fes:
        return  # only raw mode selected — nothing to detide

    _stage_header(35, "Apply tidal detiding to GESLA observations")

    import subprocess

    script = _ROOT / "scripts" / "validation" / "apply_tidal_detiding.py"

    # Determine which methods to run
    if needs_godin and needs_fes:
        method = "all"
    elif needs_godin:
        method = "godin"
    else:
        method = "fes2022"

    cmd = [sys.executable, str(script), "--method", method, "--workers", str(n_workers)]
    if force:
        cmd.append("--force")
    if dry_run:
        cmd.append("--dry-run")
    if station:
        cmd += ["--station", station]

    logger.info("  Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("  apply_tidal_detiding.py exited with code %d", result.returncode)


# ---------------------------------------------------------------------------
# Stage 4 — Build comparison CSVs  (mode-aware)
# ---------------------------------------------------------------------------

def stage4_build_comparison(
    station_list,
    n_workers: int,
    force: bool,
    dry_run: bool,
    t_start: str | None,
    t_end: str | None,
    mode_label: str,
    mode_cfg: dict,
) -> None:
    """Merge GESLA obs + model TS → final comparison CSVs (parallel)."""
    _stage_header(4, f"Build comparison CSVs  [{mode_label}]")

    from scripts.validation.build_comparison_csvs import merge_one_station as _merge

    obs_dir   = pathlib.Path(mode_cfg["obs_dir"])
    model_dir = MODEL_TS_DIR
    out_dir   = pathlib.Path(mode_cfg["comp_dir"])

    if not obs_dir.exists():
        logger.warning("  obs_dir not found: %s — skipping stage 4 for mode '%s'", obs_dir, mode_label)
        return

    # f.stem on "foo.csv.gz" returns "foo.csv" — strip both suffixes
    file_names = sorted(f.with_suffix("").stem for f in obs_dir.glob("*.csv.gz"))

    # Intersect with station_list file names
    stn_set = set(station_list["file_name"].str.strip())
    file_names = [fn for fn in file_names if fn in stn_set]

    logger.info("  Merging %d stations  (obs: %s)", len(file_names), obs_dir)

    def _worker(file_name: str) -> str:
        return _merge(
            file_name=file_name,
            obs_dir=obs_dir,
            model_dir=model_dir,
            out_dir=out_dir,
            t_start=t_start,
            t_end=t_end,
            resample_freq=None,
            force=force,
        )

    counts = _run_parallel(file_names, _worker, desc=f"Stage 4 [{mode_label}]",
                           n_workers=n_workers, dry_run=dry_run)
    _log_counts(counts)


# ---------------------------------------------------------------------------
# Stage 4b — Build tide-derived comparison CSVs (godin_tide / fes2022_tide)
# ---------------------------------------------------------------------------

def stage4b_build_tide_modes(
    force: bool,
    dry_run: bool,
    mode_label: str,
    station: str | None,
    n_workers: int,
) -> None:
    """Build comparison CSVs for godin_tide / fes2022_tide by post-processing
    existing godin_notide / fes2022_notide comparison CSVs."""
    _stage_header(4, f"Build tide-derived comparison CSVs  [{mode_label}]")

    if dry_run:
        logger.info("[DRY-RUN] Would run build_tide_mode_comparisons.py --mode %s", mode_label)
        return

    import subprocess
    script = _ROOT / "scripts" / "validation" / "build_tide_mode_comparisons.py"
    cmd = [
        sys.executable, str(script),
        "--mode", mode_label,
        "--workers", str(n_workers),
    ]
    if force:
        cmd.append("--force")
    if station:
        cmd += ["--station", station]

    logger.info("  Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("  build_tide_mode_comparisons.py exited with code %d", result.returncode)


# ---------------------------------------------------------------------------
# Stage 5 — Compute station metrics  (mode-aware)
# ---------------------------------------------------------------------------

def stage5_compute_metrics(
    force: bool,
    dry_run: bool,
    mode_label: str,
    mode_cfg: dict,
) -> None:
    """Compute per-station skill metrics and save to results/validation/."""
    _stage_header(5, f"Compute per-station validation metrics  [{mode_label}]")

    metrics_csv = pathlib.Path(mode_cfg["metrics_csv"])
    comp_dir    = pathlib.Path(mode_cfg["comp_dir"])
    targets     = mode_cfg["targets"]

    if metrics_csv.exists() and not force:
        logger.info(
            "  Metrics already exist at %s — skipping.  Use --force-metrics to recompute.",
            metrics_csv,
        )
        return

    if not comp_dir.exists() or not any(comp_dir.glob("*.csv.gz")):
        logger.warning(
            "  No comparison CSVs in %s — skipping stage 5 for mode '%s'.",
            comp_dir, mode_label,
        )
        return

    if dry_run:
        logger.info("[DRY-RUN] Would compute metrics → %s", metrics_csv)
        return

    import subprocess
    script = _ROOT / "scripts" / "validation" / "compute_station_metrics.py"
    cmd = [
        sys.executable, str(script),
        "--comp-dir", str(comp_dir),
        "--out",      str(metrics_csv),
        "--targets",  targets,
    ]
    if force:
        cmd.append("--force")
    logger.info("  Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("  compute_station_metrics.py exited with code %d", result.returncode)


# ---------------------------------------------------------------------------
# Stage 6 — Generate maps  (mode-aware)
# ---------------------------------------------------------------------------

# Metrics available per target set
_METRICS_NOTIDE_ONLY = [
    "rmse_notide", "bias_notide", "pearson_r_notide",
    "obs_mean_m",  "model_notide_mean_m",
    "obs_max_m",   "model_notide_max_m",
    "n_valid",
]
_METRICS_BOTH = _METRICS_NOTIDE_ONLY + [
    "rmse_tide",   "bias_tide",   "pearson_r_tide",
    "model_tide_mean_m", "model_tide_max_m",
]


def stage6_generate_maps(
    force: bool,
    dry_run: bool,
    mode_label: str,
    mode_cfg: dict,
) -> None:
    """Generate validation maps coloured by per-station metrics."""
    _stage_header(6, f"Generate station validation maps  [{mode_label}]")

    metrics_csv = pathlib.Path(mode_cfg["metrics_csv"])
    fig_dir     = pathlib.Path(mode_cfg["fig_dir"])
    targets     = mode_cfg["targets"]

    # Choose which metrics to plot based on active targets.
    # targets is a comma-separated string like "notide,tide" or "notide".
    # Use set membership to avoid the "tide" in "notide" substring trap.
    target_set = {t.strip() for t in targets.split(",")}
    metrics = _METRICS_BOTH if "tide" in target_set else _METRICS_NOTIDE_ONLY

    if dry_run:
        logger.info("[DRY-RUN] Would generate %d maps → %s", len(metrics), fig_dir)
        return

    if not metrics_csv.exists():
        logger.warning(
            "  Metrics file not found: %s — skipping stage 6 for mode '%s'.",
            metrics_csv, mode_label,
        )
        return

    import subprocess
    script = _ROOT / "scripts" / "validation" / "plot_station_metric_map.py"

    for metric in metrics:
        out_file = fig_dir / f"station_map_{metric}.png"
        if out_file.exists() and not force:
            logger.debug("  SKIP (exists): %s", out_file.name)
            continue
        cmd = [
            sys.executable, str(script),
            "--metric",       metric,
            "--metrics-file", str(metrics_csv),
            "--out-dir",      str(fig_dir),
        ]
        if force:
            cmd.append("--force")
        logger.info("  Plotting metric: %s", metric)
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            logger.warning("  plot_station_metric_map.py failed for metric '%s'", metric)

    logger.info("  Maps saved to %s", fig_dir)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    # Expand --force-all (also covers detiding)
    if args.force_all:
        args.force_download = True
        args.force_prepare  = True
        args.force_extract  = True
        args.force_detide   = True
        args.force_build    = True
        args.force_metrics  = True
        args.force_maps     = True

    # Expand "all" mode to all five concrete modes
    selected_modes: list[str] = []
    for m in args.mode:
        if m == "all":
            selected_modes.extend([
                "raw_tide", "godin_notide", "fes2022_notide",
                "godin_tide", "fes2022_tide",
            ])
        else:
            selected_modes.append(m)
    # Deduplicate while preserving order
    seen: set[str] = set()
    active_modes: list[str] = []
    for m in selected_modes:
        if m not in seen:
            active_modes.append(m)
            seen.add(m)

    t0 = time.time()

    logger.info("=" * 64)
    logger.info("  POM / GESLA-4 Validation Pipeline")
    logger.info("  modes=%s   workers=%d   dry_run=%s",
                active_modes, args.workers, args.dry_run)
    logger.info("=" * 64)

    # ---- Load station list --------------------------------------------------
    station_list = load_station_list(str(SURGEMIP_STNLIST))
    if args.station:
        mask = station_list["file_name"] == args.station
        if not mask.any():
            logger.error("Station '%s' not found in station list.", args.station)
            sys.exit(1)
        station_list = station_list[mask]
        logger.info("Single-station mode: %s", args.station)
    logger.info("Station list: %d stations", len(station_list))

    # ---- Stage 1 — always runs ---------------------------------------------
    stage1_check_gesla(args)

    # ---- Stage 2 — always runs ---------------------------------------------
    stage2_prepare_gesla(
        station_list=station_list,
        n_workers=args.workers,
        force=args.force_prepare,
        dry_run=args.dry_run,
        zip_file_override=args.zip_file,
    )

    # ---- Stage 3 — always runs ---------------------------------------------
    stage3_extract_model(
        station_list=station_list,
        n_workers=args.workers,
        force=args.force_extract,
        dry_run=args.dry_run,
        t_start=args.t_start,
        t_end=args.t_end,
    )

    # ---- Stage 3.5 — tidal detiding (only for non-raw modes) ---------------
    stage35_apply_detiding(
        modes=active_modes,
        force=args.force_detide,
        dry_run=args.dry_run,
        n_workers=args.workers,
        station=args.station,
    )

    # ---- Stages 4–6: loop over each selected validation mode ----------------
    for mode_label in active_modes:
        mode_cfg = VALIDATION_MODES[mode_label]

        if mode_cfg.get("tide_derived", False):
            # godin_tide / fes2022_tide: comparison CSVs are derived from
            # existing godin_notide / fes2022_notide CSVs — use stage 4b.
            stage4b_build_tide_modes(
                force=args.force_build,
                dry_run=args.dry_run,
                mode_label=mode_label,
                station=args.station,
                n_workers=args.workers,
            )
        else:
            stage4_build_comparison(
                station_list=station_list,
                n_workers=args.workers,
                force=args.force_build,
                dry_run=args.dry_run,
                t_start=args.t_start,
                t_end=args.t_end,
                mode_label=mode_label,
                mode_cfg=mode_cfg,
            )

        stage5_compute_metrics(
            force=args.force_metrics,
            dry_run=args.dry_run,
            mode_label=mode_label,
            mode_cfg=mode_cfg,
        )

        stage6_generate_maps(
            force=args.force_maps,
            dry_run=args.dry_run,
            mode_label=mode_label,
            mode_cfg=mode_cfg,
        )

    # ---- Final summary ------------------------------------------------------
    elapsed = time.time() - t0
    logger.info("")
    logger.info("=" * 64)
    logger.info("  Pipeline complete in %.1f s (%.1f min)", elapsed, elapsed / 60)
    for mode_label in active_modes:
        cfg = VALIDATION_MODES[mode_label]
        logger.info("  [%s]", mode_label)
        logger.info("    Comparison CSVs : %s", cfg["comp_dir"])
        logger.info("    Metrics         : %s", cfg["metrics_csv"])
        logger.info("    Maps            : %s", cfg["fig_dir"])
    logger.info("=" * 64)


if __name__ == "__main__":
    main()
