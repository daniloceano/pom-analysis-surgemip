"""
run_gesla_validation_pipeline.py
=================================
One-command orchestrator for the full GESLA-4 / POM validation workflow.

Stages
------
  Stage 1 – Check / download / extract GESLA-4 data
  Stage 2 – Prepare GESLA observation CSVs            (parallelised)
  Stage 3 – Extract model time series for all stations (parallelised)
  Stage 4 – Build observation-vs-model comparison CSVs (parallelised)
  Stage 5 – Compute per-station validation metrics
  Stage 6 – Generate station-map figures

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
    # Run full pipeline (skip completed stages):
    python scripts/pipeline/run_gesla_validation_pipeline.py

    # Run with 50 parallel workers (default):
    python scripts/pipeline/run_gesla_validation_pipeline.py --workers 50

    # Dry-run (show what would be done, do nothing):
    python scripts/pipeline/run_gesla_validation_pipeline.py --dry-run

    # Force everything from scratch:
    python scripts/pipeline/run_gesla_validation_pipeline.py --force-all

    # Force only Stage 3 and later:
    python scripts/pipeline/run_gesla_validation_pipeline.py \\
        --force-extract --force-build --force-metrics --force-maps

    # Restrict to a time window (passed to stages 3 & 4):
    python scripts/pipeline/run_gesla_validation_pipeline.py \\
        --t-start 2016-01-01 --t-end 2017-01-01

    # Restrict to a single station (useful for testing):
    python scripts/pipeline/run_gesla_validation_pipeline.py \\
        --station san_francisco_ca-551a-usa-uhslc

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
    VALIDATION_DIR,
    GESLA_VS_MODEL_DIR,
    STATION_METRICS_CSV,
    FIG_VALID_DIR,
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

MODEL_TS_DIR = VALIDATION_DIR / "model_ts"
GESLA_STATIONS_DIR = GESLA_RAW_DIR / "stations"


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
# Stage 4 — Build comparison CSVs
# ---------------------------------------------------------------------------

def stage4_build_comparison(
    station_list,
    n_workers: int,
    force: bool,
    dry_run: bool,
    t_start: str | None,
    t_end: str | None,
) -> None:
    """Merge GESLA obs + model TS → final comparison CSVs (parallel)."""
    _stage_header(4, "Build observation-vs-model comparison CSVs")

    from scripts.validation.build_comparison_csvs import merge_one_station as _merge

    obs_dir   = GESLA_OBS_DIR
    model_dir = MODEL_TS_DIR
    out_dir   = GESLA_VS_MODEL_DIR

    # f.stem on "foo.csv.gz" returns "foo.csv" — strip both suffixes
    file_names = sorted(
        f.with_suffix("").stem for f in obs_dir.glob("*.csv.gz")
    ) if obs_dir.exists() else []

    # Filter to single station if requested
    if hasattr(station_list, '_station_filter') and station_list._station_filter:
        stn = station_list._station_filter
        file_names = [stn] if stn in file_names else []

    # Intersect with station_list file names
    stn_set = set(station_list["file_name"].str.strip())
    file_names = [fn for fn in file_names if fn in stn_set]

    logger.info("  Merging %d stations", len(file_names))

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

    counts = _run_parallel(file_names, _worker, desc="Stage 4", n_workers=n_workers, dry_run=dry_run)
    _log_counts(counts)


# ---------------------------------------------------------------------------
# Stage 5 — Compute station metrics
# ---------------------------------------------------------------------------

def stage5_compute_metrics(force: bool, dry_run: bool) -> None:
    """Compute per-station skill metrics and save to results/validation/."""
    _stage_header(5, "Compute per-station validation metrics")

    if STATION_METRICS_CSV.exists() and not force:
        logger.info(
            "  Metrics already exist at %s — skipping.  Use --force-metrics to recompute.",
            STATION_METRICS_CSV,
        )
        return

    if dry_run:
        logger.info("[DRY-RUN] Would compute metrics → %s", STATION_METRICS_CSV)
        return

    import subprocess
    script = _ROOT / "scripts" / "validation" / "compute_station_metrics.py"
    cmd = [sys.executable, str(script)]
    if force:
        cmd.append("--force")
    logger.info("  Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("  compute_station_metrics.py exited with code %d", result.returncode)


# ---------------------------------------------------------------------------
# Stage 6 — Generate maps
# ---------------------------------------------------------------------------

def stage6_generate_maps(force: bool, dry_run: bool) -> None:
    """Generate validation maps coloured by per-station metrics."""
    _stage_header(6, "Generate station validation maps")

    if dry_run:
        logger.info("[DRY-RUN] Would generate maps → %s", FIG_VALID_DIR)
        return

    if not STATION_METRICS_CSV.exists():
        logger.error(
            "  Metrics file not found: %s\n  Run Stage 5 first.",
            STATION_METRICS_CSV,
        )
        return

    import subprocess
    script = _ROOT / "scripts" / "validation" / "plot_station_metric_map.py"
    metrics = [
        "rmse_notide", "bias_notide", "pearson_r_notide",
        "rmse_tide",   "bias_tide",   "pearson_r_tide",
        "obs_mean_m",  "model_notide_mean_m", "model_tide_mean_m",
        "obs_max_m",   "model_notide_max_m",  "model_tide_max_m",
        "n_valid",
    ]
    for metric in metrics:
        out_file = FIG_VALID_DIR / f"station_map_{metric}.png"
        if out_file.exists() and not force:
            logger.debug("  SKIP (exists): %s", out_file.name)
            continue
        cmd = [sys.executable, str(script), "--metric", metric]
        if force:
            cmd.append("--force")
        logger.info("  Plotting metric: %s", metric)
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            logger.warning("  plot_station_metric_map.py failed for metric '%s'", metric)

    logger.info("  Maps saved to %s", FIG_VALID_DIR)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    # Expand --force-all
    if args.force_all:
        args.force_download = True
        args.force_prepare  = True
        args.force_extract  = True
        args.force_build    = True
        args.force_metrics  = True
        args.force_maps     = True

    t0 = time.time()

    logger.info("=" * 64)
    logger.info("  POM / GESLA-4 Validation Pipeline")
    logger.info("  workers=%d   dry_run=%s", args.workers, args.dry_run)
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

    # ---- Stage 1 ------------------------------------------------------------
    stage1_check_gesla(args)

    # ---- Stage 2 ------------------------------------------------------------
    stage2_prepare_gesla(
        station_list=station_list,
        n_workers=args.workers,
        force=args.force_prepare,
        dry_run=args.dry_run,
        zip_file_override=args.zip_file,
    )

    # ---- Stage 3 ------------------------------------------------------------
    stage3_extract_model(
        station_list=station_list,
        n_workers=args.workers,
        force=args.force_extract,
        dry_run=args.dry_run,
        t_start=args.t_start,
        t_end=args.t_end,
    )

    # ---- Stage 4 ------------------------------------------------------------
    stage4_build_comparison(
        station_list=station_list,
        n_workers=args.workers,
        force=args.force_build,
        dry_run=args.dry_run,
        t_start=args.t_start,
        t_end=args.t_end,
    )

    # ---- Stage 5 ------------------------------------------------------------
    stage5_compute_metrics(force=args.force_metrics, dry_run=args.dry_run)

    # ---- Stage 6 ------------------------------------------------------------
    stage6_generate_maps(force=args.force_maps, dry_run=args.dry_run)

    # ---- Final summary ------------------------------------------------------
    elapsed = time.time() - t0
    logger.info("")
    logger.info("=" * 64)
    logger.info("  Pipeline complete in %.1f s (%.1f min)", elapsed, elapsed / 60)
    logger.info("  Comparison CSVs : %s", GESLA_VS_MODEL_DIR)
    logger.info("  Metrics         : %s", STATION_METRICS_CSV)
    logger.info("  Maps            : %s", FIG_VALID_DIR)
    logger.info("=" * 64)


if __name__ == "__main__":
    main()
