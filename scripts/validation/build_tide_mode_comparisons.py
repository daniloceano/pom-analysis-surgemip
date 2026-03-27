"""
build_tide_mode_comparisons.py
==============================
Build comparison CSVs for the two validation modes that compare detided
observations against *detided POM_tide*:

  godin_tide   — obs_godin  vs  POM_tide_godin  (both Godin-filtered)
  fes2022_tide — obs_fes    vs  POM_tide_fes    (both FES2022-detided)

These modes complement the existing modes (godin_notide, fes2022_notide)
that compare detided observations against POM_notide.

Scientific rationale
--------------------
* godin_tide: applying the Godin low-pass filter to both the observed and
  modelled sea level removes the tidal signal from both records.  The
  residual of POM_tide after Godin filtering is approximately equal to
  POM_notide, because the tidal component (period < ~30 h) is attenuated
  by > 98 %.  Small differences arise from very long-period tidal harmonics
  (Mf ~13.7 d, Mm ~27.6 d) and edge effects.

* fes2022_tide: subtracting the same FES2022 tidal prediction from both
  the observed and modelled sea level.  Since POM_tide was forced with
  FES2022 tidal harmonics, POM_tide - FES2022 ≈ POM_notide.  This mode
  verifies that both approaches yield consistent results.

Data flow
---------
godin_tide:
  input : data/processed/validation/godin_notide/gesla_vs_model/<stn>.csv.gz
          (contains sea_level_obs_m=obs_godin + model_eta_tide_m)
  step  : apply godin_filter() to model_eta_tide_m column
  output: data/processed/validation/godin_tide/gesla_vs_model/<stn>.csv.gz
          columns: sea_level_obs_m (obs_godin), model_eta_notide_m (POM_tide_godin)

fes2022_tide:
  input : data/processed/validation/fes2022_notide/gesla_vs_model/<stn>.csv.gz
          (contains sea_level_obs_m=obs_fes + model_eta_tide_m)
        + data/processed/gesla/observations_fes/<stn>.csv.gz
          (contains fes_tide_m)
  step  : model_eta_tide_fes = model_eta_tide_m - fes_tide_m
  output: data/processed/validation/fes2022_tide/gesla_vs_model/<stn>.csv.gz
          columns: sea_level_obs_m (obs_fes), model_eta_notide_m (POM_tide_fes)

Output column convention
------------------------
Both output CSVs use `model_eta_notide_m` for the detided model target.
This is intentional: the column holds the "surge-equivalent" signal
(tidal component removed) and allows reuse of compute_station_metrics.py
without modification.  The mode name (godin_tide / fes2022_tide) clarifies
the physical meaning of the comparison.

Usage
-----
    python scripts/validation/build_tide_mode_comparisons.py
    python scripts/validation/build_tide_mode_comparisons.py --mode godin_tide
    python scripts/validation/build_tide_mode_comparisons.py --force
    python scripts/validation/build_tide_mode_comparisons.py --workers 50
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    VALID_GODIN_DIR,
    VALID_FES_DIR,
    GESLA_OBS_FES_DIR,
    VALIDATION_DIR,
)
from utils.tidal_filters import godin_filter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Output comparison directories for the new modes
VALID_GODIN_TIDE_DIR = VALIDATION_DIR / "godin_tide"    / "gesla_vs_model"
VALID_FES_TIDE_DIR   = VALIDATION_DIR / "fes2022_tide"  / "gesla_vs_model"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--mode",
        nargs="+",
        default=["godin_tide", "fes2022_tide"],
        choices=["godin_tide", "fes2022_tide", "all"],
        help="Modes to build (default: both).",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Number of parallel workers (default: 50).",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output files.",
    )
    p.add_argument(
        "--station",
        default=None,
        help="Process only this one station (for testing).",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# godin_tide: apply Godin filter to model_eta_tide_m
# ---------------------------------------------------------------------------

def _build_godin_tide_one(
    file_name: str,
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    force: bool,
) -> str:
    out_file = out_dir / f"{file_name}.csv.gz"
    if out_file.exists() and not force:
        return "skipped"

    in_file = in_dir / f"{file_name}.csv.gz"
    if not in_file.exists():
        return "input_missing"

    try:
        df = pd.read_csv(in_file, compression="gzip", parse_dates=["datetime_utc"],
                         index_col="datetime_utc", low_memory=False)
    except Exception as exc:
        logger.error("  READ ERROR %s: %s", file_name, exc)
        return "error"

    if "model_eta_tide_m" not in df.columns:
        logger.debug("  NO model_eta_tide_m in %s", file_name)
        return "error"

    # Apply Godin filter to model_eta_tide_m.
    # The comparison CSV is a left-join of obs×model; the model column may
    # have gaps where obs timestamps didn't match any model step.
    # godin_filter handles NaN by propagating them (min_periods=1).
    try:
        model_tide_godin = godin_filter(
            df["model_eta_tide_m"],
            min_periods=1,
            check_hourly=False,   # obs timestamps may not be exactly hourly
        )
    except Exception as exc:
        logger.error("  GODIN FILTER ERROR %s: %s", file_name, exc)
        return "error"

    # Build output DataFrame keeping obs and metadata, replacing model target
    meta_cols = [c for c in df.columns if c not in (
        "model_eta_tide_m", "model_eta_notide_m", "model_tide_minus_notide_m",
    )]
    out = df[meta_cols].copy()
    out["model_eta_notide_m"] = model_tide_godin  # POM_tide_godin stored as notide

    out_dir.mkdir(parents=True, exist_ok=True)
    out.reset_index().to_csv(out_file, index=False, compression="gzip")
    return "written"


# ---------------------------------------------------------------------------
# fes2022_tide: subtract FES2022 from model_eta_tide_m
# ---------------------------------------------------------------------------

def _build_fes_tide_one(
    file_name: str,
    in_dir: pathlib.Path,
    fes_obs_dir: pathlib.Path,
    out_dir: pathlib.Path,
    force: bool,
) -> str:
    out_file = out_dir / f"{file_name}.csv.gz"
    if out_file.exists() and not force:
        return "skipped"

    in_file      = in_dir     / f"{file_name}.csv.gz"
    fes_obs_file = fes_obs_dir / f"{file_name}.csv.gz"

    if not in_file.exists():
        return "input_missing"
    if not fes_obs_file.exists():
        logger.debug("  FES obs missing for %s", file_name)
        return "fes_obs_missing"

    try:
        comp = pd.read_csv(in_file, compression="gzip", parse_dates=["datetime_utc"],
                           index_col="datetime_utc", low_memory=False)
        fes_obs = pd.read_csv(fes_obs_file, compression="gzip",
                              parse_dates=["datetime_utc"],
                              index_col="datetime_utc", low_memory=False)
    except Exception as exc:
        logger.error("  READ ERROR %s: %s", file_name, exc)
        return "error"

    if "model_eta_tide_m" not in comp.columns:
        return "error"
    if "fes_tide_m" not in fes_obs.columns:
        logger.debug("  NO fes_tide_m in %s", file_name)
        return "error"

    # Align fes_tide_m to the comparison DataFrame's index
    fes_tide = fes_obs["fes_tide_m"].reindex(comp.index)

    # model_eta_tide_fes = model_eta_tide_m - fes_tide_m
    model_tide_fes = comp["model_eta_tide_m"] - fes_tide

    # Build output DataFrame
    meta_cols = [c for c in comp.columns if c not in (
        "model_eta_tide_m", "model_eta_notide_m", "model_tide_minus_notide_m",
    )]
    out = comp[meta_cols].copy()
    out["model_eta_notide_m"] = model_tide_fes  # POM_tide_fes stored as notide

    out_dir.mkdir(parents=True, exist_ok=True)
    out.reset_index().to_csv(out_file, index=False, compression="gzip")
    return "written"


# ---------------------------------------------------------------------------
# Parallel runners
# ---------------------------------------------------------------------------

def _run_parallel(items, worker_fn, desc, n_workers):
    counts: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(worker_fn, item): item for item in items}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                status = fut.result()
            except Exception as exc:
                logger.error("Unhandled exception: %s", exc)
                status = "error"
            counts[status] = counts.get(status, 0) + 1
            if i % 100 == 0 or i == len(items):
                logger.info("  %s  %d/%d  %s", desc, i, len(items),
                            "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    return counts


def build_godin_tide(file_names, force, n_workers):
    logger.info("Building godin_tide comparison CSVs (%d stations)…", len(file_names))
    VALID_GODIN_TIDE_DIR.mkdir(parents=True, exist_ok=True)

    def worker(fn):
        return _build_godin_tide_one(fn, VALID_GODIN_DIR, VALID_GODIN_TIDE_DIR, force)

    counts = _run_parallel(file_names, worker, "godin_tide", n_workers)
    logger.info("  godin_tide done: %s", counts)
    return counts


def build_fes_tide(file_names, force, n_workers):
    logger.info("Building fes2022_tide comparison CSVs (%d stations)…", len(file_names))
    VALID_FES_TIDE_DIR.mkdir(parents=True, exist_ok=True)

    def worker(fn):
        return _build_fes_tide_one(fn, VALID_FES_DIR, GESLA_OBS_FES_DIR,
                                   VALID_FES_TIDE_DIR, force)

    counts = _run_parallel(file_names, worker, "fes2022_tide", n_workers)
    logger.info("  fes2022_tide done: %s", counts)
    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    modes = set()
    for m in args.mode:
        if m == "all":
            modes.update(["godin_tide", "fes2022_tide"])
        else:
            modes.add(m)

    # Enumerate stations from the input directories
    godin_files = sorted(
        f.with_suffix("").stem
        for f in VALID_GODIN_DIR.glob("*.csv.gz")
    ) if VALID_GODIN_DIR.exists() else []

    fes_files = sorted(
        f.with_suffix("").stem
        for f in VALID_FES_DIR.glob("*.csv.gz")
    ) if VALID_FES_DIR.exists() else []

    if args.station:
        godin_files = [args.station] if args.station in godin_files else []
        fes_files   = [args.station] if args.station in fes_files   else []

    if "godin_tide" in modes:
        if not godin_files:
            logger.warning(
                "No godin_notide comparison CSVs found in %s.\n"
                "Run the pipeline with --mode godin_notide first.",
                VALID_GODIN_DIR,
            )
        else:
            build_godin_tide(godin_files, args.force, args.workers)

    if "fes2022_tide" in modes:
        if not fes_files:
            logger.warning(
                "No fes2022_notide comparison CSVs found in %s.\n"
                "Run the pipeline with --mode fes2022_notide first.",
                VALID_FES_DIR,
            )
        else:
            build_fes_tide(fes_files, args.force, args.workers)

    logger.info("Done.")


if __name__ == "__main__":
    main()
