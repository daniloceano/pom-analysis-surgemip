"""
apply_tidal_detiding.py
=======================
Remove the astronomical tidal signal from GESLA observation CSVs and write
new per-station CSVs ready for de-tided validation.

Two independent methods are supported:

  godin_filter
    Apply the Godin (1972) low-pass filter (24 h + 24 h + 25 h running
    means) to each observation series.  Returns the *subtidal* signal
    (storm surge + mean sea level).  No external model is required.

  minus_fes_tide
    Predict the astronomical tide at each station location with the FES2022
    harmonic model (via ``eo-tides`` / ``pyTMD``) and subtract it from the
    observation.  Requires the clipped tide-model NetCDF files at
    ``data/tide_models_clipped_brasil/fes2022b/``.

Output schema
-------------
The output CSVs have the same column schema as the raw observation CSVs with
two modifications:

  * ``sea_level_obs_m``     – replaced by the de-tided value
  * ``sea_level_obs_raw_m`` – original (tidal) sea level, preserved for
                              reference and provenance

For ``minus_fes_tide`` an additional column is written:
  * ``fes_tide_m``          – predicted astronomical tide [m]

Output directories
------------------
  godin_filter   → data/processed/gesla/observations_godin/
  minus_fes_tide → data/processed/gesla/observations_fes/

Usage
-----
    # Both methods (default):
    python scripts/validation/apply_tidal_detiding.py

    # Single method:
    python scripts/validation/apply_tidal_detiding.py --method godin_filter
    python scripts/validation/apply_tidal_detiding.py --method minus_fes_tide

    # Single station (useful for testing):
    python scripts/validation/apply_tidal_detiding.py \\
        --method godin_filter --station san_francisco_ca-551a-usa-uhslc

    # Overwrite existing outputs:
    python scripts/validation/apply_tidal_detiding.py --force

    # Show what would be done without writing:
    python scripts/validation/apply_tidal_detiding.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    GESLA_OBS_DIR,
    GESLA_OBS_GODIN_DIR,
    GESLA_OBS_FES_DIR,
    TIDE_MODELS_DIR,
    FES_MODEL_NAME,
)
from utils.tidal_filters import godin_filter, predict_fes_tide

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm as _tqdm
    _TQDM = True
except ImportError:
    _TQDM = False


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--method",
        default="all",
        choices=["all", "godin_filter", "minus_fes_tide"],
        help=(
            "Detiding method to apply.  'all' runs both methods.  "
            "(default: all)"
        ),
    )
    p.add_argument(
        "--obs-dir",
        default=str(GESLA_OBS_DIR),
        help=f"Directory with raw observation CSVs. (default: {GESLA_OBS_DIR})",
    )
    p.add_argument(
        "--station",
        default=None,
        help="Process only this station (file name without extension).",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Parallel threads for godin_filter.  FES is always sequential. (default: 50)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output files.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without writing files.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Per-station detiding helpers
# ---------------------------------------------------------------------------

def _read_obs(path: pathlib.Path) -> pd.DataFrame | None:
    """Read a gzipped observation CSV; return None on failure."""
    try:
        df = pd.read_csv(path, compression="gzip", parse_dates=["datetime_utc"])
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=False)
        return df
    except Exception as exc:
        logger.error("  Cannot read %s: %s", path.name, exc)
        return None


def detide_godin(
    file_name: str,
    obs_dir: pathlib.Path,
    out_dir: pathlib.Path,
    force: bool,
) -> str:
    """
    Apply Godin filter to one station and save the result.

    Returns
    -------
    str : "written" | "skipped" | "obs_missing" | "empty" | "error"
    """
    out_path = out_dir / f"{file_name}.csv.gz"
    if out_path.exists() and not force:
        return "skipped"

    obs_path = obs_dir / f"{file_name}.csv.gz"
    if not obs_path.exists():
        return "obs_missing"

    df = _read_obs(obs_path)
    if df is None or df.empty:
        return "empty"

    try:
        raw = df["sea_level_obs_m"].copy()

        # --- Validate temporal resolution before filtering -------------------
        if isinstance(df["datetime_utc"].dtype, object) or pd.api.types.is_datetime64_any_dtype(df["datetime_utc"]):
            df = df.set_index("datetime_utc")
            diffs_min = df.index.to_series().diff().dropna().dt.total_seconds() / 60.0
            median_step = float(diffs_min.median()) if len(diffs_min) > 0 else 60.0
        else:
            df = df.set_index("datetime_utc")
            median_step = 60.0

        if abs(median_step - 60.0) > 30.0:
            logger.warning(
                "  %s: median step %.1f min (expected 60 min).  "
                "Godin filter may not correctly remove tides.  Skipping.",
                file_name, median_step,
            )
            return "non_hourly"

        filtered = godin_filter(df["sea_level_obs_m"], check_hourly=False)

        # Reset index so datetime_utc is a column again
        df = df.reset_index()
        df["sea_level_obs_raw_m"] = raw.values
        df["sea_level_obs_m"]     = filtered.values

        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, compression="gzip")
        return "written"

    except Exception as exc:
        logger.error("  %s: Godin filter error: %s", file_name, exc)
        return "error"


def detide_fes(
    file_name: str,
    obs_dir: pathlib.Path,
    out_dir: pathlib.Path,
    tide_models_dir: pathlib.Path,
    fes_model: str,
    force: bool,
) -> str:
    """
    Predict FES tide and subtract from one station; save the result.

    Returns
    -------
    str : "written" | "skipped" | "obs_missing" | "empty" | "no_coords" | "error"
    """
    out_path = out_dir / f"{file_name}.csv.gz"
    if out_path.exists() and not force:
        return "skipped"

    obs_path = obs_dir / f"{file_name}.csv.gz"
    if not obs_path.exists():
        return "obs_missing"

    df = _read_obs(obs_path)
    if df is None or df.empty:
        return "empty"

    # Station coordinates come from the observation CSV metadata columns
    if "station_lon" not in df.columns or "station_lat" not in df.columns:
        logger.warning("  %s: no station_lon/station_lat columns found.", file_name)
        return "no_coords"

    lon = float(df["station_lon"].iloc[0])
    lat = float(df["station_lat"].iloc[0])

    try:
        df = df.set_index("datetime_utc")
        times = pd.DatetimeIndex(df.index)

        tide = predict_fes_tide(
            times=times,
            lon=lon,
            lat=lat,
            directory=tide_models_dir,
            model=fes_model,
        )

        # Align tide to obs index (exact match on UTC timestamp)
        tide_aligned = tide.reindex(df.index)
        n_missing = int(tide_aligned.isna().sum())
        if n_missing > 0:
            logger.debug(
                "  %s: %d/%d timestamps had no FES tide prediction (NaN).",
                file_name, n_missing, len(df),
            )

        df = df.reset_index()
        df["sea_level_obs_raw_m"] = df["sea_level_obs_m"].values
        df["fes_tide_m"]           = tide_aligned.values
        df["sea_level_obs_m"]      = df["sea_level_obs_raw_m"] - df["fes_tide_m"]

        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, compression="gzip")
        return "written"

    except Exception as exc:
        logger.error("  %s: FES detiding error: %s", file_name, exc)
        return "error"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _log_counts(label: str, counts: dict[str, int]) -> None:
    logger.info("  [%s] results:", label)
    for status, n in sorted(counts.items()):
        logger.info("    %-20s : %d", status, n)


def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    obs_dir = pathlib.Path(args.obs_dir)
    if not obs_dir.exists():
        logger.error("Observation directory not found: %s", obs_dir)
        sys.exit(1)

    # Collect station list
    all_files = sorted(f.with_suffix("").stem for f in obs_dir.glob("*.csv.gz"))
    if args.station:
        if args.station not in all_files:
            logger.error("Station '%s' not found in %s", args.station, obs_dir)
            sys.exit(1)
        all_files = [args.station]

    logger.info("Found %d stations in %s", len(all_files), obs_dir)

    run_godin = args.method in ("all", "godin_filter")
    run_fes   = args.method in ("all", "minus_fes_tide")

    # ---- Godin filter (parallel via ThreadPoolExecutor) --------------------
    if run_godin:
        logger.info("")
        logger.info("── Godin filter (%d stations) → %s", len(all_files), GESLA_OBS_GODIN_DIR)

        if args.dry_run:
            logger.info("[DRY-RUN] Would apply Godin filter to %d stations.", len(all_files))
        else:
            counts: dict[str, int] = {}

            def _godin_worker(fn: str) -> str:
                return detide_godin(fn, obs_dir, GESLA_OBS_GODIN_DIR, args.force)

            futures_map = {}
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                for fn in all_files:
                    fut = pool.submit(_godin_worker, fn)
                    futures_map[fut] = fn

                iterable = as_completed(futures_map)
                if _TQDM:
                    iterable = _tqdm(iterable, total=len(all_files),
                                     desc="Godin", unit="stn", dynamic_ncols=True)
                for fut in iterable:
                    try:
                        status = fut.result()
                    except Exception as exc:
                        logger.error("Unhandled exception: %s", exc)
                        status = "error"
                    counts[status] = counts.get(status, 0) + 1

            _log_counts("godin_filter", counts)

    # ---- FES tide prediction (sequential — NetCDF I/O not guaranteed thread-safe)
    if run_fes:
        logger.info("")
        logger.info("── FES2022 detiding (%d stations) → %s", len(all_files), GESLA_OBS_FES_DIR)

        if args.dry_run:
            logger.info("[DRY-RUN] Would predict FES tide for %d stations.", len(all_files))
        else:
            counts_fes: dict[str, int] = {}
            iterable = all_files
            if _TQDM:
                iterable = _tqdm(all_files, desc="FES2022", unit="stn", dynamic_ncols=True)

            for fn in iterable:
                status = detide_fes(
                    file_name=fn,
                    obs_dir=obs_dir,
                    out_dir=GESLA_OBS_FES_DIR,
                    tide_models_dir=TIDE_MODELS_DIR,
                    fes_model=FES_MODEL_NAME,
                    force=args.force,
                )
                counts_fes[status] = counts_fes.get(status, 0) + 1

            _log_counts("minus_fes_tide", counts_fes)

    logger.info("")
    logger.info("Detiding complete.")


if __name__ == "__main__":
    main()
