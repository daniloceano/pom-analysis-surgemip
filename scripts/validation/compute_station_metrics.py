"""
compute_station_metrics.py
==========================
Read all comparison CSVs from ``data/processed/validation/gesla_vs_model/``
and compute a per-station skill-score summary table.

For each station, the script:
  1. Reads ``<file_name>.csv.gz`` from the comparison directory.
  2. Filters to rows where ``gesla_qc_flag == 1`` and ``gesla_use_flag == 1``
     and both ``sea_level_obs_m`` and the model columns are non-NaN.
  3. **Removes the long-term mean** from both obs and model series (demeaning).
  4. Computes metrics on the demeaned series for model targets:
       - ``model_eta_notide_m``  (meteorological sea level / storm surge)
       - ``model_eta_tide_m``    (full sea level including tides)

Demeaning preprocessing
-----------------------
By default, the long-term mean is subtracted from both observations and model
series BEFORE computing RMSE, bias, and Pearson r. This is critical because:

- Tide gauges use chart datum (typically 1.5–3 m above mean sea level)
- Models use approximately mean sea level as reference (~0 m)
- Without demeaning, RMSE/bias are dominated by this constant datum offset

**After demeaning:**
- RMSE measures error in VARIABILITY (storm-surge amplitude)
- Bias measures systematic over/under-estimation of surge amplitude
- Pearson r measures phase/timing agreement of surge events

The raw means are still reported in `obs_mean_m` and `model_*_mean_m` columns
for reference and provenance.

To disable demeaning (e.g., for legacy comparison), use ``--no-demean``.

Output columns
--------------
station_file_name, station_name, site_code, country,
station_lon, station_lat, model_lon, model_lat, distance_km,
n_total, n_valid,
obs_mean_m, obs_std_m, obs_max_m,
model_notide_mean_m, model_notide_std_m, model_notide_max_m,
rmse_notide, bias_notide, pearson_r_notide,
model_tide_mean_m, model_tide_std_m, model_tide_max_m,
rmse_tide, bias_tide, pearson_r_tide

Outputs
-------
- results/validation/station_metrics.csv
- results/validation/station_metrics.parquet  (if pyarrow/fastparquet available)

Usage
-----
    # Compute metrics for all stations (with demeaning, default):
    python scripts/validation/compute_station_metrics.py

    # Overwrite existing output:
    python scripts/validation/compute_station_metrics.py --force

    # Single station (for testing):
    python scripts/validation/compute_station_metrics.py \\
        --station san_francisco_ca-551a-usa-uhslc
    
    # Disable demeaning (NOT recommended for surge validation):
    python scripts/validation/compute_station_metrics.py --no-demean
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
    GESLA_VS_MODEL_DIR,
    GESLA_QC_GOOD_FLAGS,
    GESLA_USE_GOOD_FLAGS,
    STATION_METRICS_CSV,
    STATION_METRICS_PARQUET,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Metadata columns copied verbatim from the comparison CSV
_META_COLS = [
    "station_file_name", "station_name", "site_code", "country",
    "station_lon", "station_lat", "model_lon", "model_lat", "distance_km",
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
        "--comp-dir",
        default=str(GESLA_VS_MODEL_DIR),
        help=f"Directory containing per-station comparison CSVs. (default: {GESLA_VS_MODEL_DIR})",
    )
    p.add_argument(
        "--out",
        default=str(STATION_METRICS_CSV),
        help=f"Output path for the metrics CSV. (default: {STATION_METRICS_CSV})",
    )
    p.add_argument(
        "--station",
        default=None,
        help="Compute metrics for only this station (file name, no extension).",
    )
    p.add_argument(
        "--targets",
        default="notide,tide",
        help=(
            "Comma-separated list of model targets to compute metrics for.  "
            "Choices: 'notide', 'tide', or 'notide,tide'.  "
            "Use '--targets notide' for de-tided validations where comparing "
            "against model_eta_tide_m would be physically inconsistent.  "
            "(default: notide,tide)"
        ),
    )
    p.add_argument(
        "--no-demean",
        action="store_true",
        help=(
            "Disable demeaning preprocessing. By default, the long-term mean "
            "is subtracted from both obs and model before computing metrics. "
            "NOT recommended for surge validation — use only for legacy comparison."
        ),
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output file.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def _demean(arr: np.ndarray) -> np.ndarray:
    """
    Remove the long-term mean from an array.
    
    This preprocessing step removes the chart datum / reference level offset,
    focusing the metrics on variability rather than absolute levels. This is
    essential because tide gauges and models use different reference datums.
    """
    mean_val = np.nanmean(arr)
    if np.isnan(mean_val):
        return arr
    return arr - mean_val


def _rmse(obs: np.ndarray, model: np.ndarray) -> float:
    diff = obs - model
    return float(np.sqrt(np.mean(diff ** 2)))


def _bias(obs: np.ndarray, model: np.ndarray) -> float:
    """Mean model bias: mean(model - obs)."""
    return float(np.mean(model - obs))


def _pearson_r(obs: np.ndarray, model: np.ndarray) -> float:
    if obs.std() == 0 or model.std() == 0:
        return float("nan")
    return float(np.corrcoef(obs, model)[0, 1])


def compute_metrics_for_file(
    csv_path: pathlib.Path,
    targets: set[str] | None = None,
    demean: bool = True,
) -> dict | None:
    """
    Read one comparison CSV and compute skill metrics.

    Parameters
    ----------
    csv_path : pathlib.Path
        Path to the ``*.csv.gz`` comparison file.
    targets : set of str, optional
        Which model targets to compute metrics for.  Valid elements:
        ``"notide"`` and/or ``"tide"``.  Default (``None``) computes both.
        Use ``{"notide"}`` for de-tided validation modes where comparing
        against ``model_eta_tide_m`` would be physically inconsistent.
    demean : bool, default True
        If True, subtract the long-term mean from both obs and model series
        before computing RMSE, bias, and Pearson r. This removes the chart
        datum offset and focuses the metrics on variability. Statistics
        (obs_mean_m, model_*_mean_m) are computed BEFORE demeaning.

    Returns
    -------
    dict or None
        Metrics row, or None if the file is empty / unreadable.
    
    Notes
    -----
    **Methodological rationale for demeaning:**
    
    Tide gauges use chart datum as their reference level (typically 1.5–3 m
    above mean sea level), while ocean models use approximately mean sea level.
    Without demeaning, RMSE and bias are dominated by this constant offset,
    which has no physical meaning for storm-surge validation.
    
    By subtracting the long-term mean from each series independently, we:
    1. Remove the reference level difference
    2. Focus metrics on the agreement of VARIABILITY (storm-surge signal)
    3. Make RMSE interpretable as the typical error in surge amplitude
    4. Make Pearson r measure the timing/phase agreement of surge events
    
    The raw means are still reported in obs_mean_m and model_*_mean_m for
    reference and provenance.
    """
    if targets is None:
        targets = {"notide", "tide"}

    do_notide = "notide" in targets
    do_tide   = "tide"   in targets

    try:
        df = pd.read_csv(csv_path, compression="gzip")
    except Exception as exc:
        logger.error("  Cannot read %s: %s", csv_path.name, exc)
        return None

    if df.empty:
        logger.warning("  Empty file: %s", csv_path.name)
        return None

    n_total = len(df)

    # ---- Build good-quality mask -------------------------------------------
    mask = pd.Series(True, index=df.index)
    if "gesla_qc_flag" in df.columns:
        mask &= df["gesla_qc_flag"].isin(GESLA_QC_GOOD_FLAGS)
    if "gesla_use_flag" in df.columns:
        mask &= df["gesla_use_flag"].isin(GESLA_USE_GOOD_FLAGS)
    mask &= df["sea_level_obs_m"].notna()
    if do_notide:
        mask &= df.get("model_eta_notide_m", pd.Series(dtype=float)).notna()
    if do_tide:
        mask &= df.get("model_eta_tide_m",   pd.Series(dtype=float)).notna()

    df_good = df[mask].copy()
    n_valid = len(df_good)

    nan_row: dict = {}
    for col in _META_COLS:
        nan_row[col] = df[col].iloc[0] if col in df.columns else np.nan
    nan_row.update({"n_total": n_total, "n_valid": n_valid,
                    "obs_mean_m": np.nan, "obs_std_m": np.nan, "obs_max_m": np.nan})
    for pfx in ("notide", "tide"):
        nan_row.update({
            f"model_{pfx}_mean_m": np.nan, f"model_{pfx}_std_m": np.nan,
            f"model_{pfx}_max_m":  np.nan,
            f"rmse_{pfx}": np.nan, f"bias_{pfx}": np.nan, f"pearson_r_{pfx}": np.nan,
        })

    if n_valid < 10:
        logger.debug("  Too few valid samples (%d) for %s", n_valid, csv_path.stem)
        return nan_row

    # Get raw obs array (for statistics BEFORE demeaning)
    obs_raw = df_good["sea_level_obs_m"].to_numpy(dtype=float)

    row: dict = {}
    for col in _META_COLS:
        row[col] = df[col].iloc[0] if col in df.columns else np.nan
    row.update({
        "n_total": n_total,
        "n_valid": n_valid,
        "obs_mean_m": float(np.mean(obs_raw)),  # raw mean (before demeaning)
        "obs_std_m":  float(np.std(obs_raw)),
        "obs_max_m":  float(np.max(np.abs(obs_raw))),
    })
    
    # Apply demeaning for metrics computation
    obs = _demean(obs_raw) if demean else obs_raw

    # ---- notide metrics -------------------------------------------------------
    if do_notide and "model_eta_notide_m" in df_good.columns:
        notide_raw = df_good["model_eta_notide_m"].to_numpy(dtype=float)
        notide = _demean(notide_raw) if demean else notide_raw
        row.update({
            "model_notide_mean_m": float(np.mean(notide_raw)),  # raw mean
            "model_notide_std_m":  float(np.std(notide_raw)),
            "model_notide_max_m":  float(np.max(np.abs(notide_raw))),
            "rmse_notide":         _rmse(obs, notide),  # computed on demeaned
            "bias_notide":         _bias(obs, notide),  # computed on demeaned
            "pearson_r_notide":    _pearson_r(obs, notide),
        })
    else:
        row.update({
            "model_notide_mean_m": np.nan, "model_notide_std_m": np.nan,
            "model_notide_max_m": np.nan,
            "rmse_notide": np.nan, "bias_notide": np.nan, "pearson_r_notide": np.nan,
        })

    # ---- tide metrics ---------------------------------------------------------
    if do_tide and "model_eta_tide_m" in df_good.columns:
        tide_raw = df_good["model_eta_tide_m"].to_numpy(dtype=float)
        tide = _demean(tide_raw) if demean else tide_raw
        row.update({
            "model_tide_mean_m":   float(np.mean(tide_raw)),  # raw mean
            "model_tide_std_m":    float(np.std(tide_raw)),
            "model_tide_max_m":    float(np.max(np.abs(tide_raw))),
            "rmse_tide":           _rmse(obs, tide),  # computed on demeaned
            "bias_tide":           _bias(obs, tide),  # computed on demeaned
            "pearson_r_tide":      _pearson_r(obs, tide),
        })
    else:
        row.update({
            "model_tide_mean_m": np.nan, "model_tide_std_m": np.nan,
            "model_tide_max_m": np.nan,
            "rmse_tide": np.nan, "bias_tide": np.nan, "pearson_r_tide": np.nan,
        })

    return row


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    comp_dir  = pathlib.Path(args.comp_dir)
    out_path  = pathlib.Path(args.out)

    if out_path.exists() and not args.force:
        logger.info(
            "Metrics file already exists: %s  (use --force to recompute)", out_path
        )
        return

    if not comp_dir.exists():
        logger.error(
            "Comparison directory not found: %s\n"
            "  Run build_comparison_csvs.py first.",
            comp_dir,
        )
        sys.exit(1)

    csv_files = sorted(comp_dir.glob("*.csv.gz"))
    if args.station:
        csv_files = [f for f in csv_files if f.stem == args.station]

    if not csv_files:
        logger.error("No comparison CSVs found in %s", comp_dir)
        sys.exit(1)

    # Parse --targets flag
    targets_set = {t.strip() for t in args.targets.split(",") if t.strip()}
    invalid = targets_set - {"notide", "tide"}
    if invalid:
        logger.error("Unknown --targets value(s): %s.  Use 'notide', 'tide', or both.", invalid)
        sys.exit(1)
    
    # Demeaning setting
    demean = not args.no_demean
    logger.info("Targets: %s", sorted(targets_set))
    logger.info("Demeaning: %s", "ENABLED (default)" if demean else "DISABLED (--no-demean)")
    logger.info("Computing metrics for %d stations …", len(csv_files))

    rows: list[dict] = []
    n_ok = 0
    n_fail = 0

    for i, csv_path in enumerate(csv_files, 1):
        row = compute_metrics_for_file(csv_path, targets=targets_set, demean=demean)
        if row is not None:
            rows.append(row)
            n_ok += 1
        else:
            n_fail += 1

        if i % 100 == 0 or i == len(csv_files):
            logger.info("  Progress: %d/%d  ok=%d  failed=%d", i, len(csv_files), n_ok, n_fail)

    if not rows:
        logger.error("No metrics computed — check comparison CSVs.")
        sys.exit(1)

    metrics_df = pd.DataFrame(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_df.to_csv(out_path, index=False)
    logger.info("Metrics saved: %s  (%d stations)", out_path, len(metrics_df))

    # ---- Optional Parquet output --------------------------------------------
    parquet_path = pathlib.Path(str(STATION_METRICS_PARQUET))
    try:
        metrics_df.to_parquet(parquet_path, index=False)
        logger.info("Parquet saved: %s", parquet_path)
    except Exception as exc:
        logger.debug("Parquet output skipped (%s) — install pyarrow or fastparquet to enable.", exc)

    # ---- Quick summary ------------------------------------------------------
    numeric = metrics_df.select_dtypes(include="number")
    logger.info(
        "\nSummary (medians across %d stations):\n%s",
        len(metrics_df),
        numeric[["rmse_notide", "bias_notide", "pearson_r_notide",
                 "rmse_tide",   "bias_tide",   "pearson_r_tide",
                 "n_valid"]].median().to_string(),
    )


if __name__ == "__main__":
    main()
