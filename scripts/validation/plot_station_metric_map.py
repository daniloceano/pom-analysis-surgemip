"""
plot_station_metric_map.py
==========================
Plot a global station map with scatter points coloured by a chosen
per-station validation metric from ``results/validation/station_metrics.csv``.

One PNG is saved per metric.  Call once for each metric, or let
``run_gesla_validation_pipeline.py`` iterate automatically.

Available metrics (column names in station_metrics.csv)
--------------------------------------------------------
    rmse_notide         – RMSE of model_eta_notide vs obs  [m]
    bias_notide         – mean bias (model − obs), notide  [m]
    pearson_r_notide    – Pearson correlation, notide
    obs_mean_m          – observed mean sea level           [m]
    obs_max_m           – max |obs| sea level               [m]
    model_notide_mean_m – model notide mean                 [m]
    model_notide_max_m  – max |model notide|                [m]
    rmse_tide           – RMSE of model_eta_tide vs obs     [m]
    bias_tide           – mean bias, tide                   [m]
    pearson_r_tide      – Pearson correlation, tide
    model_tide_mean_m   – model tide mean                   [m]
    model_tide_max_m    – max |model tide|                  [m]
    n_valid             – number of valid paired samples

Usage
-----
    # RMSE map (default metric):
    python scripts/validation/plot_station_metric_map.py

    # Choose a metric:
    python scripts/validation/plot_station_metric_map.py --metric bias_notide

    # Custom colour limits:
    python scripts/validation/plot_station_metric_map.py \\
        --metric rmse_notide --vmin 0 --vmax 0.5

    # Regional extent:
    python scripts/validation/plot_station_metric_map.py \\
        --metric rmse_notide --extent -70 20 -60 10

    # Overwrite existing figure:
    python scripts/validation/plot_station_metric_map.py --force

Outputs
-------
    figures/validation/station_map_<metric>.png
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys

import numpy as np
import pandas as pd

_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from config.settings import (
    STATION_METRICS_CSV,
    FIG_VALID_DIR,
    PLOT_STYLE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---- Metric metadata: (label, default_cmap, symmetric) --------------------
_METRIC_META: dict[str, tuple[str, str, bool]] = {
    "rmse_notide":         ("RMSE notide [m]",           "YlOrRd",   False),
    "bias_notide":         ("Bias notide (model−obs) [m]","RdBu_r",  True),
    "pearson_r_notide":    ("Pearson r (notide)",         "RdYlGn",  False),
    "obs_mean_m":          ("Observed mean [m]",          "RdBu_r",  True),
    "obs_max_m":           ("Observed max |η| [m]",       "YlOrRd",  False),
    "model_notide_mean_m": ("Model notide mean [m]",      "RdBu_r",  True),
    "model_notide_max_m":  ("Model notide max |η| [m]",   "YlOrRd",  False),
    "rmse_tide":           ("RMSE tide [m]",              "YlOrRd",  False),
    "bias_tide":           ("Bias tide (model−obs) [m]",  "RdBu_r",  True),
    "pearson_r_tide":      ("Pearson r (tide)",           "RdYlGn",  False),
    "model_tide_mean_m":   ("Model tide mean [m]",        "RdBu_r",  True),
    "model_tide_max_m":    ("Model tide max |η| [m]",     "YlOrRd",  False),
    "n_valid":             ("Valid paired samples",       "Blues",   False),
}


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--metrics-file",
        default=str(STATION_METRICS_CSV),
        help=f"Path to station_metrics.csv. (default: {STATION_METRICS_CSV})",
    )
    p.add_argument(
        "--metric",
        default="rmse_notide",
        choices=list(_METRIC_META),
        help="Which metric column to plot (default: rmse_notide).",
    )
    p.add_argument(
        "--out-dir",
        default=str(FIG_VALID_DIR),
        help=f"Output directory for figures. (default: {FIG_VALID_DIR})",
    )
    p.add_argument(
        "--vmin",
        type=float,
        default=None,
        help="Colour scale minimum (auto-derived if omitted).",
    )
    p.add_argument(
        "--vmax",
        type=float,
        default=None,
        help="Colour scale maximum (auto-derived if omitted).",
    )
    p.add_argument(
        "--extent",
        nargs=4,
        type=float,
        metavar=("LONMIN", "LONMAX", "LATMIN", "LATMAX"),
        default=None,
        help="Map extent. Default: global (-180 180 -75 80).",
    )
    p.add_argument(
        "--markersize",
        type=float,
        default=18,
        help="Scatter marker size (default: 18).",
    )
    p.add_argument(
        "--dpi",
        type=int,
        default=PLOT_STYLE.get("dpi", 150),
        help=f"Figure DPI (default: {PLOT_STYLE.get('dpi', 150)}).",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing figure.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Cartopy / matplotlib setup
# ---------------------------------------------------------------------------

def _setup_map_axes(extent: list[float]):
    """Create a Cartopy GeoAxes with coastlines and land fill."""
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    fig = plt.figure(figsize=PLOT_STYLE.get("figsize_map", (14, 8)))
    ax  = fig.add_subplot(1, 1, 1, projection=ccrs.Robinson())

    ax.set_extent(extent, crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.LAND,        facecolor="lightgrey",  zorder=0)
    ax.add_feature(cfeature.OCEAN,       facecolor="aliceblue",  zorder=0)
    ax.add_feature(cfeature.COASTLINE,   linewidth=0.4,          zorder=1)
    ax.add_feature(cfeature.BORDERS,     linewidth=0.3, linestyle=":", zorder=1)
    ax.gridlines(draw_labels=False, linewidth=0.3, color="grey", alpha=0.5)

    return fig, ax


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_metric_map(
    df: pd.DataFrame,
    metric: str,
    out_path: pathlib.Path,
    vmin: float | None,
    vmax: float | None,
    extent: list[float],
    markersize: float,
    dpi: int,
) -> None:
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs

    label, default_cmap, symmetric = _METRIC_META[metric]

    # ---- Filter to rows with valid metric values ---------------------------
    sub = df[df[metric].notna()].copy()
    if sub.empty:
        logger.warning("No valid data for metric '%s' — skipping.", metric)
        return

    values = sub[metric].to_numpy(dtype=float)

    # ---- Auto colour limits ------------------------------------------------
    if vmin is None or vmax is None:
        p2, p98 = np.nanpercentile(values, [2, 98])
        if symmetric:
            abs_max = max(abs(p2), abs(p98))
            vmin = vmin if vmin is not None else -abs_max
            vmax = vmax if vmax is not None else  abs_max
        else:
            vmin = vmin if vmin is not None else p2
            vmax = vmax if vmax is not None else p98

    # ---- Plot --------------------------------------------------------------
    fig, ax = _setup_map_axes(extent)

    sc = ax.scatter(
        sub["station_lon"].to_numpy(),
        sub["station_lat"].to_numpy(),
        c=values,
        s=markersize,
        cmap=default_cmap,
        vmin=vmin,
        vmax=vmax,
        transform=ccrs.PlateCarree(),
        zorder=5,
        edgecolors="none",
        alpha=0.85,
    )

    cbar = plt.colorbar(sc, ax=ax, orientation="vertical", shrink=0.75, pad=0.02)
    cbar.set_label(label, fontsize=PLOT_STYLE.get("fontsize", 11))

    n_shown = len(sub)
    ax.set_title(
        f"{label}  (n = {n_shown} stations)",
        fontsize=PLOT_STYLE.get("titlesize", 13),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    logger.info("  Saved: %s", out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    metrics_file = pathlib.Path(args.metrics_file)
    out_dir      = pathlib.Path(args.out_dir)
    out_path     = out_dir / f"station_map_{args.metric}.png"

    if out_path.exists() and not args.force:
        logger.info(
            "Figure already exists: %s  (use --force to regenerate)", out_path
        )
        return

    if not metrics_file.exists():
        logger.error(
            "Metrics file not found: %s\n"
            "  Run compute_station_metrics.py first.",
            metrics_file,
        )
        sys.exit(1)

    df = pd.read_csv(metrics_file)
    logger.info("Loaded metrics for %d stations from %s", len(df), metrics_file)

    if args.metric not in df.columns:
        logger.error(
            "Column '%s' not found in %s.\n"
            "  Available columns: %s",
            args.metric, metrics_file,
            ", ".join(df.columns.tolist()),
        )
        sys.exit(1)

    extent = args.extent if args.extent else PLOT_STYLE.get("extent_global", [-180, 180, -75, 80])

    plot_metric_map(
        df=df,
        metric=args.metric,
        out_path=out_path,
        vmin=args.vmin,
        vmax=args.vmax,
        extent=extent,
        markersize=args.markersize,
        dpi=args.dpi,
    )


if __name__ == "__main__":
    main()
