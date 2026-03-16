"""
inspect_data.py
===============
Exploratory script: open both POM GrADS datasets (tide and no-tide),
print a detailed description of their content, compute basic statistics
on a snapshot, and save a map figure showing the sea-surface elevation
at a chosen time step.

Usage
-----
    python scripts/exploratory/inspect_data.py [--tstep N] [--region REGION]

Arguments
---------
    --tstep   N        Time-step index to plot (default: 0 = 2013-01-01 00:00 UTC)
    --region  REGION   Map extent key from PLOT_STYLE: global | south_atlantic |
                       brazil_south  (default: south_atlantic)
    --out     PATH     Output figure path (default: figures/exploratory/snapshot_...)

Example
-------
    python scripts/exploratory/inspect_data.py --tstep 8760 --region south_atlantic
"""

import sys
import argparse
import pathlib

# Make sure the project root is importable
_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

import numpy as np
import matplotlib
matplotlib.use("Agg")   # non-interactive backend (server / HPC)
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from config.settings import (
    TIDE_CTL, NOTIDE_CTL, PLOT_STYLE, STATIONS, FIG_EXPLORE_DIR, SURGMIP_META
)
from utils.grads_reader import GrADSReader


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tstep",  type=int,   default=0,
                   help="Time-step index to map (default: 0)")
    p.add_argument("--region", type=str,   default="south_atlantic",
                   choices=["global", "south_atlantic", "brazil_south"],
                   help="Map region (default: south_atlantic)")
    p.add_argument("--out",    type=str,   default=None,
                   help="Output figure path")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_header(title: str) -> None:
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


def _describe_dataset(reader: GrADSReader, label: str) -> None:
    """Print a rich description of the dataset."""
    _print_header(f"Dataset: {label}")
    reader.describe()

    print("\n  -- Quick statistics at t=0 --")
    reader.stats()

    t_idx_mid = reader.nt // 2
    print(f"\n  -- Quick statistics at t={t_idx_mid} "
          f"({reader.times[t_idx_mid]}) --")
    for var in reader.variables:
        arr = reader.read_timestep(t_idx_mid, var_name=var)
        valid = arr.compressed() if hasattr(arr, "compressed") else arr.ravel()
        valid = valid[np.isfinite(valid)]
        if len(valid):
            print(f"    {var:>4s}: min={valid.min():+.4f}  "
                  f"max={valid.max():+.4f}  "
                  f"mean={valid.mean():+.4f}  "
                  f"std={valid.std():.4f}")


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _make_snapshot_map(
    reader_tide: GrADSReader,
    reader_notide: GrADSReader,
    t_idx: int,
    region: str,
    out_path: pathlib.Path,
) -> None:
    """
    Create a 2-panel figure:
      Left  – eta (tide run)
      Right – eta (no-tide run)
    with a third panel for the tidal signal = tide - notide.
    """
    extent_key = f"extent_{region}"
    extent = PLOT_STYLE.get(extent_key, PLOT_STYLE["extent_south_atlantic"])
    lonmin, lonmax, latmin, latmax = extent

    stamp_tide   = reader_tide.times[t_idx]
    stamp_notide = reader_notide.times[t_idx]

    eta_tide   = reader_tide.read_timestep(t_idx)
    eta_notide = reader_notide.read_timestep(t_idx)
    tidal_sig  = eta_tide.data.astype(float) - eta_notide.data.astype(float)
    # mask land / undef
    land_mask = eta_tide.mask | eta_notide.mask
    tidal_sig = np.ma.array(tidal_sig, mask=land_mask)

    datasets = [
        (eta_tide,   f"POM eta – tide run\n{stamp_tide}",    PLOT_STYLE["cmap_elev"],
         PLOT_STYLE["vmin_elev"], PLOT_STYLE["vmax_elev"]),
        (eta_notide, f"POM eta – no-tide run\n{stamp_notide}", PLOT_STYLE["cmap_elev"],
         PLOT_STYLE["vmin_elev"], PLOT_STYLE["vmax_elev"]),
        (tidal_sig,  "Tidal signal (tide − no-tide)",        "RdBu_r", -1.0, 1.0),
    ]

    proj  = ccrs.PlateCarree()
    fig, axes = plt.subplots(
        1, 3,
        figsize=(20, 7),
        subplot_kw={"projection": proj},
        constrained_layout=True,
    )

    for ax, (data, title, cmap, vmin, vmax) in zip(axes, datasets):
        ax.set_extent(extent, crs=proj)
        ax.add_feature(cfeature.LAND.with_scale("50m"),
                       facecolor="lightgray", zorder=2)
        ax.add_feature(cfeature.COASTLINE.with_scale("50m"),
                       linewidth=0.5, zorder=3)
        ax.add_feature(cfeature.BORDERS.with_scale("50m"),
                       linewidth=0.3, linestyle=":", zorder=3)
        ax.gridlines(draw_labels=True, linewidth=0.3, color="gray",
                     alpha=0.7, linestyle="--")

        im = ax.pcolormesh(
            reader_tide.lon, reader_tide.lat, data,
            vmin=vmin, vmax=vmax, cmap=cmap,
            transform=proj, shading="auto", zorder=1,
        )
        cb = fig.colorbar(im, ax=ax, orientation="horizontal",
                          pad=0.04, shrink=0.85)
        cb.set_label("Sea-surface elevation [m]", fontsize=9)
        ax.set_title(title, fontsize=10, pad=6)

        # Mark stations
        for sid, (slon, slat, sname) in STATIONS.items():
            if lonmin <= slon <= lonmax and latmin <= slat <= latmax:
                ax.plot(slon, slat, "k^", ms=4, transform=proj, zorder=5)
                ax.text(slon + 0.5, slat, sid, fontsize=7,
                        transform=proj, zorder=5,
                        bbox=dict(boxstyle="round,pad=0.1", fc="white", alpha=0.6))

    fig.suptitle(
        f"SurgeMIP / POM  –  ERA5 2013-2018  |  t_idx={t_idx}  "
        f"({stamp_tide.strftime('%Y-%m-%d %H:%M UTC')})",
        fontsize=12, fontweight="bold",
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=PLOT_STYLE["dpi"], bbox_inches="tight")
    plt.close(fig)
    print(f"\n  Figure saved: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    # ---- Project metadata --------------------------------------------------
    _print_header("SurgeMIP / POM Analysis – Dataset Inspection")
    print(f"\n  Model     : {SURGMIP_META['model']}")
    print(f"  Forcing   : {SURGMIP_META['forcing']}")
    print(f"  Tides     : {SURGMIP_META['tides']}")
    print(f"  Period    : {SURGMIP_META['period']}")
    print(f"  Resolution: {SURGMIP_META['resolution']}")
    print(f"  Domain    : {SURGMIP_META['domain']}")
    print(f"\n  Surge definition: {SURGMIP_META['surge_definition']}")

    # ---- Open datasets -----------------------------------------------------
    print("\n  Loading CTL descriptors (binary files are memory-mapped)...")
    reader_tide   = GrADSReader(TIDE_CTL,   verbose=False)
    reader_notide = GrADSReader(NOTIDE_CTL, verbose=False)

    _describe_dataset(reader_tide,   "ETA-TIDE   (with astronomical tides)")
    _describe_dataset(reader_notide, "ETA-NOTIDE (meteorological only)")

    # ---- Sanity checks -----------------------------------------------------
    _print_header("Cross-dataset consistency checks")
    assert reader_tide.nx == reader_notide.nx,   "nx mismatch!"
    assert reader_tide.ny == reader_notide.ny,   "ny mismatch!"
    assert reader_tide.nt == reader_notide.nt,   "nt mismatch!"
    assert (reader_tide.times == reader_notide.times).all(), "time mismatch!"
    print("  [OK] Grid and time axes match between tide and no-tide datasets.")

    # ---- Extract a few station time-series (first 72 h as test) -----------
    _print_header("Sample station time-series extraction (first 72 h)")
    for sid in ["santos", "buenos_aires"]:
        slon, slat, sname = STATIONS[sid]
        times, ts_t, glon, glat = reader_tide.extract_point(
            slon, slat, t_start=0, t_end=72)
        times, ts_n, _,    _    = reader_notide.extract_point(
            slon, slat, t_start=0, t_end=72)
        # Convert to plain float arrays for safe formatting
        ts_t_vals = np.ma.filled(ts_t, np.nan).astype(float)
        ts_n_vals = np.ma.filled(ts_n, np.nan).astype(float)
        print(f"\n  Station : {sname}")
        print(f"  Nearest grid point : lon={glon:.3f}, lat={glat:.3f}")
        print(f"  eta_tide   : {np.nanmin(ts_t_vals):+.4f} to {np.nanmax(ts_t_vals):+.4f} m")
        print(f"  eta_notide : {np.nanmin(ts_n_vals):+.4f} to {np.nanmax(ts_n_vals):+.4f} m")
        surge_proxy = ts_t_vals - ts_n_vals
        print(f"  tide-notide: {np.nanmin(surge_proxy):+.4f} to {np.nanmax(surge_proxy):+.4f} m")

    # ---- Snapshot map ------------------------------------------------------
    _print_header(f"Generating snapshot map  (t_idx={args.tstep}, region={args.region})")
    if args.out:
        out_path = pathlib.Path(args.out)
    else:
        stamp = reader_tide.times[args.tstep].strftime("%Y%m%d_%H%M")
        out_path = FIG_EXPLORE_DIR / f"snapshot_{args.region}_t{args.tstep:05d}_{stamp}.png"

    _make_snapshot_map(reader_tide, reader_notide, args.tstep, args.region, out_path)

    _print_header("Done")
    print("  All checks passed. Inspection complete.\n")


if __name__ == "__main__":
    main()
