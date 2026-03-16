"""
extract_point.py
================
Extract a time series at a single geographic location from the POM
GrADS binary output (tide and/or no-tide runs) and save the result as
a compressed CSV file.

The script finds the nearest model grid point and writes:
    - datetime index (UTC, hourly)
    - eta_tide   [m]  (if --run includes "tide")
    - eta_notide [m]  (if --run includes "notide")
    - tidal_signal = eta_tide - eta_notide  (if both runs are selected)

Usage
-----
    python scripts/preprocessing/extract_point.py \\
        --lon  -46.30 \\
        --lat  -23.97 \\
        --label santos \\
        [--run  tide notide] \\
        [--t_start 2013-01-01] \\
        [--t_end   2018-12-31] \\
        [--out PATH]

Defaults
--------
    --run   : both tide and notide
    --out   : data/processed/<label>_<lon>_<lat>_<t_start>_<t_end>.csv.gz

Examples
--------
    # Santos (Brazil) – full period
    python scripts/preprocessing/extract_point.py --lon -46.30 --lat -23.97 --label santos

    # Buenos Aires – January 2017 only
    python scripts/preprocessing/extract_point.py \\
        --lon -58.37 --lat -34.60 --label buenos_aires \\
        --t_start 2017-01-01 --t_end 2017-02-01
"""

import sys
import argparse
import pathlib
from datetime import datetime

# Make sure the project root is importable
_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

import numpy as np
import pandas as pd

from config.settings import (
    TIDE_CTL, NOTIDE_CTL, PROCESSED_DIR, STATIONS
)
from utils.grads_reader import GrADSReader


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--lon",    type=float, default=None,
                   help="Target longitude [decimal degrees, -180 to 180]")
    p.add_argument("--lat",    type=float, default=None,
                   help="Target latitude  [decimal degrees]")
    p.add_argument("--label",  type=str,   default=None,
                   help="Short name used in output filename (e.g. 'santos')")
    p.add_argument("--run",    type=str,   nargs="+",
                   default=["tide", "notide"],
                   choices=["tide", "notide"],
                   help="Which run(s) to extract (default: both)")
    p.add_argument("--t_start", type=str,  default=None,
                   help="Start datetime ISO 8601 (default: beginning of dataset)")
    p.add_argument("--t_end",   type=str,  default=None,
                   help="End   datetime ISO 8601 exclusive (default: end of dataset)")
    p.add_argument("--out",     type=str,  default=None,
                   help="Output file path (.csv or .csv.gz)")
    p.add_argument("--station", type=str,  default=None,
                   help="Use a named station from config (overrides --lon/--lat/--label)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _datetime_to_tidx(reader: GrADSReader, dt_str: str) -> int:
    """Convert an ISO datetime string to the nearest time-step index."""
    dt = pd.Timestamp(dt_str).tz_localize(None)
    idx = reader.times.get_indexer([dt], method="nearest")[0]
    return int(idx)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    # Handle named station shortcut
    lon, lat, label = args.lon, args.lat, args.label
    if args.station:
        if args.station not in STATIONS:
            raise ValueError(
                f"Station '{args.station}' not found. "
                f"Available: {list(STATIONS.keys())}"
            )
        lon, lat, full_name = STATIONS[args.station]
        label = args.station
        print(f"  Using named station: {full_name}  ({lon}, {lat})")
    elif lon is None or lat is None:
        raise ValueError("Either --station or both --lon and --lat must be provided.")

    if label is None:
        label = f"lon{lon:+.2f}_lat{lat:+.2f}".replace(".", "p")

    print("\n" + "=" * 60)
    print("  POM Point Extraction")
    print("=" * 60)
    print(f"  Target  : lon={lon:.3f}, lat={lat:.3f},  label='{label}'")
    print(f"  Runs    : {args.run}")

    # ---- Open datasets (memory-mapped, no data loaded yet) -----------------
    readers = {}
    if "tide" in args.run:
        print(f"\n  Opening tide dataset ...")
        readers["tide"] = GrADSReader(TIDE_CTL, verbose=False)
    if "notide" in args.run:
        print(f"  Opening notide dataset ...")
        readers["notide"] = GrADSReader(NOTIDE_CTL, verbose=False)

    ref = next(iter(readers.values()))   # any reader for grid/time reference

    # ---- Resolve time indices ----------------------------------------------
    t_start_idx = 0 if args.t_start is None else _datetime_to_tidx(ref, args.t_start)
    t_end_idx   = ref.nt if args.t_end is None else _datetime_to_tidx(ref, args.t_end)

    j, i = ref.nearest_ij(lon, lat)
    grid_lon = ref.lon[i]
    grid_lat = ref.lat[j]
    times    = ref.times[t_start_idx:t_end_idx]

    print(f"\n  Nearest grid point : lon={grid_lon:.3f}, lat={grid_lat:.3f}  "
          f"(i={i}, j={j})")
    print(f"  Grid offset        : dlon={abs(grid_lon-lon):.3f} deg, "
          f"dlat={abs(grid_lat-lat):.3f} deg")
    print(f"  Time range         : {times[0]}  →  {times[-1]}")
    print(f"  N time steps       : {len(times)}")
    print()

    # ---- Extract -----------------------------------------------------------
    series: dict[str, np.ndarray] = {}

    for run_name, reader in readers.items():
        col = f"eta_{run_name}"
        print(f"  Extracting {col} ...  ", end="", flush=True)
        _, ts, _, _ = reader.extract_point(
            lon, lat,
            t_start=t_start_idx,
            t_end=t_end_idx,
        )
        # Convert masked array to plain float with NaN for missing
        if hasattr(ts, "filled"):
            ts = ts.filled(np.nan)
        series[col] = ts.astype(np.float32)
        valid_pct = np.sum(np.isfinite(series[col])) / len(series[col]) * 100
        print(f"done  (valid={valid_pct:.1f}%,  "
              f"min={np.nanmin(series[col]):+.4f},  "
              f"max={np.nanmax(series[col]):+.4f} m)")

    # ---- Tidal signal (difference) -----------------------------------------
    if "tide" in series and "notide" in series:
        series["tidal_signal"] = series["eta_tide"] - series["eta_notide"]
        ts_valid = series["tidal_signal"][np.isfinite(series["tidal_signal"])]
        print(f"\n  Tidal signal (tide-notide): "
              f"min={ts_valid.min():+.4f}, max={ts_valid.max():+.4f} m")

    # ---- Build DataFrame ---------------------------------------------------
    df = pd.DataFrame(series, index=times)
    df.index.name = "datetime_utc"

    # Metadata as comments in the CSV header — stored in attrs
    meta = {
        "label":       label,
        "target_lon":  lon,
        "target_lat":  lat,
        "grid_lon":    grid_lon,
        "grid_lat":    grid_lat,
        "grid_i":      i,
        "grid_j":      j,
        "t_start":     str(times[0]),
        "t_end":       str(times[-1]),
        "units":       "m",
        "source":      "POM SurgeMIP ERA5 2013-2018",
    }
    df.attrs = meta

    # ---- Save --------------------------------------------------------------
    if args.out:
        out_path = pathlib.Path(args.out)
    else:
        t0_str = times[0].strftime("%Y%m%d")
        t1_str = times[-1].strftime("%Y%m%d")
        fname  = f"{label}_{t0_str}_{t1_str}.csv.gz"
        out_path = PROCESSED_DIR / fname

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Write a small header block as commented lines, then the data
    header_lines = [f"# {k}: {v}" for k, v in meta.items()]
    header = "\n".join(header_lines) + "\n"

    if str(out_path).endswith(".gz"):
        import gzip
        with gzip.open(out_path, "wt") as fh:
            fh.write(header)
            df.to_csv(fh)
    else:
        with open(out_path, "w") as fh:
            fh.write(header)
            df.to_csv(fh)

    print(f"\n  Output saved: {out_path}")
    print(f"  File size  : {out_path.stat().st_size / 1024:.1f} kB")

    # ---- Quick summary stats -----------------------------------------------
    print("\n  Summary statistics:")
    print(df.describe().to_string())
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
