"""
grads_reader.py
===============
Low-level reader for GrADS sequential-binary (.gra) files paired with a
descriptor (.ctl) file, as produced by the Princeton Ocean Model (POM).

Key features
------------
* parse_ctl()    – parse any GrADS CTL descriptor into a plain dict.
* GrADSReader    – high-level class that memory-maps the binary file so
                   individual time steps (or arbitrary slices) can be
                   read without loading the whole ~138 GB dataset into
                   RAM.  Returns plain NumPy arrays or masked arrays.

POM binary format notes
-----------------------
The POM 2-D output files written by GrADS `set fwrite` are raw
sequential IEEE-754 single-precision floats, row-major (C order),
**no** Fortran record headers.  Each time step is laid out as:

    [nz * ny * nx  float32 values]

where nz == 1 for the combined elevation files.

For files with multiple variables (monthly 3-D output), consecutive
variable blocks follow each other within the same time step:

    [var1: nz*ny*nx] [var2: nz*ny*nx] ... [varN: nz*ny*nx]

Byte order is little-endian (Linux / Intel).

Usage example
-------------
    from utils.grads_reader import GrADSReader
    r = GrADSReader("path/to/file.ctl")
    print(r)                            # summary
    eta = r.read_timestep(0)            # first time step  → (ny, nx) array
    ts  = r.extract_point(-46.3, -23.97) # full time series at Santos
"""

import re
import pathlib
import struct
import warnings
from datetime import datetime, timedelta
from typing import Optional, Union

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# CTL parser
# ---------------------------------------------------------------------------

_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_grads_time(tstr: str) -> datetime:
    """
    Parse a GrADS time string such as ``00z01jan2013`` into a datetime.

    Supported formats: ``HHz[D][D]MonYYYY`` (case-insensitive).
    """
    tstr = tstr.strip().lower()
    m = re.match(r"(\d{1,2})z(\d{1,2})([a-z]{3})(\d{4})", tstr)
    if not m:
        raise ValueError(f"Cannot parse GrADS time string: '{tstr}'")
    hour, day, mon, year = int(m.group(1)), int(m.group(2)), m.group(3), int(m.group(4))
    return datetime(year, _MONTH_ABBR[mon], day, hour)


def _parse_dt(dtstr: str) -> timedelta:
    """
    Parse a GrADS time increment like ``1hr``, ``6hr``, ``1dy`` into a
    timedelta.
    """
    dtstr = dtstr.strip().lower()
    m = re.match(r"(\d+)(mn|hr|dy|mo|yr)", dtstr)
    if not m:
        raise ValueError(f"Cannot parse GrADS time increment: '{dtstr}'")
    val, unit = int(m.group(1)), m.group(2)
    if unit == "mn":
        return timedelta(minutes=val)
    if unit == "hr":
        return timedelta(hours=val)
    if unit == "dy":
        return timedelta(days=val)
    if unit == "mo":
        return timedelta(days=val * 30)   # approximate
    if unit == "yr":
        return timedelta(days=val * 365)  # approximate
    raise ValueError(f"Unknown time unit: {unit}")


def parse_ctl(ctl_path: Union[str, pathlib.Path]) -> dict:
    """
    Parse a GrADS CTL descriptor file and return a dictionary with all
    relevant grid metadata.

    Parameters
    ----------
    ctl_path : str or Path
        Path to the ``.ctl`` descriptor file.

    Returns
    -------
    dict with keys:
        dset, undef, nx, ny, nz, nt,
        lon (1-D array), lat (1-D array),
        t_start (datetime), dt (timedelta), times (DatetimeIndex),
        variables (list of str), var_levels (dict), var_desc (dict),
        template (bool), byteorder, dtype, gra_path (Path or None)
    """
    ctl_path = pathlib.Path(ctl_path).resolve()
    info: dict = {
        "ctl_path": ctl_path,
        "template": False,
        "byteorder": "<",   # POM/GrADS on Linux = little-endian
        "dtype": "float32",
        "variables": [],
        "var_levels": {},
        "var_desc": {},
    }

    with open(ctl_path) as fh:
        lines = fh.readlines()

    in_vars = False
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("*") or line.startswith("@"):
            continue

        upper = line.upper()

        # --- dset (binary file path) -----------------------------------------
        if upper.startswith("DSET"):
            rel = line.split(None, 1)[1].strip()
            if rel.startswith("^"):
                rel = rel[1:]
            info["dset_rel"] = rel
            gra = ctl_path.parent / rel
            info["gra_path"] = gra if not info.get("template") else None

        # --- template flag ---------------------------------------------------
        elif upper.startswith("OPTIONS") and "TEMPLATE" in upper:
            info["template"] = True
            info["gra_path"] = None

        # --- undef -----------------------------------------------------------
        elif upper.startswith("UNDEF"):
            info["undef"] = float(line.split()[1])

        # --- xdef (longitude) ------------------------------------------------
        elif upper.startswith("XDEF"):
            parts = line.split()
            nx = int(parts[1])
            info["nx"] = nx
            if parts[2].upper() == "LINEAR":
                x0, dx = float(parts[3]), float(parts[4])
                info["lon"] = np.linspace(x0, x0 + (nx - 1) * dx, nx)
                info["lon_start"], info["dlon"] = x0, dx
            else:
                info["lon"] = np.array([float(v) for v in parts[3:3 + nx]])

        # --- ydef (latitude) -------------------------------------------------
        elif upper.startswith("YDEF"):
            parts = line.split()
            ny = int(parts[1])
            info["ny"] = ny
            if parts[2].upper() == "LINEAR":
                y0, dy = float(parts[3]), float(parts[4])
                info["lat"] = np.linspace(y0, y0 + (ny - 1) * dy, ny)
                info["lat_start"], info["dlat"] = y0, dy
            else:
                info["lat"] = np.array([float(v) for v in parts[3:3 + ny]])

        # --- zdef (vertical) -------------------------------------------------
        elif upper.startswith("ZDEF"):
            parts = line.split()
            info["nz"] = int(parts[1])

        # --- tdef (time) -----------------------------------------------------
        elif upper.startswith("TDEF"):
            parts = line.split()
            info["nt"] = int(parts[1])
            info["t_start"] = _parse_grads_time(parts[3])
            info["dt"] = _parse_dt(parts[4])
            info["times"] = pd.date_range(
                start=info["t_start"],
                periods=info["nt"],
                freq=pd.tseries.frequencies.to_offset(info["dt"]),
            )

        # --- vars block ------------------------------------------------------
        elif upper.startswith("VARS"):
            in_vars = True
            info["nvars"] = int(line.split()[1])

        elif upper.startswith("ENDVARS"):
            in_vars = False

        elif in_vars:
            parts = line.split(None, 3)
            if len(parts) >= 1:
                vname = parts[0]
                nlev  = int(parts[1]) if len(parts) > 1 else 1
                desc  = parts[3].strip() if len(parts) > 3 else ""
                info["variables"].append(vname)
                info["var_levels"][vname] = max(nlev, 1)
                info["var_desc"][vname]   = desc

    # Resolve gra_path for template files (replace pattern with first step)
    if info.get("template") and "dset_rel" in info:
        t0 = info["t_start"]
        first = info["dset_rel"] \
            .replace("%y4", f"{t0.year:04d}") \
            .replace("%m2", f"{t0.month:02d}")
        info["gra_path_template"] = info["dset_rel"]
        info["gra_path"] = ctl_path.parent / first

    return info


# ---------------------------------------------------------------------------
# GrADSReader
# ---------------------------------------------------------------------------

class GrADSReader:
    """
    Memory-mapped reader for a GrADS sequential-binary dataset.

    Handles both single-file and template (multi-file monthly) datasets.
    For template datasets, only the combined single-file variant is fully
    supported by this reader (i.e. pass the combined CTL, not the
    template one, for time-series access across years).

    Parameters
    ----------
    ctl_path : str or Path
        Path to the ``.ctl`` descriptor.
    verbose : bool
        Print a summary on construction.

    Examples
    --------
    >>> r = GrADSReader("eta-tide_SurgeMIP_ERA5_2013-2018.ctl")
    >>> eta_t0 = r.read_timestep(0)          # shape (ny, nx)
    >>> ts = r.extract_point(-46.3, -23.97)  # shape (nt,)
    >>> r.describe()
    """

    def __init__(self, ctl_path: Union[str, pathlib.Path], verbose: bool = True):
        self.meta = parse_ctl(ctl_path)
        self._mmap: Optional[np.ndarray] = None

        if verbose:
            self.describe()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def nx(self) -> int:
        return self.meta["nx"]

    @property
    def ny(self) -> int:
        return self.meta["ny"]

    @property
    def nz(self) -> int:
        return self.meta.get("nz", 1)

    @property
    def nt(self) -> int:
        return self.meta["nt"]

    @property
    def nvars(self) -> int:
        return len(self.meta["variables"])

    @property
    def lon(self) -> np.ndarray:
        return self.meta["lon"]

    @property
    def lat(self) -> np.ndarray:
        return self.meta["lat"]

    @property
    def times(self) -> pd.DatetimeIndex:
        return self.meta["times"]

    @property
    def undef(self) -> float:
        return self.meta["undef"]

    @property
    def variables(self) -> list:
        return self.meta["variables"]

    @property
    def gra_path(self) -> pathlib.Path:
        return self.meta["gra_path"]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_mmap(self) -> np.ndarray:
        """Open (or reuse) a memory-mapped view of the binary file."""
        if self._mmap is None:
            gra = self.gra_path
            if gra is None or not gra.exists():
                raise FileNotFoundError(
                    f"Binary file not found: {gra}\n"
                    "For template datasets use the combined single-file CTL."
                )
            dtype = np.dtype(self.meta.get("byteorder", "<") +
                             self.meta.get("dtype", "f4").replace("float32", "f4"))
            self._mmap = np.memmap(gra, dtype=dtype, mode="r")
        return self._mmap

    def _timestep_offset(self, t_idx: int, var_idx: int = 0) -> int:
        """
        Return the flat index in the memory-mapped array for a given
        (time step, variable) combination.
        """
        vals_per_var  = self.nz * self.ny * self.nx
        vals_per_step = self.nvars * vals_per_var
        return t_idx * vals_per_step + var_idx * vals_per_var

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def describe(self) -> None:
        """Print a detailed human-readable summary of the dataset."""
        m = self.meta
        sep = "=" * 60
        print(sep)
        print("  GrADS / POM dataset summary")
        print(sep)
        print(f"  CTL file   : {m['ctl_path']}")
        print(f"  Binary     : {m.get('gra_path', 'template')}")
        print(f"  Template   : {m['template']}")
        print()
        print(f"  Grid       : {self.nx} x {self.ny}  (lon x lat)")
        print(f"  Lon range  : {self.lon[0]:.3f} to {self.lon[-1]:.3f}  "
              f"(dx = {m.get('dlon', float('nan')):.3f} deg)")
        print(f"  Lat range  : {self.lat[0]:.3f} to {self.lat[-1]:.3f}  "
              f"(dy = {m.get('dlat', float('nan')):.3f} deg)")
        print(f"  Levels     : {self.nz}")
        print()
        print(f"  Time steps : {self.nt}")
        print(f"  Period     : {self.times[0]}  →  {self.times[-1]}")
        print(f"  Increment  : {m.get('dt', '?')}")
        print()
        print(f"  Variables  : {self.nvars}")
        for v in self.variables:
            desc = m["var_desc"].get(v, "")
            lvl  = m["var_levels"].get(v, 1)
            print(f"    {v:<8s}  levels={lvl}  {desc}")
        print()
        print(f"  Undef      : {self.undef}")
        print(f"  Byte order : {m.get('byteorder', '<')}  "
              f"dtype={m.get('dtype', 'float32')}")
        # File size check
        gra = m.get("gra_path")
        if gra and gra.exists():
            size_gb = gra.stat().st_size / 1e9
            expected = (self.nt * self.nvars * self.nz *
                        self.ny * self.nx * 4) / 1e9
            print(f"  File size  : {size_gb:.2f} GB  "
                  f"(expected {expected:.2f} GB)")
        print(sep)

    def stats(self) -> None:
        """
        Compute and print basic statistics using the first time step.
        Useful for a quick sanity check without loading the full file.
        """
        for i, var in enumerate(self.variables):
            data = self.read_timestep(0, var_name=var)
            masked = np.ma.masked_where(
                np.abs(data - self.undef) < np.abs(self.undef) * 0.01,
                data,
            )
            print(f"  {var}: min={float(masked.min()):.4f}  "
                  f"max={float(masked.max()):.4f}  "
                  f"mean={float(masked.mean()):.4f}  "
                  f"valid_pct={100*masked.count()/masked.size:.1f}%")

    def read_timestep(
        self,
        t_idx: int,
        var_name: str = None,
        mask_undef: bool = True,
    ) -> np.ndarray:
        """
        Read a single time step from the binary file.

        Parameters
        ----------
        t_idx : int
            Zero-based time-step index.
        var_name : str or None
            Variable name to extract.  Defaults to the first variable.
        mask_undef : bool
            If True return a masked array with undef values masked.

        Returns
        -------
        np.ndarray or np.ma.MaskedArray, shape (ny, nx)
        """
        if var_name is None:
            var_name = self.variables[0]
        if var_name not in self.variables:
            raise KeyError(f"Variable '{var_name}' not in {self.variables}")

        var_idx = self.variables.index(var_name)
        mm = self._get_mmap()
        offset = self._timestep_offset(t_idx, var_idx)
        chunk  = mm[offset: offset + self.ny * self.nx].reshape(self.ny, self.nx)
        arr = np.array(chunk, dtype=np.float32)

        if mask_undef:
            arr = np.ma.masked_where(
                np.abs(arr - self.undef) < np.abs(self.undef) * 0.01,
                arr,
            )
        return arr

    def read_slice(
        self,
        t_start: int,
        t_end: int,
        var_name: str = None,
        mask_undef: bool = True,
    ) -> np.ndarray:
        """
        Read a contiguous range of time steps.

        Parameters
        ----------
        t_start, t_end : int
            Inclusive start and exclusive end indices.

        Returns
        -------
        np.ndarray, shape (t_end - t_start, ny, nx)
        """
        if var_name is None:
            var_name = self.variables[0]
        var_idx  = self.variables.index(var_name)
        mm       = self._get_mmap()
        n        = t_end - t_start
        out      = np.empty((n, self.ny, self.nx), dtype=np.float32)

        vals_per_var  = self.nz * self.ny * self.nx
        vals_per_step = self.nvars * vals_per_var

        for i, t in enumerate(range(t_start, t_end)):
            off   = t * vals_per_step + var_idx * vals_per_var
            chunk = mm[off: off + self.ny * self.nx]
            out[i] = chunk.reshape(self.ny, self.nx)

        if mask_undef:
            out = np.ma.masked_where(
                np.abs(out - self.undef) < np.abs(self.undef) * 0.01,
                out,
            )
        return out

    def nearest_ij(self, lon: float, lat: float) -> tuple[int, int]:
        """
        Return the (j, i) grid indices nearest to a lon/lat coordinate.

        Parameters
        ----------
        lon, lat : float
            Target longitude and latitude in decimal degrees.

        Returns
        -------
        (j, i) : tuple of int
            Row (j = latitude index) and column (i = longitude index).
        """
        i = int(np.argmin(np.abs(self.lon - lon)))
        j = int(np.argmin(np.abs(self.lat - lat)))
        return j, i

    def nearest_wet_ij(
        self,
        lon: float,
        lat: float,
        t_idx: int = 0,
        max_radius: int = 10,
    ) -> tuple[int, int]:
        """
        Return the nearest **wet** (ocean) grid point to (lon, lat).

        Searches in expanding square shells around the nearest grid index
        until an ocean cell is found.  Falls back to nearest_ij if no
        wet point is found within `max_radius` grid cells.

        Parameters
        ----------
        lon, lat : float
        t_idx : int
            Time step used to determine the land/sea mask.
        max_radius : int
            Maximum search radius in grid cells.

        Returns
        -------
        (j, i) : tuple of int
        """
        j0, i0 = self.nearest_ij(lon, lat)
        mask_2d = self.read_timestep(t_idx).mask
        if not mask_2d[j0, i0]:
            return j0, i0
        # Expanding search
        for r in range(1, max_radius + 1):
            j_lo = max(j0 - r, 0); j_hi = min(j0 + r + 1, self.ny)
            i_lo = max(i0 - r, 0); i_hi = min(i0 + r + 1, self.nx)
            sub_mask = mask_2d[j_lo:j_hi, i_lo:i_hi]
            if not sub_mask.all():
                # Build distance grid and pick nearest wet cell
                jj, ii = np.meshgrid(
                    np.arange(j_lo, j_hi),
                    np.arange(i_lo, i_hi),
                    indexing="ij",
                )
                dist = (jj - j0) ** 2 + (ii - i0) ** 2
                dist_masked = np.ma.array(dist, mask=sub_mask)
                idx_flat = np.argmin(dist_masked)
                dj, di = np.unravel_index(idx_flat, sub_mask.shape)
                return int(j_lo + dj), int(i_lo + di)
        import warnings
        warnings.warn(
            f"No wet point found within {max_radius} cells of "
            f"({lon:.3f}, {lat:.3f}). Returning nearest grid point.",
            UserWarning,
        )
        return j0, i0

    def extract_point(
        self,
        lon: float,
        lat: float,
        var_name: str = None,
        t_start: int = 0,
        t_end: int = None,
        mask_undef: bool = True,
    ) -> tuple:
        """
        Extract the full time series at the grid point nearest to
        (lon, lat).

        Parameters
        ----------
        lon, lat : float
            Target coordinates.
        var_name : str or None
            Variable to extract (default = first variable).
        t_start, t_end : int
            Subset the time axis.

        Returns
        -------
        times : pd.DatetimeIndex
        ts    : np.ndarray, shape (nt,)
        grid_lon, grid_lat : float – actual grid coordinates used
        """
        if t_end is None:
            t_end = self.nt
        if var_name is None:
            var_name = self.variables[0]

        j, i = self.nearest_wet_ij(lon, lat, t_idx=t_start)
        var_idx       = self.variables.index(var_name)
        vals_per_var  = self.nz * self.ny * self.nx
        vals_per_step = self.nvars * vals_per_var

        mm  = self._get_mmap()
        ts  = np.empty(t_end - t_start, dtype=np.float32)

        for k, t in enumerate(range(t_start, t_end)):
            base   = t * vals_per_step + var_idx * vals_per_var
            flat_i = j * self.nx + i
            ts[k]  = mm[base + flat_i]

        times = self.times[t_start:t_end]

        if mask_undef:
            ts = np.ma.masked_where(
                np.abs(ts - self.undef) < np.abs(self.undef) * 0.01,
                ts,
            )

        return times, ts, self.lon[i], self.lat[j]

    def __repr__(self) -> str:
        return (
            f"GrADSReader("
            f"nx={self.nx}, ny={self.ny}, nt={self.nt}, "
            f"nvars={self.nvars}, "
            f"period={self.times[0].date()}–{self.times[-1].date()})"
        )
