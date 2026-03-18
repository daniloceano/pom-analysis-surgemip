"""
settings.py
===========
Central configuration file for the POM_analysis project.

All paths, physical constants, plotting styles, and domain parameters are
defined here so that every script can import a single source of truth.

Usage
-----
    # From any script inside the project tree:
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
    from config.settings import TIDE_CTL, PLOT_STYLE, STATIONS
"""

import pathlib
import numpy as np


# ---------------------------------------------------------------------------
# Utility helpers (defined first so they can be referenced below)
# ---------------------------------------------------------------------------

def _has_cmocean() -> bool:
    """Return True if the cmocean package is available."""
    try:
        import cmocean  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Project root  (parent of this config/ folder)
# ---------------------------------------------------------------------------
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Raw model output paths  (read-only – produced by POM / ricamarg)
# ---------------------------------------------------------------------------
RAW_DATA_DIR = pathlib.Path("/p1-sto-swell/ricamarg/SurgeMIP/RUN_3D")

# Combined 2013-2018 ERA5-forced files  (sea-surface elevation only)
TIDE_CTL   = RAW_DATA_DIR / "eta-tide_SurgeMIP_ERA5_2013-2018.ctl"
TIDE_GRA   = RAW_DATA_DIR / "eta-tide_SurgeMIP_ERA5_2013-2018.gra"
NOTIDE_CTL = RAW_DATA_DIR / "eta-notide_SurgeMIP_ERA5_2013-2018.ctl"
NOTIDE_GRA = RAW_DATA_DIR / "eta-notide_SurgeMIP_ERA5_2013-2018.gra"

# Monthly template CTL files (include ua, va, u, v, t, s as well as e)
TIDE_MONTHLY_CTL   = RAW_DATA_DIR / "SurgeMIP_3D_tide.ctl"
NOTIDE_MONTHLY_CTL = RAW_DATA_DIR / "SurgeMIP_3D_notide.ctl"

# ---------------------------------------------------------------------------
# Project output directories  (auto-created on import)
# ---------------------------------------------------------------------------
DATA_DIR        = PROJECT_ROOT / "data"
PROCESSED_DIR   = DATA_DIR / "processed"
FIGURES_DIR     = PROJECT_ROOT / "figures"
FIG_EXPLORE_DIR = FIGURES_DIR / "exploratory"
FIG_MAPS_DIR    = FIGURES_DIR / "maps"
FIG_VALID_DIR   = FIGURES_DIR / "validation"
RESULTS_DIR     = PROJECT_ROOT / "results"

for _d in [PROCESSED_DIR, FIG_EXPLORE_DIR, FIG_MAPS_DIR, FIG_VALID_DIR, RESULTS_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Model grid parameters  (mirrored from the CTL descriptor)
# ---------------------------------------------------------------------------
GRID = {
    # Longitude axis
    "nx":        1200,
    "lon_start": -179.85,
    "dlon":       0.30,
    # Latitude axis
    "ny":        584,
    "lat_start": -70.625,
    "dlat":       0.25,
    # Vertical
    "nz":        1,
    # Time axis of the combined file
    "nt":        52584,           # hourly steps, 2013-01-01 00:00 to 2018-12-31 23:00
    "t_start":   "2013-01-01 00:00",
    "dt_hours":  1,
    # Missing / fill value used by POM / GrADS
    "undef":     -9.99e+08,
    # Byte order of the binary file:  '<' = little-endian (Linux/Intel)
    "byteorder": "<",
    "dtype":     "float32",
}

# Pre-computed 1-D coordinate arrays (bin centres)
LON = np.linspace(
    GRID["lon_start"],
    GRID["lon_start"] + (GRID["nx"] - 1) * GRID["dlon"],
    GRID["nx"],
)
LAT = np.linspace(
    GRID["lat_start"],
    GRID["lat_start"] + (GRID["ny"] - 1) * GRID["dlat"],
    GRID["ny"],
)

# ---------------------------------------------------------------------------
# Monthly grid  (template CTL – individual monthly GRA files)
# ---------------------------------------------------------------------------
MONTHLY_GRID = {
    "nx":        1203,
    "lon_start": -179.85,
    "dlon":       0.30,
    "ny":         584,
    "lat_start": -70.625,
    "dlat":       0.25,
    "undef":      0.0,
    "byteorder": "<",
    "dtype":     "float32",
    "variables": ["e", "ua", "va", "u", "v", "t", "s"],
    "var_descriptions": {
        "e":  "Sea surface elevation [m]",
        "ua": "Depth-averaged eastward current [m/s]",
        "va": "Depth-averaged northward current [m/s]",
        "u":  "Surface eastward current [m/s]",
        "v":  "Surface northward current [m/s]",
        "t":  "Surface temperature [deg C]",
        "s":  "Surface salinity [PSU]",
    },
}

# ---------------------------------------------------------------------------
# Physical / oceanographic constants
# ---------------------------------------------------------------------------
CONSTANTS = {
    "g":      9.81,    # gravitational acceleration  [m/s^2]
    "rho_sw": 1025.0,  # reference seawater density  [kg/m^3]
    "rho_fw": 1000.0,  # fresh water density          [kg/m^3]
}

# ---------------------------------------------------------------------------
# Reference stations
# Format: station_id -> (lon, lat, full_name)
# Used for validation and point time-series extraction.
# ---------------------------------------------------------------------------
STATIONS = {
    # Brazilian coast – main SurgeMIP focus area
    "santos":       (-46.30,  -23.97,  "Santos, SP, Brazil"),
    "cananeia":     (-47.93,  -25.02,  "Cananeia, SP, Brazil"),
    "imbituba":     (-48.66,  -28.23,  "Imbituba, SC, Brazil"),
    "ilha_fiscal":  (-43.17,  -22.90,  "Ilha Fiscal (Rio de Janeiro), Brazil"),
    "macae":        (-41.77,  -22.38,  "Macae, RJ, Brazil"),
    "salvador":     (-38.51,  -12.96,  "Salvador, BA, Brazil"),
    "fortaleza":    (-38.53,   -3.73,  "Fortaleza, CE, Brazil"),
    "belem":        (-48.50,   -1.45,  "Belem, PA, Brazil"),
    # River Plate region
    "buenos_aires": (-58.37,  -34.60,  "Buenos Aires, Argentina"),
    "montevideo":   (-56.24,  -34.91,  "Montevideo, Uruguay"),
    # Open-ocean reference
    "open_ocean":   (-35.00,  -25.00,  "Open Ocean Reference (South Atlantic)"),
}

# ---------------------------------------------------------------------------
# Matplotlib / Cartopy plot style settings
# ---------------------------------------------------------------------------
PLOT_STYLE = {
    # Figure dimensions
    "figsize_map":   (14, 8),
    "figsize_ts":    (12, 5),
    "figsize_panel": (16, 10),
    "dpi":           150,
    "dpi_draft":     100,
    # Colormaps
    "cmap_elev":     "RdBu_r",
    "cmap_elev_abs": "viridis",
    "cmap_surge":    "seismic",
    "cmap_temp":     "plasma",
    "cmap_salt":     "cmo.haline" if _has_cmocean() else "Blues",
    "cmap_speed":    "cmo.speed"  if _has_cmocean() else "YlOrRd",
    # Colour limits (overridable per script)
    "vmin_elev":   -1.5,
    "vmax_elev":    1.5,
    "vmin_surge":  -0.5,
    "vmax_surge":   0.5,
    # Map extents  [lonmin, lonmax, latmin, latmax]
    "extent_south_atlantic": [-70,  20, -60,  10],
    "extent_brazil_south":   [-55, -35, -35, -20],
    "extent_global":         [-180, 180, -75,  80],
    # Typography
    "fontsize":  11,
    "titlesize": 13,
}

# ---------------------------------------------------------------------------
# SurgeMIP station list (GESLA-4 subset of interest)
# ---------------------------------------------------------------------------
SURGEMIP_STNLIST = DATA_DIR / "SurgeMIP_files" / "SurgeMIP_stnlist.csv"

# ---------------------------------------------------------------------------
# GESLA-4 configuration
# ---------------------------------------------------------------------------
import os as _os

# Root directories for GESLA data
GESLA_RAW_DIR  = DATA_DIR / "gesla" / "raw"      # downloaded ZIP or extracted files
GESLA_OBS_DIR  = PROCESSED_DIR / "gesla" / "observations"
GESLA_MANIFEST = PROCESSED_DIR / "gesla" / "stations_manifest.csv"

# Validation outputs
VALIDATION_DIR        = PROCESSED_DIR / "validation"
STATION_MODEL_INDEX   = VALIDATION_DIR / "station_model_index.csv"
GESLA_VS_MODEL_DIR    = VALIDATION_DIR / "gesla_vs_model"

for _d in [GESLA_OBS_DIR, VALIDATION_DIR, GESLA_VS_MODEL_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# Results
RESULTS_VALID_DIR       = RESULTS_DIR / "validation"
STATION_METRICS_CSV     = RESULTS_VALID_DIR / "station_metrics.csv"
STATION_METRICS_PARQUET = RESULTS_VALID_DIR / "station_metrics.parquet"

for _d in [RESULTS_VALID_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# GESLA-4 dataset download URL
# The dataset requires free registration at https://gesla787883612.wordpress.com/downloads/
# After registering, set the URL (or path) via the environment variable below,
# or pass it explicitly to the download script (--url / --zip-file).
GESLA_ZIP_URL  = _os.environ.get("GESLA_ZIP_URL", "")          # e.g. "https://…/GESLA4.zip"
GESLA_ZIP_FILE = _os.environ.get(                               # local path to the ZIP archive
    "GESLA_ZIP_FILE",
    str(GESLA_RAW_DIR / "GESLA4.zip"),
)

# GESLA data format constants
GESLA_NULL_VALUE_DEFAULT = -99.9999    # fallback null sentinel when not in station metadata
GESLA_QC_GOOD_FLAGS      = {1}        # QC flags considered "good quality" observations
GESLA_USE_GOOD_FLAGS     = {1}        # use-flags considered "recommended for use"

# ---------------------------------------------------------------------------
# SurgeMIP project metadata
# ---------------------------------------------------------------------------
SURGMIP_META = {
    "model":      "Princeton Ocean Model (POM) – 3-D baroclinic",
    "forcing":    "ERA5 (hourly, 0.25 deg) – wind stress + atmospheric pressure",
    "tides":      "FES2022 harmonic constants",
    "period":     "2013-01-01 to 2018-12-31",
    "resolution": "0.30 deg x 0.25 deg (lon x lat)",
    "domain":     "Near-global: 179.85W–179.85E,  70.625S–75.125N",
    "variables":  MONTHLY_GRID["var_descriptions"],
    "runs": {
        "tide":   "Full simulation with astronomical tidal forcing (FES2022)",
        "notide": "Meteorological-only run (no astronomical tides)",
    },
    "surge_definition": (
        "Storm surge (meteorological sea level) = eta_notide.  "
        "Tidal signal ~ eta_tide - eta_notide."
    ),
}
