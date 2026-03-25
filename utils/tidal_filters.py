"""
tidal_filters.py
================
Utility functions for removing the astronomical tidal signal from sea-level
observations.  Two independent methods are provided:

1. ``godin_filter``
   Classic Godin (1972) low-pass filter: three successive centred box-car
   running means of 24 h, 24 h, and 25 h.  Designed for hourly data.
   Returns the *subtidal* (non-tidal / storm-surge) signal.

2. ``predict_fes_tide``
   Predict astronomical tide at a single geographic point using FES2022
   harmonic constants via the ``eo-tides`` library.  Returns the predicted
   tidal time series; subtract from the observation to obtain the storm-surge
   signal.

Both functions are importable independently and designed to be called from
the detiding script (``scripts/validation/apply_tidal_detiding.py``) or
directly from a notebook.

References
----------
Godin, G. (1972). *The Analysis of Tides*. University of Toronto Press.
Lyard, F. H. et al. (2021). FES2014 global ocean tide atlas: design and
  performances. *Ocean Science*, 17, 615–649.
Hart-Davis, M. G. et al. (2024). EOT20: a global ocean tide model.
"""
from __future__ import annotations

import logging
import pathlib

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Method 1 — Godin filter
# ---------------------------------------------------------------------------

def godin_filter(
    series: pd.Series,
    min_periods: int = 1,
    check_hourly: bool = True,
    hourly_tol_minutes: float = 30.0,
) -> pd.Series:
    """
    Apply the Godin (1972) low-pass filter to a sea-level time series.

    The filter applies three centred box-car (running-mean) filters in
    succession: 24 h → 24 h → 25 h.  For hourly data these correspond to
    windows of 24, 24, and 25 samples.  The output is the **subtidal**
    (non-tidal) signal; tidal components with periods shorter than ~30 h
    are attenuated by > 98 %.

    Parameters
    ----------
    series : pd.Series
        Sea-level time series, preferably indexed by a ``DatetimeIndex``.
        Values should be in metres.
    min_periods : int
        Minimum non-NaN values required inside each rolling window.
        ``1`` (default) means the filter is applied at all positions,
        including near the edges of the record.  Values near the first and
        last ~36 samples are computed from a truncated window and should
        be interpreted with caution.
    check_hourly : bool
        Verify that the series has approximately 1-hourly resolution.
        Raises ``ValueError`` if the median step deviates by more than
        ``hourly_tol_minutes``.
    hourly_tol_minutes : float
        Tolerance around the expected 60-minute step.

    Returns
    -------
    pd.Series
        Subtidal sea level [m], same index as *series*.  NaN where the
        filter could not be evaluated (gaps or edges when
        ``min_periods > 1``).

    Raises
    ------
    ValueError
        If ``check_hourly=True`` and the median step is not ~60 min.

    Notes
    -----
    * The three window sizes (24, 24, 25) are chosen so that the 1-cpd
      diurnal and 2-cpd semi-diurnal tidal energy is almost completely
      removed while long-period (> ~30 h) variability is preserved.
    * For series with gaps longer than ~12 h, the filter will produce NaN
      inside those gaps and for a band of ~36 h around each gap boundary.
    * This function returns the **low-passed signal** (subtidal sea level),
      *not* the tidal residual.  To isolate the tidal signal compute
      ``series - godin_filter(series)``.
    """
    if series.empty:
        return series.copy()

    # --- Optional hourly check -----------------------------------------------
    if check_hourly and isinstance(series.index, pd.DatetimeIndex):
        diffs_min = series.index.to_series().diff().dropna().dt.total_seconds() / 60.0
        if len(diffs_min) > 0:
            median_step = float(diffs_min.median())
            if abs(median_step - 60.0) > hourly_tol_minutes:
                raise ValueError(
                    f"godin_filter: median time step is {median_step:.1f} min "
                    f"(expected 60 ± {hourly_tol_minutes} min).  "
                    "The Godin filter is defined for hourly data and will not "
                    "correctly remove tides at other resolutions.  "
                    "Pass check_hourly=False to bypass this check."
                )

    # --- Three centred running means: 24 h → 24 h → 25 h --------------------
    s1 = series.rolling(window=24, center=True, min_periods=min_periods).mean()
    s2 = s1.rolling(   window=24, center=True, min_periods=min_periods).mean()
    s3 = s2.rolling(   window=25, center=True, min_periods=min_periods).mean()

    return s3


# ---------------------------------------------------------------------------
# Method 2 — FES2022 tidal prediction via eo-tides
# ---------------------------------------------------------------------------

def predict_fes_tide(
    times: pd.DatetimeIndex,
    lon: float,
    lat: float,
    directory: str | pathlib.Path,
    model: str = "FES2022",
) -> pd.Series:
    """
    Predict the astronomical tide at a single point using FES2022 harmonic
    constants via the ``eo-tides`` library.

    Parameters
    ----------
    times : pd.DatetimeIndex
        UTC timestamps at which to evaluate the tide.  Timezone-naive
        indices are assumed to be UTC and localised accordingly before the
        call to ``eo-tides``.
    lon : float
        Station longitude [decimal degrees, WGS-84].
    lat : float
        Station latitude  [decimal degrees, WGS-84].
    directory : str or pathlib.Path
        Root directory of the tide model files.  ``eo-tides`` / ``pyTMD``
        expect the following structure for FES2022::

            directory/
            └── fes2022b/
                └── ocean_tide_20241025/
                    ├── m2_fes2022.nc
                    ├── s2_fes2022.nc
                    └── ...

        Pass the *parent* of ``fes2022b/`` (i.e.
        ``data/tide_models_clipped_brasil``).
    model : str
        Tide model identifier passed to ``eo-tides``.  Default
        ``"FES2022"``.

    Returns
    -------
    pd.Series
        Predicted tidal elevation [m], indexed by tz-naive UTC timestamps.
        Series name: ``"fes_tide_m"``.

    Raises
    ------
    ImportError
        If the ``eo-tides`` package is not installed.
    RuntimeError
        If ``eo-tides`` returns an empty result (e.g. the point is on land
        or outside the clipped model domain).

    Notes
    -----
    * ``eo-tides`` maps the ``"FES2022"`` identifier to the ``fes2022b``
      sub-directory.  Ensure that directory is present and that the
      NetCDF constituent files (``*_fes2022.nc``) are readable.
    * The tide prediction uses all constituents available in the model
      directory.  For the clipped Brazil-region files this includes the
      major semi-diurnal (M2, S2, N2, K2) and diurnal (K1, O1, P1, Q1)
      constituents plus several shallow-water harmonics.
    * The predicted tide includes the astronomical (equilibrium) component
      only — no ocean loading, atmospheric or non-linear corrections are
      applied.
    """
    try:
        from eo_tides.model import model_tides  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "eo-tides is required for FES tidal prediction.  "
            "Install with: pip install eo-tides"
        ) from exc

    directory = pathlib.Path(directory)

    # eo-tides requires timezone-aware (UTC) timestamps
    if times.tz is None:
        times_utc = times.tz_localize("UTC")
    else:
        times_utc = times.tz_convert("UTC")

    logger.debug(
        "FES tide: lon=%.4f lat=%.4f  n=%d  model=%s  dir=%s",
        lon, lat, len(times_utc), model, directory,
    )

    result = model_tides(
        x=lon,
        y=lat,
        time=times_utc,
        model=model,
        directory=str(directory),
        output_format="long",
        output_units="m",
        extrapolate=True,
    )

    if result is None or result.empty:
        raise RuntimeError(
            f"eo-tides returned no predictions for "
            f"lon={lon:.4f}, lat={lat:.4f}, model={model}."
        )

    # result has MultiIndex (time, x, y) — extract the time level only
    tide: pd.Series = result["tide_height"].copy()
    if isinstance(tide.index, pd.MultiIndex):
        tide = tide.droplevel(["x", "y"])

    # Remove timezone info to match the tz-naive UTC observation index
    if tide.index.tz is not None:
        tide.index = tide.index.tz_localize(None)

    tide.name = "fes_tide_m"
    return tide
