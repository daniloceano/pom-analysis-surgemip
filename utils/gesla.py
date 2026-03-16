"""
gesla.py
========
Utilities for working with GESLA-4 tide-gauge data in the context of the
POM/SurgeMIP validation pipeline.

This module provides:
- ``load_station_list``   – read and clean SurgeMIP_stnlist.csv
- ``parse_gesla_file``    – parse a single GESLA station file into a DataFrame
- ``build_manifest``      – generate stations_manifest.csv from the station list
- ``find_station_in_zip`` – locate a station file inside the GESLA ZIP archive

Timezone convention
-------------------
GESLA station files may be recorded in local time.  The SurgeMIP station list
contains a ``TIME ZONE HOURS`` column that encodes the offset *from* UTC:

    UTC = local_time − timedelta(hours=time_zone_hours)

For example, a station with ``TIME ZONE HOURS = −3`` has its data recorded in
UTC−3; to convert to UTC we *add* 3 hours.

If ``time_zone_hours == 0`` the timestamps are already in UTC and no shift is
applied.  When the column is missing or NaN we assume UTC and emit a warning.

This is a *conservative* interpretation: we apply only what the metadata
explicitly states.  Any residual uncertainty is documented in the output column
``tz_assumed_utc`` (True = no conversion was needed / possible).

References
----------
GESLA-4 project: https://gesla787883612.wordpress.com/
"""

from __future__ import annotations

import logging
import pathlib
import warnings
import zipfile
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column name mapping  (raw CSV header → internal snake_case)
# ---------------------------------------------------------------------------
_COL_MAP: dict[str, str] = {
    "FILE NAME":                                        "file_name",
    "SITE NAME":                                        "site_name",
    "SITE CODE":                                        "site_code",
    "COUNTRY":                                          "country",
    "CONTRIBUTOR (ABBREVIATED)":                        "contributor_abbr",
    "CONTRIBUTOR (FULL)":                               "contributor_full",
    "CONTRIBUTOR WEBSITE":                              "contributor_website",
    "CONTRIBUTOR CONTACT":                              "contributor_contact",
    "ORGINATOR":                                        "originator",
    "ORIGINATOR WEBSITE":                               "originator_website",
    "ORIGINATOR CONTACT":                               "originator_contact",
    "LATITUDE":                                         "latitude",
    "LONGITUDE":                                        "longitude",
    "COORDINATE SYSTEM":                                "coordinate_system",
    "START DATE/TIME":                                  "start_datetime",
    "END DATE/TIME":                                    "end_datetime",
    "NUMBER OF YEARS":                                  "number_of_years",
    "TIME ZONE HOURS":                                  "time_zone_hours",
    "DATUM INFORMATION":                                "datum_information",
    "INSTRUMENT":                                       "instrument",
    "PRECISION":                                        "precision",
    "NULL VALUE":                                       "null_value",
    "GAUGE TYPE":                                       "gauge_type",
    "OVERALL RECORD QUALITYDATA COMPLETENESS BETWEEN 2013-2018":
                                                        "record_quality_completeness",
}


# ---------------------------------------------------------------------------
# Station list
# ---------------------------------------------------------------------------

def load_station_list(csv_path: str | pathlib.Path) -> pd.DataFrame:
    """
    Read and clean the SurgeMIP station list CSV.

    Parameters
    ----------
    csv_path : str or Path
        Path to ``SurgeMIP_stnlist.csv``.

    Returns
    -------
    pd.DataFrame
        Cleaned station table with snake_case column names.  The original
        trailing empty column is dropped.  ``latitude``, ``longitude``, and
        ``time_zone_hours`` are cast to float.
    """
    csv_path = pathlib.Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Station list not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str)

    # Drop unnamed / blank trailing columns  (the CSV has a trailing comma)
    df = df.loc[:, ~df.columns.str.fullmatch(r"\s*")]
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    df.columns = df.columns.str.strip()

    # Rename to snake_case
    df = df.rename(columns={k: v for k, v in _COL_MAP.items() if k in df.columns})

    # Strip whitespace from all string cells
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)

    # Cast numeric columns
    for col in ("latitude", "longitude", "time_zone_hours", "null_value"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove completely empty rows
    df = df.dropna(how="all").reset_index(drop=True)

    logger.info("Loaded %d stations from %s", len(df), csv_path)
    return df


# ---------------------------------------------------------------------------
# Station manifest
# ---------------------------------------------------------------------------

def build_manifest(
    station_list: pd.DataFrame,
    out_path: str | pathlib.Path,
) -> pd.DataFrame:
    """
    Write a lean stations manifest CSV keeping only the columns relevant for
    the validation pipeline.

    Parameters
    ----------
    station_list : pd.DataFrame
        Output of :func:`load_station_list`.
    out_path : str or Path
        Destination for the manifest CSV.

    Returns
    -------
    pd.DataFrame  (the manifest DataFrame, also saved to *out_path*)
    """
    keep = [
        "file_name", "site_name", "site_code", "country",
        "latitude", "longitude", "time_zone_hours",
        "null_value", "start_datetime", "end_datetime",
        "contributor_abbr", "gauge_type",
    ]
    cols = [c for c in keep if c in station_list.columns]
    manifest = station_list[cols].copy()

    out_path = pathlib.Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(out_path, index=False)
    logger.info("Manifest saved: %s  (%d stations)", out_path, len(manifest))
    return manifest


# ---------------------------------------------------------------------------
# ZIP helpers
# ---------------------------------------------------------------------------

def find_station_in_zip(
    zip_path: str | pathlib.Path,
    file_name: str,
) -> Optional[str]:
    """
    Locate a station's data file inside the GESLA ZIP archive.

    The search is case-insensitive and also tries common sub-directory
    prefixes (``GESLA4/``, ``data/``, etc.).

    Parameters
    ----------
    zip_path : str or Path
    file_name : str
        The bare filename as given in the ``file_name`` column of the
        station list (e.g. ``san_francisco_ca-551a-usa-uhslc``).

    Returns
    -------
    str or None
        The full path inside the ZIP, or ``None`` if not found.
    """
    zip_path = pathlib.Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP archive not found: {zip_path}")

    # Normalise: strip possible extension that the caller may or may not supply
    stem = pathlib.Path(file_name).stem.lower()

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()

    return _find_stem_in_namelist(stem, names)


def _find_stem_in_namelist(stem: str, namelist: list[str]) -> Optional[str]:
    """
    Find an entry in a ZIP namelist whose filename stem matches *stem*
    (case-insensitive).  Returns the matching entry or ``None``.

    Parameters
    ----------
    stem : str
        Target filename stem (no extension, no directory prefix).
        The comparison is case-insensitive.
    namelist : list[str]
        Output of ``zipfile.ZipFile.namelist()``.
    """
    stem_lower = stem.lower()
    for name in namelist:
        if pathlib.Path(name).stem.lower() == stem_lower:
            return name
    return None


# ---------------------------------------------------------------------------
# GESLA file parser
# ---------------------------------------------------------------------------

def parse_gesla_file(
    source: str | pathlib.Path | bytes,
    station_meta: Optional[dict] = None,
    null_value: Optional[float] = None,
    time_zone_hours: Optional[float] = None,
) -> pd.DataFrame:
    """
    Parse a single GESLA-4 station data file.

    The GESLA-4 file format is:
    - Header lines that begin with ``#`` (metadata key-value pairs)
    - Data lines with columns:  DATE  TIME  SEA_LEVEL  QC_FLAG  USE_FLAG

    Timezone conversion
    -------------------
    If ``time_zone_hours`` is non-zero the timestamps are assumed to be in
    *local time* and are shifted to UTC by subtracting the offset:

        UTC = local_time − timedelta(hours=time_zone_hours)

    Parameters
    ----------
    source : str, Path, or bytes
        Path to the file on disk, or the raw file content as bytes.
    station_meta : dict, optional
        Metadata dict (typically one row from :func:`load_station_list`)
        used to annotate the output DataFrame.
    null_value : float, optional
        Sentinel value for missing data.  If ``None``, looks for ``NULL VALUE``
        in the file header; falls back to ``-99.9999``.
    time_zone_hours : float, optional
        Timezone offset *from* UTC (see above).  Overrides any value inferred
        from *station_meta*.

    Returns
    -------
    pd.DataFrame with columns:
        datetime_utc, sea_level_obs_m, gesla_qc_flag, gesla_use_flag,
        tz_assumed_utc,
        [and station columns from station_meta if provided]
    """
    from io import StringIO

    # ------------------------------------------------------------------
    # Resolve timezone offset
    # ------------------------------------------------------------------
    tz_hrs: float = 0.0
    tz_assumed_utc: bool = True

    if time_zone_hours is not None:
        tz_hrs = float(time_zone_hours)
        tz_assumed_utc = (tz_hrs == 0.0)
    elif station_meta is not None and "time_zone_hours" in station_meta:
        raw = station_meta["time_zone_hours"]
        if raw is not None and not (isinstance(raw, float) and np.isnan(raw)):
            tz_hrs = float(raw)
            tz_assumed_utc = (tz_hrs == 0.0)
        else:
            warnings.warn(
                f"time_zone_hours is NaN/None for station "
                f"'{station_meta.get('file_name', '?')}'; assuming UTC.",
                UserWarning,
                stacklevel=2,
            )

    # ------------------------------------------------------------------
    # Read raw content
    # ------------------------------------------------------------------
    if isinstance(source, (str, pathlib.Path)):
        source = pathlib.Path(source)
        if not source.exists():
            raise FileNotFoundError(f"GESLA file not found: {source}")
        raw_bytes = source.read_bytes()
    else:
        raw_bytes = source

    text = raw_bytes.decode("utf-8", errors="replace")
    lines = text.splitlines()

    # ------------------------------------------------------------------
    # Parse header
    # ------------------------------------------------------------------
    header_null: Optional[float] = None
    data_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#"):
            # Try to extract null value from header
            if "null value" in stripped.lower():
                parts = stripped.split(":", 1)
                if len(parts) == 2:
                    try:
                        header_null = float(parts[1].strip())
                    except ValueError:
                        pass
        else:
            if stripped:
                data_lines.append(stripped)

    # Resolve null value precedence: explicit arg > file header > default
    if null_value is None:
        if header_null is not None:
            null_value = header_null
        elif station_meta is not None:
            sn = station_meta.get("null_value")
            if sn is not None and not (isinstance(sn, float) and np.isnan(sn)):
                null_value = float(sn)
        if null_value is None:
            from config.settings import GESLA_NULL_VALUE_DEFAULT
            null_value = GESLA_NULL_VALUE_DEFAULT

    # ------------------------------------------------------------------
    # Parse data
    # ------------------------------------------------------------------
    if not data_lines:
        logger.warning("No data lines found in GESLA file.")
        return pd.DataFrame(columns=[
            "datetime_utc", "sea_level_obs_m", "gesla_qc_flag",
            "gesla_use_flag", "tz_assumed_utc",
        ])

    buf = StringIO("\n".join(data_lines))
    try:
        df = pd.read_csv(
            buf,
            sep=r"\s+",
            header=None,
            names=["_date", "_time", "sea_level_obs_m", "gesla_qc_flag", "gesla_use_flag"],
            dtype={
                "sea_level_obs_m": "float32",
                "gesla_qc_flag":   "int8",
                "gesla_use_flag":  "int8",
            },
        )
    except Exception as exc:
        raise ValueError(f"Failed to parse GESLA data section: {exc}") from exc

    # ------------------------------------------------------------------
    # Build datetime index
    # ------------------------------------------------------------------
    df["_datetime_local"] = pd.to_datetime(
        df["_date"] + " " + df["_time"],
        format="mixed",
        dayfirst=False,
        errors="coerce",
    )
    df = df.drop(columns=["_date", "_time"])

    n_bad_dt = df["_datetime_local"].isna().sum()
    if n_bad_dt > 0:
        logger.warning("  %d rows with unparseable datetime — dropped.", n_bad_dt)
        df = df.dropna(subset=["_datetime_local"])

    # Convert to UTC
    if tz_hrs != 0.0:
        df["datetime_utc"] = df["_datetime_local"] - pd.Timedelta(hours=tz_hrs)
        logger.debug("  Applied UTC offset: −%.1f h", tz_hrs)
    else:
        df["datetime_utc"] = df["_datetime_local"]

    df = df.drop(columns=["_datetime_local"])
    df["tz_assumed_utc"] = tz_assumed_utc

    # ------------------------------------------------------------------
    # Mask null / missing values
    # ------------------------------------------------------------------
    null_tol = abs(null_value) * 0.01
    df.loc[np.abs(df["sea_level_obs_m"] - null_value) < null_tol, "sea_level_obs_m"] = np.nan
    df["sea_level_obs_m"] = df["sea_level_obs_m"].astype("float32")

    # ------------------------------------------------------------------
    # Annotate with station metadata
    # ------------------------------------------------------------------
    if station_meta is not None:
        for col, key in [
            ("station_file_name", "file_name"),
            ("station_name",      "site_name"),
            ("site_code",         "site_code"),
            ("country",           "country"),
            ("station_lon",       "longitude"),
            ("station_lat",       "latitude"),
        ]:
            df[col] = station_meta.get(key, np.nan)

    # Sort and deduplicate on datetime_utc
    df = df.sort_values("datetime_utc").drop_duplicates(subset=["datetime_utc"])
    df = df.reset_index(drop=True)

    # Ensure required column order
    order = [
        "datetime_utc",
        "sea_level_obs_m",
        "gesla_qc_flag",
        "gesla_use_flag",
        "tz_assumed_utc",
    ]
    if station_meta is not None:
        order += [
            "station_file_name", "station_name", "site_code",
            "country", "station_lon", "station_lat",
        ]
    extra = [c for c in df.columns if c not in order]
    df = df[order + extra]

    return df
