# Data

This directory holds station metadata and processed model/observation outputs.

---

## What IS versioned

| Path | Description |
|------|-------------|
| `data/SurgeMIP_files/SurgeMIP_stnlist.csv` | SurgeMIP station list (~550 stations, ~25 columns) |

The station list is a lightweight metadata CSV and is safe to version.

---

## What is NOT versioned

The following are excluded via `.gitignore` because they are either large,
reproducible from scripts, or require external access:

| Path | Description | How to reproduce |
|------|-------------|-----------------|
| `data/gesla/raw/` | GESLA-4 ZIP archive and extracted station files | `scripts/data/download_gesla.py` |
| `data/processed/gesla/` | Parsed per-station observation CSVs | `scripts/data/prepare_gesla.py` |
| `data/processed/validation/model_ts/` | Model time series per station | `scripts/validation/extract_model_for_gesla_stations.py` |
| `data/processed/validation/gesla_vs_model/` | Final comparison CSVs | `scripts/validation/build_comparison_csvs.py` |
| `data/raw_links/` | Optional symlinks to raw GrADS files | (manual) |

---

## Processed data schema

### `data/processed/gesla/observations/<file_name>.csv.gz`

One file per GESLA station.

| Column | Type | Description |
|--------|------|-------------|
| `datetime_utc` | datetime | Timestamp in UTC |
| `sea_level_obs_m` | float32 | Sea level [m]; NaN where missing |
| `gesla_qc_flag` | int8 | GESLA QC flag (1 = good) |
| `gesla_use_flag` | int8 | GESLA use flag (1 = recommended) |
| `tz_assumed_utc` | bool | True if no timezone correction was applied |
| `station_file_name` | str | Station file identifier |
| `station_name` | str | Human-readable station name |
| `site_code` | str | GESLA site code |
| `country` | str | Country |
| `station_lon` | float | Station longitude |
| `station_lat` | float | Station latitude |

### `data/processed/gesla/stations_manifest.csv`

Lean metadata table with one row per station of interest.

### `data/processed/validation/model_ts/<file_name>.csv.gz`

Model time series at the nearest grid point for each GESLA station.

| Column | Description |
|--------|-------------|
| `datetime_utc` | UTC timestamp (hourly) |
| `model_eta_tide_m` | Sea-surface elevation from the `tide` run [m] |
| `model_eta_notide_m` | Sea-surface elevation from the `notide` run [m] |
| `model_tide_minus_notide_m` | Tidal signal = tide − notide [m] |
| `station_*`, `model_*`, `grid_*`, `distance_km` | Station and grid-point metadata |

### `data/processed/validation/station_model_index.csv`

Summary of station–grid-point pairings.

| Column | Description |
|--------|-------------|
| `station_file_name` | GESLA station identifier |
| `station_lon/lat` | Station coordinates |
| `model_lon/lat` | Nearest model grid-point coordinates |
| `grid_i/j` | Grid indices (column, row) |
| `distance_km` | Great-circle distance [km] |

### `data/processed/validation/gesla_vs_model/<file_name>.csv.gz`

Final comparison CSV (one per station).  Columns are the union of the
observation and model-TS schemas above.  See [`scripts/README.md`](../scripts/README.md)
for the full column list.
