# Scripts

This directory contains all executable scripts for the POM_analysis project.
Each subdirectory groups scripts by purpose.

---

## Directory layout

```
scripts/
├── pipeline/
│   └── run_gesla_validation_pipeline.py  ← ONE-COMMAND end-to-end orchestrator
├── exploratory/
│   └── inspect_data.py                   ← dataset description + snapshot map
├── preprocessing/
│   └── extract_point.py                  ← point time-series extraction from model
├── data/
│   ├── download_gesla.py                 ← download GESLA-4 archive
│   └── prepare_gesla.py                  ← parse GESLA files → per-station CSVs
└── validation/
    ├── extract_model_for_gesla_stations.py  ← model extraction for all GESLA stations
    ├── build_comparison_csvs.py             ← merge obs + model → final CSVs
    ├── compute_station_metrics.py           ← per-station skill scores
    └── plot_station_metric_map.py           ← station maps coloured by metric
```

All paths and settings are centralised in [`config/settings.py`](../config/settings.py).

---

## Exploratory

### `inspect_data.py`

Prints a detailed dataset summary and saves a 3-panel snapshot map.

```bash
python scripts/exploratory/inspect_data.py
python scripts/exploratory/inspect_data.py --tstep 100 --region global
# Options: --tstep N | --region {global,south_atlantic,brazil_south} | --out PATH
```

---

## Preprocessing

### `extract_point.py`

Extracts the full (or period-filtered) `eta_tide` and `eta_notide` time series
at the nearest model grid point and saves a compressed CSV.

```bash
# By coordinates:
python scripts/preprocessing/extract_point.py \
    --lon -46.30 --lat -23.97 --label santos

# Using a predefined station name:
python scripts/preprocessing/extract_point.py --station santos

# Subset period:
python scripts/preprocessing/extract_point.py \
    --station buenos_aires --t_start 2016-01-01 --t_end 2016-12-31

# Only the notide run:
python scripts/preprocessing/extract_point.py --station santos --run notide
```

Predefined stations (from `config/settings.py`): `santos`, `cananeia`,
`imbituba`, `ilha_fiscal`, `macae`, `salvador`, `fortaleza`, `belem`,
`buenos_aires`, `montevideo`, `open_ocean`.

Output: `data/processed/<label>_<t0>_<t1>.csv.gz`

---

## Data — GESLA pipeline

The GESLA-4 pipeline prepares tide-gauge observations for comparison with the
model.  Run the steps **in order**.

### Step 1 — `download_gesla.py`

Downloads the GESLA-4 ZIP archive and (optionally) extracts station files.

> GESLA-4 requires **free registration** at
> <https://gesla787883612.wordpress.com/downloads/>.  After registering you
> receive a direct download link.

```bash
# Download and extract all SurgeMIP stations from the ZIP:
python scripts/data/download_gesla.py \
    --url "https://<your-download-link>/GESLA4.zip" \
    --extract

# Use a ZIP you already have:
python scripts/data/download_gesla.py \
    --zip-file /path/to/GESLA4.zip --extract

# Or set the URL via environment variable:
export GESLA_ZIP_URL="https://…/GESLA4.zip"
python scripts/data/download_gesla.py --extract

# Force re-download:
python scripts/data/download_gesla.py --url "…" --force
```

Outputs:
- `data/gesla/raw/GESLA4.zip`
- `data/gesla/raw/stations/<file_name>` (one file per station)

### Step 2 — `prepare_gesla.py`

Parses each GESLA station file and saves a standardised observation CSV.

```bash
# From extracted files (default):
python scripts/data/prepare_gesla.py

# From the ZIP directly (no prior extraction needed):
python scripts/data/prepare_gesla.py --zip-file data/gesla/raw/GESLA4.zip

# Single station (for testing):
python scripts/data/prepare_gesla.py \
    --station san_francisco_ca-551a-usa-uhslc

# Overwrite existing:
python scripts/data/prepare_gesla.py --force

# Only generate the manifest:
python scripts/data/prepare_gesla.py --manifest-only
```

Outputs:
- `data/processed/gesla/observations/<file_name>.csv.gz` — one per station
- `data/processed/gesla/stations_manifest.csv` — lean metadata table

**Observation CSV columns:**
`datetime_utc`, `sea_level_obs_m`, `gesla_qc_flag`, `gesla_use_flag`,
`tz_assumed_utc`, `station_file_name`, `station_name`, `site_code`,
`country`, `station_lon`, `station_lat`

---

## Validation

### Step 3a — `extract_model_for_gesla_stations.py`

Extracts model (`eta_tide`, `eta_notide`) time series at the nearest grid
point for every station.

```bash
# All stations, full period:
python scripts/validation/extract_model_for_gesla_stations.py

# Restrict time window:
python scripts/validation/extract_model_for_gesla_stations.py \
    --t-start 2016-01-01 --t-end 2017-01-01

# Single station:
python scripts/validation/extract_model_for_gesla_stations.py \
    --station san_francisco_ca-551a-usa-uhslc

# Overwrite existing:
python scripts/validation/extract_model_for_gesla_stations.py --force
```

Outputs:
- `data/processed/validation/model_ts/<file_name>.csv.gz`
- `data/processed/validation/station_model_index.csv`

**Model-TS CSV columns:**
`datetime_utc`, `model_eta_tide_m`, `model_eta_notide_m`,
`model_tide_minus_notide_m`, `station_file_name`, `station_name`,
`site_code`, `country`, `station_lon`, `station_lat`, `model_lon`,
`model_lat`, `grid_i`, `grid_j`, `distance_km`

**Station-model index columns:**
`station_file_name`, `station_name`, `site_code`, `country`,
`station_lon`, `station_lat`, `model_lon`, `model_lat`,
`grid_i`, `grid_j`, `distance_km`

### Step 3b — `build_comparison_csvs.py`

Merges GESLA observations with model time series → final comparison CSV per
station.

```bash
# All stations:
python scripts/validation/build_comparison_csvs.py

# Restrict to the model period:
python scripts/validation/build_comparison_csvs.py \
    --t-start 2013-01-01 --t-end 2019-01-01

# Single station:
python scripts/validation/build_comparison_csvs.py \
    --station san_francisco_ca-551a-usa-uhslc

# Resample GESLA to 1-hourly before merging:
python scripts/validation/build_comparison_csvs.py --resample 1h

# Overwrite:
python scripts/validation/build_comparison_csvs.py --force
```

Output: `data/processed/validation/gesla_vs_model/<file_name>.csv.gz`

**Final CSV columns:**
`datetime_utc`, `sea_level_obs_m`, `gesla_qc_flag`, `gesla_use_flag`,
`model_eta_tide_m`, `model_eta_notide_m`, `model_tide_minus_notide_m`,
`station_file_name`, `station_name`, `site_code`, `country`,
`station_lon`, `station_lat`, `model_lon`, `model_lat`,
`grid_i`, `grid_j`, `distance_km`

---

### Step 4 — `compute_station_metrics.py`

Reads all comparison CSVs and computes per-station skill scores.

```bash
# All stations:
python scripts/validation/compute_station_metrics.py

# Overwrite existing results:
python scripts/validation/compute_station_metrics.py --force

# Single station (for testing):
python scripts/validation/compute_station_metrics.py \
    --station san_francisco_ca-551a-usa-uhslc
```

Output: `results/validation/station_metrics.csv` (and `.parquet` if pyarrow is available)

**Metrics computed** (for both `model_eta_notide` and `model_eta_tide` targets):
RMSE, bias (model − obs), Pearson r, observed mean/std/max, model mean/std/max, valid-sample count.
Only rows with `gesla_qc_flag == 1` **and** `gesla_use_flag == 1` are used.

### Step 5 — `plot_station_metric_map.py`

Generates a global station map with scatter points coloured by a chosen metric.

```bash
# Default (RMSE notide):
python scripts/validation/plot_station_metric_map.py

# Choose metric:
python scripts/validation/plot_station_metric_map.py --metric bias_notide

# Custom limits and regional extent:
python scripts/validation/plot_station_metric_map.py \
    --metric rmse_notide --vmin 0 --vmax 0.5 \
    --extent -70 20 -60 10

# Overwrite:
python scripts/validation/plot_station_metric_map.py --force
```

Available `--metric` values: `rmse_notide`, `bias_notide`, `pearson_r_notide`,
`obs_mean_m`, `obs_max_m`, `model_notide_mean_m`, `model_notide_max_m`,
`rmse_tide`, `bias_tide`, `pearson_r_tide`, `model_tide_mean_m`,
`model_tide_max_m`, `n_valid`.

Output: `figures/validation/station_map_<metric>.png`

---

## One-command pipeline

### `run_gesla_validation_pipeline.py`

Orchestrates all stages in order with parallel execution (default 50 workers).

```bash
# Full pipeline, skip completed stages:
python scripts/pipeline/run_gesla_validation_pipeline.py

# Custom worker count:
python scripts/pipeline/run_gesla_validation_pipeline.py --workers 50

# Dry-run (print plan, do nothing):
python scripts/pipeline/run_gesla_validation_pipeline.py --dry-run

# Force everything from scratch:
python scripts/pipeline/run_gesla_validation_pipeline.py --force-all

# Force individual stages:
python scripts/pipeline/run_gesla_validation_pipeline.py \
    --force-extract --force-build --force-metrics --force-maps

# Restrict to a time window:
python scripts/pipeline/run_gesla_validation_pipeline.py \
    --t-start 2016-01-01 --t-end 2017-01-01

# Single station (testing):
python scripts/pipeline/run_gesla_validation_pipeline.py \
    --station san_francisco_ca-551a-usa-uhslc
```

**Stages run by the pipeline:**

| Stage | Script called | Parallelised |
|-------|--------------|-------------|
| 1 | `download_gesla.py` logic | No (one-shot) |
| 2 | `prepare_gesla.py:process_station` | Yes — ThreadPoolExecutor |
| 3 | `extract_model_for_gesla_stations.py:extract_one_station` | Yes — ThreadPoolExecutor |
| 4 | `build_comparison_csvs.py:merge_one_station` | Yes — ThreadPoolExecutor |
| 5 | `compute_station_metrics.py` | No (fast sequential) |
| 6 | `plot_station_metric_map.py` × N metrics | No (matplotlib not thread-safe) |

**Force flags:** `--force-download`, `--force-prepare`, `--force-extract`,
`--force-build`, `--force-metrics`, `--force-maps`, `--force-all`.

---

## Workflow diagram

```
GESLA-4 ZIP archive
        │
        ▼
download_gesla.py ──► data/gesla/raw/stations/
        │
        ▼
prepare_gesla.py  ──► data/processed/gesla/observations/<stn>.csv.gz
                  ──► data/processed/gesla/stations_manifest.csv

Raw GrADS binaries (read-only)
        │
        ▼
extract_model_for_gesla_stations.py
        ──► data/processed/validation/model_ts/<stn>.csv.gz
        ──► data/processed/validation/station_model_index.csv

observations + model_ts
        │
        ▼
build_comparison_csvs.py
        ──► data/processed/validation/gesla_vs_model/<stn>.csv.gz
        │
        ▼
compute_station_metrics.py
        ──► results/validation/station_metrics.csv
        ──► results/validation/station_metrics.parquet
        │
        ▼
plot_station_metric_map.py  (× N metrics)
        ──► figures/validation/station_map_<metric>.png
```

All steps above are automated by the pipeline orchestrator:
```
run_gesla_validation_pipeline.py  (parallelised, idempotent)
```
