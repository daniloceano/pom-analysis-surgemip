# POM_analysis

Python toolkit for reading, processing, and validating Princeton Ocean Model (POM) outputs for the **SurgeMIP** inter-comparison project — storm-surge modelling over a near-global domain, ERA5-forced (2013–2018).

---

## Quick navigation

| Where to look | What you'll find |
|---|---|
| This file | Project overview, setup, quick-start |
| [`scripts/README.md`](scripts/README.md) | All scripts, with usage examples |
| [`utils/README.md`](utils/README.md) | Utility modules (`grads_reader`, `gesla`) |
| [`data/README.md`](data/README.md) | Data layout and what is/isn't versioned |
| [`docs/decisions.md`](docs/decisions.md) | Key assumptions and design decisions |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Coding conventions and commit checklist |
| [`site/README.md`](site/README.md) | Interactive validation website |

---

## Overview

Two parallel POM runs are available:

| Run | Description |
|-----|-------------|
| **tide** | Full 3-D POM with FES2022 tidal forcing |
| **notide** | Meteorological-only (no tides) — the storm-surge signal |

Tidal signal ≈ `eta_tide − eta_notide`

---

## Dataset

| Property | Value |
|----------|-------|
| Model | Princeton Ocean Model (POM) — 3-D baroclinic |
| Atmospheric forcing | ERA5 (hourly, 0.25°) |
| Tidal forcing | FES2022 harmonic constants |
| Period | 2013-01-01 → 2018-12-31 |
| Resolution | 0.30° × 0.25° (lon × lat), 1200 × 584 points |
| Domain | ~Global: 179.85°W–179.85°E, 70.625°S–75.125°N |
| Time step | 1 hour (52 584 steps per file) |
| File format | GrADS binary (`.gra`) + descriptor (`.ctl`) |

### Raw data location (read-only)
```
/p1-sto-swell/ricamarg/SurgeMIP/RUN_3D/
  eta-tide_SurgeMIP_ERA5_2013-2018.{ctl,gra}
  eta-notide_SurgeMIP_ERA5_2013-2018.{ctl,gra}
  SurgeMIP_3D_{tide,notide}.ctl            ← monthly template CTL
  SurgeMIP_YYYYMM_3D_{tide,notide}.gra     ← monthly binaries
```

---

## Project structure

```
POM_analysis/
├── config/settings.py        ← ALL paths, constants, styles — edit here
├── utils/
│   ├── grads_reader.py       ← GrADS CTL parser + memory-mapped binary reader
│   ├── gesla.py              ← GESLA-4 station-list parser + file parser
│   └── tidal_filters.py      ← Godin filter + FES2022 tide prediction
├── scripts/
│   ├── pipeline/
│   │   ├── run_gesla_validation_pipeline.py  ← one-command orchestrator
│   │   └── prepare_site_data.py              ← generate JSON data for website
│   ├── exploratory/          ← dataset inspection and quick-look plots
│   ├── preprocessing/        ← point extraction from model files
│   ├── data/                 ← GESLA download and preparation
│   └── validation/           ← model vs. observations comparison + metrics + maps
├── data/
│   ├── SurgeMIP_files/       ← SurgeMIP_stnlist.csv (versioned)
│   ├── gesla/raw/            ← GESLA raw data (NOT versioned)
│   └── processed/            ← extracted CSVs (NOT versioned)
│       ├── gesla/
│       │   ├── observations/         ← raw GESLA obs CSVs
│       │   ├── observations_godin/   ← Godin-filtered de-tided obs
│       │   └── observations_fes/     ← FES2022-subtracted de-tided obs
│       └── validation/
│           ├── gesla_vs_model/       ← raw comparison CSVs
│           ├── godin_filter/         ← Godin-mode comparison CSVs
│           └── minus_fes_tide/       ← FES2022-mode comparison CSVs
├── figures/
│   └── validation/
│       ├── raw/              ← station maps for raw validation
│       ├── godin_filter/     ← station maps for Godin-filter validation
│       └── minus_fes_tide/   ← station maps for FES2022 validation
├── results/
│   └── validation/
│       ├── station_metrics.csv               ← raw mode per-station metrics
│       ├── godin_filter/station_metrics.csv  ← Godin mode
│       └── minus_fes_tide/station_metrics.csv ← FES2022 mode
├── site/                     ← interactive validation website (Next.js)
├── docs/                     ← design decisions and notes
├── notebooks/                ← exploratory Jupyter notebooks
├── environment.yml
├── setup_env.sh
├── CONTRIBUTING.md
└── README.md
```

---

## Setup

```bash
# 1. Create the conda environment
bash setup_env.sh
conda activate pom

# 2. Verify
python -c "from utils.grads_reader import GrADSReader; print('OK')"
python -c "from utils.gesla import load_station_list; print('OK')"
```

To update after changes to `environment.yml`:
```bash
bash setup_env.sh --update
```

---

## Quick start

### Inspect the model dataset
```bash
python scripts/exploratory/inspect_data.py
python scripts/exploratory/inspect_data.py --tstep 100 --region brazil_south
```

### Extract a model time series at a point
```bash
python scripts/preprocessing/extract_point.py --station santos
python scripts/preprocessing/extract_point.py --lon -46.30 --lat -23.97 --label santos
```

### GESLA validation pipeline — one command

Three validation modes are supported:

| Mode | Description | Model target |
|------|-------------|--------------|
| `raw` | Raw tidal observations vs model | notide + tide |
| `godin_filter` | Godin low-pass filter removes tides from obs | notide only |
| `minus_fes_tide` | FES2022 predicted tide subtracted from obs | notide only |

```bash
# Run all three modes end-to-end (skips completed stages automatically):
python scripts/pipeline/run_gesla_validation_pipeline.py --mode all --workers 50

# Raw mode only (default):
python scripts/pipeline/run_gesla_validation_pipeline.py --workers 50

# First run requires GESLA-4 download URL:
python scripts/pipeline/run_gesla_validation_pipeline.py \
    --url "https://<your-download-link>/GESLA4.zip" --mode all --workers 50

# Dry-run to preview what will happen:
python scripts/pipeline/run_gesla_validation_pipeline.py --mode all --dry-run

# Force re-generate figures only:
python scripts/pipeline/run_gesla_validation_pipeline.py --mode all --force-maps
```

Output locations:
```
figures/validation/raw/             ← station maps, raw validation
figures/validation/godin_filter/    ← station maps, Godin de-tided
figures/validation/minus_fes_tide/  ← station maps, FES2022 de-tided
results/validation/station_metrics.csv
results/validation/godin_filter/station_metrics.csv
results/validation/minus_fes_tide/station_metrics.csv
```

See [`scripts/README.md`](scripts/README.md) for full options.

### Interactive validation website

```bash
# 1. Generate JSON data for the website (run after pipeline)
python scripts/pipeline/prepare_site_data.py

# 2. Run locally
cd site && npm install && npm run dev
# → open http://localhost:3000

# 3. Deploy to Vercel
cd site && npx vercel --prod
```

See [`site/README.md`](site/README.md) for full deployment instructions.

---

## Configuration

All settings live in [`config/settings.py`](config/settings.py).

| Symbol | Description |
|--------|-------------|
| `TIDE_CTL / NOTIDE_CTL` | Paths to the combined CTL descriptors |
| `SURGEMIP_STNLIST` | Path to `SurgeMIP_stnlist.csv` |
| `GESLA_ZIP_URL` | Download URL (or set `GESLA_ZIP_URL` env var) |
| `GESLA_OBS_DIR` | Per-station GESLA observation CSVs |
| `GESLA_VS_MODEL_DIR` | Final comparison CSVs |
| `STATION_MODEL_INDEX` | Station–grid-point pairing table |
| `STATION_METRICS_CSV` | Per-station skill scores (RMSE, bias, r, …) |
| `RESULTS_VALID_DIR` | Parent directory for results/validation/ outputs |
| `STATIONS` | Dict of reference tide-gauge locations |
| `PLOT_STYLE` | Colormaps, figure sizes, DPI, map extents |

---

## Authors

- **D. C. de Souza** — analysis, scripts
- **R. Camargo** — POM model runs, raw data

Part of the **SurgeMIP** inter-comparison project for storm-surge models.

---

> **Maintainers:** every functional change must be accompanied by a
> documentation update — see [`CONTRIBUTING.md`](CONTRIBUTING.md).
