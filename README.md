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
│   └── gesla.py              ← GESLA-4 station-list parser + file parser
├── scripts/
│   ├── exploratory/          ← dataset inspection and quick-look plots
│   ├── preprocessing/        ← point extraction from model files
│   ├── data/                 ← GESLA download and preparation
│   └── validation/           ← model vs. observations comparison
├── data/
│   ├── SurgeMIP_files/       ← SurgeMIP_stnlist.csv (versioned)
│   ├── gesla/raw/            ← GESLA raw data (NOT versioned)
│   └── processed/            ← extracted CSVs (NOT versioned)
├── docs/                     ← design decisions and notes
├── notebooks/                ← exploratory Jupyter notebooks
├── figures/                  ← output figures (NOT versioned)
├── results/                  ← numerical results (NOT versioned)
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

### GESLA validation pipeline (3 steps)

```bash
# Step 1 — download GESLA-4 (free registration at gesla787883612.wordpress.com)
python scripts/data/download_gesla.py \
    --url "https://<your-download-link>/GESLA4.zip" --extract

# Step 2 — parse GESLA station files → per-station observation CSVs
python scripts/data/prepare_gesla.py

# Step 3a — extract model time series for every GESLA station
python scripts/validation/extract_model_for_gesla_stations.py

# Step 3b — merge obs + model → final comparison CSVs
python scripts/validation/build_comparison_csvs.py
```

See [`scripts/README.md`](scripts/README.md) for full options.

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
