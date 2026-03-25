# POM / GESLA-4 Validation Explorer — Website

Interactive scientific website for exploring the validation of Princeton Ocean Model (POM) storm-surge outputs against GESLA-4 tide-gauge observations (2013–2018).

## Features

- **Interactive global map** — stations coloured by any validation metric (RMSE, bias, Pearson r, etc.)
- **Mode switching** — toggle between Raw, Godin, and FES2022 validation modes
- **Station selection** — click any station on the map to inspect it
- **Time series chart** — daily-mean time series (obs + model) with Plotly.js zoom/pan/rangeslider
- **Metrics card** — per-station skill scores summary panel

## Data preparation

Before running the site, generate the JSON data files **from the project root**:

```bash
# Generate station_metrics.json + all time series JSONs:
python scripts/pipeline/prepare_site_data.py

# Force rebuild after re-running the pipeline:
python scripts/pipeline/prepare_site_data.py --force

# Metrics only (faster, skip time series):
python scripts/pipeline/prepare_site_data.py --skip-ts
```

This writes to `site/public/data/`:
```
station_metrics.json              ← unified metrics for all modes (map data)
ts/raw/<station_id>.json          ← daily-mean time series, raw mode
ts/godin_filter/<station_id>.json ← daily-mean time series, Godin mode
ts/minus_fes_tide/<station_id>.json ← daily-mean time series, FES2022 mode
```

Only modes with an existing `results/validation/*/station_metrics.csv` will appear.
After running the full pipeline (`--mode all`), re-run `prepare_site_data.py`.

## Run locally

```bash
cd site
npm install        # only needed once
npm run dev        # → http://localhost:3000
```

For a production preview:
```bash
npm run build && npm start
```

## Deploy to Vercel

```bash
cd site
npx vercel --prod
```

Or connect the GitHub repository on [vercel.com](https://vercel.com) and set the
**root directory** to `site/`. No environment variables are required — all data
is served as static files from `public/data/`.

**Note**: The `public/data/` directory must be committed or uploaded to Vercel.
Time series files (~500 stations × 3 modes) total ~15–20 MB compressed; this is
within Vercel's static asset limits.

## Architecture

| Layer | Technology |
|-------|-----------|
| Framework | Next.js 16 (App Router, Turbopack) |
| Language | TypeScript |
| Styling | Tailwind CSS v4 |
| Map | Leaflet (loaded client-side via `dynamic({ ssr: false })`) |
| Charts | Plotly.js basic dist (loaded client-side via `dynamic({ ssr: false })`) |
| Data | Static JSON in `public/data/` — no backend required |
| Hosting | Vercel (static) |

## Validation modes

| Mode | Button label | Description |
|------|-------------|-------------|
| `raw` | Raw | Compare raw tidal obs vs model_eta_notide + model_eta_tide |
| `godin_filter` | Godin | Godin (1972) low-pass filter de-tides obs; compare vs surge only |
| `minus_fes_tide` | FES2022 | FES2022 harmonic tide subtracted from obs; compare vs surge only |

## Updating after pipeline changes

```bash
# From project root
python scripts/pipeline/prepare_site_data.py --force

# Redeploy
cd site && npx vercel --prod
```
