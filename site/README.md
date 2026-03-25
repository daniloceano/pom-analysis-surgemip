# POM / GESLA-4 Validation Explorer — Website

Interactive scientific website for exploring the validation of Princeton Ocean Model (POM)
storm-surge outputs against GESLA-4 tide-gauge observations (2013–2018).

## Features

- **Interactive global map** — stations coloured by any validation metric (RMSE, bias, Pearson r, etc.)
- **Observation treatment selector** — switch between Raw (descriptive), Godin, and FES2022 (surge validation)
- **Station selection** — click any station on the map to inspect it
- **Time series chart** — daily-mean time series (obs + model) with Plotly.js zoom/pan/rangeslider
- **Metrics card** — per-station skill scores with scientific context (descriptive vs. validation)

---

## Data preparation

Before running the site, generate the JSON data files **from the project root**:

```bash
# First run (or when new pipeline outputs are available):
python scripts/pipeline/prepare_site_data.py --force

# If data has not changed since last run, this is a no-op:
python scripts/pipeline/prepare_site_data.py

# Metrics only (faster — skips ~15-min time series generation):
python scripts/pipeline/prepare_site_data.py --force --skip-ts
```

Output written to `site/public/data/`:
```
station_metrics.json                   ← unified metrics for all modes (map data)
ts/raw/<station_id>.json               ← raw mode time series
ts/godin_filter/<station_id>.json      ← Godin-detided time series
ts/minus_fes_tide/<station_id>.json    ← FES2022-detided time series
```

Only modes with an existing `results/validation/*/station_metrics.csv` appear in the site.

---

## Run locally

```bash
cd site
npm install        # only needed once
npm run dev        # → http://localhost:3000
```

Production preview:
```bash
npm run build && npm start
```

---

## Deploy to Vercel (GitHub integration)

The site is connected to GitHub. Vercel automatically redeploys on every `git push`.
The data files in `site/public/data/` are committed to the repository so that
Vercel's GitHub integration picks them up.

### Full update workflow

```bash
# 1. Re-run the pipeline if model/obs data changed (optional, ~2–4 hours)
python scripts/pipeline/run_gesla_validation_pipeline.py --mode all --force-metrics

# 2. Regenerate site data files (always use --force to overwrite existing JSONs)
python scripts/pipeline/prepare_site_data.py --force

# 3. Stage the updated data files
git add site/public/data/

# 4. Check if anything actually changed
git status
# If "nothing to commit" → the JSON content is identical to what is already
# in git → the site already has the latest data → skip to step 6 if desired.

# 5. Commit and push (triggers automatic Vercel redeploy)
git commit -m "chore: update site data — $(date '+%Y-%m-%d')"
git push
```

### When "nothing to commit" after prepare_site_data.py --force

This is normal and expected when the pipeline outputs have not changed since the
last commit. The data in git already matches the locally generated files.
Vercel already has the latest data. No action needed unless you re-ran the pipeline.

### Manual deploy (bypasses git, uploads local files directly)

Use this when you cannot or do not want to commit data files to git:

```bash
cd site
npx vercel --prod
```

This uploads local files directly to Vercel, including `public/data/`, regardless
of `.gitignore` or git state.

---

## Architecture

| Layer | Technology |
|-------|-----------|
| Framework | Next.js 16 (App Router, Turbopack) |
| Language | TypeScript |
| Styling | Tailwind CSS v4 |
| Map | Leaflet (loaded client-side via `dynamic({ ssr: false })`) |
| Charts | Plotly.js basic dist (loaded client-side via `dynamic({ ssr: false })`) |
| Data | Static JSON in `public/data/` — no backend required |
| Hosting | Vercel (GitHub integration, auto-deploy on push) |

---

## Observation treatments and scientific context

| Button | Internal key | Obs treatment | Model target | Context |
|--------|-------------|--------------|--------------|---------|
| Raw | `raw` | none (obs with tide) | POM tide | Descriptive only |
| Godin | `godin_filter` | Godin (1972) low-pass filter | POM no-tide | Surge validation ✓ |
| FES2022 | `minus_fes_tide` | FES2022 harmonic subtraction | POM no-tide | Surge validation ✓ |

**Important:** in Raw mode, `rmse_notide` (obs_raw vs POM no-tide) is dominated by the
tidal signal in obs and is NOT a surge validation metric. The site displays `rmse_tide`
(obs_raw vs POM tide) as the primary metric in Raw mode.

See `results/validation/README.md` for a detailed description of each comparison type.
