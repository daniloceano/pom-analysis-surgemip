# SCIENTIFIC_NOTES.md — POM / GESLA-4 Storm-Surge Validation

**Project:** SurgeMIP — Princeton Ocean Model inter-comparison against GESLA-4 tide gauges
**Period of analysis:** 2013–2018
**PI / Contact:** Danilo Couto de Souza (IAG-USP / Petrobras CENPES cooperation)
**Last updated:** 2026-03-25

---

## Research Questions

1. How well does the Princeton Ocean Model (POM), forced by ERA5 reanalysis, reproduce
   observed storm-surge (non-tidal) sea-level variability at global tide-gauge locations?

2. How sensitive are the skill scores to the method used to remove the astronomical tidal
   signal from the observations (Godin low-pass filter vs FES2022 harmonic subtraction)?

3. Where do the largest model biases and RMSE occur, and what physical mechanisms
   (tidal interference, coastal geometry, storm-track climatology) might explain them?

4. What fraction of the GESLA-4 global tide-gauge network has sufficient data overlap
   with the model period (2013–2018) to support robust skill assessment?

---

## Physical / Statistical Framework

### Storm surge definition

Storm surge (meteorological sea level) is defined here as the sea-level anomaly driven
exclusively by atmospheric forcing (wind stress, atmospheric pressure gradient):

```
η_surge(x, t) = η_notide(x, t)
```

where `η_notide` is the sea-surface elevation from the POM run without tidal forcing.

The total sea level in the tidal run decomposes approximately as:

```
η_tide(x, t) ≈ η_surge(x, t) + η_astro(x, t) + ε(x, t)
```

where `η_astro` is the astronomical tidal response and `ε` is the non-linear residual
from tide–surge interactions. Given POM uses FES2022 harmonic constants as tidal forcing:

```
η_tide(x, t) − η_notide(x, t) ≈ η_astro(x, t)          [Eq. 1]
```

This tidal signal is stored in the comparison CSVs as `model_tide_minus_notide_m`.

### Consequence for "POM tide detided"

Detiding POM tide using the model's own tidal signal gives:

```
η_tide − [η_tide − η_notide] = η_notide                  [Eq. 2]
```

Therefore, comparing `obs_detided vs POM_tide_detided` is algebraically equivalent
to comparing `obs_detided vs POM_notide`. This justifies why the pipeline does not
need a separate "POM tide detided" product — the existing comparisons are sufficient.

### Skill metrics

For each station, the following are computed on hourly pairs passing QC:

| Metric | Formula | Unit |
|--------|---------|------|
| RMSE | √( mean[(obs − model)²] ) | m |
| Bias | mean(model − obs) | m |
| Pearson r | cov(obs, model) / (σ_obs · σ_model) | — |

QC filter applied: `gesla_qc_flag == 1` AND `gesla_use_flag == 1` (GESLA-4 recommended flags).
Minimum 10 valid co-located samples required; otherwise all metrics are set to NaN.

---

## Datasets and Variables

### Princeton Ocean Model (POM) — SurgeMIP runs

| Parameter | Value |
|-----------|-------|
| Model | POM 3-D baroclinic |
| Spatial resolution | 0.30° × 0.25° (lon × lat), 1200 × 584 grid points |
| Temporal resolution | Hourly |
| Period | 2013-01-01 00:00 UTC to 2018-12-31 23:00 UTC (52 584 steps) |
| Domain | Near-global: 179.85°W–179.85°E, 70.625°S–75.125°N |
| Atmospheric forcing | ERA5 (hourly, 0.25°): 10-m wind stress + mean sea-level pressure |
| Tidal forcing | FES2022 harmonic constants |
| Runs | `tide`: full run with tidal forcing; `notide`: meteorological only |
| Output variables | Sea-surface elevation (`η`), depth-averaged currents, SST, SSS |
| Storage | GrADS binary format (~138 GB per run, memory-mapped access) |
| Produced by | R. Marques (ricamarg), IAG-USP |

**Files:**
- `eta-tide_SurgeMIP_ERA5_2013-2018.{ctl,gra}` — full tidal + surge run
- `eta-notide_SurgeMIP_ERA5_2013-2018.{ctl,gra}` — surge-only (meteorological) run

### GESLA-4 tide-gauge observations

| Parameter | Value |
|-----------|-------|
| Dataset | Global Extreme Sea Level Analysis version 4 (GESLA-4) |
| Station count | 5 119 records globally; 515 stations used after SurgeMIP filtering |
| Variables | Sea-surface elevation [m], QC flag, use flag |
| Temporal resolution | Varies: hourly to 5-minute (station-dependent) |
| Period | Multi-decadal; overlap with model: 2013–2018 |
| QC | GESLA-4 quality-controlled; only flag=1 (good) and use=1 (recommended) retained |
| Access | GESLA-4 ZIP archive (registration required at gesla787883612.wordpress.com) |
| Reference | Haigh et al. (2022); Woodworth et al. (2023) |

**Station selection criteria (SurgeMIP_stnlist.csv):**
- Selected by the SurgeMIP coordination group
- Covers South Atlantic, European Atlantic, Indian Ocean, Pacific, and Arctic coasts

### FES2022 tidal model (for obs detiding)

| Parameter | Value |
|-----------|-------|
| Model | FES2022 (Finite Element Solution) |
| Type | Hydrodynamic tide model with harmonic constants |
| Access via | `eo-tides` Python library |
| Coverage | Global; NetCDF files clipped to project domain |
| Application | Predict and subtract astronomical tide from GESLA-4 obs (fes2022_notide mode) |

---

## Methodology

### Pipeline overview (6 stages)

```
Stage 1  → Download and extract GESLA-4 archive
Stage 2  → Parse GESLA-4 files → hourly CSVs per station
Stage 3  → Extract POM (tide + notide) time series at nearest wet grid point
Stage 3.5→ Apply tidal detiding to obs (Godin filter or FES2022 subtraction)
Stage 4  → Merge obs + model → comparison CSVs (left outer join on datetime_utc)
Stage 5  → Compute per-station skill metrics (RMSE, bias, Pearson r)
Stage 6  → Generate global station-map figures coloured by metric
```

### Grid matching (Stage 3)

The nearest model grid point to each tide gauge is found by Haversine distance,
constrained to **wet grid points** only (land-masked cells are excluded). The search
expands in concentric shells until a wet cell is found. Station–grid distances
(in km) are stored in `station_model_index.csv` and in the metrics CSVs.

### Observation treatments (Stage 3.5)

#### A) Raw (no treatment)
Observations are used as-is (astronomical tidal signal present). This is suitable
only for descriptive comparison of total sea-level variability against POM tide.
**Not a surge validation metric.**

#### B) Godin filter (Godin 1972)
Three-pass centred running mean applied to the hourly GESLA series:

```
η_godin = MA₂₅(MA₂₄(MA₂₄(η_raw)))
```

where MA_N denotes a centred moving average of N hours. This filter passes
energy at periods > ~30 h and rejects the M₂/S₂ tidal band and higher
harmonics. The leading/trailing 36 h of each record are set to NaN (filter
edge effects).

**Requirement:** median timestep ≤ 90 min (hourly or coarser data).
Sub-hourly stations (5-min, 10-min, 15-min) are skipped (N=48 excluded).

#### C) FES2022 subtraction
Astronomical tidal elevation predicted by FES2022 harmonic analysis at each
station location:

```
η_detided = η_raw − η_FES2022(lon, lat, t)
```

Prediction uses all major tidal constituents available in FES2022.
Valid for any temporal resolution; station coverage limited to FES2022 model domain.

### Comparison pairs (Stage 4)

| Mode | Obs | Model | Context |
|------|-----|-------|---------|
| `raw_tide` | η_raw (with tide) | η_notide AND η_tide | Descriptive |
| `godin_notide` | η_godin (tide removed) | η_notide | Surge validation |
| `fes2022_notide` | η_raw − η_FES2022 (tide removed) | η_notide | Surge validation |

Comparison CSVs are joined on exact `datetime_utc` (hourly precision, no fuzzy
tolerance). Model data is extracted only for 2013–2018; earlier GESLA records
have NaN in model columns.

### Output structure

```
results/validation/
├── README.md
├── raw_tide/station_metrics.csv       ← descriptive metrics (515 stations)
├── godin_notide/station_metrics.csv   ← surge validation, Godin (467 stations)
└── fes2022_notide/station_metrics.csv ← surge validation, FES2022 (233 stations)

figures/validation/
├── raw_tide/       ← station maps coloured by tidal metrics
├── godin_notide/   ← station maps coloured by surge skill (Godin)
└── fes2022_notide/ ← station maps coloured by surge skill (FES2022)
```

---

## Assumptions

1. **ERA5 forcing is adequate** for reproducing synoptic-scale surge events globally.
   ERA5 has known wind speed underestimation in tropical cyclones; this may affect
   skill scores at exposed tropical stations.

2. **Nearest wet grid point is representative** of the tide-gauge location. For
   stations in estuaries, inlets, or semi-enclosed bays, the nearest open-ocean
   grid point may not capture local dynamics. Station–grid distances up to ~50 km
   are common in complex coastal geometries.

3. **FES2022 adequately predicts the astronomical tide** at each GESLA station.
   In regions with non-linear tide–surge interaction (e.g., shallow tidal flats,
   the North Sea), FES2022 residuals may not be purely meteorological.

4. **The Godin filter is appropriate for hourly data.** The filter was designed for
   hourly tide-gauge records. Sub-hourly data would alias tidal energy into the
   filter output; such stations are correctly excluded.

5. **Model period 2013–2018 is representative.** Surge climatology may differ from
   longer baselines. Short record lengths at some stations may produce unreliable
   skill estimates.

6. **η_tide − η_notide ≈ η_astro** (Eq. 1). Non-linear tide–surge interactions
   are absorbed into the residual ε. In highly non-linear regimes (estuaries, wide
   continental shelves), this approximation may break down.

---

## Results and Interpretation

### 2026-03-25 — Initial validation complete, 3 modes

**Station counts after full pipeline run:**

| Mode | Stations with metrics |
|------|-----------------------|
| raw_tide | 515 |
| godin_notide | 467 |
| fes2022_notide | 233 |

**[PRELIMINARY] Key patterns (from site exploration):**

- Stations with strong tidal forcing (e.g., Abashiri, Japan) show extremely high
  `rmse_notide` in raw_tide mode (e.g., 1.521 m) — confirming this metric is dominated
  by the tidal signal and is not informative for surge validation.

- In godin_notide mode, `pearson_r_notide` is substantially higher than in raw_tide
  mode for tidally dominated stations, confirming that Godin filtering exposes
  the surge–model correlation that was masked by the tidal signal.

- FES2022 mode covers only 233/515 stations due to the regional extent of the
  clipped FES2022 NetCDF files. Expanding coverage would require downloading
  global FES2022 tidal constants.

**[UNCERTAIN]** Spatial patterns of skill scores (regional RMSE gradients, bias
sign patterns) have not yet been systematically interpreted. This requires visual
inspection of the station-map figures.

---

## Caveats and Limitations

1. **Sub-hourly stations excluded from Godin mode:** ~48 stations (9.3% of total)
   have sub-hourly sampling and cannot be processed by the Godin filter. Their
   surge skill is only assessed in FES2022 mode.

2. **FES2022 regional coverage:** Only 233 stations fall within the clipped
   FES2022 domain (`data/tide_models_clipped_brasil/fes2022b/`). This severely
   limits the global coverage of the FES2022-detided validation.

3. **Short record overlap:** Some GESLA-4 stations have data only for part of
   2013–2018. Skill scores based on < 100 valid samples should be treated with caution.

4. **No seasonal or event-based stratification:** Metrics aggregate the full
   2013–2018 period. Seasonal biases (e.g., summer vs. winter storm-surge
   amplitude) are not yet resolved.

5. **No quantification of tide–surge non-linearity:** The decomposition η_tide =
   η_surge + η_astro + ε assumes small ε. In shallow coastal regions this may
   not hold.

6. **No wave setup or river discharge:** POM is an ocean circulation model; wave
   radiation stress and fluvial discharge are not included. This may affect skill
   at river-influenced and wave-exposed stations.

---

## Next Steps

- [ ] Expand FES2022 coverage to global domain (download full FES2022b constants)
- [ ] Analyse spatial patterns of godin_notide RMSE and bias (regional clusters)
- [ ] Seasonal stratification of skill scores (DJF/MAM/JJA/SON)
- [ ] Case-study validation for specific extreme surge events (e.g., 2016–2018 cyclones)
- [ ] Comparison of POM skill with other SurgeMIP models (when inter-comparison data available)
- [ ] Sensitivity test: impact of grid resolution on nearest-point distance and skill
- [ ] Document and quantify the 48 sub-hourly stations excluded from godin_notide mode

---

## References

- Godin, G. (1972). *The Analysis of Tides.* University of Toronto Press.
- Haigh, I. D., et al. (2022). The Global Extreme Sea Level Analysis v3 (GESLA-3).
  *Scientific Data*, 9, 566. https://doi.org/10.1038/s41597-022-01359-6
- Lyard, F., et al. (2021). FES2014 global ocean tidal model. *Ocean Science*, 17,
  615–649. https://doi.org/10.5194/os-17-615-2021
  *(FES2022 is the successor; documentation forthcoming from AVISO+)*
- Hersbach, H., et al. (2020). The ERA5 global reanalysis. *Quarterly Journal of the
  Royal Meteorological Society*, 146, 1999–2049. https://doi.org/10.1002/qj.3803
- Woodworth, P. L., et al. (2023). GESLA-4: a new global dataset of sea-level records.
  *Geoscience Data Journal* (in preparation — check GESLA website for updates).
