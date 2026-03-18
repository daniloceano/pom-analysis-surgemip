# Design decisions and assumptions

This document records non-obvious implementation choices so that future
maintainers understand *why* things were done a certain way.

---

## 1. Timezone handling (GESLA observations)

**Decision:** `UTC = local_time − timedelta(hours=TIME_ZONE_HOURS)`

**Rationale:**
The `TIME_ZONE_HOURS` column in `SurgeMIP_stnlist.csv` encodes the offset
*from* UTC (e.g. `TIME_ZONE_HOURS = −3` means the station records in UTC−3,
i.e. local clock is 3 hours behind UTC → add 3 hours to get UTC).

For the majority of SurgeMIP stations `TIME_ZONE_HOURS = 0` (already UTC),
so no shift is applied.

When `TIME_ZONE_HOURS` is missing or NaN, UTC is assumed and a warning is
logged.  This is a **conservative** choice that avoids silent errors.

The boolean column `tz_assumed_utc` in the observation CSV records whether
no correction was applied (`True`) or a shift was performed (`False`).

---

## 2. Nearest grid-point matching

**Decision:** Use `GrADSReader.nearest_wet_ij()` (expanding-shell search for
nearest ocean cell) as the default grid-point finder.

**Rationale:**
A coastal tide gauge at the shoreline often maps to a land-masked model cell
under strict nearest-point interpolation.  The wet-point search avoids this
by expanding outward (up to `max_radius=10` grid cells) until an ocean cell
is found.

**Future work:** Bilinear interpolation would be more accurate for gauges
between grid cells.  A stub comment is left in
`extract_model_for_gesla_stations.py` for future implementation.

---

## 3. Temporal merge strategy

**Decision:** Left outer join on exact UTC timestamps (no fuzzy matching).

**Rationale:**
Both GESLA observations and model output are nominally hourly.  An exact-match
join is the safest default.  Observations without a model counterpart (e.g.
outside the 2013–2018 model period) will have NaN model columns.  Model steps
without an observation are dropped from the final CSV.

If GESLA data are at a different frequency (e.g. 15-min), use
`build_comparison_csvs.py --resample 1h` to align before merging.  The
resampling uses `mean()` aggregation over the chosen period.

---

## 4. GESLA null / missing value handling

**Decision:** Sentinel values (typically `−99.9999`) in the sea-level column
are replaced with `NaN`.  The threshold is `|value − null| < |null| × 0.01`.

**Rationale:**
The null value per station is read from (in order of precedence):
1. The explicit `--null-value` argument.
2. The `# Null value` line in the GESLA file header.
3. The `NULL VALUE` column in `SurgeMIP_stnlist.csv`.
4. The global default `GESLA_NULL_VALUE_DEFAULT = −99.9999`.

Flags (`gesla_qc_flag`, `gesla_use_flag`) are preserved as-is; filtering on
flags is left to downstream analysis scripts.

---

## 5. GESLA file discovery in ZIP

**Decision:** Case-insensitive stem matching (`file_name` column from the
station list, without extension).

**Rationale:**
GESLA-4 ZIP archives may include files at varying subdirectory depths, with
mixed case.  Matching on the lowercased stem (filename without extension or
directory prefix) is robust to these variations.

---

## 6. Idempotency of the pipeline

All scripts check for existing outputs and skip them by default (`--force`
overrides).  This allows partial re-runs without reprocessing everything.
The station-model index table is merged (not replaced) when new stations are
added incrementally.

---

## 7. Memory efficiency

The POM GrADS binary files are ~138 GB each.  `GrADSReader` uses
`numpy.memmap` so only the requested bytes are loaded.  The
`extract_point` method reads one scalar per time step (no full 2-D slice
loaded into RAM).

For the batch extraction of ~550 stations the model files are opened once and
reused across all stations — both `tide_reader` and `notide_reader` are
opened at the top of `extract_model_for_gesla_stations.py`.

---

## 8. Output format: `.csv.gz`

Compressed CSV was chosen over NetCDF/Parquet for simplicity and wide
compatibility.  All intermediate and final outputs use `.csv.gz` (gzip
compression via pandas `to_csv(compression='gzip')`).

---

## 9. What the `tidal_signal` / `model_tide_minus_notide_m` represents

`model_tide_minus_notide_m = eta_tide − eta_notide`

This approximates the **astronomical tidal signal** as simulated by the model.
The `eta_notide` field is the **meteorological sea level** (storm surge +
mean sea level).  See `SURGMIP_META["surge_definition"]` in `config/settings.py`.

---

## 10. Pipeline parallelisation strategy (Stage 2–4)

`ThreadPoolExecutor` is used (not `ProcessPoolExecutor`) for the following
reasons:

* **Stage 2 (prepare observations):** Each station reads one small text file
  from disk.  Purely I/O-bound; no shared mutable state.  Threads are
  sufficient and avoid fork/spawn overhead.

* **Stage 3 (model extraction):** Two `GrADSReader` instances backed by
  `numpy.memmap` (read-only memory-mapped files) are opened once in the main
  thread and shared across all worker threads.  `numpy.memmap` is safe for
  concurrent reads — the OS memory-mapping layer serialises page faults
  internally.  Forking 50 processes each opening the ~138 GB files would add
  significant start-up latency and OS overhead.

* **Stage 4 (merge obs + model):** Reads and writes independent per-station
  `.csv.gz` files.  Purely I/O-bound.

The default worker count is **50**, overridable via `--workers`.

---

## 11. Validation metrics

Per-station skill scores are computed on the intersection of good-quality
GESLA observations (`gesla_qc_flag == 1` AND `gesla_use_flag == 1`) and
non-NaN model values.  Stations with fewer than 10 valid paired samples
receive NaN metrics but are still included in the output table.

Two model targets are assessed independently:
  * `model_eta_notide_m` — meteorological sea level (storm surge signal)
  * `model_eta_tide_m`   — full sea level including astronomical tides

Metrics: RMSE, mean bias (model − obs), Pearson r, observed/model mean,
standard deviation, and max absolute value.
