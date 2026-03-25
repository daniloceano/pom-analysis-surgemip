// ──────────────────────────────────────────────────────────────────────────────
// data.ts — Types and metric registry for the POM/GESLA-4 Validation Explorer
//
// Observation treatment (ValidationMode) describes what was done to the GESLA
// tide-gauge series before comparison with the model:
//
//   "raw"             — no treatment; obs includes astronomical tidal signal.
//                       Used for DESCRIPTIVE visualisation only.
//                       Valid comparison: obs_raw  vs  POM tide (model_eta_tide)
//                       ⚠ Comparing obs_raw vs POM no-tide is NOT a surge metric.
//
//   "godin_filter"    — tidal signal removed using the Godin (1972) low-pass
//                       filter (24h + 24h + 25h running means).
//                       Valid comparison: obs_detided  vs  POM no-tide  ✓ SURGE
//
//   "minus_fes_tide"  — astronomical tide predicted by FES2022 harmonic model
//                       subtracted from observations.
//                       Valid comparison: obs_detided  vs  POM no-tide  ✓ SURGE
//
// Note on "POM tide detided":
//   model_tide_detided = model_eta_tide − model_tide_minus_notide = model_eta_notide
//   Therefore comparing obs_detided vs model_tide_detided is mathematically
//   identical to comparing obs_detided vs model_notide, which is already what
//   godin_filter and minus_fes_tide modes implement.
// ──────────────────────────────────────────────────────────────────────────────

export interface RawMetrics {
  n_valid: number | null;
  obs_mean_m: number | null;
  obs_std_m: number | null;
  obs_max_m: number | null;
  model_notide_mean_m: number | null;
  model_notide_std_m: number | null;
  model_notide_max_m: number | null;
  rmse_notide: number | null;
  bias_notide: number | null;
  pearson_r_notide: number | null;
  // Only present in raw mode (obs includes tidal signal)
  model_tide_mean_m?: number | null;
  model_tide_std_m?: number | null;
  model_tide_max_m?: number | null;
  rmse_tide?: number | null;
  bias_tide?: number | null;
  pearson_r_tide?: number | null;
}

export interface Station {
  id: string;
  name: string;
  site_code: string;
  country: string;
  lon: number;
  lat: number;
  model_lon: number | null;
  model_lat: number | null;
  distance_km: number | null;
  metrics: {
    raw?: RawMetrics;
    godin_filter?: RawMetrics;
    minus_fes_tide?: RawMetrics;
  };
}

export interface StationData {
  stations: Station[];
  modes_available: string[];
}

export interface TimeSeriesData {
  station_id: string;
  mode: string;
  dates: string[];
  obs: (number | null)[];
  notide?: (number | null)[];
  tide?: (number | null)[];
}

export type ValidationMode = "raw" | "godin_filter" | "minus_fes_tide";

export type MetricKey =
  | "rmse_notide" | "bias_notide" | "pearson_r_notide"
  | "rmse_tide" | "bias_tide" | "pearson_r_tide"
  | "obs_mean_m" | "obs_max_m"
  | "model_notide_mean_m" | "model_notide_max_m"
  | "model_tide_mean_m" | "model_tide_max_m"
  | "n_valid";

export interface MetricInfo {
  label: string;
  unit: string;
  colorScale: "YlOrRd" | "RdBu" | "RdYlGn" | "Blues";
  symmetric: boolean;
  /** Modes in which this metric is scientifically meaningful to display. */
  modesAvailable: ValidationMode[];
}

export const METRIC_DEFS: Record<MetricKey, MetricInfo> = {
  // ── Surge validation metrics ──────────────────────────────────────────────
  // Only valid when obs has been detided (godin_filter or minus_fes_tide).
  // In raw mode, rmse_notide = RMSE(obs_with_tide, model_notide) is dominated
  // by the tidal signal and DOES NOT represent surge skill.
  rmse_notide:         { label: "RMSE — obs detided vs POM no-tide",  unit: "m", colorScale: "YlOrRd", symmetric: false, modesAvailable: ["godin_filter", "minus_fes_tide"] },
  bias_notide:         { label: "Bias — obs detided vs POM no-tide",  unit: "m", colorScale: "RdBu",   symmetric: true,  modesAvailable: ["godin_filter", "minus_fes_tide"] },
  pearson_r_notide:    { label: "Pearson r — surge validation",       unit: "",  colorScale: "RdYlGn", symmetric: false, modesAvailable: ["godin_filter", "minus_fes_tide"] },

  // ── Descriptive metrics (raw obs vs POM tide) ─────────────────────────────
  // Both obs and model include the tidal signal → useful for tidal
  // characterisation, NOT for surge validation.
  rmse_tide:           { label: "RMSE — obs raw vs POM tide",         unit: "m", colorScale: "YlOrRd", symmetric: false, modesAvailable: ["raw"] },
  bias_tide:           { label: "Bias — obs raw vs POM tide",         unit: "m", colorScale: "RdBu",   symmetric: true,  modesAvailable: ["raw"] },
  pearson_r_tide:      { label: "Pearson r — tidal signal",           unit: "",  colorScale: "RdYlGn", symmetric: false, modesAvailable: ["raw"] },

  // ── Observation statistics (available in all modes) ───────────────────────
  // Note: in raw mode obs_mean/max include tidal signal; in detided modes
  // they reflect the residual (surge) signal statistics.
  obs_mean_m:          { label: "Obs mean",                           unit: "m", colorScale: "RdBu",   symmetric: true,  modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
  obs_max_m:           { label: "Obs max |η|",                        unit: "m", colorScale: "YlOrRd", symmetric: false, modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },

  // ── Model statistics — no-tide / surge ────────────────────────────────────
  // Shown only with detided obs modes to maintain conceptual consistency.
  model_notide_mean_m: { label: "POM no-tide mean (surge)",           unit: "m", colorScale: "RdBu",   symmetric: true,  modesAvailable: ["godin_filter", "minus_fes_tide"] },
  model_notide_max_m:  { label: "POM no-tide max |η| (surge)",        unit: "m", colorScale: "YlOrRd", symmetric: false, modesAvailable: ["godin_filter", "minus_fes_tide"] },

  // ── Model statistics — tide ───────────────────────────────────────────────
  model_tide_mean_m:   { label: "POM tide mean",                      unit: "m", colorScale: "RdBu",   symmetric: true,  modesAvailable: ["raw"] },
  model_tide_max_m:    { label: "POM tide max |η|",                   unit: "m", colorScale: "YlOrRd", symmetric: false, modesAvailable: ["raw"] },

  // ── Data coverage ─────────────────────────────────────────────────────────
  n_valid:             { label: "Valid samples",                      unit: "",  colorScale: "Blues",   symmetric: false, modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
};

// Default metric for each observation treatment
export const DEFAULT_METRIC: Record<ValidationMode, MetricKey> = {
  raw:            "rmse_tide",       // descriptive: tidal RMSE
  godin_filter:   "rmse_notide",     // validation: surge RMSE
  minus_fes_tide: "rmse_notide",     // validation: surge RMSE
};
