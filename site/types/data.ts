// ──────────────────────────────────────────────────────────────────────────────
// data.ts — Types and metric registry for the POM/GESLA-4 Validation Explorer
//
// Four validation modes (observation treatment × model target):
//
//   "godin_tide"    — obs detided with Godin filter; compared with POM_tide
//                     detided with Godin filter.
//                     POM_tide_godin ≈ POM_notide (tidal component is removed).
//                     Surge validation via consistent filter on both.
//
//   "fes2022_tide"  — obs detided via FES2022 subtraction; compared with
//                     POM_tide with FES2022 subtracted.
//                     POM_tide_fes2022 ≈ POM_notide (FES2022 was POM's forcing).
//                     Surge validation via consistent subtraction on both.
//
//   "godin_notide"  — obs detided with Godin filter; compared directly with
//                     POM_notide (no-tide run = meteorological sea level only).
//                     Surge validation — obs detided, model needs no treatment.
//
//   "fes2022_notide"— obs detided via FES2022 subtraction; compared directly
//                     with POM_notide.
//                     Surge validation — obs detided, model needs no treatment.
//
// Time-series visualisation:
//   Regardless of mode, the chart ALWAYS shows raw GESLA obs + POM_tide.
//   Both signals include the tidal component, making them visually comparable.
//   Mode selection only affects the metric maps and StationCard metrics.
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
  // Present only in raw_tide descriptive mode
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
    godin_tide?:    RawMetrics;
    fes2022_tide?:  RawMetrics;
    godin_notide?:  RawMetrics;
    fes2022_notide?:RawMetrics;
    // raw_tide kept for backwards compatibility (descriptive mode)
    raw_tide?:      RawMetrics;
  };
}

export interface StationData {
  stations: Station[];
  modes_available: string[];
}

export interface TimeSeriesData {
  station_id: string;
  dates: string[];
  obs:    (number | null)[];   // raw GESLA observation (with tide)
  tide?:  (number | null)[];   // POM_tide daily mean
  notide?:(number | null)[];   // POM_notide daily mean (surge reference)
}

export type ValidationMode =
  | "godin_tide"
  | "fes2022_tide"
  | "godin_notide"
  | "fes2022_notide";

export type MetricKey =
  | "rmse_notide" | "bias_notide" | "pearson_r_notide"
  | "obs_mean_m"  | "obs_max_m"
  | "model_notide_mean_m" | "model_notide_max_m"
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
  // ── Surge validation metrics (detided obs vs detided/no-tide model) ────────
  rmse_notide:         { label: "RMSE — surge validation",         unit: "m", colorScale: "YlOrRd", symmetric: false, modesAvailable: ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"] },
  bias_notide:         { label: "Bias — surge validation",         unit: "m", colorScale: "RdBu",   symmetric: true,  modesAvailable: ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"] },
  pearson_r_notide:    { label: "Pearson r — surge validation",    unit: "",  colorScale: "RdYlGn", symmetric: false, modesAvailable: ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"] },

  // ── Observation statistics ─────────────────────────────────────────────────
  obs_mean_m:          { label: "Obs mean (detided)",              unit: "m", colorScale: "RdBu",   symmetric: true,  modesAvailable: ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"] },
  obs_max_m:           { label: "Obs max |η| (detided)",           unit: "m", colorScale: "YlOrRd", symmetric: false, modesAvailable: ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"] },

  // ── Model surge statistics ─────────────────────────────────────────────────
  model_notide_mean_m: { label: "Model surge mean",                unit: "m", colorScale: "RdBu",   symmetric: true,  modesAvailable: ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"] },
  model_notide_max_m:  { label: "Model surge max |η|",             unit: "m", colorScale: "YlOrRd", symmetric: false, modesAvailable: ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"] },

  // ── Data coverage ─────────────────────────────────────────────────────────
  n_valid:             { label: "Valid samples",                   unit: "",  colorScale: "Blues",   symmetric: false, modesAvailable: ["godin_tide", "fes2022_tide", "godin_notide", "fes2022_notide"] },
};

// Human-readable labels for each validation mode
export const MODE_LABELS: Record<ValidationMode, { short: string; full: string; badge: string; badgeClass: string }> = {
  godin_tide:    {
    short: "Godin / tide",
    full:  "Godin filter applied to obs and POM_tide",
    badge: "obs Godin · model tide Godin",
    badgeClass: "bg-indigo-100 text-indigo-800",
  },
  fes2022_tide:  {
    short: "FES / tide",
    full:  "FES2022 subtracted from obs and POM_tide",
    badge: "obs FES2022 · model tide FES2022",
    badgeClass: "bg-violet-100 text-violet-800",
  },
  godin_notide:  {
    short: "Godin / no-tide",
    full:  "Godin filter on obs; compared with POM_notide",
    badge: "obs Godin · model no-tide",
    badgeClass: "bg-emerald-100 text-emerald-800",
  },
  fes2022_notide:{
    short: "FES / no-tide",
    full:  "FES2022 subtracted from obs; compared with POM_notide",
    badge: "obs FES2022 · model no-tide",
    badgeClass: "bg-teal-100 text-teal-800",
  },
};

// Default metric for each mode (all are surge validation → rmse_notide)
export const DEFAULT_METRIC: Record<ValidationMode, MetricKey> = {
  godin_tide:    "rmse_notide",
  fes2022_tide:  "rmse_notide",
  godin_notide:  "rmse_notide",
  fes2022_notide:"rmse_notide",
};
