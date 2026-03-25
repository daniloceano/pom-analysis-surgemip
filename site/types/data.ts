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
  // Only present in raw mode
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
  modesAvailable: ValidationMode[];
}

export const METRIC_DEFS: Record<MetricKey, MetricInfo> = {
  rmse_notide:         { label: "RMSE (surge)",         unit: "m",   colorScale: "YlOrRd", symmetric: false, modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
  bias_notide:         { label: "Bias (surge)",         unit: "m",   colorScale: "RdBu",   symmetric: true,  modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
  pearson_r_notide:    { label: "Pearson r (surge)",    unit: "",    colorScale: "RdYlGn", symmetric: false, modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
  rmse_tide:           { label: "RMSE (tide)",          unit: "m",   colorScale: "YlOrRd", symmetric: false, modesAvailable: ["raw"] },
  bias_tide:           { label: "Bias (tide)",          unit: "m",   colorScale: "RdBu",   symmetric: true,  modesAvailable: ["raw"] },
  pearson_r_tide:      { label: "Pearson r (tide)",     unit: "",    colorScale: "RdYlGn", symmetric: false, modesAvailable: ["raw"] },
  obs_mean_m:          { label: "Obs mean",             unit: "m",   colorScale: "RdBu",   symmetric: true,  modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
  obs_max_m:           { label: "Obs max |η|",          unit: "m",   colorScale: "YlOrRd", symmetric: false, modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
  model_notide_mean_m: { label: "Model surge mean",     unit: "m",   colorScale: "RdBu",   symmetric: true,  modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
  model_notide_max_m:  { label: "Model surge max |η|",  unit: "m",   colorScale: "YlOrRd", symmetric: false, modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
  model_tide_mean_m:   { label: "Model tide mean",      unit: "m",   colorScale: "RdBu",   symmetric: true,  modesAvailable: ["raw"] },
  model_tide_max_m:    { label: "Model tide max |η|",   unit: "m",   colorScale: "YlOrRd", symmetric: false, modesAvailable: ["raw"] },
  n_valid:             { label: "Valid samples",        unit: "",    colorScale: "Blues",   symmetric: false, modesAvailable: ["raw", "godin_filter", "minus_fes_tide"] },
};
