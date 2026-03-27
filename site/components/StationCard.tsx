"use client";

import type { Station, ValidationMode, RawMetrics } from "@/types/data";
import { MODE_LABELS } from "@/types/data";

interface Props {
  station: Station | null;
  mode: ValidationMode;
}

function MetricRow({ label, value, unit = "" }: {
  label: string;
  value: number | null | undefined;
  unit?: string;
}) {
  const formatted =
    value == null
      ? "—"
      : Math.abs(value) < 0.001
        ? value.toExponential(2)
        : value.toFixed(4);
  return (
    <tr className="border-b border-slate-100 last:border-0">
      <td className="py-1 pr-3 text-xs text-slate-500 whitespace-nowrap">{label}</td>
      <td className="py-1 text-xs font-mono font-medium text-slate-800 text-right">
        {formatted}
        {unit && value != null ? (
          <span className="text-slate-400 ml-0.5 font-sans">{unit}</span>
        ) : ""}
      </td>
    </tr>
  );
}

export default function StationCard({ station, mode }: Props) {
  if (!station) {
    return (
      <div className="p-4 text-xs text-slate-400 italic">
        Click a station on the map to see its metrics.
      </div>
    );
  }

  const m: RawMetrics | undefined = station.metrics[mode];
  const modeInfo = MODE_LABELS[mode];

  // Determine model target description
  const modelTarget = mode.endsWith("_tide")
    ? "POM_tide (detided)"
    : "POM_notide (surge)";

  const obsMethod = mode.startsWith("godin")
    ? "Godin filter (1972)"
    : "FES2022 subtraction";

  return (
    <div className="p-4">
      {/* Station identity */}
      <div className="mb-3">
        <p className="font-semibold text-slate-900 text-sm leading-tight">
          {station.name}
        </p>
        <p className="text-xs text-slate-500 mt-0.5">
          {station.site_code} · {station.country}
        </p>
        <p className="text-xs text-slate-400 mt-0.5 font-mono">
          {station.lon.toFixed(3)}°, {station.lat.toFixed(3)}°
        </p>
        {station.distance_km != null && (
          <p className="text-xs text-slate-400 mt-0.5">
            Model grid distance: {station.distance_km.toFixed(1)} km
          </p>
        )}
      </div>

      {/* Mode context */}
      <div className="mb-3 bg-slate-50 rounded px-2 py-1.5 text-xs space-y-0.5">
        <p className="text-slate-600">
          <span className="font-medium">Obs treatment:</span>{" "}
          <span className="text-slate-700">{obsMethod}</span>
        </p>
        <p className="text-slate-600">
          <span className="font-medium">Model target:</span>{" "}
          <span className="text-slate-700">{modelTarget}</span>
        </p>
      </div>

      {!m ? (
        <p className="text-xs text-slate-400 italic">
          No metrics available for this mode.
        </p>
      ) : (
        <>
          <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
            Surge validation metrics
          </p>
          <table className="w-full">
            <tbody>
              <MetricRow label="Valid samples"       value={m.n_valid} />
              <MetricRow label="RMSE (surge)"        value={m.rmse_notide}      unit=" m" />
              <MetricRow label="Bias (surge)"        value={m.bias_notide}      unit=" m" />
              <MetricRow label="Pearson r (surge)"   value={m.pearson_r_notide} />
              <MetricRow label="Obs mean (detided)"  value={m.obs_mean_m}       unit=" m" />
              <MetricRow label="Obs max |η|"         value={m.obs_max_m}        unit=" m" />
              <MetricRow label="Model surge mean"    value={m.model_notide_mean_m} unit=" m" />
              <MetricRow label="Model surge max |η|" value={m.model_notide_max_m}  unit=" m" />
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
