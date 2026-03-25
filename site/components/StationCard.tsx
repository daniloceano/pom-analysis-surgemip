"use client";

import type { Station, ValidationMode, RawMetrics } from "@/types/data";

interface Props {
  station: Station | null;
  mode: ValidationMode;
}

function MetricRow({ label, value, unit = "" }: { label: string; value: number | null | undefined; unit?: string }) {
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
        {formatted}{unit && value != null ? <span className="text-slate-400 ml-0.5 font-sans">{unit}</span> : ""}
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
  const modeLabel: Record<ValidationMode, string> = {
    raw: "Raw",
    godin_filter: "Godin",
    minus_fes_tide: "FES2022",
  };

  return (
    <div className="p-4">
      <div className="mb-3">
        <p className="font-semibold text-slate-900 text-sm leading-tight">{station.name}</p>
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

      {!m ? (
        <p className="text-xs text-slate-400 italic">
          No metrics for mode &quot;{modeLabel[mode]}&quot;.
        </p>
      ) : (
        <>
          <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
            {modeLabel[mode]} metrics
          </p>
          <table className="w-full">
            <tbody>
              <MetricRow label="Valid samples"    value={m.n_valid} />
              <MetricRow label="RMSE (surge)"     value={m.rmse_notide}       unit=" m" />
              <MetricRow label="Bias (surge)"     value={m.bias_notide}       unit=" m" />
              <MetricRow label="Pearson r"        value={m.pearson_r_notide} />
              {mode === "raw" && (
                <>
                  <MetricRow label="RMSE (tide)"  value={m.rmse_tide}         unit=" m" />
                  <MetricRow label="Bias (tide)"  value={m.bias_tide}         unit=" m" />
                  <MetricRow label="r (tide)"     value={m.pearson_r_tide} />
                </>
              )}
              <MetricRow label="Obs mean"         value={m.obs_mean_m}        unit=" m" />
              <MetricRow label="Obs max |η|"      value={m.obs_max_m}         unit=" m" />
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
