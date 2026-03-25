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

// Section header + context badge for each observation treatment
const MODE_SECTION: Record<ValidationMode, { header: string; badge: string; badgeClass: string }> = {
  raw: {
    header: "Descriptive metrics",
    badge: "obs with tide",
    badgeClass: "bg-amber-100 text-amber-700",
  },
  godin_filter: {
    header: "Surge validation metrics",
    badge: "Godin-detided",
    badgeClass: "bg-emerald-100 text-emerald-700",
  },
  minus_fes_tide: {
    header: "Surge validation metrics",
    badge: "FES2022-detided",
    badgeClass: "bg-emerald-100 text-emerald-700",
  },
};

export default function StationCard({ station, mode }: Props) {
  if (!station) {
    return (
      <div className="p-4 text-xs text-slate-400 italic">
        Click a station on the map to see its metrics.
      </div>
    );
  }

  const m: RawMetrics | undefined = station.metrics[mode];
  const section = MODE_SECTION[mode];

  return (
    <div className="p-4">
      {/* Station identity */}
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
          No metrics available for this treatment.
        </p>
      ) : (
        <>
          {/* Section header */}
          <div className="flex items-center gap-2 mb-2">
            <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
              {section.header}
            </p>
            <span className={`text-xs px-1.5 py-0.5 rounded-full font-medium ${section.badgeClass}`}>
              {section.badge}
            </span>
          </div>

          <table className="w-full">
            <tbody>
              <MetricRow label="Valid samples" value={m.n_valid} />

              {mode === "raw" ? (
                // Raw mode: show tidal comparison metrics (descriptive)
                <>
                  <MetricRow label="RMSE — raw vs POM tide"  value={m.rmse_tide}       unit=" m" />
                  <MetricRow label="Bias — raw vs POM tide"  value={m.bias_tide}       unit=" m" />
                  <MetricRow label="Pearson r (tidal)"       value={m.pearson_r_tide} />
                </>
              ) : (
                // Detided modes: show surge validation metrics
                <>
                  <MetricRow label="RMSE (surge)"            value={m.rmse_notide}      unit=" m" />
                  <MetricRow label="Bias (surge)"            value={m.bias_notide}      unit=" m" />
                  <MetricRow label="Pearson r (surge)"       value={m.pearson_r_notide} />
                </>
              )}

              {/* Observation statistics */}
              <MetricRow label="Obs mean" value={m.obs_mean_m} unit=" m" />
              <MetricRow label="Obs max |η|" value={m.obs_max_m} unit=" m" />
            </tbody>
          </table>

          {/* Note for raw mode to prevent misinterpretation */}
          {mode === "raw" && (
            <p className="mt-2 text-xs text-amber-700 bg-amber-50 rounded px-2 py-1.5 leading-relaxed">
              Obs. mean and max include the tidal signal. Switch to Godin or FES2022 for surge-only statistics.
            </p>
          )}
        </>
      )}
    </div>
  );
}
