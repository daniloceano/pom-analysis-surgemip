"use client";

import type { ValidationMode, MetricKey } from "@/types/data";
import { METRIC_DEFS } from "@/types/data";

interface Props {
  metric: MetricKey;
  mode: ValidationMode;
  onMetricChange: (m: MetricKey) => void;
  stationCount: number;
}

// Scientific context for each observation treatment
const MODE_CONTEXT: Record<ValidationMode, {
  label: string;
  comparison: string;
  note: string;
  noteClass: string;
}> = {
  raw: {
    label: "Raw (no treatment)",
    comparison: "obs (with tide)  vs  POM tide",
    note: "Descriptive only — observations include the astronomical tidal signal. Not suitable for surge validation.",
    noteClass: "bg-amber-50 border-amber-200 text-amber-800",
  },
  godin_filter: {
    label: "Godin filter (1972)",
    comparison: "obs (Godin-detided)  vs  POM no-tide",
    note: "Surge validation — 24 h + 24 h + 25 h low-pass filter removes tidal periods. Only valid for hourly or coarser data.",
    noteClass: "bg-emerald-50 border-emerald-200 text-emerald-800",
  },
  minus_fes_tide: {
    label: "FES2022 subtraction",
    comparison: "obs (FES2022-detided)  vs  POM no-tide",
    note: "Surge validation — FES2022 harmonic tide prediction subtracted from observations. Removes astronomical tidal signal.",
    noteClass: "bg-emerald-50 border-emerald-200 text-emerald-800",
  },
};

export default function ControlPanel({ metric, mode, onMetricChange, stationCount }: Props) {
  // Filter metrics to those scientifically valid for the current obs treatment
  const availableMetrics = (Object.entries(METRIC_DEFS) as [MetricKey, (typeof METRIC_DEFS)[MetricKey]][])
    .filter(([, def]) => def.modesAvailable.includes(mode));

  const ctx = MODE_CONTEXT[mode];

  return (
    <div className="p-4 flex flex-col gap-4">
      {/* Observation treatment */}
      <div>
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">
          Observation treatment
        </p>
        <p className="text-sm font-medium text-slate-800">{ctx.label}</p>
        <p className="text-xs text-slate-500 mt-0.5 font-mono">{ctx.comparison}</p>
      </div>

      {/* Scientific context note */}
      <div className={`rounded border px-3 py-2 text-xs leading-relaxed ${ctx.noteClass}`}>
        {ctx.note}
      </div>

      {/* Map metric selector */}
      <div>
        <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider block mb-1">
          Map metric layer
        </label>
        <select
          className="w-full rounded border border-slate-300 bg-white text-xs px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-400"
          value={metric}
          onChange={(e) => onMetricChange(e.target.value as MetricKey)}
        >
          {availableMetrics.map(([key, def]) => (
            <option key={key} value={key}>
              {def.label}{def.unit ? ` [${def.unit}]` : ""}
            </option>
          ))}
        </select>
      </div>

      <div className="text-xs text-slate-400">
        {stationCount} stations loaded
      </div>

      {/* Usage hints */}
      <div className="mt-1 border-t border-slate-100 pt-3">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
          How to use
        </p>
        <ul className="text-xs text-slate-500 space-y-1 leading-relaxed">
          <li>• Click a station on the map to inspect it</li>
          <li>• Switch obs. treatment with the header buttons</li>
          <li>• Change the map colouring above</li>
          <li>• Time series appears below the map</li>
          <li>• Hover button labels for full descriptions</li>
        </ul>
      </div>
    </div>
  );
}
