"use client";

import type { ValidationMode, MetricKey } from "@/types/data";
import { METRIC_DEFS, MODE_LABELS } from "@/types/data";

interface Props {
  metric: MetricKey;
  mode: ValidationMode;
  onMetricChange: (m: MetricKey) => void;
  stationCount: number;
}

// Scientific note for each validation mode
const MODE_NOTES: Record<ValidationMode, { comparison: string; note: string; noteClass: string }> = {
  godin_tide: {
    comparison: "obs (Godin)  vs  POM_tide (Godin)",
    note: "Both obs and POM_tide are low-pass filtered with the Godin (1972) filter (24 h + 24 h + 25 h). POM_tide_godin ≈ POM_notide since the tidal component is removed.",
    noteClass: "bg-indigo-50 border-indigo-200 text-indigo-800",
  },
  fes2022_tide: {
    comparison: "obs (FES2022)  vs  POM_tide (FES2022)",
    note: "FES2022 harmonic tide subtracted from both obs and POM_tide. Since POM was forced by FES2022, POM_tide − FES2022 ≈ POM_notide.",
    noteClass: "bg-violet-50 border-violet-200 text-violet-800",
  },
  godin_notide: {
    comparison: "obs (Godin)  vs  POM_notide",
    note: "Godin low-pass filter applied to obs only. POM_notide is the meteorological-only run (no tidal forcing) — no treatment needed.",
    noteClass: "bg-emerald-50 border-emerald-200 text-emerald-800",
  },
  fes2022_notide: {
    comparison: "obs (FES2022)  vs  POM_notide",
    note: "FES2022 tidal prediction subtracted from obs only. POM_notide is the meteorological-only run — no treatment needed.",
    noteClass: "bg-teal-50 border-teal-200 text-teal-800",
  },
};

export default function ControlPanel({ metric, mode, onMetricChange, stationCount }: Props) {
  const availableMetrics = (
    Object.entries(METRIC_DEFS) as [MetricKey, (typeof METRIC_DEFS)[MetricKey]][]
  ).filter(([, def]) => def.modesAvailable.includes(mode));

  const modeInfo = MODE_LABELS[mode];
  const ctx = MODE_NOTES[mode];

  return (
    <div className="p-4 flex flex-col gap-4">
      {/* Validation mode summary */}
      <div>
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">
          Validation mode
        </p>
        <p className="text-sm font-medium text-slate-800">{modeInfo.short}</p>
        <p className="text-xs text-slate-500 mt-0.5 font-mono">{ctx.comparison}</p>
      </div>

      {/* Scientific note */}
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

      <div className="text-xs text-slate-400">{stationCount} stations loaded</div>

      {/* Usage hints */}
      <div className="mt-1 border-t border-slate-100 pt-3">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
          How to use
        </p>
        <ul className="text-xs text-slate-500 space-y-1 leading-relaxed">
          <li>• Click a station on the map to inspect it</li>
          <li>• Switch validation mode with header buttons</li>
          <li>• Change the map colouring above</li>
          <li>• Time series (raw obs + POM tide) below the map</li>
          <li>• Surge metrics in the station card →</li>
        </ul>
      </div>
    </div>
  );
}
