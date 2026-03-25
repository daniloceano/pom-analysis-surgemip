"use client";

import type { ValidationMode, MetricKey } from "@/types/data";
import { METRIC_DEFS } from "@/types/data";

interface Props {
  metric: MetricKey;
  mode: ValidationMode;
  onMetricChange: (m: MetricKey) => void;
  stationCount: number;
}

export default function ControlPanel({ metric, mode, onMetricChange, stationCount }: Props) {
  // Filter metrics to those available for the current mode
  const availableMetrics = (Object.entries(METRIC_DEFS) as [MetricKey, (typeof METRIC_DEFS)[MetricKey]][])
    .filter(([, def]) => def.modesAvailable.includes(mode));

  const modeLabel: Record<ValidationMode, string> = {
    raw: "Raw (tidal)",
    godin_filter: "Godin filter (de-tided)",
    minus_fes_tide: "FES2022 (de-tided)",
  };

  return (
    <div className="p-4 flex flex-col gap-4">
      <div>
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-1">
          Active mode
        </p>
        <p className="text-sm font-medium text-slate-800">{modeLabel[mode]}</p>
      </div>

      <div>
        <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider block mb-1">
          Map metric
        </label>
        <select
          className="w-full rounded border border-slate-300 bg-white text-sm px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-400"
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

      <div className="mt-2 border-t border-slate-100 pt-3">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
          How to use
        </p>
        <ul className="text-xs text-slate-500 space-y-1 leading-relaxed">
          <li>• Click a station on the map to inspect it</li>
          <li>• Switch mode with the header buttons</li>
          <li>• Change the map colouring above</li>
          <li>• Time series shows below the map</li>
        </ul>
      </div>
    </div>
  );
}
