"use client";

import { useEffect, useState, useCallback } from "react";
import dynamic from "next/dynamic";
import type { Station, StationData, ValidationMode, MetricKey } from "@/types/data";
import { METRIC_DEFS, DEFAULT_METRIC } from "@/types/data";
import StationCard from "@/components/StationCard";
import ControlPanel from "@/components/ControlPanel";

// Dynamic imports for client-only components (Leaflet + Plotly require browser APIs)
const StationMap = dynamic(() => import("@/components/StationMap"), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-full bg-slate-100">
      <div className="text-slate-500 text-sm">Loading map…</div>
    </div>
  ),
});

const TimeSeriesChart = dynamic(() => import("@/components/TimeSeriesChart"), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-full bg-white">
      <div className="text-slate-500 text-sm">Loading chart…</div>
    </div>
  ),
});

const DEFAULT_MODE: ValidationMode = "godin_notide";

// Labels for the observation treatment selector
const OBS_TREATMENT_LABELS: Record<ValidationMode, { short: string; full: string }> = {
  raw_tide:       { short: "Raw",     full: "No treatment — obs includes tidal signal" },
  godin_notide:   { short: "Godin",   full: "Godin filter (1972) — tidal signal removed" },
  fes2022_notide: { short: "FES2022", full: "FES2022 harmonics subtracted — tidal signal removed" },
};

// Visual tags distinguishing descriptive from validation contexts
const MODE_TAG: Record<ValidationMode, { text: string; className: string }> = {
  raw_tide:       { text: "Descriptive",     className: "bg-amber-100 text-amber-800" },
  godin_notide:   { text: "Surge validation", className: "bg-emerald-100 text-emerald-800" },
  fes2022_notide: { text: "Surge validation", className: "bg-emerald-100 text-emerald-800" },
};

export default function Home() {
  const [stationData, setStationData] = useState<StationData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [selectedStation, setSelectedStation] = useState<Station | null>(null);
  const [mode, setMode] = useState<ValidationMode>(DEFAULT_MODE);
  const [metric, setMetric] = useState<MetricKey>(DEFAULT_METRIC[DEFAULT_MODE]);

  // Load station metrics manifest on mount
  useEffect(() => {
    fetch("/data/station_metrics.json")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}: ${r.url}`);
        return r.json();
      })
      .then((data: StationData) => {
        setStationData(data);
        setLoading(false);
      })
      .catch((e) => {
        setError(String(e));
        setLoading(false);
      });
  }, []);

  // When obs treatment changes, reset to the appropriate default metric for that mode
  const handleModeChange = useCallback((newMode: ValidationMode) => {
    setMode(newMode);
    const def = METRIC_DEFS[metric];
    if (!def.modesAvailable.includes(newMode)) {
      setMetric(DEFAULT_METRIC[newMode]);
    }
  }, [metric]);

  const handleSelectStation = useCallback((station: Station) => {
    setSelectedStation(station);
  }, []);

  const modesAvailable = stationData?.modes_available ?? ["raw_tide"];

  const tag = MODE_TAG[mode];

  return (
    <div className="flex flex-col h-full">
      {/* ── Header ──────────────────────────────────────────────────────────── */}
      <header className="flex items-center justify-between px-5 py-3 bg-slate-900 text-white shrink-0 shadow-md">
        <div>
          <h1 className="text-base font-semibold tracking-tight leading-tight">
            POM / GESLA-4 Validation Explorer
          </h1>
          <p className="text-xs text-slate-400 mt-0.5">
            Princeton Ocean Model · ERA5 forced · 2013–2018 · SurgeMIP
          </p>
        </div>

        <div className="flex items-center gap-3">
          {/* Context tag */}
          <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${tag.className}`}>
            {tag.text}
          </span>

          {/* Observation treatment selector */}
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-400 whitespace-nowrap">Obs. treatment:</span>
            <div className="flex gap-1">
              {(["raw_tide", "godin_notide", "fes2022_notide"] as ValidationMode[]).map((m) => {
                const available = modesAvailable.includes(m);
                const lbl = OBS_TREATMENT_LABELS[m];
                return (
                  <button
                    key={m}
                    disabled={!available}
                    onClick={() => handleModeChange(m)}
                    title={lbl.full}
                    className={`px-3 py-1 rounded text-xs font-medium transition-colors
                      ${mode === m
                        ? "bg-indigo-500 text-white"
                        : available
                          ? "bg-slate-700 text-slate-300 hover:bg-slate-600"
                          : "bg-slate-800 text-slate-600 cursor-not-allowed"
                      }`}
                  >
                    {lbl.short}
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      </header>

      {/* ── Main content ────────────────────────────────────────────────────── */}
      {loading && (
        <div className="flex flex-1 items-center justify-center">
          <div className="text-slate-500">Loading station data…</div>
        </div>
      )}
      {error && (
        <div className="flex flex-1 items-center justify-center">
          <div className="text-red-600 text-sm max-w-md text-center p-4">
            <p className="font-semibold">Could not load station data</p>
            <p className="mt-1 text-slate-600 text-xs">{error}</p>
            <p className="mt-2 text-xs text-slate-500">
              Run <code className="bg-slate-100 px-1 rounded">python scripts/pipeline/prepare_site_data.py</code> to
              generate <code className="bg-slate-100 px-1 rounded">site/public/data/station_metrics.json</code>.
            </p>
          </div>
        </div>
      )}
      {!loading && !error && stationData && (
        <div className="flex flex-1 min-h-0 overflow-hidden">
          {/* ── Left: Map + Time series ──────────────────────────────────────── */}
          <div className="flex flex-col flex-1 min-w-0">
            <div className="flex-1 min-h-0 relative">
              <StationMap
                stations={stationData.stations}
                mode={mode}
                metric={metric}
                selectedStation={selectedStation}
                onSelectStation={handleSelectStation}
              />
            </div>
            <div className="h-56 border-t border-slate-200 bg-white shrink-0">
              <TimeSeriesChart
                station={selectedStation}
                mode={mode}
              />
            </div>
          </div>

          {/* ── Right: Controls + Station card ──────────────────────────────── */}
          <div className="w-80 shrink-0 flex flex-col border-l border-slate-200 bg-white overflow-y-auto">
            <ControlPanel
              metric={metric}
              mode={mode}
              onMetricChange={setMetric}
              stationCount={stationData.stations.length}
            />
            <div className="border-t border-slate-200">
              <StationCard station={selectedStation} mode={mode} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
