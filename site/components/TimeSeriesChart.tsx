"use client";

/**
 * TimeSeriesChart.tsx
 * Plotly.js time series for the selected GESLA station.
 * Loaded via dynamic({ ssr: false }) in page.tsx.
 */

import { useEffect, useState } from "react";
import type { Station, ValidationMode, TimeSeriesData } from "@/types/data";

interface Props {
  station: Station | null;
  mode: ValidationMode;
}

const MODE_PATH: Record<ValidationMode, string> = {
  raw:            "raw",
  godin_filter:   "godin_filter",
  minus_fes_tide: "minus_fes_tide",
};

const MODE_LABEL: Record<ValidationMode, string> = {
  raw:            "Raw (tidal)",
  godin_filter:   "Godin de-tided",
  minus_fes_tide: "FES2022 de-tided",
};

export default function TimeSeriesChart({ station, mode }: Props) {
  const [tsData, setTsData] = useState<TimeSeriesData | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (!station) {
      setTsData(null);
      return;
    }
    setLoading(true);
    setErr(null);
    const path = MODE_PATH[mode];
    fetch(`/data/ts/${path}/${station.id}.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`No time series for ${station.id} / ${mode}`);
        return r.json();
      })
      .then((d: TimeSeriesData) => {
        setTsData(d);
        setLoading(false);
      })
      .catch((e) => {
        setErr(String(e));
        setLoading(false);
      });
  }, [station, mode]);

  if (!station) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-xs text-slate-400 italic">Select a station to view its time series.</p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-xs text-slate-400">Loading time series…</p>
      </div>
    );
  }

  if (err || !tsData) {
    return (
      <div className="flex items-center justify-center h-full px-4">
        <p className="text-xs text-slate-400 italic text-center">
          {err ?? "No time series available for this station / mode."}
        </p>
      </div>
    );
  }

  // Build Plotly traces
  const traces: object[] = [
    {
      x: tsData.dates,
      y: tsData.obs,
      name: "Observed",
      type: "scatter",
      mode: "lines",
      line: { color: "#374151", width: 1.5 },
      connectgaps: false,
    },
  ];

  if (tsData.notide) {
    traces.push({
      x: tsData.dates,
      y: tsData.notide,
      name: "Model surge",
      type: "scatter",
      mode: "lines",
      line: { color: "#6366f1", width: 1.5, dash: "dot" },
      connectgaps: false,
    });
  }

  if (tsData.tide) {
    traces.push({
      x: tsData.dates,
      y: tsData.tide,
      name: "Model tide",
      type: "scatter",
      mode: "lines",
      line: { color: "#0891b2", width: 1.5, dash: "dash" },
      connectgaps: false,
    });
  }

  const title = `${station.name} · ${MODE_LABEL[mode]} (daily mean, 2013–2018)`;

  const layout = {
    title: { text: title, font: { size: 11, color: "#374151" }, x: 0.01, xanchor: "left" },
    margin: { t: 28, r: 12, b: 36, l: 48 },
    paper_bgcolor: "white",
    plot_bgcolor: "white",
    xaxis: {
      showgrid: true,
      gridcolor: "#e2e8f0",
      tickfont: { size: 9 },
      rangeslider: { visible: true, thickness: 0.05 },
    },
    yaxis: {
      title: { text: "Sea level [m]", font: { size: 10 } },
      showgrid: true,
      gridcolor: "#e2e8f0",
      tickfont: { size: 9 },
      zeroline: true,
      zerolinecolor: "#cbd5e1",
    },
    legend: { orientation: "h", x: 0, y: -0.25, font: { size: 9 } },
    showlegend: true,
    autosize: true,
  };

  const config = {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ["select2d", "lasso2d", "toggleSpikelines"],
    displaylogo: false,
    toImageButtonOptions: {
      format: "png",
      filename: `${station.id}_${mode}`,
    },
  };

  // Lazy-import Plotly to avoid bundling it in the SSR chunk
  return (
    <PlotlyWrapper traces={traces} layout={layout} config={config} />
  );
}

// Lazy Plotly wrapper — imports plotly only when rendering
function PlotlyWrapper({
  traces,
  layout,
  config,
}: {
  traces: object[];
  layout: object;
  config: object;
}) {
  useEffect(() => {
    let destroyed = false;
    import("plotly.js-basic-dist-min").then((Plotly) => {
      if (destroyed) return;
      const el = document.getElementById("plotly-ts-chart");
      if (!el) return;
      (Plotly as typeof import("plotly.js")).newPlot(el, traces as import("plotly.js").Data[], layout as import("plotly.js").Layout, config as import("plotly.js").Config);
    });
    return () => {
      destroyed = true;
      import("plotly.js-basic-dist-min").then((Plotly) => {
        const el = document.getElementById("plotly-ts-chart");
        if (el) (Plotly as typeof import("plotly.js")).purge(el);
      });
    };
  }, [traces, layout, config]);

  return <div id="plotly-ts-chart" className="w-full h-full" />;
}
