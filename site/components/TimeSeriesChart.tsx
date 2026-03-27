"use client";

/**
 * TimeSeriesChart.tsx
 * Plotly.js time series for the selected GESLA station.
 * Loaded via dynamic({ ssr: false }) in page.tsx.
 *
 * Always shows the SAME three traces regardless of the validation mode:
 *
 *   obs   — raw GESLA observation (with tidal signal)
 *   tide  — POM_tide (surge + tidal signal)
 *   notide— POM_notide (storm surge only, reference)
 *
 * The validation mode selector only affects the metric maps and station
 * card metrics.  The primary time-series view is always obs vs POM_tide
 * so that both signals are on comparable scales (both include the tide).
 */

import { useEffect, useState } from "react";
import type { Station, TimeSeriesData } from "@/types/data";

interface Props {
  station: Station | null;
}

export default function TimeSeriesChart({ station }: Props) {
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
    fetch(`/data/ts/${station.id}.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`No time series for ${station.id}`);
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
  }, [station]);

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
          {err ?? "No time series available for this station."}
        </p>
      </div>
    );
  }

  // Build Plotly traces
  const traces: object[] = [
    {
      x: tsData.dates,
      y: tsData.obs,
      name: "GESLA obs (raw)",
      type: "scatter",
      mode: "lines",
      line: { color: "#374151", width: 1.5 },
      connectgaps: false,
    },
  ];

  if (tsData.tide) {
    traces.push({
      x: tsData.dates,
      y: tsData.tide,
      name: "POM tide",
      type: "scatter",
      mode: "lines",
      line: { color: "#0891b2", width: 1.5, dash: "dash" },
      connectgaps: false,
    });
  }

  if (tsData.notide) {
    traces.push({
      x: tsData.dates,
      y: tsData.notide,
      name: "POM no-tide (surge ref.)",
      type: "scatter",
      mode: "lines",
      line: { color: "#94a3b8", width: 1.0, dash: "dot" },
      connectgaps: false,
    });
  }

  const title = `${station.name} · daily mean 2013–2018`;

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
      filename: `${station.id}_timeseries`,
    },
  };

  return <PlotlyWrapper traces={traces} layout={layout} config={config} />;
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
