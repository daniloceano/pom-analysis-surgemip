"use client";

/**
 * StationMap.tsx
 * Interactive Leaflet map showing GESLA stations coloured by a chosen metric.
 * Must be loaded with dynamic({ ssr: false }) — Leaflet requires browser APIs.
 */

import { useEffect, useRef, useMemo } from "react";
import type { Map as LeafletMap } from "leaflet";
import type { Station, ValidationMode, MetricKey } from "@/types/data";
import { METRIC_DEFS } from "@/types/data";

interface Props {
  stations: Station[];
  mode: ValidationMode;
  metric: MetricKey;
  selectedStation: Station | null;
  onSelectStation: (s: Station) => void;
}

// ── Color interpolation helpers ──────────────────────────────────────────────

function lerp(a: number, b: number, t: number) {
  return a + (b - a) * t;
}

function clamp(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v));
}

/** Map value in [vmin, vmax] to a CSS color string using a simple palette. */
function valueToColor(
  value: number,
  vmin: number,
  vmax: number,
  scale: "YlOrRd" | "RdBu" | "RdYlGn" | "Blues"
): string {
  const t = clamp((value - vmin) / (vmax - vmin || 1), 0, 1);

  if (scale === "YlOrRd") {
    // yellow → orange → red
    const r = Math.round(lerp(255, 165, Math.min(t * 2, 1)) * (t > 0.5 ? 1 : 1));
    const g = Math.round(lerp(255, 0, t));
    const b = 0;
    return `rgb(${Math.round(lerp(255, 215 * (1 - t), t))},${g},${b})`;
  }
  if (scale === "RdBu") {
    // red ← white → blue  (t=0.5 → white)
    const mid = 0.5;
    if (t < mid) {
      const s = 1 - t / mid;
      return `rgb(${Math.round(lerp(255, 50, s))},${Math.round(lerp(255, 100, s * 0.3))},${Math.round(lerp(255, 180, s))})`;
    } else {
      const s = (t - mid) / (1 - mid);
      return `rgb(${Math.round(lerp(255, 178, s))},${Math.round(lerp(255, 24, s))},${Math.round(lerp(255, 43, s))})`;
    }
  }
  if (scale === "RdYlGn") {
    // red → yellow → green
    if (t < 0.5) {
      const s = t / 0.5;
      return `rgb(215,${Math.round(lerp(48, 220, s))},${Math.round(lerp(39, 39, s))})`;
    } else {
      const s = (t - 0.5) / 0.5;
      return `rgb(${Math.round(lerp(215, 26, s))},${Math.round(lerp(220, 152, s))},${Math.round(lerp(39, 80, s))})`;
    }
  }
  // Blues
  return `rgb(${Math.round(lerp(247, 8, t))},${Math.round(lerp(251, 81, t))},${Math.round(lerp(255, 156, t))})`;
}

// ── Compute color scale bounds (p2 / p98) ────────────────────────────────────

function computeBounds(
  stations: Station[],
  mode: ValidationMode,
  metric: MetricKey,
  symmetric: boolean
): [number, number] {
  const vals: number[] = [];
  for (const s of stations) {
    const m = s.metrics[mode];
    if (!m) continue;
    const v = (m as unknown as Record<string, number | null | undefined>)[metric];
    if (v != null && isFinite(v)) vals.push(v);
  }
  if (vals.length === 0) return [0, 1];
  vals.sort((a, b) => a - b);
  const p2  = vals[Math.floor(vals.length * 0.02)] ?? vals[0];
  const p98 = vals[Math.ceil(vals.length * 0.98 - 1)] ?? vals[vals.length - 1];
  if (symmetric) {
    const abs = Math.max(Math.abs(p2), Math.abs(p98));
    return [-abs, abs];
  }
  return [p2, p98];
}

// ── ColorBar component ────────────────────────────────────────────────────────

function ColorBar({
  vmin, vmax, scale, label,
}: {
  vmin: number; vmax: number; scale: "YlOrRd" | "RdBu" | "RdYlGn" | "Blues"; label: string;
}) {
  const stops = Array.from({ length: 20 }, (_, i) => {
    const t = i / 19;
    const v = vmin + (vmax - vmin) * t;
    return valueToColor(v, vmin, vmax, scale);
  });
  const gradient = `linear-gradient(to right, ${stops.join(",")})`;

  return (
    <div className="absolute bottom-4 left-4 bg-white/90 rounded shadow-md px-3 py-2 pointer-events-none z-[1000]">
      <p className="text-[10px] text-slate-600 mb-1 font-medium">{label}</p>
      <div
        className="w-36 h-3 rounded"
        style={{ background: gradient }}
      />
      <div className="flex justify-between mt-0.5">
        <span className="text-[9px] text-slate-500">{vmin.toFixed(2)}</span>
        <span className="text-[9px] text-slate-500">{vmax.toFixed(2)}</span>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function StationMap({
  stations, mode, metric, selectedStation, onSelectStation,
}: Props) {
  const mapRef   = useRef<LeafletMap | null>(null);
  const divRef   = useRef<HTMLDivElement>(null);
  const markersRef = useRef<Map<string, ReturnType<typeof import("leaflet")["circleMarker"]>>>(new Map());

  const def = METRIC_DEFS[metric];
  const [vmin, vmax] = useMemo(
    () => computeBounds(stations, mode, metric, def.symmetric),
    [stations, mode, metric, def.symmetric]
  );

  // ── Initialize Leaflet map once ──────────────────────────────────────────
  useEffect(() => {
    if (mapRef.current || !divRef.current) return;

    // Leaflet must be imported inside the effect (browser-only)
    import("leaflet").then((L) => {
      const map = L.map(divRef.current!, {
        center: [20, 0],
        zoom: 2,
        zoomControl: true,
      });

      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "© OpenStreetMap contributors",
        maxZoom: 18,
      }).addTo(map);

      mapRef.current = map;
    });

    return () => {
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
      }
    };
  }, []);

  // ── Re-render markers when stations / mode / metric changes ─────────────
  useEffect(() => {
    if (!mapRef.current) return;

    import("leaflet").then((L) => {
      const map = mapRef.current!;

      // Remove existing markers
      markersRef.current.forEach((m) => m.remove());
      markersRef.current.clear();

      for (const station of stations) {
        const m = station.metrics[mode];
        const rawVal = m ? (m as unknown as Record<string, number | null | undefined>)[metric] : null;
        const color =
          rawVal != null && isFinite(rawVal)
            ? valueToColor(rawVal, vmin, vmax, def.colorScale)
            : "#aaaaaa";

        const isSelected = selectedStation?.id === station.id;
        const marker = L.circleMarker([station.lat, station.lon], {
          radius: isSelected ? 8 : 5,
          fillColor: color,
          color: isSelected ? "#1e40af" : "#ffffff",
          weight: isSelected ? 2 : 0.5,
          fillOpacity: 0.85,
          opacity: 1,
        }).addTo(map);

        marker.bindTooltip(
          `<b>${station.name}</b><br/>${station.country} · ${station.site_code}<br/>` +
            (rawVal != null
              ? `${def.label}: ${rawVal.toFixed(4)}${def.unit ? " " + def.unit : ""}`
              : "No data for this mode"),
          { sticky: true, className: "leaflet-tooltip-simple" }
        );

        marker.on("click", () => onSelectStation(station));
        markersRef.current.set(station.id, marker);
      }
    });
  }, [stations, mode, metric, vmin, vmax, def, selectedStation, onSelectStation]);

  return (
    <div className="relative w-full h-full">
      <div ref={divRef} className="w-full h-full" />
      <ColorBar vmin={vmin} vmax={vmax} scale={def.colorScale} label={`${def.label}${def.unit ? " [" + def.unit + "]" : ""}`} />
    </div>
  );
}
