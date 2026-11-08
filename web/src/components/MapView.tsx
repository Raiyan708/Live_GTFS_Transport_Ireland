import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
// MapLibre 6 loads tiles in a separate worker module. `?worker&url` makes Vite BUNDLE that worker (with its shared
// chunk) in dev and in the production build; a plain URL works in dev only and breaks `vite build`.
import workerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url";
import { useEffect, useRef } from "react";
import type { Vehicle } from "../api";
import { BANDS, type BandId, bandFor } from "../lib/delay";
import { type Track, alphaAt, positionAt, retarget } from "../lib/glide";

// Third-party basemap, loaded by the VIEWER's browser (tile requests reveal the viewport to the provider).
// Free for development with attribution; use a licensed provider for anything public. Override with VITE_MAP_STYLE.
const STYLE = import.meta.env.VITE_MAP_STYLE ?? "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
const BLANK: maplibregl.StyleSpecification = {
  version: 8,
  sources: {},
  layers: [{ id: "bg", type: "background", paint: { "background-color": "#0c0e11" } }],
};

export interface FlyRequest {
  lon: number;
  lat: number;
  zoom: number;
  bounds?: [number, number, number, number]; // [west, south, east, north]: fit this box instead of using lon/lat/zoom
  nonce: number; // a new nonce = a new request, even to the same place
}

export const IRELAND_BOX: [number, number, number, number] = [-10.8, 51.3, -5.4, 55.5];

/** Keep the island clear of the floating panels: left rail on desktop, bottom sheet on phones. */
function cameraPadding(): maplibregl.PaddingOptions {
  const w = window.innerWidth;
  const h = window.innerHeight;
  return w <= 800 ? { top: 150, bottom: Math.round(h * 0.45), left: 12, right: 12 } : { top: 110, bottom: 30, left: 370, right: 30 };
}

interface Props {
  vehicles: Vehicle[];
  selectedId: string | null;
  minBand: BandId | "all";
  fly: FlyRequest | null;
  reducedMotion: boolean;
  onSelect: (id: string | null) => void;
}

maplibregl.setWorkerUrl(workerUrl);

const FRAME_MS = 1000 / 30;
const PULSE_PERIOD_MS = 2000;
const bandRank = (id: BandId) => BANDS.findIndex((b) => b.id === id);
const hash01 = (s: string) => {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) % 997;
  return h / 997;
};

export function MapView({ vehicles, selectedId, minBand, fly, reducedMotion, onSelect }: Props) {
  const el = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const tracks = useRef<Map<string, Track>>(new Map());
  const meta = useRef<Map<string, Vehicle>>(new Map());
  // Props the frame loop reads: kept in refs so the loop is created once, not on every render.
  const live = useRef({ selectedId, minBand, reducedMotion, onSelect });
  live.current = { selectedId, minBand, reducedMotion, onSelect };
  const draw = useRef<() => void>(() => {});

  // ---- create the map once
  useEffect(() => {
    if (!el.current) return;
    const m = new maplibregl.Map({
      container: el.current, style: STYLE, bounds: IRELAND_BOX, fitBoundsOptions: { padding: cameraPadding() },
      minZoom: 4, maxZoom: 17, attributionControl: { compact: true }, dragRotate: false, pitchWithRotate: false,
    });
    map.current = m;
    m.touchZoomRotate.disableRotation();
    let fallback = false;
    m.on("error", () => {
      if (!fallback && !m.isStyleLoaded()) {
        fallback = true;
        m.setStyle(BLANK); // basemap unreachable: still show the vehicles on plain dark
      }
    });

    const addLayers = () => {
      if (m.getSource("vehicles")) return;
      const empty: GeoJSON.FeatureCollection = { type: "FeatureCollection", features: [] };
      m.addSource("vehicles", { type: "geojson", data: empty });
      m.addSource("selected", { type: "geojson", data: empty });
      m.addLayer({
        id: "pulse", type: "circle", source: "vehicles", filter: ["get", "pulsing"],
        paint: {
          "circle-radius": ["+", ["get", "radius"], ["*", ["get", "pulse"], ["*", ["get", "radius"], 2.4]]],
          "circle-color": ["get", "color"], "circle-blur": 0.35,
          "circle-opacity": ["*", ["get", "alpha"], ["*", ["get", "dimmed"], ["-", 0.5, ["*", ["get", "pulse"], 0.5]]]],
        },
      });
      m.addLayer({
        id: "dots", type: "circle", source: "vehicles",
        paint: {
          "circle-radius": ["get", "radius"],
          // unknown delay = hollow ring: it must never read as "on time"
          "circle-color": ["case", ["get", "hollow"], "rgba(12,14,17,0.65)", ["get", "color"]],
          "circle-stroke-color": ["case", ["get", "hollow"], ["get", "color"], "#0c0e11"],
          "circle-stroke-width": ["case", ["get", "hollow"], 1.4, 0.8],
          "circle-opacity": ["*", ["get", "alpha"], ["get", "dimmed"]],
          "circle-stroke-opacity": ["*", ["get", "alpha"], ["get", "dimmed"]],
        },
      });
      m.addLayer({
        id: "selected-ring", type: "circle", source: "selected",
        paint: { "circle-radius": ["get", "radius"], "circle-color": "rgba(0,0,0,0)", "circle-stroke-color": "#ffffff", "circle-stroke-width": 2 },
      });
    };
    m.on("style.load", addLayers); // also re-adds after the basemap fallback swaps the style

    m.on("click", (e) => {
      const hit = m.getLayer("dots") ? m.queryRenderedFeatures(e.point, { layers: ["dots"] }) : [];
      live.current.onSelect(hit.length ? String(hit[0].properties?.id) : null);
    });
    m.on("mousemove", (e) => {
      const hit = m.getLayer("dots") ? m.queryRenderedFeatures(e.point, { layers: ["dots"] }) : [];
      m.getCanvas().style.cursor = hit.length ? "pointer" : "";
    });

    // ---- the frame loop: positions glide, delayed vehicles pulse
    draw.current = () => {
      const src = m.getSource("vehicles") as maplibregl.GeoJSONSource | undefined;
      const sel = m.getSource("selected") as maplibregl.GeoJSONSource | undefined;
      if (!src || !sel) return;
      const { minBand: min, reducedMotion: rm, selectedId: selId } = live.current;
      const now = performance.now();
      const zoomScale = Math.min(2.4, Math.max(0.8, 0.8 + (m.getZoom() - 5.5) * 0.17)); // MapLibre forbids zoom exprs inside arithmetic
      const minRank = min === "all" ? -1 : bandRank(min);
      const features: GeoJSON.Feature[] = [];
      let selFeature: GeoJSON.Feature | null = null;
      for (const [id, v] of meta.current) {
        const t = tracks.current.get(id);
        if (!t) continue;
        const p = positionAt(t, now);
        const band = bandFor(v.delay_s);
        const phase = ((now / PULSE_PERIOD_MS) + hash01(id)) % 1;
        const radius = band.radius * zoomScale;
        features.push({
          type: "Feature", geometry: { type: "Point", coordinates: [p.lon, p.lat] },
          properties: {
            id, color: band.color, radius, alpha: alphaAt(t, now, rm), hollow: v.delay_s === null,
            pulsing: !rm && band.id !== "calm", pulse: phase,
            dimmed: bandRank(band.id) < minRank ? 0.1 : 1,
          },
        });
        if (id === selId) {
          selFeature = {
            type: "Feature", geometry: { type: "Point", coordinates: [p.lon, p.lat] },
            properties: { radius: radius + 7 + (rm ? 0 : Math.sin(now / 300) * 1.5) },
          };
        }
      }
      src.setData({ type: "FeatureCollection", features });
      sel.setData({ type: "FeatureCollection", features: selFeature ? [selFeature] : [] });
    };

    let raf = 0;
    let last = 0;
    const loop = (ts: number) => {
      raf = requestAnimationFrame(loop);
      if (live.current.reducedMotion || ts - last < FRAME_MS) return;
      last = ts;
      draw.current();
    };
    raf = requestAnimationFrame(loop);
    return () => {
      cancelAnimationFrame(raf);
      m.remove();
      map.current = null;
    };
  }, []);

  // ---- new snapshot: re-aim the glides
  useEffect(() => {
    meta.current = new Map(vehicles.map((v) => [v.id, v]));
    tracks.current = retarget(tracks.current, vehicles, performance.now(), { reducedMotion });
    draw.current();
  }, [vehicles, reducedMotion]);

  // ---- with reduced motion there is no loop, so redraw when selection/filter change
  useEffect(() => {
    draw.current();
  }, [selectedId, minBand, reducedMotion]);

  // ---- fly requests (city buttons, clicking a row in the list)
  useEffect(() => {
    if (!fly || !map.current) return;
    const duration = reducedMotion ? 0 : 1600;
    if (fly.bounds) map.current.fitBounds(fly.bounds, { padding: cameraPadding(), duration });
    else map.current.flyTo({ center: [fly.lon, fly.lat], zoom: fly.zoom, duration, essential: false });
  }, [fly, reducedMotion]);

  return <div ref={el} className="map" aria-label="Live map of vehicles" role="application" />;
}
