import { useEffect, useMemo, useState } from "react";
import { type Alert, type Vehicle, useAlerts, useReliability, useRoutes, useSnapshot } from "./api";
import { AboutPanel } from "./components/AboutPanel";
import { AlertsList } from "./components/AlertsList";
import { DemoBar } from "./components/DemoBar";
import { DetailCard } from "./components/DetailCard";
import { Legend } from "./components/Legend";
import { type FlyRequest, IRELAND_BOX, MapView } from "./components/MapView";
import { ReliabilityPanel } from "./components/ReliabilityPanel";
import { TopBar } from "./components/TopBar";
import { WorstList } from "./components/WorstList";
import { IS_DEMO } from "./demo";
import type { BandId } from "./lib/delay";

const PLACES: { name: string; lon: number; lat: number; zoom: number; bounds?: typeof IRELAND_BOX }[] = [
  { name: "Ireland", lon: -7.7, lat: 53.4, zoom: 6.3, bounds: IRELAND_BOX },
  { name: "Dublin", lon: -6.26, lat: 53.35, zoom: 11 },
  { name: "Cork", lon: -8.47, lat: 51.9, zoom: 11.5 },
  { name: "Galway", lon: -9.05, lat: 53.27, zoom: 11.5 },
  { name: "Limerick", lon: -8.63, lat: 52.66, zoom: 11.5 },
];

function useReducedMotion(): boolean {
  const [rm, setRm] = useState(() => window.matchMedia("(prefers-reduced-motion: reduce)").matches);
  useEffect(() => {
    const q = window.matchMedia("(prefers-reduced-motion: reduce)");
    const on = () => setRm(q.matches);
    q.addEventListener("change", on);
    return () => q.removeEventListener("change", on);
  }, []);
  return rm;
}

export default function App() {
  const { snapshot, status, receivedAt } = useSnapshot();
  const { routes, attribution } = useRoutes();
  const { alerts, stale } = useAlerts();
  const reducedMotion = useReducedMotion();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [minBand, setMinBand] = useState<BandId | "all">("all");
  const [fly, setFly] = useState<FlyRequest | null>(null);
  const [tab, setTab] = useState<"alerts" | "delays">("alerts");
  // First visit: open the About panel so a visitor learns what they are looking at before they start clicking.
  const [aboutOpen, setAboutOpen] = useState(() => {
    try { return !localStorage.getItem("about-seen-v1"); } catch { return true; }
  });
  const closeAbout = () => {
    setAboutOpen(false);
    try { localStorage.setItem("about-seen-v1", "1"); } catch { /* private mode: it will just show again */ }
  };
  const [reliabilityOpen, setReliabilityOpen] = useState(false);
  // Default to alerts backed by a vehicle on the map: untracked ("predicted") alerts flip in bulk (up to 45% in one step).
  const [showPredicted, setShowPredicted] = useState(false);
  const reliability = useReliability(reliabilityOpen);
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  const vehicles = snapshot?.vehicles ?? [];
  const selected = useMemo(() => vehicles.find((v) => v.id === selectedId) ?? null, [vehicles, selectedId]);

  // Median position age at snapshot time, plus how long ago the snapshot arrived: honest "how stale is this".
  const dataAgeSeconds = useMemo(() => {
    if (!snapshot || receivedAt === null || snapshot.vehicles.length === 0) return null;
    const ages = snapshot.vehicles.map((v) => v.age_s).sort((a, b) => a - b);
    return ages[Math.floor(ages.length / 2)] + Math.max(0, (now - receivedAt) / 1000);
  }, [snapshot, receivedAt, now]);

  const flyTo = (lon: number, lat: number, zoom: number, bounds?: typeof IRELAND_BOX) =>
    setFly({ lon, lat, zoom, bounds, nonce: Date.now() });
  const pick = (v: Vehicle) => {
    setSelectedId(v.id);
    flyTo(v.lon, v.lat, 13.5);
  };
  const pickAlert = (a: Alert) => {
    if (!a.vehicle) return;
    setSelectedId(a.vehicle.id);
    flyTo(a.vehicle.lon, a.vehicle.lat, 13.5);
  };
  // If the alert pipeline is down, fall back to the raw list rather than showing an empty panel.
  const activeTab = alerts.available || tab === "delays" ? tab : "delays";

  return (
    <div className={`app ${IS_DEMO ? "has-demobar" : ""}`}>
      <MapView vehicles={vehicles} selectedId={selectedId} minBand={minBand} fly={fly} reducedMotion={reducedMotion} onSelect={setSelectedId} />
      <div className="vignette" aria-hidden="true" />
      <TopBar stats={snapshot?.stats ?? null} alerts={alerts} status={status} dataAgeSeconds={dataAgeSeconds} reducedMotion={reducedMotion} />
      <div className="rail">
        <div className="tabs panel" role="tablist" aria-label="Lists">
          <button role="tab" aria-selected={activeTab === "alerts"} className={activeTab === "alerts" ? "tab-on" : ""} onClick={() => setTab("alerts")}>
            Alerts <span className="num">{alerts.available ? (showPredicted ? alerts.summary.active : alerts.summary.with_vehicle) : "–"}</span>
          </button>
          <button role="tab" aria-selected={activeTab === "delays"} className={activeTab === "delays" ? "tab-on" : ""} onClick={() => setTab("delays")}>
            Live delays <span className="num">{snapshot ? snapshot.stats.late : "–"}</span>
          </button>
        </div>
        {activeTab === "alerts" ? (
          <AlertsList alerts={alerts} stale={stale} showPredicted={showPredicted} onShowPredicted={setShowPredicted} routes={routes} selectedVehicleId={selectedId} minBand={minBand} onMinBand={setMinBand} onPick={pickAlert} reducedMotion={reducedMotion} />
        ) : (
          <WorstList routes={routes} vehicles={vehicles} selectedId={selectedId} minBand={minBand} onMinBand={setMinBand} onPick={pick} reducedMotion={reducedMotion} />
        )}
        <DetailCard routes={routes} vehicle={selected} onClose={() => setSelectedId(null)} reducedMotion={reducedMotion} />
      </div>
      <ReliabilityPanel open={reliabilityOpen} onClose={() => setReliabilityOpen(false)} state={reliability} routes={routes} reducedMotion={reducedMotion} />
      <nav className="places panel" aria-label="Jump to">
        <button className="chip chip-accent" onClick={() => setReliabilityOpen((o) => !o)} aria-expanded={reliabilityOpen}>Route reliability</button>
        <button className="chip" onClick={() => setAboutOpen(true)}>About</button>
        {PLACES.map((p) => (
          <button key={p.name} className="chip" onClick={() => flyTo(p.lon, p.lat, p.zoom, p.bounds)}>{p.name}</button>
        ))}
      </nav>
      <Legend attribution={attribution} />
      {IS_DEMO && <DemoBar onAbout={() => setAboutOpen(true)} />}
      <AboutPanel open={aboutOpen} onClose={closeAbout} reducedMotion={reducedMotion} />
      {status !== "live" && !snapshot && <div className="boot">Connecting to the live feed…</div>}
    </div>
  );
}
