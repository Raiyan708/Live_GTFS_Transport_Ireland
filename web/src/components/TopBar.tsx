import type { AlertsResponse, LinkStatus, Stats } from "../api";
import { Ticker } from "./Ticker";

interface Props {
  stats: Stats | null;
  alerts: AlertsResponse;
  status: LinkStatus;
  dataAgeSeconds: number | null;
  reducedMotion: boolean;
}

const STATUS_TEXT: Record<LinkStatus, string> = { connecting: "CONNECTING", live: "LIVE", reconnecting: "RECONNECTING", replay: "REPLAY" };

export function TopBar({ stats, alerts, status, dataAgeSeconds, reducedMotion }: Props) {
  const latePct = stats && stats.with_delay ? Math.round((stats.late / stats.with_delay) * 100) : null;
  const age = dataAgeSeconds === null ? "—" : dataAgeSeconds < 90 ? `${Math.round(dataAgeSeconds)} s` : `${(dataAgeSeconds / 60).toFixed(1)} min`;
  return (
    <header className="topbar panel">
      <div className="brand">
        <span className={`beacon beacon-${status}`} aria-hidden="true" />
        <div>
          <h1>Ireland Transit Control Room</h1>
          <p className={`link link-${status}`}>
            <span aria-hidden="true">{status === "live" ? "●" : status === "replay" ? "▶" : "◌"}</span> {STATUS_TEXT[status]}
            <span className="muted"> · {status === "replay" ? "recorded NTA data" : "NTA GTFS-Realtime"}</span>
          </p>
        </div>
      </div>
      <dl className="stats">
        <div className="stat">
          <dt>Tracked</dt>
          <dd><Ticker value={stats?.vehicles ?? null} reducedMotion={reducedMotion} /></dd>
          <small>vehicles on the road</small>
        </div>
        <div className="stat stat-hot">
          <dt>Running late</dt>
          <dd><Ticker value={stats?.late ?? null} reducedMotion={reducedMotion} /></dd>
          <small>{latePct === null ? "—" : `${latePct}% of tracked · ≥ 5 min${stats && stats.stalled ? ` · ${stats.stalled} stalled` : ""}`}</small>
        </div>
        <div className="stat">
          <dt>Active alerts</dt>
          <dd><Ticker value={alerts.available ? alerts.summary.with_vehicle : null} reducedMotion={reducedMotion} /></dd>
          <small>{alerts.available ? `on the map · +${(alerts.summary.active - alerts.summary.with_vehicle).toLocaleString("en-IE")} predicted` : "alert service offline"}</small>
        </div>
        <div className="stat">
          <dt>Position age</dt>
          <dd className="num">{age}</dd>
          <small>median, feed refreshes ~2 min</small>
        </div>
      </dl>
    </header>
  );
}
