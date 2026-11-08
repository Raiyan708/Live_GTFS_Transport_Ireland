import { REPLAY_STEP_MS, demoControls, useDemoState } from "../demo";

const dublin = (epoch: number, opts: Intl.DateTimeFormatOptions) => new Date(epoch * 1000).toLocaleString("en-IE", { timeZone: "Europe/Dublin", ...opts });

/** Replay controls for the recorded demo: play/pause, a scrubber over the recording, and an honest label of what is being shown. */
export function DemoBar({ onAbout }: { onAbout: () => void }) {
  const s = useDemoState();
  if (!s.frames || !s.meta) return null;
  const f = s.frames[s.index];
  const speed = Math.round(s.meta.median_gap_s / (REPLAY_STEP_MS / 1000));
  return (
    <div className="demobar panel" role="group" aria-label="Replay controls">
      <button className="playbtn" onClick={demoControls.toggle} aria-label={s.playing ? "Pause replay" : "Play replay"}>{s.playing ? "❚❚" : "▶"}</button>
      <div className="demo-mid">
        <input
          type="range" min={0} max={s.frames.length - 1} value={s.index} aria-label="Position in the recording"
          onChange={(e) => demoControls.seek(Number(e.target.value))}
        />
        <p>
          <strong className="num">{dublin(f.t, { hour: "2-digit", minute: "2-digit", hour12: false })}</strong>{" "}
          <span className="muted">Dublin time · {dublin(f.t, { weekday: "short", day: "numeric", month: "short", year: "numeric" })} · replay of real NTA data at {speed}× speed</span>
        </p>
      </div>
      <button className="chip" onClick={onAbout}>About this project</button>
    </div>
  );
}
