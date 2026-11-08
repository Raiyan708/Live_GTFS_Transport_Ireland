import { BANDS } from "../lib/delay";

const RANGE: Record<string, string> = { calm: "under 5 min", moderate: "5–10 min", severe: "10–15 min", critical: "15+ min" };

/** Every entry pairs a shape + a word with the colour: severity is never colour alone. */
export function Legend({ attribution }: { attribution: string | null }) {
  return (
    <aside className="legend panel" aria-label="Legend">
      <h2>Delay</h2>
      <ul>
        {BANDS.map((b) => (
          <li key={b.id}>
            <span className="sev" style={{ color: b.color }} aria-hidden="true">{b.icon}</span>
            <span>{b.label}</span>
            <span className="muted">{RANGE[b.id]}</span>
          </li>
        ))}
        <li>
          <span className="sev ring" aria-hidden="true">◌</span>
          <span>NO DATA</span>
          <span className="muted">delay unknown</span>
        </li>
      </ul>
      <p className="muted fine">Positions refresh about every 2 min. Movement between updates is animated from real reported positions, never extrapolated.</p>
      {attribution && <p className="muted fine fine-tight">{attribution}</p>}
    </aside>
  );
}
