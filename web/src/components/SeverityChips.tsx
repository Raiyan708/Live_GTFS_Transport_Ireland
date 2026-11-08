import type { BandId } from "../lib/delay";

const FILTERS: { id: BandId | "all"; label: string }[] = [
  { id: "all", label: "All ≥ 5 min" },
  { id: "severe", label: "Severe+" },
  { id: "critical", label: "Critical" },
];

/** One severity filter shared by both lists AND the map, so the three can never disagree. */
export function SeverityChips({ value, onChange }: { value: BandId | "all"; onChange: (b: BandId | "all") => void }) {
  return (
    <div className="chips" role="group" aria-label="Severity filter">
      {FILTERS.map((f) => (
        <button key={f.id} className={`chip ${value === f.id ? "chip-on" : ""}`} aria-pressed={value === f.id} onClick={() => onChange(f.id)}>
          {f.label}
        </button>
      ))}
    </div>
  );
}
