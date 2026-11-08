import { AnimatePresence, motion } from "motion/react";
import { useEffect, useRef } from "react";
import { IS_DEMO } from "../demo";
import { BUGS_FOUND, FACTS, LIMITS, REPO_URL, STACK } from "../about";

function Arrow({ d, dashed }: { d: string; dashed?: boolean }) {
  return <path d={d} className={`arch-line ${dashed ? "dashed" : ""}`} markerEnd="url(#arrow)" />;
}
function Node({ x, y, w = 150, h = 46, title, sub, dashed, accent }: { x: number; y: number; w?: number; h?: number; title: string; sub?: string; dashed?: boolean; accent?: boolean }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx={9} className={`arch-box ${dashed ? "dashed" : ""} ${accent ? "accent" : ""}`} />
      <text x={x + w / 2} y={y + (sub ? 19 : 28)} textAnchor="middle" className="arch-title">{title}</text>
      {sub && <text x={x + w / 2} y={y + 35} textAnchor="middle" className="arch-sub">{sub}</text>}
    </g>
  );
}

/** Solid = ran end to end on the live feed. Dashed = written and unit-tested, never run against the real service. */
function Architecture() {
  return (
    <svg viewBox="0 0 980 330" role="img" aria-label="Architecture: NTA feed to producer to Kafka, then a hot path (Spark alert state machine, Lambda, DynamoDB, API, dashboard) and a cold path (Spark, S3, Glue, Snowflake, Airflow)." className="arch">
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" className="arch-head" /></marker>
      </defs>
      <text x="8" y="16" className="arch-lane">INGEST</text>
      <Node x={8} y={120} title="NTA GTFS-Realtime" sub="protobuf, 1 call/min" w={140} />
      <Node x={190} y={120} title="Producer" sub="Python · idempotent" w={120} />
      <Node x={352} y={120} title="Kafka" sub="KRaft · keyed by trip" w={130} accent />
      <Arrow d="M148,143 L190,143" /><Arrow d="M310,143 L352,143" />

      <text x="520" y="16" className="arch-lane">HOT PATH · seconds-to-minutes</text>
      <Node x={520} y={28} title="Spark Streaming" sub="per-trip alert state machine" w={168} />
      <Node x={716} y={28} title="Lambda → DynamoDB" sub="idempotent · local emulator" w={158} />
      <Node x={716} y={96} title="FastAPI + SSE" sub="single-flight cache" w={158} />
      <Node x={900} y={96} title="React" sub="MapLibre" w={74} />
      <Arrow d="M482,132 C500,132 500,51 520,51" /><Arrow d="M688,51 L716,51" /><Arrow d="M795,74 L795,96" /><Arrow d="M874,119 L900,119" />

      <text x="520" y="192" className="arch-lane">COLD PATH · analytics</text>
      <Node x={520} y={204} title="Spark" sub="raw + 5-min aggregates" w={128} />
      <Node x={672} y={204} title="S3 → Glue" sub="curated · serving" w={118} />
      <Node x={814} y={204} title="Snowflake" sub="partition-replace load" w={160} dashed />
      <Node x={672} y={272} title="Airflow" sub="freshness · DQ · reconcile" w={160} dashed />
      <Arrow d="M482,154 C500,154 500,227 520,227" /><Arrow d="M648,227 L672,227" /><Arrow d="M790,227 L814,227" dashed />
      <Arrow d="M752,272 L752,250" dashed />
      <path d="M730,250 C730,262 840,262 894,250" className="arch-line dashed" fill="none" />
      <Arrow d="M790,227 C800,160 760,150 760,142" />
      <text x="8" y="290" className="arch-note">AWS services (S3, DynamoDB, SNS) run on a local emulator; Terraform for the real thing validates but has not been applied.</text>
      <g transform="translate(8,305)">
        <line x1="0" y1="0" x2="30" y2="0" className="arch-line" /><text x="38" y="4" className="arch-note">ran end to end on the live feed</text>
        <line x1="260" y1="0" x2="290" y2="0" className="arch-line dashed" /><text x="298" y="4" className="arch-note">written and unit-tested; not run against the real service yet</text>
      </g>
    </svg>
  );
}

export function AboutPanel({ open, onClose, reducedMotion }: { open: boolean; onClose: () => void; reducedMotion: boolean }) {
  const closeBtn = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (!open) return;
    closeBtn.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  return (
    <AnimatePresence>
      {open && (
        <motion.div className="about-backdrop" onClick={onClose} initial={reducedMotion ? false : { opacity: 0 }} animate={{ opacity: 1 }} exit={reducedMotion ? undefined : { opacity: 0 }}>
          <motion.section
            className="about panel" role="dialog" aria-modal="true" aria-labelledby="about-title" onClick={(e) => e.stopPropagation()}
            initial={reducedMotion ? false : { opacity: 0, y: 24, scale: 0.98 }} animate={{ opacity: 1, y: 0, scale: 1 }} exit={reducedMotion ? undefined : { opacity: 0, y: 24 }}
            transition={{ type: "spring", stiffness: 320, damping: 32 }}
          >
            <header className="about-head">
              <div>
                <p className="eyebrow">Data engineering project</p>
                <h2 id="about-title">Real-time delay alerting for Irish public transport</h2>
                <p className="lede">
                  A streaming pipeline over the National Transport Authority's live GTFS-Realtime feed: about 1,000 vehicles and 3,000 trips every two minutes,
                  turned into per-trip delay alerts and route-reliability analytics. {IS_DEMO ? "You are looking at a replay of real data recorded on 8 November 2026." : "This is the live dashboard."}
                </p>
              </div>
              <button ref={closeBtn} className="close" onClick={onClose} aria-label="Close">✕</button>
            </header>

            <div className="about-body">
              <h3>How it fits together</h3>
              <Architecture />

              <h3>What running it on real data taught me</h3>
              <ul className="facts">
                {FACTS.map((f) => (
                  <li key={f.big}><strong className="num">{f.big}</strong><span>{f.text}</span></li>
                ))}
              </ul>

              <div className="about-cols">
                <div>
                  <h3>Bugs only the live feed exposed</h3>
                  <ul className="plain">{BUGS_FOUND.map((b) => <li key={b}>{b}</li>)}</ul>
                </div>
                <div>
                  <h3>What is not proven (yet)</h3>
                  <ul className="plain">{LIMITS.map((b) => <li key={b}>{b}</li>)}</ul>
                </div>
              </div>

              <h3>Built with</h3>
              <p className="stack">{STACK.map((s) => <span key={s} className="tag">{s}</span>)}</p>

              <footer className="about-foot">
                <button className="cta" onClick={onClose}>{IS_DEMO ? "Explore the replay" : "Open the dashboard"}</button>
                <a href={REPO_URL} target="_blank" rel="noreferrer noopener">Source code and decision log on GitHub ↗</a>
                <span className="muted">Contains data from the National Transport Authority / Transport for Ireland, CC BY 4.0. Delay values are NTA's; they have not been independently verified.</span>
              </footer>
            </div>
          </motion.section>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
