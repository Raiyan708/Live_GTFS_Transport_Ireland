import { animate } from "motion";
import { useEffect, useRef } from "react";

/** A number that counts to its new value instead of jumping. Respects reduced motion (snaps). */
export function Ticker({ value, reducedMotion, suffix = "" }: { value: number | null; reducedMotion: boolean; suffix?: string }) {
  const ref = useRef<HTMLSpanElement>(null);
  const shown = useRef(0);

  useEffect(() => {
    if (value === null || !ref.current) return;
    const node = ref.current;
    if (reducedMotion) {
      shown.current = value;
      node.textContent = value.toLocaleString("en-IE") + suffix;
      return;
    }
    const ctrl = animate(shown.current, value, {
      duration: 0.9,
      ease: "easeOut",
      onUpdate: (v) => {
        shown.current = v;
        node.textContent = Math.round(v).toLocaleString("en-IE") + suffix;
      },
    });
    return () => ctrl.stop();
  }, [value, reducedMotion, suffix]);

  return <span ref={ref} className="num">{value === null ? "—" : value.toLocaleString("en-IE") + suffix}</span>;
}
