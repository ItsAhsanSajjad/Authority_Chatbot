"use client";

import Image from "next/image";
import { memo, useState, useEffect } from "react";

/* ═══════════════════════════════════════════════════════════
   PERA AI — Premium Thinking Indicator (v2)
   Multi-phase progress visualization with rich micro-animations.
   ═══════════════════════════════════════════════════════════ */

const PHASES = [
  { id: 0, label: "Identifying intent" },
  { id: 1, label: "Scanning knowledge base" },
  { id: 2, label: "Cross-referencing official sources" },
  { id: 3, label: "Synthesising verified response" },
];
const PHASE_DURATION_MS = 1800;

export const ThinkingIndicator = memo(function ThinkingIndicator() {
  const [phaseIdx, setPhaseIdx] = useState(0);
  const [elapsed, setElapsed] = useState(0);

  // Phase progression — cycles forward, holds at the last one
  useEffect(() => {
    const t = window.setInterval(() => {
      setPhaseIdx((p) => Math.min(p + 1, PHASES.length - 1));
    }, PHASE_DURATION_MS);
    return () => window.clearInterval(t);
  }, []);

  // Elapsed seconds counter for the corner indicator
  useEffect(() => {
    const t = window.setInterval(() => setElapsed((e) => e + 1), 1000);
    return () => window.clearInterval(t);
  }, []);

  const totalDone = phaseIdx; // phases before the active one are "done"
  const progressPct = ((phaseIdx + 0.6) / PHASES.length) * 100;

  return (
    <div className="flex gap-2.5 thinking-container">
      {/* Avatar */}
      <div className="flex-shrink-0 mt-1">
        <div className="think-avatar">
          <Image
            src="/Authority_Logo.png"
            alt=""
            width={20}
            height={20}
            className="rounded-md"
          />
          <span className="think-avatar-ring" aria-hidden />
        </div>
      </div>

      {/* Card */}
      <div className="think-card">
        {/* Aurora glow inside */}
        <span className="think-aurora" aria-hidden />

        {/* Top scan-line */}
        <span className="think-scan" aria-hidden />

        {/* Header row */}
        <div className="think-header">
          <span className="think-badge">
            <span className="think-badge-dot" />
            <span className="think-badge-text">PERA AI · ANALYSING</span>
          </span>
          <span className="think-elapsed">{elapsed}s</span>
        </div>

        {/* Progress meter */}
        <div className="think-progress">
          <div
            className="think-progress-fill"
            style={{ width: `${progressPct}%` }}
          >
            <span className="think-progress-shine" aria-hidden />
          </div>
        </div>

        {/* Phase list */}
        <ul className="think-phases" aria-live="polite">
          {PHASES.map((p, i) => {
            const state =
              i < totalDone ? "done" : i === totalDone ? "active" : "pending";
            return (
              <li key={p.id} className={`think-phase is-${state}`}>
                <span className="think-phase-marker">
                  {state === "done" ? (
                    <svg
                      viewBox="0 0 24 24"
                      width="11"
                      height="11"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="3"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <polyline points="20 6 9 17 4 12" />
                    </svg>
                  ) : state === "active" ? (
                    <>
                      <span className="think-phase-pulse" />
                      <span className="think-phase-pulse think-phase-pulse-2" />
                      <span className="think-phase-core" />
                    </>
                  ) : (
                    <span className="think-phase-empty" />
                  )}
                </span>
                <span className="think-phase-text">{p.label}</span>
                {state === "active" && (
                  <span className="think-phase-dots" aria-hidden>
                    <span /> <span /> <span />
                  </span>
                )}
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
});
