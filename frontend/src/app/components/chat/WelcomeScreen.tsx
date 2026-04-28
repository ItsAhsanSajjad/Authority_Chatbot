"use client";

import Image from "next/image";
import { memo } from "react";

/* ═══════════════════════════════════════════════════════════
   PERA AUTHORITY CHATBOT — Clean Institutional Landing (v7)
   Focused, restrained, government-grade.
   ═══════════════════════════════════════════════════════════ */

interface SuggestionItem {
  title: string;
  prompt: string;
  icon: React.ReactNode;
  category: string;
}

const SUGGESTIONS: SuggestionItem[] = [
  {
    category: "Authority",
    title: "Powers under the Act",
    prompt: "What powers does PERA have under the Act?",
    icon: (
      // Scales of justice
      <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
        <path d="M12 3v18" />
        <path d="M5 21h14" />
        <path d="M5 6h14" />
        <path d="M2 12l3-6 3 6c0 1.66-1.34 3-3 3s-3-1.34-3-3z" />
        <path d="M16 12l3-6 3 6c0 1.66-1.34 3-3 3s-3-1.34-3-3z" />
      </svg>
    ),
  },
  {
    category: "Structure",
    title: "PERA Board composition",
    prompt: "What is the composition and structure of the PERA Board?",
    icon: (
      // Building / institution
      <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
        <path d="M3 21h18" />
        <path d="M5 21V8l7-5 7 5v13" />
        <path d="M9 21v-7h6v7" />
      </svg>
    ),
  },
  {
    category: "Enforcement",
    title: "Overall Challan Summary",
    prompt: "Give me the overall challan summary",
    icon: (
      // Clipboard with checkmark
      <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
        <rect x="6" y="4" width="12" height="17" rx="2" />
        <path d="M9 4V3a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v1" />
        <path d="M9 12l2 2 4-4" />
        <path d="M9 17h6" />
      </svg>
    ),
  },
  {
    category: "Operations",
    title: "Lahore Division Inspections",
    prompt: "Give me the inspections summary of Lahore Division",
    icon: (
      // Magnifying glass over chart (inspections analytics)
      <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="11" cy="11" r="7" />
        <path d="M21 21l-4.3-4.3" />
        <path d="M8 13l2-2 2 2 3-3" />
      </svg>
    ),
  },
];

const CAPABILITIES = [
  "Regulations & policy interpretation",
  "Roles, powers, and governance structures",
  "Enforcement procedures and protocols",
  "KPI frameworks and operational performance",
  "Live institutional data via API",
];

interface Props {
  onSendMessage: (text: string) => void;
}

export const WelcomeScreen = memo(function WelcomeScreen({ onSendMessage }: Props) {
  return (
    <div className="welcome-v7">
      {/* ─── Hero ─── */}
      <section className="welcome-v7-hero">
        <div className="welcome-v7-crest" aria-label="PERA Authority Official Emblem">
          {/* L1 — Ambient aurora glow */}
          <span className="crest-aurora" aria-hidden />

          {/* L2 — Slow concentric guide rings */}
          <span className="crest-ring crest-ring-outer" aria-hidden />
          <span className="crest-ring crest-ring-mid" aria-hidden />
          <span className="crest-ring crest-ring-inner" aria-hidden />

          {/* L3 — Chronograph tick corona (SVG, 60 ticks) */}
          <svg className="crest-ticks" viewBox="0 0 200 200" aria-hidden>
            {Array.from({ length: 60 }).map((_, i) => {
              const angle = (i * 360) / 60;
              const isMajor = i % 5 === 0;
              const isQuarter = i % 15 === 0;
              const r1 = 92;
              const r2 = isQuarter ? 82 : isMajor ? 86 : 89;
              const x1 = 100 + r1 * Math.cos((angle - 90) * (Math.PI / 180));
              const y1 = 100 + r1 * Math.sin((angle - 90) * (Math.PI / 180));
              const x2 = 100 + r2 * Math.cos((angle - 90) * (Math.PI / 180));
              const y2 = 100 + r2 * Math.sin((angle - 90) * (Math.PI / 180));
              return (
                <line
                  key={i}
                  x1={x1}
                  y1={y1}
                  x2={x2}
                  y2={y2}
                  stroke={isQuarter ? "#f4d37a" : isMajor ? "#d4a017" : "rgba(212,160,23,0.45)"}
                  strokeWidth={isQuarter ? 2 : isMajor ? 1.4 : 0.7}
                  strokeLinecap="round"
                />
              );
            })}
          </svg>

          {/* L4 — Inscribed text ring (rotates slowly) */}
          <svg className="crest-text-ring" viewBox="0 0 200 200" aria-hidden>
            <defs>
              <path
                id="crestTextPath"
                d="M 100,100 m -78,0 a 78,78 0 1,1 156,0 a 78,78 0 1,1 -156,0"
              />
            </defs>
            <text className="crest-text" textLength="450">
              <textPath href="#crestTextPath" startOffset="0">
                {"• PUNJAB ENFORCEMENT & REGULATORY AUTHORITY CHATBOT "}
              </textPath>
            </text>
          </svg>

          {/* L5 — Compass-point gemstone studs */}
          <span className="crest-stud crest-stud-n" aria-hidden />
          <span className="crest-stud crest-stud-e" aria-hidden />
          <span className="crest-stud crest-stud-s" aria-hidden />
          <span className="crest-stud crest-stud-w" aria-hidden />

          {/* L6 — Conic scanning highlight (slow, dignified) */}
          <span className="crest-scan" aria-hidden />

          {/* L7 — Halo bloom */}
          <span className="crest-halo" aria-hidden />

          {/* L8 — Medal: bezel + emblem + glass refraction */}
          <span className="crest-medal">
            <span className="crest-bezel-outer" aria-hidden />
            <span className="crest-bezel-inner" aria-hidden />
            <Image
              src="/Authority_Logo.png"
              alt="PERA Emblem"
              width={104}
              height={104}
              priority
              className="crest-image"
            />
            <span className="crest-glass" aria-hidden />
          </span>
        </div>


        {/* Premium typographic title block */}
        <div className="title-block">
          {/* Top flourish */}
          <div className="title-flourish title-flourish-top" aria-hidden>
            <span className="title-flourish-line" />
            <svg className="title-flourish-jewel" width="14" height="14" viewBox="0 0 14 14">
              <defs>
                <linearGradient id="jewelGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#fff5d6" />
                  <stop offset="50%" stopColor="#f4d37a" />
                  <stop offset="100%" stopColor="#b8860b" />
                </linearGradient>
              </defs>
              <path d="M7 0 L14 7 L7 14 L0 7 Z" fill="url(#jewelGrad)" />
              <path d="M7 2 L12 7 L7 12 L2 7 Z" fill="none" stroke="rgba(255,255,255,0.35)" strokeWidth="0.6" />
            </svg>
            <span className="title-flourish-line" />
          </div>

          {/* Display title — PERA as hero word, suffix as restrained caps */}
          <h1 className="title-main">
            <span className="title-pera">
              {"PERA".split("").map((c, i) => (
                <span
                  key={i}
                  className="title-pera-char"
                  style={{ animationDelay: `${i * 80 + 200}ms` }}
                >
                  {c}
                </span>
              ))}
            </span>
            <span className="title-rule">
              <span className="title-rule-line" />
              <span className="title-rule-text">AUTHORITY · CHATBOT</span>
              <span className="title-rule-line" />
            </span>
          </h1>

          {/* Mission statement — restrained institutional treatment */}
          <div className="mission" aria-label="Mission statement">
            <div className="mission-eyebrow">
              <span className="mission-eyebrow-dot" />
              <span className="mission-eyebrow-text">Purpose</span>
            </div>
            <p className="mission-text">
              An official intelligence assistant that provides clear operational
              insights and supports authorities in effective decision-making.
            </p>
          </div>

          {/* Bottom flourish — mirror of top */}
          <div className="title-flourish title-flourish-bottom" aria-hidden>
            <span className="title-flourish-line" />
            <svg className="title-flourish-jewel" width="10" height="10" viewBox="0 0 14 14">
              <defs>
                <linearGradient id="jewelGradBottom" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#fff5d6" />
                  <stop offset="100%" stopColor="#b8860b" />
                </linearGradient>
              </defs>
              <circle cx="7" cy="7" r="3" fill="url(#jewelGradBottom)" />
            </svg>
            <span className="title-flourish-line" />
          </div>
        </div>

        {/* Premium credentials panel — three data cells with Roman indices */}
        <div className="meta-panel" role="group" aria-label="System credentials">
          <div className="meta-panel-rim" aria-hidden />

          {/* I. Authorisation */}
          <div className="meta-col">
            <div className="meta-col-head">
              <span className="meta-col-numeral">I</span>
              <span className="meta-col-label">Authorised Use</span>
            </div>
            <div className="meta-col-body">
              <span className="meta-col-icon" aria-hidden>
                <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M12 2 L4 6 V12 C4 17 7.5 21 12 22 C16.5 21 20 17 20 12 V6 Z" />
                  <path d="M9 12 L11 14 L15 10" />
                </svg>
              </span>
              <span className="meta-col-value">
                Internal Authority
                <span className="meta-col-status">
                  <span className="meta-status-dot" />
                  Active
                </span>
              </span>
            </div>
          </div>

          <span className="meta-divider" aria-hidden />

          {/* II. Scope */}
          <div className="meta-col">
            <div className="meta-col-head">
              <span className="meta-col-numeral">II</span>
              <span className="meta-col-label">Knowledge Scope</span>
            </div>
            <div className="meta-col-body">
              <span className="meta-col-icon" aria-hidden>
                <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M4 4 v15 a1 1 0 0 0 1 1 h14" />
                  <path d="M8 4 h11 a1 1 0 0 1 1 1 v13" />
                  <line x1="8" y1="9" x2="16" y2="9" />
                  <line x1="8" y1="13" x2="14" y2="13" />
                </svg>
              </span>
              <span className="meta-col-value">
                Statutes &amp; Live Operations
                <span className="meta-col-status">
                  Acts · Rules · KPIs
                </span>
              </span>
            </div>
          </div>

          <span className="meta-divider" aria-hidden />

          {/* III. Data currency */}
          <div className="meta-col">
            <div className="meta-col-head">
              <span className="meta-col-numeral">III</span>
              <span className="meta-col-label">Data Currency</span>
            </div>
            <div className="meta-col-body">
              <span className="meta-col-icon meta-col-icon-spin" aria-hidden>
                <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21 12 a9 9 0 0 1 -15.4 6.4 L3 16" />
                  <path d="M3 12 a9 9 0 0 1 15.4 -6.4 L21 8" />
                  <path d="M21 3 V8 H16" />
                  <path d="M3 21 V16 H8" />
                </svg>
              </span>
              <span className="meta-col-value">
                Real-time PERA Sync
                <span className="meta-col-status">
                  <span className="meta-status-dot meta-status-dot-amber" />
                  Auto-refresh
                </span>
              </span>
            </div>
          </div>
        </div>
      </section>

      {/* ─── Suggestions card ─── */}
      <section className="welcome-v7-card">
        <div className="welcome-v7-card-head">
          <h2 className="welcome-v7-card-title">How may I help today?</h2>
          <span className="welcome-v7-card-sub">
            Tap a topic to begin, or type your own question below.
          </span>
        </div>

        <div className="welcome-v7-grid">
          {SUGGESTIONS.map((s) => (
            <button
              key={s.title}
              type="button"
              onClick={() => onSendMessage(s.prompt)}
              className="welcome-v7-suggestion"
            >
              <span className="welcome-v7-suggestion-icon">{s.icon}</span>
              <span className="welcome-v7-suggestion-body">
                <span className="welcome-v7-suggestion-cat">{s.category}</span>
                <span className="welcome-v7-suggestion-title">{s.title}</span>
              </span>
              <span className="welcome-v7-suggestion-arrow" aria-hidden>
                <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M5 12h14" />
                  <path d="M13 5l7 7-7 7" />
                </svg>
              </span>
            </button>
          ))}
        </div>
      </section>

      {/* ─── Capabilities (compact strip) ─── */}
      <section className="welcome-v7-capabilities" aria-label="Assistant capabilities">
        <div className="welcome-v7-cap-head">
          <span className="welcome-v7-cap-eyebrow">Capabilities</span>
          <span className="welcome-v7-cap-rule" />
        </div>
        <ul className="welcome-v7-cap-list">
          {CAPABILITIES.map((cap) => (
            <li key={cap}>
              <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
                <polyline points="20 6 9 17 4 12" />
              </svg>
              <span>{cap}</span>
            </li>
          ))}
        </ul>
      </section>

      {/* ─── Footer notice ─── */}
      <p className="welcome-v7-footer-note">
        Responses are generated from official PERA sources. Use as informational guidance,
        not as a substitute for formal legal interpretation.
      </p>
    </div>
  );
});
