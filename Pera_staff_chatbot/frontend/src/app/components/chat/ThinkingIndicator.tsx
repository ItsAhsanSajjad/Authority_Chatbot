"use client";

import Image from "next/image";
import { memo, useState, useEffect } from "react";

const PHASES = [
  { text: "Scanning knowledge base", icon: "scan" },
  { text: "Cross-referencing documents", icon: "cross" },
  { text: "Analyzing relevant data", icon: "data" },
  { text: "Synthesizing insights", icon: "synth" },
  { text: "Composing answer", icon: "compose" },
] as const;

const PHASE_ICONS: Record<string, React.ReactNode> = {
  scan: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
    </svg>
  ),
  cross: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <polyline points="14 2 14 8 20 8" />
    </svg>
  ),
  data: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" /><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
    </svg>
  ),
  synth: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" />
    </svg>
  ),
  compose: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <path d="M12 19l7-7 3 3-7 7-3-3z" /><path d="M18 13l-1.5-7.5L2 2l3.5 14.5L13 18l5-5z" /><path d="M2 2l7.586 7.586" />
    </svg>
  ),
};

export const ThinkingIndicator = memo(function ThinkingIndicator() {
  const [phase, setPhase] = useState(0);
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    const t = setInterval(() => setPhase((p) => (p + 1) % PHASES.length), 2800);
    return () => clearInterval(t);
  }, []);

  // Smooth progress animation
  useEffect(() => {
    setProgress(0);
    const t = setInterval(() => {
      setProgress((p) => Math.min(p + 1.0, 100));
    }, 28);
    return () => clearInterval(t);
  }, [phase]);

  const current = PHASES[phase];

  return (
    <div className="flex gap-2.5 animate-fade-in">
      {/* Avatar with pulse ring */}
      <div className="flex-shrink-0 mt-1">
        <div className="thinking-avatar">
          <Image src="/pera_logo.png" alt="" width={20} height={20} className="rounded-md" />
          <span className="thinking-avatar-ring" />
          <span className="thinking-avatar-ring thinking-avatar-ring--2" />
        </div>
      </div>

      {/* Premium thinking bubble */}
      <div className="thinking-bubble">
        {/* Animated gradient border overlay */}
        <div className="thinking-border-glow" />

        {/* Shimmer sweep overlay */}
        <div className="thinking-shimmer" />

        {/* Floating particles inside */}
        <div className="thinking-particles">
          {Array.from({ length: 6 }).map((_, i) => (
            <span key={i} className="thinking-spark" style={{
              left: `${10 + i * 16}%`,
              animationDelay: `${i * 0.5}s`,
              animationDuration: `${2 + (i % 3) * 0.6}s`,
            }} />
          ))}
        </div>

        {/* Content */}
        <div className="thinking-content">
          {/* Animated orb dots */}
          <div className="thinking-orbs">
            <span className="thinking-orb" />
            <span className="thinking-orb" />
            <span className="thinking-orb" />
          </div>

          {/* Phase text with icon */}
          <div className="thinking-phase">
            <span className="thinking-phase-icon" key={`icon-${phase}`}>
              {PHASE_ICONS[current.icon]}
            </span>
            <span className="thinking-phase-text" key={`text-${phase}`}>
              {current.text}
            </span>
          </div>

          {/* Phase counter */}
          <span className="thinking-counter">
            {phase + 1}/{PHASES.length}
          </span>
        </div>

        {/* Progress bar */}
        <div className="thinking-progress-track">
          <div
            className="thinking-progress-fill"
            style={{ width: `${progress}%` }}
          />
          {/* Glowing head on progress bar */}
          <div
            className="thinking-progress-head"
            style={{ left: `${progress}%` }}
          />
        </div>
      </div>
    </div>
  );
});
