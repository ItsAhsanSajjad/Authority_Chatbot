"use client";

import Image from "next/image";
import { memo, useEffect, useState } from "react";

const SUGGESTIONS = [
  {
    query: "What powers does PERA have under the Act?",
    label: "What powers does PERA have?",
    icon: (
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
        <path d="M9 12l2 2 4-4" />
      </svg>
    ),
  },
  {
    query: "What is the composition of PERA Authority?",
    label: "Composition of Authority?",
    icon: (
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
        <polyline points="14 2 14 8 20 8" />
        <line x1="16" y1="13" x2="8" y2="13" />
        <line x1="16" y1="17" x2="8" y2="17" />
        <polyline points="10 9 9 9 8 9" />
      </svg>
    ),
  },
  {
    query: "Overall summary of challans",
    label: "Overall Summary of challans",
    icon: (
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <ellipse cx="12" cy="5" rx="9" ry="3" />
        <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
        <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
      </svg>
    ),
  },
  {
    query: "Overall summary of inspection in Shalimar stations",
    label: "Overall summary of inspection in Shalimar stations",
    icon: (
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
        <circle cx="12" cy="7" r="4" />
      </svg>
    ),
  },
];

const TITLE_WORDS = ["PERA", "Authority", "Assistant"];

interface Props {
  onSendMessage: (text: string) => void;
}

export const WelcomeScreen = memo(function WelcomeScreen({ onSendMessage }: Props) {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    // Small delay so the stagger animations play after mount
    const t = setTimeout(() => setMounted(true), 100);
    return () => clearTimeout(t);
  }, []);

  return (
    <div className={`welcome-container ${mounted ? "welcome-mounted" : ""}`}>
      {/* ── Premium Animated Backdrop ── */}
      <div className="welcome-backdrop">
        {/* Central spotlight */}
        <div className="welcome-spotlight" />
        {/* Aurora blobs */}
        <div className="welcome-aurora welcome-aurora--1" />
        <div className="welcome-aurora welcome-aurora--2" />
        <div className="welcome-aurora welcome-aurora--3" />
        {/* Light rays */}
        <div className="welcome-rays" />
        {/* Subtle grid overlay */}
        <div className="welcome-grid-pattern" />
      </div>

      {/* Decorative sparkle particles */}
      <div className="welcome-particles">
        <span className="particle particle-1" />
        <span className="particle particle-2" />
        <span className="particle particle-3" />
        <span className="particle particle-4" />
        <span className="particle particle-5" />
        <span className="particle particle-6" />
        <span className="particle particle-7" />
        <span className="particle particle-8" />
      </div>

      {/* Logo with animated glow ring */}
      <div className="welcome-logo-glow">
        <div className="welcome-logo-ring" />
        <Image src="/pera_logo.png" alt="PERA" width={52} height={52} priority className="welcome-logo-img" />
      </div>

      {/* Animated title — each word fades in separately */}
      <h1 className="welcome-title">
        {TITLE_WORDS.map((word, i) => (
          <span
            key={word}
            className="welcome-title-word"
            style={{ animationDelay: `${0.3 + i * 0.15}s` }}
          >
            {word}
          </span>
        ))}
      </h1>

      {/* Animated underline accent */}
      <div className="welcome-title-underline" />

      {/* Subtitle with fade-in */}
      <p className="welcome-subtitle">
        Your intelligent guide to Punjab regulatory enforcement
      </p>

      {/* 2x2 Suggestion Grid — cards stagger in */}
      <div className="welcome-grid">
        {SUGGESTIONS.map((s, i) => (
          <button
            key={s.query}
            onClick={() => onSendMessage(s.query)}
            className="welcome-card"
            style={{ animationDelay: `${0.7 + i * 0.1}s` }}
          >
            <div className="welcome-card-row">
              <div className="welcome-card-icon">
                {s.icon}
              </div>
              <span className="welcome-card-text">
                {s.label}
              </span>
            </div>
          </button>
        ))}
      </div>

      {/* Tagline */}
      <p className="welcome-tagline">
        Ask anything about PERA regulations, enforcement, or governance
      </p>
    </div>
  );
});
