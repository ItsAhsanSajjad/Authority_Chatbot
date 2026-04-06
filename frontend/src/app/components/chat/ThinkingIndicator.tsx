"use client";

import Image from "next/image";
import { memo, useState, useEffect } from "react";

const PHASES = [
  { icon: "🔎", text: "Scanning knowledge base..." },
  { icon: "📑", text: "Cross-referencing documents..." },
  { icon: "⚙️", text: "Synthesizing insights..." },
  { icon: "✍️", text: "Composing answer..." },
];

export const ThinkingIndicator = memo(function ThinkingIndicator() {
  const [phase, setPhase] = useState(0);

  useEffect(() => {
    const t = setInterval(() => setPhase((p) => (p + 1) % PHASES.length), 2200);
    return () => clearInterval(t);
  }, []);

  return (
    <div className="flex gap-2.5 thinking-container">
      <div className="flex-shrink-0 mt-1">
        <div
          className="w-8 h-8 rounded-xl overflow-hidden flex items-center justify-center"
          style={{ background: "var(--accent-soft)" }}
        >
          <Image src="/pera_logo.png" alt="" width={20} height={20} className="rounded-md" />
        </div>
      </div>
      <div className="bot-bubble px-5 py-4 max-w-xs">
        <div className="flex items-center gap-3">
          <div className="thinking-wave-wrapper">
            <div className="thinking-bar" />
            <div className="thinking-bar" />
            <div className="thinking-bar" />
            <div className="thinking-bar" />
            <div className="thinking-bar" />
          </div>
          <p
            className="text-sm font-medium thinking-text"
            style={{ color: "var(--text-primary)" }}
          >
            {PHASES[phase].text}
          </p>
        </div>
      </div>
    </div>
  );
});
