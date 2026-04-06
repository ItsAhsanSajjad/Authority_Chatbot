"use client";

import Image from "next/image";
import { memo } from "react";

/* ═══════════════════════════════════════════════════════════
   PERA AI — Flagship Institutional Landing Experience
   Regulatory Intelligence Platform
   ═══════════════════════════════════════════════════════════ */

// ─── Topic Explorer Data ───

const TOPIC_GROUPS = [
  {
    theme: "Governance",
    icon: "⚖️",
    prompts: [
      "What powers does PERA have under the Act?",
      "What is the structure of the PERA Board?",
    ],
  },
  {
    theme: "Roles & Responsibilities",
    icon: "👤",
    prompts: [
      "What are the responsibilities of the Chief Technology Officer?",
      "What are the duties of enforcement officers?",
    ],
  },
  {
    theme: "Enforcement",
    icon: "📋",
    prompts: [
      "How is an EPO issued?",
      "What enforcement powers does PERA have?",
    ],
  },
  {
    theme: "Performance",
    icon: "📊",
    prompts: [
      "What KPIs measure PERA operational success?",
      "How does PERA measure operational efficiency?",
    ],
  },
  {
    theme: "Service Delivery",
    icon: "🏛️",
    prompts: [
      "How is responsiveness to public complaints evaluated?",
      "What service delivery benchmarks apply to PERA?",
    ],
  },
  {
    theme: "Compliance",
    icon: "🔒",
    prompts: [
      "What professional conduct standards apply to PERA employees?",
      "What are the confidentiality obligations for staff?",
    ],
  },
  {
    theme: "Learning & Development",
    icon: "📚",
    prompts: [
      "What training expectations exist for PERA staff?",
      "How is capacity building measured in the KPI framework?",
    ],
  },
];



// ─── Capabilities ───

const CAPABILITIES = [
  "Guidance on PERA regulations and powers",
  "Interpretation of governance structures and roles",
  "Explanation of enforcement procedures",
  "Insights into institutional performance frameworks",
  "Clarification of policies, rules, and operational standards",
];

// ─── Reference Materials ───

const REFERENCE_CATEGORIES = [
  { label: "Punjab Enforcement & Regulation Act", icon: "📜" },
  { label: "PERA Service Rules", icon: "📋" },
  { label: "Governance Documentation", icon: "🏛️" },
  { label: "Institutional Policies", icon: "📁" },
  { label: "KPI Frameworks", icon: "📊" },
  { label: "Official Notifications", icon: "📄" },
];

// ─── Regulatory Intelligence ───

const INTEL_ITEMS = [
  {
    icon: "📢",
    title: "Latest Notifications",
    desc: "Recent PERA notices and regulatory updates",
    status: "Active",
    statusColor: "#34d399",
    accent: "#c9943e",
    meta: "Regulatory Notices",
  },
  {
    icon: "📝",
    title: "Policy Updates",
    desc: "Recently updated governance documents",
    status: "Current",
    statusColor: "#60a5fa",
    accent: "#60a5fa",
    meta: "Governance Documents",
  },
  {
    icon: "🔍",
    title: "Enforcement Insights",
    desc: "Current enforcement focus areas and regulatory priorities",
    status: "Monitoring",
    statusColor: "#a78bfa",
    accent: "#a78bfa",
    meta: "Enforcement Data",
  },
  {
    icon: "📂",
    title: "Knowledge Base",
    desc: "Official documents indexed in the assistant",
    status: "Operational",
    statusColor: "#34d399",
    accent: "#34d399",
    meta: "Document Repository",
  },
];

// ─── "What you can ask" examples ───

const EXAMPLE_QUERIES = [
  "What are PERA enforcement powers?",
  "What KPIs measure PERA performance?",
  "How is an EPO issued?",
  "What is the structure of the PERA Board?",
];

interface Props {
  onSendMessage: (text: string) => void;
}

export const WelcomeScreen = memo(function WelcomeScreen({ onSendMessage }: Props) {
  return (
    <div className="inst-landing">

      {/* ═══ 1. OFFICIAL AUTHORITY HEADER ═══ */}
      <section className="inst-hero">
        <div className="inst-hero-inner">
          <div className="inst-authority-block">
            <p className="inst-super-title">Government of Punjab</p>
            <p className="inst-org-name">Punjab Enforcement & Regulatory Authority</p>
          </div>

          <div className="inst-logo-row">
            <div className="inst-logo-wrap">
              <Image src="/pera_logo.png" alt="PERA Emblem" width={64} height={64} priority />
            </div>
          </div>

          <h1 className="inst-main-title">PERA AI Assistant</h1>
          <p className="inst-subtitle">
            Official informational assistant for regulations, governance, enforcement guidance, and institutional performance.
          </p>

          <div className="inst-meta-strip">
            <span className="inst-meta-item">
              <span className="inst-meta-dot" /> Official Government System
            </span>
            <span className="inst-meta-sep">|</span>
            <span className="inst-meta-item">Version 2.0</span>
            <span className="inst-meta-sep">|</span>
            <span className="inst-meta-item">Knowledge Base: Current</span>
          </div>
        </div>
      </section>

      {/* ═══ 2. FORMAL INFORMATION NOTICE ═══ */}
      <section className="inst-notice">
        <div className="inst-notice-bar" />
        <p>
          This assistant provides informational guidance derived from PERA regulations, policies, governance materials, and performance frameworks. Responses are generated from official reference sources to support understanding of PERA operations and institutional standards.
        </p>
      </section>

      {/* ═══ 3. WHAT YOU CAN ASK ═══ */}
      <section className="inst-ask-helper">
        <h2 className="inst-ask-title">What you can ask</h2>
        <div className="inst-ask-grid">
          {EXAMPLE_QUERIES.map((q) => (
            <button key={q} onClick={() => onSendMessage(q)} className="inst-ask-chip">
              {q}
            </button>
          ))}
        </div>
      </section>

      <hr className="inst-divider" />

      {/* ═══ 4. REGULATORY INTELLIGENCE ═══ */}
      <section className="inst-section">
        <div className="inst-intel-header">
          <h2 className="inst-section-heading">Regulatory Intelligence</h2>
          <span className="inst-intel-badge">Live Overview</span>
        </div>
        <div className="inst-intel-grid">
          {INTEL_ITEMS.map((item) => (
            <div key={item.title} className="inst-intel-card" style={{ '--accent': item.accent } as React.CSSProperties}>
              <div className="inst-intel-card-top">
                <div className="inst-intel-icon-wrap">
                  <span className="inst-intel-icon">{item.icon}</span>
                </div>
                <div className="inst-intel-status">
                  <span className="inst-intel-status-dot" style={{ background: item.statusColor }} />
                  <span className="inst-intel-status-text">{item.status}</span>
                </div>
              </div>
              <h3 className="inst-intel-title">{item.title}</h3>
              <p className="inst-intel-desc">{item.desc}</p>
              <div className="inst-intel-meta">
                <span className="inst-intel-meta-label">{item.meta}</span>
              </div>
            </div>
          ))}
        </div>
      </section>

      <hr className="inst-divider" />

      {/* ═══ 5. EXPLORE PERA TOPICS ═══ */}
      <section className="inst-section">
        <h2 className="inst-section-heading">Explore PERA Topics</h2>
        <div className="inst-topics-grid">
          {TOPIC_GROUPS.map((group) => (
            <div key={group.theme} className="inst-topic-group">
              <div className="inst-topic-header">
                <span className="inst-topic-icon">{group.icon}</span>
                <h3 className="inst-topic-theme">{group.theme}</h3>
              </div>
              <div className="inst-topic-prompts">
                {group.prompts.map((prompt) => (
                  <button
                    key={prompt}
                    onClick={() => onSendMessage(prompt)}
                    className="inst-topic-btn"
                  >
                    <span className="inst-topic-arrow">→</span>
                    {prompt}
                  </button>
                ))}
              </div>
            </div>
          ))}
        </div>
      </section>



      {/* ═══ 7. CAPABILITIES ═══ */}
      <section className="inst-section">
        <h2 className="inst-section-heading">Capabilities of the Assistant</h2>
        <ul className="inst-capabilities">
          {CAPABILITIES.map((cap) => (
            <li key={cap}>
              <span className="inst-cap-check">✓</span>
              {cap}
            </li>
          ))}
        </ul>
      </section>

      <hr className="inst-divider" />

      {/* ═══ 8. REFERENCE MATERIALS ═══ */}
      <section className="inst-section">
        <h2 className="inst-section-heading">Official Reference Materials</h2>
        <p className="inst-ref-intro">
          Responses generated by this assistant are derived from the following official materials.
        </p>
        <div className="inst-ref-grid">
          {REFERENCE_CATEGORIES.map((cat) => (
            <div key={cat.label} className="inst-ref-card">
              <span className="inst-ref-icon">{cat.icon}</span>
              <span className="inst-ref-label">{cat.label}</span>
            </div>
          ))}
        </div>
      </section>

      {/* ═══ 9. FOOTER ═══ */}
      <footer className="inst-footer">
        <div className="inst-footer-links">
          <span>Privacy Policy</span>
          <span className="inst-footer-sep">·</span>
          <span>Terms of Use</span>
          <span className="inst-footer-sep">·</span>
          <span>Accessibility Statement</span>
          <span className="inst-footer-sep">·</span>
          <span>Data Handling Notice</span>
        </div>
        <p className="inst-footer-highlight">
          PERA AI Assistant — Punjab Enforcement & Regulatory Authority
        </p>
      </footer>
    </div>
  );
});
