"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Image from "next/image";
import { adminLogin, getSession } from "../lib/adminApi";

/**
 * PERA AUTHORITY CHATBOT — Premium Centered Login (v2)
 *
 * Single focused glass card on a starlit aurora background. Geometric
 * grid lines, drifting gold orbs, parallax mouse-follow on the card,
 * and staggered entrance animations. Fully responsive.
 */
export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [stage, setStage] = useState<"booting" | "ready">("booting");
  const [focused, setFocused] = useState<string | null>(null);
  // FOUC gate. The whole page is styled via <style jsx> which only
  // attaches after JS hydrates — the server-sent HTML therefore has no
  // CSS at all and shows a flash of raw text on first paint. Hold the
  // root invisible until React has mounted on the client; the
  // hydrated render contains the styles, so the first visible frame is
  // already styled. Net effect: instant styled paint, no flash.
  const [mounted, setMounted] = useState(false);
  useEffect(() => { setMounted(true); }, []);
  const cardRef = useRef<HTMLDivElement | null>(null);

  // Auth gate + boot animation
  useEffect(() => {
    const s = getSession();
    if (s) {
      router.replace("/admin");
      return;
    }
    const t = window.setTimeout(() => setStage("ready"), 400);
    return () => window.clearTimeout(t);
  }, [router]);

  // Subtle 3D parallax on the card following the cursor
  useEffect(() => {
    const el = cardRef.current;
    if (!el) return;
    function onMove(e: MouseEvent) {
      if (!el) return;
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2;
      const cy = rect.top + rect.height / 2;
      const dx = (e.clientX - cx) / window.innerWidth;
      const dy = (e.clientY - cy) / window.innerHeight;
      el.style.setProperty("--rx", `${(-dy * 4).toFixed(2)}deg`);
      el.style.setProperty("--ry", `${(dx * 5).toFixed(2)}deg`);
    }
    function onLeave() {
      if (!el) return;
      el.style.setProperty("--rx", "0deg");
      el.style.setProperty("--ry", "0deg");
    }
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseleave", onLeave);
    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseleave", onLeave);
    };
  }, []);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (!email || !password) {
      setError("Please enter both email and password.");
      return;
    }
    setSubmitting(true);
    const res = await adminLogin(email.trim(), password);
    setSubmitting(false);
    if (res.ok) {
      router.push("/admin");
    } else {
      if (res.status === 429) {
        setError("Too many attempts. Please wait a minute and try again.");
      } else {
        setError("Invalid email or password.");
      }
    }
  }

  // Floating gold particles
  const particles = useMemo(
    () =>
      Array.from({ length: 28 }).map((_, i) => ({
        id: i,
        left: Math.random() * 100,
        top: Math.random() * 100,
        size: 1.5 + Math.random() * 3,
        delay: Math.random() * 8,
        duration: 14 + Math.random() * 10,
        opacity: 0.35 + Math.random() * 0.5,
      })),
    [],
  );

  return (
    <div
      className={`lp-root stage-${stage}`}
      style={{
        opacity: mounted ? 1 : 0,
        transition: "opacity 80ms ease-out",
      }}
    >
      {/* ─── Background layers ─── */}
      <div className="lp-bg-gradient" aria-hidden />
      <div className="lp-bg-grid" aria-hidden />
      <div className="lp-bg-orb lp-bg-orb-1" aria-hidden />
      <div className="lp-bg-orb lp-bg-orb-2" aria-hidden />
      <div className="lp-bg-orb lp-bg-orb-3" aria-hidden />
      <div className="lp-bg-vignette" aria-hidden />

      {/* Floating particles */}
      <div className="lp-particles" aria-hidden>
        {particles.map((p) => (
          <span
            key={p.id}
            className="lp-particle"
            style={{
              left: `${p.left}%`,
              top: `${p.top}%`,
              width: p.size,
              height: p.size,
              opacity: p.opacity,
              animationDelay: `${p.delay}s`,
              animationDuration: `${p.duration}s`,
            }}
          />
        ))}
      </div>

      {/* ─── Top corner badge ─── */}
      <header className="lp-topbar">
        <div className="lp-topbar-brand">
          <Image
            src="/Authority_Logo.png"
            alt="PERA"
            width={28}
            height={28}
            priority
            className="lp-topbar-logo"
          />
          <div className="lp-topbar-text">
            <span className="lp-topbar-name">PERA Authority Chatbot</span>
            <span className="lp-topbar-org">Government of Punjab</span>
          </div>
        </div>
        <div className="lp-topbar-status">
          <span className="lp-topbar-dot" />
          Secure Access
        </div>
      </header>

      {/* ─── Centered card ─── */}
      <main className="lp-stage">
        <div
          ref={cardRef}
          className="lp-card"
          style={
            {
              "--rx": "0deg",
              "--ry": "0deg",
            } as React.CSSProperties
          }
        >
          {/* Decorative glows / borders */}
          <div className="lp-card-glow" aria-hidden />
          <div className="lp-card-border" aria-hidden />
          <div className="lp-card-scan" aria-hidden />
          <div className="lp-card-corner lp-card-corner-tl" aria-hidden />
          <div className="lp-card-corner lp-card-corner-tr" aria-hidden />
          <div className="lp-card-corner lp-card-corner-bl" aria-hidden />
          <div className="lp-card-corner lp-card-corner-br" aria-hidden />

          <div className="lp-card-body">
            {/* Logo crest at top of card */}
            <div className="lp-crest">
              <span className="lp-crest-ring lp-crest-ring-1" aria-hidden />
              <span className="lp-crest-ring lp-crest-ring-2" aria-hidden />
              <span className="lp-crest-halo" aria-hidden />
              <Image
                src="/Authority_Logo.png"
                alt="PERA Emblem"
                width={68}
                height={68}
                priority
                className="lp-crest-image"
              />
            </div>

            {/* Title block */}
            <div className="lp-title-block">
              <span className="lp-title-eyebrow">
                <span className="lp-title-eyebrow-line" />
                <span className="lp-title-eyebrow-text">ADMIN PORTAL</span>
                <span className="lp-title-eyebrow-line" />
              </span>
              <h1 className="lp-title">
                <span className="lp-title-pera" aria-label="PERA">
                  {"PERA".split("").map((c, i) => (
                    <span
                      key={`p-${i}`}
                      className="lp-title-char"
                      style={{ animationDelay: `${i * 80 + 250}ms` }}
                    >
                      {c}
                    </span>
                  ))}
                </span>
                <span className="lp-title-sub">
                  <span className="lp-title-sub-rule" />
                  <span className="lp-title-sub-text">AUTHORITY · CHATBOT</span>
                  <span className="lp-title-sub-rule" />
                </span>
              </h1>
              <p className="lp-tagline">
                Sign in to manage knowledge sources, monitor data freshness,
                and oversee chatbot operations.
              </p>
            </div>

            {/* Form */}
            <form onSubmit={onSubmit} className="lp-form" noValidate>
              <div
                className={`lp-field ${focused === "email" ? "is-focus" : ""} ${email ? "has-value" : ""}`}
              >
                <label className="lp-field-label" htmlFor="lp-email">
                  Email Address
                </label>
                <div className="lp-field-input-wrap">
                  <span className="lp-field-icon" aria-hidden>
                    <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                      <rect x="2" y="5" width="20" height="14" rx="2" />
                      <path d="M2 7l10 7 10-7" />
                    </svg>
                  </span>
                  <input
                    id="lp-email"
                    type="email"
                    autoComplete="username"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    onFocus={() => setFocused("email")}
                    onBlur={() => setFocused(null)}
                    placeholder="name@pera.gop.pk"
                    className="lp-field-input"
                  />
                </div>
              </div>

              <div
                className={`lp-field ${focused === "password" ? "is-focus" : ""} ${password ? "has-value" : ""}`}
              >
                <label className="lp-field-label" htmlFor="lp-pass">
                  Password
                </label>
                <div className="lp-field-input-wrap">
                  <span className="lp-field-icon" aria-hidden>
                    <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                      <rect x="4" y="11" width="16" height="10" rx="2" />
                      <path d="M8 11V7a4 4 0 0 1 8 0v4" />
                    </svg>
                  </span>
                  <input
                    id="lp-pass"
                    type={showPassword ? "text" : "password"}
                    autoComplete="current-password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    onFocus={() => setFocused("password")}
                    onBlur={() => setFocused(null)}
                    placeholder="Enter your password"
                    className="lp-field-input"
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword((v) => !v)}
                    className="lp-field-toggle"
                    aria-label="Toggle password visibility"
                  >
                    {showPassword ? (
                      <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M17.94 17.94A10.94 10.94 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94" />
                        <path d="M22.54 6.42A18.07 18.07 0 0 0 12 4C5 4 1 12 1 12s4 8 11 8a10.94 10.94 0 0 0 5.94-1.94" />
                        <line x1="1" y1="1" x2="23" y2="23" />
                      </svg>
                    ) : (
                      <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                        <circle cx="12" cy="12" r="3" />
                      </svg>
                    )}
                  </button>
                </div>
              </div>

              {/* Error banner */}
              <div
                className={`lp-error ${error ? "is-open" : ""}`}
                role={error ? "alert" : undefined}
              >
                {error && (
                  <>
                    <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <circle cx="12" cy="12" r="10" />
                      <line x1="12" y1="8" x2="12" y2="12" />
                      <line x1="12" y1="16" x2="12.01" y2="16" />
                    </svg>
                    <span>{error}</span>
                  </>
                )}
              </div>

              <button
                type="submit"
                disabled={submitting}
                className={`lp-submit ${submitting ? "is-loading" : ""}`}
              >
                <span className="lp-submit-shine" aria-hidden />
                <span className="lp-submit-content">
                  {submitting ? (
                    <>
                      <span className="lp-submit-spinner" />
                      Authenticating…
                    </>
                  ) : (
                    <>
                      Continue to Dashboard
                      <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M5 12h14" />
                        <path d="M13 5l7 7-7 7" />
                      </svg>
                    </>
                  )}
                </span>
              </button>
            </form>

            {/* Trust strip */}
            <div className="lp-trust">
              <div className="lp-trust-item">
                <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M12 2 L4 6 V12 C4 17 7.5 21 12 22 C16.5 21 20 17 20 12 V6 Z" />
                  <path d="M9 12 L11 14 L15 10" />
                </svg>
                Encrypted Session
              </div>
              <span className="lp-trust-sep" />
              <div className="lp-trust-item">
                <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <rect x="4" y="11" width="16" height="10" rx="2" />
                  <path d="M8 11V7a4 4 0 0 1 8 0v4" />
                </svg>
                12h Session
              </div>
              <span className="lp-trust-sep" />
              <div className="lp-trust-item">
                <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <circle cx="12" cy="12" r="10" />
                  <path d="M12 2 a10 10 0 0 1 0 20 a10 10 0 0 1 0-20" />
                  <path d="M2 12 h20" />
                </svg>
                Government System
              </div>
            </div>
          </div>
        </div>
      </main>

      {/* ─── Footer ─── */}
      <footer className="lp-footer">
        <span className="lp-footer-text">
          © Government of Punjab · Punjab Enforcement &amp; Regulatory Authority
        </span>
      </footer>

      {/* ─── Styles ─── */}
      <style jsx>{`
        .lp-root {
          position: relative;
          min-height: 100vh;
          width: 100%;
          display: flex;
          flex-direction: column;
          overflow: hidden;
          background: #060914;
          color: #f1f5fb;
          font-family: var(--font-inter, "Inter"), system-ui, -apple-system, sans-serif;
        }

        /* ── Background layers ── */
        .lp-bg-gradient {
          position: fixed;
          inset: 0;
          background:
            radial-gradient(1400px 800px at 20% 10%, rgba(212, 160, 23, 0.08) 0%, transparent 50%),
            radial-gradient(1200px 700px at 80% 90%, rgba(99, 102, 241, 0.06) 0%, transparent 55%),
            linear-gradient(180deg, #060914 0%, #0a0e1c 50%, #050811 100%);
          z-index: 0;
        }
        .lp-bg-grid {
          position: fixed;
          inset: 0;
          background-image:
            linear-gradient(rgba(212, 160, 23, 0.05) 1px, transparent 1px),
            linear-gradient(90deg, rgba(212, 160, 23, 0.05) 1px, transparent 1px);
          background-size: 60px 60px;
          mask-image: radial-gradient(ellipse 80% 60% at 50% 50%, black 30%, transparent 90%);
          -webkit-mask-image: radial-gradient(ellipse 80% 60% at 50% 50%, black 30%, transparent 90%);
          opacity: 0.5;
          z-index: 1;
          animation: lp-grid-drift 60s linear infinite;
        }
        .lp-bg-orb {
          position: fixed;
          border-radius: 50%;
          filter: blur(120px);
          opacity: 0.32;
          z-index: 1;
          pointer-events: none;
          animation: lp-orb-drift 22s ease-in-out infinite alternate;
        }
        .lp-bg-orb-1 {
          width: 600px; height: 600px;
          top: -180px; left: -150px;
          background: radial-gradient(circle, #d4a017, transparent 70%);
        }
        .lp-bg-orb-2 {
          width: 520px; height: 520px;
          bottom: -160px; right: -140px;
          background: radial-gradient(circle, #b8860b, transparent 70%);
          animation-delay: -8s;
        }
        .lp-bg-orb-3 {
          width: 400px; height: 400px;
          top: 40%; left: 50%;
          transform: translate(-50%, -50%);
          background: radial-gradient(circle, rgba(99, 102, 241, 0.4), transparent 70%);
          opacity: 0.18;
          animation-delay: -14s;
        }
        .lp-bg-vignette {
          position: fixed;
          inset: 0;
          background: radial-gradient(ellipse 70% 60% at 50% 50%, transparent 30%, rgba(0, 0, 0, 0.6) 100%);
          pointer-events: none;
          z-index: 2;
        }

        /* ── Particles ── */
        .lp-particles {
          position: fixed;
          inset: 0;
          z-index: 3;
          pointer-events: none;
          overflow: hidden;
        }
        .lp-particle {
          position: absolute;
          background: rgba(244, 211, 122, 0.85);
          border-radius: 50%;
          box-shadow: 0 0 8px rgba(244, 211, 122, 0.7);
          animation: lp-particle-rise linear infinite;
          opacity: 0;
        }

        /* ── Top bar ── */
        .lp-topbar {
          position: relative;
          z-index: 10;
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 18px 28px;
          opacity: 0;
          animation: lp-fade-down 700ms cubic-bezier(0.22, 1, 0.36, 1) 100ms forwards;
        }
        .lp-topbar-brand {
          display: inline-flex;
          align-items: center;
          gap: 10px;
        }
        .lp-topbar-logo {
          border-radius: 8px;
          background: linear-gradient(160deg, rgba(244, 211, 122, 0.18), rgba(212, 160, 23, 0.06));
          padding: 3px;
          border: 1px solid rgba(212, 160, 23, 0.32);
        }
        .lp-topbar-text {
          display: flex;
          flex-direction: column;
          line-height: 1.2;
        }
        .lp-topbar-name {
          font-size: 12.5px;
          font-weight: 700;
          letter-spacing: 0.04em;
          color: #ffffff;
        }
        .lp-topbar-org {
          font-size: 10px;
          font-weight: 600;
          letter-spacing: 0.14em;
          text-transform: uppercase;
          color: rgba(244, 211, 122, 0.7);
        }
        .lp-topbar-status {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          padding: 6px 12px;
          border-radius: 9999px;
          background: linear-gradient(180deg, rgba(74, 222, 128, 0.12), rgba(74, 222, 128, 0.04));
          border: 1px solid rgba(74, 222, 128, 0.32);
          font-size: 11px;
          font-weight: 600;
          letter-spacing: 0.04em;
          color: #86efac;
        }
        .lp-topbar-dot {
          width: 7px;
          height: 7px;
          border-radius: 50%;
          background: #4ade80;
          box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.6);
          animation: lp-pulse 1.8s ease-out infinite;
        }

        /* ── Card stage ── */
        .lp-stage {
          position: relative;
          z-index: 5;
          flex: 1;
          display: flex;
          align-items: center;
          justify-content: center;
          padding: 20px 16px 40px;
          perspective: 1400px;
        }

        /* ── The card ── */
        .lp-card {
          position: relative;
          width: 100%;
          max-width: 460px;
          border-radius: 24px;
          transform: perspective(1400px) rotateX(var(--rx, 0deg)) rotateY(var(--ry, 0deg));
          transform-style: preserve-3d;
          transition: transform 380ms ease-out;
          opacity: 0;
          animation: lp-card-in 900ms cubic-bezier(0.22, 1, 0.36, 1) 200ms forwards;
        }
        /* Scale the card up on tablet+ and desktop+. Without these
           breakpoints a 460px card looks lost on a 1920px monitor —
           the form ends up centred in a sea of empty space and reads
           as a mobile-only screen. */
        @media (min-width: 768px) {
          .lp-card { max-width: 520px; }
        }
        @media (min-width: 1280px) {
          .lp-card { max-width: 580px; }
        }
        @media (min-width: 1600px) {
          .lp-card { max-width: 640px; }
        }
        .lp-card-glow {
          position: absolute;
          inset: -3px;
          border-radius: 26px;
          background: conic-gradient(
            from 180deg at 50% 50%,
            rgba(212, 160, 23, 0.65),
            rgba(212, 160, 23, 0) 30%,
            rgba(212, 160, 23, 0.45) 60%,
            rgba(212, 160, 23, 0) 85%,
            rgba(212, 160, 23, 0.65)
          );
          filter: blur(14px);
          opacity: 0.55;
          z-index: -1;
          animation: lp-conic-spin 9s linear infinite;
        }
        .lp-card-border {
          position: absolute;
          inset: 0;
          border-radius: 24px;
          padding: 1px;
          background: linear-gradient(
            135deg,
            rgba(244, 211, 122, 0.7) 0%,
            rgba(212, 160, 23, 0.12) 40%,
            rgba(244, 211, 122, 0.6) 100%
          );
          -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
          -webkit-mask-composite: xor;
                  mask-composite: exclude;
          pointer-events: none;
        }
        /* Vertical scan-line continuously sweeping the card */
        .lp-card-scan {
          position: absolute;
          inset: 0;
          border-radius: 24px;
          overflow: hidden;
          pointer-events: none;
          z-index: 0;
        }
        .lp-card-scan::before {
          content: "";
          position: absolute;
          top: -50%;
          left: 0;
          right: 0;
          height: 60%;
          background: linear-gradient(
            180deg,
            transparent 0%,
            rgba(244, 211, 122, 0.05) 45%,
            rgba(244, 211, 122, 0.15) 50%,
            rgba(244, 211, 122, 0.05) 55%,
            transparent 100%
          );
          animation: lp-card-scan-sweep 6s ease-in-out infinite;
        }
        @keyframes lp-card-scan-sweep {
          0%, 100% { transform: translateY(-30%); opacity: 0; }
          50%      { transform: translateY(170%); opacity: 1; }
        }
        /* Decorative corner brackets */
        .lp-card-corner {
          position: absolute;
          width: 20px;
          height: 20px;
          border: 1.5px solid rgba(244, 211, 122, 0.55);
          pointer-events: none;
        }
        .lp-card-corner-tl { top: 12px;    left: 12px;    border-right: 0; border-bottom: 0; border-radius: 6px 0 0 0; }
        .lp-card-corner-tr { top: 12px;    right: 12px;   border-left:  0; border-bottom: 0; border-radius: 0 6px 0 0; }
        .lp-card-corner-bl { bottom: 12px; left: 12px;    border-right: 0; border-top:    0; border-radius: 0 0 0 6px; }
        .lp-card-corner-br { bottom: 12px; right: 12px;   border-left:  0; border-top:    0; border-radius: 0 0 6px 0; }

        .lp-card-body {
          position: relative;
          z-index: 1;
          padding: 36px 36px 28px;
          background: linear-gradient(
            165deg,
            rgba(15, 22, 42, 0.95) 0%,
            rgba(10, 14, 26, 0.95) 100%
          );
          border-radius: 24px;
          backdrop-filter: blur(18px) saturate(150%);
          box-shadow:
            0 32px 64px -22px rgba(0, 0, 0, 0.7),
            0 8px 24px -8px rgba(212, 160, 23, 0.18);
        }
        @media (max-width: 480px) {
          .lp-card-body { padding: 28px 22px 22px; }
        }
        @media (min-width: 1280px) {
          .lp-card-body { padding: 48px 52px 36px; }
        }
        @media (min-width: 1600px) {
          .lp-card-body { padding: 56px 64px 44px; }
        }

        /* ── Crest ── */
        .lp-crest {
          position: relative;
          width: 92px;
          height: 92px;
          margin: 0 auto 18px;
          border-radius: 50%;
          display: flex;
          align-items: center;
          justify-content: center;
          isolation: isolate;
        }
        @media (min-width: 1280px) {
          .lp-crest { width: 112px; height: 112px; margin-bottom: 22px; }
        }
        @media (min-width: 1600px) {
          .lp-crest { width: 128px; height: 128px; margin-bottom: 26px; }
        }
        .lp-crest-image {
          position: relative;
          z-index: 5;
          border-radius: 50% !important;
          background: radial-gradient(circle at 35% 25%, rgba(255, 235, 175, 0.4), rgba(184, 134, 11, 0.18));
          padding: 4px;
          box-shadow:
            0 14px 28px -10px rgba(212, 160, 23, 0.45),
            inset 0 0 0 1.5px rgba(244, 211, 122, 0.5);
          animation: lp-crest-float 5s ease-in-out infinite;
        }
        .lp-crest-ring {
          position: absolute;
          border-radius: 50%;
          border: 1px dashed rgba(212, 160, 23, 0.55);
          pointer-events: none;
        }
        .lp-crest-ring-1 { inset: -8px;  animation: lp-spin 18s linear infinite; }
        .lp-crest-ring-2 { inset: -16px; border-style: dotted; opacity: 0.4; animation: lp-spin 36s linear infinite reverse; }
        .lp-crest-halo {
          position: absolute;
          inset: -50%;
          border-radius: 50%;
          background: radial-gradient(circle, rgba(244, 211, 122, 0.3), transparent 60%);
          filter: blur(26px);
          z-index: 0;
          animation: lp-halo-pulse 4s ease-in-out infinite;
        }

        /* ── Title block ── */
        .lp-title-block {
          text-align: center;
          margin-bottom: 26px;
        }
        .lp-title-eyebrow {
          display: inline-flex;
          align-items: center;
          gap: 10px;
          margin-bottom: 12px;
          opacity: 0;
          animation: lp-fade-down 600ms ease 600ms forwards;
        }
        .lp-title-eyebrow-line {
          width: 24px;
          height: 1px;
          background: linear-gradient(90deg, transparent, rgba(244, 211, 122, 0.6));
        }
        .lp-title-eyebrow-line:last-child {
          background: linear-gradient(90deg, rgba(244, 211, 122, 0.6), transparent);
        }
        .lp-title-eyebrow-text {
          font-size: 9.5px;
          font-weight: 700;
          letter-spacing: 0.32em;
          color: #f4d37a;
        }
        .lp-title {
          margin: 0 0 14px;
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 12px;
        }
        /* Hero gold "PERA" word */
        .lp-title-pera {
          display: inline-flex;
          font-size: clamp(2rem, 5.4vw, 2.85rem);
          font-weight: 900;
          letter-spacing: 0.1em;
          line-height: 1;
          text-shadow: 0 0 60px rgba(244, 211, 122, 0.35);
          filter: drop-shadow(0 2px 1px rgba(0, 0, 0, 0.45));
        }
        .lp-title-char {
          display: inline-block;
          opacity: 0;
          transform: translateY(12px);
          filter: blur(6px);
          background: linear-gradient(180deg, #fff5d6 0%, #f4d37a 35%, #d4a017 65%, #b8860b 100%);
          -webkit-background-clip: text;
                  background-clip: text;
          -webkit-text-fill-color: transparent;
                  color: transparent;
          animation: lp-char-in 700ms cubic-bezier(0.22, 1, 0.36, 1) forwards;
        }
        /* Sub-line "AUTHORITY · CHATBOT" with flanking gold rules */
        .lp-title-sub {
          display: inline-flex;
          align-items: center;
          gap: 12px;
          width: 100%;
          max-width: 340px;
          opacity: 0;
          animation: lp-fade-down 700ms ease 750ms forwards;
        }
        .lp-title-sub-rule {
          flex: 1;
          height: 1px;
          background: linear-gradient(
            90deg,
            transparent 0%,
            rgba(244, 211, 122, 0.55) 50%,
            transparent 100%
          );
        }
        .lp-title-sub-text {
          font-size: 11.5px;
          font-weight: 700;
          letter-spacing: 0.36em;
          color: #ffffff;
          text-indent: 0.36em;
          white-space: nowrap;
        }
        .lp-tagline {
          margin: 0 auto;
          font-size: 13px;
          line-height: 1.6;
          color: rgba(241, 245, 251, 0.78);
          max-width: 340px;
          opacity: 0;
          animation: lp-fade-down 600ms ease 1300ms forwards;
        }

        /* ── Form ── */
        .lp-form {
          display: flex;
          flex-direction: column;
          gap: 14px;
          opacity: 0;
          animation: lp-fade-down 700ms ease 900ms forwards;
        }
        .lp-field {
          display: flex;
          flex-direction: column;
          gap: 6px;
        }
        .lp-field-label {
          font-size: 10.5px;
          font-weight: 700;
          letter-spacing: 0.22em;
          text-transform: uppercase;
          color: rgba(244, 211, 122, 0.85);
          transition: color 220ms ease, letter-spacing 220ms ease;
          padding-left: 2px;
        }
        .lp-field.is-focus .lp-field-label {
          color: #f4d37a;
          letter-spacing: 0.26em;
        }
        .lp-field-input-wrap {
          position: relative;
          display: flex;
          align-items: center;
        }
        .lp-field-icon {
          position: absolute;
          left: 14px;
          color: rgba(244, 211, 122, 0.55);
          pointer-events: none;
          transition: color 220ms ease, transform 220ms ease;
          display: inline-flex;
          z-index: 1;
        }
        .lp-field.is-focus .lp-field-icon {
          color: #f4d37a;
          transform: scale(1.08);
        }
        :global(.lp-field-input) {
          width: 100%;
          background: rgba(8, 12, 24, 0.78);
          border: 1.5px solid rgba(244, 211, 122, 0.22);
          border-radius: 12px;
          padding: 14px 14px 14px 44px;
          color: #ffffff;
          font-size: 14px;
          font-weight: 500;
          font-family: inherit;
          letter-spacing: 0.01em;
          transition:
            border-color 220ms ease,
            box-shadow 280ms ease,
            background 220ms ease;
          outline: none;
        }
        :global(.lp-field-input::placeholder) {
          color: rgba(241, 245, 251, 0.3);
        }
        :global(.lp-field-input:focus) {
          border-color: rgba(244, 211, 122, 0.65);
          background: rgba(15, 22, 42, 0.85);
          box-shadow:
            0 0 0 4px rgba(244, 211, 122, 0.12),
            0 10px 22px -10px rgba(244, 211, 122, 0.3);
        }
        /* Force the dark theme even when the browser autofills saved
           credentials (Chromium normally overrides background to a
           pale yellow which broke the visual). */
        :global(.lp-field-input:-webkit-autofill),
        :global(.lp-field-input:-webkit-autofill:hover),
        :global(.lp-field-input:-webkit-autofill:focus),
        :global(.lp-field-input:-webkit-autofill:active) {
          -webkit-text-fill-color: #ffffff !important;
          -webkit-box-shadow: 0 0 0 1000px rgba(8, 12, 24, 0.95) inset !important;
          box-shadow: 0 0 0 1000px rgba(8, 12, 24, 0.95) inset !important;
          caret-color: #ffffff !important;
          transition: background-color 9999s ease-in-out 0s;
        }
        .lp-field-toggle {
          position: absolute;
          right: 8px;
          width: 32px;
          height: 32px;
          border-radius: 8px;
          background: rgba(244, 211, 122, 0.06);
          border: 1px solid rgba(244, 211, 122, 0.16);
          color: rgba(244, 211, 122, 0.8);
          display: inline-flex;
          align-items: center;
          justify-content: center;
          cursor: pointer;
          transition: all 220ms ease;
        }
        .lp-field-toggle:hover {
          background: rgba(244, 211, 122, 0.18);
          border-color: rgba(244, 211, 122, 0.45);
          color: #f4d37a;
          transform: translateY(-1px);
        }

        /* ── Error ── */
        .lp-error {
          max-height: 0;
          opacity: 0;
          overflow: hidden;
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 12px;
          color: #fda4af;
          background: rgba(190, 18, 60, 0.14);
          border: 1px solid rgba(244, 63, 94, 0.3);
          border-radius: 10px;
          padding: 0 12px;
          transition:
            max-height 280ms ease,
            opacity 280ms ease,
            padding 280ms ease;
        }
        .lp-error.is-open {
          max-height: 80px;
          opacity: 1;
          padding: 9px 12px;
          animation: lp-shake 420ms ease;
        }

        /* ── Submit ── */
        .lp-submit {
          position: relative;
          width: 100%;
          padding: 14px;
          margin-top: 4px;
          background: linear-gradient(180deg, #f4d37a 0%, #d4a017 50%, #b8860b 100%);
          color: #1a1307;
          font-weight: 700;
          font-size: 13.5px;
          letter-spacing: 0.04em;
          border: none;
          border-radius: 12px;
          cursor: pointer;
          overflow: hidden;
          box-shadow:
            0 14px 28px -12px rgba(212, 160, 23, 0.7),
            inset 0 1px 0 rgba(255, 255, 255, 0.45);
          transition:
            transform 220ms ease,
            filter 220ms ease,
            box-shadow 280ms ease;
        }
        .lp-submit:hover:not(:disabled) {
          transform: translateY(-1px);
          filter: brightness(1.08);
          box-shadow:
            0 18px 32px -12px rgba(212, 160, 23, 0.75),
            inset 0 1px 0 rgba(255, 255, 255, 0.5);
        }
        .lp-submit:active:not(:disabled) {
          transform: translateY(0);
        }
        .lp-submit:disabled {
          cursor: not-allowed;
          filter: saturate(0.7);
        }
        .lp-submit-content {
          position: relative;
          z-index: 1;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          gap: 8px;
        }
        .lp-submit-shine {
          position: absolute;
          top: 0;
          left: -120%;
          width: 80%;
          height: 100%;
          background: linear-gradient(100deg, transparent, rgba(255, 255, 255, 0.5), transparent);
          transform: skewX(-18deg);
          animation: lp-shine 3.4s ease-in-out infinite;
        }
        .lp-submit.is-loading .lp-submit-shine {
          animation-duration: 1.1s;
        }
        .lp-submit-spinner {
          width: 14px;
          height: 14px;
          border-radius: 50%;
          border: 2px solid rgba(26, 19, 7, 0.25);
          border-top-color: #1a1307;
          animation: lp-spin 700ms linear infinite;
        }

        /* ── Trust strip ── */
        .lp-trust {
          margin-top: 22px;
          padding-top: 18px;
          border-top: 1px dashed rgba(244, 211, 122, 0.18);
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 8px;
          flex-wrap: wrap;
          opacity: 0;
          animation: lp-fade-down 600ms ease 1500ms forwards;
        }
        .lp-trust-item {
          display: inline-flex;
          align-items: center;
          gap: 5px;
          font-size: 10.5px;
          font-weight: 600;
          letter-spacing: 0.04em;
          color: rgba(241, 245, 251, 0.55);
        }
        .lp-trust-item svg { color: rgba(244, 211, 122, 0.65); }
        .lp-trust-sep {
          width: 3px;
          height: 3px;
          border-radius: 50%;
          background: rgba(244, 211, 122, 0.35);
        }
        @media (max-width: 380px) {
          .lp-trust-sep { display: none; }
          .lp-trust { gap: 12px; }
        }

        /* ── Footer ── */
        .lp-footer {
          position: relative;
          z-index: 5;
          padding: 18px 28px;
          text-align: center;
          opacity: 0;
          animation: lp-fade-down 600ms ease 1700ms forwards;
        }
        .lp-footer-text {
          font-size: 10.5px;
          font-weight: 600;
          letter-spacing: 0.12em;
          text-transform: uppercase;
          color: rgba(241, 245, 251, 0.35);
        }

        /* ── Mobile / responsive ── */
        @media (max-width: 640px) {
          .lp-topbar { padding: 14px 16px; }
          .lp-topbar-text { display: none; }
          .lp-bg-grid { background-size: 40px 40px; }
        }
        @media (max-width: 480px) {
          .lp-topbar-status { padding: 4px 9px; font-size: 10px; }
          .lp-stage { padding: 8px 12px 24px; }
        }

        /* ── Keyframes ── */
        @keyframes lp-grid-drift {
          from { background-position: 0 0; }
          to   { background-position: 60px 60px; }
        }
        @keyframes lp-orb-drift {
          from { transform: translate(0, 0); }
          to   { transform: translate(40px, -30px); }
        }
        @keyframes lp-particle-rise {
          0%   { opacity: 0; transform: translateY(30px) scale(0.6); }
          15%  { opacity: 1; }
          85%  { opacity: 1; }
          100% { opacity: 0; transform: translateY(-160px) scale(1); }
        }
        @keyframes lp-fade-down {
          from { opacity: 0; transform: translateY(-8px); }
          to   { opacity: 1; transform: translateY(0); }
        }
        @keyframes lp-card-in {
          from { opacity: 0; transform: perspective(1400px) translateY(20px) rotateX(0) rotateY(0); }
          to   { opacity: 1; transform: perspective(1400px) translateY(0)    rotateX(var(--rx, 0deg)) rotateY(var(--ry, 0deg)); }
        }
        @keyframes lp-conic-spin {
          from { transform: rotate(0deg); }
          to   { transform: rotate(360deg); }
        }
        @keyframes lp-spin {
          from { transform: rotate(0deg); }
          to   { transform: rotate(360deg); }
        }
        @keyframes lp-crest-float {
          0%, 100% { transform: translateY(0) scale(1); }
          50%      { transform: translateY(-3px) scale(1.02); }
        }
        @keyframes lp-halo-pulse {
          0%, 100% { opacity: 0.5; transform: scale(1); }
          50%      { opacity: 0.9; transform: scale(1.08); }
        }
        @keyframes lp-pulse {
          0%   { box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.6); }
          70%  { box-shadow: 0 0 0 7px rgba(74, 222, 128, 0); }
          100% { box-shadow: 0 0 0 0 rgba(74, 222, 128, 0); }
        }
        @keyframes lp-char-in {
          to { opacity: 1; transform: translateY(0); filter: blur(0); }
        }
        @keyframes lp-shine {
          0%       { left: -120%; }
          60%, 100% { left: 140%; }
        }
        @keyframes lp-shake {
          0%, 100% { transform: translateX(0); }
          20%, 60% { transform: translateX(-4px); }
          40%, 80% { transform: translateX(4px); }
        }

        /* Reduced motion */
        @media (prefers-reduced-motion: reduce) {
          *, *::before, *::after {
            animation-duration: 0.01ms !important;
            animation-iteration-count: 1 !important;
            transition-duration: 0.01ms !important;
          }
        }
      `}</style>
    </div>
  );
}
