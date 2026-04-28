"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Image from "next/image";
import { adminLogin, getSession } from "../lib/adminApi";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [stage, setStage] = useState<"splash" | "ready">("splash");
  const [focused, setFocused] = useState<string | null>(null);
  const cardRef = useRef<HTMLDivElement | null>(null);

  // Splash -> ready transition on every mount (i.e. every refresh).
  useEffect(() => {
    const s = getSession();
    if (s) {
      router.replace("/admin");
      return;
    }
    const t = window.setTimeout(() => setStage("ready"), 1100);
    return () => window.clearTimeout(t);
  }, [router]);

  // Subtle 3D parallax tilt on the sign-in card.
  useEffect(() => {
    const el = cardRef.current;
    if (!el) return;
    function onMove(e: MouseEvent) {
      if (!el) return;
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2;
      const cy = rect.top + rect.height / 2;
      const dx = (e.clientX - cx) / rect.width;
      const dy = (e.clientY - cy) / rect.height;
      el.style.setProperty("--rx", `${(-dy * 5).toFixed(2)}deg`);
      el.style.setProperty("--ry", `${(dx * 6).toFixed(2)}deg`);
      el.style.setProperty("--mx", `${(dx * 100).toFixed(1)}%`);
      el.style.setProperty("--my", `${(dy * 100).toFixed(1)}%`);
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

  const particles = useMemo(
    () =>
      Array.from({ length: 18 }).map((_, i) => ({
        id: i,
        left: Math.random() * 100,
        top: Math.random() * 100,
        size: 2 + Math.random() * 4,
        delay: Math.random() * 6,
        duration: 9 + Math.random() * 8,
      })),
    [],
  );

  return (
    <div
      className={`login-root stage-${stage}`}
      // Critical paint styles inlined so there's no FOUC before
      // styled-jsx hydrates. The styled-jsx block below still owns
      // all animation / layered effect declarations; these are just
      // the essentials needed to make the initial SSR render look
      // correct.
      style={{
        position: "relative",
        minHeight: "100vh",
        width: "100%",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "2.5rem 1.5rem",
        overflow: "hidden",
        background:
          "radial-gradient(1200px 600px at 20% 30%, rgba(35,43,74,0.55) 0%, rgba(11,16,32,0.9) 55%, #0b1020 100%)",
      }}
    >
      {/* Splash overlay — shows on every refresh */}
      <div
        className="splash"
        aria-hidden={stage === "ready"}
        style={{
          position: "fixed",
          inset: 0,
          zIndex: 50,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          pointerEvents: "none",
        }}
      >
        <div className="splash-inner">
          <div className="splash-logo">
            <span className="splash-rune splash-rune-1" />
            <span className="splash-rune splash-rune-2" />
            <span className="splash-rune splash-rune-3" />
            <Image
              src="/Authority_Logo.png"
              alt=""
              width={120}
              height={120}
              priority
              className="splash-mark"
            />
          </div>
          <div className="splash-text splash-text-stack">
            <div className="splash-line">
              {"PERA AUTHORITY".split("").map((c, i) => (
                <span
                  key={`l1-${i}`}
                  className="splash-letter splash-letter-gold"
                  style={{ animationDelay: `${i * 50}ms` }}
                >
                  {c === " " ? " " : c}
                </span>
              ))}
            </div>
            <div className="splash-line">
              {"CHATBOT".split("").map((c, i) => (
                <span
                  key={`l2-${i}`}
                  className="splash-letter"
                  style={{ animationDelay: `${800 + i * 60}ms` }}
                >
                  {c}
                </span>
              ))}
            </div>
          </div>
          <div className="splash-bar" />
        </div>
        <div
          className="splash-reveal-left"
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            width: "50%",
            height: "100%",
            background:
              "linear-gradient(160deg, #0a0f1f 0%, #0b1020 55%, #050814 100%)",
            zIndex: 1,
          }}
        />
        <div
          className="splash-reveal-right"
          style={{
            position: "absolute",
            top: 0,
            right: 0,
            width: "50%",
            height: "100%",
            background:
              "linear-gradient(160deg, #0a0f1f 0%, #0b1020 55%, #050814 100%)",
            zIndex: 1,
          }}
        />
      </div>

      {/* Ambient animated background */}
      <div className="bg-grid" aria-hidden="true" />
      <div className="bg-orb bg-orb-1" aria-hidden="true" />
      <div className="bg-orb bg-orb-2" aria-hidden="true" />
      <div className="bg-orb bg-orb-3" aria-hidden="true" />
      <div className="bg-vignette" aria-hidden="true" />

      {/* Floating particles */}
      <div className="particles" aria-hidden="true">
        {particles.map((p) => (
          <span
            key={p.id}
            className="particle"
            style={{
              left: `${p.left}%`,
              top: `${p.top}%`,
              width: p.size,
              height: p.size,
              animationDelay: `${p.delay}s`,
              animationDuration: `${p.duration}s`,
            }}
          />
        ))}
      </div>

      <div
        className={`login-shell ${stage === "ready" ? "is-ready" : ""}`}
        style={{
          position: "relative",
          zIndex: 1,
          display: "grid",
          width: "100%",
          maxWidth: "1100px",
          alignItems: "center",
          opacity: stage === "ready" ? 1 : 0,
        }}
      >
        {/* Branding */}
        <div className="brand-panel">
          <div className="logo-wrap">
            <div className="logo-ring logo-ring-1" aria-hidden="true" />
            <div className="logo-ring logo-ring-2" aria-hidden="true" />
            <div className="logo-ring logo-ring-3" aria-hidden="true" />
            <div className="logo-halo" aria-hidden="true" />
            <div className="relative w-56 h-56 md:w-72 md:h-72 logo-float">
              <Image
                src="/Authority_Logo.png"
                alt="PERA AUTHORITY CHATBOT"
                fill
                priority
                sizes="(max-width: 768px) 224px, 288px"
                className="object-contain drop-shadow-[0_0_50px_rgba(212,160,23,0.4)]"
              />
            </div>
          </div>

          <h1 className="brand-title brand-title-stack">
            <span className="brand-word brand-word-accent brand-line">
              {"PERA AUTHORITY".split("").map((c, i) => (
                <span
                  key={`bt1-${i}`}
                  className="brand-char brand-char-gold"
                  style={{ animationDelay: `${i * 50 + 900}ms` }}
                >
                  {c === " " ? " " : c}
                </span>
              ))}
            </span>
            <span className="brand-word brand-line">
              {"CHATBOT".split("").map((c, i) => (
                <span
                  key={`bt2-${i}`}
                  className="brand-char"
                  style={{ animationDelay: `${i * 60 + 1700}ms` }}
                >
                  {c}
                </span>
              ))}
            </span>
          </h1>
          <p className="brand-subtitle">
            Punjab Enforcement &amp; Regulatory Authority
          </p>
          <div className="brand-divider">
            <span className="brand-divider-dot" />
          </div>
        </div>

        {/* Sign-in card */}
        <div className="card-outer">
          <div
            ref={cardRef}
            className="card"
            style={
              {
                "--rx": "0deg",
                "--ry": "0deg",
                "--mx": "50%",
                "--my": "50%",
              } as React.CSSProperties
            }
          >
            <div className="card-glow" aria-hidden="true" />
            <div className="card-sheen" aria-hidden="true" />
            <div className="card-border" aria-hidden="true" />
            <div className="card-body">
              <div className="card-header">
                <div className="card-badge">
                  <span className="card-badge-dot" />
                  ADMIN ACCESS
                </div>
                <h2 className="card-title">Sign In</h2>
                <p className="card-hint">
                  Enter your administrator credentials to continue
                </p>
              </div>

              <form onSubmit={onSubmit} className="space-y-5" noValidate>
                <div className={`field ${focused === "email" ? "is-focus" : ""}`}>
                  <label className="field-label">EMAIL</label>
                  <div className="field-input-wrap">
                    <input
                      type="email"
                      autoComplete="username"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      onFocus={() => setFocused("email")}
                      onBlur={() => setFocused(null)}
                      placeholder="name@pera.gop.pk"
                      className="field-input"
                    />
                    <span className="field-underline" />
                  </div>
                </div>

                <div
                  className={`field ${focused === "password" ? "is-focus" : ""}`}
                >
                  <label className="field-label">PASSWORD</label>
                  <div className="field-input-wrap">
                    <input
                      type={showPassword ? "text" : "password"}
                      autoComplete="current-password"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      onFocus={() => setFocused("password")}
                      onBlur={() => setFocused(null)}
                      placeholder="••••••••••"
                      className="field-input pr-16"
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword((v) => !v)}
                      className="field-toggle"
                      aria-label="Toggle password visibility"
                    >
                      {showPassword ? "Hide" : "Show"}
                    </button>
                    <span className="field-underline" />
                  </div>
                </div>

                <div
                  className={`error-box ${error ? "error-box-open" : ""}`}
                  role={error ? "alert" : undefined}
                >
                  {error}
                </div>

                <button
                  type="submit"
                  disabled={submitting}
                  className={`submit-btn ${submitting ? "is-loading" : ""}`}
                >
                  <span className="submit-btn-label">
                    {submitting ? (
                      <span className="btn-loader">
                        <span className="btn-dot" />
                        <span className="btn-dot" />
                        <span className="btn-dot" />
                        <span>Signing in</span>
                      </span>
                    ) : (
                      <>
                        Go to Dashboard
                        <svg
                          viewBox="0 0 24 24"
                          width="16"
                          height="16"
                          className="btn-arrow"
                        >
                          <path
                            d="M5 12h14M13 5l7 7-7 7"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="2.2"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                        </svg>
                      </>
                    )}
                  </span>
                  <span className="submit-btn-shine" aria-hidden="true" />
                </button>
              </form>
            </div>
          </div>
        </div>
      </div>

      <style jsx>{`
        .login-root {
          position: relative;
          min-height: 100vh;
          width: 100%;
          display: flex;
          align-items: center;
          justify-content: center;
          background:
            radial-gradient(
              1200px 600px at 20% 30%,
              rgba(35, 43, 74, 0.55) 0%,
              rgba(11, 16, 32, 0.9) 55%,
              #0b1020 100%
            );
          padding: 2.5rem 1.5rem;
          overflow: hidden;
          perspective: 1400px;
        }
        @media (max-width: 640px) {
          .login-root {
            padding: 0.9rem 0.9rem 1.5rem;
            align-items: flex-start;
            min-height: 100dvh;
          }
        }

        /* ---------- Splash screen ---------- */
        .splash {
          position: fixed;
          inset: 0;
          z-index: 50;
          display: flex;
          align-items: center;
          justify-content: center;
          pointer-events: none;
          background: transparent;
        }
        .stage-ready .splash {
          animation: splash-root-fade 900ms ease forwards;
          animation-delay: 900ms;
        }
        .stage-ready .splash-inner {
          animation: splash-fade 650ms ease forwards;
        }
        .stage-ready .splash-reveal-left {
          animation: splash-slide-left 900ms
            cubic-bezier(0.85, 0, 0.15, 1) forwards;
        }
        .stage-ready .splash-reveal-right {
          animation: splash-slide-right 900ms
            cubic-bezier(0.85, 0, 0.15, 1) forwards;
        }
        .splash-reveal-left,
        .splash-reveal-right {
          position: absolute;
          top: 0;
          width: 50%;
          height: 100%;
          background: linear-gradient(
            160deg,
            #0a0f1f 0%,
            #0b1020 55%,
            #050814 100%
          );
          z-index: 1;
        }
        .splash-reveal-left {
          left: 0;
        }
        .splash-reveal-right {
          right: 0;
        }
        .splash-inner {
          position: relative;
          z-index: 2;
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 1.25rem;
          animation: splash-pop 900ms cubic-bezier(0.22, 1, 0.36, 1);
        }
        .splash-logo {
          position: relative;
          width: 120px;
          height: 120px;
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .splash-rune {
          position: absolute;
          border-radius: 50%;
          border: 1px solid rgba(212, 160, 23, 0.35);
        }
        .splash-rune-1 {
          width: 100%;
          height: 100%;
          animation: splash-spin 2.4s linear infinite;
          border-style: dashed;
          border-color: rgba(212, 160, 23, 0.6);
        }
        .splash-rune-2 {
          width: 128%;
          height: 128%;
          animation: splash-spin 3.6s linear infinite reverse;
          border-color: rgba(212, 160, 23, 0.25);
        }
        .splash-rune-3 {
          width: 156%;
          height: 156%;
          animation: splash-spin 5.4s linear infinite;
          border-color: rgba(212, 160, 23, 0.12);
          border-style: dotted;
        }
        .splash-mark {
          filter: drop-shadow(0 0 20px rgba(212, 160, 23, 0.55));
          animation: splash-breath 2.4s ease-in-out infinite;
        }
        .splash-text {
          display: flex;
          font-weight: 800;
          font-size: 1.75rem;
          letter-spacing: 0.04em;
          line-height: 1;
        }
        .splash-text-stack {
          flex-direction: column;
          align-items: center;
          gap: 0.4rem;
        }
        .splash-line {
          display: flex;
          justify-content: center;
        }
        @media (max-width: 480px) {
          .splash-text-stack {
            font-size: 1.25rem;
          }
        }
        .splash-letter {
          display: inline-block;
          opacity: 0;
          transform: translateY(10px);
          animation: letter-in 420ms ease forwards;
          color: #ffffff;
        }
        .splash-letter-gold {
          background: linear-gradient(180deg, #f4d37a, #d4a017 55%, #b8860b);
          background-clip: text;
          -webkit-background-clip: text;
          color: transparent;
        }
        .splash-gap {
          width: 0.35rem;
        }
        .splash-bar {
          width: 180px;
          height: 2px;
          background: rgba(255, 255, 255, 0.08);
          border-radius: 2px;
          overflow: hidden;
          position: relative;
        }
        .splash-bar::after {
          content: "";
          position: absolute;
          inset: 0;
          width: 30%;
          background: linear-gradient(
            90deg,
            transparent,
            #f4d37a,
            #d4a017,
            transparent
          );
          animation: bar-slide 1.3s ease-in-out infinite;
        }

        /* ---------- Background layers ---------- */
        .bg-grid {
          position: absolute;
          inset: 0;
          background-image: linear-gradient(
              rgba(212, 160, 23, 0.04) 1px,
              transparent 1px
            ),
            linear-gradient(
              90deg,
              rgba(212, 160, 23, 0.04) 1px,
              transparent 1px
            );
          background-size: 48px 48px;
          mask-image: radial-gradient(
            ellipse at center,
            black 40%,
            transparent 80%
          );
          opacity: 0.55;
          animation: grid-drift 30s linear infinite;
        }
        .bg-vignette {
          position: absolute;
          inset: 0;
          background: radial-gradient(
            circle at 50% 50%,
            transparent 40%,
            rgba(0, 0, 0, 0.55) 100%
          );
          pointer-events: none;
        }
        .bg-orb {
          position: absolute;
          border-radius: 9999px;
          filter: blur(100px);
          opacity: 0.35;
          pointer-events: none;
          animation: orb-drift 18s ease-in-out infinite;
        }
        .bg-orb-1 {
          width: 540px;
          height: 540px;
          background: radial-gradient(
            circle,
            rgba(212, 160, 23, 0.65),
            transparent 70%
          );
          top: -160px;
          left: -160px;
        }
        .bg-orb-2 {
          width: 440px;
          height: 440px;
          background: radial-gradient(
            circle,
            rgba(184, 134, 11, 0.5),
            transparent 70%
          );
          bottom: -160px;
          right: -120px;
          animation-delay: -6s;
          animation-duration: 22s;
        }
        .bg-orb-3 {
          width: 340px;
          height: 340px;
          background: radial-gradient(
            circle,
            rgba(88, 70, 180, 0.35),
            transparent 70%
          );
          top: 45%;
          left: 48%;
          transform: translate(-50%, -50%);
          animation-delay: -12s;
          animation-duration: 26s;
          opacity: 0.22;
        }

        .particles {
          position: absolute;
          inset: 0;
          overflow: hidden;
          pointer-events: none;
        }
        .particle {
          position: absolute;
          background: rgba(212, 160, 23, 0.7);
          border-radius: 50%;
          box-shadow: 0 0 10px rgba(212, 160, 23, 0.8);
          animation: particle-rise linear infinite;
          opacity: 0;
        }

        /* ---------- Shell ---------- */
        .login-shell {
          position: relative;
          z-index: 1;
          display: grid;
          grid-template-columns: 1fr;
          gap: 3rem;
          width: 100%;
          max-width: 1100px;
          align-items: center;
          opacity: 0;
        }
        .login-shell.is-ready {
          opacity: 1;
        }
        @media (min-width: 768px) {
          .login-shell {
            grid-template-columns: 1fr 1fr;
            gap: 4rem;
          }
        }
        /* Phones — tighter gap so the brand panel + sign-in card
           both fit in the viewport without requiring a scroll. */
        @media (max-width: 767px) {
          .login-shell {
            gap: 1.5rem;
          }
        }
        @media (max-width: 400px) {
          .login-shell {
            gap: 1.1rem;
          }
        }

        .brand-panel {
          display: flex;
          flex-direction: column;
          align-items: center;
          text-align: center;
        }
        .is-ready .brand-panel {
          animation: slide-in-left 900ms cubic-bezier(0.2, 0.8, 0.2, 1) both;
          animation-delay: 120ms;
        }

        .logo-wrap {
          position: relative;
          display: flex;
          align-items: center;
          justify-content: center;
        }
        /* Smaller logo on phones so the whole flow fits without scroll */
        @media (max-width: 767px) {
          .logo-wrap > div {
            width: 96px !important;
            height: 96px !important;
          }
        }
        @media (max-width: 400px) {
          .logo-wrap > div {
            width: 82px !important;
            height: 82px !important;
          }
        }
        .logo-ring {
          position: absolute;
          border-radius: 9999px;
          border: 1px dashed rgba(212, 160, 23, 0.35);
        }
        .logo-ring-1 {
          width: 110%;
          height: 110%;
          animation: ring-spin 22s linear infinite;
        }
        .logo-ring-2 {
          width: 128%;
          height: 128%;
          border-style: solid;
          border-color: rgba(212, 160, 23, 0.1);
          animation: ring-spin 40s linear infinite reverse;
        }
        .logo-ring-3 {
          width: 150%;
          height: 150%;
          border-style: dotted;
          border-color: rgba(212, 160, 23, 0.22);
          animation: ring-spin 60s linear infinite;
        }
        .logo-halo {
          position: absolute;
          width: 140%;
          height: 140%;
          border-radius: 50%;
          background: radial-gradient(
            circle,
            rgba(212, 160, 23, 0.2),
            transparent 60%
          );
          filter: blur(30px);
          animation: halo-pulse 4s ease-in-out infinite;
        }
        .logo-float {
          animation: float 6s ease-in-out infinite;
        }

        .brand-title {
          margin-top: 2rem;
          font-size: clamp(1.8rem, 7vw, 3.5rem);
          font-weight: 800;
          letter-spacing: -0.02em;
          color: #ffffff;
          line-height: 1;
          display: flex;
          align-items: baseline;
          justify-content: center;
          gap: 0.45rem;
        }
        /* Stacked-title variant for the longer "PERA AUTHORITY / CHATBOT"
           name — keeps the gold/white split + per-char animation but lays
           the two words on separate lines so they fit the brand panel
           without truncation. */
        .brand-title-stack {
          flex-direction: column;
          align-items: center;
          gap: 0.35rem;
          font-size: clamp(1.55rem, 5.5vw, 2.85rem);
          letter-spacing: 0.01em;
        }
        .brand-line {
          display: inline-flex;
          justify-content: center;
        }
        @media (max-width: 640px) {
          .brand-title {
            margin-top: 0.7rem;
          }
          .brand-title-stack {
            gap: 0.2rem;
            font-size: clamp(1.25rem, 6.5vw, 2rem);
          }
        }
        .brand-word {
          display: inline-flex;
        }
        .brand-char {
          display: inline-block;
          opacity: 0;
          transform: translateY(14px);
          filter: blur(4px);
          animation: char-in 620ms cubic-bezier(0.22, 1, 0.36, 1) forwards;
        }
        .brand-char-gold {
          background: linear-gradient(
            180deg,
            #f4d37a 0%,
            #d4a017 50%,
            #b8860b 100%
          );
          background-clip: text;
          -webkit-background-clip: text;
          color: transparent;
          text-shadow: 0 0 40px rgba(212, 160, 23, 0.25);
        }
        .brand-subtitle {
          margin-top: 0.85rem;
          font-size: 0.95rem;
          color: rgba(254, 243, 199, 0.65);
          max-width: 22rem;
          opacity: 0;
          transform: translateY(6px);
        }
        @media (max-width: 640px) {
          .brand-subtitle {
            margin-top: 0.5rem;
            font-size: 0.82rem;
            padding: 0 0.5rem;
          }
        }
        .is-ready .brand-subtitle {
          animation: fade-up 600ms ease 1500ms forwards;
        }
        .brand-divider {
          margin-top: 1.5rem;
          width: 96px;
          height: 1px;
          background: linear-gradient(
            90deg,
            transparent,
            rgba(212, 160, 23, 0.6),
            transparent
          );
          position: relative;
          opacity: 0;
        }
        @media (max-width: 640px) {
          .brand-divider {
            margin-top: 0.7rem;
          }
        }
        .is-ready .brand-divider {
          animation: fade-up 600ms ease 1700ms forwards;
        }
        .brand-divider-dot {
          position: absolute;
          width: 6px;
          height: 6px;
          top: -2px;
          left: 50%;
          transform: translateX(-50%);
          background: #d4a017;
          border-radius: 50%;
          box-shadow: 0 0 12px rgba(212, 160, 23, 0.8);
          animation: dot-pulse 2s ease-in-out infinite;
        }

        /* ---------- Card ---------- */
        .card-outer {
          display: flex;
          justify-content: center;
        }
        .is-ready .card-outer {
          animation: slide-in-right 900ms cubic-bezier(0.2, 0.8, 0.2, 1) both;
          animation-delay: 220ms;
        }
        .card {
          position: relative;
          width: 100%;
          max-width: 440px;
          transform-style: preserve-3d;
          transform: perspective(1200px)
            rotateX(var(--rx, 0deg))
            rotateY(var(--ry, 0deg));
          transition: transform 220ms ease-out;
        }
        .card-glow {
          position: absolute;
          inset: -2px;
          border-radius: 22px;
          background: conic-gradient(
            from 180deg at 50% 50%,
            rgba(212, 160, 23, 0.6),
            rgba(212, 160, 23, 0) 30%,
            rgba(212, 160, 23, 0.45) 60%,
            rgba(212, 160, 23, 0) 85%,
            rgba(212, 160, 23, 0.6)
          );
          filter: blur(3px);
          opacity: 0.6;
          pointer-events: none;
          z-index: 0;
          animation: conic-spin 8s linear infinite;
        }
        .card-border {
          position: absolute;
          inset: 0;
          border-radius: 20px;
          padding: 1px;
          background: linear-gradient(
            135deg,
            rgba(212, 160, 23, 0.55) 0%,
            rgba(212, 160, 23, 0.1) 45%,
            rgba(212, 160, 23, 0.5) 100%
          );
          -webkit-mask: linear-gradient(#000 0 0) content-box,
            linear-gradient(#000 0 0);
          -webkit-mask-composite: xor;
          mask-composite: exclude;
          pointer-events: none;
          z-index: 1;
        }
        .card-sheen {
          position: absolute;
          inset: 0;
          border-radius: 20px;
          pointer-events: none;
          z-index: 2;
          background: radial-gradient(
            400px circle at var(--mx, 50%) var(--my, 50%),
            rgba(255, 255, 255, 0.08),
            transparent 45%
          );
          transition: background-position 300ms ease;
        }
        .card-body {
          position: relative;
          z-index: 1;
          background: linear-gradient(
            165deg,
            rgba(19, 26, 46, 0.95) 0%,
            rgba(14, 20, 38, 0.95) 100%
          );
          border-radius: 20px;
          padding: 2.25rem;
          backdrop-filter: blur(14px);
          box-shadow:
            0 30px 60px -20px rgba(0, 0, 0, 0.7),
            0 8px 24px -8px rgba(212, 160, 23, 0.18);
        }
        @media (max-width: 640px) {
          .card-body {
            padding: 1.35rem 1.2rem 1.5rem;
            border-radius: 16px;
          }
        }

        .card-header {
          margin-bottom: 1.9rem;
        }
        @media (max-width: 640px) {
          .card-header {
            margin-bottom: 1.1rem;
          }
        }
        .card-badge {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          font-size: 10px;
          letter-spacing: 0.22em;
          font-weight: 600;
          padding: 4px 12px 4px 10px;
          border-radius: 9999px;
          background: rgba(212, 160, 23, 0.1);
          color: #e9c464;
          border: 1px solid rgba(212, 160, 23, 0.3);
          margin-bottom: 0.9rem;
        }
        .card-badge-dot {
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: #f4d37a;
          box-shadow: 0 0 8px #f4d37a;
          animation: dot-pulse 1.8s ease-in-out infinite;
        }
        .card-title {
          font-size: 1.85rem;
          font-weight: 700;
          color: #ffffff;
          letter-spacing: -0.02em;
          line-height: 1.1;
        }
        @media (max-width: 640px) {
          .card-title {
            font-size: 1.4rem;
          }
        }
        .card-hint {
          margin-top: 0.5rem;
          font-size: 0.8125rem;
          color: rgba(254, 243, 199, 0.55);
        }

        /* ---------- Fields ---------- */
        .field {
          display: flex;
          flex-direction: column;
        }
        .field-label {
          font-size: 11px;
          font-weight: 600;
          letter-spacing: 0.14em;
          color: rgba(254, 243, 199, 0.6);
          margin-bottom: 0.55rem;
          transition: color 180ms ease, transform 180ms ease;
        }
        .field.is-focus .field-label {
          color: #f4d37a;
          transform: translateY(-1px);
        }
        .field-input-wrap {
          position: relative;
        }
        :global(.field-input) {
          width: 100%;
          background: #eef1f8;
          color: #0b1020;
          border-radius: 12px;
          padding: 0.95rem 1rem;
          font-size: 0.9rem;
          border: 1px solid transparent;
          transition: border-color 180ms ease, box-shadow 220ms ease,
            transform 220ms ease, background 220ms ease;
        }
        @media (max-width: 640px) {
          :global(.field-input) {
            padding: 0.75rem 0.9rem;
            font-size: 0.88rem;
          }
          /* Tighten form field spacing on mobile */
          :global(.card-body form.space-y-5 > * + *) {
            margin-top: 0.85rem !important;
          }
        }
        :global(.field-input::placeholder) {
          color: #94a3b8;
        }
        :global(.field-input:focus) {
          outline: none;
          background: #ffffff;
          border-color: rgba(212, 160, 23, 0.6);
          box-shadow:
            0 0 0 4px rgba(212, 160, 23, 0.18),
            0 10px 28px -10px rgba(212, 160, 23, 0.3);
          transform: translateY(-1px);
        }
        .field-underline {
          position: absolute;
          left: 10%;
          right: 10%;
          bottom: -4px;
          height: 2px;
          background: linear-gradient(
            90deg,
            transparent,
            #d4a017,
            transparent
          );
          transform: scaleX(0);
          transform-origin: center;
          transition: transform 300ms cubic-bezier(0.22, 1, 0.36, 1);
          border-radius: 2px;
        }
        .field.is-focus .field-underline {
          transform: scaleX(1);
        }
        .field-toggle {
          position: absolute;
          top: 50%;
          right: 0.5rem;
          transform: translateY(-50%);
          padding: 0.35rem 0.65rem;
          font-size: 11px;
          font-weight: 600;
          color: #475569;
          background: transparent;
          border: 0;
          cursor: pointer;
          border-radius: 8px;
          transition: background 150ms ease, color 150ms ease;
        }
        .field-toggle:hover {
          background: rgba(15, 23, 42, 0.08);
          color: #0b1020;
        }

        .error-box {
          max-height: 0;
          overflow: hidden;
          font-size: 0.8125rem;
          color: #fda4af;
          background: rgba(190, 18, 60, 0.14);
          border: 1px solid rgba(244, 63, 94, 0.3);
          border-radius: 10px;
          padding: 0 0.8rem;
          transition: max-height 260ms ease, padding 260ms ease,
            opacity 260ms ease;
          opacity: 0;
        }
        .error-box-open {
          max-height: 120px;
          padding: 0.6rem 0.8rem;
          opacity: 1;
          animation: shake 420ms ease;
        }

        /* ---------- Submit button ---------- */
        .submit-btn {
          position: relative;
          overflow: hidden;
          width: 100%;
          margin-top: 0.5rem;
          background: linear-gradient(
            180deg,
            #f4d37a 0%,
            #d4a017 55%,
            #b8860b 100%
          );
          color: #1a1307;
          font-weight: 700;
          font-size: 0.95rem;
          border: 0;
          border-radius: 12px;
          padding: 1rem 1rem;
          cursor: pointer;
          transition: transform 220ms ease, box-shadow 220ms ease,
            filter 220ms ease;
          box-shadow:
            0 10px 24px -12px rgba(212, 160, 23, 0.8),
            inset 0 1px 0 rgba(255, 255, 255, 0.35);
        }
        @media (max-width: 640px) {
          .submit-btn {
            padding: 0.8rem 1rem;
            font-size: 0.9rem;
            margin-top: 0.25rem;
          }
        }
        .submit-btn:hover:not(:disabled) {
          filter: brightness(1.08);
          transform: translateY(-2px);
          box-shadow:
            0 18px 36px -14px rgba(212, 160, 23, 0.7),
            inset 0 1px 0 rgba(255, 255, 255, 0.4);
        }
        .submit-btn:active:not(:disabled) {
          transform: translateY(0);
          filter: brightness(0.95);
        }
        .submit-btn:disabled {
          cursor: not-allowed;
          filter: saturate(0.75);
        }
        .submit-btn-label {
          position: relative;
          z-index: 1;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          gap: 0.6rem;
        }
        .btn-arrow {
          transition: transform 220ms ease;
        }
        .submit-btn:hover .btn-arrow {
          transform: translateX(4px);
        }
        .submit-btn-shine {
          position: absolute;
          top: 0;
          left: -120%;
          width: 80%;
          height: 100%;
          background: linear-gradient(
            100deg,
            transparent,
            rgba(255, 255, 255, 0.55),
            transparent
          );
          transform: skewX(-18deg);
          animation: shine 3.2s ease-in-out infinite;
        }
        .submit-btn.is-loading .submit-btn-shine {
          animation: shine 1.2s ease-in-out infinite;
        }
        .btn-loader {
          display: inline-flex;
          align-items: center;
          gap: 0.4rem;
        }
        .btn-dot {
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: #1a1307;
          display: inline-block;
          animation: dot-bounce 1.2s ease-in-out infinite;
        }
        .btn-dot:nth-child(2) {
          animation-delay: 0.15s;
        }
        .btn-dot:nth-child(3) {
          animation-delay: 0.3s;
        }

        /* ---------- Keyframes ---------- */
        @keyframes orb-drift {
          0%, 100% {
            transform: translate(0, 0);
          }
          50% {
            transform: translate(30px, -25px);
          }
        }
        @keyframes grid-drift {
          from {
            background-position: 0 0;
          }
          to {
            background-position: 48px 48px;
          }
        }
        @keyframes ring-spin {
          from {
            transform: rotate(0deg);
          }
          to {
            transform: rotate(360deg);
          }
        }
        @keyframes conic-spin {
          from {
            transform: rotate(0deg);
          }
          to {
            transform: rotate(360deg);
          }
        }
        @keyframes halo-pulse {
          0%, 100% {
            opacity: 0.25;
            transform: scale(1);
          }
          50% {
            opacity: 0.6;
            transform: scale(1.07);
          }
        }
        @keyframes float {
          0%, 100% {
            transform: translateY(0);
          }
          50% {
            transform: translateY(-12px);
          }
        }
        @keyframes dot-pulse {
          0%, 100% {
            opacity: 1;
            transform: scale(1);
          }
          50% {
            opacity: 0.55;
            transform: scale(0.85);
          }
        }
        @keyframes char-in {
          to {
            opacity: 1;
            transform: translateY(0);
            filter: blur(0);
          }
        }
        @keyframes fade-up {
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
        @keyframes slide-in-left {
          from {
            opacity: 0;
            transform: translateX(-30px);
          }
          to {
            opacity: 1;
            transform: translateX(0);
          }
        }
        @keyframes slide-in-right {
          from {
            opacity: 0;
            transform: translateX(30px);
          }
          to {
            opacity: 1;
            transform: translateX(0);
          }
        }
        @keyframes shake {
          0%, 100% {
            transform: translateX(0);
          }
          20%, 60% {
            transform: translateX(-5px);
          }
          40%, 80% {
            transform: translateX(5px);
          }
        }
        @keyframes shine {
          0% {
            left: -120%;
          }
          60%, 100% {
            left: 140%;
          }
        }
        @keyframes dot-bounce {
          0%, 80%, 100% {
            transform: translateY(0);
            opacity: 0.6;
          }
          40% {
            transform: translateY(-4px);
            opacity: 1;
          }
        }
        @keyframes particle-rise {
          0% {
            opacity: 0;
            transform: translateY(30px) scale(0.6);
          }
          10%, 85% {
            opacity: 1;
          }
          100% {
            opacity: 0;
            transform: translateY(-120px) scale(1);
          }
        }
        @keyframes splash-spin {
          from {
            transform: rotate(0deg);
          }
          to {
            transform: rotate(360deg);
          }
        }
        @keyframes splash-breath {
          0%, 100% {
            transform: scale(1);
            filter: drop-shadow(0 0 20px rgba(212, 160, 23, 0.55));
          }
          50% {
            transform: scale(1.04);
            filter: drop-shadow(0 0 36px rgba(212, 160, 23, 0.8));
          }
        }
        @keyframes letter-in {
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
        @keyframes bar-slide {
          0% {
            transform: translateX(-100%);
          }
          100% {
            transform: translateX(500%);
          }
        }
        @keyframes splash-pop {
          from {
            opacity: 0;
            transform: scale(0.94);
          }
          to {
            opacity: 1;
            transform: scale(1);
          }
        }
        @keyframes splash-fade {
          to {
            opacity: 0;
            transform: scale(0.96);
          }
        }
        @keyframes splash-slide-left {
          to {
            transform: translateX(-100%);
          }
        }
        @keyframes splash-slide-right {
          to {
            transform: translateX(100%);
          }
        }
        @keyframes splash-root-fade {
          to {
            opacity: 0;
            visibility: hidden;
          }
        }
      `}</style>
    </div>
  );
}
