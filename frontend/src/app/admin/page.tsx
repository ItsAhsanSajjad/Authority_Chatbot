"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useRouter } from "next/navigation";
import Image from "next/image";
import {
  AdminDocument,
  FreshnessStatus,
  FreshnessTrack,
  IndexStatus,
  deleteDocument,
  freshnessStatus,
  getSession,
  indexStatus,
  listDocuments,
  logout,
  uploadDocument,
} from "../lib/adminApi";

function fmtBytes(n: number): string {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

function fmtDate(ts: number): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return "—";
  }
}

/** Human-friendly age — "12s ago", "4m ago", "2h ago". */
function fmtAge(seconds: number | null): string {
  if (seconds === null || seconds === undefined) return "never";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
  return `${Math.round(seconds / 86400)}d ago`;
}

/** Pretty label for each track id. */
const TRACK_LABELS: Record<string, string> = {
  inspection_summaries: "Inspection Summaries",
  inspection_officers: "Officer Breakdown",
  operational_activity: "Operational Activity",
  operational_activity_breakdown: "OA Tehsil Breakdown",
};

/** Smoothly animates a number from 0 → target. */
function AnimatedNumber({ value }: { value: number }) {
  const [display, setDisplay] = useState(0);
  const raf = useRef<number | null>(null);
  useEffect(() => {
    const start = performance.now();
    const from = display;
    const to = value || 0;
    const duration = 700;
    function tick(now: number) {
      const t = Math.min(1, (now - start) / duration);
      // ease-out-quart
      const eased = 1 - Math.pow(1 - t, 4);
      setDisplay(Math.round(from + (to - from) * eased));
      if (t < 1) raf.current = requestAnimationFrame(tick);
    }
    raf.current = requestAnimationFrame(tick);
    return () => {
      if (raf.current) cancelAnimationFrame(raf.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);
  return <>{display.toLocaleString()}</>;
}

export default function AdminDashboard() {
  const router = useRouter();
  const [sessionEmail, setSessionEmail] = useState<string>("");
  const [docs, setDocs] = useState<AdminDocument[]>([]);
  const [status, setStatus] = useState<IndexStatus | null>(null);
  const [freshness, setFreshness] = useState<FreshnessStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [toast, setToast] = useState<
    { id: number; kind: "ok" | "err"; msg: string } | null
  >(null);
  const [uploading, setUploading] = useState(false);
  const [uploadPct, setUploadPct] = useState(0);
  const [uploadFileName, setUploadFileName] = useState<string>("");
  // Upload stages: idle -> uploading -> indexing -> success (brief)
  const [uploadStage, setUploadStage] =
    useState<"idle" | "uploading" | "indexing" | "success">("idle");
  // Fake a moving "chunk" counter while indexing so the user sees progress.
  const [indexingTick, setIndexingTick] = useState(0);
  const [successBurst, setSuccessBurst] = useState(false);
  const [deletingName, setDeletingName] = useState<string | null>(null);
  const [confirm, setConfirm] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);
  const [stage, setStage] = useState<"splash" | "ready">("splash");
  // Tracks rows exiting so we can animate them out before unmount.
  const [exitingRows, setExitingRows] = useState<Set<string>>(new Set());
  const inputRef = useRef<HTMLInputElement | null>(null);
  const toastIdRef = useRef(0);

  // Auth gate + splash transition
  useEffect(() => {
    const s = getSession();
    if (!s) {
      router.replace("/login");
      return;
    }
    setSessionEmail(s.email);
    const t = window.setTimeout(() => setStage("ready"), 900);
    return () => window.clearTimeout(t);
  }, [router]);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const [d, s, f] = await Promise.all([
        listDocuments(),
        indexStatus(),
        freshnessStatus().catch(() => null),  // never block on freshness
      ]);
      setDocs(d);
      setStatus(s);
      if (f) setFreshness(f);
    } catch (e) {
      showToast("err", e instanceof Error ? e.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (sessionEmail) refresh();
  }, [sessionEmail, refresh]);

  // Light 15-second tick that re-fetches ONLY the freshness status (no
  // doc/index refetch — those are heavier). This keeps the "X seconds ago"
  // labels live without thrashing the rest of the dashboard.
  useEffect(() => {
    if (!sessionEmail) return;
    const id = window.setInterval(() => {
      freshnessStatus()
        .then((f) => setFreshness(f))
        .catch(() => {/* swallow — endpoint may be momentarily down */});
    }, 15000);
    return () => window.clearInterval(id);
  }, [sessionEmail]);

  // Auto-refresh docs + index status while a background re-index is
  // actively running — OR while ANY scanned doc is still pending. Without
  // this, an upload that returned instantly leaves the table stuck on the
  // pre-upload snapshot until the user manually clicks Refresh. Polls
  // every 4 s, stops the moment the indexer is idle and nothing is
  // pending. Cheap (just one /index-status + one /documents call).
  useEffect(() => {
    if (!sessionEmail) return;
    const hasPendingDoc = docs.some((d) => !d.indexed);
    const isIndexing = !!status?.reindex_running;
    if (!hasPendingDoc && !isIndexing) return;
    const id = window.setInterval(async () => {
      try {
        const [d, s] = await Promise.all([listDocuments(), indexStatus()]);
        setDocs(d);
        setStatus(s);
      } catch {/* swallow — try again next tick */}
    }, 4000);
    return () => window.clearInterval(id);
  }, [sessionEmail, status?.reindex_running, docs]);

  // Gentle ticking counter during indexing so the stage feels alive.
  useEffect(() => {
    if (uploadStage !== "indexing") return;
    setIndexingTick(0);
    const id = window.setInterval(() => {
      setIndexingTick((t) => t + 1);
    }, 420);
    return () => window.clearInterval(id);
  }, [uploadStage]);

  function showToast(kind: "ok" | "err", msg: string) {
    const id = ++toastIdRef.current;
    setToast({ id, kind, msg });
    window.setTimeout(() => {
      setToast((cur) => (cur && cur.id === id ? null : cur));
    }, 4600);
  }

  async function handleFiles(files: FileList | null) {
    if (!files || !files.length) return;
    const file = files[0];
    if (!file.name.toLowerCase().endsWith(".pdf")) {
      showToast("err", "Only PDF files are accepted.");
      return;
    }
    // Client-side duplicate check — name AND size match = almost
    // certainly the same file. Catches it instantly without wasting
    // an upload round-trip. Server still performs a SHA-256 content
    // hash check as the authoritative guard.
    const nameLower = file.name.toLowerCase();
    const duplicate = docs.find(
      (d) =>
        d.filename.toLowerCase() === nameLower ||
        (d.filename.toLowerCase().replace(/\s+/g, "") === nameLower.replace(/\s+/g, "") &&
          d.size === file.size),
    );
    if (duplicate) {
      showToast(
        "err",
        `"${duplicate.filename}" is already uploaded. Delete it first if you want to replace it.`,
      );
      if (inputRef.current) inputRef.current.value = "";
      return;
    }
    setUploading(true);
    setUploadPct(0);
    setUploadFileName(file.name);
    setUploadStage("uploading");
    const uploadStarted = Date.now();
    try {
      const result = await uploadDocument(file, (pct) => {
        setUploadPct(pct);
        if (pct >= 100) setUploadStage("indexing");
      });
      // Backend now returns immediately while indexing runs in the
      // background. Poll /api/admin/index-status until the indexer
      // reports done (reindex_running flips false), then show success.
      setUploadStage("indexing");
      const isBackground =
        (result as unknown as { index?: { background?: boolean } })?.index
          ?.background === true;
      if (isBackground) {
        await waitForIndexingDone();
      }
      setUploadStage("success");
      setSuccessBurst(true);
      window.setTimeout(() => setSuccessBurst(false), 1400);
      const elapsedSec = ((Date.now() - uploadStarted) / 1000).toFixed(1);
      showToast(
        "ok",
        `Uploaded ${result.filename} · indexed in ${elapsedSec}s`,
      );
      await refresh();
      window.setTimeout(() => {
        setUploading(false);
        setUploadPct(0);
        setUploadStage("idle");
        setUploadFileName("");
      }, 900);
    } catch (e) {
      showToast("err", e instanceof Error ? e.message : "Upload failed");
      setUploading(false);
      setUploadPct(0);
      setUploadStage("idle");
      setUploadFileName("");
    } finally {
      if (inputRef.current) inputRef.current.value = "";
    }
  }

  /** Polls /api/admin/index-status until reindex_running flips to false.
   * Resolves after success, or rejects on a 5-minute hard cap so a
   * stuck indexer doesn't keep the UI in "Indexing…" forever. */
  async function waitForIndexingDone(): Promise<void> {
    const deadline = Date.now() + 5 * 60 * 1000;
    // give the indexer a beat to mark itself running before we poll
    await new Promise((r) => window.setTimeout(r, 800));
    while (Date.now() < deadline) {
      try {
        const s = await indexStatus();
        setStatus(s);
        if (!s?.reindex_running) {
          if (s?.last_error) throw new Error(s.last_error);
          return;
        }
      } catch (e) {
        // last_error from the indexer is surfaced as a real failure
        if (e instanceof Error && e.message && !e.message.startsWith("index_status_failed")) {
          throw e;
        }
        // network blips — try again
      }
      await new Promise((r) => window.setTimeout(r, 1500));
    }
    throw new Error("indexing_timeout");
  }

  async function performDelete(name: string) {
    setDeletingName(name);
    const startedAt = Date.now();
    // animate the row out first
    setExitingRows((prev) => new Set(prev).add(name));
    try {
      const result = await deleteDocument(name);
      // Backend now returns instantly and re-indexes in the background.
      // Close the modal + show "Re-indexing…" toast immediately so the
      // user feels the action complete; the actual re-index finishes
      // asynchronously and we surface its completion in a second toast.
      const isBackground =
        (result as unknown as { index?: { background?: boolean } })?.index
          ?.background === true;
      await refresh();
      setDeletingName(null);
      setConfirm(null);
      setExitingRows(new Set());

      if (isBackground) {
        showToast("ok", `Removed ${name} · re-indexing in background…`);
        // Don't block the UI — let the user keep working. We still wait
        // (silently) for completion so we can fire a final success toast
        // and refresh document state when the new index goes live.
        try {
          await waitForIndexingDone();
          const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
          showToast("ok", `Index rebuilt · ${elapsedSec}s`);
          await refresh();
        } catch (innerErr) {
          showToast(
            "err",
            innerErr instanceof Error
              ? `Re-index failed: ${innerErr.message}`
              : "Re-index failed",
          );
        }
      } else {
        showToast(
          "ok",
          `Removed ${name} · re-indexed in ${(
            (result.duration_ms || 0) / 1000
          ).toFixed(1)}s`,
        );
      }
    } catch (e) {
      showToast("err", e instanceof Error ? e.message : "Delete failed");
      // Revert the exit animation on failure
      setExitingRows((prev) => {
        const n = new Set(prev);
        n.delete(name);
        return n;
      });
      setDeletingName(null);
      setConfirm(null);
    }
  }

  const particles = useMemo(
    () =>
      Array.from({ length: 22 }).map((_, i) => ({
        id: i,
        left: Math.random() * 100,
        top: Math.random() * 100,
        size: 2 + Math.random() * 3,
        delay: Math.random() * 8,
        duration: 10 + Math.random() * 10,
      })),
    [],
  );

  if (!sessionEmail) {
    return (
      <div
        style={{
          minHeight: "100vh",
          background: "#0b1020",
          color: "white",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <span style={{ opacity: 0.6, fontSize: "0.875rem" }}>Redirecting…</span>
      </div>
    );
  }

  const DocumentsSkeleton = () => (
    <div className="skel-wrap">
      {Array.from({ length: 5 }).map((_, i) => (
        <div
          key={i}
          className="skel-row"
          style={{ animationDelay: `${i * 60}ms` }}
        >
          <div className="skel skel-name" />
          <div className="skel skel-size" />
          <div className="skel skel-date" />
          <div className="skel skel-badge" />
          <div className="skel skel-action" />
        </div>
      ))}
    </div>
  );

  return (
    <div
      className={`admin-root stage-${stage}`}
      style={{
        position: "relative",
        height: "100vh",
        width: "100%",
        overflowY: "auto",
        overflowX: "hidden",
        background:
          "radial-gradient(1400px 700px at 15% -10%, rgba(35,43,74,0.55) 0%, rgba(11,16,32,0.92) 55%, #0b1020 100%)",
        color: "#f5f3ef",
      }}
    >
      {/* Splash cover — shows on every refresh, fades out once ready */}
      <div
        className="splash"
        aria-hidden={stage === "ready"}
        style={{
          position: "fixed",
          inset: 0,
          zIndex: 60,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          pointerEvents: "none",
        }}
      >
        <div
          className="splash-cover"
          style={{
            position: "absolute",
            inset: 0,
            background:
              "linear-gradient(160deg, #0a0f1f 0%, #0b1020 55%, #050814 100%)",
          }}
        />
        <div className="splash-inner">
          <div className="splash-logo">
            <span className="splash-rune splash-rune-1" />
            <span className="splash-rune splash-rune-2" />
            <Image
              src="/Authority_Logo.png"
              alt=""
              width={88}
              height={88}
              priority
              className="splash-mark"
            />
          </div>
          <div className="splash-bar" />
        </div>
      </div>

      {/* Ambient background layers */}
      <div className="bg-grid" aria-hidden="true" />
      <div className="bg-orb bg-orb-1" aria-hidden="true" />
      <div className="bg-orb bg-orb-2" aria-hidden="true" />
      <div className="bg-orb bg-orb-3" aria-hidden="true" />
      <div className="bg-vignette" aria-hidden="true" />
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

      {/* Header */}
      <header
        className={`admin-header ${stage === "ready" ? "is-ready" : ""}`}
      >
        <div className="admin-header-inner">
          <div className="admin-brand">
            <div className="admin-brand-logo">
              <span className="brand-ring-1" />
              <span className="brand-ring-2" />
              <Image
                src="/Authority_Logo.png"
                alt="PERA AUTHORITY CHATBOT"
                fill
                sizes="48px"
                className="object-contain"
              />
            </div>
            <div>
              <div className="admin-brand-title">
                <span className="brand-gold">PERA AUTHORITY</span> CHATBOT
              </div>
              <div className="admin-brand-sub">
                <span className="live-dot" /> ADMIN DASHBOARD
              </div>
            </div>
          </div>
          <div className="admin-user">
            <div className="admin-user-email">
              <svg viewBox="0 0 24 24" width="14" height="14" aria-hidden>
                <path
                  d="M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8Zm0 2c-4.5 0-8 2.1-8 5v1h16v-1c0-2.9-3.5-5-8-5Z"
                  fill="currentColor"
                />
              </svg>
              {sessionEmail}
            </div>
            <button onClick={logout} className="admin-signout" aria-label="Sign out">
              <svg viewBox="0 0 24 24" width="15" height="15" aria-hidden>
                <path
                  d="M10 17l-1.4-1.4 2.6-2.6H3v-2h8.2L8.6 8.4 10 7l5 5-5 5Zm9-14v18h-7v-2h5V5h-5V3h7Z"
                  fill="currentColor"
                />
              </svg>
              <span className="admin-signout-label">Sign Out</span>
            </button>
          </div>
        </div>
      </header>

      <main className={`admin-main ${stage === "ready" ? "is-ready" : ""}`}>
        {/* Index error banner */}
        {status?.last_error && (
          <div className="idx-error-banner" role="alert">
            <span className="idx-error-icon" aria-hidden>
              <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor">
                <path d="M12 2 1 21h22L12 2Zm1 7v6h-2V9h2Zm0 8v2h-2v-2h2Z" />
              </svg>
            </span>
            <div className="idx-error-body">
              <div className="idx-error-title">Indexing paused</div>
              <div className="idx-error-msg">{status.last_error}</div>
            </div>
            {status.reindex_running && (
              <span className="idx-error-retry">
                <span className="idx-error-retry-dot" />
                Retrying
              </span>
            )}
          </div>
        )}

        {/* KPI strip */}
        <section className="kpi-row">
          <div className="kpi-card stagger" style={{ "--d": "0ms" } as React.CSSProperties}>
            <div className="kpi-glow" />
            <div className="kpi-icon">
              <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden>
                <path
                  d="M4 4h16v4H4V4Zm0 6h16v4H4v-4Zm0 6h10v4H4v-4Z"
                  fill="currentColor"
                />
              </svg>
            </div>
            <div className="kpi-meta">
              <div className="kpi-label">ACTIVE INDEX</div>
              <div className="kpi-value kpi-value-mono">
                {status?.active_index_dir
                  ? status.active_index_dir.split("/").pop()
                  : "—"}
              </div>
            </div>
          </div>
          <div className="kpi-card stagger" style={{ "--d": "90ms" } as React.CSSProperties}>
            <div className="kpi-glow" />
            <div className="kpi-icon kpi-icon-gold">
              <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden>
                <path
                  d="M6 2h9l5 5v15H6V2Zm8 1.5V8h4.5L14 3.5Z"
                  fill="currentColor"
                />
              </svg>
            </div>
            <div className="kpi-meta">
              <div className="kpi-label">DOCUMENTS</div>
              <div className="kpi-value">
                <AnimatedNumber value={status?.doc_count ?? 0} />
              </div>
            </div>
          </div>
          <div className="kpi-card stagger" style={{ "--d": "180ms" } as React.CSSProperties}>
            <div className="kpi-glow" />
            <div className="kpi-icon kpi-icon-gold">
              <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden>
                <path
                  d="M3 6h18v2H3V6Zm2 5h14v2H5v-2Zm-2 5h18v2H3v-2Z"
                  fill="currentColor"
                />
              </svg>
            </div>
            <div className="kpi-meta">
              <div className="kpi-label">CHUNKS</div>
              <div className="kpi-value">
                <AnimatedNumber value={status?.chunk_count ?? 0} />
              </div>
            </div>
          </div>
          <div className="kpi-refresh stagger" style={{ "--d": "260ms" } as React.CSSProperties}>
            <button
              onClick={refresh}
              disabled={loading}
              className={`refresh-btn ${loading ? "spin" : ""}`}
            >
              <svg viewBox="0 0 24 24" width="14" height="14" aria-hidden>
                <path
                  d="M17.65 6.35A8 8 0 1 0 19.73 14h-2.1a6 6 0 1 1-1.4-6.24L13 11h7V4l-2.35 2.35Z"
                  fill="currentColor"
                />
              </svg>
              {loading ? "Refreshing…" : "Refresh"}
            </button>
          </div>
        </section>

        {/* Data Freshness — auto-refreshing scheduler status (Phase 7) */}
        <section className="freshness-card stagger" style={{ "--d": "300ms" } as React.CSSProperties}>
          <div className="fr-card-glow" aria-hidden />
          <div className="freshness-head">
            <div className="freshness-title">
              <span className="freshness-dot" />
              <span className="freshness-title-text">DATA FRESHNESS</span>
              <span className="freshness-live-tag">
                <span className="live-pulse" />
                LIVE
              </span>
            </div>
            <div className="freshness-sub">
              {freshness?.enabled === false
                ? "Continuous refresh is disabled (FRESHNESS_ENABLED=0)"
                : "Background ingest keeps stored data current. Auto-updates every 15 s."}
            </div>
          </div>
          <div className="freshness-grid">
            {freshness?.tracks &&
              Object.entries(freshness.tracks).map(
                ([key, t]: [string, FreshnessTrack], idx) => {
                  const label = TRACK_LABELS[key] ?? key;
                  const intervalSec = t.interval_s || 1;
                  const intervalLabel =
                    intervalSec >= 3600
                      ? `${(intervalSec / 3600).toFixed(intervalSec % 3600 ? 1 : 0)} h`
                      : `${Math.round(intervalSec / 60)} min`;
                  const ageStr = fmtAge(t.seconds_since_success);
                  const lastSuccessAbs = t.last_success_at
                    ? fmtDate(t.last_success_at)
                    : "—";
                  // Progress through the current interval window (0..1).
                  // When running, show a shimmering "indeterminate" bar.
                  const progress =
                    t.seconds_since_success !== null && intervalSec > 0
                      ? Math.min(1, t.seconds_since_success / intervalSec)
                      : 0;
                  const progressPct = Math.round(progress * 100);
                  // Time-to-next-cycle estimate (only meaningful after a
                  // successful cycle; before that we show "—").
                  const remaining =
                    t.seconds_since_success !== null
                      ? Math.max(0, intervalSec - t.seconds_since_success)
                      : null;
                  const nextStr =
                    remaining === null
                      ? "—"
                      : remaining < 60
                      ? `${remaining}s`
                      : remaining < 3600
                      ? `${Math.round(remaining / 60)} min`
                      : `${(remaining / 3600).toFixed(1)} h`;

                  return (
                    <div
                      key={key}
                      className={`freshness-item is-${t.staleness_status} ${
                        t.running_now ? "is-running" : ""
                      }`}
                      style={{ "--d": `${idx * 90}ms` } as React.CSSProperties}
                    >
                      <div className="fr-item-glow" aria-hidden />
                      <div className="fr-item-border" aria-hidden />

                      <div className="fr-row fr-head-row">
                        <span className="fr-name">{label}</span>
                        <span className={`fr-pill is-${t.staleness_status}`}>
                          {t.running_now ? (
                            <>
                              <span className="fr-pill-dot" />
                              SYNCING
                            </>
                          ) : (
                            t.staleness_status.toUpperCase()
                          )}
                        </span>
                      </div>

                      {/* Progress bar — indeterminate shimmer while
                          running, otherwise fills toward the next cycle */}
                      <div className="fr-bar">
                        <div
                          className={`fr-bar-fill ${
                            t.running_now ? "is-indeterminate" : ""
                          }`}
                          style={{
                            width: t.running_now ? "100%" : `${progressPct}%`,
                          }}
                        />
                        <div className="fr-bar-shine" aria-hidden />
                      </div>

                      <div className="fr-rows">
                        <div className="fr-row fr-meta">
                          <span className="fr-meta-key">Last success</span>
                          <span title={lastSuccessAbs} className="fr-mono fr-age">
                            {ageStr}
                          </span>
                        </div>
                        <div className="fr-row fr-meta">
                          <span className="fr-meta-key">Next sync</span>
                          <span className="fr-mono">{nextStr}</span>
                        </div>
                        <div className="fr-row fr-meta">
                          <span className="fr-meta-key">Interval</span>
                          <span className="fr-mono">{intervalLabel}</span>
                        </div>
                        <div className="fr-row fr-meta">
                          <span className="fr-meta-key">Cycles done</span>
                          <span className="fr-mono fr-cycles">
                            <AnimatedNumber value={t.cycle_count} />
                          </span>
                        </div>
                      </div>

                      {t.last_error && (
                        <div className="fr-error" title={t.last_error}>
                          <span className="fr-error-icon">⚠</span>
                          <span className="fr-error-msg">
                            {t.last_error.slice(0, 90)}
                            {t.last_error.length > 90 ? "…" : ""}
                          </span>
                        </div>
                      )}
                    </div>
                  );
                },
              )}
            {!freshness && (
              <div className="freshness-empty">
                <span className="freshness-empty-spinner" />
                Loading freshness status…
              </div>
            )}
            {freshness && freshness.tracks &&
              Object.keys(freshness.tracks).length === 0 && (
                <div className="freshness-empty">No freshness tracks reported.</div>
              )}
          </div>
        </section>

        {/* Upload */}
        <section
          className={`upload-card stagger ${dragging ? "is-drag" : ""} ${
            uploading ? "is-uploading" : ""
          } ${uploadStage === "success" ? "is-success" : ""} ${
            uploadStage === "indexing" ? "is-indexing-card" : ""
          }`}
          style={{ "--d": "340ms" } as React.CSSProperties}
          onDragEnter={(e) => {
            e.preventDefault();
            setDragging(true);
          }}
          onDragOver={(e) => {
            e.preventDefault();
            if (!dragging) setDragging(true);
          }}
          onDragLeave={(e) => {
            e.preventDefault();
            if (e.currentTarget.contains(e.relatedTarget as Node)) return;
            setDragging(false);
          }}
          onDrop={(e) => {
            e.preventDefault();
            setDragging(false);
            if (!uploading) handleFiles(e.dataTransfer?.files || null);
          }}
        >
          <div className="upload-border" />
          <div className="upload-row">
            <div className="upload-icon">
              <svg viewBox="0 0 24 24" width="22" height="22" aria-hidden>
                <path
                  d="M12 3l5 5h-3v6h-4V8H7l5-5Zm-8 14h16v4H4v-4Z"
                  fill="currentColor"
                />
              </svg>
            </div>
            <div className="upload-text">
              <div className="upload-title">Upload a PDF document</div>
              <div className="upload-sub">
                Drop a file here, or click <b>Choose PDF</b>. New documents are
                automatically indexed and become searchable in the chat. Max
                100&nbsp;MB.
              </div>
            </div>
            <input
              ref={inputRef}
              type="file"
              accept="application/pdf,.pdf"
              className="hidden"
              onChange={(e) => handleFiles(e.target.files)}
              style={{ display: "none" }}
            />
            <button
              onClick={() => inputRef.current?.click()}
              disabled={uploading}
              className="upload-btn"
            >
              <span className="upload-btn-label">
                {uploading ? "Uploading…" : "Choose PDF"}
              </span>
              <span className="upload-btn-shine" />
            </button>
          </div>
          {uploading && (
            <div className="upload-progress">
              {/* Multi-stage tracker */}
              <div className="stage-track">
                <div
                  className={`stage-step ${
                    uploadStage === "uploading" ? "is-active" : "is-done"
                  }`}
                >
                  <span className="stage-num">1</span>
                  <span className="stage-name">Uploading</span>
                </div>
                <div
                  className={`stage-line ${
                    uploadStage !== "uploading" ? "is-filled" : ""
                  }`}
                />
                <div
                  className={`stage-step ${
                    uploadStage === "indexing"
                      ? "is-active"
                      : uploadStage === "success"
                        ? "is-done"
                        : ""
                  }`}
                >
                  <span className="stage-num">2</span>
                  <span className="stage-name">Indexing</span>
                </div>
                <div
                  className={`stage-line ${
                    uploadStage === "success" ? "is-filled" : ""
                  }`}
                />
                <div
                  className={`stage-step ${
                    uploadStage === "success" ? "is-active is-done" : ""
                  }`}
                >
                  <span className="stage-num">
                    {uploadStage === "success" ? (
                      <svg
                        viewBox="0 0 24 24"
                        width="12"
                        height="12"
                        aria-hidden
                      >
                        <path
                          d="M9 16.2 4.8 12l-1.4 1.4L9 19 21 7l-1.4-1.4L9 16.2Z"
                          fill="currentColor"
                        />
                      </svg>
                    ) : (
                      "3"
                    )}
                  </span>
                  <span className="stage-name">Ready</span>
                </div>
              </div>

              <div className="upload-progress-track">
                <div
                  className={`upload-progress-bar ${
                    uploadStage === "indexing" ? "is-indexing" : ""
                  } ${uploadStage === "success" ? "is-done" : ""}`}
                  style={{
                    width:
                      uploadStage === "uploading"
                        ? `${Math.max(uploadPct, 4)}%`
                        : "100%",
                  }}
                />
                <span className="bar-sparkle" aria-hidden />
              </div>

              <div className="upload-status">
                {uploadStage === "uploading" && (
                  <>
                    <span className="upload-status-ic">
                      <span className="spinner" />
                    </span>
                    <span className="upload-status-text">
                      {uploadPct}% · Sending <b>{uploadFileName}</b> to server
                    </span>
                  </>
                )}
                {uploadStage === "indexing" && (
                  <>
                    <span className="upload-status-ic indexing-ic">
                      <span className="chunk-dot" />
                      <span className="chunk-dot" />
                      <span className="chunk-dot" />
                    </span>
                    <span className="upload-status-text">
                      Indexing{" "}
                      <b>{uploadFileName}</b>{" "}
                      <span className="index-subtext">
                        · processing chunk&nbsp;
                        <span className="index-ticker">
                          {1 + (indexingTick % 32)}
                        </span>
                      </span>
                    </span>
                  </>
                )}
                {uploadStage === "success" && (
                  <>
                    <span className="upload-status-ic upload-ok-ic">
                      <svg
                        viewBox="0 0 24 24"
                        width="14"
                        height="14"
                        aria-hidden
                      >
                        <path
                          d="M9 16.2 4.8 12l-1.4 1.4L9 19 21 7l-1.4-1.4L9 16.2Z"
                          fill="currentColor"
                        />
                      </svg>
                    </span>
                    <span className="upload-status-text">
                      <b>{uploadFileName}</b> is now searchable in the chat
                    </span>
                  </>
                )}
              </div>
            </div>
          )}

          {/* Success confetti burst */}
          {successBurst && (
            <div className="success-burst" aria-hidden="true">
              {Array.from({ length: 14 }).map((_, i) => (
                <span
                  key={i}
                  className="spark"
                  style={
                    {
                      "--angle": `${(360 / 14) * i}deg`,
                      "--delay": `${i * 14}ms`,
                    } as React.CSSProperties
                  }
                />
              ))}
              <span className="burst-ring" />
            </div>
          )}
        </section>

        {/* Documents */}
        <section
          className="docs-card stagger"
          style={{ "--d": "430ms" } as React.CSSProperties}
        >
          <div className="docs-header">
            <div className="docs-title">
              Documents <span className="docs-count">({docs.length})</span>
            </div>
            {docs.length > 0 && (
              <div className="docs-meta">
                Indexed: {docs.filter((d) => d.indexed).length} · Pending:{" "}
                {docs.filter((d) => !d.indexed).length}
              </div>
            )}
          </div>
          <div className="docs-table-wrap">
            <div className="docs-table">
              <div className="docs-thead">
                <div className="col col-name">FILENAME</div>
                <div className="col col-size">SIZE</div>
                <div className="col col-date">UPLOADED</div>
                <div className="col col-badge">INDEXED</div>
                <div className="col col-action">ACTION</div>
              </div>
              {loading && docs.length === 0 && <DocumentsSkeleton />}
              {!loading && docs.length === 0 && (
                <div className="empty-state">
                  <div className="empty-icon">
                    <svg viewBox="0 0 24 24" width="36" height="36" aria-hidden>
                      <path
                        d="M6 2h9l5 5v15H6V2Zm8 1.5V8h4.5L14 3.5Z"
                        fill="currentColor"
                      />
                    </svg>
                  </div>
                  <div className="empty-title">No documents yet</div>
                  <div className="empty-sub">
                    Upload a PDF to get started — new files are indexed
                    automatically.
                  </div>
                </div>
              )}
              {docs.map((d, i) => {
                const isExiting = exitingRows.has(d.filename);
                const isDeleting = deletingName === d.filename;
                return (
                  <div
                    key={d.filename}
                    className={`doc-row ${isExiting ? "is-exiting" : ""} ${
                      isDeleting ? "is-deleting" : ""
                    }`}
                    style={
                      {
                        "--idx": String(i),
                      } as React.CSSProperties
                    }
                  >
                    <div className="col col-name">
                      <div className="doc-icon">
                        <svg
                          viewBox="0 0 24 24"
                          width="14"
                          height="14"
                          aria-hidden
                        >
                          <path
                            d="M6 2h9l5 5v15H6V2Zm8 1.5V8h4.5L14 3.5Z"
                            fill="currentColor"
                          />
                        </svg>
                      </div>
                      <span className="doc-name" title={d.filename}>
                        {d.filename}
                      </span>
                    </div>
                    <div className="col col-size">{fmtBytes(d.size)}</div>
                    <div className="col col-date">{fmtDate(d.mtime)}</div>
                    <div className="col col-badge">
                      {d.indexed ? (
                        <span className="badge badge-ok">
                          <span className="badge-dot" />
                          Indexed
                        </span>
                      ) : (
                        <span className="badge badge-pending">
                          <span className="badge-dot" />
                          Pending
                        </span>
                      )}
                    </div>
                    <div className="col col-action">
                      <button
                        onClick={() => setConfirm(d.filename)}
                        disabled={isDeleting}
                        className="del-btn"
                      >
                        {isDeleting ? (
                          <span className="del-loading">
                            <span className="d" />
                            <span className="d" />
                            <span className="d" />
                          </span>
                        ) : (
                          <>
                            <svg
                              viewBox="0 0 24 24"
                              width="14"
                              height="14"
                              aria-hidden
                            >
                              <path
                                d="M6 7h12l-1 13H7L6 7Zm3-3h6v2H9V4Z"
                                fill="currentColor"
                              />
                            </svg>
                            Delete
                          </>
                        )}
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </section>
      </main>

      {/* Confirmation modal */}
      {confirm && (
        <div
          className="modal-backdrop"
          onClick={() => {
            if (!deletingName) setConfirm(null);
          }}
        >
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-glow" />
            <div className="modal-icon">
              <svg viewBox="0 0 24 24" width="26" height="26" aria-hidden>
                <path
                  d="M12 2 1 21h22L12 2Zm1 7v6h-2V9h2Zm0 8v2h-2v-2h2Z"
                  fill="currentColor"
                />
              </svg>
            </div>
            <div className="modal-title">Delete this document?</div>
            <div className="modal-filename" title={confirm}>
              {confirm}
            </div>
            <div className="modal-hint">
              The document will be removed from the index and the chatbot will
              no longer use it when answering questions.
            </div>
            <div className="modal-actions">
              <button
                onClick={() => setConfirm(null)}
                disabled={!!deletingName}
                className="modal-cancel"
              >
                Cancel
              </button>
              <button
                onClick={() => performDelete(confirm)}
                disabled={!!deletingName}
                className="modal-del"
              >
                {deletingName === confirm ? "Removing…" : "Delete & Re-index"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Toast */}
      {toast && (
        <div key={toast.id} className={`toast toast-${toast.kind}`}>
          <span className="toast-ic">
            {toast.kind === "ok" ? (
              <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden>
                <path
                  d="M9 16.2 4.8 12l-1.4 1.4L9 19 21 7l-1.4-1.4L9 16.2Z"
                  fill="currentColor"
                />
              </svg>
            ) : (
              <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden>
                <path
                  d="M19 6.4 17.6 5 12 10.6 6.4 5 5 6.4 10.6 12 5 17.6 6.4 19 12 13.4 17.6 19 19 17.6 13.4 12 19 6.4Z"
                  fill="currentColor"
                />
              </svg>
            )}
          </span>
          <span className="toast-msg">{toast.msg}</span>
          <span className="toast-bar" />
        </div>
      )}

      <style jsx>{`
        /* ---------- Scroll container ---------- */
        .admin-root::-webkit-scrollbar {
          width: 10px;
        }
        .admin-root::-webkit-scrollbar-track {
          background: rgba(11, 16, 32, 0.6);
        }
        .admin-root::-webkit-scrollbar-thumb {
          background: linear-gradient(
            180deg,
            rgba(212, 160, 23, 0.4),
            rgba(184, 134, 11, 0.6)
          );
          border-radius: 999px;
          border: 2px solid rgba(11, 16, 32, 0.6);
          background-clip: padding-box;
        }
        .admin-root::-webkit-scrollbar-thumb:hover {
          background: linear-gradient(
            180deg,
            rgba(244, 211, 122, 0.55),
            rgba(212, 160, 23, 0.75)
          );
          background-clip: padding-box;
        }
        .admin-root {
          scrollbar-color: rgba(212, 160, 23, 0.5) rgba(11, 16, 32, 0.6);
          scrollbar-width: thin;
        }

        /* ---------- Splash ---------- */
        .stage-splash .splash {
          opacity: 1;
        }
        .stage-ready .splash {
          animation: splash-out 800ms cubic-bezier(0.85, 0, 0.15, 1) forwards;
          animation-delay: 400ms;
        }
        .splash-cover {
          transform-origin: center bottom;
        }
        .stage-ready .splash-cover {
          animation: cover-iris 820ms cubic-bezier(0.85, 0, 0.15, 1) forwards;
        }
        .splash-inner {
          position: relative;
          z-index: 2;
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 1rem;
          animation: splash-pop 700ms cubic-bezier(0.22, 1, 0.36, 1);
        }
        .splash-logo {
          position: relative;
          width: 88px;
          height: 88px;
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .splash-rune {
          position: absolute;
          border-radius: 50%;
          border: 1px solid rgba(212, 160, 23, 0.4);
        }
        .splash-rune-1 {
          width: 100%;
          height: 100%;
          border-style: dashed;
          animation: splash-spin 2.2s linear infinite;
        }
        .splash-rune-2 {
          width: 130%;
          height: 130%;
          border-color: rgba(212, 160, 23, 0.2);
          animation: splash-spin 3.5s linear infinite reverse;
        }
        .splash-mark {
          filter: drop-shadow(0 0 16px rgba(212, 160, 23, 0.6));
          animation: splash-breath 2.2s ease-in-out infinite;
        }
        .splash-bar {
          width: 140px;
          height: 2px;
          background: rgba(255, 255, 255, 0.08);
          overflow: hidden;
          border-radius: 2px;
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
          animation: bar-slide 1.2s ease-in-out infinite;
        }

        /* ---------- Background ---------- */
        .bg-grid {
          position: fixed;
          inset: 0;
          background-image: linear-gradient(
              rgba(212, 160, 23, 0.035) 1px,
              transparent 1px
            ),
            linear-gradient(
              90deg,
              rgba(212, 160, 23, 0.035) 1px,
              transparent 1px
            );
          background-size: 52px 52px;
          mask-image: radial-gradient(
            ellipse at 30% 20%,
            black 40%,
            transparent 85%
          );
          opacity: 0.55;
          animation: grid-drift 34s linear infinite;
          pointer-events: none;
          z-index: 0;
        }
        .bg-vignette {
          position: fixed;
          inset: 0;
          pointer-events: none;
          background: radial-gradient(
            1000px circle at 50% 120%,
            rgba(212, 160, 23, 0.05),
            transparent 60%
          );
          z-index: 0;
        }
        .bg-orb {
          position: fixed;
          border-radius: 50%;
          filter: blur(120px);
          opacity: 0.3;
          pointer-events: none;
          animation: orb-drift 22s ease-in-out infinite;
          z-index: 0;
        }
        .bg-orb-1 {
          width: 540px;
          height: 540px;
          top: -180px;
          left: -160px;
          background: radial-gradient(
            circle,
            rgba(212, 160, 23, 0.55),
            transparent 70%
          );
        }
        .bg-orb-2 {
          width: 460px;
          height: 460px;
          bottom: -180px;
          right: -160px;
          background: radial-gradient(
            circle,
            rgba(184, 134, 11, 0.45),
            transparent 70%
          );
          animation-delay: -8s;
          animation-duration: 26s;
        }
        .bg-orb-3 {
          width: 340px;
          height: 340px;
          top: 50%;
          left: 60%;
          background: radial-gradient(
            circle,
            rgba(88, 70, 180, 0.35),
            transparent 70%
          );
          animation-delay: -14s;
          animation-duration: 30s;
          opacity: 0.2;
        }
        .particles {
          position: fixed;
          inset: 0;
          overflow: hidden;
          pointer-events: none;
          z-index: 0;
        }
        .particle {
          position: absolute;
          background: rgba(212, 160, 23, 0.7);
          border-radius: 50%;
          box-shadow: 0 0 10px rgba(212, 160, 23, 0.8);
          animation: particle-rise linear infinite;
          opacity: 0;
        }

        /* ---------- Header ---------- */
        .admin-header {
          position: sticky;
          top: 0;
          z-index: 20;
          backdrop-filter: blur(18px);
          background: linear-gradient(
            180deg,
            rgba(11, 16, 32, 0.85),
            rgba(11, 16, 32, 0.55)
          );
          border-bottom: 1px solid rgba(212, 160, 23, 0.14);
          transform: translateY(-12px);
          opacity: 0;
          transition: transform 700ms cubic-bezier(0.22, 1, 0.36, 1),
            opacity 700ms ease;
          transition-delay: 100ms;
        }
        .admin-header.is-ready {
          transform: translateY(0);
          opacity: 1;
        }
        .admin-header-inner {
          max-width: 1180px;
          margin: 0 auto;
          padding: 0.9rem 1.75rem;
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 1rem;
        }
        @media (max-width: 640px) {
          .admin-header-inner {
            padding: 0.7rem 1rem;
            gap: 0.6rem;
          }
        }
        .admin-brand {
          display: flex;
          align-items: center;
          gap: 0.85rem;
          min-width: 0;
          flex: 1;
        }
        @media (max-width: 640px) {
          .admin-brand {
            gap: 0.55rem;
          }
        }
        .admin-brand-logo {
          position: relative;
          width: 48px;
          height: 48px;
          display: flex;
          align-items: center;
          justify-content: center;
          flex-shrink: 0;
        }
        @media (max-width: 640px) {
          .admin-brand-logo {
            width: 36px;
            height: 36px;
          }
        }
        .brand-ring-1,
        .brand-ring-2 {
          position: absolute;
          border-radius: 50%;
          pointer-events: none;
        }
        .brand-ring-1 {
          width: 110%;
          height: 110%;
          border: 1px dashed rgba(212, 160, 23, 0.5);
          animation: ring-spin 14s linear infinite;
        }
        .brand-ring-2 {
          width: 140%;
          height: 140%;
          border: 1px solid rgba(212, 160, 23, 0.12);
          animation: ring-spin 28s linear infinite reverse;
        }
        .admin-brand-title {
          font-size: 1.05rem;
          font-weight: 800;
          color: #ffffff;
          letter-spacing: -0.01em;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        @media (max-width: 640px) {
          .admin-brand-title {
            font-size: 0.92rem;
          }
        }
        .brand-gold {
          background: linear-gradient(180deg, #f4d37a 0%, #d4a017 55%, #b8860b);
          background-clip: text;
          -webkit-background-clip: text;
          color: transparent;
        }
        .admin-brand-sub {
          margin-top: 2px;
          font-size: 10px;
          letter-spacing: 0.24em;
          font-weight: 600;
          color: rgba(254, 243, 199, 0.6);
          display: inline-flex;
          align-items: center;
          gap: 6px;
          white-space: nowrap;
        }
        @media (max-width: 640px) {
          .admin-brand-sub {
            font-size: 9px;
            letter-spacing: 0.18em;
          }
        }
        .live-dot {
          display: inline-block;
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: #34d399;
          box-shadow: 0 0 8px #34d399;
          animation: dot-pulse 1.8s ease-in-out infinite;
        }
        .admin-user {
          display: flex;
          align-items: center;
          gap: 0.9rem;
          flex-shrink: 0;
        }
        @media (max-width: 640px) {
          .admin-user {
            gap: 0.4rem;
          }
          /* Hide the email on mobile — Sign Out button stays visible */
          .admin-user-email {
            display: none !important;
          }
        }
        .admin-user-email {
          display: inline-flex;
          align-items: center;
          gap: 0.4rem;
          font-size: 0.78rem;
          color: rgba(254, 243, 199, 0.7);
        }
        .admin-signout {
          display: inline-flex;
          align-items: center;
          gap: 0.4rem;
          font-size: 0.78rem;
          padding: 0.5rem 0.85rem;
          border-radius: 10px;
          background: rgba(212, 160, 23, 0.08);
          color: #f4d37a;
          border: 1px solid rgba(212, 160, 23, 0.3);
          cursor: pointer;
          transition: background 180ms ease, transform 180ms ease,
            box-shadow 180ms ease;
          flex-shrink: 0;
        }
        @media (max-width: 640px) {
          .admin-signout {
            padding: 0.45rem 0.6rem;
            font-size: 0.72rem;
          }
          .admin-signout-label {
            display: none;
          }
        }
        .admin-signout:hover {
          background: rgba(212, 160, 23, 0.18);
          transform: translateY(-1px);
          box-shadow: 0 10px 20px -10px rgba(212, 160, 23, 0.4);
        }

        /* ---------- Main ---------- */
        .admin-main {
          position: relative;
          z-index: 1;
          max-width: 1180px;
          margin: 0 auto;
          padding: 2.25rem 1.75rem 4rem;
        }
        @media (max-width: 640px) {
          .admin-main {
            padding: 1.5rem 1rem 3rem;
          }
        }
        .stagger {
          opacity: 0;
          transform: translateY(16px);
        }
        .admin-main.is-ready .stagger {
          animation: stagger-in 720ms cubic-bezier(0.22, 1, 0.36, 1) forwards;
          animation-delay: var(--d, 0ms);
        }

        /* Index error banner */
        .idx-error-banner {
          display: flex;
          align-items: center;
          gap: 14px;
          padding: 14px 18px;
          margin-bottom: 1.25rem;
          border-radius: 14px;
          background: linear-gradient(
            135deg,
            rgba(127, 29, 29, 0.26),
            rgba(76, 5, 25, 0.22)
          );
          border: 1px solid rgba(244, 63, 94, 0.35);
          box-shadow: 0 10px 28px -16px rgba(244, 63, 94, 0.4);
          animation: banner-in 300ms cubic-bezier(0.22, 1, 0.36, 1);
        }
        .idx-error-icon {
          flex-shrink: 0;
          width: 32px;
          height: 32px;
          border-radius: 8px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          background: rgba(244, 63, 94, 0.2);
          color: #fca5a5;
          border: 1px solid rgba(244, 63, 94, 0.3);
        }
        .idx-error-body { flex: 1; min-width: 0; }
        .idx-error-title {
          font-size: 0.82rem;
          font-weight: 700;
          color: #fecaca;
          letter-spacing: 0.01em;
        }
        .idx-error-msg {
          margin-top: 2px;
          font-size: 0.78rem;
          color: rgba(254, 202, 202, 0.85);
          line-height: 1.45;
          word-break: break-word;
        }
        .idx-error-retry {
          flex-shrink: 0;
          display: inline-flex;
          align-items: center;
          gap: 6px;
          font-size: 0.7rem;
          font-weight: 600;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: #fbbf24;
          padding: 6px 10px;
          border-radius: 999px;
          background: rgba(234, 179, 8, 0.14);
          border: 1px solid rgba(234, 179, 8, 0.3);
        }
        .idx-error-retry-dot {
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: #fbbf24;
          box-shadow: 0 0 6px #fbbf24;
          animation: dot-pulse 1.6s ease-in-out infinite;
        }
        @keyframes banner-in {
          from { opacity: 0; transform: translateY(-6px); }
          to { opacity: 1; transform: translateY(0); }
        }

        /* ---------- KPI ---------- */
        @media (max-width: 640px) {
          .kpi-row {
            grid-template-columns: 1fr 1fr !important;
            gap: 0.65rem !important;
          }
          .kpi-card {
            padding: 0.8rem 0.9rem !important;
            gap: 0.6rem !important;
          }
          .kpi-icon {
            width: 32px !important;
            height: 32px !important;
          }
          .kpi-label {
            font-size: 9px !important;
            letter-spacing: 0.14em !important;
          }
          .kpi-value {
            font-size: 1.1rem !important;
          }
          .kpi-value-mono {
            font-size: 0.72rem !important;
          }
          .kpi-refresh {
            grid-column: span 2;
          }
          .refresh-btn {
            min-height: 48px !important;
            width: 100%;
          }
        }

        .kpi-row {
          display: grid;
          grid-template-columns: 1.4fr 1fr 1fr auto;
          gap: 1rem;
          margin-bottom: 1.5rem;
        }
        @media (max-width: 800px) {
          .kpi-row {
            grid-template-columns: 1fr 1fr;
          }
          .kpi-refresh {
            grid-column: span 2;
          }
        }
        .kpi-card {
          position: relative;
          overflow: hidden;
          padding: 1.1rem 1.25rem;
          border-radius: 16px;
          background: linear-gradient(
            160deg,
            rgba(19, 26, 46, 0.9) 0%,
            rgba(14, 20, 38, 0.9) 100%
          );
          border: 1px solid rgba(212, 160, 23, 0.14);
          display: flex;
          align-items: center;
          gap: 0.9rem;
          transition: transform 260ms cubic-bezier(0.22, 1, 0.36, 1),
            border-color 260ms ease, box-shadow 260ms ease;
        }
        .kpi-card:hover {
          transform: translateY(-2px);
          border-color: rgba(212, 160, 23, 0.3);
          box-shadow: 0 20px 40px -22px rgba(212, 160, 23, 0.35);
        }
        .kpi-glow {
          position: absolute;
          inset: -1px;
          pointer-events: none;
          border-radius: 16px;
          background: radial-gradient(
            260px circle at 20% 0%,
            rgba(212, 160, 23, 0.15),
            transparent 60%
          );
        }
        .kpi-icon {
          position: relative;
          width: 40px;
          height: 40px;
          border-radius: 12px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: rgba(254, 243, 199, 0.7);
          background: rgba(148, 163, 184, 0.12);
          border: 1px solid rgba(148, 163, 184, 0.14);
        }
        .kpi-icon-gold {
          color: #f4d37a;
          background: rgba(212, 160, 23, 0.12);
          border-color: rgba(212, 160, 23, 0.3);
          box-shadow: inset 0 0 16px rgba(212, 160, 23, 0.08);
        }
        .kpi-meta {
          position: relative;
          min-width: 0;
        }
        .kpi-label {
          font-size: 10px;
          letter-spacing: 0.22em;
          font-weight: 600;
          color: rgba(254, 243, 199, 0.55);
          margin-bottom: 2px;
        }
        .kpi-value {
          font-size: 1.4rem;
          font-weight: 700;
          color: #ffffff;
          letter-spacing: -0.01em;
        }
        .kpi-value-mono {
          font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas,
            "Liberation Mono", monospace;
          font-size: 0.9rem;
          color: rgba(254, 243, 199, 0.9);
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          max-width: 260px;
        }
        .kpi-refresh {
          display: flex;
          align-items: stretch;
        }
        .refresh-btn {
          display: inline-flex;
          align-items: center;
          gap: 0.5rem;
          padding: 0 1.15rem;
          height: 100%;
          background: linear-gradient(
            160deg,
            rgba(19, 26, 46, 0.9),
            rgba(14, 20, 38, 0.9)
          );
          border: 1px solid rgba(212, 160, 23, 0.22);
          border-radius: 16px;
          color: #f4d37a;
          font-size: 0.8rem;
          font-weight: 600;
          letter-spacing: 0.05em;
          cursor: pointer;
          transition: background 180ms ease, transform 180ms ease,
            box-shadow 220ms ease;
          min-height: 72px;
        }
        .refresh-btn:hover:not(:disabled) {
          transform: translateY(-2px);
          box-shadow: 0 18px 32px -18px rgba(212, 160, 23, 0.4);
        }
        .refresh-btn:disabled {
          opacity: 0.65;
          cursor: not-allowed;
        }
        .refresh-btn.spin svg {
          animation: spin-full 900ms linear infinite;
        }

        /* ============================================================
           Data Freshness panel — premium animated edition
           ============================================================ */
        .freshness-card {
          position: relative;
          padding: 1.4rem 1.5rem 1.5rem;
          border-radius: 22px;
          background:
            linear-gradient(
              165deg,
              rgba(18, 24, 44, 0.96) 0%,
              rgba(11, 16, 32, 0.96) 100%
            );
          border: 1px solid rgba(212, 160, 23, 0.22);
          box-shadow:
            0 22px 50px -22px rgba(0, 0, 0, 0.78),
            0 4px 18px -8px rgba(212, 160, 23, 0.18),
            inset 0 1px 0 rgba(255, 255, 255, 0.04);
          overflow: hidden;
          isolation: isolate;
        }
        .fr-card-glow {
          position: absolute;
          inset: -2px;
          border-radius: 24px;
          background: conic-gradient(
            from 0deg at 50% 50%,
            rgba(212, 160, 23, 0) 0deg,
            rgba(212, 160, 23, 0.18) 90deg,
            rgba(212, 160, 23, 0) 180deg,
            rgba(74, 222, 128, 0.16) 270deg,
            rgba(212, 160, 23, 0) 360deg
          );
          filter: blur(14px);
          opacity: 0.55;
          z-index: -1;
          animation: fr-conic-spin 14s linear infinite;
          pointer-events: none;
        }

        .freshness-head {
          display: flex;
          flex-direction: column;
          gap: 0.35rem;
          margin-bottom: 1.1rem;
        }
        .freshness-title {
          display: inline-flex;
          align-items: center;
          gap: 0.6rem;
          font-size: 11px;
          letter-spacing: 0.22em;
          font-weight: 700;
          color: #f4d37a;
        }
        .freshness-title-text {
          background: linear-gradient(
            90deg,
            #f4d37a 0%,
            #fde68a 50%,
            #d4a017 100%
          );
          background-clip: text;
          -webkit-background-clip: text;
          color: transparent;
        }
        .freshness-dot {
          width: 8px;
          height: 8px;
          border-radius: 50%;
          background: #f4d37a;
          box-shadow:
            0 0 10px #f4d37a,
            0 0 22px rgba(244, 211, 122, 0.55);
          animation: fr-dot-breathe 2.2s ease-in-out infinite;
        }
        .freshness-live-tag {
          margin-left: auto;
          display: inline-flex;
          align-items: center;
          gap: 6px;
          font-size: 10px;
          letter-spacing: 0.2em;
          font-weight: 700;
          padding: 3px 9px 3px 8px;
          border-radius: 9999px;
          background: linear-gradient(
            180deg,
            rgba(74, 222, 128, 0.18),
            rgba(74, 222, 128, 0.08)
          );
          color: #86efac;
          border: 1px solid rgba(74, 222, 128, 0.35);
          box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.08);
        }
        .live-pulse {
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: #4ade80;
          box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.6);
          animation: fr-live-pulse 1.6s ease-out infinite;
        }
        .freshness-sub {
          font-size: 12px;
          color: rgba(254, 243, 199, 0.6);
          letter-spacing: 0.01em;
        }

        .freshness-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(244px, 1fr));
          gap: 1rem;
        }

        /* ---------- Card per track ---------- */
        .freshness-item {
          position: relative;
          padding: 1rem 1.1rem 1rem;
          border-radius: 16px;
          background:
            linear-gradient(
              160deg,
              rgba(10, 14, 28, 0.85) 0%,
              rgba(8, 12, 24, 0.85) 100%
            );
          border: 1px solid rgba(255, 255, 255, 0.06);
          display: flex;
          flex-direction: column;
          gap: 0.6rem;
          transition:
            border-color 320ms ease,
            transform 380ms cubic-bezier(0.22, 1, 0.36, 1),
            box-shadow 380ms cubic-bezier(0.22, 1, 0.36, 1);
          overflow: hidden;
          isolation: isolate;
          opacity: 0;
          transform: translateY(8px);
          animation: fr-item-enter 620ms cubic-bezier(0.22, 1, 0.36, 1) forwards;
          animation-delay: var(--d, 0ms);
          will-change: transform;
        }
        .freshness-item:hover {
          transform: translateY(-3px);
          box-shadow:
            0 18px 38px -14px rgba(0, 0, 0, 0.65),
            0 4px 14px -6px rgba(212, 160, 23, 0.22);
        }

        .fr-item-glow {
          position: absolute;
          inset: -40% -30% auto auto;
          width: 220px;
          height: 220px;
          border-radius: 50%;
          background: radial-gradient(
            circle,
            rgba(212, 160, 23, 0.18) 0%,
            transparent 60%
          );
          filter: blur(28px);
          opacity: 0;
          transition: opacity 420ms ease;
          pointer-events: none;
          z-index: -1;
        }
        .freshness-item:hover .fr-item-glow {
          opacity: 1;
        }

        /* Status-aware border + ambient glow */
        .fr-item-border {
          position: absolute;
          inset: 0;
          border-radius: 16px;
          padding: 1px;
          background: linear-gradient(
            135deg,
            rgba(255, 255, 255, 0.08),
            rgba(255, 255, 255, 0.02)
          );
          -webkit-mask:
            linear-gradient(#000 0 0) content-box,
            linear-gradient(#000 0 0);
          -webkit-mask-composite: xor;
                  mask-composite: exclude;
          pointer-events: none;
          transition: opacity 320ms ease;
        }
        .freshness-item.is-fresh {
          border-color: rgba(74, 222, 128, 0.35);
          box-shadow: inset 0 0 0 1px rgba(74, 222, 128, 0.06);
        }
        .freshness-item.is-fresh .fr-item-border {
          background: linear-gradient(
            135deg,
            rgba(74, 222, 128, 0.55),
            rgba(74, 222, 128, 0.06) 60%,
            rgba(74, 222, 128, 0.4)
          );
        }
        .freshness-item.is-aging {
          border-color: rgba(251, 191, 36, 0.45);
        }
        .freshness-item.is-aging .fr-item-border {
          background: linear-gradient(
            135deg,
            rgba(251, 191, 36, 0.55),
            rgba(251, 191, 36, 0.08) 60%,
            rgba(251, 191, 36, 0.45)
          );
        }
        .freshness-item.is-stale {
          border-color: rgba(244, 63, 94, 0.55);
        }
        .freshness-item.is-stale .fr-item-border {
          background: linear-gradient(
            135deg,
            rgba(244, 63, 94, 0.55),
            rgba(244, 63, 94, 0.08) 60%,
            rgba(244, 63, 94, 0.45)
          );
        }
        .freshness-item.is-never_run {
          border-color: rgba(148, 163, 184, 0.35);
        }
        .freshness-item.is-running {
          /* breathing glow while a sync is in flight */
          animation:
            fr-item-enter 620ms cubic-bezier(0.22, 1, 0.36, 1) forwards,
            fr-running-glow 2.4s ease-in-out infinite 620ms;
        }

        /* ---------- Rows ---------- */
        .fr-row {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 0.5rem;
        }
        .fr-head-row {
          margin-bottom: 0.1rem;
        }
        .fr-rows {
          display: flex;
          flex-direction: column;
          gap: 0.35rem;
          margin-top: 0.15rem;
        }
        .fr-name {
          font-size: 13.5px;
          font-weight: 700;
          color: #ffffff;
          letter-spacing: 0.005em;
        }

        /* ---------- Status pill ---------- */
        .fr-pill {
          position: relative;
          display: inline-flex;
          align-items: center;
          gap: 6px;
          font-size: 9.5px;
          font-weight: 700;
          letter-spacing: 0.18em;
          padding: 4px 10px 4px 10px;
          border-radius: 9999px;
          background: rgba(255, 255, 255, 0.06);
          color: rgba(254, 243, 199, 0.7);
          border: 1px solid rgba(255, 255, 255, 0.08);
          transition:
            background-color 320ms ease,
            color 320ms ease,
            border-color 320ms ease,
            box-shadow 320ms ease;
        }
        .fr-pill.is-fresh {
          background: rgba(74, 222, 128, 0.16);
          color: #86efac;
          border-color: rgba(74, 222, 128, 0.4);
          box-shadow: 0 0 14px -4px rgba(74, 222, 128, 0.5);
        }
        .fr-pill.is-aging {
          background: rgba(251, 191, 36, 0.2);
          color: #fbbf24;
          border-color: rgba(251, 191, 36, 0.5);
          box-shadow: 0 0 14px -4px rgba(251, 191, 36, 0.5);
        }
        .fr-pill.is-stale {
          background: rgba(244, 63, 94, 0.2);
          color: #fda4af;
          border-color: rgba(244, 63, 94, 0.5);
          box-shadow: 0 0 14px -4px rgba(244, 63, 94, 0.5);
          animation: fr-pill-attention 2.2s ease-in-out infinite;
        }
        .fr-pill.is-never_run {
          background: rgba(148, 163, 184, 0.18);
          color: #cbd5e1;
        }
        .fr-pill-dot {
          width: 5px;
          height: 5px;
          border-radius: 50%;
          background: currentColor;
          box-shadow: 0 0 6px currentColor;
          animation: fr-syncing-dot 0.9s ease-in-out infinite;
        }

        /* ---------- Progress bar ---------- */
        .fr-bar {
          position: relative;
          height: 4px;
          width: 100%;
          background: rgba(255, 255, 255, 0.05);
          border-radius: 9999px;
          overflow: hidden;
        }
        .fr-bar-fill {
          position: absolute;
          inset: 0 auto 0 0;
          height: 100%;
          width: 0%;
          background: linear-gradient(
            90deg,
            rgba(212, 160, 23, 0.55),
            #f4d37a 50%,
            rgba(212, 160, 23, 0.55)
          );
          border-radius: 9999px;
          transition: width 700ms cubic-bezier(0.22, 1, 0.36, 1);
        }
        .freshness-item.is-fresh .fr-bar-fill {
          background: linear-gradient(
            90deg,
            rgba(74, 222, 128, 0.55),
            #4ade80 50%,
            rgba(74, 222, 128, 0.55)
          );
        }
        .freshness-item.is-aging .fr-bar-fill {
          background: linear-gradient(
            90deg,
            rgba(251, 191, 36, 0.55),
            #fbbf24 50%,
            rgba(251, 191, 36, 0.55)
          );
        }
        .freshness-item.is-stale .fr-bar-fill {
          background: linear-gradient(
            90deg,
            rgba(244, 63, 94, 0.55),
            #f43f5e 50%,
            rgba(244, 63, 94, 0.55)
          );
        }
        .fr-bar-fill.is-indeterminate {
          background: linear-gradient(
            90deg,
            transparent,
            #f4d37a,
            transparent
          );
          width: 35% !important;
          animation: fr-bar-indet 1.2s ease-in-out infinite;
        }
        .fr-bar-shine {
          position: absolute;
          top: 0;
          left: -40%;
          width: 40%;
          height: 100%;
          background: linear-gradient(
            90deg,
            transparent,
            rgba(255, 255, 255, 0.45),
            transparent
          );
          animation: fr-bar-shine 3.6s ease-in-out infinite;
        }

        /* ---------- Meta rows ---------- */
        .fr-meta {
          font-size: 11.5px;
          color: rgba(254, 243, 199, 0.55);
          letter-spacing: 0.005em;
        }
        .fr-meta-key {
          opacity: 0.85;
        }
        .fr-mono {
          font-family: ui-monospace, "SF Mono", Menlo, monospace;
          color: rgba(254, 243, 199, 0.92);
          font-weight: 600;
          letter-spacing: 0.01em;
        }
        .fr-age {
          color: #f4d37a;
          text-shadow: 0 0 12px rgba(244, 211, 122, 0.4);
        }
        .fr-cycles {
          color: #ffffff;
          font-weight: 700;
        }

        /* ---------- Error strip ---------- */
        .fr-error {
          display: flex;
          align-items: flex-start;
          gap: 6px;
          font-size: 11px;
          color: #fecaca;
          background: linear-gradient(
            180deg,
            rgba(244, 63, 94, 0.18),
            rgba(190, 18, 60, 0.12)
          );
          border: 1px solid rgba(244, 63, 94, 0.4);
          border-radius: 9px;
          padding: 7px 9px;
          margin-top: 4px;
          animation: fr-shake 600ms ease;
        }
        .fr-error-icon {
          font-size: 13px;
          line-height: 1;
        }
        .fr-error-msg {
          line-height: 1.35;
        }

        /* ---------- Empty state ---------- */
        .freshness-empty {
          font-size: 12px;
          color: rgba(254, 243, 199, 0.55);
          padding: 0.6rem 0.25rem;
          display: inline-flex;
          align-items: center;
          gap: 0.6rem;
        }
        .freshness-empty-spinner {
          width: 12px;
          height: 12px;
          border-radius: 50%;
          border: 2px solid rgba(244, 211, 122, 0.25);
          border-top-color: #f4d37a;
          animation: spin-full 800ms linear infinite;
        }

        /* ---------- Keyframes ---------- */
        @keyframes fr-conic-spin {
          to { transform: rotate(360deg); }
        }
        @keyframes fr-dot-breathe {
          0%, 100% {
            transform: scale(1);
            box-shadow: 0 0 10px #f4d37a, 0 0 22px rgba(244, 211, 122, 0.55);
          }
          50% {
            transform: scale(1.18);
            box-shadow: 0 0 14px #f4d37a, 0 0 30px rgba(244, 211, 122, 0.7);
          }
        }
        @keyframes fr-live-pulse {
          0% { box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.55); }
          70% { box-shadow: 0 0 0 7px rgba(74, 222, 128, 0); }
          100% { box-shadow: 0 0 0 0 rgba(74, 222, 128, 0); }
        }
        @keyframes fr-item-enter {
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
        @keyframes fr-running-glow {
          0%, 100% {
            box-shadow:
              0 0 0 0 rgba(244, 211, 122, 0.0),
              inset 0 0 0 1px rgba(244, 211, 122, 0.18);
          }
          50% {
            box-shadow:
              0 0 26px -6px rgba(244, 211, 122, 0.45),
              inset 0 0 0 1px rgba(244, 211, 122, 0.32);
          }
        }
        @keyframes fr-pill-attention {
          0%, 100% { transform: scale(1); }
          50% { transform: scale(1.04); }
        }
        @keyframes fr-syncing-dot {
          0%, 100% { transform: scale(1); opacity: 0.7; }
          50% { transform: scale(1.5); opacity: 1; }
        }
        @keyframes fr-bar-indet {
          0%   { transform: translateX(-120%); }
          100% { transform: translateX(380%); }
        }
        @keyframes fr-bar-shine {
          0%   { left: -40%; }
          60%, 100% { left: 110%; }
        }
        @keyframes fr-shake {
          0%, 100% { transform: translateX(0); }
          25% { transform: translateX(-3px); }
          50% { transform: translateX(3px); }
          75% { transform: translateX(-2px); }
        }

        /* ---------- Upload ---------- */
        .upload-card {
          position: relative;
          padding: 1.5rem 1.5rem 1.75rem;
          border-radius: 18px;
          background: linear-gradient(
            165deg,
            rgba(14, 20, 38, 0.92),
            rgba(11, 16, 32, 0.92)
          );
          overflow: hidden;
          margin-bottom: 1.75rem;
          transition: transform 220ms ease, box-shadow 260ms ease;
        }
        .upload-card:hover {
          transform: translateY(-1px);
          box-shadow: 0 20px 40px -22px rgba(212, 160, 23, 0.25);
        }
        .upload-card.is-drag {
          box-shadow: 0 0 0 3px rgba(212, 160, 23, 0.4),
            0 30px 60px -20px rgba(212, 160, 23, 0.35);
        }
        .upload-border {
          position: absolute;
          inset: 0;
          border-radius: 18px;
          border: 1.5px dashed rgba(212, 160, 23, 0.35);
          pointer-events: none;
          transition: border-color 220ms ease;
        }
        .upload-card.is-drag .upload-border {
          border-color: #f4d37a;
          animation: dash-march 16s linear infinite;
          background-image: repeating-linear-gradient(
            45deg,
            transparent 0 8px,
            rgba(212, 160, 23, 0.05) 8px 16px
          );
        }
        .upload-card.is-uploading .upload-border {
          border-style: solid;
          border-color: rgba(212, 160, 23, 0.45);
        }
        .upload-card.is-indexing-card {
          box-shadow: 0 0 0 1px rgba(212, 160, 23, 0.3),
            0 30px 60px -22px rgba(212, 160, 23, 0.35);
        }
        .upload-card.is-indexing-card .upload-border {
          border-color: rgba(212, 160, 23, 0.7);
          animation: pulse-border 2s ease-in-out infinite;
        }
        .upload-card.is-success {
          box-shadow: 0 0 0 1px rgba(52, 211, 153, 0.45),
            0 30px 60px -20px rgba(52, 211, 153, 0.35);
        }
        .upload-card.is-success .upload-border {
          border-color: rgba(52, 211, 153, 0.6);
          border-style: solid;
        }
        @keyframes pulse-border {
          0%, 100% {
            opacity: 0.7;
          }
          50% {
            opacity: 1;
          }
        }
        .upload-row {
          display: flex;
          align-items: center;
          gap: 1.25rem;
          position: relative;
          z-index: 1;
        }
        @media (max-width: 640px) {
          .upload-row {
            flex-direction: column;
            align-items: stretch;
          }
        }
        .upload-icon {
          flex-shrink: 0;
          width: 52px;
          height: 52px;
          border-radius: 14px;
          background: rgba(212, 160, 23, 0.12);
          border: 1px solid rgba(212, 160, 23, 0.3);
          display: flex;
          align-items: center;
          justify-content: center;
          color: #f4d37a;
          animation: icon-float 4s ease-in-out infinite;
        }
        .upload-text {
          flex: 1;
          min-width: 0;
        }
        .upload-title {
          font-size: 0.98rem;
          font-weight: 700;
          color: #ffffff;
        }
        .upload-sub {
          margin-top: 4px;
          font-size: 0.8125rem;
          color: rgba(254, 243, 199, 0.65);
          line-height: 1.55;
        }
        .upload-btn {
          position: relative;
          overflow: hidden;
          padding: 0.85rem 1.6rem;
          border-radius: 12px;
          font-weight: 700;
          font-size: 0.9rem;
          background: linear-gradient(
            180deg,
            #f4d37a 0%,
            #d4a017 55%,
            #b8860b 100%
          );
          color: #1a1307;
          border: 0;
          cursor: pointer;
          transition: transform 200ms ease, filter 200ms ease,
            box-shadow 260ms ease;
          box-shadow:
            0 10px 24px -12px rgba(212, 160, 23, 0.7),
            inset 0 1px 0 rgba(255, 255, 255, 0.35);
          white-space: nowrap;
        }
        .upload-btn:hover:not(:disabled) {
          filter: brightness(1.08);
          transform: translateY(-2px);
          box-shadow: 0 18px 36px -14px rgba(212, 160, 23, 0.7);
        }
        .upload-btn:disabled {
          filter: saturate(0.7);
          cursor: not-allowed;
        }
        .upload-btn-label {
          position: relative;
          z-index: 1;
        }
        .upload-btn-shine {
          position: absolute;
          top: 0;
          left: -120%;
          width: 80%;
          height: 100%;
          background: linear-gradient(
            100deg,
            transparent,
            rgba(255, 255, 255, 0.5),
            transparent
          );
          transform: skewX(-18deg);
          animation: shine 3.2s ease-in-out infinite;
        }
        .upload-progress {
          position: relative;
          z-index: 1;
          margin-top: 1.25rem;
        }

        /* Multi-step stage tracker */
        .stage-track {
          display: flex;
          align-items: center;
          gap: 0.5rem;
          margin-bottom: 0.9rem;
        }
        .stage-step {
          display: inline-flex;
          align-items: center;
          gap: 0.45rem;
          padding: 0.28rem 0.7rem 0.28rem 0.4rem;
          border-radius: 999px;
          background: rgba(148, 163, 184, 0.08);
          border: 1px solid rgba(148, 163, 184, 0.18);
          transition: all 320ms cubic-bezier(0.22, 1, 0.36, 1);
        }
        .stage-step .stage-num {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 20px;
          height: 20px;
          border-radius: 50%;
          background: rgba(148, 163, 184, 0.18);
          color: rgba(254, 243, 199, 0.5);
          font-size: 0.72rem;
          font-weight: 700;
          transition: background 320ms ease, color 320ms ease,
            box-shadow 320ms ease;
        }
        .stage-step .stage-name {
          font-size: 0.72rem;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: rgba(254, 243, 199, 0.55);
          font-weight: 600;
        }
        .stage-step.is-active {
          background: rgba(212, 160, 23, 0.14);
          border-color: rgba(212, 160, 23, 0.5);
          box-shadow: 0 0 0 4px rgba(212, 160, 23, 0.1),
            0 6px 20px -10px rgba(212, 160, 23, 0.6);
        }
        .stage-step.is-active .stage-num {
          background: linear-gradient(180deg, #f4d37a, #d4a017);
          color: #1a1307;
          box-shadow: 0 0 12px rgba(212, 160, 23, 0.7);
          animation: pulse-soft 1.6s ease-in-out infinite;
        }
        .stage-step.is-active .stage-name {
          color: #f4d37a;
        }
        .stage-step.is-done .stage-num {
          background: linear-gradient(180deg, #34d399, #059669);
          color: #022c1d;
          box-shadow: 0 0 10px rgba(52, 211, 153, 0.55);
          animation: none;
        }
        .stage-step.is-done .stage-name {
          color: rgba(167, 243, 208, 0.9);
        }
        .stage-step.is-done.is-active .stage-num {
          background: linear-gradient(180deg, #34d399, #059669);
        }
        .stage-line {
          flex: 1;
          height: 2px;
          border-radius: 2px;
          background: rgba(148, 163, 184, 0.15);
          position: relative;
          overflow: hidden;
        }
        .stage-line::after {
          content: "";
          position: absolute;
          inset: 0;
          width: 0%;
          background: linear-gradient(90deg, #f4d37a, #34d399);
          transition: width 500ms cubic-bezier(0.22, 1, 0.36, 1);
        }
        .stage-line.is-filled::after {
          width: 100%;
        }

        .upload-progress-track {
          height: 8px;
          border-radius: 10px;
          overflow: hidden;
          background: rgba(212, 160, 23, 0.08);
          position: relative;
          box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.25);
        }
        .upload-progress-bar {
          height: 100%;
          border-radius: 10px;
          background: linear-gradient(90deg, #e9c464, #d4a017, #b8860b);
          box-shadow: 0 0 16px rgba(212, 160, 23, 0.7);
          transition: width 320ms cubic-bezier(0.22, 1, 0.36, 1);
          position: relative;
        }
        .upload-progress-bar::after {
          content: "";
          position: absolute;
          inset: 0;
          background: linear-gradient(
            90deg,
            transparent,
            rgba(255, 255, 255, 0.5),
            transparent
          );
          transform: translateX(-100%);
          animation: bar-sheen 1.6s linear infinite;
        }
        .upload-progress-bar.is-indexing {
          background: linear-gradient(
            90deg,
            #f4d37a 0%,
            #d4a017 50%,
            #f4d37a 100%
          );
          background-size: 220% 100%;
          animation: index-pulse 1.6s ease-in-out infinite;
        }
        .upload-progress-bar.is-done {
          background: linear-gradient(90deg, #34d399, #059669, #34d399);
          box-shadow: 0 0 22px rgba(52, 211, 153, 0.7);
        }
        .bar-sparkle {
          position: absolute;
          top: -3px;
          left: 0;
          width: 14px;
          height: 14px;
          border-radius: 50%;
          background: radial-gradient(circle, #fff 0%, #f4d37a 35%, transparent 70%);
          pointer-events: none;
          opacity: 0;
          transition: opacity 200ms ease;
        }

        .upload-status {
          margin-top: 10px;
          display: flex;
          align-items: center;
          gap: 0.55rem;
          font-size: 0.78rem;
          color: rgba(254, 243, 199, 0.8);
          min-height: 22px;
        }
        .upload-status-text b {
          color: #f4d37a;
          font-weight: 600;
        }
        .index-subtext {
          color: rgba(254, 243, 199, 0.55);
        }
        .index-ticker {
          display: inline-block;
          min-width: 18px;
          font-variant-numeric: tabular-nums;
          color: #f4d37a;
          font-weight: 700;
          animation: ticker-flash 0.42s ease-out;
          animation-iteration-count: infinite;
        }
        .upload-status-ic {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 22px;
          height: 22px;
          border-radius: 50%;
        }
        .spinner {
          width: 14px;
          height: 14px;
          border-radius: 50%;
          border: 2px solid rgba(212, 160, 23, 0.25);
          border-top-color: #f4d37a;
          animation: spin-full 800ms linear infinite;
        }
        .indexing-ic {
          display: inline-flex;
          gap: 3px;
        }
        .chunk-dot {
          width: 4px;
          height: 4px;
          border-radius: 50%;
          background: #f4d37a;
          animation: chunk-bounce 1s ease-in-out infinite;
        }
        .chunk-dot:nth-child(2) {
          animation-delay: 120ms;
        }
        .chunk-dot:nth-child(3) {
          animation-delay: 240ms;
        }
        .upload-ok-ic {
          background: rgba(52, 211, 153, 0.15);
          color: #34d399;
          border: 1px solid rgba(52, 211, 153, 0.4);
          box-shadow: 0 0 14px rgba(52, 211, 153, 0.6);
          animation: pop-in 400ms cubic-bezier(0.22, 1, 0.36, 1);
        }

        /* Confetti success burst */
        .success-burst {
          position: absolute;
          top: 50%;
          left: 50%;
          width: 1px;
          height: 1px;
          pointer-events: none;
          z-index: 3;
        }
        .burst-ring {
          position: absolute;
          top: 50%;
          left: 50%;
          width: 40px;
          height: 40px;
          border-radius: 50%;
          border: 2px solid rgba(212, 160, 23, 0.7);
          transform: translate(-50%, -50%);
          animation: burst-ring 900ms cubic-bezier(0.16, 1, 0.3, 1) forwards;
        }
        .spark {
          position: absolute;
          top: 0;
          left: 0;
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: radial-gradient(circle, #fff, #f4d37a 40%, transparent 75%);
          box-shadow: 0 0 10px #f4d37a;
          transform: rotate(var(--angle)) translate(0, 0);
          animation: spark-fly 1200ms cubic-bezier(0.16, 1, 0.3, 1) var(--delay) forwards;
          opacity: 0;
        }

        /* ---------- Docs ---------- */
        .docs-card {
          position: relative;
          border-radius: 18px;
          background: linear-gradient(
            165deg,
            rgba(14, 20, 38, 0.92),
            rgba(11, 16, 32, 0.92)
          );
          border: 1px solid rgba(212, 160, 23, 0.14);
          overflow: hidden;
        }
        .docs-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 1.1rem 1.5rem;
          border-bottom: 1px solid rgba(212, 160, 23, 0.08);
        }
        .docs-title {
          font-size: 1rem;
          font-weight: 700;
          color: #ffffff;
        }
        .docs-count {
          color: rgba(254, 243, 199, 0.5);
          font-weight: 500;
        }
        .docs-meta {
          font-size: 0.72rem;
          letter-spacing: 0.03em;
          color: rgba(254, 243, 199, 0.55);
        }
        .docs-table-wrap {
          overflow-x: auto;
        }
        .docs-table {
          min-width: 680px;
        }
        .docs-thead,
        .doc-row {
          display: grid;
          grid-template-columns: 2.4fr 0.9fr 1.3fr 1fr 0.9fr;
          align-items: center;
          padding: 0.9rem 1.5rem;
          gap: 0.75rem;
        }
        .docs-thead {
          font-size: 10px;
          letter-spacing: 0.22em;
          font-weight: 600;
          color: rgba(254, 243, 199, 0.5);
          background: rgba(11, 16, 32, 0.6);
          border-bottom: 1px solid rgba(212, 160, 23, 0.08);
        }
        .doc-row {
          font-size: 0.84rem;
          border-top: 1px solid rgba(212, 160, 23, 0.05);
          transition: background 180ms ease, transform 200ms ease;
          animation: row-in 520ms cubic-bezier(0.22, 1, 0.36, 1) both;
          animation-delay: calc(var(--idx, 0) * 35ms);
        }
        .doc-row:hover {
          background: rgba(212, 160, 23, 0.04);
        }
        .doc-row.is-deleting {
          background: linear-gradient(
            90deg,
            rgba(244, 63, 94, 0.14),
            rgba(244, 63, 94, 0.04)
          );
          box-shadow: inset 3px 0 0 #f43f5e;
          animation: deleting-throb 1.2s ease-in-out infinite;
        }
        .doc-row.is-exiting {
          animation: row-dissolve 620ms cubic-bezier(0.4, 0, 0.2, 1) forwards;
          transform-origin: left center;
          position: relative;
        }
        .doc-row.is-exiting::before {
          content: "";
          position: absolute;
          inset: 0;
          background: linear-gradient(
            90deg,
            rgba(244, 63, 94, 0.25) 0%,
            rgba(244, 63, 94, 0) 80%
          );
          animation: delete-sweep 620ms ease-out forwards;
          pointer-events: none;
        }
        .doc-row .col-name {
          display: flex;
          align-items: center;
          gap: 0.6rem;
          font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas,
            "Liberation Mono", monospace;
          color: rgba(254, 243, 199, 0.92);
          overflow: hidden;
        }
        .doc-icon {
          flex-shrink: 0;
          width: 26px;
          height: 26px;
          border-radius: 7px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #f4d37a;
          background: rgba(212, 160, 23, 0.1);
          border: 1px solid rgba(212, 160, 23, 0.22);
        }
        .doc-name {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .col-size,
        .col-date {
          color: rgba(254, 243, 199, 0.62);
          font-size: 0.78rem;
        }
        .col-action {
          text-align: right;
        }

        .badge {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 3px 10px;
          border-radius: 9999px;
          font-size: 0.72rem;
          font-weight: 600;
          letter-spacing: 0.04em;
        }
        .badge-ok {
          color: #34d399;
          background: rgba(16, 185, 129, 0.1);
          border: 1px solid rgba(16, 185, 129, 0.24);
        }
        .badge-pending {
          color: #fbbf24;
          background: rgba(234, 179, 8, 0.12);
          border: 1px solid rgba(234, 179, 8, 0.28);
        }
        .badge-dot {
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: currentColor;
          box-shadow: 0 0 8px currentColor;
          animation: dot-pulse 1.8s ease-in-out infinite;
        }

        .del-btn {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 0.4rem 0.8rem;
          border-radius: 8px;
          font-size: 0.75rem;
          font-weight: 600;
          color: #fda4af;
          background: rgba(244, 63, 94, 0.08);
          border: 1px solid rgba(244, 63, 94, 0.25);
          cursor: pointer;
          transition: background 180ms ease, transform 180ms ease,
            box-shadow 180ms ease, color 180ms ease;
        }
        .del-btn:hover:not(:disabled) {
          background: rgba(244, 63, 94, 0.18);
          color: #fecaca;
          transform: translateY(-1px);
          box-shadow: 0 10px 20px -10px rgba(244, 63, 94, 0.4);
        }
        .del-btn:disabled {
          cursor: not-allowed;
        }
        .del-loading {
          display: inline-flex;
          gap: 4px;
        }
        .del-loading .d {
          width: 5px;
          height: 5px;
          border-radius: 50%;
          background: #fda4af;
          animation: dot-bounce 1.2s ease-in-out infinite;
        }
        .del-loading .d:nth-child(2) {
          animation-delay: 0.15s;
        }
        .del-loading .d:nth-child(3) {
          animation-delay: 0.3s;
        }

        /* ---------- Skeleton rows ---------- */
        .skel-row {
          display: grid;
          grid-template-columns: 2.4fr 0.9fr 1.3fr 1fr 0.9fr;
          gap: 0.75rem;
          padding: 0.9rem 1.5rem;
          border-top: 1px solid rgba(212, 160, 23, 0.05);
          animation: stagger-in 600ms ease both;
        }
        .skel {
          height: 14px;
          border-radius: 6px;
          background: linear-gradient(
            90deg,
            rgba(148, 163, 184, 0.08) 0%,
            rgba(148, 163, 184, 0.18) 50%,
            rgba(148, 163, 184, 0.08) 100%
          );
          background-size: 200% 100%;
          animation: skeleton-shimmer 1.6s ease-in-out infinite;
        }
        .skel-name {
          width: 70%;
        }
        .skel-size {
          width: 55%;
        }
        .skel-date {
          width: 75%;
        }
        .skel-badge {
          width: 60%;
        }
        .skel-action {
          width: 50%;
          justify-self: end;
        }
        .empty-state {
          padding: 3rem 1.5rem;
          display: flex;
          flex-direction: column;
          align-items: center;
          text-align: center;
          color: rgba(254, 243, 199, 0.5);
        }
        .empty-icon {
          width: 60px;
          height: 60px;
          border-radius: 14px;
          display: flex;
          align-items: center;
          justify-content: center;
          margin-bottom: 0.9rem;
          color: #f4d37a;
          background: rgba(212, 160, 23, 0.08);
          border: 1px solid rgba(212, 160, 23, 0.2);
          animation: icon-float 4s ease-in-out infinite;
        }
        .empty-title {
          font-size: 1rem;
          font-weight: 700;
          color: #fefce8;
        }
        .empty-sub {
          margin-top: 4px;
          font-size: 0.8125rem;
          max-width: 22rem;
        }

        /* ---------- Modal ---------- */
        .modal-backdrop {
          position: fixed;
          inset: 0;
          z-index: 80;
          display: flex;
          align-items: center;
          justify-content: center;
          padding: 1rem;
          background: rgba(0, 0, 0, 0.55);
          backdrop-filter: blur(6px);
          animation: modal-fade 220ms ease;
        }
        .modal {
          position: relative;
          max-width: 440px;
          width: 100%;
          padding: 2rem 1.8rem 1.6rem;
          border-radius: 18px;
          background: linear-gradient(
            165deg,
            rgba(19, 26, 46, 0.96),
            rgba(14, 20, 38, 0.96)
          );
          border: 1px solid rgba(212, 160, 23, 0.25);
          box-shadow: 0 40px 80px -30px rgba(0, 0, 0, 0.8);
          text-align: center;
          animation: modal-pop 360ms cubic-bezier(0.22, 1, 0.36, 1);
          overflow: hidden;
        }
        .modal-glow {
          position: absolute;
          inset: -1px;
          border-radius: 18px;
          pointer-events: none;
          background: conic-gradient(
            from 180deg at 50% 50%,
            rgba(244, 63, 94, 0.45),
            rgba(244, 63, 94, 0) 30%,
            rgba(244, 63, 94, 0.3) 60%,
            rgba(244, 63, 94, 0) 85%,
            rgba(244, 63, 94, 0.5)
          );
          filter: blur(3px);
          opacity: 0.5;
          animation: conic-spin 6s linear infinite;
        }
        .modal > :not(.modal-glow) {
          position: relative;
          z-index: 1;
        }
        .modal-icon {
          width: 56px;
          height: 56px;
          margin: 0 auto 0.9rem;
          border-radius: 16px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #fda4af;
          background: rgba(244, 63, 94, 0.12);
          border: 1px solid rgba(244, 63, 94, 0.3);
          animation: icon-float 3s ease-in-out infinite;
        }
        .modal-title {
          font-size: 1.15rem;
          font-weight: 700;
          color: #ffffff;
        }
        .modal-filename {
          margin-top: 0.4rem;
          font-size: 0.78rem;
          color: #f4d37a;
          font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas,
            "Liberation Mono", monospace;
          word-break: break-all;
        }
        .modal-hint {
          margin-top: 0.75rem;
          font-size: 0.8125rem;
          color: rgba(254, 243, 199, 0.65);
          line-height: 1.5;
        }
        .modal-actions {
          margin-top: 1.4rem;
          display: flex;
          justify-content: center;
          gap: 0.65rem;
        }
        .modal-cancel,
        .modal-del {
          flex: 1;
          padding: 0.75rem 1rem;
          border-radius: 10px;
          font-size: 0.82rem;
          font-weight: 600;
          cursor: pointer;
          border: 0;
          transition: transform 180ms ease, filter 180ms ease,
            box-shadow 220ms ease;
        }
        .modal-cancel {
          background: rgba(148, 163, 184, 0.1);
          color: rgba(254, 243, 199, 0.8);
          border: 1px solid rgba(148, 163, 184, 0.22);
        }
        .modal-cancel:hover:not(:disabled) {
          background: rgba(148, 163, 184, 0.18);
          transform: translateY(-1px);
        }
        .modal-del {
          background: linear-gradient(180deg, #fb7185 0%, #e11d48 100%);
          color: white;
          box-shadow: 0 12px 24px -14px rgba(244, 63, 94, 0.8),
            inset 0 1px 0 rgba(255, 255, 255, 0.25);
        }
        .modal-del:hover:not(:disabled) {
          filter: brightness(1.08);
          transform: translateY(-2px);
          box-shadow: 0 18px 36px -14px rgba(244, 63, 94, 0.8);
        }
        .modal-del:disabled,
        .modal-cancel:disabled {
          opacity: 0.6;
          cursor: not-allowed;
        }

        /* ---------- Toast ---------- */
        .toast {
          position: fixed;
          bottom: 1.5rem;
          right: 1.5rem;
          z-index: 90;
          padding: 0.85rem 1.1rem 0.95rem;
          border-radius: 12px;
          min-width: 280px;
          max-width: 420px;
          display: flex;
          align-items: center;
          gap: 0.65rem;
          font-size: 0.82rem;
          color: white;
          overflow: hidden;
          animation: toast-in 420ms cubic-bezier(0.22, 1, 0.36, 1),
            toast-out 320ms ease 4.3s forwards;
          backdrop-filter: blur(14px);
          box-shadow: 0 22px 44px -16px rgba(0, 0, 0, 0.55);
        }
        .toast-ok {
          background: linear-gradient(
            140deg,
            rgba(6, 95, 70, 0.95),
            rgba(4, 61, 45, 0.95)
          );
          border: 1px solid rgba(16, 185, 129, 0.35);
          color: #a7f3d0;
        }
        .toast-err {
          background: linear-gradient(
            140deg,
            rgba(127, 29, 29, 0.95),
            rgba(88, 16, 16, 0.95)
          );
          border: 1px solid rgba(244, 63, 94, 0.35);
          color: #fecaca;
        }
        .toast-ic {
          flex-shrink: 0;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 26px;
          height: 26px;
          border-radius: 8px;
          background: rgba(255, 255, 255, 0.08);
        }
        .toast-msg {
          flex: 1;
          line-height: 1.4;
        }
        .toast-bar {
          position: absolute;
          left: 0;
          bottom: 0;
          height: 2px;
          background: currentColor;
          opacity: 0.6;
          width: 100%;
          animation: toast-progress 4.3s linear forwards;
        }

        /* ---------- Keyframes ---------- */
        @keyframes splash-out {
          to {
            opacity: 0;
            visibility: hidden;
          }
        }
        @keyframes cover-iris {
          to {
            clip-path: circle(0% at 50% 50%);
          }
        }
        @keyframes splash-pop {
          from {
            opacity: 0;
            transform: scale(0.9);
          }
          to {
            opacity: 1;
            transform: scale(1);
          }
        }
        @keyframes splash-spin {
          to {
            transform: rotate(360deg);
          }
        }
        @keyframes splash-breath {
          0%, 100% {
            transform: scale(1);
            filter: drop-shadow(0 0 16px rgba(212, 160, 23, 0.6));
          }
          50% {
            transform: scale(1.04);
            filter: drop-shadow(0 0 28px rgba(212, 160, 23, 0.85));
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
        @keyframes grid-drift {
          to {
            background-position: 52px 52px;
          }
        }
        @keyframes orb-drift {
          0%, 100% {
            transform: translate(0, 0);
          }
          50% {
            transform: translate(36px, -26px);
          }
        }
        @keyframes particle-rise {
          0% {
            opacity: 0;
            transform: translateY(30px) scale(0.6);
          }
          10%, 85% {
            opacity: 0.9;
          }
          100% {
            opacity: 0;
            transform: translateY(-140px) scale(1);
          }
        }
        @keyframes ring-spin {
          to {
            transform: rotate(360deg);
          }
        }
        @keyframes conic-spin {
          to {
            transform: rotate(360deg);
          }
        }
        @keyframes dot-pulse {
          0%, 100% {
            opacity: 1;
            transform: scale(1);
          }
          50% {
            opacity: 0.5;
            transform: scale(0.82);
          }
        }
        @keyframes icon-float {
          0%, 100% {
            transform: translateY(0);
          }
          50% {
            transform: translateY(-4px);
          }
        }
        @keyframes stagger-in {
          from {
            opacity: 0;
            transform: translateY(16px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
        @keyframes row-in {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
        @keyframes row-out {
          to {
            opacity: 0;
            transform: translateX(24px) scale(0.98);
            max-height: 0;
            padding-top: 0;
            padding-bottom: 0;
            border-top-color: transparent;
          }
        }
        @keyframes row-dissolve {
          0% {
            opacity: 1;
            transform: translateX(0) scale(1);
            filter: blur(0);
            max-height: 80px;
          }
          35% {
            opacity: 0.9;
            transform: translateX(6px) scale(1);
            filter: blur(0);
          }
          75% {
            opacity: 0.1;
            transform: translateX(80px) scale(0.97);
            filter: blur(4px);
          }
          100% {
            opacity: 0;
            transform: translateX(120px) scale(0.9);
            filter: blur(8px);
            max-height: 0;
            padding-top: 0;
            padding-bottom: 0;
            border-top-color: transparent;
          }
        }
        @keyframes delete-sweep {
          0% {
            transform: translateX(-100%);
          }
          100% {
            transform: translateX(100%);
          }
        }
        @keyframes deleting-throb {
          0%, 100% {
            box-shadow: inset 3px 0 0 #f43f5e,
              0 0 0 0 rgba(244, 63, 94, 0);
          }
          50% {
            box-shadow: inset 3px 0 0 #fb7185,
              0 0 20px -4px rgba(244, 63, 94, 0.35);
          }
        }
        @keyframes pulse-soft {
          0%, 100% {
            filter: brightness(1);
          }
          50% {
            filter: brightness(1.15);
          }
        }
        @keyframes bar-sheen {
          0% {
            transform: translateX(-100%);
          }
          100% {
            transform: translateX(250%);
          }
        }
        @keyframes ticker-flash {
          0% {
            opacity: 0;
            transform: translateY(4px) scale(0.9);
          }
          25%, 100% {
            opacity: 1;
            transform: translateY(0) scale(1);
          }
        }
        @keyframes chunk-bounce {
          0%, 80%, 100% {
            transform: translateY(0);
            opacity: 0.5;
          }
          40% {
            transform: translateY(-4px);
            opacity: 1;
          }
        }
        @keyframes pop-in {
          from {
            opacity: 0;
            transform: scale(0.4);
          }
          to {
            opacity: 1;
            transform: scale(1);
          }
        }
        @keyframes burst-ring {
          0% {
            opacity: 1;
            width: 40px;
            height: 40px;
            border-width: 3px;
          }
          100% {
            opacity: 0;
            width: 220px;
            height: 220px;
            border-width: 1px;
          }
        }
        @keyframes spark-fly {
          0% {
            opacity: 1;
            transform: rotate(var(--angle)) translate(0, 0) scale(1);
          }
          80% {
            opacity: 1;
          }
          100% {
            opacity: 0;
            transform: rotate(var(--angle)) translate(140px, 0) scale(0.3);
          }
        }
        @keyframes skeleton-shimmer {
          0% {
            background-position: 200% 0;
          }
          100% {
            background-position: -200% 0;
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
        @keyframes dash-march {
          to {
            background-position: 400px 400px;
          }
        }
        @keyframes index-pulse {
          0% {
            background-position: 200% 50%;
          }
          100% {
            background-position: 0% 50%;
          }
        }
        @keyframes spin-full {
          to {
            transform: rotate(360deg);
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
        @keyframes modal-fade {
          from {
            opacity: 0;
          }
          to {
            opacity: 1;
          }
        }
        @keyframes modal-pop {
          from {
            opacity: 0;
            transform: translateY(20px) scale(0.95);
          }
          to {
            opacity: 1;
            transform: translateY(0) scale(1);
          }
        }
        @keyframes toast-in {
          from {
            opacity: 0;
            transform: translateX(40px);
          }
          to {
            opacity: 1;
            transform: translateX(0);
          }
        }
        @keyframes toast-out {
          to {
            opacity: 0;
            transform: translateX(40px);
          }
        }
        @keyframes toast-progress {
          from {
            width: 100%;
          }
          to {
            width: 0%;
          }
        }
      `}</style>
    </div>
  );
}
