/* ═══════════════════════════════════════════════════════════
   PERA AUTHORITY CHATBOT — Admin API Client
   Thin fetch wrappers that handle Bearer auth + 401 redirect.
   ═══════════════════════════════════════════════════════════ */

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
const SESSION_KEY = "pera_admin_session";

export interface AdminSession {
  token: string;
  expires_at: number; // unix seconds
  email: string;
}

export interface AdminDocument {
  filename: string;
  size: number;
  mtime: number;
  authority: number;
  rank: number;
  indexed: boolean;
}

export interface IndexStatus {
  active_index_dir: string;
  built_at: number;
  doc_count: number;
  chunk_count: number;
  reindex_running?: boolean;
  last_error?: string;
}

export interface IndexResult {
  changed?: boolean;
  active_index_dir?: string;
  error?: string;
  [k: string]: unknown;
}

export interface UploadResponse {
  filename: string;
  size: number;
  duration_ms: number;
  index: IndexResult;
}

export interface DeleteResponse {
  filename: string;
  duration_ms: number;
  index: IndexResult;
}

/* ---- Freshness (continuous-refresh scheduler) ---- */

export interface FreshnessTrack {
  interval_s: number;
  last_started_at: number;     // unix seconds (0 = never)
  last_finished_at: number;    // unix seconds (0 = never)
  last_success_at: number;     // unix seconds (0 = never)
  last_error: string;
  cycle_count: number;
  running_now: boolean;
  last_result: unknown;
  seconds_since_success: number | null;
  staleness_status: "fresh" | "aging" | "stale" | "never_run";
}

export interface FreshnessStatus {
  enabled: boolean;
  tracks: Record<string, FreshnessTrack>;
  error?: string;
}

/* ---- session storage ---- */

export function getSession(): AdminSession | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as AdminSession;
    if (!parsed?.token || !parsed?.expires_at) return null;
    if (parsed.expires_at * 1000 < Date.now()) {
      window.localStorage.removeItem(SESSION_KEY);
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function setSession(s: AdminSession) {
  window.localStorage.setItem(SESSION_KEY, JSON.stringify(s));
}

export function clearSession() {
  if (typeof window !== "undefined") {
    window.localStorage.removeItem(SESSION_KEY);
  }
}

export function logout() {
  clearSession();
  if (typeof window !== "undefined") {
    window.location.href = "/login";
  }
}

/* ---- fetch wrapper ---- */

async function authFetch(
  path: string,
  init: RequestInit = {},
): Promise<Response> {
  const session = getSession();
  const headers = new Headers(init.headers);
  if (session) headers.set("Authorization", `Bearer ${session.token}`);
  const res = await fetch(`${API_URL}${path}`, { ...init, headers });
  if (res.status === 401) {
    clearSession();
    if (typeof window !== "undefined" && !window.location.pathname.startsWith("/login")) {
      window.location.href = "/login";
    }
  }
  return res;
}

/* ---- endpoints ---- */

export async function adminLogin(
  email: string,
  password: string,
): Promise<
  | { ok: true; session: AdminSession }
  | { ok: false; status: number; error: string }
> {
  try {
    const res = await fetch(`${API_URL}/api/admin/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
    if (!res.ok) {
      let err = "invalid_credentials";
      try {
        const body = await res.json();
        err = body?.detail?.error || body?.error || err;
      } catch {
        /* ignore */
      }
      return { ok: false, status: res.status, error: err };
    }
    const body = (await res.json()) as { token: string; expires_at: number };
    const session: AdminSession = {
      token: body.token,
      expires_at: body.expires_at,
      email,
    };
    setSession(session);
    return { ok: true, session };
  } catch (e) {
    return {
      ok: false,
      status: 0,
      error: e instanceof Error ? e.message : "network_error",
    };
  }
}

export async function listDocuments(): Promise<AdminDocument[]> {
  const res = await authFetch("/api/admin/documents");
  if (!res.ok) throw new Error(`list_documents_failed_${res.status}`);
  return (await res.json()) as AdminDocument[];
}

export async function indexStatus(): Promise<IndexStatus> {
  const res = await authFetch("/api/admin/index-status");
  if (!res.ok) throw new Error(`index_status_failed_${res.status}`);
  return (await res.json()) as IndexStatus;
}

export function uploadDocument(
  file: File,
  onProgress?: (pct: number) => void,
): Promise<UploadResponse> {
  return new Promise((resolve, reject) => {
    const session = getSession();
    if (!session) {
      reject(new Error("not_authenticated"));
      return;
    }
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${API_URL}/api/admin/documents`, true);
    xhr.setRequestHeader("Authorization", `Bearer ${session.token}`);
    // Indexing can take a while (embedding API round-trip). Give the
    // request up to 5 minutes before the browser aborts it.
    xhr.timeout = 5 * 60 * 1000;
    xhr.ontimeout = () => reject(new Error("indexing_timeout"));
    xhr.upload.onprogress = (ev) => {
      if (ev.lengthComputable && onProgress) {
        onProgress(Math.round((ev.loaded / ev.total) * 100));
      }
    };
    xhr.onload = () => {
      if (xhr.status === 401) {
        clearSession();
        if (typeof window !== "undefined") window.location.href = "/login";
        reject(new Error("unauthorized"));
        return;
      }
      try {
        const body = JSON.parse(xhr.responseText);
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(body as UploadResponse);
        } else {
          // Surface the server-side human-readable message when present
          // (e.g. duplicate detection) so the user sees a real reason
          // instead of a generic "upload_failed_409".
          const msg =
            body?.detail?.message ||
            body?.message ||
            body?.detail?.error ||
            body?.error ||
            `upload_failed_${xhr.status}`;
          reject(new Error(msg));
        }
      } catch {
        reject(new Error(`upload_failed_${xhr.status}`));
      }
    };
    xhr.onerror = () => reject(new Error("network_error"));
    const fd = new FormData();
    fd.append("file", file);
    xhr.send(fd);
  });
}

export async function deleteDocument(filename: string): Promise<DeleteResponse> {
  const res = await authFetch(
    `/api/admin/documents/${encodeURIComponent(filename)}`,
    { method: "DELETE" },
  );
  if (!res.ok) {
    let err = `delete_failed_${res.status}`;
    try {
      const body = await res.json();
      err = body?.detail?.error || body?.error || err;
    } catch {
      /* ignore */
    }
    throw new Error(err);
  }
  return (await res.json()) as DeleteResponse;
}

/**
 * Fetch the freshness scheduler status (per-track last successful sync,
 * staleness classification, error if any). Public endpoint — no Bearer
 * needed — returns immediately even if the scheduler is disabled.
 */
export async function freshnessStatus(): Promise<FreshnessStatus> {
  const res = await fetch(`${API_URL}/api/freshness`, { cache: "no-store" });
  if (!res.ok) throw new Error(`freshness_failed_${res.status}`);
  return (await res.json()) as FreshnessStatus;
}
