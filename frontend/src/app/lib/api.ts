/* ═══════════════════════════════════════════════════════════
   PERA AI — API Client
   Typed requests with proper error classification
   ═══════════════════════════════════════════════════════════ */

import type { AskRequest, AskResponse, ApiError, TranscribeResponse } from "./types";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

function makeError(
  type: ApiError["type"],
  message: string,
  status?: number,
): ApiError {
  return { type, message, status };
}

/** POST /api/ask */
export async function askQuestion(
  req: AskRequest,
): Promise<{ ok: true; data: AskResponse } | { ok: false; error: ApiError }> {
  try {
    const res = await fetch(`${API_URL}/api/ask`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });

    if (!res.ok) {
      let detail = `Server error (${res.status})`;
      try {
        const body = await res.json();
        if (body?.detail) detail = String(body.detail);
      } catch { /* use default */ }
      return { ok: false, error: makeError("http", detail, res.status) };
    }

    const data: AskResponse = await res.json();
    return { ok: true, data };
  } catch (err) {
    const msg =
      err instanceof TypeError
        ? "Cannot reach the server. Check your connection."
        : err instanceof Error
          ? err.message
          : "An unexpected error occurred.";
    return { ok: false, error: makeError("network", msg) };
  }
}

/** POST /transcribe */
export async function transcribeAudio(
  blob: Blob,
): Promise<{ ok: true; text: string } | { ok: false; error: ApiError }> {
  try {
    const fd = new FormData();
    fd.append("audio", blob, "voice.webm");
    const res = await fetch(`${API_URL}/transcribe`, {
      method: "POST",
      body: fd,
    });

    if (!res.ok) {
      return {
        ok: false,
        error: makeError("http", `Transcription failed (${res.status})`, res.status),
      };
    }

    const data: TranscribeResponse = await res.json();
    if (!data.success || !data.text) {
      return {
        ok: false,
        error: makeError("http", "Could not transcribe audio. Please try again."),
      };
    }
    return { ok: true, text: data.text };
  } catch {
    return {
      ok: false,
      error: makeError("network", "Cannot reach transcription service."),
    };
  }
}

/** Build PDF URL — used by components to open PDFs */
export function buildPdfUrl(ref: {
  document: string;
  page_start?: number | string;
  page?: number | string;
  open_url?: string;
}): string {
  const pg = ref.page_start || ref.page || 1;
  if (ref.open_url) {
    const url = ref.open_url.replace(/^https?:\/\/[^/]+/, API_URL);
    return url.includes("#") ? url : `${url}#page=${pg}`;
  }
  return `${API_URL}/pdf/${encodeURIComponent(ref.document)}#page=${pg}`;
}
