/* ═══════════════════════════════════════════════════════════
   PERA AI — localStorage Persistence Layer
   Versioned storage with safe migration support
   ═══════════════════════════════════════════════════════════ */

import type { ChatSession, StorageEnvelope } from "./types";
import { STORAGE_VERSION } from "./types";

const STORAGE_KEY = "pera-chat-data";
const THEME_KEY = "pera-theme";

// ─── Chat Data ───

function isValidEnvelope(data: unknown): data is StorageEnvelope {
  if (!data || typeof data !== "object") return false;
  const d = data as Record<string, unknown>;
  return (
    typeof d.version === "number" &&
    Array.isArray(d.sessions)
  );
}

/** Attempt to migrate legacy shapes (sessions stored as raw array, etc.) */
function migrate(raw: unknown): StorageEnvelope | null {
  // Already valid
  if (isValidEnvelope(raw)) {
    if (raw.version === STORAGE_VERSION) return raw;
    // Future: handle version upgrades here
    return raw;
  }

  // Legacy: raw array of sessions (pre-versioned)
  if (Array.isArray(raw)) {
    return {
      version: STORAGE_VERSION,
      sessions: raw.filter(
        (s) =>
          s &&
          typeof s === "object" &&
          typeof s.id === "string" &&
          Array.isArray(s.messages)
      ) as ChatSession[],
      activeChatId: null,
    };
  }

  return null;
}

export function loadChatData(): StorageEnvelope {
  const empty: StorageEnvelope = {
    version: STORAGE_VERSION,
    sessions: [],
    activeChatId: null,
  };

  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return empty;
    const parsed = JSON.parse(raw);
    return migrate(parsed) ?? empty;
  } catch {
    return empty;
  }
}

export function saveChatData(envelope: StorageEnvelope): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(envelope));
  } catch {
    // Storage full or unavailable — fail silently
  }
}

// ─── Theme ───

export function loadTheme(): "light" | "dark" {
  try {
    const v = localStorage.getItem(THEME_KEY);
    return v === "light" ? "light" : "dark";
  } catch {
    return "dark";
  }
}

export function saveTheme(theme: "light" | "dark"): void {
  try {
    localStorage.setItem(THEME_KEY, theme);
  } catch {
    // fail silently
  }
}
