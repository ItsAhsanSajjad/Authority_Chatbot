/* ═══════════════════════════════════════════════════════════
   PERA AI — Domain & API Types
   ═══════════════════════════════════════════════════════════ */

// ─── Answer Source Mode ───

export type AnswerSourceMode = "documents" | "stored_api" | "both" | "live_api";

export const SOURCE_MODE_OPTIONS: { value: AnswerSourceMode; label: string; desc: string }[] = [
  { value: "documents", label: "Documents", desc: "Regulatory documents only" },
  { value: "stored_api", label: "Stored API", desc: "Indexed API snapshots" },
  { value: "both", label: "Both", desc: "Documents + API data" },
  { value: "live_api", label: "Live API", desc: "Real-time API queries" },
];

// ─── Domain Models ───

export interface Reference {
  id?: number;
  document: string;
  page?: number | string;
  page_start?: number | string;
  page_end?: number | string;
  open_url?: string;
  snippet?: string;
  score?: number;
  chunk_index?: number;
  source_type?: string;      // "pdf" | "api" | "live_api"
  endpoint_key?: string;     // for live API refs
  timestamp?: string;        // for live API refs
}

export interface Message {
  role: "user" | "assistant";
  content: string;
  references?: Reference[];
  timestamp: number;
  /** When true the message represents a failed send attempt */
  failed?: boolean;
  /** Source mode used for this answer */
  sourceMode?: AnswerSourceMode;
  sourceModeLabel?: string;
  provenance?: string;
}

export interface ChatSession {
  id: string;
  title: string;
  messages: Message[];
  createdAt: number;   // epoch ms
  updatedAt: number;   // epoch ms
}

// ─── API Request / Response ───

export interface AskRequest {
  question: string;
  conversation_history?: { role: string; content: string }[];
  session_id?: string;
  answer_source_mode?: AnswerSourceMode;
}

export interface AskResponse {
  answer: string;
  decision?: string;
  references: Reference[];
  session_id?: string;
  source_mode?: AnswerSourceMode;
  source_mode_label?: string;
  provenance?: string;
}

export interface TranscribeResponse {
  text: string;
  success: boolean;
}

// ─── API Error ───

export interface ApiError {
  type: "network" | "http" | "parse" | "unknown";
  status?: number;
  message: string;
}

// ─── Health Status ───

export type ConnectionStatus = "connecting" | "online" | "offline";

// ─── Voice Recorder State ───

export type VoiceState = "idle" | "recording" | "transcribing" | "error";

// ─── Toast ───

export interface ToastMessage {
  id: string;
  text: string;
  type: "info" | "success" | "error";
}

// ─── Storage ───

export const STORAGE_VERSION = 1;

export interface StorageEnvelope {
  version: number;
  sessions: ChatSession[];
  activeChatId: string | null;
}
