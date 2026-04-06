"use client";

import { memo, useState, useCallback, type KeyboardEvent, useEffect } from "react";
import { useAutoResizeTextarea } from "../../hooks/useAutoResizeTextarea";
import { useVoiceRecorder } from "../../hooks/useVoiceRecorder";
import type { VoiceState, AnswerSourceMode } from "../../lib/types";
import { SOURCE_MODE_OPTIONS } from "../../lib/types";

interface Props {
  onSend: (text: string) => void;
  disabled?: boolean;
  sourceMode: AnswerSourceMode;
  onSourceModeChange: (mode: AnswerSourceMode) => void;
}

function formatSeconds(s: number): string {
  const m = Math.floor(s / 60);
  const sec = s % 60;
  return `${m}:${sec.toString().padStart(2, "0")}`;
}

const MODE_ICONS: Record<AnswerSourceMode, string> = {
  documents: "📄",
  stored_api: "🗃️",
  both: "🔗",
  live_api: "⚡",
};

export const Composer = memo(function Composer({ onSend, disabled, sourceMode, onSourceModeChange }: Props) {
  const [input, setInput] = useState("");
  const { ref: textareaRef, resize } = useAutoResizeTextarea(150);

  const handleTranscribed = useCallback((text: string) => {
    setInput(text);
    textareaRef.current?.focus();
  }, [textareaRef]);

  const { voiceState, elapsed, errorMessage, toggleRecording, clearError } =
    useVoiceRecorder(handleTranscribed);

  const handleSend = useCallback(() => {
    if (!input.trim() || disabled) return;
    onSend(input);
    setInput("");
    // Reset textarea height
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto";
    }
  }, [input, disabled, onSend, textareaRef]);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    },
    [handleSend],
  );

  const handleInput = useCallback(() => {
    resize();
  }, [resize]);

  const isRecordingOrTranscribing = voiceState === "recording" || voiceState === "transcribing";

  return (
    <div className="px-4 md:px-0 pt-3 pb-6 relative z-10" style={{ background: "var(--bg-page)", borderTop: "1px solid var(--border)" }}>
      <div className="max-w-3xl mx-auto">
        {/* Source Mode Selector */}
        <div className="source-mode-bar mb-2">
          <span className="source-mode-label-text">Answer Source:</span>
          <div className="source-mode-group">
            {SOURCE_MODE_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                className={`source-mode-pill ${sourceMode === opt.value ? "source-mode-pill--active" : ""}`}
                onClick={() => onSourceModeChange(opt.value)}
                title={opt.desc}
                aria-pressed={sourceMode === opt.value}
                type="button"
              >
                <span className="source-mode-icon">{MODE_ICONS[opt.value]}</span>
                <span className="source-mode-pill-label">{opt.label}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Voice error banner */}
        {voiceState === "error" && errorMessage && (
          <div
            className="mb-2 px-3 py-2 rounded-xl text-xs flex items-center justify-between"
            style={{ background: "rgba(239,68,68,0.1)", color: "var(--red)" }}
          >
            <span>🎤 {errorMessage}</span>
            <button
              onClick={clearError}
              className="ml-2 font-bold text-sm"
              aria-label="Dismiss error"
            >
              ✕
            </button>
          </div>
        )}

        {/* Recording / Transcribing banner */}
        {voiceState === "recording" && (
          <div
            className="mb-2 px-3 py-2 rounded-xl text-xs font-medium flex items-center gap-2"
            style={{ background: "rgba(239,68,68,0.1)", color: "var(--red)" }}
          >
            <span className="recording-pulse inline-block w-2.5 h-2.5 rounded-full" style={{ background: "var(--red)" }} />
            Recording… {formatSeconds(elapsed)}
          </div>
        )}
        {voiceState === "transcribing" && (
          <div
            className="mb-2 px-3 py-2 rounded-xl text-xs font-medium flex items-center gap-2"
            style={{ background: "var(--accent-soft)", color: "var(--accent)" }}
          >
            <span className="thinking-text">Transcribing audio…</span>
          </div>
        )}

        <div className="flex items-end gap-2">
          <div className="flex-1 relative">
            <textarea
              ref={textareaRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              onInput={handleInput}
              placeholder="Ask about PERA regulations, governance, enforcement, or KPIs"
              rows={1}
              className="chat-input w-full resize-none px-4 py-3 pr-24 text-sm"
              style={{ maxHeight: 150, minHeight: 44 }}
              disabled={disabled || isRecordingOrTranscribing}
              aria-label="Message input"
            />

            {/* Voice Button */}
            <button
              onClick={toggleRecording}
              disabled={disabled || voiceState === "transcribing"}
              className={`absolute right-12 bottom-2 w-9 h-9 rounded-xl flex items-center justify-center transition-all z-10 ${voiceState === "recording" ? "recording-pulse" : ""}`}
              style={{
                background: voiceState === "recording" ? "var(--red)" : "transparent",
                color: voiceState === "recording" ? "white" : "var(--text-secondary)",
              }}
              aria-label={voiceState === "recording" ? "Stop recording" : "Start voice input"}
              onMouseEnter={voiceState !== "recording" ? (e) => (e.currentTarget.style.background = "var(--bg-hover)") : undefined}
              onMouseLeave={voiceState !== "recording" ? (e) => (e.currentTarget.style.background = "transparent") : undefined}
            >
              {voiceState === "recording" ? (
                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="2" /></svg>
              ) : (
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                  <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z" />
                  <path d="M19 10v2a7 7 0 0 1-14 0v-2" /><line x1="12" y1="19" x2="12" y2="23" /><line x1="8" y1="23" x2="16" y2="23" />
                </svg>
              )}
            </button>

            {/* Send Button */}
            <button
              onClick={handleSend}
              disabled={!input.trim() || disabled || isRecordingOrTranscribing}
              className="send-btn absolute right-2 bottom-2 w-9 h-9 flex items-center justify-center"
              aria-label="Send message"
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" />
              </svg>
            </button>
          </div>
        </div>


      </div>
    </div>
  );
});
