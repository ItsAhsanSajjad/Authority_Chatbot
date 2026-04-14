"use client";

import { memo, useState, useCallback, useEffect, useMemo, type KeyboardEvent } from "react";
import { useAutoResizeTextarea } from "../../hooks/useAutoResizeTextarea";
import { useVoiceRecorder } from "../../hooks/useVoiceRecorder";
import type { AnswerSourceMode } from "../../lib/types";
import { SOURCE_MODE_OPTIONS } from "../../lib/types";

interface Props {
  onSend: (text: string) => void;
  disabled?: boolean;
  sourceMode: AnswerSourceMode;
  onSourceModeChange: (mode: AnswerSourceMode) => void;
}

const MODE_ICONS: Record<string, React.ReactNode> = {
  documents: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
      <polyline points="14 2 14 8 20 8"/>
      <line x1="16" y1="13" x2="8" y2="13"/>
      <line x1="16" y1="17" x2="8" y2="17"/>
    </svg>
  ),
  stored_api: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <ellipse cx="12" cy="5" rx="9" ry="3"/>
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
    </svg>
  ),
  both: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/>
      <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>
    </svg>
  ),
};

function fmtTime(s: number): string {
  const m = Math.floor(s / 60);
  const sec = s % 60;
  return `${m}:${sec.toString().padStart(2, "0")}`;
}

export const Composer = memo(function Composer({ onSend, disabled, sourceMode, onSourceModeChange }: Props) {
  const [input, setInput] = useState("");
  const { ref: textareaRef, resize } = useAutoResizeTextarea(150);

  const handleTranscribed = useCallback((text: string) => {
    setInput(text);
    setTimeout(() => resize(), 0);
  }, [resize]);

  const {
    voiceState, elapsed, interimText, errorMessage,
    toggleRecording, clearError,
  } = useVoiceRecorder(handleTranscribed);

  useEffect(() => {
    if (voiceState === "idle" && input) textareaRef.current?.focus();
  }, [voiceState, input, textareaRef]);

  const handleSend = useCallback(() => {
    if (!input.trim() || disabled) return;
    onSend(input);
    setInput("");
    if (textareaRef.current) textareaRef.current.style.height = "auto";
  }, [input, disabled, onSend, textareaRef]);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(); }
    },
    [handleSend],
  );

  const isListening    = voiceState === "recording";
  const isTranscribing = voiceState === "transcribing";
  const micBusy        = isListening || isTranscribing;
  const hasText = input.trim().length > 0;

  const displayValue = isListening
    ? input + (input && interimText ? " " : "") + interimText
    : input;

  const wordCount = useMemo(() => {
    return displayValue.trim().split(/\s+/).filter(Boolean).length;
  }, [displayValue]);

  return (
    <div className="cx-wrapper">
      <div className="cx-inner">

        {/* Error */}
        {voiceState === "error" && errorMessage && (
          <div className="cx-error">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>
            <span>{errorMessage}</span>
            <button onClick={clearError} aria-label="Dismiss">
              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
          </div>
        )}

        {/* Main Card */}
        <div className={`cx-card ${isListening ? "cx-card--listening" : ""} ${isTranscribing ? "cx-card--transcribing" : ""}`}>

          {/* Transcribing overlay */}
          {isTranscribing && (
            <div className="cx-transcribing-bar">
              <span className="cx-transcribing-spinner" />
              <span>Transcribing…</span>
            </div>
          )}

          {/* Source pills row — top */}
          <div className={`cx-pills-row ${micBusy ? "cx-pills-row--hidden" : ""}`}>
            {SOURCE_MODE_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                className={`cx-pill ${sourceMode === opt.value ? "cx-pill--on" : ""}`}
                onClick={() => onSourceModeChange(opt.value)}
                title={opt.desc}
                aria-pressed={sourceMode === opt.value}
                type="button"
              >
                <span className="cx-pill-icon">{MODE_ICONS[opt.value]}</span>
                <span className="cx-pill-label">{opt.label}</span>
              </button>
            ))}
          </div>

          {/* Voice Visualizer — replaces pills when listening */}
          <div className={`cx-voice ${micBusy ? "cx-voice--on" : ""}`}>
            <div className="cx-voice-aurora" />
            <div className="cx-voice-particles">
              {Array.from({ length: 12 }).map((_, i) => (
                <span key={i} className="cx-particle" style={{
                  left: `${8 + (i * 7.5)}%`,
                  animationDelay: `${i * 0.3}s`,
                  animationDuration: `${2 + (i % 3) * 0.8}s`,
                }} />
              ))}
            </div>
            <div className="cx-wave-row">
              {Array.from({ length: 32 }).map((_, i) => (
                <span key={i} className="cx-bar" style={{ animationDelay: `${i * 0.04}s` }} />
              ))}
            </div>
            <div className="cx-voice-meta">
              <span className="cx-voice-timer">{fmtTime(elapsed)}</span>
              <span className="cx-voice-dot" />
              <span className="cx-voice-label">
                {isTranscribing ? "Transcribing…" : `Listening${elapsed > 0 ? ` — ${elapsed}s` : ""}`}
              </span>
            </div>
          </div>

          {/* Textarea */}
          <textarea
            ref={textareaRef}
            value={displayValue}
            onChange={(e) => { if (!isListening) setInput(e.target.value); }}
            onKeyDown={handleKeyDown}
            onInput={() => resize()}
            placeholder={
              isListening
                ? "Your words appear here..."
                : "Ask about PERA regulations, governance, or enforcement..."
            }
            rows={1}
            className={`cx-textarea ${isListening ? "cx-textarea--live" : ""}`}
            style={{ maxHeight: 150, minHeight: 44 }}
            disabled={disabled}
            readOnly={isListening}
            aria-label="Message input"
          />

          {/* Bottom action row */}
          <div className="cx-actions">
            {/* Mic */}
            <button
              onClick={toggleRecording}
              disabled={disabled || isTranscribing}
              className={`cx-btn cx-mic ${isListening ? "cx-mic--on" : ""} ${isTranscribing ? "cx-mic--transcribing" : ""}`}
              aria-label={isListening ? "Stop recording" : isTranscribing ? "Transcribing…" : "Record voice"}
            >
              {isListening && (
                <>
                  <span className="cx-mic-ring cx-mic-ring--1" />
                  <span className="cx-mic-ring cx-mic-ring--2" />
                </>
              )}
              <span className="cx-btn-icon">
                {isTranscribing ? (
                  /* Spinner while waiting for Whisper */
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" className="cx-spin">
                    <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/>
                  </svg>
                ) : isListening ? (
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                    <rect x="6" y="6" width="12" height="12" rx="2" />
                  </svg>
                ) : (
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                    <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z" />
                    <path d="M19 10v2a7 7 0 0 1-14 0v-2" />
                    <line x1="12" y1="19" x2="12" y2="23" />
                    <line x1="8" y1="23" x2="16" y2="23" />
                  </svg>
                )}
              </span>
            </button>

            {/* Send */}
            <button
              onClick={handleSend}
              disabled={!hasText || disabled || micBusy}
              className={`cx-btn cx-send ${hasText && !disabled && !micBusy ? "cx-send--ready" : ""}`}
              aria-label="Send"
            >
              <span className="cx-btn-icon">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="12" y1="19" x2="12" y2="5" />
                  <polyline points="5 12 12 5 19 12" />
                </svg>
              </span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
});
