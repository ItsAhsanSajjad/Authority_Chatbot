"use client";

import { useEffect, useRef, useState, memo } from "react";

interface Props {
  url: string;
  title: string;
  onClose: () => void;
}

export const PdfModal = memo(function PdfModal({ url, title, onClose }: Props) {
  const modalRef = useRef<HTMLDivElement>(null);
  const closeBtnRef = useRef<HTMLButtonElement>(null);
  const previousFocus = useRef<HTMLElement | null>(null);
  const [iframeLoaded, setIframeLoaded] = useState(false);

  // Reset loaded state when URL changes
  useEffect(() => {
    setIframeLoaded(false);
  }, [url]);

  // Store the element that had focus when the modal opened
  useEffect(() => {
    previousFocus.current = document.activeElement as HTMLElement | null;
    closeBtnRef.current?.focus();

    return () => {
      previousFocus.current?.focus();
    };
  }, []);

  // Escape key + focus trap
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
        return;
      }
      if (e.key === "Tab" && modalRef.current) {
        const focusable = modalRef.current.querySelectorAll<HTMLElement>(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
        );
        if (focusable.length === 0) return;
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const isMobile = typeof window !== "undefined" && window.innerWidth < 768;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-label={title}
    >
      <div className="modal-overlay absolute inset-0" onClick={onClose} aria-hidden="true" />
      <div
        ref={modalRef}
        className="modal-content relative w-full max-w-5xl h-[85vh] flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3" style={{ borderBottom: "1px solid var(--border)" }}>
          <div className="flex items-center gap-2 min-w-0">
            <span className="text-lg">📄</span>
            <span className="font-semibold text-sm truncate" style={{ color: "var(--text-primary)" }}>
              {title}
            </span>
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            <a
              href={url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs px-3 py-1.5 rounded-lg font-medium transition-colors"
              style={{ background: "var(--bg-hover)", color: "var(--text-secondary)" }}
            >
              Open in Tab ↗
            </a>
            <button
              ref={closeBtnRef}
              onClick={onClose}
              className="w-8 h-8 rounded-lg flex items-center justify-center transition-colors text-sm"
              style={{ background: "var(--bg-hover)", color: "var(--text-secondary)" }}
              aria-label="Close PDF viewer"
            >
              ✕
            </button>
          </div>
        </div>

        {/* Content */}
        {isMobile ? (
          <div className="flex-1 flex flex-col items-center justify-center gap-4 p-8">
            <span className="text-4xl">📄</span>
            <p className="text-sm text-center" style={{ color: "var(--text-secondary)" }}>
              PDF preview is not supported on this device.
            </p>
            <a
              href={url}
              target="_blank"
              rel="noopener noreferrer"
              className="px-5 py-2.5 rounded-xl text-sm font-semibold text-white"
              style={{ background: "linear-gradient(135deg, var(--accent), var(--accent-hover))" }}
            >
              Open PDF in Browser ↗
            </a>
          </div>
        ) : (
          <div className="flex-1 relative">
            {/* Loading state */}
            {!iframeLoaded && (
              <div
                className="absolute inset-0 flex items-center justify-center z-10"
                style={{ background: "var(--bg-card)" }}
              >
                <div className="flex flex-col items-center gap-3">
                  <div className="thinking-wave-wrapper">
                    <div className="thinking-bar" />
                    <div className="thinking-bar" />
                    <div className="thinking-bar" />
                  </div>
                  <span className="text-xs" style={{ color: "var(--text-faint)" }}>Loading PDF…</span>
                </div>
              </div>
            )}
            <iframe
              src={url}
              className="w-full h-full border-0"
              title="PDF Viewer"
              onLoad={() => setIframeLoaded(true)}
            />
          </div>
        )}
      </div>
    </div>
  );
});
