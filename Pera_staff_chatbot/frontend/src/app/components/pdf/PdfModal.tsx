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
      className="pdf-modal-wrapper"
      role="dialog"
      aria-modal="true"
      aria-label={title}
    >
      {/* Glass overlay */}
      <div className="modal-overlay" onClick={onClose} aria-hidden="true" />

      {/* Modal card with scale-in animation */}
      <div
        ref={modalRef}
        className="modal-content"
      >
        {/* Header */}
        <div className="modal-header">
          <div className="modal-header-title">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="1.5" style={{ flexShrink: 0 }}>
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
              <polyline points="14 2 14 8 20 8" />
            </svg>
            <span className="modal-header-text">
              {title}
            </span>
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            <a
              href={url}
              target="_blank"
              rel="noopener noreferrer"
              className="modal-open-tab-btn"
            >
              Open in Tab
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            </a>
            <button
              ref={closeBtnRef}
              onClick={onClose}
              className="modal-close-btn"
              aria-label="Close PDF viewer"
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
          </div>
        </div>

        {/* Content */}
        {isMobile ? (
          <div className="modal-mobile-fallback">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" style={{ color: "var(--text-faint)" }}>
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
              <polyline points="14 2 14 8 20 8" />
            </svg>
            <p className="text-sm text-center" style={{ color: "var(--text-secondary)" }}>
              PDF preview is not supported on this device.
            </p>
            <a
              href={url}
              target="_blank"
              rel="noopener noreferrer"
              className="modal-mobile-open-btn"
            >
              Open PDF in Browser
            </a>
          </div>
        ) : (
          <div className="modal-iframe-wrapper">
            {/* Loading state */}
            {!iframeLoaded && (
              <div className="modal-loading">
                <div className="flex flex-col items-center gap-3">
                  <div className="thinking-dots">
                    <div className="thinking-dot" />
                    <div className="thinking-dot" />
                    <div className="thinking-dot" />
                  </div>
                  <span className="text-xs" style={{ color: "var(--text-faint)" }}>Loading PDF...</span>
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
