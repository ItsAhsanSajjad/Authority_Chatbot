"use client";

import { useEffect, useState, useCallback } from "react";
import type { ToastMessage } from "../../lib/types";

let globalId = 0;
const listeners: Set<(msg: ToastMessage) => void> = new Set();

/** Call this from anywhere to show a toast */
export function showToast(text: string, type: ToastMessage["type"] = "info") {
  const msg: ToastMessage = { id: String(++globalId), text, type };
  listeners.forEach((fn) => fn(msg));
}

/** Toast container -- mount once in the app shell */
export function ToastContainer() {
  const [toasts, setToasts] = useState<ToastMessage[]>([]);

  const addToast = useCallback((msg: ToastMessage) => {
    setToasts((prev) => [...prev, msg]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== msg.id));
    }, 4000);
  }, []);

  useEffect(() => {
    listeners.add(addToast);
    return () => { listeners.delete(addToast); };
  }, [addToast]);

  if (toasts.length === 0) return null;

  const borderColors: Record<string, string> = {
    success: "var(--green)",
    error: "var(--red)",
    info: "var(--accent)",
  };

  return (
    <div className="fixed top-16 right-4 z-[60] flex flex-col gap-2 pointer-events-none">
      {toasts.map((t) => (
        <div
          key={t.id}
          className="toast-item"
          style={{
            borderLeftColor: borderColors[t.type] || "var(--accent)",
          }}
        >
          {t.type === "success" && (
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--green)" strokeWidth="2.5"><polyline points="20 6 9 17 4 12"/></svg>
          )}
          {t.type === "error" && (
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--red)" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>
          )}
          {t.type === "info" && (
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
          )}
          <span className="toast-text">{t.text}</span>
        </div>
      ))}
    </div>
  );
}
