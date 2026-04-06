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

/** Toast container — mount once in the app shell */
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

  return (
    <div className="fixed top-16 right-4 z-[60] flex flex-col gap-2 pointer-events-none">
      {toasts.map((t) => (
        <div
          key={t.id}
          className="pointer-events-auto px-4 py-2.5 rounded-xl text-sm font-medium shadow-lg animate-toast-in"
          style={{
            background: t.type === "error" ? "var(--red)" : t.type === "success" ? "var(--green)" : "var(--accent)",
            color: "white",
          }}
        >
          {t.text}
        </div>
      ))}
    </div>
  );
}
