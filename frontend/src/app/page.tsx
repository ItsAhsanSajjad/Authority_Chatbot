"use client";

import { useState, useCallback, useEffect, useRef } from "react";
import Image from "next/image";
import type { Reference, AnswerSourceMode } from "./lib/types";
import { buildPdfUrl } from "./lib/api";
import { useChatSessions } from "./hooks/useChatSessions";
import { useThemePreference } from "./hooks/useThemePreference";
import { useHealthCheck } from "./hooks/useHealthCheck";
import { ChatSidebar } from "./components/sidebar/ChatSidebar";
import { ChatWindow } from "./components/chat/ChatWindow";
import { Composer } from "./components/chat/Composer";
import { PdfModal } from "./components/pdf/PdfModal";
import { ToastContainer } from "./components/common/Toast";
import type { ConnectionStatus } from "./lib/types";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

function StatusBadge({ status }: { status: ConnectionStatus }) {
  const colors: Record<ConnectionStatus, string> = {
    online: "var(--green)",
    connecting: "var(--gold)",
    offline: "var(--red)",
  };
  const labels: Record<ConnectionStatus, string> = {
    online: "System Status: Active",
    connecting: "Connecting",
    offline: "System Status: Unavailable",
  };
  return (
    <div className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg" style={{ background: "var(--bg-card)", border: "1px solid var(--border)" }}>
      <div
        className={`w-2 h-2 rounded-full ${status === "online" ? "status-ping" : ""}`}
        style={{ background: colors[status] }}
      />
      <span className="text-[11px] font-medium tracking-wide" style={{ color: "var(--text-secondary)" }}>
        {labels[status]}
      </span>
    </div>
  );
}

export default function Home() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [pdfModal, setPdfModal] = useState<{ url: string; title: string } | null>(null);
  const [sourceMode, setSourceMode] = useState<AnswerSourceMode>("both");

  const { theme, toggleTheme } = useThemePreference();
  const { status, reportSuccess, reportFailure } = useHealthCheck();

  /* — Startup: confirm API origin — */
  useEffect(() => {
    console.log("[PERA] API_URL →", API_URL);
  }, []);

  const {
    messages,
    chatHistory,
    currentChatId,
    loading,
    failedPrompt,
    lastBotIsNew,
    clearNewFlag,
    sendMessage,
    retryLastFailed,
    startNewChat,
    loadChat,
    deleteChat,
  } = useChatSessions({ reportSuccess, reportFailure });

  const handleSend = useCallback(
    (text: string) => {
      sendMessage(text, sourceMode);
    },
    [sendMessage, sourceMode],
  );

  const handleOpenPdf = useCallback((ref: Reference) => {
    const pg = ref.page_start || ref.page || 1;
    const url = buildPdfUrl(ref);
    setPdfModal({ url, title: `${ref.document} — Page ${pg}` });
  }, []);

  const handleSidebarToggle = useCallback(() => {
    setSidebarOpen((p) => !p);
  }, []);

  const handleNewChat = useCallback(() => {
    startNewChat();
    setSidebarOpen(false);
  }, [startNewChat]);

  const handleLoadChat = useCallback(
    (session: Parameters<typeof loadChat>[0]) => {
      loadChat(session);
      setSidebarOpen(false);
    },
    [loadChat],
  );

  return (
    <div className="flex h-screen overflow-hidden relative" style={{ background: "var(--bg-page)" }}>
      {/* Subtle ambient — reduced for institutional feel */}
      <div className="ambient-bg" />

      {/* Sidebar */}
      <ChatSidebar
        isOpen={sidebarOpen}
        onToggle={handleSidebarToggle}
        chatHistory={chatHistory}
        currentChatId={currentChatId}
        onNewChat={handleNewChat}
        onLoadChat={handleLoadChat}
        onDeleteChat={deleteChat}
      />

      {/* Main Area */}
      <main className="flex-1 flex flex-col relative z-10 min-w-0">
        {/* Institutional Header */}
        <header className="inst-header flex items-center justify-between px-4 md:px-6 py-3 z-20 relative">
          <div className="flex items-center gap-3">
            <button
              onClick={handleSidebarToggle}
              className="p-2 rounded-lg transition-colors"
              style={{ color: "var(--text-secondary)" }}
              aria-label={sidebarOpen ? "Close sidebar" : "Open sidebar"}
              onMouseEnter={(e) => (e.currentTarget.style.background = "var(--bg-hover)")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                <line x1="3" y1="6" x2="21" y2="6" /><line x1="3" y1="12" x2="15" y2="12" /><line x1="3" y1="18" x2="18" y2="18" />
              </svg>
            </button>
            <Image src="/pera_logo.png" alt="PERA Emblem" width={32} height={32} className="rounded-lg" priority />
            <div>
              <div className="flex items-center gap-2">
                <h1 className="font-semibold text-sm tracking-wide" style={{ color: "var(--text-primary)" }}>
                  PERA AI Assistant
                </h1>
              </div>
              <p className="text-[11px] font-medium tracking-wide hidden sm:block" style={{ color: "var(--text-secondary)" }}>
                Punjab Enforcement & Regulatory Authority
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <button
              onClick={toggleTheme}
              className="theme-toggle"
              role="switch"
              aria-checked={theme === "dark"}
              aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
            >
              <div className="theme-toggle-knob">{theme === "dark" ? "🌙" : "☀️"}</div>
            </button>
            <StatusBadge status={status} />
          </div>
        </header>

        {/* Chat Messages */}
        <ChatWindow
          messages={messages}
          loading={loading}
          failedPrompt={failedPrompt}
          lastBotIsNew={lastBotIsNew}
          clearNewFlag={clearNewFlag}
          onSendSuggestion={handleSend}
          onOpenPdf={handleOpenPdf}
          onRetry={retryLastFailed}
        />

        {/* Query Interface */}
        <Composer
          onSend={handleSend}
          disabled={loading}
          sourceMode={sourceMode}
          onSourceModeChange={setSourceMode}
        />
      </main>

      {/* PDF Viewer Modal */}
      {pdfModal && (
        <PdfModal
          url={pdfModal.url}
          title={pdfModal.title}
          onClose={() => setPdfModal(null)}
        />
      )}

      {/* Toasts */}
      <ToastContainer />
    </div>
  );
}
