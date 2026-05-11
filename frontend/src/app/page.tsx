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
  const labels: Record<ConnectionStatus, string> = {
    online: "System Active",
    connecting: "Connecting",
    offline: "System Unavailable",
  };
  const stateClass =
    status === "online" ? "" : status === "connecting" ? "is-connecting" : "is-offline";
  return (
    <div className={`status-pill-pro ${stateClass}`} role="status" aria-live="polite">
      <span className="status-pill-dot-pro" />
      {labels[status]}
    </div>
  );
}


export default function Home() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [pdfModal, setPdfModal] = useState<{ url: string; title: string } | null>(null);
  const [sourceMode, setSourceMode] = useState<AnswerSourceMode>("stored_api");

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
    stopGeneration,
    editLastQuery,
    editMessage,
    retryLastFailed,
    startNewChat,
    loadChat,
    deleteChat,
  } = useChatSessions({ reportSuccess, reportFailure });

  // Pre-fill state pushed into the Composer when the user clicks the
  // edit icon on one of their own message bubbles. Bumping `key` makes
  // the Composer re-apply even if the same text is re-edited.
  const [prefill, setPrefill] = useState<{ text: string; key: number } | null>(null);
  const handleEditMessage = useCallback(
    (index: number) => {
      const original = editMessage(index);
      if (original) {
        setPrefill({ text: original, key: Date.now() });
      }
    },
    [editMessage],
  );

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
        {/* Premium Institutional Header */}
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
            <div className="brand-emblem">
              <Image src="/Authority_Logo.png" alt="PERA Emblem" width={32} height={32} priority />
            </div>
            <div className="brand-name-stack">
              <span className="brand-name-primary">PERA AUTHORITY CHATBOT</span>
              <span className="brand-name-secondary hidden sm:inline">Punjab Enforcement &amp; Regulatory Authority</span>
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
          onEditMessage={handleEditMessage}
        />

        {/* Query Interface */}
        <Composer
          onSend={handleSend}
          disabled={loading}
          sourceMode={sourceMode}
          onSourceModeChange={setSourceMode}
          isGenerating={loading}
          onStop={stopGeneration}
          onEdit={editLastQuery}
          prefill={prefill}
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
