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

function StatusDot({ status }: { status: ConnectionStatus }) {
  const colors: Record<ConnectionStatus, string> = {
    online: "var(--green)",
    connecting: "var(--gold)",
    offline: "var(--red)",
  };
  const labels: Record<ConnectionStatus, string> = {
    online: "Online",
    connecting: "Connecting",
    offline: "Offline",
  };
  return (
    <div className="status-badge">
      <span className="status-badge-dot-wrapper">
        <span
          className="status-badge-dot"
          style={{ background: colors[status] }}
        />
        {status === "online" && (
          <span
            className="status-badge-ping"
            style={{ background: colors[status] }}
          />
        )}
      </span>
      <span className="status-badge-label">{labels[status]}</span>
    </div>
  );
}

export default function Home() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [pdfModal, setPdfModal] = useState<{ url: string; title: string } | null>(null);
  const [sourceMode, setSourceMode] = useState<AnswerSourceMode>("both");

  const { theme, toggleTheme } = useThemePreference();
  const { status, reportSuccess, reportFailure } = useHealthCheck();

  useEffect(() => {
    console.log("[PERA] API_URL ->", API_URL);
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
      {/* Animated ambient background mesh */}
      <div className="ambient-mesh" />

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
      <main className="flex-1 flex flex-col relative min-w-0">
        {/* Header */}
        <header className="app-header">
          <div className="flex items-center gap-3 min-w-0 flex-1">
            <button
              onClick={handleSidebarToggle}
              className="app-header-menu-btn flex-shrink-0"
              aria-label={sidebarOpen ? "Close sidebar" : "Open sidebar"}
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                <line x1="3" y1="6" x2="21" y2="6" />
                <line x1="3" y1="12" x2="15" y2="12" />
                <line x1="3" y1="18" x2="18" y2="18" />
              </svg>
            </button>
            <Image src="/pera_logo.png" alt="PERA" width={30} height={30} className="rounded-lg header-logo flex-shrink-0" priority />
            <div className="min-w-0">
              <h1 className="app-header-title truncate">PERA Authority Assistant</h1>
              <p className="app-header-subtitle">
                Punjab Enforcement & Regulatory Authority
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2 flex-shrink-0">
            <StatusDot status={status} />
            <button
              onClick={toggleTheme}
              className="theme-toggle"
              role="switch"
              aria-checked={theme === "dark"}
              aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
            >
              <div className="theme-toggle-knob">
                {theme === "dark" ? (
                  <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ color: "white" }}>
                    <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
                  </svg>
                ) : (
                  <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ color: "white" }}>
                    <circle cx="12" cy="12" r="5" />
                    <path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42" />
                  </svg>
                )}
              </div>
            </button>
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

        {/* Composer */}
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
