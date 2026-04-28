"use client";

import Image from "next/image";
import { memo } from "react";
import type { ChatSession } from "../../lib/types";

interface Props {
  isOpen: boolean;
  onToggle: () => void;
  chatHistory: ChatSession[];
  currentChatId: string;
  onNewChat: () => void;
  onLoadChat: (session: ChatSession) => void;
  onDeleteChat: (id: string) => void;
}

export const ChatSidebar = memo(function ChatSidebar({
  isOpen,
  onToggle,
  chatHistory,
  currentChatId,
  onNewChat,
  onLoadChat,
  onDeleteChat,
}: Props) {
  return (
    <>
      <aside
        className={`sidebar fixed md:relative h-full flex flex-col transition-all duration-300
          ${isOpen ? "w-72 translate-x-0" : "w-0 -translate-x-full md:w-0 md:-translate-x-full"}`}
        style={{ overflow: "hidden" }}
        aria-label="Chat history sidebar"
      >
        {isOpen && (
          <div className="flex flex-col h-full w-72 p-4" style={{ overflow: "hidden" }}>
            {/* Sidebar Header */}
            <div className="flex items-center gap-3 mb-1">
              <div
                className="w-10 h-10 rounded-xl overflow-hidden flex items-center justify-center"
                style={{
                  background: "linear-gradient(160deg, rgba(244, 211, 122, 0.18), rgba(212, 160, 23, 0.06))",
                  border: "1px solid rgba(212, 160, 23, 0.28)",
                  boxShadow: "0 6px 18px -8px rgba(212, 160, 23, 0.4)",
                }}
              >
                <Image src="/Authority_Logo.png" alt="PERA" width={32} height={32} />
              </div>
              <div>
                <h2
                  className="font-bold text-[13px] leading-tight tracking-wide"
                  style={{ color: "var(--text-primary)" }}
                >
                  PERA AUTHORITY
                  <br />
                  CHATBOT
                </h2>
                <p
                  className="text-[10px] mt-0.5 font-semibold uppercase tracking-widest"
                  style={{ color: "var(--text-faint)" }}
                >
                  Conversation History
                </p>
              </div>
            </div>

            {/* Subtle accent rule */}
            <div
              className="my-4"
              style={{
                height: 1,
                background:
                  "linear-gradient(90deg, transparent, rgba(212,160,23,0.35), transparent)",
              }}
              aria-hidden
            />

            {/* New Chat */}
            <button
              onClick={onNewChat}
              className="new-chat-btn flex items-center justify-center gap-2 py-2.5 mb-4 text-sm w-full"
              aria-label="Start a new chat"
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                <line x1="12" y1="5" x2="12" y2="19" />
                <line x1="5" y1="12" x2="19" y2="12" />
              </svg>
              New Conversation
            </button>

            {/* Chat List header */}
            <div
              className="px-2 mb-2 text-[9px] font-bold tracking-[0.18em] uppercase flex items-center justify-between"
              style={{ color: "var(--text-faint)" }}
            >
              <span>Recent Sessions</span>
              <span style={{ fontSize: 9 }}>{chatHistory.length}</span>
            </div>

            {/* Chat List */}
            <div className="flex-1 overflow-y-auto space-y-1">
              {chatHistory.length === 0 ? (
                <div
                  className="text-center py-10 px-4"
                  style={{ color: "var(--text-faint)" }}
                >
                  <div style={{ fontSize: 28, opacity: 0.4 }}>💬</div>
                  <p className="text-xs mt-2 font-medium">No conversations yet</p>
                  <p className="text-[10px] mt-1" style={{ color: "var(--text-faint)" }}>
                    Your sessions will appear here
                  </p>
                </div>
              ) : (
                chatHistory.map((s) => (
                  <div
                    key={s.id}
                    role="button"
                    tabIndex={0}
                    onClick={() => onLoadChat(s)}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onLoadChat(s); } }}
                    className={`sidebar-item group flex items-center gap-2 ${s.id === currentChatId ? "active" : ""}`}
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ color: "var(--text-secondary)", flexShrink: 0 }}>
                      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
                    </svg>
                    <span className="flex-1 text-xs truncate font-medium" style={{ color: "var(--text-primary)" }}>
                      {s.title}
                    </span>
                    <button
                      onClick={(e) => { e.stopPropagation(); onDeleteChat(s.id); }}
                      className="opacity-0 group-hover:opacity-100 focus:opacity-100 text-[10px] px-1.5 py-0.5 rounded-md transition-opacity"
                      style={{ color: "var(--red)", background: "rgba(244,63,94,0.12)" }}
                      aria-label={`Delete chat: ${s.title}`}
                    >
                      ✕
                    </button>
                  </div>
                ))
              )}
            </div>

            {/* Sidebar Footer */}
            <div className="pt-4 mt-3" style={{ borderTop: "1px solid var(--border)" }}>
              <div className="flex items-center justify-center gap-2 mb-1.5">
                <span
                  className="inst-meta-dot"
                  style={{ width: 6, height: 6 }}
                />
                <p
                  className="text-[10px] font-bold tracking-[0.16em] uppercase"
                  style={{ color: "var(--text-secondary)" }}
                >
                  Official System
                </p>
              </div>
              <p
                className="text-[9.5px] tracking-widest text-center uppercase"
                style={{ color: "var(--text-faint)" }}
              >
                Built by PERA AI Team
              </p>
            </div>
          </div>
        )}
      </aside>

      {/* Mobile Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 z-20 bg-black/40 md:hidden"
          onClick={onToggle}
          aria-hidden="true"
        />
      )}
    </>
  );
});
