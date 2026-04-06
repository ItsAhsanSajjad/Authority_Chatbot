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
            <div className="flex items-center gap-3 mb-5">
              <div className="w-9 h-9 rounded-xl overflow-hidden" style={{ boxShadow: "var(--shadow-glow)" }}>
                <Image src="/pera_logo.png" alt="PERA" width={36} height={36} />
              </div>
              <div>
                <h2 className="font-bold text-sm" style={{ color: "var(--text-primary)" }}>PERA AI</h2>
                <p className="text-xs font-medium" style={{ color: "var(--text-secondary)" }}>Chat History</p>
              </div>
            </div>

            {/* New Chat */}
            <button
              onClick={onNewChat}
              className="new-chat-btn flex items-center justify-center gap-2 py-2.5 mb-4 text-sm w-full"
              aria-label="Start a new chat"
            >
              <span>✦</span> New Chat
            </button>

            {/* Chat List */}
            <div className="flex-1 overflow-y-auto space-y-1">
              {chatHistory.length === 0 ? (
                <p className="text-center text-xs py-8" style={{ color: "var(--text-faint)" }}>
                  No conversations yet
                </p>
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
                    <span className="text-sm">💬</span>
                    <span className="flex-1 text-xs truncate" style={{ color: "var(--text-primary)" }}>
                      {s.title}
                    </span>
                    <button
                      onClick={(e) => { e.stopPropagation(); onDeleteChat(s.id); }}
                      className="opacity-0 group-hover:opacity-100 focus:opacity-100 text-[10px] px-1.5 py-0.5 rounded-md transition-opacity"
                      style={{ color: "var(--red)", background: "var(--bg-hover)" }}
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
              <p className="text-xs font-semibold tracking-wider text-center uppercase" style={{ color: "var(--text-secondary)" }}>
                Built by PERA AI TEAM
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
