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
        className={`sidebar ${isOpen ? "open" : ""}`}
        aria-label="Chat history sidebar"
      >
        {isOpen && (
          <div className="sidebar-inner">
            {/* Sidebar Header */}
            <div className="sidebar-header">
              <div className="sidebar-logo">
                <Image src="/pera_logo.png" alt="PERA" width={32} height={32} />
              </div>
              <div>
                <h2 className="sidebar-brand-title">PERA AI</h2>
                <p className="sidebar-brand-subtitle">Chat History</p>
              </div>
            </div>

            {/* New Chat Button */}
            <button
              onClick={onNewChat}
              className="new-chat-btn"
              aria-label="Start a new chat"
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                <line x1="12" y1="5" x2="12" y2="19" />
                <line x1="5" y1="12" x2="19" y2="12" />
              </svg>
              New Chat
            </button>

            {/* Chat List */}
            <div className="sidebar-chat-list">
              {chatHistory.length === 0 ? (
                <p className="sidebar-empty-text">
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
                    className={`sidebar-item group ${s.id === currentChatId ? "sidebar-item--active" : ""}`}
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" className="sidebar-item-icon">
                      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
                    </svg>
                    <span className="sidebar-item-title">
                      {s.title}
                    </span>
                    <button
                      onClick={(e) => { e.stopPropagation(); onDeleteChat(s.id); }}
                      className="sidebar-delete-btn"
                      aria-label={`Delete chat: ${s.title}`}
                    >
                      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <polyline points="3 6 5 6 21 6" />
                        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                      </svg>
                    </button>
                  </div>
                ))
              )}
            </div>

            {/* Sidebar Footer */}
            <div className="sidebar-footer">
              <p className="sidebar-footer-text">
                BUILT BY PERA AI TEAM
              </p>
            </div>
          </div>
        )}
      </aside>

      {/* Mobile Backdrop with blur */}
      {isOpen && (
        <div
          className="sidebar-backdrop"
          onClick={onToggle}
          aria-hidden="true"
        />
      )}
    </>
  );
});
