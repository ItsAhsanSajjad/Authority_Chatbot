"use client";

import { useState, useCallback, useEffect, useRef } from "react";
import type { Message, ChatSession, ConnectionStatus, AnswerSourceMode } from "../lib/types";
import { STORAGE_VERSION } from "../lib/types";
import { loadChatData, saveChatData } from "../lib/storage";
import { askQuestion } from "../lib/api";

const genId = () =>
  Math.random().toString(36).slice(2) + Date.now().toString(36);

export function useChatSessions(healthCallbacks: {
  reportSuccess: () => void;
  reportFailure: () => void;
}) {
  const [chatHistory, setChatHistory] = useState<ChatSession[]>([]);
  const [currentChatId, setCurrentChatId] = useState<string>(genId());
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(false);
  const [failedPrompt, setFailedPrompt] = useState<string | null>(null);
  const [lastBotIsNew, setLastBotIsNew] = useState(false);
  const serverSessionId = useRef<string | undefined>(undefined);

  const initialized = useRef(false);

  // ─── Load from localStorage on mount ───
  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    const data = loadChatData();
    setChatHistory(data.sessions);
    if (data.activeChatId) {
      const active = data.sessions.find((s) => s.id === data.activeChatId);
      if (active) {
        setCurrentChatId(active.id);
        setMessages(active.messages);
      }
    }
  }, []);

  // ─── Persist to localStorage whenever state changes ───
  const persist = useCallback(
    (sessions: ChatSession[], activeId: string, msgs: Message[]) => {
      // Upsert the current chat into sessions
      const now = Date.now();
      let updated = [...sessions];

      if (msgs.length > 0) {
        const title = msgs[0].content.slice(0, 38);
        const idx = updated.findIndex((s) => s.id === activeId);
        const session: ChatSession = {
          id: activeId,
          title: title.length >= 38 ? title + "…" : title,
          messages: msgs,
          createdAt: idx >= 0 ? updated[idx].createdAt : now,
          updatedAt: now,
        };
        if (idx >= 0) {
          updated[idx] = session;
        } else {
          updated = [session, ...updated];
        }
      }

      saveChatData({
        version: STORAGE_VERSION,
        sessions: updated,
        activeChatId: activeId,
      });

      return updated;
    },
    [],
  );

  // ─── Auto-persist on every message change ───
  useEffect(() => {
    if (!initialized.current) return;
    const updated = persist(chatHistory, currentChatId, messages);
    // Update chatHistory without re-triggering this effect unnecessarily
    setChatHistory((prev) => {
      // Only update if contents actually changed
      if (JSON.stringify(prev) === JSON.stringify(updated)) return prev;
      return updated;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [messages, currentChatId]);

  // ─── Send Message ───
  const sendMessage = useCallback(
    async (text: string, sourceMode: AnswerSourceMode = "both") => {
      const trimmed = text.trim();
      if (!trimmed) return;

      setFailedPrompt(null);
      const userMsg: Message = {
        role: "user",
        content: trimmed,
        timestamp: Date.now(),
      };
      setMessages((prev) => [...prev, userMsg]);
      setLoading(true);

      const history = messages.map((m) => ({ role: m.role, content: m.content }));
      const result = await askQuestion({
        question: trimmed,
        conversation_history: history,
        session_id: serverSessionId.current,
        answer_source_mode: sourceMode,
      });

      if (result.ok) {
        healthCallbacks.reportSuccess();
        // Persist server session_id for follow-up anchoring
        if (result.data.session_id) {
          serverSessionId.current = result.data.session_id;
        }
        const botMsg: Message = {
          role: "assistant",
          content: result.data.answer || "Sorry, I could not process that.",
          references: result.data.references || [],
          timestamp: Date.now(),
          sourceMode: result.data.source_mode,
          sourceModeLabel: result.data.source_mode_label,
          provenance: result.data.provenance,
        };
        setMessages((prev) => [...prev, botMsg]);
        setLastBotIsNew(true);
      } else {
        healthCallbacks.reportFailure();
        setFailedPrompt(trimmed);
        const errorMsg: Message = {
          role: "assistant",
          content: `⚠️ ${result.error.message}`,
          timestamp: Date.now(),
          failed: true,
        };
        setMessages((prev) => [...prev, errorMsg]);
      }
      setLoading(false);
    },
    [messages, healthCallbacks],
  );

  // ─── Retry failed message ───
  const retryLastFailed = useCallback(() => {
    if (!failedPrompt) return;
    // Remove the failed assistant message
    setMessages((prev) => {
      const last = prev[prev.length - 1];
      if (last?.failed) return prev.slice(0, -1);
      return prev;
    });
    // Also remove the user message that triggered the failure
    setMessages((prev) => {
      const last = prev[prev.length - 1];
      if (last?.role === "user" && last.content === failedPrompt) {
        return prev.slice(0, -1);
      }
      return prev;
    });
    // Re-send with the same prompt
    const prompt = failedPrompt;
    setFailedPrompt(null);
    // Use setTimeout to allow state to settle before sending
    setTimeout(() => sendMessage(prompt), 0);
  }, [failedPrompt, sendMessage]);

  // ─── Start New Chat ───
  const startNewChat = useCallback(() => {
    setMessages([]);
    setCurrentChatId(genId());
    setFailedPrompt(null);
    serverSessionId.current = undefined;
  }, []);

  // ─── Load Chat ───
  const loadChat = useCallback(
    (session: ChatSession) => {
      setMessages(session.messages);
      setCurrentChatId(session.id);
      setFailedPrompt(null);
    },
    [],
  );

  // ─── Delete Chat ───
  const deleteChat = useCallback(
    (id: string) => {
      setChatHistory((prev) => {
        const filtered = prev.filter((c) => c.id !== id);
        // Persist immediately with filtered list
        saveChatData({
          version: STORAGE_VERSION,
          sessions: filtered,
          activeChatId: id === currentChatId ? null : currentChatId,
        });
        return filtered;
      });
      // If deleting the current chat, start fresh without saving it back
      if (id === currentChatId) {
        setMessages([]);
        setCurrentChatId(genId());
        setFailedPrompt(null);
      }
    },
    [currentChatId],
  );

  return {
    messages,
    chatHistory,
    currentChatId,
    loading,
    failedPrompt,
    lastBotIsNew,
    clearNewFlag: useCallback(() => setLastBotIsNew(false), []),
    sendMessage,
    retryLastFailed,
    startNewChat,
    loadChat,
    deleteChat,
  } as const;
}
