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

  // ─── In-flight request bookkeeping ───
  // Tracks which chat owns the active request so a late response from
  // an old chat can't leak into the user's current view, AND tracks the
  // user prompt so the "edit & stop" button can return it.
  const inflightChatId = useRef<string | null>(null);
  const inflightAbort = useRef<AbortController | null>(null);
  const inflightPrompt = useRef<string | null>(null);

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

      // Cancel any prior in-flight request before starting a new one.
      if (inflightAbort.current) {
        inflightAbort.current.abort();
        inflightAbort.current = null;
      }

      // Stamp this request with the chat that owns it. If the user
      // switches chats / starts a new one before the response arrives,
      // we'll detect the mismatch and discard the response so it never
      // leaks into the wrong conversation.
      const ownerChatId = currentChatId;
      const controller = new AbortController();
      inflightChatId.current = ownerChatId;
      inflightAbort.current = controller;
      inflightPrompt.current = trimmed;

      setFailedPrompt(null);
      const userMsg: Message = {
        role: "user",
        content: trimmed,
        timestamp: Date.now(),
      };
      setMessages((prev) => [...prev, userMsg]);
      setLoading(true);

      const history = messages.map((m) => ({ role: m.role, content: m.content }));
      const result = await askQuestion(
        {
          question: trimmed,
          conversation_history: history,
          session_id: serverSessionId.current,
          answer_source_mode: sourceMode,
        },
        controller.signal,
      );

      // ── Late-response guard ──
      // If the user switched chats while this fetch was in flight,
      // ownerChatId !== currentChatId — silently drop the result.
      // (We use the ref because closures capture the OLD currentChatId;
      // the ref always reflects the latest value via the ref's lifetime.)
      if (inflightChatId.current !== ownerChatId) {
        return;
      }

      // Clear bookkeeping for this completed request.
      inflightChatId.current = null;
      inflightAbort.current = null;
      inflightPrompt.current = null;

      if (result.ok) {
        healthCallbacks.reportSuccess();
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
      } else if (result.error.type === "abort") {
        // Cancelled by user (stop / edit / new-chat) — surface nothing.
        // Loading was already turned off by stopGeneration().
        return;
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
    [messages, healthCallbacks, currentChatId],
  );

  // ─── Stop generation (cancel in-flight) ───
  const stopGeneration = useCallback(() => {
    if (!inflightAbort.current) return;
    inflightAbort.current.abort();
    inflightAbort.current = null;
    inflightChatId.current = null;
    inflightPrompt.current = null;
    setLoading(false);
  }, []);

  // ─── Edit last query: cancel in-flight, pop the user message back ───
  // Returns the original prompt so the caller (Composer) can pre-fill it.
  const editLastQuery = useCallback((): string | null => {
    const promptToEdit = inflightPrompt.current;
    // Always cancel first (even if there's no user message to pop)
    if (inflightAbort.current) {
      inflightAbort.current.abort();
      inflightAbort.current = null;
    }
    inflightChatId.current = null;
    inflightPrompt.current = null;

    // Remove the trailing user message so the conversation reverts to
    // its prior state — the user is going to re-send a corrected query.
    setMessages((prev) => {
      if (prev.length === 0) return prev;
      const last = prev[prev.length - 1];
      if (last.role === "user") return prev.slice(0, -1);
      return prev;
    });
    setLoading(false);
    return promptToEdit;
  }, []);

  // ─── Edit ANY previously-sent user message ───
  // Used by the per-bubble hover edit button on the user's messages.
  // Slices the conversation back to (but not including) the chosen
  // message — the user can then edit and re-send. Also aborts any
  // in-flight request because the conversation is being rewritten.
  const editMessage = useCallback((index: number): string | null => {
    if (index < 0 || index >= messages.length) return null;
    const target = messages[index];
    if (target.role !== "user") return null;

    if (inflightAbort.current) {
      inflightAbort.current.abort();
      inflightAbort.current = null;
      inflightChatId.current = null;
      inflightPrompt.current = null;
    }
    setLoading(false);
    setMessages((prev) => prev.slice(0, index));
    return target.content;
  }, [messages]);

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

  // Cancel an in-flight fetch (used by stop button).
  const abortInflight = useCallback(() => {
    if (inflightAbort.current) {
      inflightAbort.current.abort();
      inflightAbort.current = null;
    }
    inflightChatId.current = null;
    inflightPrompt.current = null;
    setLoading(false);
  }, []);

  // When the user navigates AWAY from the current chat (new chat / load
  // another / delete current) WHILE a request is in flight, the old
  // chat would be left with a dangling user query and no answer below
  // it ("PERA Board composition" with nothing under it — looks broken).
  //
  // This helper does the right thing synchronously BEFORE switching:
  //   1. Aborts the in-flight request
  //   2. Pops the trailing user message
  //   3. Persists the cleaned state to chat-history + localStorage
  //   4. Removes the chat entirely if it becomes empty
  // Returns true if cleanup actually happened.
  const cleanupOrphanQuery = useCallback((): boolean => {
    if (!inflightAbort.current) return false;

    // Abort + clear refs
    inflightAbort.current.abort();
    inflightAbort.current = null;
    inflightChatId.current = null;
    inflightPrompt.current = null;
    setLoading(false);

    // Decide if there's a trailing user message to drop
    const last = messages[messages.length - 1];
    if (!last || last.role !== "user") return true;
    const cleaned = messages.slice(0, -1);

    // Persist directly — do NOT rely on the auto-persist effect because
    // it would also fire AFTER setCurrentChatId switches us away, and
    // by then the OLD chat's id is no longer current state.
    if (cleaned.length === 0) {
      // Was a fresh chat that only ever held this one query — drop it
      const filtered = chatHistory.filter((s) => s.id !== currentChatId);
      setChatHistory(filtered);
      saveChatData({
        version: STORAGE_VERSION,
        sessions: filtered,
        activeChatId: null,
      });
    } else {
      // Existing chat with prior messages — keep it but pop the orphan
      const updated = chatHistory.map((s) =>
        s.id === currentChatId
          ? { ...s, messages: cleaned, updatedAt: Date.now() }
          : s,
      );
      setChatHistory(updated);
      saveChatData({
        version: STORAGE_VERSION,
        sessions: updated,
        activeChatId: currentChatId,
      });
    }
    return true;
  }, [messages, chatHistory, currentChatId]);

  // ─── Start New Chat ───
  const startNewChat = useCallback(() => {
    cleanupOrphanQuery();
    setMessages([]);
    setCurrentChatId(genId());
    setFailedPrompt(null);
    serverSessionId.current = undefined;
  }, [cleanupOrphanQuery]);

  // ─── Load Chat ───
  const loadChat = useCallback(
    (session: ChatSession) => {
      cleanupOrphanQuery();
      setMessages(session.messages);
      setCurrentChatId(session.id);
      setFailedPrompt(null);
    },
    [cleanupOrphanQuery],
  );

  // ─── Delete Chat ───
  const deleteChat = useCallback(
    (id: string) => {
      // If deleting the current chat AND a request is in flight, just
      // abort + clear bookkeeping (no orphan-query cleanup needed —
      // we're deleting the whole chat anyway).
      if (id === currentChatId) {
        abortInflight();
      }
      setChatHistory((prev) => {
        const filtered = prev.filter((c) => c.id !== id);
        saveChatData({
          version: STORAGE_VERSION,
          sessions: filtered,
          activeChatId: id === currentChatId ? null : currentChatId,
        });
        return filtered;
      });
      if (id === currentChatId) {
        setMessages([]);
        setCurrentChatId(genId());
        setFailedPrompt(null);
      }
    },
    [currentChatId, abortInflight],
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
    stopGeneration,
    editLastQuery,
    editMessage,
    retryLastFailed,
    startNewChat,
    loadChat,
    deleteChat,
  } as const;
}
