"use client";

import { useRef, useEffect, useState, memo } from "react";
import type { Message, Reference } from "../../lib/types";
import { WelcomeScreen } from "./WelcomeScreen";
import { MessageBubble } from "./MessageBubble";
import { ThinkingIndicator } from "./ThinkingIndicator";

interface Props {
  messages: Message[];
  loading: boolean;
  failedPrompt: string | null;
  lastBotIsNew: boolean;
  clearNewFlag: () => void;
  onSendSuggestion: (text: string) => void;
  onOpenPdf: (ref: Reference) => void;
  onRetry: () => void;
}

/**
 * ChatWindow manages the typewriter effect in its own state,
 * so the parent (and sidebar/header) does NOT rerender on every character.
 */
export const ChatWindow = memo(function ChatWindow({
  messages,
  loading,
  failedPrompt,
  lastBotIsNew,
  clearNewFlag,
  onSendSuggestion,
  onOpenPdf,
  onRetry,
}: Props) {
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);

  // Typewriter state -- scoped to this component only
  const [displayedText, setDisplayedText] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [shouldAnimate, setShouldAnimate] = useState(false);

  // Only animate when the hook explicitly tells us a fresh bot message arrived
  useEffect(() => {
    if (lastBotIsNew) {
      setShouldAnimate(true);
      clearNewFlag();
    }
  }, [lastBotIsNew, clearNewFlag]);

  // Typewriter effect using requestAnimationFrame (not setInterval)
  useEffect(() => {
    if (!shouldAnimate || messages.length === 0) return;
    const last = messages[messages.length - 1];
    if (last.role !== "assistant") return;
    const full = last.content;

    setIsTyping(true);
    setDisplayedText("");
    let i = 0;
    let lastTime = 0;
    const speed = Math.max(6, Math.min(18, 1500 / full.length));
    let rafId: number;

    const tick = (time: number) => {
      if (!lastTime) lastTime = time;
      const elapsed = time - lastTime;
      const charsToAdvance = Math.max(1, Math.floor(elapsed / speed));

      if (elapsed >= speed) {
        i = Math.min(i + charsToAdvance, full.length);
        setDisplayedText(full.slice(0, i));
        lastTime = time;
      }

      if (i >= full.length) {
        setIsTyping(false);
        setShouldAnimate(false);
        return;
      }
      rafId = requestAnimationFrame(tick);
    };

    rafId = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafId);
  }, [shouldAnimate, messages]);

  // Scroll to bottom when loading indicator appears
  useEffect(() => {
    if (loading) {
      chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [loading]);

  // Scroll when new user message is sent
  useEffect(() => {
    const last = messages[messages.length - 1];
    if (last?.role === "user") {
      chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages]);

  // Gentle scroll during typing -- only if user is already near the bottom
  useEffect(() => {
    if (!isTyping) return;
    const container = scrollContainerRef.current;
    if (!container) return;
    const threshold = 100;
    const isNearBottom =
      container.scrollHeight - container.scrollTop - container.clientHeight < threshold;
    if (isNearBottom) {
      chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [displayedText, isTyping]);

  const showWelcome = messages.length === 0 && !loading;

  return (
    <div
      ref={scrollContainerRef}
      className="flex-1 overflow-y-auto px-4 md:px-0"
      style={{ background: "var(--bg-chat)" }}
    >
      <div className="max-w-3xl mx-auto py-6 space-y-4">
        {showWelcome && <WelcomeScreen onSendMessage={onSendSuggestion} />}

        {messages.map((msg, i) => {
          const isLast = i === messages.length - 1;
          const showTypewriter = isLast && isTyping && msg.role === "assistant";
          return (
            <MessageBubble
              key={`${msg.timestamp}-${i}`}
              message={msg}
              typingText={showTypewriter ? displayedText : undefined}
              isTyping={showTypewriter}
              onOpenPdf={onOpenPdf}
              onRetry={msg.failed ? onRetry : undefined}
              onSendQuery={onSendSuggestion}
            />
          );
        })}

        {loading && <ThinkingIndicator />}

        <div ref={chatEndRef} />
      </div>
    </div>
  );
});
