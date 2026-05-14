"use client";

import { useRef, useEffect, useState, useCallback, memo } from "react";
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
  /** Edit a previously-sent user message at the given index. */
  onEditMessage?: (index: number) => void;
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
  onEditMessage,
}: Props) {
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);

  // Typewriter state — scoped to this component only
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

  // ── Scroll behavior — stable during streaming ────────────────────
  //
  // Root cause of the streaming-time "bubble shake" was firing
  // `scrollIntoView({ behavior: "smooth" })` on every typewriter tick
  // (~60Hz). Each call started a fresh 200–500ms smooth animation,
  // colliding with the next tick before it finished — visual jitter.
  //
  // Fix:
  //   • During typing → direct `scrollTop = scrollHeight` write (no
  //     animation), throttled to once per ~100ms, only when the user
  //     is near the bottom. No fight with browser scroll anchoring.
  //   • For one-shot events (loading appears, user sends a message,
  //     typewriter completes) → one smooth scroll.
  //   • If the user scrolls up, we stop pinning; resumes when they
  //     return within `STICKY_PX`.
  const STICKY_PX = 120;
  const isNearBottom = useCallback(() => {
    const el = scrollContainerRef.current;
    if (!el) return true;
    return el.scrollHeight - el.scrollTop - el.clientHeight < STICKY_PX;
  }, []);

  // Mark each programmatic scroll so the scroll listener can
  // distinguish "we wrote this" from "user scrolled" and avoid the
  // feedback loop that flipped the sticky flag during streaming.
  const programmaticUntilRef = useRef(0);
  const PROG_GRACE_MS = 80;

  const scrollToBottomInstant = useCallback(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    programmaticUntilRef.current = performance.now() + PROG_GRACE_MS;
    el.scrollTop = el.scrollHeight;
  }, []);

  // Smooth scroll retained only for the moment the user submits a new
  // message — there is nothing else in flight at that point so it
  // cannot collide with another scroll. Streaming and completion both
  // use the instant variant exclusively.
  const scrollToBottomSmoothUserSend = useCallback(() => {
    programmaticUntilRef.current = performance.now() + 800;
    chatEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, []);

  // One-shot: thinking indicator appears. Use INSTANT scroll —
  // a smooth scroll here collides with the smooth scroll from a
  // freshly added user message AND with typewriter ticks moments
  // later, producing UP-direction jerks while the content grows.
  useEffect(() => {
    if (loading) scrollToBottomInstant();
  }, [loading, scrollToBottomInstant]);

  // One-shot: user sent a fresh message (last item is user). Also
  // instant — see above note. Smooth scrolls only on the typewriter
  // completion handler where no other animation can race it.
  const lastMsgRoleRef = useRef<string | null>(null);
  const lastMsgTsRef = useRef<number | null>(null);
  useEffect(() => {
    const last = messages[messages.length - 1];
    if (!last) return;
    const changed =
      last.role !== lastMsgRoleRef.current ||
      last.timestamp !== lastMsgTsRef.current;
    if (changed && last.role === "user") {
      scrollToBottomSmoothUserSend();
    }
    lastMsgRoleRef.current = last.role;
    lastMsgTsRef.current = last.timestamp;
  }, [messages, scrollToBottomSmoothUserSend]);

  // Streaming pin: a ResizeObserver on the message list pins the
  // viewport to the bottom whenever the content actually changes
  // size. This is more stable than firing a React effect on every
  // `displayedText` token — the observer waits for layout to settle
  // before we write `scrollTop`, so the bubble doesn't oscillate
  // between "before-flip" and "after-flip" heights as the markdown
  // parser commits a fresh table row.
  const userScrolledAwayRef = useRef(false);
  const contentRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const target = contentRef.current;
    if (!target) return;
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(() => {
      if (userScrolledAwayRef.current) return;
      if (!isTyping) return;
      scrollToBottomInstant();
    });
    ro.observe(target);
    return () => ro.disconnect();
  }, [isTyping, scrollToBottomInstant]);

  // Detect user-initiated scrolls — wheel, trackpad gestures, touch
  // drag, scrollbar drag (mousedown on container), PageUp/PageDown/
  // Home/End/arrows. We deliberately do NOT listen to plain `scroll`
  // events because they also fire for our own programmatic writes
  // and produced a feedback loop that detached the stickiness flag
  // mid-stream.
  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    const reevaluate = () => {
      userScrolledAwayRef.current = !isNearBottom();
    };
    const onWheel = (e: WheelEvent) => {
      if (e.deltaY < 0) {
        userScrolledAwayRef.current = true;
        return;
      }
      requestAnimationFrame(reevaluate);
    };
    const onTouchEnd = () => requestAnimationFrame(reevaluate);
    const onMouseDown = (e: MouseEvent) => {
      // mousedown on the container itself (not a child button) is
      // most likely a scrollbar drag. Re-evaluate on mouseup.
      if (e.target === el) {
        const onUp = () => {
          requestAnimationFrame(reevaluate);
          window.removeEventListener("mouseup", onUp);
        };
        window.addEventListener("mouseup", onUp);
      }
    };
    const onKeyDown = (e: KeyboardEvent) => {
      if (
        e.key === "PageUp" || e.key === "PageDown" ||
        e.key === "ArrowUp" || e.key === "ArrowDown" ||
        e.key === "Home" || e.key === "End"
      ) {
        requestAnimationFrame(reevaluate);
      }
    };
    el.addEventListener("wheel", onWheel, { passive: true });
    el.addEventListener("touchend", onTouchEnd, { passive: true });
    el.addEventListener("mousedown", onMouseDown);
    el.addEventListener("keydown", onKeyDown);
    return () => {
      el.removeEventListener("wheel", onWheel);
      el.removeEventListener("touchend", onTouchEnd);
      el.removeEventListener("mousedown", onMouseDown);
      el.removeEventListener("keydown", onKeyDown);
    };
  }, [isNearBottom]);

  // Settle exactly at the bottom when the typewriter completes.
  // We schedule three checks — next frame, +150ms, +500ms — to catch
  // any late layout (e.g. footer/freshness line appended after the
  // last typewriter tick). Each settle is an instant write when the
  // remaining gap is small (≤40px), or a single smooth scroll when
  // the user is far up. Skipping when gap ≤ 4px avoids any bounce.
  const prevIsTypingRef = useRef(false);
  useEffect(() => {
    if (!(prevIsTypingRef.current && !isTyping && !userScrolledAwayRef.current)) {
      prevIsTypingRef.current = isTyping;
      return;
    }
    prevIsTypingRef.current = isTyping;
    const el = scrollContainerRef.current;
    if (!el) return;
    const settleOnce = () => {
      if (!el || userScrolledAwayRef.current) return;
      const dist = el.scrollHeight - el.scrollTop - el.clientHeight;
      if (dist <= 4) return;
      // Instant scroll for ALL settle cases. No smooth fallback even
      // for large distances — if the user has not detached, snap them
      // to the bottom; if they have detached, the early return above
      // skips entirely.
      scrollToBottomInstant();
    };
    const rafId = requestAnimationFrame(settleOnce);
    const t1 = window.setTimeout(settleOnce, 150);
    const t2 = window.setTimeout(settleOnce, 500);
    const t3 = window.setTimeout(settleOnce, 1200);
    return () => {
      cancelAnimationFrame(rafId);
      window.clearTimeout(t1);
      window.clearTimeout(t2);
      window.clearTimeout(t3);
    };
  }, [isTyping, scrollToBottomInstant]);

  const showWelcome = messages.length === 0 && !loading;

  return (
    <div
      ref={scrollContainerRef}
      className="flex-1 overflow-y-auto px-4 md:px-0"
      style={{
        background: "var(--bg-chat)",
        // Disable browser scroll-anchoring so it does not fight our
        // manual auto-scroll while tokens stream in.
        overflowAnchor: "none",
      }}
    >
      <div ref={contentRef} className="max-w-3xl mx-auto py-6 space-y-5">
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
              onEditOwn={
                msg.role === "user" && onEditMessage
                  ? () => onEditMessage(i)
                  : undefined
              }
            />
          );
        })}

        {loading && <ThinkingIndicator />}

        <div ref={chatEndRef} />
      </div>
    </div>
  );
});
