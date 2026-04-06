"use client";

import Image from "next/image";
import { memo, useState, useCallback, useMemo } from "react";
import type { Message, Reference } from "../../lib/types";
import { renderMarkdown } from "../../lib/markdown";
import { showToast } from "../common/Toast";

const timeAgo = (ts: number) => {
  const d = Math.floor((Date.now() - ts) / 1000);
  if (d < 60) return "just now";
  if (d < 3600) return `${Math.floor(d / 60)}m ago`;
  if (d < 86400) return `${Math.floor(d / 3600)}h ago`;
  return new Date(ts).toLocaleDateString();
};


const TOPIC_KEYWORDS: { keyword: string; topic: string }[] = [
  { keyword: "enforcement", topic: "Enforcement Procedures" },
  { keyword: "epo", topic: "Enforcement Procedure Orders" },
  { keyword: "compliance", topic: "Compliance Standards" },
  { keyword: "governance", topic: "Governance Structures" },
  { keyword: "board", topic: "PERA Board Composition" },
  { keyword: "kpi", topic: "KPI Frameworks" },
  { keyword: "performance", topic: "Institutional Performance" },
  { keyword: "salary", topic: "Pay Scales & Benefits" },
  { keyword: "pay scale", topic: "Pay Scales & Benefits" },
  { keyword: "training", topic: "Learning & Development" },
  { keyword: "discipline", topic: "Work Discipline & Ethics" },
  { keyword: "inspection", topic: "Regulatory Inspections" },
  { keyword: "service delivery", topic: "Service Delivery Standards" },
];

function extractRelatedTopics(text: string, maxTopics = 3): string[] {
  const lower = text.toLowerCase();
  const found: string[] = [];
  const seen = new Set<string>();
  for (const { keyword, topic } of TOPIC_KEYWORDS) {
    if (lower.includes(keyword) && !seen.has(topic)) {
      found.push(topic);
      seen.add(topic);
    }
    if (found.length >= maxTopics) break;
  }
  return found;
}

interface Props {
  message: Message;
  typingText?: string;
  isTyping?: boolean;
  onOpenPdf?: (ref: Reference) => void;
  onRetry?: () => void;
  onSendQuery?: (text: string) => void;
}

export const MessageBubble = memo(function MessageBubble({
  message,
  typingText,
  isTyping,
  onOpenPdf,
  onRetry,
  onSendQuery,
}: Props) {
  const [copied, setCopied] = useState(false);

  const copyText = useCallback(() => {
    navigator.clipboard.writeText(message.content);
    setCopied(true);
    showToast("Copied to clipboard", "success");
    setTimeout(() => setCopied(false), 2000);
  }, [message.content]);

  // Compute related topics once
  const relatedTopics = useMemo(
    () => (!isTyping && message.role === "assistant" ? extractRelatedTopics(message.content) : []),
    [message.content, message.role, isTyping],
  );

  if (message.role === "user") {
    return (
      <div className="flex justify-end gap-2.5">
        <div className="max-w-[82%] md:max-w-[72%] user-bubble">
          <div className="px-4 py-3 relative z-10">
            <p className="text-sm leading-relaxed text-white">{message.content}</p>
          </div>
          <div className="px-4 pb-2 text-[10px] text-right text-white/50">
            {timeAgo(message.timestamp)}
          </div>
        </div>
      </div>
    );
  }

  // Assistant message
  const displayText = isTyping && typingText !== undefined ? typingText : message.content;
  const showRefs = message.references && message.references.length > 0 && !isTyping;
  const showStructure = !isTyping && !message.failed;

  return (
    <div className="flex justify-start gap-2.5">
      <div className="flex-shrink-0 mt-1">
        <div
          className="w-8 h-8 rounded-xl overflow-hidden flex items-center justify-center"
          style={{ background: "var(--accent-soft)" }}
        >
          <Image src="/pera_logo.png" alt="" width={20} height={20} className="rounded-md" />
        </div>
      </div>
      <div className="max-w-[82%] md:max-w-[72%] bot-bubble">
        <div className="px-4 py-3">
          {/* Source Mode Badge */}
          {showStructure && message.sourceModeLabel && (
            <div className="source-mode-badge">
              {message.sourceMode === "documents" && "📄"}
              {message.sourceMode === "stored_api" && "🗃️"}
              {message.sourceMode === "both" && "🔗"}
              {message.sourceMode === "live_api" && "⚡"}
              {" "}Answer Mode: {message.sourceModeLabel}
            </div>
          )}

          {/* Answer Section */}
          {showStructure && (
            <div className="ans-section-label">Answer</div>
          )}
          <div className="msg-bot-text">
            {renderMarkdown(displayText)}
            {isTyping && <span className="typewriter-cursor" />}
          </div>

          {/* Provenance Note */}
          {showStructure && message.provenance && (
            <div className="provenance-note">
              {message.provenance}
            </div>
          )}

          {/* Authority / Source Block */}
          {showRefs && (
            <div className="ans-authority-block">
              <div className="ans-section-label">
                <span className="ans-verified-dot" />
                {message.sourceMode === "live_api"
                  ? "Source — Live API Response"
                  : message.sourceMode === "stored_api"
                    ? "Source — Indexed API Snapshots"
                    : message.sourceMode === "documents"
                      ? "Source — Official PERA Documents"
                      : "Source — Documents + API Data"}
              </div>
              <div className="flex flex-wrap gap-1.5 mt-1.5">
                {message.references!.slice(0, 8).map((ref, ri) => {
                  const isLiveRef = ref.source_type === "live_api";
                  const isApiRef = ref.source_type === "api";
                  const docName = ref.document?.replace(/\.pdf$/i, "") || "Document";
                  const truncated = docName.length > 22 ? docName.slice(0, 22) + "…" : docName;
                  const page = ref.page_start || ref.page;

                  if (isLiveRef) {
                    return (
                      <span key={ri} className="ref-chip ref-chip--live" title={`Live: ${ref.document}\nFetched: ${ref.timestamp || "now"}`}>
                        ⚡ {truncated}
                        {ref.timestamp ? ` · ${new Date(ref.timestamp).toLocaleTimeString()}` : ""}
                      </span>
                    );
                  }

                  if (isApiRef) {
                    return (
                      <span key={ri} className="ref-chip ref-chip--api" title={`API: ${ref.document}`}>
                        🗃️ {truncated}
                      </span>
                    );
                  }

                  return (
                    <button
                      key={ri}
                      onClick={() => onOpenPdf?.(ref)}
                      className="ref-chip"
                      title={`${ref.document}${page ? ` — Page ${page}` : ""}`}
                    >
                      [{ref.id ?? ri + 1}] {truncated}
                      {page ? ` · p.${page}` : ""}
                    </button>
                  );
                })}
              </div>
            </div>
          )}


          {/* Related Topics */}
          {showStructure && relatedTopics.length > 0 && (
            <div className="ans-related-block">
              <div className="ans-section-label">Related Topics</div>
              <div className="flex flex-wrap gap-1.5 mt-1">
                {relatedTopics.map((topic) => (
                  <button
                    key={topic}
                    className="ans-related-chip"
                    onClick={() => onSendQuery?.(`Tell me about ${topic} in PERA`)}
                  >
                    {topic}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Retry button for failed messages */}
          {message.failed && onRetry && (
            <button
              onClick={onRetry}
              className="mt-2 px-3 py-1.5 rounded-lg text-xs font-semibold transition-all"
              style={{ background: "var(--accent-soft)", color: "var(--accent)" }}
            >
              ↻ Retry
            </button>
          )}

          {/* Copy */}
          {!message.failed && (
            <button
              onClick={copyText}
              className="copy-btn absolute top-2 right-2 text-[10px] px-2 py-1 rounded-lg font-medium"
              style={{ background: "var(--bg-hover)", color: "var(--text-faint)" }}
              aria-label="Copy message"
            >
              {copied ? "Copied ✓" : "Copy"}
            </button>
          )}
        </div>
        <div className="px-4 pb-2 text-[10px]" style={{ color: "var(--text-faint)" }}>
          {timeAgo(message.timestamp)}
        </div>
      </div>
    </div>
  );
});
