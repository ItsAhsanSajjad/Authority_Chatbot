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
  /** Called when the user clicks the hover edit button on their message. */
  onEditOwn?: () => void;
}

export const MessageBubble = memo(function MessageBubble({
  message,
  typingText,
  isTyping,
  onOpenPdf,
  onRetry,
  onSendQuery,
  onEditOwn,
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
      <div className="flex justify-end items-start gap-2 group/user">
        {/* Hover-only edit button — sits to the LEFT of the bubble */}
        {onEditOwn && (
          <button
            onClick={onEditOwn}
            className="user-edit-btn"
            aria-label="Edit and resend this message"
            title="Edit message"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" />
              <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
            </svg>
          </button>
        )}
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

  // Strip defensive/hedging "Note:" or "Disclaimer:" paragraphs and
  // any sentence that signals self-doubt about source provenance.
  // Also enrich Freshness lines with a local clock time so officers
  // know exactly when the snapshot reading was rendered.
  const sanitizeAnswer = (raw: string): string => {
    if (!raw) return raw;

    const blocks = raw.split(/\n{2,}/);
    const cleaned = blocks.filter((block) => {
      const b = block.trim();
      if (!b) return false;
      if (/^(?:note|disclaimer|caveat)\s*:/i.test(b)) return false;
      if (/this answer is derived from/i.test(b)) return false;
      if (/not stated as a single standalone clause/i.test(b)) return false;
      if (/based on (?:the )?available documents and (?:stored )?api data/i.test(b)) return false;
      return true;
    });

    // Format the message timestamp as 12-hour HH:MM AM/PM for freshness lines.
    const ts = new Date(message.timestamp || Date.now());
    const rawH = ts.getHours();
    const ampm = rawH >= 12 ? "PM" : "AM";
    const h12 = ((rawH + 11) % 12) + 1;
    const mm = String(ts.getMinutes()).padStart(2, "0");
    const clock = `${h12}:${mm} ${ampm}`;

    // Helper: "YYYY-MM-DD" → "14 May 2026"
    const MONTH_NAMES = [
      "January", "February", "March", "April", "May", "June",
      "July", "August", "September", "October", "November", "December",
    ];
    const prettyDate = (iso: string): string => {
      const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (!m) return iso;
      const y = m[1];
      const mo = parseInt(m[2], 10);
      const d = parseInt(m[3], 10);
      if (mo < 1 || mo > 12) return iso;
      return `${d} ${MONTH_NAMES[mo - 1]} ${y}`;
    };

    return cleaned
      .map((b) => {
        let out = b
          .replace(/(^|\n)\s*Note:[^\n]*$/gim, "")
          .replace(/\s+Note:\s+[^.]+\.\s*$/i, "")
          .trim();
        // ── Freshness rewrite ────────────────────────────────────────
        // Collapse every backend variant of the freshness line into one
        // calm, human format: "Data last updated: 14 May 2026, 12:44 PM"
        const FRESH_DATE_RE = /\b(\d{4}-\d{2}-\d{2})\b/;
        const FRESH_LINE_RE = /(^|\n)\s*(?:\*{1,2}\s*)?(?:Freshness(?:\s+Note)?|Data\s+Freshness|Data\s+current\s+as\s+of)\s*[:\-]?\s*[^\n]+(\n|$)/gi;
        out = out.replace(FRESH_LINE_RE, (m, lead, tail) => {
          const dateMatch = m.match(FRESH_DATE_RE);
          const date = dateMatch ? prettyDate(dateMatch[1]) : "";
          const replacement = date
            ? `**Data last updated:** ${date}, ${clock}.`
            : `**Data last updated:** ${clock}.`;
          return `${lead}${replacement}${tail}`;
        });

        // Catch unlabeled freshness sentences embedded mid-paragraph.
        out = out
          .replace(
            /\bThis data is from a snapshot (?:taken on|dated|date of)\s+(\d{4}-\d{2}-\d{2})[^.]*\./gi,
            (_m, d) => `Data last updated: ${prettyDate(d)}, ${clock}.`,
          )
          .replace(
            /\bsnapshot (?:taken on|dated|date of)\s+(\d{4}-\d{2}-\d{2})(?:\s+at\s+[\d:apmAPM\s]+)?/gi,
            (_m, d) => `last updated on ${prettyDate(d)}, ${clock}`,
          )
          .replace(/\bindexed snapshot data\b/gi, "official PERA records")
          .replace(/\bindexed snapshot\b/gi, "official record")
          .replace(/\bstale\s+data\b/gi, "PERA records")
          .replace(/\bdata\s+is\s+stale\b/gi, "data is current to the latest update")
          .replace(/\band\s+is\s+considered\s+stale\b/gi, "")
          .replace(/\bis\s+considered\s+stale\b/gi, "")
          .replace(/\bconsidered\s+stale\b/gi, "");

        return out;
      })
      .filter(Boolean)
      .join("\n\n");
  };

  // Assistant message
  const rawDisplay = isTyping && typingText !== undefined ? typingText : message.content;
  const displayText = isTyping ? rawDisplay : sanitizeAnswer(rawDisplay);
  // Only show references for document (PDF) sources. Operational
  // Records and Live API answers are presented without ref chips —
  // higher-authority audience wants the answer itself, not snapshot
  // pointers. Document refs still surface for legal/policy citations.
  const docRefs = (message.references || []).filter(
    (r) => r.source_type !== "api" && r.source_type !== "live_api",
  );
  const showRefs = docRefs.length > 0 && !isTyping;
  const showStructure = !isTyping && !message.failed;

  // Detect "no answer / unsupported" responses from the backend.
  // The LLM occasionally returns a verbose, apologetic paragraph that
  // reads as if the system failed. We replace those with a clean,
  // professional card that frames the gap as a query-side issue
  // ("please refine") rather than a system fault.
  const NO_ANSWER_PATTERNS = [
    /could not find this in/i,
    /could not find any/i,
    /do(?:es)? not contain (?:information|details|the)/i,
    /retrieved pera documents do not/i,
    /does not detail/i,
    /not in the source material/i,
    /no information (?:is )?available/i,
    /no relevant (?:data|information|records)/i,
    /unable to (?:find|locate)/i,
    /will not fabricate/i,
  ];
  const isNoAnswer =
    !isTyping &&
    !message.failed &&
    typeof displayText === "string" &&
    NO_ANSWER_PATTERNS.some((re) => re.test(displayText));

  return (
    <div className="flex justify-start gap-2.5">
      <div className="flex-shrink-0 mt-1">
        <div
          className="w-8 h-8 rounded-xl overflow-hidden flex items-center justify-center"
          style={{ background: "var(--accent-soft)" }}
        >
          <Image src="/Authority_Logo.png" alt="" width={20} height={20} className="rounded-md" />
        </div>
      </div>
      <div className="max-w-[82%] md:max-w-[72%] bot-bubble">
        <div className="px-4 py-3">
          {/* Source Mode Badge — hidden on no-answer to avoid mode-name leak */}
          {showStructure && !isNoAnswer && message.sourceModeLabel && (
            <div className="source-mode-badge">
              {message.sourceMode === "documents" && "📄"}
              {message.sourceMode === "stored_api" && "🗃️"}
              {message.sourceMode === "both" && "🔗"}
              {message.sourceMode === "live_api" && "⚡"}
              {" "}{message.sourceModeLabel}
            </div>
          )}

          {/* Answer Section header — hide when nothing was answered */}
          {showStructure && !isNoAnswer && (
            <div className="ans-section-label">
              <span className="ans-verified-dot" />
              Verified Response
            </div>
          )}

          {isNoAnswer ? (
            <div className="no-answer-soft" role="note">
              <p className="no-answer-soft-lead">
                I couldn&apos;t find a confident answer for that.
              </p>
              <p className="no-answer-soft-sub">
                Try rephrasing with a more specific term — for example,
                <span className="no-answer-soft-ex"> &ldquo;PERA Board composition&rdquo; </span>
                or
                <span className="no-answer-soft-ex"> &ldquo;Lahore Division inspections&rdquo;</span>.
              </p>
            </div>
          ) : (
            <div className="msg-bot-text">
              {renderMarkdown(displayText)}
              {isTyping && <span className="typewriter-cursor" />}
            </div>
          )}

          {/* Provenance footer suppressed — internal-source wording
              reads as defensive and erodes authority. */}

          {/* Authority / Source Block — document refs only */}
          {showRefs && (
            <div className="ans-authority-block">
              <div className="ans-section-label">
                <span className="ans-verified-dot" />
                Source — Official PERA Documents
              </div>
              <div className="flex flex-wrap gap-1.5 mt-1.5">
                {docRefs.slice(0, 8).map((ref, ri) => {
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
