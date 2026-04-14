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

/**
 * Generate a pgAdmin-ready SQL query from the api_source_id pattern.
 * Falls back to a helpful table reference if pattern isn't recognized.
 */
function generateVerifyQuery(apiSourceId: string, displayName: string): string {
  if (!apiSourceId) return "";

  // Challan location patterns: challan_location:tehsil:Lahore Saddar
  const locMatch = apiSourceId.match(/^challan_location:(\w+):(.+)$/);
  if (locMatch) {
    const [, locType, locName] = locMatch;
    const tableMap: Record<string, string> = {
      tehsil: "challan_tehsil_drill",
      district: "challan_by_district",
      division: "challan_by_division",
    };
    const tbl = tableMap[locType] || "challan_by_division";
    return `SELECT * FROM ${tbl}\nWHERE TRIM(${locType}_name) = '${locName}'\nORDER BY status;`;
  }

  // Challan location type patterns
  const locTypeMatch = apiSourceId.match(/^challan_location_type:(\w+):(\w+):(.+)$/);
  if (locTypeMatch) {
    const [, reqType, locType, locName] = locTypeMatch;
    return `SELECT * FROM challan_tehsil_breakdown\nWHERE TRIM(${locType}_name) = '${locName}'\nORDER BY status;`;
  }

  // Simple source IDs
  const simpleMap: Record<string, string> = {
    challan_totals: "SELECT * FROM challan_totals\nORDER BY status;",
    challan_by_division: "SELECT * FROM challan_by_division\nORDER BY division_name, status;",
    challan_by_district: "SELECT * FROM challan_by_district\nORDER BY district_name, status;",
    challan_by_tehsil: "SELECT * FROM challan_by_tehsil\nORDER BY tehsil_name, status;",
    challan_by_officer: "SELECT * FROM challan_by_officer\nORDER BY total_challans DESC\nLIMIT 30;",
    challan_comparison: "SELECT * FROM challan_by_division\nORDER BY total_challans DESC;",
    insp_summary: "SELECT * FROM inspection_officer_summary\nLIMIT 50;",
  };

  if (simpleMap[apiSourceId]) return simpleMap[apiSourceId];

  // Inspection patterns
  if (apiSourceId.startsWith("insp_")) {
    const inspLocMatch = apiSourceId.match(/^insp_(\w+):(.+)$/);
    if (inspLocMatch) {
      const [, locType, locName] = inspLocMatch;
      return `SELECT * FROM inspection_officer_summary\nWHERE TRIM(${locType}_name) = '${locName}';`;
    }
  }

  // Daterange pattern
  if (apiSourceId === "challan_daterange") {
    return "SELECT * FROM challan_data\nWHERE challan_date BETWEEN '<start>' AND '<end>'\nORDER BY challan_date DESC;";
  }

  // Fallback: extract table hint from display name
  if (displayName.toLowerCase().includes("challan")) {
    return "-- Check challan tables:\nSELECT * FROM challan_totals;\nSELECT * FROM challan_by_division;";
  }

  return "";
}

function getSourceLabel(mode?: string): { label: string; icon: React.ReactNode; color: string } {
  switch (mode) {
    case "stored_api":
      return {
        label: "Verified from API Data",
        color: "var(--blue, #3b82f6)",
        icon: (
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <ellipse cx="12" cy="5" rx="9" ry="3" />
            <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
            <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
          </svg>
        ),
      };
    case "documents":
      return {
        label: "Verified from Official Documents",
        color: "var(--gold)",
        icon: (
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
            <polyline points="14 2 14 8 20 8" />
            <line x1="16" y1="13" x2="8" y2="13" />
            <line x1="16" y1="17" x2="8" y2="17" />
          </svg>
        ),
      };
    default:
      return {
        label: "Verified from Documents + API Data",
        color: "var(--gold)",
        icon: (
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <path d="M9 12l2 2 4-4" />
            <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
          </svg>
        ),
      };
  }
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
  const [feedback, setFeedback] = useState<"up" | "down" | null>(null);
  const [refsExpanded, setRefsExpanded] = useState(false);

  const copyText = useCallback(() => {
    navigator.clipboard.writeText(message.content);
    setCopied(true);
    showToast("Copied to clipboard", "success");
    setTimeout(() => setCopied(false), 2000);
  }, [message.content]);

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
          <div className="px-4 pb-2 text-[10px] text-right text-white/40">
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
  const sourceInfo = getSourceLabel(message.sourceMode);
  const refCount = message.references?.length || 0;

  return (
    <div className="flex justify-start gap-2.5">
      {/* Bot Avatar */}
      <div className="flex-shrink-0 mt-1">
        <div className="bot-avatar">
          <Image src="/pera_logo.png" alt="" width={20} height={20} className="rounded-md" />
        </div>
      </div>

      <div className="max-w-[82%] md:max-w-[72%] bot-bubble group">
        <div className="px-4 py-3">
          {/* Answer text */}
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

          {/* ============ VERIFICATION PANEL ============ */}
          {showRefs && (
            <div className="verify-panel">
              {/* Verification Header — clickable to expand */}
              <button
                className="verify-header"
                onClick={() => setRefsExpanded(!refsExpanded)}
                aria-expanded={refsExpanded}
              >
                <div className="verify-header-left">
                  <span className="verify-shield" style={{ color: sourceInfo.color }}>
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" stroke="none">
                      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" opacity="0.15"/>
                      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" fill="none" stroke="currentColor" strokeWidth="1.5"/>
                      <polyline points="9 12 11 14 15 10" fill="none" stroke="currentColor" strokeWidth="2"/>
                    </svg>
                  </span>
                  <span className="verify-label">{sourceInfo.label}</span>
                  <span className="verify-count">{refCount} source{refCount !== 1 ? "s" : ""}</span>
                </div>
                <span className={`verify-chevron ${refsExpanded ? "verify-chevron--open" : ""}`}>
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                    <polyline points="6 9 12 15 18 9" />
                  </svg>
                </span>
              </button>

              {/* Expanded references list */}
              <div className={`verify-body ${refsExpanded ? "verify-body--open" : ""}`}>
                {message.references!.slice(0, 8).map((ref, ri) => {
                  const isApiRef = ref.source_type === "api";
                  const docName = ref.document?.replace(/\.pdf$/i, "") || "Document";
                  const page = ref.page_start || ref.page;
                  const snippet = ref.snippet;
                  const displayName = ref.api_display_name || docName;
                  const dataTable = ref.data_table || "";
                  const dataQuery = ref.data_query || "";
                  const apiSourceId = ref.api_source_id || "";

                  // Generate a verification query hint for API refs
                  const verifyQuery = dataQuery || generateVerifyQuery(apiSourceId, displayName);

                  return (
                    <div key={ri} className="verify-ref-item">
                      <button
                        className="verify-ref-card"
                        onClick={() => !isApiRef && onOpenPdf?.(ref)}
                        title={isApiRef ? `API: ${displayName}` : `${ref.document}${page ? ` — Page ${page}` : ""}`}
                      >
                        <div className="verify-ref-icon" style={{ color: isApiRef ? "var(--blue, #3b82f6)" : "var(--gold)" }}>
                          {isApiRef ? (
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
                              <ellipse cx="12" cy="5" rx="9" ry="3" />
                              <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                              <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
                            </svg>
                          ) : (
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
                              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                              <polyline points="14 2 14 8 20 8" />
                              <line x1="16" y1="13" x2="8" y2="13" />
                              <line x1="16" y1="17" x2="8" y2="17" />
                            </svg>
                          )}
                        </div>
                        <div className="verify-ref-content">
                          <div className="verify-ref-title">
                            {isApiRef ? displayName : docName}
                            {page && <span className="verify-ref-page">Page {page}</span>}
                            {dataTable && <span className="verify-ref-table">{dataTable}</span>}
                          </div>
                          {snippet && (
                            <div className="verify-ref-snippet">
                              &ldquo;{snippet.length > 120 ? snippet.slice(0, 120) + "..." : snippet}&rdquo;
                            </div>
                          )}
                        </div>
                        {!isApiRef && (
                          <span className="verify-ref-arrow">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                              <path d="M7 17l9.2-9.2M17 17V7H7" />
                            </svg>
                          </span>
                        )}
                      </button>

                      {/* SQL Query block for API refs */}
                      {isApiRef && verifyQuery && (
                        <div className="verify-query-block">
                          <div className="verify-query-header">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                              <polyline points="16 18 22 12 16 6" />
                              <polyline points="8 6 2 12 8 18" />
                            </svg>
                            <span>Verify in pgAdmin</span>
                            <button
                              className="verify-query-copy"
                              onClick={(e) => {
                                e.stopPropagation();
                                navigator.clipboard.writeText(verifyQuery);
                                showToast("Query copied to clipboard", "success");
                              }}
                              title="Copy query"
                            >
                              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                              </svg>
                              Copy
                            </button>
                          </div>
                          <code className="verify-query-code">{verifyQuery}</code>
                        </div>
                      )}
                    </div>
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
            <button onClick={onRetry} className="retry-btn">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
              Retry
            </button>
          )}

        </div>

        {/* Footer: actions + time */}
        <div className="msg-footer">
          {showStructure && (
            <div className="msg-actions-group">
              {/* Copy */}
              {!message.failed && (
                <button onClick={copyText} className="msg-action-btn" aria-label="Copy message" title="Copy response">
                  {copied ? (
                    <>
                      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="20 6 9 17 4 12"/></svg>
                      <span>Copied</span>
                    </>
                  ) : (
                    <>
                      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
                      <span>Copy</span>
                    </>
                  )}
                </button>
              )}
              {/* Thumbs up */}
              <button
                className={`msg-action-btn ${feedback === "up" ? "msg-action-btn--active" : ""}`}
                onClick={() => setFeedback(feedback === "up" ? null : "up")}
                aria-label="Thumbs up"
                title="Good response"
              >
                <svg width="13" height="13" viewBox="0 0 24 24" fill={feedback === "up" ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M14 9V5a3 3 0 0 0-3-3l-4 9v11h11.28a2 2 0 0 0 2-1.7l1.38-9a2 2 0 0 0-2-2.3zM7 22H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h3" />
                </svg>
              </button>
              {/* Thumbs down */}
              <button
                className={`msg-action-btn ${feedback === "down" ? "msg-action-btn--active" : ""}`}
                onClick={() => setFeedback(feedback === "down" ? null : "down")}
                aria-label="Thumbs down"
                title="Bad response"
              >
                <svg width="13" height="13" viewBox="0 0 24 24" fill={feedback === "down" ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M10 15v4a3 3 0 0 0 3 3l4-9V2H5.72a2 2 0 0 0-2 1.7l-1.38 9a2 2 0 0 0 2 2.3zm7-13h2.67A2.31 2.31 0 0 1 22 4v7a2.31 2.31 0 0 1-2.33 2H17" />
                </svg>
              </button>
            </div>
          )}
          <span className="msg-time">{timeAgo(message.timestamp)}</span>
        </div>
      </div>
    </div>
  );
});
