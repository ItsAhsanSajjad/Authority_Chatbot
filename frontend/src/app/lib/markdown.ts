/* ═══════════════════════════════════════════════════════════
   PERA AI — Safe Markdown Renderer
   Converts a minimal markdown subset to React elements.
   NO dangerouslySetInnerHTML — fully structured rendering.
   ═══════════════════════════════════════════════════════════ */

import { createElement, type ReactNode } from "react";

type InlineSegment =
  | { type: "text"; value: string }
  | { type: "bold"; value: string }
  | { type: "italic"; value: string }
  | { type: "cite"; value: string };

/** Parse inline formatting: **bold**, *italic*, [N] citations */
function parseInline(text: string): InlineSegment[] {
  const segments: InlineSegment[] = [];
  // Regex alternation: bold first, then italic, then citations
  const re = /\*\*(.+?)\*\*|\*(.+?)\*|\[(\d+)\]/g;
  let lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = re.exec(text)) !== null) {
    if (match.index > lastIndex) {
      segments.push({ type: "text", value: text.slice(lastIndex, match.index) });
    }
    if (match[1] !== undefined) {
      segments.push({ type: "bold", value: match[1] });
    } else if (match[2] !== undefined) {
      segments.push({ type: "italic", value: match[2] });
    } else if (match[3] !== undefined) {
      segments.push({ type: "cite", value: match[3] });
    }
    lastIndex = match.index + match[0].length;
  }

  if (lastIndex < text.length) {
    segments.push({ type: "text", value: text.slice(lastIndex) });
  }

  return segments;
}

/** Render inline segments to React nodes */
function renderInline(text: string, keyPrefix: string): ReactNode[] {
  return parseInline(text).map((seg, i) => {
    const key = `${keyPrefix}-${i}`;
    switch (seg.type) {
      case "bold":
        return createElement("strong", { key, className: "msg-inline-bold" }, seg.value);
      case "italic":
        return createElement("em", { key, className: "msg-inline-italic" }, seg.value);
      case "cite":
        return createElement(
          "span",
          { key, className: "cite-badge" },
          `[${seg.value}]`,
        );
      default:
        return createElement("span", { key }, seg.value);
    }
  });
}

interface BlockElement {
  type: "heading" | "paragraph" | "ul" | "ol" | "spacer";
  level?: number;        // heading level (2-4)
  content?: string;      // for heading/paragraph
  items?: string[];      // for lists
}

/** Parse markdown text into block-level elements */
function parseBlocks(text: string): BlockElement[] {
  const rawLines = text.split("\n");
  const blocks: BlockElement[] = [];
  let listBuffer: { type: "ul" | "ol"; items: string[] } | null = null;

  const flushList = () => {
    if (!listBuffer) return;
    blocks.push({ type: listBuffer.type, items: listBuffer.items });
    listBuffer = null;
  };

  for (let i = 0; i < rawLines.length; i++) {
    const trimmed = rawLines[i].trim();

    if (!trimmed) {
      flushList();
      if (i > 0 && rawLines[i - 1]?.trim()) {
        blocks.push({ type: "spacer" });
      }
      continue;
    }

    // Headings: ####, ###, ##
    const headingMatch = trimmed.match(/^(#{2,4})\s+(.+)$/);
    if (headingMatch) {
      flushList();
      blocks.push({
        type: "heading",
        level: headingMatch[1].length,
        content: headingMatch[2],
      });
      continue;
    }

    // Bullet list
    if (/^[-•]\s/.test(trimmed)) {
      const content = trimmed.replace(/^[-•]\s*/, "");
      if (listBuffer?.type === "ul") {
        listBuffer.items.push(content);
      } else {
        flushList();
        listBuffer = { type: "ul", items: [content] };
      }
      continue;
    }

    // Numbered list
    if (/^\d+[.)]\s/.test(trimmed)) {
      const content = trimmed.replace(/^\d+[.)]\s*/, "");
      if (listBuffer?.type === "ol") {
        listBuffer.items.push(content);
      } else {
        flushList();
        listBuffer = { type: "ol", items: [content] };
      }
      continue;
    }

    // Normal paragraph
    flushList();
    blocks.push({ type: "paragraph", content: trimmed });
  }

  flushList();
  return blocks;
}

/** Render a full markdown string to an array of React elements — fully safe, no raw HTML */
export function renderMarkdown(text: string): ReactNode[] {
  const blocks = parseBlocks(text);

  return blocks.map((block, i) => {
    const key = `b-${i}`;

    switch (block.type) {
      case "heading": {
        const tag = block.level === 2 ? "h3" : block.level === 3 ? "h4" : "h5";
        return createElement(
          tag,
          { key, className: "msg-heading" },
          ...renderInline(block.content!, key),
        );
      }

      case "paragraph":
        return createElement(
          "p",
          { key, className: "msg-para" },
          ...renderInline(block.content!, key),
        );

      case "ul":
        return createElement(
          "ul",
          { key, className: "msg-list msg-list-ul" },
          block.items!.map((item, j) =>
            createElement(
              "li",
              { key: `${key}-li-${j}` },
              ...renderInline(item, `${key}-li-${j}`),
            ),
          ),
        );

      case "ol":
        return createElement(
          "ol",
          { key, className: "msg-list msg-list-ol" },
          block.items!.map((item, j) =>
            createElement(
              "li",
              { key: `${key}-li-${j}` },
              ...renderInline(item, `${key}-li-${j}`),
            ),
          ),
        );

      case "spacer":
        return createElement("div", { key, className: "h-2" });

      default:
        return null;
    }
  });
}
