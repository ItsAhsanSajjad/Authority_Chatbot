/* ═══════════════════════════════════════════════════════════
   PERA AUTHORITY CHATBOT — Safe Markdown Renderer
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

interface TableBlock {
  type: "table";
  headers: string[];
  /** Per-row cell strings */
  rows: string[][];
  /** Per-column right-align flag (true if every data cell parses as a number) */
  align: ("left" | "right")[];
}

type BlockElement =
  | { type: "heading"; level: number; content: string }
  | { type: "paragraph"; content: string }
  | { type: "ul" | "ol"; items: string[] }
  | { type: "spacer" }
  | TableBlock;

/** Split a markdown table row "| a | b | c |" into cells.
 *  Tolerant of partial trailing content while a row is still streaming
 *  — "| a | b | c" (no trailing pipe yet) still produces 3 cells. */
function splitTableRow(line: string): string[] {
  let s = line.trim();
  if (s.startsWith("|")) s = s.slice(1);
  if (s.endsWith("|")) s = s.slice(0, -1);
  return s.split("|").map((c) => c.trim());
}

/** A separator row looks like "| --- | ---: | :---: |". */
function isTableSeparator(line: string): boolean {
  if (!/^\s*\|/.test(line)) return false;
  const cells = splitTableRow(line);
  if (cells.length === 0) return false;
  return cells.every((c) => /^:?-{2,}:?$/.test(c.replace(/\s+/g, "")));
}

/** Strict table row — must start AND end with `|`. Used for the
 *  initial header detection so we never enter table mode on a stray
 *  pipe-prefixed paragraph line. */
function isTableRowStrict(line: string): boolean {
  const t = line.trim();
  return /^\|.*\|\s*$/.test(t);
}

/** Loose table row — any line that starts with `|`. Used for the
 *  body-row continuation loop once the parser has already confirmed
 *  a header + separator + at least one strict body row. A partial
 *  cell line ("| Lahore | 26") still belongs to the table this way,
 *  so the bubble does not flicker between paragraph and table while
 *  cells are streaming in. */
function isTableRowLoose(line: string): boolean {
  const t = line.trim();
  return /^\|./.test(t);
}

/** Heuristic: cell is numeric-looking (digits, commas, %, currency, dashes). */
function looksNumeric(s: string): boolean {
  const t = s.trim();
  if (!t) return false;
  if (t === "—" || t === "-" || t === "–") return true;
  return /^[\d.,\s%+-]+$/.test(t) || /^(?:Rs\.?|₨|PKR)\s*[\d.,\s]+$/i.test(t);
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

    // Markdown table: STRICT header row + valid separator is enough
    // to commit to table mode. A body row is allowed but not
    // required at entry — this prevents the "paragraph → table"
    // shrink that happened when the bubble was rendering pipes as
    // text right up until the first complete body row arrived. The
    // separator alone is a strong enough signal that the LLM is
    // producing a markdown table.
    //
    // Misclassification risk is low: an isolated paragraph line that
    // starts and ends with `|` is rare in PERA content, and an
    // immediately-following valid markdown separator (`| --- |`)
    // is essentially never produced by accident.
    if (
      isTableRowStrict(rawLines[i] || "") &&
      isTableSeparator(rawLines[i + 1] || "")
    ) {
      flushList();
      const headers = splitTableRow(rawLines[i]);
      // Skip separator on i+1
      i += 1;
      const dataRows: string[][] = [];
      while (i + 1 < rawLines.length && isTableRowLoose(rawLines[i + 1] || "")) {
        i += 1;
        dataRows.push(splitTableRow(rawLines[i]));
      }
      const colCount = headers.length;
      // Pad/truncate every row to header width so the table is stable.
      const normRows = dataRows.map((r) => {
        if (r.length === colCount) return r;
        if (r.length > colCount) return r.slice(0, colCount);
        return [...r, ...Array(colCount - r.length).fill("")];
      });
      // Right-align a column when every non-empty cell looks numeric.
      const align: ("left" | "right")[] = headers.map((_, ci) => {
        const colCells = normRows
          .map((r) => r[ci])
          .filter((c) => c && c.trim() !== "");
        if (colCells.length === 0) return "left";
        return colCells.every(looksNumeric) ? "right" : "left";
      });
      blocks.push({
        type: "table",
        headers,
        rows: normRows,
        align,
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
          ...renderInline(block.content, key),
        );
      }

      case "paragraph":
        return createElement(
          "p",
          { key, className: "msg-para" },
          ...renderInline(block.content, key),
        );

      case "ul":
        return createElement(
          "ul",
          { key, className: "msg-list msg-list-ul" },
          block.items.map((item, j) =>
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
          block.items.map((item, j) =>
            createElement(
              "li",
              { key: `${key}-li-${j}` },
              ...renderInline(item, `${key}-li-${j}`),
            ),
          ),
        );

      case "table": {
        const tableEl = createElement(
          "table",
          { className: "msg-table" },
          createElement(
            "thead",
            { key: `${key}-thead` },
            createElement(
              "tr",
              { key: `${key}-thr` },
              block.headers.map((h, ci) =>
                createElement(
                  "th",
                  {
                    key: `${key}-th-${ci}`,
                    className: block.align[ci] === "right" ? "is-num" : undefined,
                  },
                  ...renderInline(h, `${key}-th-${ci}`),
                ),
              ),
            ),
          ),
          createElement(
            "tbody",
            { key: `${key}-tbody` },
            block.rows.map((row, ri) =>
              createElement(
                "tr",
                { key: `${key}-tr-${ri}` },
                row.map((cell, ci) => {
                  const cellKey = `${key}-td-${ri}-${ci}`;
                  const cls = block.align[ci] === "right" ? "is-num" : undefined;
                  if (cell.trim() === "") {
                    return createElement("td", { key: cellKey, className: cls }, "—");
                  }
                  return createElement(
                    "td",
                    { key: cellKey, className: cls },
                    ...renderInline(cell, cellKey),
                  );
                }),
              ),
            ),
          ),
        );
        return createElement(
          "div",
          { key, className: "msg-table-wrap" },
          tableEl,
        );
      }

      case "spacer":
        return createElement("div", { key, className: "h-2" });

      default:
        return null;
    }
  });
}
