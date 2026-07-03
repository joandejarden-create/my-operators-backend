/**
 * Minimal markdown → runbook contentBlocks converter for internal admin pages.
 * Supports ## headings, paragraphs, bullet lists, fenced code, and **bold** inline.
 */

function escapeHtml(text) {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function inlineMarkdownToHtml(text) {
  return escapeHtml(text).replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
}

/**
 * @param {string} markdown
 * @returns {Array<{ type: string, [key: string]: unknown }>}
 */
export function markdownToRunbookBlocks(markdown) {
  const blocks = [];
  const lines = String(markdown || "").replace(/\r\n/g, "\n").split("\n");
  let paragraph = [];
  let inCode = false;
  let codeLines = [];
  let i = 0;

  function flushParagraph() {
    const text = paragraph.join(" ").trim();
    if (text) {
      blocks.push({ type: "paragraph", html: inlineMarkdownToHtml(text) });
    }
    paragraph = [];
  }

  function flushCode() {
    if (codeLines.length) {
      blocks.push({ type: "code", text: codeLines.join("\n") });
      codeLines = [];
    }
  }

  while (i < lines.length) {
    const line = lines[i];

    if (line.startsWith("```")) {
      if (inCode) {
        inCode = false;
        flushCode();
      } else {
        flushParagraph();
        inCode = true;
      }
      i += 1;
      continue;
    }
    if (inCode) {
      codeLines.push(line);
      i += 1;
      continue;
    }
    if (/^##\s+/.test(line)) {
      flushParagraph();
      blocks.push({ type: "heading", level: 3, text: line.replace(/^##\s+/, "").trim() });
      i += 1;
      continue;
    }
    if (/^#\s+/.test(line)) {
      flushParagraph();
      blocks.push({ type: "heading", level: 4, text: line.replace(/^#\s+/, "").trim() });
      i += 1;
      continue;
    }
    if (/^-\s+/.test(line)) {
      flushParagraph();
      const items = [];
      while (i < lines.length && /^-\s+/.test(lines[i])) {
        items.push(inlineMarkdownToHtml(lines[i].replace(/^-\s+/, "").trim()));
        i += 1;
      }
      blocks.push({ type: "unorderedList", items });
      continue;
    }
    if (!line.trim()) {
      flushParagraph();
      i += 1;
      continue;
    }
    paragraph.push(line.trim());
    i += 1;
  }

  flushParagraph();
  if (inCode) flushCode();
  return blocks;
}

/**
 * @param {string} markdown
 * @param {{ id: string, title: string, defaultOpen?: boolean }} sectionMeta
 */
export function markdownToRunbookSection(markdown, sectionMeta) {
  return {
    id: sectionMeta.id,
    title: sectionMeta.title,
    defaultOpen: !!sectionMeta.defaultOpen,
    contentBlocks: markdownToRunbookBlocks(markdown),
  };
}
