/**
 * Export Brand AI Visibility Help Center "How to Read" guide to PDF.
 * Source: docs/ai-visibility/brand-ai-visibility-how-to-read.md
 *
 * Usage: node scripts/export-brand-ai-visibility-how-to-read-pdf.mjs
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";

const CHROME =
  process.env.CHROME_PATH ||
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

const mdPath = path.resolve("docs/ai-visibility/brand-ai-visibility-how-to-read.md");
const outDir = path.resolve("data/ai-visibility/exports");
fs.mkdirSync(outDir, { recursive: true });

const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const htmlPath = path.join(outDir, `brand-ai-visibility-how-to-read-${stamp}.html`);
const pdfPath = path.join(outDir, `brand-ai-visibility-how-to-read-${stamp}.pdf`);
const stablePdf = path.join(outDir, "brand-ai-visibility-how-to-read.pdf");
const stableHtml = path.join(outDir, "brand-ai-visibility-how-to-read.html");

const md = fs.readFileSync(mdPath, "utf8");

function esc(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function inline(text) {
  let t = esc(text);
  t = t.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
  t = t.replace(/`([^`]+)`/g, "<code>$1</code>");
  return t;
}

function renderTable(rows) {
  if (!rows.length) return "";
  const header = rows[0];
  const body = rows.slice(1);
  return `<table><thead><tr>${header
    .map((c) => `<th>${inline(c)}</th>`)
    .join("")}</tr></thead><tbody>${body
    .map((r) => `<tr>${r.map((c) => `<td>${inline(c)}</td>`).join("")}</tr>`)
    .join("")}</tbody></table>`;
}

function mdToHtml(source) {
  const lines = source.replace(/\r\n/g, "\n").split("\n");
  const out = [];
  let i = 0;
  let inList = null; // 'ul' | 'ol'
  let para = [];

  function flushPara() {
    if (!para.length) return;
    out.push(`<p>${inline(para.join(" "))}</p>`);
    para = [];
  }
  function closeList() {
    if (!inList) return;
    out.push(`</${inList}>`);
    inList = null;
  }

  while (i < lines.length) {
    const line = lines[i];

    if (line.trim() === "---") {
      flushPara();
      closeList();
      out.push("<hr/>");
      i += 1;
      continue;
    }

    // table
    if (line.trim().startsWith("|") && line.includes("|")) {
      flushPara();
      closeList();
      const rows = [];
      while (i < lines.length && lines[i].trim().startsWith("|")) {
        const raw = lines[i].trim();
        if (!/^\|?\s*-+/.test(raw.replace(/\|/g, "|"))) {
          const cells = raw
            .replace(/^\|/, "")
            .replace(/\|$/, "")
            .split("|")
            .map((c) => c.trim());
          // skip separator row
          if (!cells.every((c) => /^:?-+:?$/.test(c))) rows.push(cells);
        }
        i += 1;
      }
      out.push(renderTable(rows));
      continue;
    }

    const h = line.match(/^(#{1,3})\s+(.*)$/);
    if (h) {
      flushPara();
      closeList();
      const level = h[1].length;
      out.push(`<h${level}>${inline(h[2])}</h${level}>`);
      i += 1;
      continue;
    }

    const ul = line.match(/^\s*[-*]\s+(.*)$/);
    if (ul) {
      flushPara();
      if (inList !== "ul") {
        closeList();
        out.push("<ul>");
        inList = "ul";
      }
      out.push(`<li>${inline(ul[1])}</li>`);
      i += 1;
      continue;
    }

    const ol = line.match(/^\s*\d+\.\s+(.*)$/);
    if (ol) {
      flushPara();
      if (inList !== "ol") {
        closeList();
        out.push("<ol>");
        inList = "ol";
      }
      out.push(`<li>${inline(ol[1])}</li>`);
      i += 1;
      continue;
    }

    if (!line.trim()) {
      flushPara();
      closeList();
      i += 1;
      continue;
    }

    closeList();
    para.push(line.trim());
    i += 1;
  }
  flushPara();
  closeList();
  return out.join("\n");
}

const body = mdToHtml(md);
const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Brand AI Visibility — How to Read</title>
  <style>
    @page { size: A4; margin: 16mm 14mm; }
    body {
      font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
      color: #1a1f2c;
      font-size: 11.5px;
      line-height: 1.5;
      margin: 0;
    }
    h1 {
      font-size: 24px;
      margin: 0 0 8px;
      letter-spacing: -0.02em;
    }
    h2 {
      font-size: 15px;
      margin: 22px 0 8px;
      padding-bottom: 4px;
      border-bottom: 2px solid #1a1f2c;
      page-break-after: avoid;
    }
    h3 {
      font-size: 12.5px;
      margin: 16px 0 6px;
      color: #243047;
      page-break-after: avoid;
    }
    p { margin: 0 0 8px; }
    ul, ol { margin: 4px 0 10px 18px; padding: 0; }
    li { margin: 3px 0; }
    hr {
      border: none;
      border-top: 1px solid #d7dbe3;
      margin: 16px 0;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin: 8px 0 14px;
      font-size: 10.5px;
      page-break-inside: avoid;
    }
    th, td {
      border-bottom: 1px solid #e5e8ef;
      padding: 6px 7px;
      text-align: left;
      vertical-align: top;
    }
    th {
      background: #f4f6f9;
      font-size: 9.5px;
      text-transform: uppercase;
      letter-spacing: .03em;
      color: #445;
    }
    tr:nth-child(even) td { background: #fafbfc; }
    code {
      font-family: ui-monospace, Consolas, monospace;
      font-size: 10.5px;
      background: #f1f4f8;
      padding: 1px 4px;
      border-radius: 4px;
    }
    strong { font-weight: 650; }
    .cover {
      background: #f4f6f9;
      border: 1px solid #e2e6ee;
      border-radius: 10px;
      padding: 12px 14px;
      margin: 0 0 16px;
    }
    .cover .kicker {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: .06em;
      color: #667;
      margin-bottom: 4px;
    }
    .footer {
      margin-top: 24px;
      padding-top: 10px;
      border-top: 1px solid #dde3ec;
      font-size: 9.5px;
      color: #778;
    }
  </style>
</head>
<body>
  <div class="cover">
    <div class="kicker">Dealality Help Center</div>
    <div><strong>Brand AI Visibility</strong> · How to Read Guide</div>
    <div>For brand development, brand strategy, and commercial leadership</div>
  </div>
  ${body}
  <div class="footer">
    Source: docs/ai-visibility/brand-ai-visibility-how-to-read.md · Exported ${new Date().toISOString()}
  </div>
</body>
</html>`;

fs.writeFileSync(htmlPath, html, "utf8");
fs.writeFileSync(stableHtml, html, "utf8");

if (!fs.existsSync(CHROME)) {
  console.error(JSON.stringify({ ok: false, error: "CHROME_NOT_FOUND", htmlPath }, null, 2));
  process.exit(1);
}

const userData = path.join(outDir, "_chrome-print-profile-howto");
fs.mkdirSync(userData, { recursive: true });
const fileUrl = "file:///" + htmlPath.replace(/\\/g, "/");
const args = [
  "--headless=new",
  "--disable-gpu",
  "--no-pdf-header-footer",
  `--user-data-dir=${userData}`,
  `--print-to-pdf=${pdfPath}`,
  "--print-to-pdf-no-header",
  fileUrl,
];
const r = spawnSync(CHROME, args, { encoding: "utf8", timeout: 90000 });
const ok = fs.existsSync(pdfPath) && fs.statSync(pdfPath).size > 1000;
if (ok) fs.copyFileSync(pdfPath, stablePdf);

console.log(
  JSON.stringify(
    {
      ok,
      mdPath,
      htmlPath,
      pdfPath,
      stablePdf: ok ? stablePdf : null,
      pdfBytes: ok ? fs.statSync(pdfPath).size : 0,
      status: r.status,
      stderr: (r.stderr || "").slice(-500),
    },
    null,
    2
  )
);
if (!ok) process.exit(1);
