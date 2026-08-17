#!/usr/bin/env node
/**
 * Generate a local HTML steward page for Choice amenity pilot saves.
 *
 *   node scripts/generate-choice-amenity-pilot-opener.mjs
 */
import "../load-env.js";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const CSV = join("reports", "choice-amenities-pilot-worklist.csv");
const OUT = join("reports", "choice-amenity-pilot-opener.html");

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else inQ = !inQ;
    } else if (c === "," && !inQ) {
      out.push(cur);
      cur = "";
    } else cur += c;
  }
  out.push(cur);
  return out;
}

const raw = readFileSync(CSV, "utf8").trim().split(/\r?\n/);
const header = parseCsvLine(raw[0]);
const idx = Object.fromEntries(header.map((h, i) => [h, i]));
const rows = raw.slice(1).filter(Boolean).map((line) => {
  const p = parseCsvLine(line);
  return {
    name: p[idx.censusName],
    pid: p[idx.propertyId],
    country: p[idx.censusCountry] || "",
    url: p[idx.propertyUrl],
    htmlFile: p[idx.htmlFile],
  };
});

const items = rows
  .map(
    (r) => `
    <tr>
      <td><code>${escapeHtml(r.pid)}</code></td>
      <td>${escapeHtml(r.name)}</td>
      <td>${escapeHtml(r.country)}</td>
      <td><a href="${escapeAttr(r.url)}" target="_blank" rel="noopener">Open property page</a></td>
      <td><code>${escapeHtml(r.htmlFile)}</code></td>
    </tr>`
  )
  .join("");

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
function escapeAttr(s) {
  return escapeHtml(s).replace(/'/g, "&#39;");
}

const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Choice amenity pilot — steward saves</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem; max-width: 1100px; color: #1a1a1a; }
    h1 { font-size: 1.25rem; }
    .note { background: #fff8e6; border: 1px solid #e6d9a8; padding: 0.75rem 1rem; margin: 1rem 0; }
    ol { line-height: 1.65; }
    table { border-collapse: collapse; width: 100%; margin-top: 1.5rem; font-size: 0.9rem; }
    th, td { border: 1px solid #ccc; padding: 0.5rem 0.75rem; text-align: left; vertical-align: top; }
    th { background: #f4f4f4; }
    code { font-size: 0.85em; }
    pre { background: #f6f6f6; padding: 0.75rem 1rem; overflow-x: auto; }
  </style>
</head>
<body>
  <h1>Choice amenities pilot (${rows.length} properties)</h1>
  <div class="note">
    <strong>Automated fetch is blocked</strong> (403 / Akamai). Do not invent amenity lists.
    Only HTML saved from a real browser property page can be applied.
  </div>
  <p>Save each page to <code>reports/choice-amenity-html/{propertyId}.html</code> (lowercase id, e.g. <code>mx077.html</code>), then:</p>
  <pre>node scripts/apply-choice-amenities-from-html.mjs
node scripts/apply-choice-amenities-from-html.mjs --apply</pre>
  <ol>
    <li>Click <strong>Open property page</strong> (Chrome/Edge — not Puppeteer/headless).</li>
    <li>Wait until the page fully loads and an <strong>amenities</strong> section is visible.</li>
    <li><kbd>Ctrl+S</kbd> → <strong>Webpage, Complete</strong>.</li>
    <li>Save exactly to the path in the <strong>Save as</strong> column (under <code>reports/choice-amenity-html/</code>).</li>
    <li>Repeat for each row. Dry-run apply, then <code>--apply</code> (fill-blank Amenities only).</li>
  </ol>
  <table>
    <thead><tr><th>ID</th><th>Name</th><th>Country</th><th>Link</th><th>Save as</th></tr></thead>
    <tbody>${items}</tbody>
  </table>
  <p style="margin-top:1.5rem;font-size:0.9rem;color:#555;">
    Runbook: <code>reports/choice-amenities-steward-runbook.md</code>
  </p>
</body>
</html>`;

mkdirSync("reports", { recursive: true });
writeFileSync(OUT, html, "utf8");
console.log(`Wrote ${OUT} (${rows.length} rows)`);
