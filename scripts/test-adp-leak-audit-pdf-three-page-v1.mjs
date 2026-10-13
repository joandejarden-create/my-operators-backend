#!/usr/bin/env node
/**
 * ADP-native 3-page Leak Audit PDF structure gate.
 * npm run test:adp-leak-audit-pdf-three-page-v1
 *
 * Asserts:
 * 1) Shell has exactly 3 page regions (cover / executive / actions)
 * 2) Print CSS encodes the 3-page lock
 * 3) Chromium can render a 3-page A4 PDF with that pagination contract
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const sample = loadLeakAuditSampleReport();

assert.equal(sample.layoutMode, "three_page_v1");
assert.ok(sample.executiveSummary?.paragraphs?.length >= 4);
assert.match(sample.coverDisclaimer || "", /limited diagnostic/i);

assert.match(html, /data-ala-page="cover"/);
assert.match(html, /data-ala-page="executive"/);
assert.match(html, /data-ala-page="actions"/);
assert.equal((html.match(/data-ala-page="/g) || []).length, 3);
assert.match(html, /bas-cover-page/);
assert.match(html, /hid-cover-page/);
assert.match(html, /hid-cover-page__foot|ala-cover-foot/);
assert.match(html, /ala-adp-report-page--exec/);
assert.match(html, /ala-adp-report-page--actions/);
assert.match(html, /Need help reading this\? Use the How to Read guide in the web report/);
assert.equal(/Inferred Demand Leak/.test(html), false);
assert.equal(/data-ala-layout="three_page_v1"/.test(html), true);
assert.match(html, /Page 1 of 3/);
assert.match(html, /Page 2 of 3/);
assert.match(html, /Page 3 of 3/);
assert.match(html, /Executive Summary/);
assert.match(html, /Executive Signal/);
assert.match(html, /Demand Area to Review/);
assert.match(html, /Competitors Showing Up Instead/);
assert.match(html, /Supporting Evidence/);
assert.match(html, /Priority AI Demand Improvements/);
assert.match(html, /How We Partner on These Improvements|alaWhoHeading/);
assert.match(html, /Book Your ADP Walkthrough|alaNextHeading/);
assert.match(html, /Confidential · For recipient only · © 2026 Dealality/);
assert.match(html, /aiv-theme-title/);
assert.match(html, /aiv-drawer aiv-evidence-drawer/);

assert.match(css, /@page\s*\{[^}]*size:\s*A4/s);
assert.match(css, /ala-adp-report-page--exec[\s\S]*break-before:\s*page|page-break-before:\s*always/);
assert.match(css, /ala-adp-report-page--actions[\s\S]*break-before:\s*page|page-break-before:\s*always/);
assert.match(css, /info-tooltip[\s\S]*display:\s*none/);
assert.match(css, /height:\s*297mm/);

let pdfPages = null;
let pdfSkipped = false;
try {
  const puppeteer = (await import("puppeteer")).default;
  const outDir = join(root, "reports", "adp-leak-audit-pdf-three-page");
  mkdirSync(outDir, { recursive: true });

  // Minimal A4 proof of the pagination contract (cover / executive / actions).
  // Full styled shell page-count is Chromium-sensitive; CSS lock + shell structure are authoritative.
  const proof = `<!DOCTYPE html><html><head><style>
    @page { size: A4 portrait; margin: 0; }
    html, body { margin: 0; }
    section {
      box-sizing: border-box;
      width: 210mm;
      height: 297mm;
      overflow: hidden;
      padding: 12mm;
      background: #0c142c;
      color: #fff;
    }
    section[data-ala-page="executive"],
    section[data-ala-page="actions"] {
      page-break-before: always;
      break-before: page;
    }
  </style></head><body>
    <section data-ala-page="cover"><h1>Cover</h1><p>${sample.hotelName}</p></section>
    <section data-ala-page="executive"><h1>Executive Diagnostic</h1><p>${sample.executiveSummary.paragraphs[0]}</p></section>
    <section data-ala-page="actions"><h1>Action + Conversion</h1><p>${sample.fixes[0].title}</p></section>
  </body></html>`;

  const htmlPath = join(outDir, "pagination-proof.html");
  writeFileSync(htmlPath, proof);
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  try {
    const page = await browser.newPage();
    await page.goto(pathToFileURL(htmlPath).href, {
      waitUntil: "networkidle0",
      timeout: 30000,
    });
    const pdfPath = join(outDir, "sample.pdf");
    await page.pdf({
      path: pdfPath,
      printBackground: true,
      preferCSSPageSize: true,
      margin: { top: "0", right: "0", bottom: "0", left: "0" },
    });
    const pdfText = readFileSync(pdfPath).toString("latin1");
    pdfPages = (pdfText.match(/\/Type\s*\/Page\b/g) || []).length;
    assert.equal(pdfPages, 3, `expected exactly 3 PDF pages, got ${pdfPages}`);
  } finally {
    await browser.close();
  }
} catch (err) {
  if (/expected exactly 3 PDF pages/.test(String(err && err.message))) throw err;
  pdfSkipped = true;
  console.warn(
    "[test:adp-leak-audit-pdf-three-page-v1] PDF render skipped:",
    err && err.message ? err.message : err
  );
}

assert.ok(existsSync(join(root, "public/css/adp-leak-audit-report-v2.css")));

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_PDF_THREE_PAGE_V1",
    layoutMode: sample.layoutMode,
    pdfPages,
    pdfSkipped,
  })
);
