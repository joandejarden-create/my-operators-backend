#!/usr/bin/env node
/**
 * ADP / Deep Research style consistency for Leak Audit.
 * npm run test:adp-leak-audit-adp-style-consistency-v1
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const root = process.cwd();
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const reportJs = readFileSync(join(root, "public/js/adp-leak-audit-report.js"), "utf8");
const sharedJs = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
const qaPath = join(
  root,
  "docs/ai-demand-positioning/adp-leak-audit-demo-pack/FINAL_STYLE_QA.md"
);

assert.ok(existsSync(qaPath), "missing FINAL_STYLE_QA.md");
const qa = readFileSync(qaPath, "utf8");
assert.match(qa, /hotel-intelligence-dossier|Deep Research|COVER_PARITY/i);
assert.match(qa, /brand-alignment-snapshot\.css/);
assert.match(qa, /hotel-intelligence-dossier\.css|hotel-intelligence-dossier\.js/);
assert.match(qa, /ai-visibility-shared\.css/);
assert.match(qa, /three_page_v1|3 pages/i);

// Cover reuses BAS/HID system
assert.match(html, /brand-alignment-snapshot\.css/);
assert.match(html, /hotel-intelligence-dossier\.css/);
assert.match(html, /brand-alignment-snapshot/);
assert.match(html, /bas-cover-page/);
assert.match(html, /bas-cover-geometric/);
assert.match(html, /hid-cover-page/);
assert.match(html, /data-cover-template="hotel-intelligence-dossier"/);
assert.match(html, /hid-cover-page__foot/);
assert.equal(/drs-cover-shell/.test(html), false);
assert.equal(/ala-adp-cover/.test(html), false);
assert.match(html, /dealality-report-print-chrome/);

// Navy ADP surface + 3 pages
assert.match(html, /ala-adp-report-page--exec/);
assert.match(html, /ala-adp-report-page--actions/);
assert.match(css, /#080f25|#0c142c|#101935/);
assert.match(css, /\.ala-adp-report-page[\s\S]*background:\s*(var\(--neutral--800|#080f25|linear-gradient)/);

// KPI / info icons ADP-native
assert.match(html, /ai-visibility-shared\.css/);
assert.match(html, /aiv-kpi-row|alaKpis/);
assert.match(sharedJs, /info-tooltip aiv-col-info/);
assert.match(sharedJs, /class="info-icon/);
assert.match(reportJs, /renderKpiBand|bindInfoTooltips/);
assert.match(css, /info-tooltip[\s\S]*display:\s*none/);

// Evidence + guide behavior
assert.match(html, /aiv-drawer aiv-evidence-drawer/);
assert.match(html, /aiv-evidence|ala-adp-evidence/);
assert.match(html, /How to [Rr]ead [Tt]his [Rr]eport/);
assert.match(html, /ala-adp-print-guide-note|ala-print-only/);
assert.match(html, /aiv-theme-title/);
assert.match(html, /How We Partner on These Improvements|alaWhoHeading/);
assert.match(css, /button\.aiv-btn-text\.aiv-link[\s\S]*display:\s*none|ala-evidence-links[\s\S]*display:\s*none/);
assert.match(sharedJs, /aiv-info-icon/);
assert.match(sharedJs, /aiv-btn-text aiv-link/);
assert.match(sharedJs, /drs-action ala-adp-action/);

// Footers / page labels
assert.match(html, /Page 1 of 3/);
assert.match(html, /Page 2 of 3/);
assert.match(html, /Page 3 of 3/);
assert.match(css, /\.ala-page-foot/);

// Sample + live same shell
const server = readFileSync(join(root, "server.js"), "utf8");
assert.match(server, /\/adp-leak-audit\/sample[\s\S]*adp-leak-audit-report\.html/);
assert.match(server, /\/adp-leak-audit\/:reportId[\s\S]*adp-leak-audit-report\.html/);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_ADP_STYLE_CONSISTENCY_V1",
    cover: "bas-cover-page + hid-cover-page",
    surface: "ala-adp-report-page navy",
    qa: "FINAL_STYLE_QA.md",
  })
);
