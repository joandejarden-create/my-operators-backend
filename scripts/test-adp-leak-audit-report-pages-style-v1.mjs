#!/usr/bin/env node
/**
 * ADP-native report pages style gate for Leak Audit (pages 2–3).
 * npm run test:adp-leak-audit-report-pages-style-v1
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const root = process.cwd();
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const sharedJs = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
const reportJs = readFileSync(join(root, "public/js/adp-leak-audit-report.js"), "utf8");
const qaPath = join(
  root,
  "docs/ai-demand-positioning/adp-leak-audit-demo-pack/FINAL_REPORT_PAGES_QA.md"
);

assert.ok(existsSync(qaPath), "missing FINAL_REPORT_PAGES_QA.md");
const qa = readFileSync(qaPath, "utf8");
assert.match(qa, /ai-visibility-shared\.css/);
assert.match(qa, /aiv-kpi/);
assert.match(qa, /aiv-evidence/);
assert.match(qa, /aiv-drawer|aiv-evidence-drawer/);

// Navy ADP surface — not a generic white shell as primary layout
assert.match(html, /ala-adp-report-page--exec/);
assert.match(html, /ala-adp-report-page--actions/);
assert.match(html, /aiv-report-surface|aiv-theme-group/);
assert.match(css, /--neutral--800|#080f25/);
assert.match(css, /\.ala-adp-report-page[\s\S]*background:\s*(var\(--neutral--800|#080f25)/);
assert.equal(/background:\s*#fff[!;\s]*primary/.test(css), false);

// KPI + info icons ADP-native
assert.match(html, /aiv-kpi-row|alaKpis/);
assert.match(html, /ai-visibility-shared\.css/);
assert.match(html, /aiv-info-icon/);
assert.match(sharedJs, /info-tooltip aiv-col-info/);
assert.match(sharedJs, /use href="#aiv-info-icon"/);
assert.match(sharedJs, /renderKpiBand/);
assert.match(css, /info-tooltip[\s\S]*display:\s*none/);

// Evidence ADP-native
assert.match(html, /aiv-drawer aiv-evidence-drawer/);
assert.match(html, /aiv-evidence-body|alaEvidenceBody/);
assert.match(sharedJs, /aiv-evidence ala-adp-evidence/);
assert.match(sharedJs, /AI response|Why it matters|Management review/);
assert.match(sharedJs, /aiv-btn-text aiv-link/);

// Actions / who / next
assert.match(html, /Priority AI Demand Improvements/);
assert.match(html, /How We Partner on These Improvements|alaWhoHeading/);
assert.match(html, /Book Your ADP Walkthrough|alaNextHeading/);
assert.match(sharedJs, /drs-action ala-adp-action/);
assert.match(reportJs, /renderActionItems|renderWhoDoes|renderSupportingEvidence/);

// Hyperlinks ADP-native; no KPI evidence links
assert.match(sharedJs, /aiv-btn-text aiv-link/);
assert.equal(/webEvidenceBtn\(kpi/.test(reportJs), false);
assert.match(css, /button\.aiv-btn-text\.aiv-link[\s\S]*#6c72ff|accent--primary-1/);

// Footers / page numbers
assert.match(html, /Page 1 of 3/);
assert.match(html, /Page 2 of 3/);
assert.match(html, /Page 3 of 3/);
assert.match(html, /Confidential · For recipient only · © 2026 Dealality/);
assert.match(css, /\.ala-page-foot/);

// No custom white primary shell for pages 2–3
assert.equal(/drs-cover-shell/.test(html), false);
assert.match(html, /data-ala-page="executive"/);
assert.match(html, /data-ala-page="actions"/);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_REPORT_PAGES_STYLE_V1",
    surface: "aiv-theme-group + aiv-kpi + aiv-drawer",
    qa: "FINAL_REPORT_PAGES_QA.md",
  })
);
