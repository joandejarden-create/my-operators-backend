#!/usr/bin/env node
/**
 * ADP-native brand styling gate for leak-audit reports.
 * npm run test:adp-leak-audit-brand-style-v1
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const sample = loadLeakAuditSampleReport();
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const portfolioHtml = readFileSync(
  join(root, "public/adp-leak-audit-sample-portfolio.html"),
  "utf8"
);
const styleQa = readFileSync(
  join(root, "docs/ai-demand-positioning/adp-leak-audit-demo-pack/STYLE_QA.md"),
  "utf8"
);

assert.equal(sample.layoutMode, "adp_two_page_v1");
assert.match(sample.productFamily || "", /Dealality AI Demand Positioning \(ADP\)/);

assert.match(html, /brand-alignment-snapshot\.css/);
assert.match(html, /dealality-report-system-v1\.css/);
assert.match(html, /dealality-report-print-chrome\.css/);
assert.match(html, /adp-monthly-review-report-v1\.css/);
assert.match(html, /adp-leak-audit-report-v2\.css/);
assert.match(html, /dealality-report-print-chrome\.js/);

assert.match(html, /bas-cover-page/);
assert.match(html, /bas-cover-geometric/);
assert.match(html, /bas-cover-logo-img/);
assert.match(html, /ala-adp-report-page|alaKpis/);
assert.match(html, /data-adp-report-family="dealality-report-system-v1"/);
assert.match(html, /Dealality AI Demand Positioning \(ADP\)/);
assert.match(html, /Recommended Dealality Action Items|alaActionsHeading/);
assert.match(html, /Executive Summary/);
assert.match(html, /Demand Area to Review/);
assert.match(html, /Competitors Showing Up Instead/);
assert.match(html, /Supporting Evidence/);
assert.equal(/Inferred Demand Leak/.test(html), false);
assert.equal(/Priority Demand Watch/.test(html), false);
assert.match(html, /Demand Area to Review/);
assert.match(html, /Competitors Showing Up Instead/);
assert.match(html, /Supporting Evidence/);
assert.equal(/Inferred Demand Leak/.test(html), false);
assert.equal(/Priority Demand Watch/.test(html), false);

assert.match(portfolioHtml, /brand-alignment-snapshot\.css/);
assert.match(portfolioHtml, /dealality-report-system-v1\.css/);
assert.match(portfolioHtml, /bas-cover-page/);

assert.match(css, /#080f25|#0c142c|#101935/);
assert.match(css, /#6c72ff|var\(--ala-accent/);
assert.match(css, /#fdb52a|var\(--ala-gold/);
assert.match(css, /ala-drawer|aiv-evidence/);
assert.match(css, /ala-adp-report-page/);
assert.equal(/Fraunces/i.test(html), false);
assert.equal(/DM Sans/i.test(html), false);
assert.match(html, /Inter/);

assert.match(styleQa, /dealality-report-system-v1|adp-leak-audit|COVER_PARITY|Hotel Intelligence|FINAL_REPORT_PAGES/);
const coverParity = join(
  root,
  "docs/ai-demand-positioning/adp-leak-audit-demo-pack/COVER_PARITY_QA.md"
);
assert.ok(existsSync(coverParity), "missing COVER_PARITY_QA.md");
assert.match(readFileSync(coverParity, "utf8"), /hotel-intelligence-dossier\.js/);
assert.match(readFileSync(coverParity, "utf8"), /bas-cover-geometric/);
assert.match(JSON.stringify(sample.competitorDisplacementRank), /not a full market ranking/i);

const pagesQa = join(
  root,
  "docs/ai-demand-positioning/adp-leak-audit-demo-pack/FINAL_REPORT_PAGES_QA.md"
);
assert.ok(existsSync(pagesQa), "missing FINAL_REPORT_PAGES_QA.md");
assert.match(readFileSync(pagesQa, "utf8"), /aiv-kpi|aiv-evidence|aiv-drawer/);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_BRAND_STYLE_V1",
    reportFamily: "dealality-report-system-v1",
    layoutMode: sample.layoutMode,
    shell: "adp-leak-audit-report.html",
  })
);
