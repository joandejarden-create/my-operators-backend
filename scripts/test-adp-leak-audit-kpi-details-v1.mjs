#!/usr/bin/env node
/**
 * Leak Audit KPI detail polish gate.
 * npm run test:adp-leak-audit-kpi-details-v1
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const shared = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const sample = loadLeakAuditSampleReport();

assert.match(shared, /PRIMARY AREA/);
assert.match(shared, /TO REVIEW/);
assert.match(shared, /ala-label-2line/);
assert.match(shared, /aivTooltipContainer/);
assert.match(shared, /openColumnInfo/);
assert.match(html, /id="aivTooltipContainer"/);
assert.match(css, /aiv-value--phrase/);
assert.match(css, /right:\s*6px/);
assert.match(css, /word-break:\s*normal/);

/* KPI cards: value + label only — description lives in info popup */
assert.match(shared, /function kpiValueHtml/);
assert.match(shared, /aiv-value--phrase/);
assert.equal(
  /aiv-meta" title="'\s*\+\s*\n?\s*esc\(desc\)/.test(shared),
  false,
  "KPI band must not render repetitive aiv-meta description"
);
assert.match(css, /aiv-value--phrase/);
assert.match(css, /right:\s*6px/);
assert.match(css, /word-break:\s*normal/);

/* KPI cards: value + label only — description lives in info popup */
assert.match(shared, /function kpiValueHtml/);
assert.match(shared, /aiv-value--phrase/);
assert.equal(
  /aiv-meta" title="'\s*\+\s*\n?\s*esc\(desc\)/.test(shared),
  false,
  "KPI band must not render repetitive aiv-meta description"
);

const primary = (sample.executiveSignals || []).find(
  (k) => k.id === "primary_area_to_review"
);
assert.ok(primary, "primary_area_to_review KPI");
assert.equal(primary.value, "Meetings & Groups");
assert.match(primary.description || "", /Demand area that appears most important to review/);

const helpKeys = [
  "ai_consideration",
  "scenario_presence",
  "reality_coverage",
  "leisure_advantage",
  "primary_area_to_review",
];
for (const id of helpKeys) {
  assert.match(shared, new RegExp(`${id}:\\s*[\\s\\S]{0,40}"[^"]{40,}`), `KPI_HELP ${id}`);
}

assert.match(
  shared,
  /How often the hotel appeared in monitored AI answers\. This helps show whether the hotel is part of the AI-generated consideration set\./
);
assert.match(
  shared,
  /The demand area that appears most important to review based on the monitored results\. This should be confirmed with the hotel before action\./
);

assert.match(css, /-webkit-line-clamp:\s*unset/);
assert.match(css, /overflow:\s*visible\s*!important/);
assert.match(css, /\.ala-adp-report-page\s+\.aiv-kpi\s*\{[\s\S]{0,450}overflow:\s*visible/);

assert.match(css, /info-tooltip\.aiv-col-info/);
assert.match(shared, /info-tooltip aiv-col-info/);
assert.match(shared, /role="button"/);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_KPI_DETAILS_V1",
    twoLinePrimaryArea: true,
    infoPopupTexts: helpKeys.length,
    noPdfEllipsisClamp: true,
  })
);
