#!/usr/bin/env node
/**
 * GDI How to Read This Report — structural gates (tab-dynamic V1.2).
 *   node scripts/test-gdi-guided-report-tour-v1.mjs
 *
 * Permanent help-content accuracy: whenever a GDI feature, filter, column,
 * metric, or interaction changes, re-run this suite and update matching
 * guide copy / tour targets in gdi-guided-report-tour.js. Do not invent
 * a separate help CMS — keep guides in the existing tour module.
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

const ROOT = process.cwd();
const paths = {
  engine: join(ROOT, "public/js/dealality-guided-tour/dealality-guided-tour-engine.js"),
  tour: join(ROOT, "public/js/group-demand-intelligence/gdi-guided-report-tour.js"),
  ui: join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  es: join(ROOT, "public/js/group-demand-intelligence/executive-summary-tab.js"),
  list: join(ROOT, "public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
  hcp: join(ROOT, "public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js"),
  adpCss: join(ROOT, "public/js/ai-demand-positioning/adp-guided-report-tour.css"),
  html: join(ROOT, "public/group-demand-intelligence.html"),
  share: join(ROOT, "public/group-demand-intelligence-share.html"),
};

for (const [k, p] of Object.entries(paths)) {
  assert.ok(existsSync(p), `missing ${k}: ${p}`);
}

const engine = readFileSync(paths.engine, "utf8");
const tour = readFileSync(paths.tour, "utf8");
const ui = readFileSync(paths.ui, "utf8");
const es = readFileSync(paths.es, "utf8");
const list = readFileSync(paths.list, "utf8");
const hcp = readFileSync(paths.hcp, "utf8");
const html = readFileSync(paths.html, "utf8");
const share = readFileSync(paths.share, "utf8");
const adpCss = readFileSync(paths.adpCss, "utf8");

assert.match(engine, /DealalityGuidedTourEngine/);
assert.match(engine, /createGuidedTour|create:\s*createGuidedTour/);
assert.match(engine, /useTopLayerPopover/);
assert.match(engine, /deferTargetCheck/);

assert.match(tour, /GdiGuidedReportTour/);
assert.match(tour, /GDI_GUIDED_REPORT_TOUR_V1_2_CONTEXTUAL/);
assert.match(tour, /DealalityGuidedTourEngine/);
assert.match(tour, /resolveGuideKey/);
assert.match(tour, /useTopLayerPopover:\s*true/);
assert.match(tour, /demoteModalDrawerForTour/);
assert.match(tour, /bindTabSwitchRefresh/);

// Dynamic tab resolution
assert.match(tour, /executive-summary/);
assert.match(tour, /priorities/);
assert.match(tour, /opportunities/);
assert.match(tour, /Portfolio keeps prior/);
assert.equal(
  /return\s+["']portfolio["']/.test(tour) && /guidePack\(["']portfolio/.test(tour),
  false,
  "portfolio must reuse opportunities guide (no separate portfolio pack)"
);

// Executive Summary guide
assert.match(tour, /Commercial Snapshot/);
assert.match(tour, /Executive Commercial Assessment/);
assert.match(tour, /Demand Composition/);
assert.match(tour, /Commercial Demand Outlook/);
assert.match(tour, /Dealality Research Receipt/);
assert.match(tour, /Priority Commercial Opportunities/);
assert.match(tour, /Opportunity of the Week/);
assert.match(tour, /Commercial Outcomes/);
assert.match(tour, /Estimated productivity benefits/);
assert.match(tour, /verified commercial outcomes/);
assert.match(tour, /es-snapshot/);
assert.match(tour, /es-assessment/);
assert.match(tour, /es-composition/);
assert.match(tour, /es-outlook/);
assert.match(tour, /es-receipt/);
assert.match(tour, /es-priority-opps/);
assert.match(tour, /es-outcomes/);

// Opportunities guide — current UI only
assert.match(tour, /Group & Demand Opportunities/);
assert.match(tour, /Property and Top Filters/);
assert.match(tour, /Market Scope/);
assert.match(tour, /Priority Filters/);
assert.match(tour, /Sales Opportunities Filters/);
assert.match(tour, /List and Tiles Views/);
assert.match(tour, /Columns control/);
assert.match(tour, /Export CSV/);
assert.match(tour, /Who to Contact/);
assert.match(tour, /Opportunity Drawer/);
assert.match(tour, /[Oo]n-demand contact enrichment/);
assert.match(tour, /Competitive is market intelligence/);
assert.match(tour, /_openDrawer/);
assert.match(tour, /_requireDrawer/);
assert.match(tour, /_closeDrawer/);

// Commercial Priorities guide
assert.match(tour, /Commercial Priorities Overview/);
assert.match(tour, /Need Dates/);
assert.match(tour, /Revenue Priorities/);
assert.match(tour, /Preferred Business/);
assert.match(tour, /Target Demand Segments/);
assert.match(tour, /Local Market Intelligence/);
assert.match(tour, /hcp-summary/);
assert.match(tour, /hcp-section-need-dates/);

// Obsolete UI must not reappear in customer-facing guide copy
for (const banned of [
  "Contact Readiness",
  "Opportunity Status",
  "Sales Opportunity Type",
  "Understand opportunity readiness",
  "Hotel Validation / Action / Outcome",
  "How to use GDI",
]) {
  assert.equal(
    tour.includes(banned),
    false,
    `obsolete or retired guide phrase must not appear: ${banned}`
  );
}

// Tour targets must exist in live UI modules (help accuracy gate)
const requiredUiTargets = [
  'data-gdi-tour-target="filters"',
  'data-gdi-tour-target="market-scope"',
  'data-gdi-tour-target="opportunity-list"',
  'data-gdi-tour-target="priority-filters"',
  'data-gdi-tour-target="sales-opportunity-filters"',
  'data-gdi-tour-target="view-toggle"',
  'data-gdi-tour-target="export"',
  'data-gdi-tour-target="drawer-contact"',
  'data-gdi-tour-target="drawer-evidence"',
];
for (const t of requiredUiTargets) {
  assert.ok(ui.includes(t) || list.includes(t), `missing live tour target ${t}`);
}
assert.match(list, /data-gdi-tour-target="columns-control"/);
assert.match(list, /data-gdi-tour-target="opportunity-list"/);

const requiredEsTargets = [
  'data-gdi-tour-target="es-snapshot"',
  'data-gdi-tour-target="es-assessment"',
  'data-gdi-tour-target="es-composition"',
  'data-gdi-tour-target="es-outlook"',
  'data-gdi-tour-target="es-receipt"',
  'data-gdi-tour-target="es-priority-opps"',
  'data-gdi-tour-target="es-outcomes"',
];
for (const t of requiredEsTargets) {
  assert.ok(es.includes(t), `missing ES tour target ${t}`);
}

assert.match(hcp, /data-gdi-tour-target="hcp-summary"/);
assert.match(hcp, /data-gdi-tour-target="hcp-section-/);

assert.match(ui, /id="gdiHowToReadReport"/);
assert.match(ui, /How to Read This Report/);
assert.match(ui, /gdi-section-nav-with-tour/);

assert.match(adpCss, /\.adp-gt/);
assert.match(adpCss, /\.adp-howto-read-btn/);

assert.match(html, /gdi-guided-report-tour\.js\?v=gdi-howto-read-v1-2-contextual/);
assert.match(html, /dealality-guided-tour-engine\.js\?v=gdi-howto-read-v1-2-contextual/);
assert.match(share, /gdi-guided-report-tour\.js\?v=gdi-howto-read-v1-2-contextual/);
assert.match(html, /data-gdi-tour-target="drawer"/);
assert.match(share, /data-gdi-tour-target="drawer"/);

assert.equal(/\bHotel \(67\)|\bAll \(67\)|\bCompetitive \(11\)/.test(tour), false);

console.log(
  JSON.stringify(
    {
      suite: "gdi-guided-report-tour-v1",
      version: "GDI_GUIDED_REPORT_TOUR_V1_2_CONTEXTUAL",
      passed: true,
      reused: ["adp-guided-report-tour.css", "DealalityGuidedTourEngine"],
      guides: ["executive-summary", "opportunities", "priorities"],
      portfolioBehavior: "reuses-opportunities-guide",
      helpContentAccuracyGate:
        "re-run this suite when GDI filters/columns/metrics/interactions change; update tour copy + targets in place",
      obsoleteForbidden: [
        "Contact Readiness",
        "Opportunity Status",
        "Sales Opportunity Type",
      ],
    },
    null,
    2
  )
);
