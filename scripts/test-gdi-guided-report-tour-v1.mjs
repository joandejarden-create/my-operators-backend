#!/usr/bin/env node
/**
 * GDI How to Read This Report — structural gates (Opportunities-only V1.1).
 *   node scripts/test-gdi-guided-report-tour-v1.mjs
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

const ROOT = process.cwd();
const paths = {
  engine: join(ROOT, "public/js/dealality-guided-tour/dealality-guided-tour-engine.js"),
  tour: join(ROOT, "public/js/group-demand-intelligence/gdi-guided-report-tour.js"),
  ui: join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
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
const html = readFileSync(paths.html, "utf8");
const share = readFileSync(paths.share, "utf8");
const adpCss = readFileSync(paths.adpCss, "utf8");

assert.match(engine, /DealalityGuidedTourEngine/);
assert.match(engine, /createGuidedTour|create:\s*createGuidedTour/);
assert.match(engine, /adp-gt__callout/);
assert.match(engine, /data-dgt="next"/);
assert.match(engine, /useTopLayerPopover/);
assert.match(engine, /showPopover|raiseTourTopLayer/);
assert.match(engine, /gdi-drawer-body/);
assert.match(engine, /deferTargetCheck/);
assert.match(engine, /dead-end before render/);

assert.match(tour, /GdiGuidedReportTour/);
assert.match(tour, /GDI_GUIDED_REPORT_TOUR_V1_1g/);
assert.match(tour, /DealalityGuidedTourEngine/);
assert.match(tour, /useTopLayerPopover:\s*true/);
assert.match(tour, /demoteModalDrawerForTour/);
assert.match(tour, /repairPointerCapture/);
assert.match(engine, /ensureTourPointerCapture|repairPointerCapture/);
assert.match(engine, /pendingDelta/);
assert.match(tour, /Start with the Opportunities overview/);
assert.match(tour, /Select your hotel and filters/);
assert.match(tour, /Understand Market Scope/);
assert.match(tour, /Read the opportunity cards/);
assert.match(tour, /Understand opportunity readiness/);
assert.match(tour, /Open Opportunity Details/);
assert.match(tour, /Evidence and recommended action/);
assert.match(tour, /Hotel Validation \/ Action \/ Outcome/);
assert.match(tour, /Export and continue working the list/);
assert.match(tour, /How to use GDI/);
assert.match(tour, /_openDrawer/);
assert.match(tour, /_requireDrawer/);
assert.match(tour, /_closeDrawer/);
assert.match(tour, /selectDemoCard/);
assert.match(tour, /openDemoDrawer/);
assert.match(tour, /Competitive is market intelligence/);

// Opportunities-only: must not navigate to / open Commercial Priorities during the tour.
assert.equal(/data-tab=["']commercial-priorities["']/.test(tour), false);
assert.equal(/hotel-commercial-priorities|commercialPriorities/.test(tour), false);
assert.equal(/id:\s*["']commercial-priorities["']/.test(tour), false);
assert.equal(/Click the Commercial Priorities tab/.test(tour), false);

assert.match(ui, /id="gdiHowToReadReport"/);
assert.match(ui, /How to Read This Report/);
assert.match(ui, /gdi-section-nav-with-tour/);
assert.match(ui, /data-gdi-tour-target="filters"/);
assert.match(ui, /data-gdi-tour-target="market-scope"/);
assert.match(ui, /data-gdi-tour-target="opportunity-list"/);
assert.match(ui, /data-gdi-tour-target="opportunity-card"/);
assert.match(ui, /data-gdi-tour-target="card-signals"/);
assert.match(ui, /data-gdi-tour-target="view-details"/);
assert.match(ui, /data-gdi-tour-target="export"/);
assert.match(ui, /data-gdi-tour-target="drawer-evidence"/);
assert.match(ui, /data-gdi-tour-target="drawer-validation"/);

assert.match(adpCss, /\.adp-gt/);
assert.match(adpCss, /\.adp-howto-read-btn/);
assert.match(adpCss, /\.adp-gt\[popover\]/);

assert.match(html, /adp-guided-report-tour\.css/);
assert.match(html, /dealality-guided-tour-engine\.js/);
assert.match(html, /gdi-guided-report-tour\.js/);
assert.match(html, /data-gdi-tour-target="drawer"/);
assert.match(html, /data-drawer-scroll="1"/);
assert.match(share, /gdi-guided-report-tour\.js/);
assert.match(share, /data-gdi-tour-target="drawer"/);

// Must not hardcode opportunity inventory counts in tour copy.
assert.equal(/\bHotel \(67\)|\bAll \(67\)|\bCompetitive \(11\)/.test(tour), false);

console.log(
  JSON.stringify(
    {
      suite: "gdi-guided-report-tour-v1",
      version: "GDI_GUIDED_REPORT_TOUR_V1_1g",
      passed: true,
      reused: ["adp-guided-report-tour.css", "DealalityGuidedTourEngine"],
      gdiSteps: [
        "opportunities-overview",
        "top-filters",
        "market-scope",
        "opportunity-cards",
        "opportunity-readiness",
        "detail-drawer",
        "drawer-evidence",
        "hotel-validation",
        "export",
        "how-to-use",
      ],
      tabsCovered: ["opportunities"],
      drawerIntegration: "native dialog + Popover API top layer",
      commercialPrioritiesInTour: false,
    },
    null,
    2
  )
);
