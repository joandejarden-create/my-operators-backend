#!/usr/bin/env node
/**
 * Leak Audit How-to-Read must use ADP-native guided modal (report order, 8 steps).
 * npm run test:adp-leak-audit-how-to-read-guide-v1
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { createRequire } from "node:module";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const require = createRequire(import.meta.url);

const reportHtml = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const portfolioHtml = readFileSync(
  join(root, "public/adp-leak-audit-sample-portfolio.html"),
  "utf8"
);
const reportJs = readFileSync(join(root, "public/js/adp-leak-audit-report.js"), "utf8");
const portfolioJs = readFileSync(
  join(root, "public/js/adp-leak-audit-sample-portfolio.js"),
  "utf8"
);
const guideJs = readFileSync(join(root, "public/js/adp-leak-audit-how-to-read-guide.js"), "utf8");
const tourJs = readFileSync(
  join(root, "public/js/ai-demand-positioning/adp-guided-report-tour.js"),
  "utf8"
);
const tourCss = readFileSync(
  join(root, "public/js/ai-demand-positioning/adp-guided-report-tour.css"),
  "utf8"
);
const sample = loadLeakAuditSampleReport();

function assertPageWiresGuide(html, label) {
  assert.equal(
    /id="alaHowToSection"|ala-howto-stepper|How to Read This Report<\/h2>/.test(html),
    false,
    `${label}: static How-to-Read body section must not render`
  );
  assert.match(html, /id="adpHowToReadReport"/);
  assert.match(html, /How to [Rr]ead [Tt]his [Rr]eport/);
  assert.match(html, /adp-guided-report-tour\.css/);
  assert.match(html, /adp-guided-report-tour\.js/);
  assert.match(html, /adp-leak-audit-how-to-read-guide\.js/);
  assert.match(html, /class="adp-howto-read-btn"/);
  assert.match(html, /data-adp-guided-tour-entry/);
}

assertPageWiresGuide(reportHtml, "single");
assertPageWiresGuide(portfolioHtml, "portfolio");
assert.match(reportHtml, /ala-adp-print-guide-note|ala-print-only/);

assert.match(reportJs, /AdpLeakAuditHowToReadGuide/);
assert.match(portfolioJs, /AdpLeakAuditHowToReadGuide/);

assert.match(tourJs, /function configure/);
assert.match(tourJs, /alaReport/);
assert.match(tourJs, /finishStep === false/);
assert.match(tourCss, /\.adp-gt__callout/);

assert.match(guideJs, /Start with the Executive Summary/);
assert.match(guideJs, /Review the Executive Signal/);
assert.match(guideJs, /Demand Area to Review/);
assert.match(guideJs, /Competitors Showed Up Instead|Competitors Showing Up Instead/);
assert.match(guideJs, /Supporting Evidence/);
assert.match(guideJs, /Priority AI Demand Improvements/);
assert.match(html, /Who Does the Work/);
assert.match(html, /Who Does the Work/);
assert.match(guideJs, /How We Partner on These Improvements/);
assert.match(guideJs, /Decide the Next Step/);
assert.match(guideJs, /finishStep:\s*false/);
assert.equal(/Inferred Demand Leak/.test(guideJs), false);
assert.equal(/Check AI Consideration/.test(guideJs), false);
assert.equal(/promptId|fullPrompt|Airtable|rec[A-Za-z0-9]{10,}/.test(guideJs), false);

const orderMarkers = [
  "executive-summary",
  "executive-signal",
  "demand-area",
  "competitors",
  "supporting-evidence",
  "actions",
  "who-does-work",
  "next-step",
];
let lastIdx = -1;
for (const id of orderMarkers) {
  const idx = guideJs.indexOf(`id: "${id}"`);
  assert.ok(idx > lastIdx, `guide order broken at ${id}`);
  lastIdx = idx;
}

globalThis.window = globalThis;
globalThis.document = {
  getElementById() {
    return null;
  },
  querySelector() {
    return null;
  },
  addEventListener() {},
  dispatchEvent() {
    return true;
  },
  createElement() {
    return { style: {}, setAttribute() {}, appendChild() {} };
  },
  body: { classList: { add() {}, remove() {} } },
  documentElement: { classList: { add() {}, remove() {} } },
};
globalThis.CustomEvent = function CustomEvent() {};
globalThis.localStorage = { getItem() { return null; }, setItem() {} };

require(join(root, "public/js/ai-demand-positioning/adp-guided-report-tour.js"));
require(join(root, "public/js/adp-leak-audit-how-to-read-guide.js"));

const Tour = globalThis.AdpGuidedReportTour;
const Guide = globalThis.AdpLeakAuditHowToReadGuide;
assert.ok(Tour && typeof Tour.configure === "function");
assert.ok(Guide);
assert.equal(Guide.singlePropertySteps().length, 8);
assert.ok(Guide.singlePropertySteps().every((s) => s.guide && s.title && s.target));
assert.ok(
  Guide.singlePropertySteps().every((s) => s.signal && s.lookNext),
  "every single-property How-to-Read step needs Stronger/Weaker + Look Next"
);
assert.ok(
  Guide.portfolioSteps().every((s) => s.signal && s.lookNext),
  "every portfolio How-to-Read step needs Stronger/Weaker + Look Next"
);
assert.match(guideJs, /signal:/);
assert.match(guideJs, /lookNext:/);
assert.ok(
  Guide.singlePropertySteps().every((s) => s.signal && s.lookNext),
  "every single-property How-to-Read step needs Stronger/Weaker + Look Next"
);
assert.ok(
  Guide.portfolioSteps().every((s) => s.signal && s.lookNext),
  "every portfolio How-to-Read step needs Stronger/Weaker + Look Next"
);
assert.match(guideJs, /signal:/);
assert.match(guideJs, /lookNext:/);
assert.equal(sample.howToRead.mode, "guided_modal");
assert.equal(sample.howToRead.steps?.length, 8);
assert.match(guideJs, /Dealality can help prepare/);
assert.ok(existsSync(join(root, "public/js/adp-leak-audit-how-to-read-guide.js")));

const shared = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
assert.match(shared, /infoIconHtml|#aiv-info-icon|aiv-col-info/);
assert.match(shared, /bindInfoTooltips/);
assert.match(reportHtml, /id="aiv-info-icon"|#aiv-info-icon/);
assert.match(reportHtml, /adpHowToReadReport/);

const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
assert.match(css, /info-tooltip\.aiv-col-info[\s\S]{0,200}display:\s*none\s*!important/);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_HOW_TO_READ_GUIDE_V1",
    reused: "AdpGuidedReportTour",
    steps: 8,
    surfaces: ["report", "sample-portfolio"],
    kpiInfoIcons: true,
  })
);
