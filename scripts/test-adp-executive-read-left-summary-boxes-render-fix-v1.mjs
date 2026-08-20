#!/usr/bin/env node
/**
 *   npm run test:adp-executive-read-left-summary-boxes-render-fix-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import {
  resolveExecutiveReadPresentation,
  executiveReadNeedsUxEnrichment,
} from "../lib/ai-demand-positioning/customer/executive-read-v2.js";
import { enrichPayloadOptionalMetrics } from "../lib/ai-demand-positioning/published-read-service.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");

const PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
];

function enrichPublishedSnapshot(propertyId) {
  const snapshot = loadPublishedReport(propertyId);
  assert.ok(snapshot, `${propertyId} snapshot`);
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestPeriod(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const allPeriods = loadAllPeriods(propertyId);
  return enrichPayloadOptionalMetrics(propertyId, snapshot);
}

function simulateRenderer(presentation) {
  if (!presentation || !presentation.narrative) return { boxCount: 0, html: "" };
  const boxes = [
    presentation.biggestStrength,
    presentation.biggestConstraint,
    presentation.changeSinceLastRun,
  ];
  const html = boxes
    .map((box) => `${box.sectionLabel}|${box.headline}|${box.body}`)
    .join("\n");
  return { boxCount: boxes.filter((b) => b.headline && b.body).length, html };
}

async function main() {
  const ui = readFileSync(UI, "utf8");
  const html = readFileSync(HTML, "utf8");

  assert.ok(ui.includes("resolveExecutiveReadPresentation"));
  assert.ok(ui.includes("renderExecutiveSummaryBox"));
  assert.ok(html.includes("adpExecutiveReadSummaries"));

  let rightWriteupLeftEmpty = 0;

  for (const propertyId of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const allPeriods = loadAllPeriods(propertyId);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods });

    assert.ok(payload.executiveRead?.ux, `${propertyId} runtime ux`);
    assert.ok(payload.executiveRead?.summary?.biggestStrength, `${propertyId} summary contract`);
    assert.ok(payload.executiveRead?.writeup?.body, `${propertyId} writeup contract`);

    const presentation = resolveExecutiveReadPresentation(payload.executiveRead);
    assert.ok(presentation, `${propertyId} presentation`);
    assert.ok(presentation.biggestStrength.headline, `${propertyId} strength headline`);
    assert.ok(presentation.biggestConstraint.body, `${propertyId} constraint body`);
    assert.ok(presentation.changeSinceLastRun.body, `${propertyId} change body`);
    assert.ok(presentation.narrative, `${propertyId} narrative`);

    const rendered = simulateRenderer(presentation);
    assert.equal(rendered.boxCount, 3, `${propertyId} three boxes`);

    const enrichedSnapshot = enrichPublishedSnapshot(propertyId);
    const snapEr = enrichedSnapshot.executiveRead;
    assert.ok(snapEr?.ux || snapEr?.summary, `${propertyId} enriched snapshot ux`);
    assert.equal(executiveReadNeedsUxEnrichment(snapEr), false, `${propertyId} enrichment complete`);

    const snapPresentation = resolveExecutiveReadPresentation(snapEr);
    assert.equal(simulateRenderer(snapPresentation).boxCount, 3, `${propertyId} snapshot render boxes`);
    if (snapPresentation.narrative && simulateRenderer(snapPresentation).boxCount === 0) {
      rightWriteupLeftEmpty++;
    }

    // V1-only compatibility path
    const v1Only = {
      ...enrichedSnapshot,
      executiveRead: {
        version: "adp_executive_read_v1",
        available: true,
        CURRENT_READ_AVAILABLE: true,
        COMPARABLE_PRIOR_AVAILABLE: snapEr.COMPARABLE_PRIOR_AVAILABLE,
        current: snapEr.current,
        trend: snapEr.trend,
        narrative: snapEr.current?.narrative || snapEr.narrative,
      },
    };
    assert.equal(executiveReadNeedsUxEnrichment(v1Only.executiveRead), true, `${propertyId} v1 needs ux`);
    const v1Enriched = enrichPayloadOptionalMetrics(propertyId, v1Only);
    assert.ok(v1Enriched.executiveRead?.ux?.biggestStrength, `${propertyId} v1 enriched ux`);
    const v1Presentation = resolveExecutiveReadPresentation(v1Enriched.executiveRead);
    assert.equal(simulateRenderer(v1Presentation).boxCount, 3, `${propertyId} v1 compat boxes`);
    if (v1Presentation.narrative && simulateRenderer(v1Presentation).boxCount === 0) {
      rightWriteupLeftEmpty++;
    }
  }

  const wsEnriched = enrichPublishedSnapshot("adp_waterstone_boca_raton");
  const wsPresentation = resolveExecutiveReadPresentation(wsEnriched.executiveRead);
  assert.ok(/Couples|Romantic|Still developing/i.test(wsPresentation.biggestStrength.headline));

  assert.equal(rightWriteupLeftEmpty, 0);

  console.log("test:adp-executive-read-left-summary-boxes-render-fix-v1 PASS");
  console.log("  ROOT_CAUSE: READ_SERVICE skipped V1-only executiveRead UX enrichment; renderer only painted left boxes from er.ux");
  console.log("  LAYER: READ_SERVICE + RENDERER");
  console.log("  RIGHT_WRITEUP_VISIBLE_AND_LEFT_SUMMARY_EMPTY:", rightWriteupLeftEmpty);
  console.log("  LEFT_BOX_COUNT: 3");
  console.log("  final: ADP_EXECUTIVE_READ_LEFT_SUMMARY_BOXES_RENDER_FIX_V1_PASS");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
