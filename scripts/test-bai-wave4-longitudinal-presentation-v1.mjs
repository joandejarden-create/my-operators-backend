#!/usr/bin/env node
/**
 * BAI Wave 4 longitudinal presentation gates.
 * Consumes Wave 3 only. PROVIDER_CALLS = 0. Period 2 UNPROMOTED.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BAI_VIEW_MODE } from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";
import {
  BAI_WAVE4_CONSUMES_WAVE3_CANONICAL_HISTORY,
  BAI_TRENDS_USE_CANONICAL_LONGITUDINAL_HISTORY,
  BAI_BRAND_MOVEMENT_VISUAL_INTEGRITY,
  BAI_ABSOLUTE_RELATIVE_VISUAL_SEPARATION,
  BAI_PROVIDER_PRIOR_RUN_RECONCILIATION,
  BAI_COMPETITIVE_MOVEMENT_PERIOD_UNIVERSE_INTEGRITY,
  BAI_COMPETITIVE_NARRATIVE_RECONCILIATION,
  BAI_INTENT_NONCOMPARABILITY_PRESENTATION_INTEGRITY,
  BAI_CURRENT_POSITION_VISUAL_PRIORITY,
  BAI_WAVE4_NO_ORPHAN_VISUAL_LAYOUT,
  BAI_CHART_MARKER_CLIPPING,
  BAI_CHART_LABEL_READABILITY,
  BAI_WAVE4_ALL_PARENT_GROUPS_VISUALIZED,
  BAI_WAVE4_19_BRAND_DISPLAY_RECONCILIATION,
  BAI_WAVE4_UNPROMOTED_PERIOD_ISOLATION,
  BAI_WAVE4_NO_CUSTOMER_PUBLICATION_MUTATION,
  BAI_WAVE4_KNOWN_GOOD_VISUAL_CONTRACT,
  buildBaiWave4LongitudinalPresentationV1,
} from "../lib/ai-visibility/brand-longitudinal/bai-wave4-longitudinal-presentation-v1.js";
import { INTENT_COMPARABILITY_STATE } from "../lib/ai-visibility/brand-longitudinal/bai-wave3-longitudinal-intelligence-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const QA_HTML = fs.readFileSync(
  path.join(ROOT, "public/brand-ai-longitudinal-qa.html"),
  "utf8"
);
const QA_JS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/bai-wave4-longitudinal-qa.js"),
  "utf8"
);
const SHARE_HTML = fs.readFileSync(
  path.join(ROOT, "public/brand-ai-visibility-share.html"),
  "utf8"
);
const AUTH_HTML = fs.readFileSync(
  path.join(ROOT, "public/ai-visibility-brand.html"),
  "utf8"
);
const API_JS = fs.readFileSync(path.join(ROOT, "api/ai-visibility-brand.js"), "utf8");
const CSS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/ai-visibility-shared.css"),
  "utf8"
);

let passed = 0;
let failed = 0;
async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

const full = buildBaiWave4LongitudinalPresentationV1({
  viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
  scope: "full_cohort",
  parentCompanyName: "all",
});

await test(BAI_WAVE4_CONSUMES_WAVE3_CANONICAL_HISTORY, () => {
  assert.equal(full.ok, true, JSON.stringify(full.gates));
  assert.equal(full.gates[BAI_WAVE4_CONSUMES_WAVE3_CANONICAL_HISTORY], true);
  assert.equal(full.ANALYTICAL_CONTRACT_CHANGES, "NONE");
  assert.equal(full.LIVE_PROVIDER_CALLS, 0);
  assert.match(API_JS, /buildBaiWave4LongitudinalPresentationV1/);
});

await test(BAI_TRENDS_USE_CANONICAL_LONGITUDINAL_HISTORY, () => {
  assert.equal(full.gates[BAI_TRENDS_USE_CANONICAL_LONGITUDINAL_HISTORY], true);
  for (const p of full.parents) {
    assert.equal(p.trend.fakeLine, false);
    assert.equal(p.trend.awaitingNextPeriodCopy, false);
    assert.equal(p.trend.chartMode, "LINE");
    assert.equal(p.trend.points.length, 2);
  }
});

await test(BAI_BRAND_MOVEMENT_VISUAL_INTEGRITY, () => {
  assert.equal(full.gates[BAI_BRAND_MOVEMENT_VISUAL_INTEGRITY], true);
  assert.match(QA_HTML, /data-bai-w4="brand-table"/);
  assert.match(QA_JS, /CURRENT_POSITION|LARGEST_GAIN|LARGEST_LOSS|RANK_MOVEMENT/);
});

await test(BAI_ABSOLUTE_RELATIVE_VISUAL_SEPARATION, () => {
  assert.equal(full.gates[BAI_ABSOLUTE_RELATIVE_VISUAL_SEPARATION], true);
  for (const p of full.parents) {
    for (const r of p.brandMovement.rows) {
      assert.ok(["Improved", "Stable", "Declined"].includes(r.absoluteLabel));
      assert.ok(["Improved", "Stable", "Weakened"].includes(r.relativeLabel));
    }
  }
});

await test(BAI_PROVIDER_PRIOR_RUN_RECONCILIATION, () => {
  assert.equal(full.gates[BAI_PROVIDER_PRIOR_RUN_RECONCILIATION], true);
  for (const p of full.parents) {
    assert.equal(
      p.provider.comparabilityState,
      INTENT_COMPARABILITY_STATE.NOT_COMPARABLE_FOR_THIS_PERIOD_PAIR
    );
    assert.equal(p.provider.LIVE_PROVIDER_CALLS, 0);
    assert.ok(p.provider.rows.every((r) => r.fabricatedZero === false));
    assert.ok(p.provider.rows.every((r) => r.priorPresence == null));
  }
});

await test(BAI_COMPETITIVE_MOVEMENT_PERIOD_UNIVERSE_INTEGRITY, () => {
  assert.equal(full.gates[BAI_COMPETITIVE_MOVEMENT_PERIOD_UNIVERSE_INTEGRITY], true);
});

await test(BAI_COMPETITIVE_NARRATIVE_RECONCILIATION, () => {
  assert.equal(full.gates[BAI_COMPETITIVE_NARRATIVE_RECONCILIATION], true);
  for (const p of full.parents) {
    assert.equal(p.competitive.story.available, true);
    assert.ok(p.competitive.story.narrative.length > 20);
  }
});

await test(BAI_INTENT_NONCOMPARABILITY_PRESENTATION_INTEGRITY, () => {
  assert.equal(full.gates[BAI_INTENT_NONCOMPARABILITY_PRESENTATION_INTEGRITY], true);
  assert.match(QA_HTML, /data-bai-w4="intent-state"/);
  for (const p of full.parents) {
    assert.match(p.ownerIntent.presentation, /not yet comparable/i);
    assert.equal(p.ownerIntent.dominatePage, false);
  }
});

await test(BAI_CURRENT_POSITION_VISUAL_PRIORITY, () => {
  assert.equal(full.gates[BAI_CURRENT_POSITION_VISUAL_PRIORITY], true);
  assert.match(QA_JS, /data-bai-priority="current"/);
  assert.match(QA_JS, /bai-w4-kpi--primary/);
});

await test(BAI_WAVE4_NO_ORPHAN_VISUAL_LAYOUT, () => {
  assert.equal(full.gates[BAI_WAVE4_NO_ORPHAN_VISUAL_LAYOUT], true);
  assert.match(CSS, /bai-w4-kpi-grid/);
  assert.match(CSS, /@media \(max-width: 1279px\)/);
  assert.match(CSS, /@media \(max-width: 599px\)/);
});

await test(BAI_CHART_MARKER_CLIPPING, () => {
  assert.equal(full.gates[BAI_CHART_MARKER_CLIPPING], true);
  assert.match(QA_JS, /autoPadding:\s*true/);
  assert.match(QA_JS, /pointRadius:\s*4/);
});

await test(BAI_CHART_LABEL_READABILITY, () => {
  assert.equal(full.gates[BAI_CHART_LABEL_READABILITY], true);
  assert.match(QA_HTML, /chart\.js@4\.4\.1/);
});

await test(BAI_WAVE4_ALL_PARENT_GROUPS_VISUALIZED, () => {
  assert.equal(full.gates[BAI_WAVE4_ALL_PARENT_GROUPS_VISUALIZED], true);
  assert.equal(full.parents.length, 4);
  const keys = full.parents.map((p) => p.parentCompanyKey).sort();
  assert.deepEqual(keys, ["choice", "hilton", "ihg", "marriott"]);
});

await test(BAI_WAVE4_19_BRAND_DISPLAY_RECONCILIATION, () => {
  assert.equal(full.gates[BAI_WAVE4_19_BRAND_DISPLAY_RECONCILIATION], true);
  assert.equal(full.cohortBrandCount, 19);
  assert.equal(full.displayReconciliation.driftCount, 0);
});

await test(BAI_WAVE4_UNPROMOTED_PERIOD_ISOLATION, () => {
  assert.equal(full.PERIOD_2_PUBLICATION_STATE, "UNPROMOTED");
  assert.doesNotMatch(SHARE_HTML + AUTH_HTML, /bai-wave4-longitudinal-qa/);
  assert.doesNotMatch(SHARE_HTML, /aiv_brand_longitudinal_period_20260902_d3d713/);
});

await test(BAI_WAVE4_NO_CUSTOMER_PUBLICATION_MUTATION, () => {
  const customer = buildBaiWave4LongitudinalPresentationV1({
    viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
  });
  assert.equal(customer.ok, false);
  assert.equal(customer.wave4, null);
});

await test(BAI_WAVE4_KNOWN_GOOD_VISUAL_CONTRACT, () => {
  assert.equal(full.gates[BAI_WAVE4_KNOWN_GOOD_VISUAL_CONTRACT], true);
  for (const need of [
    'data-bai-w4-section="executive"',
    'data-bai-w4-section="brand-movement"',
    'data-bai-w4-section="competitive"',
    'data-bai-w4="trend-card"',
    'data-bai-w4="provider-card"',
    'data-bai-w4="intent-state"',
  ]) {
    assert.ok(QA_HTML.includes(need), "missing " + need);
  }
});

console.log("");
console.log(
  failed === 0
    ? `BAI Wave 4 gates PASS (${passed})`
    : `BAI Wave 4 gates FAIL (${failed} failed, ${passed} passed)`
);
process.exit(failed === 0 ? 0 : 1);
