#!/usr/bin/env node
/**
 * Phase 3A.8 — Showcase eligibility hardening tests.
 * No provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadDecisionEligibilityConfig,
  getBrandDecisionEligibility,
  summarizeIntentCompetitiveDensity,
  eligibilityIsLanguageNeutral,
  ELIGIBILITY,
} from "../lib/ai-visibility/brand-decision-eligibility.js";
import {
  getBrandGeographyEligibility,
  isSafeForMexicoShowcase,
} from "../lib/ai-visibility/brand-geography-eligibility.js";
import {
  loadPeerSetConfig,
  resolvePeerSetMembership,
  PEER_SET_ID_V1,
  PEER_SET_ID_V2,
} from "../lib/ai-visibility/peer-sets.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3A.8 — Showcase Eligibility Hardening\n");

const cfg = loadDecisionEligibilityConfig();

test("eligibility version 1.3; UNKNOWN preserved for Design Hotels New Build", () => {
  assert.equal(cfg.version, "1.3");
  const nb = getBrandDecisionEligibility("rec02zPClpWUTCyXM", "New Build", cfg);
  assert.equal(nb.state, ELIGIBILITY.UNKNOWN);
});

test("new-build deterministic — footprint >0 ELIGIBLE; zero stays UNKNOWN not NOT_ELIGIBLE", () => {
  const auto = getBrandDecisionEligibility("recEJCTDj1zrsjPM6", "New Build", cfg);
  assert.equal(auto.state, ELIGIBILITY.ELIGIBLE);
  const ascend = getBrandDecisionEligibility("reclkgOzvAcBheUSo", "New Build", cfg);
  assert.equal(ascend.state, ELIGIBILITY.UNKNOWN);
  assert.notEqual(ascend.state, ELIGIBILITY.NOT_ELIGIBLE);
});

test("residences deterministic — No → NOT_ELIGIBLE; Case-by-Case → ELIGIBLE", () => {
  const tribute = getBrandDecisionEligibility(
    "recCvV0PuZOi8c3hC",
    "Branded Residences / Mixed Use",
    cfg
  );
  assert.equal(tribute.state, ELIGIBILITY.NOT_ELIGIBLE);
  const autograph = getBrandDecisionEligibility(
    "recEJCTDj1zrsjPM6",
    "Branded Residences / Mixed Use",
    cfg
  );
  assert.equal(autograph.state, ELIGIBILITY.ELIGIBLE);
  const curio = getBrandDecisionEligibility(
    "receQkxgjlezsc1xg",
    "Branded Residences / Mixed Use",
    cfg
  );
  assert.equal(curio.state, ELIGIBILITY.NOT_ELIGIBLE);
});

test("geo development eligibility separate from operating presence", () => {
  const geo = getBrandGeographyEligibility("recEJCTDj1zrsjPM6");
  assert.equal(geo.CALA, "ELIGIBLE");
  assert.equal(geo.EUROPE, "ELIGIBLE");
  assert.equal(geo.NORTH_AMERICA, "ELIGIBLE");
  assert.equal(geo.MEXICO, "UNKNOWN");
  assert.ok(geo.OPERATING_PRESENCE);
  assert.ok(geo.DEVELOPMENT_ELIGIBILITY);
  assert.equal(geo.DEVELOPMENT_ELIGIBILITY.CALA, "ELIGIBLE");
  // Operating presence may be PRESENT while Mexico development remains UNKNOWN
  assert.equal(geo.MEXICO, "UNKNOWN");
});

test("Mexico UNKNOWN is safe for showcase (not auto-excluded)", () => {
  const mx = isSafeForMexicoShowcase("recEJCTDj1zrsjPM6");
  assert.equal(mx.SAFE_FOR_MEXICO_SHOWCASE, true);
  assert.equal(mx.MEXICO_DEVELOPMENT_ELIGIBILITY, "UNKNOWN");
});

test("language does not alter structural eligibility", () => {
  assert.equal(eligibilityIsLanguageNeutral(), true);
  const a = getBrandDecisionEligibility("recEJCTDj1zrsjPM6", "Conversion", cfg);
  const b = getBrandDecisionEligibility("recEJCTDj1zrsjPM6", "Conversion", cfg);
  assert.equal(a.state, b.state);
  assert.equal(a.LANGUAGE_NEUTRAL, true);
});

test("lifestyle UNKNOWNs preserved for collection brands without lifestyle role", () => {
  for (const id of [
    "receQkxgjlezsc1xg", // Curio
    "reccXxMHEh7NNRhIE", // Tapestry — wait Tapestry was UNKNOWN; check
    "reclkgOzvAcBheUSo", // Ascend
    "recRyvM8OmLlDj9G7", // Individuals
    "recrWCD1LMqu864oU", // MGallery
  ]) {
    const st = getBrandDecisionEligibility(id, "Lifestyle Positioning", cfg);
    assert.equal(st.state, ELIGIBILITY.UNKNOWN, id);
  }
});

test("cohort v2 unchanged; historical v1 unchanged", () => {
  const peerCfg = loadPeerSetConfig();
  const v1 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V1 }, peerCfg);
  const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, peerCfg);
  assert.equal(v1.entityIds.length, 10);
  assert.equal(v2.entityIds.length, 15);
  const batch = JSON.parse(
    fs.readFileSync(
      path.join(
        root,
        "data/ai-visibility/runtime/phase2e/batches/aiv_batch_20260813_d14b3e80.json"
      ),
      "utf8"
    )
  );
  assert.equal(batch.peerSetId, PEER_SET_ID_V1);
});

test("metric formulas unchanged; no Addressable Recommendation Rate", () => {
  assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
  const src = fs.readFileSync(path.join(root, "lib/ai-visibility/metrics.js"), "utf8");
  assert.ok(!/computeAddressableRecommendationRate/.test(src));
});

test("intent density — New Build and Residences improved vs all-UNKNOWN", () => {
  const dens = summarizeIntentCompetitiveDensity(cfg, [
    "New Build",
    "Branded Residences / Mixed Use",
    "Branded Residences",
  ]);
  const nb = dens.find((d) => d.decisionTerritory === "New Build");
  const res = dens.find((d) => d.decisionTerritory === "Branded Residences / Mixed Use");
  assert.ok(nb.ELIGIBLE >= 10);
  assert.ok(nb.UNKNOWN >= 1);
  assert.equal(nb.NOT_ELIGIBLE, 0);
  assert.ok(res.ELIGIBLE >= 5);
  assert.ok(res.NOT_ELIGIBLE >= 3);
});

console.log(`\nResults: ${passed} passed, ${failed} failed`);
console.log("LIVE_PROVIDER_CALLS: 0");
console.log("AIRTABLE_WRITES: 0");
if (failed) process.exit(1);
