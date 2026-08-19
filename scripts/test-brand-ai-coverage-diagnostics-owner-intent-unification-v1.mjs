#!/usr/bin/env node
/**
 * Brand AI Coverage Diagnostics Owner Intent Unification V1.
 * PROVIDER_CALLS = 0
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import {
  buildUnifiedOwnerIntentCoverage,
  computePeerPresentGapFromFixture,
  COVERAGE_OWNER_INTENT_DISPLAY_ORDER,
} from "../lib/ai-visibility/competitive-moat/unified-owner-intent-coverage.js";
import { getCustomerScenarioDisplayLabel } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-customer-service.js";
import { verifyFrozenBaseline } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-longitudinal-recertification.js";
import { auditPayloadForCanonicalPromptLeaks } from "../lib/ai-visibility/customer-prompt-disclosure.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

const SUBJECT = "recSubjectBrand";
const CORE_PEER = "recCorePeer";
const UNRELATED = "recUnrelatedBrand";
const SECONDARY_PEER = "recSecondaryPeer";

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

function obs(presentIds, opts = {}) {
  return {
    success: opts.success !== false,
    provider: opts.provider || "openai",
    promptId: opts.promptId || "prompt_test_v1",
    presentEntityIds: presentIds,
  };
}

console.log("\nBrand AI Coverage Diagnostics Owner Intent Unification V1\n");

await test("single customer taxonomy — governed Owner Intent labels", () => {
  assert.ok(COVERAGE_OWNER_INTENT_DISPLAY_ORDER.includes(S.SOFT_BRAND));
  assert.equal(getCustomerScenarioDisplayLabel(S.SOFT_BRAND), "Soft Brand Affiliation");
  assert.doesNotMatch(getCustomerScenarioDisplayLabel(S.CONVERSION_SUITABILITY), /_/);
});

await test("peer-present gap CASE 1 — subject present → gap 0", () => {
  const result = computePeerPresentGapFromFixture(
    [obs([SUBJECT, CORE_PEER])],
    SUBJECT,
    [CORE_PEER]
  );
  assert.equal(result.missing, 0);
  assert.equal(result.peerPresentGapCount, 0);
});

await test("peer-present gap CASE 2 — subject absent, no qualifying peer → gap 0", () => {
  const result = computePeerPresentGapFromFixture(
    [obs([UNRELATED])],
    SUBJECT,
    [CORE_PEER]
  );
  assert.equal(result.missing, 1);
  assert.equal(result.peerPresentGapCount, 0);
});

await test("peer-present gap CASE 3 — subject absent, CORE peer present → gap 1", () => {
  const result = computePeerPresentGapFromFixture(
    [obs([CORE_PEER])],
    SUBJECT,
    [CORE_PEER]
  );
  assert.equal(result.missing, 1);
  assert.equal(result.peerPresentGapCount, 1);
});

await test("peer-present gap CASE 4 — unrelated brand only → gap 0", () => {
  const result = computePeerPresentGapFromFixture(
    [obs([UNRELATED])],
    SUBJECT,
    [CORE_PEER]
  );
  assert.equal(result.missing, 1);
  assert.equal(result.peerPresentGapCount, 0);
});

await test("peer-present gap CASE 5 — SECONDARY only does not count (CORE policy)", () => {
  const result = computePeerPresentGapFromFixture(
    [obs([SECONDARY_PEER])],
    SUBJECT,
    [CORE_PEER]
  );
  assert.equal(result.missing, 1);
  assert.equal(result.peerPresentGapCount, 0);
});

await test("peer-present gap CASE 6 — provider unavailable not counted as absence", () => {
  const result = computePeerPresentGapFromFixture(
    [obs([], { success: false })],
    SUBJECT,
    [CORE_PEER]
  );
  assert.equal(result.missing, 0);
  assert.equal(result.peerPresentGapCount, 0);
});

await test("unified payload uses Owner Intent columns and zero not dash", () => {
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: [],
  });
  assert.equal(payload.SINGLE_CUSTOMER_TAXONOMY, "OWNER_INTENT");
  assert.equal(payload.LEGACY_PROMPT_FAMILY_PRIMARY_LABELS, false);
  assert.equal(payload.title, "AI Presence by Owner Intent");
  assert.equal(payload.TRUE_ZERO_RENDERED_AS_ZERO, true);
  assert.match(payload.qualifyingPeerPolicy, /CORE_PEERS_PRESENT/);
  for (const row of payload.rows || []) {
    assert.ok(row.intentLabel);
    assert.doesNotMatch(row.intentLabel, /^[A-Z_]+$/);
    if (row.peerPresentGapCount === 0) {
      assert.equal(row.peerPresentGapCount, 0);
    }
  }
});

await test("certified baseline freeze — Autograph/Tapestry 103, Ascend 67", () => {
  const baseline = verifyFrozenBaseline();
  assert.equal(baseline.AUTOGRAPH_103_DIFF, 0);
  assert.equal(baseline.TAPESTRY_103_DIFF, 0);
  assert.equal(baseline.ASCEND_67_DIFF, 0);
  const all = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: [],
  });
  const soft = (all.rows || []).find((r) => r.scenarioId === S.SOFT_BRAND);
  assert.ok(soft, "Soft Brand Affiliation row expected");
  assert.equal(soft.indexValue, 103);
});

await test("openai exact-scope certified index independent of all providers 103", () => {
  const openai = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "openai",
    observations: [],
  });
  const soft = (openai.rows || []).find((r) => r.scenarioId === S.SOFT_BRAND);
  assert.ok(soft);
  assert.notEqual(soft.indexValue, 103);
  if (soft.indexValue != null) {
    assert.equal(soft.benchmarkStatus, "CERTIFIED");
    assert.equal(soft.indexValue, 100);
  } else {
    assert.equal(soft.benchmarkStatus, "Benchmark still developing");
  }
});

await test("customer payload has no raw prompt fields", () => {
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: [],
  });
  const leak = auditPayloadForCanonicalPromptLeaks(payload);
  assert.equal(leak.leakCount, 0, leak.leaks?.join(", "));
});

await test("UI contract — unified table markers in brand JS", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"),
    "utf8"
  );
  assert.match(js, /aiv-unified-intent-table/);
  assert.match(js, /Owner Intent/);
  assert.match(js, /PEER_PRESENT_GAPS/);
  assert.match(js, /renderOwnerIntentBenchmarks\(data/);
  assert.doesNotMatch(js, /renderOwnerIntentBenchmarks\(data \|\| \{\}, peerTableHtml\)/);
});

await test("HTML — Coverage Diagnostics unified title", () => {
  const html = fs.readFileSync(path.join(ROOT, "public/ai-visibility-brand.html"), "utf8");
  assert.match(html, /AI Presence by Owner Intent/);
  assert.doesNotMatch(html, /Owner-Intent Coverage/);
});

console.log("\n" + passed + " passed, " + failed + " failed\n");

if (failed > 0) {
  process.exitCode = 1;
  console.log("BRAND_AI_COVERAGE_DIAGNOSTICS_OWNER_INTENT_UNIFICATION_REMEDIATION_REQUIRED");
} else {
  console.log("BRAND_AI_COVERAGE_DIAGNOSTICS_OWNER_INTENT_UNIFICATION_PASS");
}
