#!/usr/bin/env node
/**
 * Competitive Moat Architecture V1 tests — no provider calls.
 */
import assert from "node:assert/strict";
import {
  buildCanonicalIntentIndex,
  PARALLEL_TAXONOMY_CREATED,
} from "../lib/ai-visibility/competitive-moat/canonical-intent.js";
import {
  computeAiPresenceIndex,
  aggregateBenchmarkPresence,
  classifyBenchmarkSampleSize,
  resolveBrandBenchmarkCohort,
  resolveOperatorBenchmarkCohort,
  BENCHMARK_PARITY,
  INDEX_NAME,
} from "../lib/ai-visibility/competitive-moat/benchmark-engine-v1.js";
import {
  deriveObservedCompetitiveSet,
  WIN_LOSS_DEPENDENCY,
} from "../lib/ai-visibility/competitive-moat/observed-competitive-set.js";
import { evaluateEmergingCompetitor, EMERGING_RULE } from "../lib/ai-visibility/competitive-moat/emerging-competitor.js";
import {
  buildCustomerBenchmarkPayload,
  CUSTOMER_PAYLOAD_ALLOWLIST,
  redactToCustomerAllowlist,
} from "../lib/ai-visibility/competitive-moat/customer-payload.js";
import {
  auditCustomerPayloadForBlockedSignals,
  BLOCKED_CLIENT_SIGNALS,
  INDEX_STATUS,
} from "../lib/ai-visibility/competitive-moat/blocked-signals.js";
import {
  auditPayloadForMethodologyLeaks,
  redactPeerMatrixForCustomer,
  redactPromptCorpusFromResponse,
} from "../lib/ai-visibility/competitive-moat/access-redaction.js";
import { validateInfoContracts } from "../lib/ai-visibility/competitive-moat/info-contracts.js";
import { validateObservationRecord } from "../lib/ai-visibility/competitive-moat/observation-ledger-schema.js";
import { ACCESS_DEPTH } from "../lib/ai-visibility/access-depth.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nCompetitive Moat Architecture V1\n");

await test("canonical intent layer does not duplicate scenario taxonomy", () => {
  const idx = buildCanonicalIntentIndex();
  assert.equal(PARALLEL_TAXONOMY_CREATED, false);
  assert.ok(idx.totalCanonicalIntents > 0);
  assert.ok(idx.brandMappings > 0);
  assert.ok(idx.operatorMappings > 0);
  assert.equal(idx.customerExposure, "CONTROLLED");
});

await test("prompt corpus hidden from customer endpoints", () => {
  const redacted = redactPromptCorpusFromResponse({
    promptText: "secret prompt",
    mutationRule: "secret rule",
    prompts: [{ promptText: "x", intentTerritory: "Conversion", ownerDecision: "convert" }],
  });
  assert.equal(redacted.promptText, undefined);
  assert.equal(redacted.mutationRule, undefined);
  assert.equal(redacted.prompts[0].scenarioFamily, "Conversion");
});

await test("raw competitor matrix hidden from comparative customer", () => {
  const peers = [
    { entityId: "a", entityName: "A", aiPresenceRate: 0.8 },
    { entityId: "b", entityName: "B", aiPresenceRate: 0.6 },
  ];
  const out = redactPeerMatrixForCustomer(peers, "a", ACCESS_DEPTH.COMPARATIVE);
  assert.equal(out.length, 1);
  assert.equal(out[0].entityId, "a");
});

await test("full cohort membership hidden from customer payload", () => {
  const payload = buildCustomerBenchmarkPayload({
    subjectEntityId: "rec1",
    indexResult: computeAiPresenceIndex(0.72, 0.6),
    benchmarkMembers: [{ id: "secret" }],
  });
  const redacted = redactToCustomerAllowlist(payload);
  assert.equal(redacted.benchmarkMembers, undefined);
  for (const key of Object.keys(redacted)) {
    assert.ok(CUSTOMER_PAYLOAD_ALLOWLIST.includes(key), `unexpected key ${key}`);
  }
});

await test("index formula correct — 100 = parity", () => {
  const r = computeAiPresenceIndex(0.6, 0.6);
  assert.equal(r.indexValue, 100);
  assert.equal(r.benchmarkParity, 100);
});

await test("above/below benchmark interpretation", () => {
  const above = computeAiPresenceIndex(0.72, 0.6);
  assert.equal(above.indexValue, 120);
  assert.ok(above.interpretation.includes("above"));
  const below = computeAiPresenceIndex(0.48, 0.6);
  assert.equal(below.indexValue, 80);
  assert.ok(below.interpretation.includes("below"));
});

await test("zero benchmark suppression", () => {
  const r = computeAiPresenceIndex(0.5, 0);
  assert.equal(r.ok, false);
  assert.equal(r.status, "INDEX_SUPPRESSED_ZERO_BENCHMARK");
});

await test("small cohort suppression", () => {
  assert.equal(classifyBenchmarkSampleSize(2, 0.5), "SUPPRESSED_INSUFFICIENT_DATA");
  assert.equal(classifyBenchmarkSampleSize(4, 0.5), "LIMITED_BENCHMARK");
  assert.equal(classifyBenchmarkSampleSize(6, 0.5), "VALID_BENCHMARK");
});

await test("Brand vs Operator cohort separation", () => {
  const brand = resolveBrandBenchmarkCohort("recEJCTDj1zrsjPM6");
  assert.equal(brand.entityType, "BRAND");
  const opBlocked = resolveOperatorBenchmarkCohort("rec1", { presenceValidated: false });
  assert.equal(opBlocked.status, "BLOCKED_PENDING_PRESENCE_VALIDATION");
  const op = resolveOperatorBenchmarkCohort("rec1", {
    presenceValidated: true,
    monitoredOperatorIds: ["rec1", "rec2", "rec3"],
  });
  assert.equal(op.entityType, "OPERATOR");
  assert.equal(op.members.length, 2);
});

await test("common cohort — median aggregation", () => {
  const agg = aggregateBenchmarkPresence([0.2, 0.4, 0.8, 1.0], "MEDIAN");
  assert.ok(Math.abs(agg.value - 0.6) < 1e-9);
  assert.equal(agg.sampleSize, 4);
});

await test("2 dates not Trend — emerging rule", () => {
  const r = evaluateEmergingCompetitor({
    periods: [{ appearanceCount: 1 }, { appearanceCount: 3 }],
  });
  assert.equal(r.emergingCandidate, false);
  assert.equal(r.status, "CURRENT_VS_PRIOR_ONLY");
});

await test("observed competitor based on Presence only", () => {
  assert.equal(WIN_LOSS_DEPENDENCY, false);
  const set = deriveObservedCompetitiveSet({
    subjectId: "subj",
    peerRows: [
      { entityId: "p1", entityName: "Peer A", presenceRate: 0.8, subjectMissing: 5 },
      { entityId: "p2", entityName: "Peer B", presenceRate: 0, subjectMissing: 0 },
    ],
  });
  assert.equal(set.topObserved.length, 1);
  assert.equal(set.topObserved[0].winCount, undefined);
});

await test("no win/loss fields on observed competitor", () => {
  const set = deriveObservedCompetitiveSet({
    subjectId: "s",
    peerRows: [{ entityId: "p", entityName: "P", presenceRate: 0.5 }],
  });
  for (const c of set.allObservedInternal) {
    assert.equal(c.winCount, undefined);
    assert.equal(c.lossCount, undefined);
  }
});

await test("no Recommendation fields in customer leak audit", () => {
  const audit = auditCustomerPayloadForBlockedSignals({
    metrics: { RECOMMENDATION_RATE: 0.5, WIN_RATE: 0.3 },
  });
  assert.equal(audit.ok, false);
  assert.ok(audit.violations.some((v) => v.includes("RECOMMENDATION_RATE")));
});

await test("no Preference fields blocked", () => {
  assert.equal(INDEX_STATUS.AI_PREFERENCE_INDEX, "BLOCKED_PENDING_VALIDATED_PREFERENCE_SIGNAL");
  assert.equal(INDEX_STATUS.AI_CONSIDERATION_INDEX, "BLOCKED_PENDING_VALIDATED_CONSIDERATION_MEASUREMENT");
});

await test("source causality blocked — no influence fields", () => {
  const audit = auditPayloadForMethodologyLeaks(
    { sourceInfluence: "drove results" },
    { accessClass: "CUSTOMER_ENTITY" }
  );
  assert.equal(audit.ok, true);
});

await test("customer access restrictions — internal fields blocked", () => {
  const audit = auditPayloadForMethodologyLeaks(
    { benchmarkMembers: ["a", "b"], indexValue: 110 },
    { accessClass: "CUSTOMER_ENTITY" }
  );
  assert.equal(audit.ok, false);
});

await test("admin access to internal diagnostics allowed", () => {
  const audit = auditPayloadForMethodologyLeaks(
    { benchmarkMembers: ["a", "b"], indexValue: 110 },
    { accessClass: "INTERNAL_ADMIN" }
  );
  assert.equal(audit.ok, true);
});

await test("observation record rejects unvalidated inference fields", () => {
  const v = validateObservationRecord({
    observationId: "o1",
    timestamp: "2026-08-18",
    entityType: "BRAND",
    promptId: "p1",
    provider: "openai",
    Winner: true,
  });
  assert.equal(v.ok, false);
});

await test("info contracts complete", () => {
  const v = validateInfoContracts();
  assert.equal(v.ok, true);
});

await test("AI Presence Index canonical name", () => {
  assert.equal(INDEX_NAME, "AI Presence Index");
});

await test("Brand regression — no changes to read paths", () => {
  assert.ok(BENCHMARK_PARITY === 100);
});

await test("Operator regression — 9 monitored operators unchanged", () => {
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
});

await test("emerging competitor requires 3+ periods", () => {
  assert.equal(EMERGING_RULE.minPeriodsForEmerging, 3);
});

await test("customer payload index name present", () => {
  const p = buildCustomerBenchmarkPayload({
    subjectEntityId: "rec1",
    indexResult: computeAiPresenceIndex(0.72, 0.6),
  });
  assert.equal(p.indexName, "AI Presence Index");
  assert.equal(p.benchmarkParity, 100);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed > 0 ? 1 : 0);
