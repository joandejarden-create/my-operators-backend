#!/usr/bin/env node
/**
 * Brand AI Presence Index Pilot V1 tests — no provider calls.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listShowcaseMonitoringBrandIds,
  loadShowcaseCompaniesConfig,
} from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import {
  PEER_SET_ID_V2,
  PEER_SET_ID_V5,
  loadPeerSetConfig,
  resolvePeerSetMembership,
} from "../lib/ai-visibility/peer-sets.js";
import {
  APPROVED_INTERNAL_ADDITION_COUNT,
  listApprovedInternalAdditionIds,
  verifyApprovedInternalAdditions,
} from "../lib/ai-visibility/competitive-moat/approved-internal-additions.js";
import { reExtractPresenceForApprovedAdditions } from "../lib/ai-visibility/competitive-moat/presence-re-extraction.js";
import {
  runBrandPresenceIndexPilot,
  STABILITY_THRESHOLDS,
} from "../lib/ai-visibility/competitive-moat/brand-presence-index-pilot.js";
import {
  computeAiPresenceIndex,
  classifyBenchmarkSampleSize,
  BENCHMARK_PARITY,
} from "../lib/ai-visibility/competitive-moat/benchmark-engine-v1.js";
import { getBrandBenchmarkPayload, resetBenchmarkPilotCache } from "../lib/ai-visibility/competitive-moat/brand-benchmark-read-service.js";
import { auditCustomerPayloadForBlockedSignals } from "../lib/ai-visibility/competitive-moat/blocked-signals.js";
import { auditPayloadForMethodologyLeaks } from "../lib/ai-visibility/competitive-moat/access-redaction.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

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

console.log("\nBrand AI Presence Index Pilot V1\n");

await test("customer-visible count remains 19", () => {
  assert.equal(listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length, 19);
});

await test("7 internal additions only in approved config", () => {
  assert.equal(listApprovedInternalAdditionIds().length, APPROVED_INTERNAL_ADDITION_COUNT);
});

await test("peer-set v2 immutable; v5 adds exactly 7", () => {
  const cfg = loadPeerSetConfig();
  const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, cfg);
  const v5 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V5 }, cfg);
  assert.equal(v2.effectiveCount, 15);
  assert.equal(v5.effectiveCount, 22);
  const added = v5.entityIds.filter((id) => !v2.entityIds.includes(id));
  assert.equal(added.length, 7);
});

await test("all 7 approved identities production-safe", () => {
  const invPath = path.join(ROOT, "reports", "brand-explorer-active-universe-source-of-truth.json");
  const inventory = JSON.parse(fs.readFileSync(invPath, "utf8")).inventory || [];
  const v = verifyApprovedInternalAdditions(inventory);
  assert.equal(v.ok, true, v.excluded?.map((e) => e.brand).join(", "));
});

await test("historical re-extraction preserves timestamps, no synthetic flag", () => {
  const reext = reExtractPresenceForApprovedAdditions();
  assert.equal(reext.providerCalls, 0);
  assert.equal(reext.ok, true);
  for (const b of reext.brands) {
    assert.ok(b.resolvedMentions >= 0);
    for (const o of b.observations.slice(0, 3)) {
      assert.equal(o.synthetic, false);
      assert.equal(o.preservedFromStoredResponse, true);
    }
  }
});

await test("index formula 100 parity", () => {
  const r = computeAiPresenceIndex(0.5, 0.5);
  assert.equal(r.indexValue, 100);
  assert.equal(r.benchmarkParity, BENCHMARK_PARITY);
});

await test("above/below benchmark math", () => {
  const above = computeAiPresenceIndex(0.6, 0.5);
  assert.ok(above.indexValue > 100);
  const below = computeAiPresenceIndex(0.4, 0.5);
  assert.ok(below.indexValue < 100);
});

await test("zero benchmark suppression", () => {
  const z = computeAiPresenceIndex(0.5, 0);
  assert.equal(z.ok, false);
  assert.equal(classifyBenchmarkSampleSize(5, 0), "INDEX_SUPPRESSED_ZERO_BENCHMARK");
});

await test("small sample suppression", () => {
  assert.equal(classifyBenchmarkSampleSize(2), "SUPPRESSED_INSUFFICIENT_DATA");
  assert.equal(classifyBenchmarkSampleSize(4), "LIMITED_BENCHMARK");
  assert.equal(classifyBenchmarkSampleSize(5), "VALID_BENCHMARK");
});

await test("pilot runs offline with providerCalls 0", () => {
  resetBenchmarkPilotCache();
  const report = runBrandPresenceIndexPilot({ writeReport: false });
  assert.equal(report.providerCalls, 0);
  assert.equal(report.customerVisibleBrands, 19);
  assert.equal(report.newInternalAdditions, 7);
});

await test("customer payload redaction", () => {
  resetBenchmarkPilotCache();
  const report = runBrandPresenceIndexPilot({ writeReport: false });
  const subject = report.pilotResults.subjects[0];
  const audit = auditCustomerPayloadForBlockedSignals(subject.customerPayload);
  assert.equal(audit.ok, true, audit.violations?.join("; "));
  const leak = auditPayloadForMethodologyLeaks(subject.customerPayload, { accessClass: "CUSTOMER_ENTITY" });
  assert.equal(leak.ok, true, leak.violations?.join("; "));
});

await test("internal admin diagnostics allowed", () => {
  resetBenchmarkPilotCache();
  const brandId = listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig())[0];
  const internal = getBrandBenchmarkPayload({ brandId, internalAdmin: true, refresh: true });
  assert.equal(internal.accessClass, "INTERNAL_ADMIN");
  assert.ok(internal.payload?.benchmarkMembers?.length >= 0);
});

await test("no Recommendation fields in pilot output", () => {
  resetBenchmarkPilotCache();
  const report = runBrandPresenceIndexPilot({ writeReport: false });
  const blob = JSON.stringify(report);
  assert.ok(!blob.includes('"recommendationRate"'));
  assert.ok(!blob.includes('"winLoss"'));
});

await test("leave-one-out thresholds documented", () => {
  assert.ok(STABILITY_THRESHOLDS.STABLE_MAX < STABILITY_THRESHOLDS.MODERATE_MAX);
});

await test("operator regression unchanged", () => {
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed ? 1 : 0);
