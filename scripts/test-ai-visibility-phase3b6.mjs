#!/usr/bin/env node
/**
 * Phase 3B.6 tests — recurring monitoring foundation.
 */
import assert from "node:assert/strict";
import { verifyBaselineFreeze } from "../lib/ai-visibility/baseline-freeze-verify.js";
import { BASELINE_FREEZE_ID } from "../lib/ai-visibility/baseline-freeze.js";
import { MONITORING_RUN_PURPOSE } from "../lib/ai-visibility/monitoring-run-purpose.js";
import {
  buildMonitoringPeriodSchema,
  buildObservationUniqueKey,
  createMonitoringPeriodId,
  PERIOD_STATUS,
  PERIOD_TYPE,
  resolvePeriodStatus,
} from "../lib/ai-visibility/recurring-period-model.js";
import {
  getRecurringMonitoringConfig,
  RECURRING_CADENCE,
  RECURRING_MATRIX,
} from "../lib/ai-visibility/recurring-monitoring-config.js";
import {
  buildPeriodComparabilityKey,
  comparePeriodObservations,
  FULL_PERIOD_COMPARABILITY_RULE,
  PROVIDER_EXECUTION_CONFIG_VERSIONING_REQUIRED,
} from "../lib/ai-visibility/recurring-comparability.js";
import {
  buildRecurringPeriodDryRunMatrix,
  dryRunRecurringPeriod,
  SCHEDULER_ARCHITECTURE,
  MANUAL_EXECUTION_COMMANDS,
} from "../lib/ai-visibility/recurring-period-orchestrator.js";
import { runAllDriftGuards } from "../lib/ai-visibility/recurring-drift-guards.js";
import { buildTrendFoundation, TREND_CHANGE_LABELS } from "../lib/ai-visibility/period-trend-foundation.js";
import {
  buildSourceChangeFoundation,
  SOURCE_CHANGE_TYPES,
} from "../lib/ai-visibility/period-source-change-foundation.js";
import {
  buildPeriodReadContext,
  filterObservationsByPeriod,
} from "../lib/ai-visibility/recurring-period-read-service.js";
import {
  buildEvidenceFootprintForPeriod,
  filterResponsesByPeriod,
} from "../lib/ai-visibility/evidence-footprint.js";
import {
  buildCitedSourceIntelligenceForPeriod,
  filterSourceRowsByPeriod,
  buildMatchedPromptGroups,
} from "../lib/ai-visibility/cited-source-intelligence.js";
import { buildPhase3b6Report } from "../lib/ai-visibility/phase3b6-orchestrator.js";
import { WAVE1_EXECUTION_ORDER } from "../lib/ai-visibility/wave1-showcase-plan.js";

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") {
      return r
        .then(() => {
          passed += 1;
          console.log(`  PASS ${name}`);
        })
        .catch((err) => {
          failed += 1;
          console.error(`  FAIL ${name}: ${err.message}`);
        });
    }
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3B.6 — Recurring Monitoring Foundation\n");

test("baseline freeze — 336 immutable when manifest present", () => {
  const v = verifyBaselineFreeze();
  if (v.BASELINE_FREEZE_VALID) {
    assert.equal(v.OBSERVATIONS, 336);
    assert.equal(v.freezeId, BASELINE_FREEZE_ID);
    assert.equal(v.PROVIDERS.openai.observations, 84);
    assert.equal(v.PROVIDERS.gemini.observations, 84);
    assert.equal(v.PROVIDERS.perplexity.observations, 84);
    assert.equal(v.PROVIDERS.claude.observations, 84);
  } else {
    assert.ok(Array.isArray(v.errors));
  }
});

test("period model — baseline != recurring purpose", () => {
  const baseline = buildMonitoringPeriodSchema({ periodType: PERIOD_TYPE.BASELINE });
  const recurring = buildMonitoringPeriodSchema({ periodType: PERIOD_TYPE.RECURRING });
  assert.equal(baseline.periodPurpose, MONITORING_RUN_PURPOSE.BASELINE);
  assert.equal(recurring.periodPurpose, MONITORING_RUN_PURPOSE.RECURRING);
  assert.notEqual(baseline.periodPurpose, recurring.periodPurpose);
});

test("period model — unique period IDs", () => {
  const a = createMonitoringPeriodId();
  const b = createMonitoringPeriodId();
  assert.notEqual(a, b);
  assert.match(a, /^aiv_monitoring_period_\d{8}_[a-f0-9]{6}$/);
});

test("period model — four provider child waves", () => {
  const p = buildMonitoringPeriodSchema({});
  assert.equal(Object.keys(p.providerWaves).length, 4);
  assert.ok(p.providerWaves.openai);
  assert.ok(p.providerWaves.gemini);
  assert.ok(p.providerWaves.perplexity);
  assert.ok(p.providerWaves.claude);
  assert.equal(p.plannedCalls, 336);
});

test("matrix — 84/provider, 336 total, exact geo/language slots", () => {
  assert.equal(RECURRING_MATRIX.callsPerProvider, 84);
  assert.equal(RECURRING_MATRIX.totalLogicalCalls, 336);
  assert.equal(RECURRING_MATRIX.slots.length, 7);
  for (const s of RECURRING_MATRIX.slots) {
    assert.equal(s.planned, 12);
  }
  assert.deepEqual(
    RECURRING_MATRIX.slots.map((s) => s.key),
    WAVE1_EXECUTION_ORDER.map((s) => s.key)
  );
});

test("dry-run matrix — 336 unique observation keys", () => {
  const periodId = "aiv_monitoring_period_20260814_test01";
  const m = buildRecurringPeriodDryRunMatrix(periodId);
  assert.equal(m.ok, true);
  assert.equal(m.TOTAL, 336);
  assert.equal(m.REQUESTS_BUILDABLE, 336);
  assert.equal(m.uniqueObservationKeys, 336);
  assert.equal(m.byProvider.openai.planned, 84);
  assert.equal(m.byProvider.gemini.planned, 84);
  assert.equal(m.byProvider.perplexity.planned, 84);
  assert.equal(m.byProvider.claude.planned, 84);
});

test("comparability — same semantic fingerprint matches across periods", () => {
  const a = buildPeriodComparabilityKey({
    provider: "openai",
    providerModel: "gpt-5.6",
    promptId: "p1",
    promptVersion: "1",
    geographyKey: "CALA",
    language: "en",
  });
  const b = buildPeriodComparabilityKey({
    provider: "openai",
    providerModel: "gpt-5.6",
    promptId: "p1",
    promptVersion: "1",
    geographyKey: "CALA",
    language: "en",
  });
  const cmp = comparePeriodObservations(a, b);
  assert.equal(cmp.comparable, true);
});

test("comparability — period identity separate from semantic fingerprint", () => {
  const fp = "abc123semantic";
  const k1 = buildObservationUniqueKey("period_a", "openai", fp);
  const k2 = buildObservationUniqueKey("period_b", "openai", fp);
  assert.notEqual(k1, k2);
  assert.ok(k1.includes("period_a"));
  assert.ok(k2.includes("period_b"));
});

test("comparability — model change breaks comparability", () => {
  const cmp = comparePeriodObservations(
    { provider: "openai", providerModel: "gpt-5.6", promptVersion: "1" },
    { provider: "openai", providerModel: "gpt-5.7", promptVersion: "1" }
  );
  assert.equal(cmp.comparable, false);
  assert.equal(cmp.reasonCode, "NON_COMPARABLE_MODEL");
});

test("comparability — prompt version change breaks comparability", () => {
  const cmp = comparePeriodObservations(
    { provider: "openai", providerModel: "gpt-5.6", promptVersion: "1" },
    { provider: "openai", providerModel: "gpt-5.6", promptVersion: "2" }
  );
  assert.equal(cmp.comparable, false);
});

test("comparability — peer version change breaks series", () => {
  const cmp = comparePeriodObservations(
    { provider: "openai", providerModel: "gpt-5.6", peerSetVersion: "2" },
    { provider: "openai", providerModel: "gpt-5.6", peerSetVersion: "3" }
  );
  assert.equal(cmp.comparable, false);
});

test("cost — provider-specific caps + total emergency cap", () => {
  const config = getRecurringMonitoringConfig({
    openai: 38.41,
    gemini: 6.56,
    perplexity: 0.47,
    claude: 58.19,
  });
  assert.ok(config.hardCaps.openai >= 38.41);
  assert.ok(config.hardCaps.gemini >= 6.56);
  assert.ok(config.hardCaps.perplexity >= 0.47);
  assert.ok(config.hardCaps.claude >= 58.19);
  assert.ok(config.hardCaps.TOTAL_PERIOD_CAP >= config.hardCaps.openai);
  assert.ok(config.costEstimate.TOTAL_BASELINE_COST > 100);
});

test("partial period — failure != absence semantics", () => {
  const partial = buildMonitoringPeriodSchema({
    providerWaves: {
      openai: { successful: 84, planned: 84, status: "COMPLETED" },
      gemini: { successful: 84, planned: 84, status: "COMPLETED" },
      perplexity: { successful: 84, planned: 84, status: "COMPLETED" },
      claude: { successful: 70, planned: 84, status: "PARTIAL" },
    },
  });
  const status = resolvePeriodStatus(partial);
  assert.equal(status, PERIOD_STATUS.PARTIAL);
  assert.equal(FULL_PERIOD_COMPARABILITY_RULE.PARTIAL_NOT_COMPARABLE_AS_FULL, true);
});

test("trends — unavailable before second period", () => {
  const t = buildTrendFoundation({ completedComparablePeriods: 1 });
  assert.equal(t.AVAILABLE_NOW, false);
  assert.equal(t.TREND_CALCULATION_FOUNDATION_READY, true);
});

test("trends — no significance logic", () => {
  assert.ok(TREND_CHANGE_LABELS.INCREASED);
  assert.ok(TREND_CHANGE_LABELS.UNCHANGED);
  assert.ok(!TREND_CHANGE_LABELS.SIGNIFICANT);
});

test("sources — period dimension ready, no movement yet", () => {
  const s = buildSourceChangeFoundation({ completedComparablePeriods: 1 });
  assert.equal(s.AVAILABLE_NOW, false);
  assert.equal(s.SOURCE_CHANGE_FOUNDATION_READY, true);
  assert.ok(SOURCE_CHANGE_TYPES.NEWLY_APPEARING_SOURCE);
});

test("evidence footprint — period filter supported", () => {
  const rows = [
    { periodId: "p1", mentions: [], citations: [] },
    { periodId: "p2", mentions: [], citations: [] },
  ];
  assert.equal(filterResponsesByPeriod(rows, "p1").length, 1);
  const fp = buildEvidenceFootprintForPeriod(rows, { periodId: "p1" });
  assert.equal(fp.PERIOD_FILTER_SUPPORTED, "YES");
});

test("cited source — period filter supported", () => {
  const rows = [{ periodId: "p1", citations: [{ domain: "example.com", url: "https://example.com" }] }];
  const intel = buildCitedSourceIntelligenceForPeriod(rows, { periodId: "p1" });
  assert.equal(intel.SOURCE_PERIOD_FILTER_READY, "YES");
});

test("cross-provider — 84 matched groups foundation", () => {
  const m = buildMatchedPromptGroups({});
  assert.equal(m.CONSENSUS_METRICS, "NOT_IMPLEMENTED");
});

test("scheduler — disabled", () => {
  assert.equal(RECURRING_CADENCE.SCHEDULER_ENABLED, false);
  assert.equal(SCHEDULER_ARCHITECTURE.ENABLED, false);
});

test("drift guards — all ready", () => {
  const g = runAllDriftGuards();
  assert.equal(g.guards.prompt.PROMPT_DRIFT_GUARD_READY, true);
  assert.equal(g.guards.peer.PEER_DRIFT_GUARD_READY, true);
  assert.equal(g.guards.metric.METRIC_DRIFT_GUARD_READY, true);
  assert.equal(g.PRE_RUN_MODEL_PROBE_REQUIRED, "YES");
});

test("provider execution config versioning required", () => {
  assert.equal(PROVIDER_EXECUTION_CONFIG_VERSIONING_REQUIRED.REQUIRED, "YES");
});

test("period 2 dry run — valid when baseline present", () => {
  const dr = dryRunRecurringPeriod({ periodId: "aiv_monitoring_period_period2_test", createPeriod: false });
  assert.equal(dr.REQUESTS_BUILDABLE, 336);
  if (dr.freezeVerify.BASELINE_FREEZE_VALID) {
    assert.equal(dr.PERIOD_2_DRY_RUN_VALID, true);
  }
});

test("manual commands — execute blocked", () => {
  assert.equal(MANUAL_EXECUTION_COMMANDS.DO_NOT_RUN, true);
});

test("read service — no visible period selector recommended", () => {
  const ctx = buildPeriodReadContext();
  assert.equal(ctx.VISIBLE_PERIOD_SELECTOR_RECOMMENDED, false);
});

test("idempotency key format", () => {
  const key = buildObservationUniqueKey("period_x", "openai", "fp123");
  assert.equal(key, "period_x|openai|fp123");
});

async function runAsyncTests() {
  await Promise.all([]);
  console.log(`\nPhase 3B.6 tests: ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
}

runAsyncTests();
