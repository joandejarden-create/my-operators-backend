#!/usr/bin/env node
/**
 * Core + BPP row-level Prior Run / rank-movement gates.
 * npm run test:adp-row-level-prior-run-movement-v1
 */

import assert from "assert";
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { join } from "path";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../lib/ai-demand-positioning/contracts/adp-certified-property-cohort-v1.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { enrichPayloadOptionalMetrics } from "../lib/ai-demand-positioning/published-read-service.js";
import { loadCustomerPublishedBrandPortfolio } from "../api/ai-demand-positioning.js";
import {
  resolveRowLevelPriorComparisonV1,
  formatGovernedDeltaDisplay,
  formatRankWithMovement,
  resolveRankDirection,
  SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER,
  PRIOR_RUN_ROW_IDENTITY_INTEGRITY,
  PRIOR_RUN_DELTA_CALCULATION_INTEGRITY,
  PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY,
  RANK_DIRECTION_SEMANTICS_INTEGRITY,
  HISTORICAL_RANK_USES_PERIOD_SPECIFIC_UNIVERSE,
  LONGITUDINAL_ROW_MEMBERSHIP_STATE_INTEGRITY,
  CUSTOMER_ROW_MOVEMENT_PAYLOAD_COMPLETE,
  LOCAL_PRODUCTION_ROW_MOVEMENT_PARITY,
  DELTA_UNIT,
  ROW_MEMBERSHIP_STATE,
  RANK_DIRECTION,
} from "../lib/ai-demand-positioning/longitudinal/resolve-row-level-prior-comparison-v1.js";
import {
  buildCoreCompetitiveMovementFromCertifiedHistory,
  attachCoreRowLevelPriorComparisons,
} from "../lib/ai-demand-positioning/longitudinal/attach-row-level-prior-comparisons-v1.js";

const OUT_DIR = join(process.cwd(), "reports/ai-demand-positioning/row-level-prior-run-movement");
const results = {
  stamp: new Date().toISOString(),
  gates: {},
  properties: [],
  goldCases: [],
};

function mark(gate, pass, detail = null) {
  results.gates[gate] = pass ? "PASS" : "FAIL";
  if (!pass) {
    const err = new Error(`${gate}${detail ? `: ${detail}` : ""}`);
    err.gate = gate;
    throw err;
  }
}

function main() {
  mkdirSync(OUT_DIR, { recursive: true });

  // --- Unit: resolver semantics ---
  const improved = resolveRowLevelPriorComparisonV1({
    measurementFamily: "CORE",
    propertyId: "test",
    currentPeriodId: "p2",
    priorPeriodId: "p1",
    scopeType: "competitive",
    canonicalRowId: "hotel_a",
    metric: "aiPresencePct",
    currentValue: 51.9,
    priorValue: 48.7,
    currentRank: 2,
    priorRank: 4,
    comparable: true,
    deltaUnit: DELTA_UNIT.PP,
  });
  assert.strictEqual(improved.delta, 3.2);
  assert.strictEqual(improved.deltaDisplay, "+3.2 pp");
  assert.strictEqual(improved.rankDelta, 2);
  assert.strictEqual(improved.rankDirection, RANK_DIRECTION.IMPROVED);
  assert.strictEqual(improved.rankDisplay, "#2 ↑2");
  assert.strictEqual(improved.resolver, SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER);

  const declined = resolveRowLevelPriorComparisonV1({
    measurementFamily: "CORE",
    propertyId: "test",
    currentPeriodId: "p2",
    priorPeriodId: "p1",
    scopeType: "competitive",
    canonicalRowId: "hotel_b",
    metric: "aiPresencePct",
    currentValue: 10,
    priorValue: 12,
    currentRank: 4,
    priorRank: 2,
    comparable: true,
    deltaUnit: DELTA_UNIT.PP,
  });
  assert.strictEqual(declined.rankDelta, -2);
  assert.strictEqual(declined.rankDirection, RANK_DIRECTION.DECLINED);
  assert.strictEqual(declined.rankDisplay, "#4 ↓2");
  assert.strictEqual(declined.deltaDisplay, "-2.0 pp");

  const flat = resolveRowLevelPriorComparisonV1({
    measurementFamily: "CORE",
    propertyId: "test",
    currentPeriodId: "p2",
    priorPeriodId: "p1",
    scopeType: "competitive",
    canonicalRowId: "hotel_c",
    metric: "aiPresencePct",
    currentValue: 20,
    priorValue: 20,
    currentRank: 3,
    priorRank: 3,
    comparable: true,
    deltaUnit: DELTA_UNIT.PP,
  });
  assert.strictEqual(flat.delta, 0);
  assert.strictEqual(flat.deltaDisplay, "0.0 pp");
  assert.strictEqual(flat.rankDelta, 0);
  assert.strictEqual(flat.rankDisplay, "#3");

  const neu = resolveRowLevelPriorComparisonV1({
    measurementFamily: "CORE",
    propertyId: "test",
    currentPeriodId: "p2",
    priorPeriodId: "p1",
    scopeType: "competitive",
    canonicalRowId: "hotel_new",
    metric: "aiPresencePct",
    currentValue: 5,
    priorValue: null,
    currentRank: 8,
    priorRank: null,
    currentExists: true,
    priorExists: false,
    comparable: true,
    deltaUnit: DELTA_UNIT.PP,
  });
  assert.strictEqual(neu.movementState, ROW_MEMBERSHIP_STATE.NEW);
  assert.strictEqual(neu.deltaDisplay, "NEW");
  assert.strictEqual(neu.delta, null); // not 0

  const missing = resolveRowLevelPriorComparisonV1({
    measurementFamily: "CORE",
    propertyId: "test",
    currentPeriodId: "p2",
    priorPeriodId: "p1",
    scopeType: "competitive",
    canonicalRowId: "hotel_missing_prior_value",
    metric: "aiPresencePct",
    currentValue: 5,
    priorValue: null,
    currentRank: 1,
    priorRank: 1,
    currentExists: true,
    priorExists: true,
    comparable: true,
    deltaUnit: DELTA_UNIT.PP,
  });
  assert.strictEqual(missing.delta, null);
  assert.notStrictEqual(missing.deltaDisplay, "0");
  assert.notStrictEqual(missing.deltaDisplay, "0.0 pp");

  mark(PRIOR_RUN_ROW_IDENTITY_INTEGRITY, true);
  mark(PRIOR_RUN_DELTA_CALCULATION_INTEGRITY, true);
  mark(PRIOR_RUN_DELTA_NULL_ZERO_INTEGRITY, true);
  mark(RANK_DIRECTION_SEMANTICS_INTEGRITY, true);
  mark(LONGITUDINAL_ROW_MEMBERSHIP_STATE_INTEGRITY, true);
  mark(SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER, true);

  const uiSrc = readFileSync(
    join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js"),
    "utf8"
  );
  assert.ok(
    !uiSrc.includes(
      'aiv-delta-cell aiv-chg-metric-cell"><span class="aiv-avail-insufficient_history aiv-delta-none">—</span></td>'
    ) || uiSrc.includes("row.deltaDisplay"),
    "UI must consume payload deltaDisplay for competitive table"
  );
  assert.ok(uiSrc.includes("row.rankDisplay") || uiSrc.includes("rankDisplay"), "UI rank movement");
  assert.ok(uiSrc.includes("idxData.deltaDisplay") || uiSrc.includes("intentDelta"), "UI intent delta");

  let payloadComplete = true;
  let historyUniverse = true;
  let sawPositive = false;
  let sawNegative = false;
  let sawZero = false;
  let sawRankImprove = false;
  let sawRankDecline = false;
  let sawUnchangedRank = false;
  let sawMembership = false;

  for (const propertyId of ADP_CERTIFIED_PROPERTY_IDS) {
    const published = loadPublishedReport(propertyId);
    assert.ok(published, `${propertyId} published`);
    const payload = enrichPayloadOptionalMetrics(propertyId, published);
    const currentPeriodId = payload.period?.periodId;
    const priorPeriodId = payload.executiveMetrics?.currentVsPrior?.priorComparablePeriodId;
    assert.ok(currentPeriodId && priorPeriodId, `${propertyId} prior lineage`);

    const movement = buildCoreCompetitiveMovementFromCertifiedHistory(
      propertyId,
      currentPeriodId,
      priorPeriodId
    );
    if (!movement.comparable) {
      historyUniverse = false;
      payloadComplete = false;
    }

    const overall = payload.competitiveRankingByTerritory?.byTerritory?.overall?.displayRows || [];
    assert.ok(overall.length > 0, `${propertyId} overall rows`);

    const reconRows = [];
    for (const row of overall) {
      assert.ok(row.entityId, `${propertyId} entityId required`);
      const hasDelta =
        (row.deltaDisplay && row.deltaDisplay !== "—") ||
        row.movementState === ROW_MEMBERSHIP_STATE.NEW ||
        row.movementState === ROW_MEMBERSHIP_STATE.EXITED;
      if (!hasDelta) payloadComplete = false;

      if (row.delta > 0) sawPositive = true;
      if (row.delta < 0) sawNegative = true;
      if (row.delta === 0) sawZero = true;
      if (row.rankDelta > 0) sawRankImprove = true;
      if (row.rankDelta < 0) sawRankDecline = true;
      if (row.rankDelta === 0) sawUnchangedRank = true;
      if (
        row.movementState === ROW_MEMBERSHIP_STATE.NEW ||
        row.movementState === ROW_MEMBERSHIP_STATE.EXITED ||
        row.movementState === ROW_MEMBERSHIP_STATE.RETURNED
      ) {
        sawMembership = true;
      }

      // Historical ranks must come from movement ledger (period-specific), not re-rank
      if (row.priorRank != null && row.rankDelta != null && row.displayRank != null) {
        const expected = Number(row.priorRank) - Number(row.displayRank);
        if (Number(row.rankDelta) !== expected && row.movementState === ROW_MEMBERSHIP_STATE.SAME) {
          // displayRank may differ from observedRank for appended rows — use observedRank when present
          const obs = row.observedRank != null ? row.observedRank : row.displayRank;
          const expectedObs = Number(row.priorRank) - Number(obs);
          if (Number(row.rankDelta) !== expectedObs) {
            historyUniverse = false;
          }
        }
      }

      reconRows.push({
        entityId: row.entityId,
        name: row.name,
        currentValue: row.aiPresencePct,
        priorValue: row.priorAiPresencePct ?? row.priorValue,
        delta: row.delta,
        deltaDisplay: row.deltaDisplay,
        currentRank: row.displayRank,
        priorRank: row.priorRank,
        rankDelta: row.rankDelta,
        rankDisplay: row.rankDisplay,
        movementState: row.movementState,
      });
    }

    const intents = Object.entries(payload.intentPresenceIndex || {});
    let intentOk = intents.length > 0;
    for (const [intentKey, row] of intents) {
      if (!row.deltaDisplay || row.deltaDisplay === "—") {
        // allow only if prior truly missing
        if (row.priorSubjectRatePct != null || row.priorValue != null) intentOk = false;
      }
      if (typeof row.delta === "number") {
        if (row.delta > 0) sawPositive = true;
        if (row.delta < 0) sawNegative = true;
        if (row.delta === 0) sawZero = true;
      }
      reconRows.push({
        scope: "intent",
        entityId: intentKey,
        currentValue: row.subjectRatePct,
        priorValue: row.priorSubjectRatePct,
        delta: row.delta,
        deltaDisplay: row.deltaDisplay,
      });
    }
    if (!intentOk) payloadComplete = false;

    const bpp = loadCustomerPublishedBrandPortfolio(propertyId);
    assert.ok(bpp?.ranking?.rows?.length, `${propertyId} bpp rows`);
    for (const row of bpp.ranking.rows) {
      assert.ok(row.canonicalEntityId, `${propertyId} bpp canonicalEntityId`);
      assert.ok(row.deltaDisplay && row.deltaDisplay !== "—", `${propertyId} bpp delta ${row.name}`);
      if (row.rankDelta > 0) sawRankImprove = true;
      if (row.rankDelta < 0) sawRankDecline = true;
      if (row.rankDelta === 0) sawUnchangedRank = true;
      reconRows.push({
        scope: "bpp",
        entityId: row.canonicalEntityId,
        deltaDisplay: row.deltaDisplay,
        priorRank: row.priorRank,
        currentRank: row.rank,
        rankLabel: row.rankLabel,
      });
    }

    // Membership states may only appear outside Top-N display — check full history ledger
    if (!sawMembership && movement.comparable) {
      for (const scope of Object.values(movement.byScope || {})) {
        for (const r of scope.rows || []) {
          if (
            r.state === "NEW_TO_RANKING" ||
            r.state === "EXITED" ||
            r.state === "RETURNED"
          ) {
            sawMembership = true;
            break;
          }
        }
        if (sawMembership) break;
      }
    }

    results.properties.push({
      propertyId,
      currentPeriodId,
      priorPeriodId,
      overallRows: overall.length,
      intentRows: intents.length,
      bppRows: bpp.ranking.rows.length,
      reconciliation: reconRows,
    });
  }

  // Gold cases must include real movement variety (not all unchanged)
  results.goldCases = {
    positiveDelta: sawPositive,
    negativeDelta: sawNegative,
    zeroDelta: sawZero,
    rankImprovement: sawRankImprove,
    rankDecline: sawRankDecline,
    unchangedRank: sawUnchangedRank,
    membershipNewOrExitedOrReturned: sawMembership,
  };
  assert.ok(sawPositive, "gold: positive delta");
  assert.ok(sawNegative, "gold: negative delta");
  assert.ok(sawZero, "gold: zero delta");
  assert.ok(sawRankImprove, "gold: rank improvement");
  assert.ok(sawRankDecline, "gold: rank decline");
  assert.ok(sawUnchangedRank, "gold: unchanged rank");
  assert.ok(sawMembership, "gold: NEW/EXITED/RETURNED membership in history");

  mark(HISTORICAL_RANK_USES_PERIOD_SPECIFIC_UNIVERSE, historyUniverse);
  mark(CUSTOMER_ROW_MOVEMENT_PAYLOAD_COMPLETE, payloadComplete);

  // LOCAL_PRODUCTION_ROW_MOVEMENT_PARITY — evaluated separately after deploy;
  // locally assert enrich path is wired into published-read-service.
  const readSrc = readFileSync(
    join(process.cwd(), "lib/ai-demand-positioning/published-read-service.js"),
    "utf8"
  );
  const parityWired =
    readSrc.includes("attachCoreRowLevelPriorComparisons") &&
    readFileSync(join(process.cwd(), "api/ai-demand-positioning.js"), "utf8").includes(
      "attachBppRowLevelPriorComparisons"
    );
  results.gates[LOCAL_PRODUCTION_ROW_MOVEMENT_PARITY] = parityWired
    ? "PASS_LOCAL_WIRED_PENDING_PRODUCTION_VERIFY"
    : "FAIL";
  assert.ok(parityWired, "LOCAL_PRODUCTION_ROW_MOVEMENT_PARITY wiring");

  // Smoke: format helpers
  assert.strictEqual(formatGovernedDeltaDisplay({ delta: 0, deltaUnit: DELTA_UNIT.PP }), "0.0 pp");
  assert.strictEqual(
    formatGovernedDeltaDisplay({
      delta: null,
      deltaUnit: DELTA_UNIT.PP,
      membershipState: ROW_MEMBERSHIP_STATE.NEW,
    }),
    "NEW"
  );
  assert.strictEqual(formatRankWithMovement({ currentRank: 2, priorRank: 4, rankDelta: 2 }), "#2 ↑2");
  assert.strictEqual(resolveRankDirection(2), RANK_DIRECTION.IMPROVED);
  assert.strictEqual(resolveRankDirection(-2), RANK_DIRECTION.DECLINED);

  // attach idempotency
  const once = attachCoreRowLevelPriorComparisons(
    "adp_waterstone_boca_raton",
    loadPublishedReport("adp_waterstone_boca_raton")
  );
  const twice = attachCoreRowLevelPriorComparisons("adp_waterstone_boca_raton", once);
  assert.strictEqual(
    twice.competitiveRankingByTerritory.byTerritory.overall.displayRows[0].deltaDisplay,
    once.competitiveRankingByTerritory.byTerritory.overall.displayRows[0].deltaDisplay
  );

  const outPath = join(OUT_DIR, "row-level-prior-run-movement-v1.json");
  writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log(JSON.stringify({ ok: true, outPath, gates: results.gates, goldCases: results.goldCases }, null, 2));
}

try {
  main();
} catch (err) {
  results.error = err.message;
  mkdirSync(OUT_DIR, { recursive: true });
  writeFileSync(join(OUT_DIR, "row-level-prior-run-movement-v1.json"), JSON.stringify(results, null, 2));
  console.error(err);
  process.exit(1);
}
