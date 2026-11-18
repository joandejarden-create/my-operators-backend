/**
 * Per-segment stage-separated Exa call / USD budget enforcement V1.
 *
 * Discovery and deep research have independent call caps so a deep pass cannot
 * push a segment past its discovery table cap (PR #382 scaling 5 > 4 defect).
 *
 * Zero paid I/O. Pure accounting — call sites must consult before provider use.
 */

export const GDI_SEGMENT_STAGE_CALL_BUDGET_V1 =
  "gdi-segment-stage-call-budget-v1";

export const BUDGET_STAGE = Object.freeze({
  DISCOVERY: "DISCOVERY",
  DEEP_RESEARCH: "DEEP_RESEARCH",
});

function num(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function stageKey(stage) {
  const s = String(stage || "").toUpperCase();
  if (s === BUDGET_STAGE.DEEP_RESEARCH || s === "DEEP") {
    return BUDGET_STAGE.DEEP_RESEARCH;
  }
  return BUDGET_STAGE.DISCOVERY;
}

/**
 * @param {object} opts
 * @param {number} [opts.globalMaxCalls]
 * @param {number} [opts.globalMaxUsd]
 * @param {Record<string,{maxDiscoveryCalls?:number,maxDeepCalls?:number,maxDiscoveryUsd?:number,maxDeepUsd?:number,maxUsd?:number}>} [opts.segments]
 */
export function createSegmentStageCallBudget(opts = {}) {
  return {
    version: GDI_SEGMENT_STAGE_CALL_BUDGET_V1,
    globalMaxCalls: num(opts.globalMaxCalls, Infinity),
    globalMaxUsd: num(opts.globalMaxUsd, Infinity),
    segments: { ...(opts.segments || {}) },
    operations: [],
    blockedOperations: [],
    createdAt: new Date().toISOString(),
  };
}

export function segmentStageSpend(budget, segmentId, stage = null) {
  const ops = (budget?.operations || []).filter((op) => {
    if (String(op.segmentId || "") !== String(segmentId || "")) return false;
    if (stage && stageKey(op.stage) !== stageKey(stage)) return false;
    return true;
  });
  return {
    calls: ops.length,
    spentUsd: ops.reduce((n, op) => n + num(op.actualCostUsd || op.quotedMaxUsd), 0),
  };
}

export function globalSpend(budget) {
  const ops = budget?.operations || [];
  return {
    calls: ops.length,
    spentUsd: ops.reduce((n, op) => n + num(op.actualCostUsd || op.quotedMaxUsd), 0),
  };
}

function capsFor(budget, segmentId, stage) {
  const row = budget.segments?.[segmentId] || {};
  const st = stageKey(stage);
  if (st === BUDGET_STAGE.DEEP_RESEARCH) {
    return {
      maxCalls: num(
        row.maxDeepCalls != null ? row.maxDeepCalls : row.maxCalls,
        Infinity,
      ),
      maxUsd: num(
        row.maxDeepUsd != null
          ? row.maxDeepUsd
          : row.maxUsd != null
            ? row.maxUsd
            : Infinity,
        Infinity,
      ),
    };
  }
  return {
    maxCalls: num(
      row.maxDiscoveryCalls != null ? row.maxDiscoveryCalls : row.maxCalls,
      Infinity,
    ),
    maxUsd: num(
      row.maxDiscoveryUsd != null
        ? row.maxDiscoveryUsd
        : row.maxUsd != null
          ? row.maxUsd
          : Infinity,
      Infinity,
    ),
  };
}

/**
 * Gate a proposed metered call. Does not mutate the budget.
 */
export function canAffordSegmentStageCall(
  budget,
  {
    segmentId,
    stage = BUDGET_STAGE.DISCOVERY,
    quotedMaxUsd = 0,
  } = {},
) {
  if (!budget || budget.version !== GDI_SEGMENT_STAGE_CALL_BUDGET_V1) {
    return { ok: false, reason: "BUDGET_MISSING_OR_VERSION" };
  }
  if (!segmentId) {
    return { ok: false, reason: "SEGMENT_ID_REQUIRED" };
  }
  const st = stageKey(stage);
  const caps = capsFor(budget, segmentId, st);
  const stageSpend = segmentStageSpend(budget, segmentId, st);
  const all = globalSpend(budget);
  const quote = num(quotedMaxUsd);

  if (!(Number.isFinite(caps.maxCalls) || caps.maxCalls === Infinity)) {
    return { ok: false, reason: "INVALID_CALL_CAP" };
  }
  if (stageSpend.calls + 1 > caps.maxCalls + 1e-9) {
    return {
      ok: false,
      reason: "SEGMENT_STAGE_CALL_CAP_REACHED",
      segmentId,
      stage: st,
      calls: stageSpend.calls,
      maxCalls: caps.maxCalls,
    };
  }
  if (quote > 0 && stageSpend.spentUsd + quote > caps.maxUsd + 1e-9) {
    return {
      ok: false,
      reason: "SEGMENT_STAGE_USD_CAP_REACHED",
      segmentId,
      stage: st,
      spentUsd: stageSpend.spentUsd,
      maxUsd: caps.maxUsd,
      proposedMax: quote,
    };
  }
  if (all.calls + 1 > budget.globalMaxCalls + 1e-9) {
    return {
      ok: false,
      reason: "GLOBAL_CALL_CAP_REACHED",
      calls: all.calls,
      maxCalls: budget.globalMaxCalls,
    };
  }
  if (quote > 0 && all.spentUsd + quote > budget.globalMaxUsd + 1e-9) {
    return {
      ok: false,
      reason: "GLOBAL_USD_CAP_REACHED",
      spentUsd: all.spentUsd,
      maxUsd: budget.globalMaxUsd,
      proposedMax: quote,
    };
  }
  return {
    ok: true,
    segmentId,
    stage: st,
    stageCalls: stageSpend.calls,
    stageMaxCalls: caps.maxCalls,
    globalCalls: all.calls,
    proposedMax: quote,
  };
}

/**
 * Record a completed (or blocked) call. Mutates budget.
 */
export function settleSegmentStageCall(
  budget,
  {
    segmentId,
    stage = BUDGET_STAGE.DISCOVERY,
    quotedMaxUsd = 0,
    actualCostUsd = 0,
    ok = true,
    reason = null,
    meta = null,
  } = {},
) {
  const gate = canAffordSegmentStageCall(budget, {
    segmentId,
    stage,
    quotedMaxUsd,
  });
  if (!gate.ok && ok !== false) {
    budget.blockedOperations.push({
      at: new Date().toISOString(),
      segmentId,
      stage: stageKey(stage),
      reason: gate.reason,
      quotedMaxUsd: num(quotedMaxUsd),
      meta,
    });
    return { ok: false, ...gate };
  }
  if (ok === false) {
    budget.blockedOperations.push({
      at: new Date().toISOString(),
      segmentId,
      stage: stageKey(stage),
      reason: reason || "CALL_FAILED",
      quotedMaxUsd: num(quotedMaxUsd),
      meta,
    });
    return { ok: false, reason: reason || "CALL_FAILED" };
  }
  budget.operations.push({
    at: new Date().toISOString(),
    segmentId,
    stage: stageKey(stage),
    quotedMaxUsd: num(quotedMaxUsd),
    actualCostUsd: num(actualCostUsd),
    meta,
  });
  return { ok: true, operations: budget.operations.length };
}

/**
 * Replay PR #382 scaling defect: 4 discovery + 1 deep must not count as 5/4
 * against the discovery cap when stages are separated.
 */
export function demonstrateDiscoveryDeepSeparationFixture() {
  const budget = createSegmentStageCallBudget({
    globalMaxCalls: 30,
    globalMaxUsd: 25,
    segments: {
      corporate_accounts_scaling: {
        maxDiscoveryCalls: 4,
        maxDeepCalls: 2,
        maxDiscoveryUsd: 5,
        maxDeepUsd: 5,
      },
    },
  });
  const discoveries = [];
  for (let i = 0; i < 4; i += 1) {
    discoveries.push(
      settleSegmentStageCall(budget, {
        segmentId: "corporate_accounts_scaling",
        stage: BUDGET_STAGE.DISCOVERY,
        quotedMaxUsd: 1,
        actualCostUsd: 1,
      }),
    );
  }
  const fifthDiscovery = canAffordSegmentStageCall(budget, {
    segmentId: "corporate_accounts_scaling",
    stage: BUDGET_STAGE.DISCOVERY,
    quotedMaxUsd: 1,
  });
  const deepOk = settleSegmentStageCall(budget, {
    segmentId: "corporate_accounts_scaling",
    stage: BUDGET_STAGE.DEEP_RESEARCH,
    quotedMaxUsd: 1,
    actualCostUsd: 1,
  });
  const discoverySpend = segmentStageSpend(
    budget,
    "corporate_accounts_scaling",
    BUDGET_STAGE.DISCOVERY,
  );
  const deepSpend = segmentStageSpend(
    budget,
    "corporate_accounts_scaling",
    BUDGET_STAGE.DEEP_RESEARCH,
  );
  const totalOps = budget.operations.length;
  return {
    version: GDI_SEGMENT_STAGE_CALL_BUDGET_V1,
    discoverySettled: discoveries.every((r) => r.ok === true),
    fifthDiscoveryBlocked: fifthDiscovery.ok === false,
    fifthDiscoveryReason: fifthDiscovery.reason || null,
    deepAllowedAfterFourDiscovery: deepOk.ok === true,
    discoveryCalls: discoverySpend.calls,
    deepCalls: deepSpend.calls,
    totalOperations: totalOps,
    // Legacy scorecard bug: counting all ops against discovery maxCalls.
    legacySharedCapWouldBreach: totalOps > 4,
    correctedCapRespected:
      discoverySpend.calls <= 4 &&
      deepSpend.calls <= 2 &&
      fifthDiscovery.ok === false &&
      deepOk.ok === true,
  };
}

export default {
  GDI_SEGMENT_STAGE_CALL_BUDGET_V1,
  BUDGET_STAGE,
  createSegmentStageCallBudget,
  segmentStageSpend,
  globalSpend,
  canAffordSegmentStageCall,
  settleSegmentStageCall,
  demonstrateDiscoveryDeepSeparationFixture,
};
