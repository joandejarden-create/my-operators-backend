/**
 * GDI V37 — Research Funnel aggregates from discovery ledger.
 * Cumulative stage denominators are separate from mutually exclusive status totals.
 */
import {
  FUNNEL_STAGE,
  MUTUALLY_EXCLUSIVE_STATUS_BUCKETS,
  RESEARCH_STATUS,
} from "./research-status-transitions-v1.js";

const DEEP_DONE = new Set([
  RESEARCH_STATUS.RESEARCH_CONTINUATION,
  RESEARCH_STATUS.WATCH,
  RESEARCH_STATUS.ACCOUNT_DEVELOPMENT,
  RESEARCH_STATUS.SALES_READY_INTERNAL,
  RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION,
  RESEARCH_STATUS.CUSTOMER_FACING,
  RESEARCH_STATUS.REJECTED,
  RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE,
]);

const SALES_READY_SET = new Set([
  RESEARCH_STATUS.SALES_READY_INTERNAL,
  RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION,
  RESEARCH_STATUS.CUSTOMER_FACING,
]);

const AWAITING_DEEP = new Set([
  RESEARCH_STATUS.SCREENED_ELIGIBLE,
  RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH,
  RESEARCH_STATUS.DISCOVERED_UNSCREENED,
  RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE,
]);

export function aggregateResearchFunnel(candidates = []) {
  const rows = Array.isArray(candidates) ? candidates : [];
  const statusTotals = Object.fromEntries(
    MUTUALLY_EXCLUSIVE_STATUS_BUCKETS.map((s) => [s, 0]),
  );
  for (const r of rows) {
    const s = r.researchStatus || "UNKNOWN";
    if (statusTotals[s] == null) statusTotals[s] = 0;
    statusTotals[s] += 1;
  }

  const uniqueFamily = new Set();
  for (const r of rows) {
    if (r.researchStatus === RESEARCH_STATUS.DUPLICATE) continue;
    uniqueFamily.add(r.dedupeFamilyKey || r.discoveryId);
  }

  const cumulative = {
    [FUNNEL_STAGE.EXA_DISCOVERIES]: rows.length,
    [FUNNEL_STAGE.SUCCESSFULLY_PARSED]: rows.filter(
      (r) => r.organizationName || r.normalizedOrganization,
    ).length,
    [FUNNEL_STAGE.UNIQUE_CANDIDATES]: uniqueFamily.size,
    [FUNNEL_STAGE.DUPLICATES]: rows.filter(
      (r) => r.researchStatus === RESEARCH_STATUS.DUPLICATE,
    ).length,
    [FUNNEL_STAGE.SCREENED_OUT]: rows.filter(
      (r) => r.researchStatus === RESEARCH_STATUS.SCREENED_EXCLUDED,
    ).length,
    [FUNNEL_STAGE.ELIGIBLE_FOR_DEEP]: rows.filter(
      (r) =>
        r.researchStatus !== RESEARCH_STATUS.DUPLICATE &&
        r.researchStatus !== RESEARCH_STATUS.SCREENED_EXCLUDED &&
        r.researchStatus !== RESEARCH_STATUS.DISCOVERED_UNSCREENED,
    ).length,
    [FUNNEL_STAGE.DEEP_RESEARCHED]: rows.filter(
      (r) => r.deepArtifactPath || DEEP_DONE.has(r.researchStatus),
    ).length,
    [FUNNEL_STAGE.AWAITING_DEEP]: rows.filter(
      (r) =>
        AWAITING_DEEP.has(r.researchStatus) &&
        !r.deepArtifactPath &&
        r.researchStatus !== RESEARCH_STATUS.DUPLICATE,
    ).length,
    [FUNNEL_STAGE.RESEARCH_CONTINUATION]: rows.filter(
      (r) => r.researchStatus === RESEARCH_STATUS.RESEARCH_CONTINUATION,
    ).length,
    [FUNNEL_STAGE.WATCH]: rows.filter(
      (r) => r.researchStatus === RESEARCH_STATUS.WATCH,
    ).length,
    accountDevelopment: rows.filter(
      (r) => r.researchStatus === RESEARCH_STATUS.ACCOUNT_DEVELOPMENT,
    ).length,
    [FUNNEL_STAGE.SALES_READY_INDEPENDENT]: rows.filter((r) =>
      SALES_READY_SET.has(r.researchStatus),
    ).length,
    [FUNNEL_STAGE.AWAITING_PROMOTION]: rows.filter(
      (r) =>
        r.researchStatus === RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION ||
        r.awaitingFounderPromotion === true,
    ).length,
    [FUNNEL_STAGE.CUSTOMER_FACING]: rows.filter(
      (r) =>
        r.researchStatus === RESEARCH_STATUS.CUSTOMER_FACING ||
        r.customerFacing === true,
    ).length,
    [FUNNEL_STAGE.REJECTED]: rows.filter(
      (r) => r.researchStatus === RESEARCH_STATUS.REJECTED,
    ).length,
  };

  const dispositionCounts = {};
  for (const r of rows) {
    const code = r.disposition?.code;
    if (!code) continue;
    dispositionCounts[code] = (dispositionCounts[code] || 0) + 1;
  }

  const researchCostUsd = rows.reduce(
    (sum, r) =>
      sum +
      (Number(r.researchCostUsd) || 0) +
      (Number(r.deepResearchCostUsd) || 0),
    0,
  );

  return {
    version: "gdi-research-funnel-aggregate-v1",
    totalCandidates: rows.length,
    cumulativeStages: cumulative,
    mutuallyExclusiveStatusTotals: statusTotals,
    dispositionCounts,
    researchCostUsd: Number(researchCostUsd.toFixed(4)),
    note:
      "cumulativeStages are stage denominators (not mutually exclusive). mutuallyExclusiveStatusTotals sum to totalCandidates.",
  };
}

export function funnelReconciles(funnel) {
  const sum = Object.values(funnel.mutuallyExclusiveStatusTotals || {}).reduce(
    (a, b) => a + Number(b || 0),
    0,
  );
  return {
    ok: sum === funnel.totalCandidates,
    statusSum: sum,
    totalCandidates: funnel.totalCandidates,
  };
}
