/**
 * GDI V37 — "Why isn't this opportunity showing?" internal search.
 * Read-only. Does not create opportunities.
 */
import { dispositionReasonMeta } from "./disposition-reason-codes-v1.js";
import { listDiscoveryCandidates } from "./discovery-candidate-ledger-v1.js";
import { RESEARCH_STATUS } from "./research-status-transitions-v1.js";
import { FOUR_HOTEL_CAMPAIGN_ID } from "./four-hotel-artifact-backfill-v1.js";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function scoreMatch(row, q) {
  const needle = q.toLowerCase();
  let score = 0;
  const fields = [
    row.organizationName,
    row.programmeName,
    row.originalResultTitle,
    row.opportunityId,
    row.discoveryId,
    ...(row.originalSourceUrls || []),
  ];
  for (const f of fields) {
    const s = text(f).toLowerCase();
    if (!s) continue;
    if (s === needle) score += 100;
    else if (s.includes(needle)) score += 40;
    else if (needle.includes(s) && s.length > 3) score += 20;
  }
  return score;
}

function notShowingReason(row) {
  if (row.customerFacing || row.researchStatus === RESEARCH_STATUS.CUSTOMER_FACING) {
    return {
      isCustomerFacing: true,
      code: "CUSTOMER_FACING_APPROVED",
      explanation: "This candidate is customer-facing.",
    };
  }
  if (row.disposition?.code) {
    return {
      isCustomerFacing: false,
      code: row.disposition.code,
      explanation: row.disposition.explanation,
      label: row.disposition.label || dispositionReasonMeta(row.disposition.code).label,
    };
  }
  if (row.researchStatus === RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION) {
    return {
      isCustomerFacing: false,
      code: "AWAITING_FOUNDER_PROMOTION",
      explanation:
        "Independently accepted Sales Ready but not yet founder-promoted to the customer dashboard.",
    };
  }
  if (row.researchStatus === RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH) {
    return {
      isCustomerFacing: false,
      code: "DEEP_RESEARCH_NOT_STARTED",
      explanation:
        "Discovered but never deep-researched — this is incomplete research, not a commercial rejection.",
    };
  }
  if (row.researchStatus === RESEARCH_STATUS.DUPLICATE) {
    return {
      isCustomerFacing: false,
      code: "DUPLICATE_SAME_EVENT_FAMILY",
      explanation: "Collapsed as a duplicate of another discovery row.",
    };
  }
  return {
    isCustomerFacing: false,
    code: "STAGED_INTERNAL_ONLY",
    explanation:
      "Not on the customer-facing dashboard. See researchStatus and disposition for detail.",
  };
}

/**
 * @param {object} opts
 * @param {string} opts.hotelId
 * @param {string} opts.q
 * @param {string} [opts.campaignId]
 */
export function whyIsntThisShowing({
  hotelId,
  q,
  campaignId = FOUR_HOTEL_CAMPAIGN_ID,
} = {}) {
  const query = text(q);
  if (!hotelId || !query) {
    return { ok: false, error: "hotelId_and_q_required", matches: [] };
  }
  const { candidates } = listDiscoveryCandidates(hotelId, { campaignId });
  const ranked = candidates
    .map((row) => ({ row, score: scoreMatch(row, query) }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 25);

  const matches = ranked.map(({ row, score }) => {
    const reason = notShowingReason(row);
    const missingEvidence = []
      .concat(row.structuredExaCandidateFields?.uncertainties || [])
      .concat(row.disposition?.evidence || [])
      .filter(Boolean)
      .slice(0, 8);
    return {
      score,
      discoveryId: row.discoveryId,
      opportunityId: row.opportunityId,
      organizationName: row.organizationName,
      programmeName: row.programmeName,
      workflowId: row.workflowId,
      powershell: row.powershell,
      exaRunId: row.exaRunId,
      queryTimestamp: row.queryTimestamp,
      originalSourceUrls: row.originalSourceUrls,
      researchStatus: row.researchStatus,
      statusHistory: row.statusHistory,
      disposition: row.disposition,
      deepResearched: Boolean(row.deepArtifactPath),
      deepArtifactPath: row.deepArtifactPath,
      staged: Boolean(row.staged),
      awaitingFounderPromotion: Boolean(row.awaitingFounderPromotion),
      customerFacing: Boolean(row.customerFacing),
      researchCostUsd:
        (Number(row.researchCostUsd) || 0) +
        (Number(row.deepResearchCostUsd) || 0),
      whyNotShowing: reason,
      missingEvidence,
      recommendedNextAction:
        row.disposition?.nextAction ||
        (row.researchStatus === RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH
          ? "Authorize deep research if commercially justified"
          : null),
      relatedPromotedOpportunityId:
        row.disposition?.supportingOpportunityId &&
        row.researchStatus === RESEARCH_STATUS.DUPLICATE
          ? row.disposition.supportingOpportunityId
          : null,
      provenance: row.provenance,
    };
  });

  return {
    ok: true,
    hotelId,
    query,
    matchCount: matches.length,
    matches,
    note: "Search is read-only and does not create or promote opportunities.",
  };
}
