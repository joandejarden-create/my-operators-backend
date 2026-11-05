/**
 * Human-readable opportunity research decision records.
 * Maps process-layer terminals to commercial dispositions without inventing facts.
 */

import {
  RESEARCH_TERMINAL,
  STOP_REASONS,
  HARD_TERMINAL_STOP_REASONS,
  RESUMABLE_STOP_REASONS,
} from "./research-constants-v1.js";
import {
  evaluateOpportunity,
  isActionableContactRoute,
  isHotelFacingAdvisory,
} from "./opportunity-research-supervisor-v1.js";
import {
  remainingPublicStrategiesForBlocker,
  publicStrategiesForBlocker,
} from "./research-planner-v1.js";

/** Commercial disposition vocabulary (reuse equivalents of existing process enums). */
export const COMMERCIAL_DISPOSITION = Object.freeze({
  QUALIFIED_ACTIONABLE: "QUALIFIED_ACTIONABLE",
  QUALIFIED_EXPLORATORY: "QUALIFIED_EXPLORATORY",
  PARTNERSHIP: "PARTNERSHIP",
  MARKET_INTELLIGENCE: "MARKET_INTELLIGENCE",
  LEGITIMATELY_REJECTED: "LEGITIMATELY_REJECTED",
  RESEARCH_BLOCKED: "RESEARCH_BLOCKED",
  BUDGET_BLOCKED: "BUDGET_BLOCKED",
  PROVIDER_BLOCKED: "PROVIDER_BLOCKED",
  FURTHER_RESEARCH_AVAILABLE: "FURTHER_RESEARCH_AVAILABLE",
  IN_PROGRESS: "IN_PROGRESS",
});

const REJECT_STOPS = new Set([
  STOP_REASONS.LEGITIMATELY_REJECTED,
  STOP_REASONS.NO_FUTURE_DEMAND_EVIDENCE,
  STOP_REASONS.EVENT_CANCELLED,
  STOP_REASONS.DATES_ALREADY_PAST,
  STOP_REASONS.OUTSIDE_HOTEL_MARKET,
  STOP_REASONS.INSUFFICIENT_DEMAND_SCALE,
  STOP_REASONS.ALREADY_PLACED_OR_CONTRACTED,
  STOP_REASONS.ORGANIZATION_INACTIVE,
  STOP_REASONS.HOTEL_CATCHMENT_MISMATCH,
  STOP_REASONS.BELOW_COMMERCIAL_RELEVANCE,
  STOP_REASONS.NOT_APPLICABLE,
  STOP_REASONS.DUPLICATE_OPPORTUNITY,
]);

/**
 * Map opportunity process state → commercial disposition.
 */
export function mapCommercialDisposition(opportunity = {}) {
  const status = opportunity.researchStatus;
  const stop = opportunity.stopReason;
  const type = String(opportunity.opportunityType || "").toUpperCase();

  if (status === RESEARCH_TERMINAL.IN_PROGRESS) {
    return COMMERCIAL_DISPOSITION.IN_PROGRESS;
  }
  if (status === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT) {
    return COMMERCIAL_DISPOSITION.QUALIFIED_ACTIONABLE;
  }
  if (status === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT) {
    return COMMERCIAL_DISPOSITION.QUALIFIED_EXPLORATORY;
  }
  if (type === "PARTNERSHIP" || opportunity.classification === "PARTNERSHIP") {
    return COMMERCIAL_DISPOSITION.PARTNERSHIP;
  }
  if (
    type === "MARKET_INTELLIGENCE" ||
    opportunity.classification === "MARKET_INTELLIGENCE" ||
    opportunity.customerVisible === false &&
      opportunity.prospectHotelFacing === false &&
      stop === STOP_REASONS.BELOW_COMMERCIAL_RELEVANCE
  ) {
    if (type === "MARKET_INTELLIGENCE") {
      return COMMERCIAL_DISPOSITION.MARKET_INTELLIGENCE;
    }
  }
  if (stop === STOP_REASONS.BUDGET_BLOCKED || stop === STOP_REASONS.BUDGET_LIMIT_REACHED) {
    return COMMERCIAL_DISPOSITION.BUDGET_BLOCKED;
  }
  if (stop === STOP_REASONS.PROVIDER_BLOCKED) {
    return COMMERCIAL_DISPOSITION.PROVIDER_BLOCKED;
  }
  if (RESUMABLE_STOP_REASONS.includes(stop)) {
    return COMMERCIAL_DISPOSITION.FURTHER_RESEARCH_AVAILABLE;
  }
  if (stop === STOP_REASONS.RESEARCH_EXHAUSTED) {
    return COMMERCIAL_DISPOSITION.RESEARCH_BLOCKED;
  }
  if (REJECT_STOPS.has(stop) || HARD_TERMINAL_STOP_REASONS.includes(stop)) {
    return COMMERCIAL_DISPOSITION.LEGITIMATELY_REJECTED;
  }
  return COMMERCIAL_DISPOSITION.IN_PROGRESS;
}

function evidenceUrls(opportunity = {}) {
  const urls = [];
  for (const e of opportunity.evidence || []) {
    if (e?.url) urls.push(e.url);
  }
  for (const s of opportunity.sources || []) {
    if (s?.url) urls.push(s.url);
    else if (typeof s === "string" && s.startsWith("http")) urls.push(s);
  }
  return [...new Set(urls)].slice(0, 12);
}

function attemptSummary(opportunity = {}) {
  return (opportunity.researchAttempts || []).map((a) => ({
    blocker: a.blocker || null,
    strategy: a.strategy || null,
    provider: a.provider || null,
    disposition: a.disposition || null,
    newMaterialEvidence: Boolean(a.newMaterialEvidence),
    costUsd: a.costUsd ?? a.actualCostUsd ?? null,
    providerRunId: a.providerRunId || a.jobId || null,
    sourceFamilies: a.sourceFamilies || [],
  }));
}

/**
 * Build an auditable decision record for one opportunity.
 */
export function buildOpportunityResearchDecisionRecord(opportunity = {}, opts = {}) {
  const evalResult = evaluateOpportunity(opportunity, opts);
  const disposition = mapCommercialDisposition(opportunity);
  const contact = opportunity.contact || {};
  const actionable = disposition === COMMERCIAL_DISPOSITION.QUALIFIED_ACTIONABLE;
  const exploratory =
    disposition === COMMERCIAL_DISPOSITION.QUALIFIED_EXPLORATORY;
  const hotelFacing = actionable || exploratory || isHotelFacingAdvisory(opportunity);

  const blockerKey =
    evalResult.blockers?.[0]?.key ||
    (opportunity.researchAttempts || []).slice(-1)[0]?.blocker ||
    "future_demand";
  const remainingPublic = remainingPublicStrategiesForBlocker(
    opportunity,
    blockerKey
  );
  const allPublic = publicStrategiesForBlocker(blockerKey);

  const whyPursue = hotelFacing
    ? [
        opportunity.summaryWhyHotel ||
          opportunity.hotelFitRationale ||
          opportunity.proposedHotelUse ||
          null,
        opportunity.localConnection || null,
        opportunity.whyNow || null,
      ]
        .filter(Boolean)
        .join(" ")
    : null;

  const whyNot = !hotelFacing
    ? [
        opportunity.stopDetail?.rationale ||
          opportunity.materialLimitations ||
          null,
        opportunity.stopReason
          ? `Process stop: ${opportunity.stopReason}`
          : null,
        remainingPublic.length
          ? `Untried public strategies remain: ${remainingPublic.join(", ")}`
          : `Public strategies tried: ${allPublic.filter((s) =>
              (opportunity.researchAttempts || []).some((a) => a.strategy === s)
            ).join(", ") || "none"}`,
      ]
        .filter(Boolean)
        .join(" ")
    : null;

  const record = {
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    organizationName: opportunity.organizationName || opportunity.name || null,
    primarySegment:
      opportunity.segment ||
      opportunity.researchSegment ||
      opportunity.primarySegment ||
      null,
    secondarySegments: opportunity.secondarySegments || [],
    commercialDisposition: disposition,
    researchStatus: opportunity.researchStatus || null,
    stopReason: opportunity.stopReason || null,
    publicationEligible: Boolean(
      opportunity.customerVisible === true ||
        opportunity.prospectHotelFacing === true
    ),
    readyPacketComplete: Boolean(opportunity.salesPacket?.ready === true),
    whyPursueThisOpportunity: whyPursue,
    whyIsThisHotelRelevant:
      opportunity.hotelFitRationale ||
      opportunity.summaryWhyHotel ||
      opportunity.hotelFit?.rationale ||
      null,
    whatSupportsDemandHypothesis:
      opportunity.futureDemand?.summary ||
      opportunity.localConnection ||
      opportunity.proposedHotelUse ||
      null,
    whoShouldBeContacted: hotelFacing
      ? {
          route: isActionableContactRoute(contact)
            ? {
                email: contact.email || opportunity.contactEmail || null,
                phone: contact.phone || opportunity.contactPhone || null,
                url: contact.url || opportunity.contactUrl || null,
              }
            : null,
          namedBuyer:
            contact.name ||
            opportunity.primaryContactName ||
            null,
          role:
            contact.role ||
            opportunity.primaryContactRole ||
            null,
        }
      : null,
    recommendedNextAction: hotelFacing
      ? opportunity.recommendedAction ||
        "Introduce hotel capabilities and request the booking authority for applicable programmes."
      : remainingPublic.length
        ? `Resume research with: ${remainingPublic[0]}`
        : opportunity.recommendedAction ||
          "No further research justified under current authorization.",
    whyNotSuitable: whyNot,
    evidenceMissing: (evalResult.blockers || []).map((b) => ({
      key: b.key,
      reason: b.reason || null,
    })),
    researchAttempted: attemptSummary(opportunity),
    remainingJustifiedStrategies: remainingPublic,
    sourceUrls: evidenceUrls(opportunity),
    blockers: evalResult.blockers || [],
    evaluatedAt: new Date().toISOString(),
  };

  return record;
}
