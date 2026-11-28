/**
 * Lightweight commercial viability precheck — end of Stage 2 / before Stage 3 WHO/HOW.
 *
 * Not a sixth permanent research stage. Reuses FQO demand / fit / future pathway
 * rules via evaluateFinalizedQualifiedOpportunity — not a competing classifier.
 *
 * Does not auto-reject emerging demand solely for missing exact dates.
 * Historical demand requires a credible future pathway before paid contact research.
 */

import {
  evaluateFinalizedQualifiedOpportunity,
  FQO_GATE,
} from "./gdi-finalized-qualified-opportunity-contract-v1.js";
import { isHistoricalOnlyCustomerExclusion } from "../commercial-inventory/gdi-unified-commercial-inventory-contract-v1.js";

export const GDI_COMMERCIAL_VIABILITY_PRECHECK_V1 =
  "gdi-commercial-viability-precheck-v1";

export const COMMERCIAL_VIABILITY_DECISION = Object.freeze({
  PROCEED_TO_WHO_HOW: "PROCEED_TO_WHO_HOW",
  HOLD_CONTINUATION: "HOLD_CONTINUATION",
  DEFER_CONTACT_RESEARCH: "DEFER_CONTACT_RESEARCH",
  SKIP_PAID_CONTACT_RESEARCH: "SKIP_PAID_CONTACT_RESEARCH",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * @param {object} opportunity
 * @param {object} [opts]
 * @returns {{
 *   ok: boolean,
 *   decision: string,
 *   reasons: string[],
 *   gates: object,
 *   contactResearchWouldAdvance: boolean,
 *   preserveWhoHow: boolean,
 *   schema: string,
 * }}
 */
export function evaluateCommercialViabilityPrecheck(opportunity = {}, opts = {}) {
  const reasons = [];
  const fqo = evaluateFinalizedQualifiedOpportunity(opportunity, {
    historicalApprovedInventory: opts.historicalApprovedInventory === true,
  });
  const gates = {
    evidenceBackedDemand: fqo?.gates?.[FQO_GATE.EVIDENCE_BACKED_DEMAND] || null,
    hotelFit: fqo?.gates?.[FQO_GATE.HOTEL_GEO_COMMERCIAL_FIT] || null,
    futurePathway: fqo?.gates?.[FQO_GATE.FUTURE_SOURCING_PATHWAY] || null,
  };

  const historicalOnly = isHistoricalOnlyCustomerExclusion(opportunity, opts);
  const demandOk = gates.evidenceBackedDemand?.ok === true;
  const fitOk = gates.hotelFit?.ok === true;
  const futureOk = gates.futurePathway?.ok === true;

  const datesUnknown =
    !text(opportunity.eventStartDate || opportunity.startDate || opportunity.demandStartDate) &&
    !text(opportunity.expectedDemandPeriod || opportunity.demandPeriodLabel);

  // Emerging demand with unknown dates is allowed when demand+fit hold and not historical-only.
  const emergingOk =
    datesUnknown &&
    demandOk &&
    fitOk &&
    !historicalOnly &&
    opportunity.futureDemandPathway !== false;

  if (!demandOk) reasons.push("MISSING_OR_WEAK_DEMAND_EVIDENCE");
  if (!fitOk) reasons.push("MISSING_HOTEL_SPECIFIC_RELEVANCE");
  if (historicalOnly && !futureOk) {
    reasons.push("HISTORICAL_WITHOUT_CREDIBLE_FUTURE_PATHWAY");
  } else if (!futureOk && !emergingOk) {
    reasons.push("NO_ACTIONABLE_FUTURE_OR_RECURRING_PATHWAY");
  }

  const namedAbsent = !(
    text(opportunity.primaryContact?.name) ||
    text(opportunity.contactName) ||
    text(opportunity.whoHow?.directBuyer?.fullName)
  );
  const contactResearchWouldAdvance =
    namedAbsent &&
    demandOk &&
    fitOk &&
    (futureOk || emergingOk) &&
    !historicalOnly;

  let decision = COMMERCIAL_VIABILITY_DECISION.PROCEED_TO_WHO_HOW;
  if (historicalOnly && !futureOk) {
    decision = COMMERCIAL_VIABILITY_DECISION.SKIP_PAID_CONTACT_RESEARCH;
  } else if (!demandOk || !fitOk) {
    decision = COMMERCIAL_VIABILITY_DECISION.HOLD_CONTINUATION;
  } else if (!futureOk && !emergingOk) {
    decision = COMMERCIAL_VIABILITY_DECISION.DEFER_CONTACT_RESEARCH;
  } else if (!contactResearchWouldAdvance && !namedAbsent) {
    decision = COMMERCIAL_VIABILITY_DECISION.PROCEED_TO_WHO_HOW;
  }

  const ok =
    decision === COMMERCIAL_VIABILITY_DECISION.PROCEED_TO_WHO_HOW ||
    (decision === COMMERCIAL_VIABILITY_DECISION.PROCEED_TO_WHO_HOW &&
      contactResearchWouldAdvance);

  return {
    schema: GDI_COMMERCIAL_VIABILITY_PRECHECK_V1,
    ok:
      decision === COMMERCIAL_VIABILITY_DECISION.PROCEED_TO_WHO_HOW,
    decision,
    reasons,
    gates: {
      credibleFutureDemand: futureOk || emergingOk,
      hotelSpecificRelevance: fitOk,
      actionableCommercialPathway: demandOk && (futureOk || emergingOk),
      demandTimingOrRecurring: futureOk || emergingOk,
      emergingDatesUnknownAllowed: emergingOk,
      historicalOnly,
    },
    contactResearchWouldAdvance,
    preserveWhoHow: true,
    preserveAsContinuation:
      decision === COMMERCIAL_VIABILITY_DECISION.HOLD_CONTINUATION ||
      decision === COMMERCIAL_VIABILITY_DECISION.DEFER_CONTACT_RESEARCH ||
      decision === COMMERCIAL_VIABILITY_DECISION.SKIP_PAID_CONTACT_RESEARCH,
    fqoGateSnapshot: {
      [FQO_GATE.EVIDENCE_BACKED_DEMAND]: demandOk,
      [FQO_GATE.HOTEL_GEO_COMMERCIAL_FIT]: fitOk,
      [FQO_GATE.FUTURE_SOURCING_PATHWAY]: futureOk,
    },
    competingClassifierProhibited: true,
  };
}

export function batchCommercialViabilityPrecheck(opportunities = [], opts = {}) {
  const rows = (opportunities || []).map((opp) => {
    const id = text(opp.id || opp.opportunityId);
    return {
      opportunityId: id,
      organizationName: opp.organizationName || opp.organization || null,
      title: opp.title || null,
      ...evaluateCommercialViabilityPrecheck(opp, opts),
    };
  });
  const counts = {};
  for (const r of rows) {
    counts[r.decision] = (counts[r.decision] || 0) + 1;
  }
  return {
    schema: GDI_COMMERCIAL_VIABILITY_PRECHECK_V1,
    count: rows.length,
    counts,
    proceedToWhoHow: rows.filter(
      (r) => r.decision === COMMERCIAL_VIABILITY_DECISION.PROCEED_TO_WHO_HOW,
    ).length,
    rows,
  };
}
