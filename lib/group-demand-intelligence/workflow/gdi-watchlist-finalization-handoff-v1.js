/**
 * WATCHLIST → finalization handoff helpers for the EXISTING Full E2E chain.
 *
 * Fixes proven carry-forward defects without lowering qualification standards:
 * - Stale discovery unresolvedQuestions after verified buyers exist
 * - Offline demand-thesis recovery from existing evidence (no invention)
 * - Guest Rooms / Local Catering / Both classification attach
 * - Publication-REVIEW readiness (founder-staged) vs customerPublicationReady
 * - Read-only providerRunId reconstruction from ORS (never mutates ledgers)
 *
 * Does not publish. Does not invent demand. Does not spend.
 */

import { applyDemandThesisRecoveryOverlay } from "../demand-thesis-recovery-v1.js";
import { attachCommercialDemandTypeClassification } from "../classify-opportunity-commercial-demand-type-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";

export const GDI_WATCHLIST_FINALIZATION_HANDOFF_V1 =
  "gdi-watchlist-finalization-handoff-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function looksNamedPerson(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/organization contact|contact desk|info@|sales@|front desk/i.test(n)) {
    return false;
  }
  return /[A-Za-z]/.test(n);
}

/**
 * Clear stale discovery gaps once contact research verified a relevant buyer.
 * Does not invent contacts.
 */
export function reconcileUnresolvedQuestionsAfterContact(opportunity = {}) {
  const prior = Array.isArray(opportunity.unresolvedQuestions)
    ? [...opportunity.unresolvedQuestions]
    : [];
  const next = new Set(prior);
  const primaryName =
    opportunity.primaryContact?.name ||
    opportunity.contactName ||
    opportunity.namedStakeholders?.[0]?.name ||
    opportunity.namedStakeholdersFound?.[0]?.name ||
    null;
  const relevance =
    opportunity.primaryContact?.contactRelevanceLabel ||
    opportunity.primaryContact?.whyThisContact ||
    "";
  const hasVerifiedBuyer =
    looksNamedPerson(primaryName) &&
    /CONTACT_RELEVANT|DIRECT_BUYER|RELEVANT_COMMERCIAL|hotel_sourcing_decision_maker|CONTRACTING_AUTHORITY_VERIFIED/i.test(
      `${relevance} ${opportunity.primaryContact?.commercialClass || ""} ${opportunity.primaryContact?.contractingAuthorityStatus || ""} ${opportunity.primaryContact?.title || ""} ${opportunity.primaryContact?.role || ""}`
    );

  if (hasVerifiedBuyer) {
    next.delete("named_buyer_not_verified");
  }

  const sourcing =
    opportunity.venueSourcingStatus ||
    opportunity.pursuitSourcingStatus ||
    opportunity.sourcingStatus ||
    "";
  if (
    /OPEN|UNCONFIRMED|FUTURE_CYCLE|OVERFLOW|COMPETITOR|PARTNERSHIP/i.test(
      String(sourcing)
    )
  ) {
    // Open / future-cycle pursuits are valid — do not keep discovery-era
    // "sourcing_accessibility_unconfirmed" as if no pathway exists.
    next.delete("sourcing_accessibility_unconfirmed");
  }

  return {
    opportunity: {
      ...opportunity,
      unresolvedQuestions: [...next],
      unresolvedQuestionsReconciledAt: new Date().toISOString(),
      unresolvedQuestionsPrior: prior,
    },
    cleared: prior.filter((q) => !next.has(q)),
    remaining: [...next],
    hasVerifiedBuyer,
  };
}

/**
 * Apply offline demand-thesis recovery overlay in-memory (no storage invent).
 */
export function applyOfflineDemandThesisRecoveryInChain(opportunity = {}) {
  const applied = applyDemandThesisRecoveryOverlay(opportunity);
  return {
    opportunity: applied.opportunity,
    applied: applied.applied === true,
    category: applied.recovery?.category || null,
    afterDemandOk: applied.recovery?.afterDemandOk === true,
    recovery: applied.recovery,
  };
}

export function attachCommercialDemandTypeInChain(opportunity = {}, opts = {}) {
  return attachCommercialDemandTypeClassification(opportunity, opts);
}

/**
 * Publication-REVIEW readiness: complete body + sales-ready eligibility.
 * Never authorizes customer publication (founder auth still required).
 */
export function evaluatePublicationReviewReadiness(opportunity = {}, opts = {}) {
  const hotel = opts.hotel || {
    hotelId: opportunity.hotelId,
    displayName: opportunity.hotelName || "",
  };
  const eligibility =
    opts.eligibilityResult ||
    opportunity.commercialEligibilityContractV1 ||
    evaluateSharedCommercialEligibility(opportunity, { hotel, ...opts });

  const sections = opportunity.tenSectionDrawer?.sections || null;
  const sectionCount = sections ? Object.keys(sections).length : 0;
  const completePersistedContent =
    opportunity.tenSectionDrawer?.completePersistedContent === true ||
    sectionCount >= 8;

  const salesReady = eligibility?.salesReady === true;
  const publicationReviewReady =
    salesReady === true && completePersistedContent === true;

  const blockers = [];
  if (!salesReady) {
    blockers.push(
      ...(eligibility?.gaps || ["COMMERCIAL_ELIGIBILITY_NOT_SALES_READY"])
    );
  }
  if (!completePersistedContent) {
    blockers.push("TEN_SECTION_CONTENT_INCOMPLETE");
  }

  return {
    version: GDI_WATCHLIST_FINALIZATION_HANDOFF_V1,
    publicationReviewReady,
    customerPublicationReady: false,
    founderAuthorizationRequired: true,
    salesReady,
    completePersistedContent,
    sectionCount,
    commercialEligibilityStatus: eligibility?.status || null,
    commercialDemandType: opportunity.commercialDemandType || null,
    blockers,
    note:
      "publicationReviewReady stages a complete body for founder review; it does not publish or set customerPublicationReady.",
  };
}

/**
 * Reconstruct providerRunId from opportunity/ORS fields for audit.
 * Does not mutate historical budget ledgers.
 */
export function reconstructProviderRunIdFromOpportunity(opportunity = {}) {
  const providerRunId =
    opportunity.exaAgentRouting?.providerRunId ||
    opportunity.researchJobId ||
    opportunity.exaRunId ||
    opportunity.discoveryProviderRunId ||
    null;
  return {
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    providerRunId,
    source: providerRunId
      ? opportunity.exaAgentRouting?.providerRunId
        ? "exaAgentRouting.providerRunId"
        : opportunity.researchJobId
          ? "researchJobId"
          : opportunity.exaRunId
            ? "exaRunId"
            : "discoveryProviderRunId"
      : null,
    ledgerMutation: false,
    note: "Read-only reconstruction for settlement representation / audit — historical ledgers remain immutable.",
  };
}

export default {
  GDI_WATCHLIST_FINALIZATION_HANDOFF_V1,
  reconcileUnresolvedQuestionsAfterContact,
  applyOfflineDemandThesisRecoveryInChain,
  attachCommercialDemandTypeInChain,
  evaluatePublicationReviewReadiness,
  reconstructProviderRunIdFromOpportunity,
};
