/**
 * Autonomous lead-following V2 — evaluate related-entity leads by commercial VoI
 * and select the strongest authorized follow-ups (does not invent independence).
 */

import {
  proposeRelatedEntityFollowUps,
  RESEARCH_INTENT,
  COMMERCIAL_RESEARCH_DIMENSION,
  RELATIONSHIP_VERIFICATION,
  isIndependentPublisherUrl,
} from "./commercial-source-selection-v1.js";

export const AUTONOMOUS_LEAD_FOLLOWING_VERSION = "gdi-autonomous-lead-following-v2";

const IRRELEVANT = /\b(?:stock\s*photo|careers?\s*page|privacy\s*policy|cookie|terms\s*of\s*service)\b/i;

/**
 * Score a related-entity lead for hotel-specific commercial value.
 */
export function scoreRelatedEntityLead(lead = {}, { targetHotel = "", remainingBudgetUsd = Infinity } = {}) {
  let score = 0;
  const reasons = [];
  if (!lead || !lead.entityName) return { score: 0, pursue: false, reasons: ["missing_entity"] };

  if (lead.entityType === "hotel_or_accommodation_partner") {
    score += 20;
    reasons.push("accommodation_partner_lead");
  }
  if (lead.entityType === "programme") {
    score += 14;
    reasons.push("programme_lead");
  }
  if (lead.entityType === "client_or_partner_organization") {
    score += 12;
    reasons.push("client_partner_lead");
  }
  if (lead.commercialDimension === COMMERCIAL_RESEARCH_DIMENSION.ACCOMMODATION_PARTNERS) {
    score += 8;
  }
  if (targetHotel && new RegExp(String(targetHotel).split(/\s+/).slice(0, 2).join("\\s+"), "i").test(lead.entityName || "")) {
    score += 10;
    reasons.push("names_target_hotel");
  }
  if (lead.suggestedUrl && lead.suggestedUrlIndependentPublisher) {
    score += 15;
    reasons.push("independent_url_available");
  } else if (lead.suggestedUrl) {
    score += 4;
    reasons.push("url_available_may_be_same_org");
  } else {
    score -= 5;
    reasons.push("no_url_yet");
  }
  if (lead.independentlyVerified === true) {
    score -= 30;
    reasons.push("already_verified_skip");
  }
  if (IRRELEVANT.test(`${lead.entityName} ${lead.note || ""}`)) {
    score -= 50;
    reasons.push("commercially_irrelevant");
  }
  if (!(remainingBudgetUsd > 0.001)) {
    score -= 100;
    reasons.push("budget_exhausted");
  }

  const pursue = score >= 18 && lead.independentlyVerified !== true;
  return { score, pursue, reasons };
}

/**
 * Record leads from a page, score them, and return ordered pursuit queue.
 */
export function planAutonomousLeadFollowing({
  findingsText = "",
  sourceUrl = "",
  organizationHost = "",
  organizationName = "",
  investigatedUrls = [],
  researchIntent = RESEARCH_INTENT.OPPORTUNITY_DISCOVERY,
  entityUrlHints = {},
  targetHotel = "",
  remainingBudgetUsd = Infinity,
  maxPursue = 3,
} = {}) {
  const proposed = proposeRelatedEntityFollowUps({
    findingsText,
    sourceUrl,
    organizationHost,
    organizationName,
    investigatedUrls,
    researchIntent,
    entityUrlHints,
  });

  const recorded = proposed.map((lead) => {
    const ranking = scoreRelatedEntityLead(lead, { targetHotel, remainingBudgetUsd });
    return {
      ...lead,
      recorded: true,
      commercialValueScore: ranking.score,
      pursueRecommended: ranking.pursue,
      rankingReasons: ranking.reasons,
      researchQuestion: lead.researchQuestion,
      appropriateSourceFamily: lead.suggestedUrlIndependentPublisher
        ? "independent_publisher"
        : lead.relationship === "SAME_ORGANIZATION_PAGE"
          ? "organizer_publication"
          : "related_entity_unverified",
      authorizedRetrievalStrategy: lead.suggestedUrlIndependentPublisher
        ? "direct_public_then_priced_firecrawl_if_authorized"
        : "prefer_discover_independent_publisher_before_more_organizer_pages",
    };
  });

  const pursueQueue = recorded
    .filter((item) => item.pursueRecommended)
    .sort((a, b) => b.commercialValueScore - a.commercialValueScore)
    .slice(0, maxPursue);

  const deferred = recorded.filter((item) => !item.pursueRecommended);

  return {
    version: AUTONOMOUS_LEAD_FOLLOWING_VERSION,
    leadsRecorded: recorded.length,
    leadsToPursue: pursueQueue,
    leadsDeferred: deferred,
    doNotCollectWithoutFollowingStrongest: true,
    avoidEndlessIrrelevantExpansion: true,
    neverPromoteToIndependentlyVerifiedWithoutConnectingEvidence: true,
    relationshipClasses: [
      RELATIONSHIP_VERIFICATION.ORGANIZER_CLAIMED,
      RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
      RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED,
      RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE,
    ],
  };
}

/**
 * After a follow-up retrieval, update relationship confidence without inventing verification.
 */
export function updateLeadRelationshipFromRetrieval({
  lead = {},
  retrievedUrl = "",
  pageText = "",
  organizationHost = "",
} = {}) {
  const independent = isIndependentPublisherUrl(retrievedUrl, organizationHost);
  const namesEntity = lead.entityName
    ? new RegExp(String(lead.entityName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 40), "i").test(pageText)
    : false;
  const namesOrganizer = organizationHost
    ? new RegExp(String(organizationHost).split(".")[0], "i").test(pageText)
    : false;
  const connects = namesEntity && namesOrganizer;

  if (!independent) {
    return {
      ...lead,
      verificationStatus: RELATIONSHIP_VERIFICATION.ORGANIZER_CLAIMED,
      independentlyVerified: false,
      note: "Follow-up page is not an independent publisher relative to the organizer.",
    };
  }
  if (independent && connects) {
    return {
      ...lead,
      verificationStatus: RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED,
      independentlyVerified: true,
      confirmingUrl: retrievedUrl,
      note: "Independent publisher evidence explicitly connects the parties/programs.",
    };
  }
  if (independent && namesEntity && !namesOrganizer) {
    return {
      ...lead,
      verificationStatus: RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
      independentlyVerified: false,
      confirmingUrl: retrievedUrl,
      note: "Independent page names the entity but does not connect it to the organizer.",
    };
  }
  return {
    ...lead,
    verificationStatus: RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE,
    independentlyVerified: false,
    note: "Independent retrieval did not corroborate the claimed relationship.",
  };
}

/**
 * Bounded execution of highest-value related-entity follow-ups.
 * Offline mode uses stored page text only (no HTTP / no paid providers).
 * Live mode requires authorizeLiveRetrieval + fetchPage; still respects maxExecute.
 */
export function executeBoundedLeadFollowUps({
  leads = [],
  mode = "offline_stored_evidence_only",
  storedPages = [],
  organizationHost = "",
  maxExecute = 2,
  authorizeLiveRetrieval = false,
  fetchPage = null,
} = {}) {
  const queue = (leads || []).filter((l) => l && l.pursueRecommended !== false).slice(0, maxExecute);
  const outcomes = [];

  for (const lead of queue) {
    if (mode === "offline_stored_evidence_only" || !authorizeLiveRetrieval) {
      const stored = (storedPages || []).find((p) => {
        if (lead.suggestedUrl && p.url && String(p.url) === String(lead.suggestedUrl)) return true;
        return lead.entityName && new RegExp(
          String(lead.entityName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 24),
          "i",
        ).test(String(p.text || ""));
      });
      if (!stored) {
        outcomes.push({
          entityName: lead.entityName,
          executed: false,
          reason: "no_stored_page_for_offline_followup",
          verificationStatus: lead.verificationStatus || RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
          independentlyVerified: false,
          paidSpendUsd: 0,
        });
        continue;
      }
      const updated = updateLeadRelationshipFromRetrieval({
        lead,
        retrievedUrl: stored.url || lead.suggestedUrl || "",
        pageText: stored.text || "",
        organizationHost,
      });
      outcomes.push({
        entityName: lead.entityName,
        executed: true,
        mode: "offline_stored_evidence_only",
        url: stored.url || null,
        verificationStatus: updated.verificationStatus,
        independentlyVerified: updated.independentlyVerified === true,
        note: updated.note,
        paidSpendUsd: 0,
      });
      continue;
    }

    // Live path — only when ORS authorizes and supplies a sync/async-capable fetchPage.
    // Prefer matureOpportunity's related_entity_followup strategy (async researcher adapters).
    if (typeof fetchPage !== "function") {
      outcomes.push({
        entityName: lead.entityName,
        executed: false,
        reason: "live_retrieval_not_authorized",
        suggestedUrl: lead.suggestedUrl || null,
        researchQuestion: lead.researchQuestion || null,
        paidSpendUsd: 0,
      });
      continue;
    }
    if (!lead.suggestedUrl) {
      outcomes.push({
        entityName: lead.entityName,
        executed: false,
        reason: "no_suggested_url",
        researchQuestion: lead.researchQuestion || null,
        paidSpendUsd: 0,
      });
      continue;
    }
    outcomes.push({
      entityName: lead.entityName,
      executed: false,
      reason: "deferred_to_ors_related_entity_followup_strategy",
      suggestedUrl: lead.suggestedUrl,
      researchQuestion: lead.researchQuestion || null,
      commercialValueScore: lead.commercialValueScore ?? null,
      paidSpendUsd: 0,
      note: "ORS matureOpportunity executes this lead via researcher adapters with budget reservation.",
    });
  }

  return {
    version: AUTONOMOUS_LEAD_FOLLOWING_VERSION,
    mode,
    queued: (leads || []).length,
    executedCount: outcomes.filter((o) => o.executed === true).length,
    pendingOrsExecution: outcomes.filter((o) => o.reason === "deferred_to_ors_related_entity_followup_strategy"),
    outcomes,
    notMerelyQueued: true,
    sourceOwnershipRulesPreserved: true,
    budgetReservationsRequiredForLive: true,
  };
}

/**
 * Select the single highest-value related-entity lead still needing retrieval.
 * Used by matureOpportunity so follow-ups execute through ORS, not a side pipeline.
 */
export function selectNextAuthorizedLeadFollowUp({
  pursueQueue = [],
  investigatedUrls = [],
  alreadyFollowedCount = 0,
  maxFollowUps = 2,
  remainingBudgetUsd = Infinity,
  commerciallyUsefulInternal = false,
} = {}) {
  if (alreadyFollowedCount >= maxFollowUps) {
    return { lead: null, skipReason: "max_lead_followups_reached" };
  }
  if (!(remainingBudgetUsd > 0.001)) {
    return { lead: null, skipReason: "budget_exhausted" };
  }
  const investigated = new Set((investigatedUrls || []).map(String).filter(Boolean));
  const candidates = (pursueQueue || [])
    .filter((l) => l && l.pursueRecommended !== false)
    .filter((l) => l.suggestedUrl && !investigated.has(String(l.suggestedUrl)))
    .filter((l) => !IRRELEVANT.test(`${l.entityName || ""} ${l.note || ""}`))
    .sort((a, b) => (b.commercialValueScore || 0) - (a.commercialValueScore || 0));

  if (!candidates.length) {
    return { lead: null, skipReason: "no_uninvestigated_authorized_leads" };
  }

  // VoI: if already internally useful, only pursue independent publisher corroboration.
  const next = commerciallyUsefulInternal
    ? candidates.find((l) => l.suggestedUrlIndependentPublisher === true) || null
    : candidates[0];

  if (!next) {
    return { lead: null, skipReason: "voi_no_independent_corroboration_needed" };
  }
  return {
    lead: next,
    skipReason: null,
    strategy: "related_entity_followup",
    fingerprint: `related_entity_followup::${String(next.entityName || "").slice(0, 40)}::${String(next.suggestedUrl).slice(0, 120)}`,
  };
}
