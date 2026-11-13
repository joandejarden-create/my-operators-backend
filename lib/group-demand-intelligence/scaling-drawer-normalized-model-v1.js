/**
 * Canonical Scaling Companies drawer model V1
 *
 * Exactly ten top-level section IDs in fixed order. No alternate top-level
 * sections (Commercial context / Why Dealality selected / Company background).
 * Useful content from those sources is folded into overview / what-is-happening.
 *
 * Presentation only — does not rewrite sealed research stores.
 */

import {
  buildScalingCorporateResearchGapAction,
  hasAssociationDemandBoilerplate,
  hasAssociationProcurementBoilerplate,
  isScalingCompaniesSegment,
  rebuildScalingWhyThisHotelFromBrief,
} from "./scaling-drawer-presentation-contract-v1.js";
import { SCALING_CUSTOMER_CLASSIFICATION_LABEL } from "./scaling-display-identity-v1.js";
import {
  buildHotelIndependentOpportunityOverview,
  marketLabelForOverview,
  overviewContainsHotelIdentity,
  stripHotelSpecificFromOverview,
} from "./scaling-sales-leader-editorial-v1.js";
import { buildScalingTileCustomerSummary } from "./scaling-tile-customer-summary-v1.js";

export const SCALING_DRAWER_NORMALIZED_MODEL_V1 =
  "gdi-scaling-drawer-normalized-model-v1";

/** Tile/drawer classification — never the filter label "Scaling Company". */
function normalizeScalingCustomerClassification(raw) {
  const s = String(raw || "").replace(/\s+/g, " ").trim();
  if (!s || /^scaling company$/i.test(s)) {
    return SCALING_CUSTOMER_CLASSIFICATION_LABEL;
  }
  return s;
}

/** Exact top-level section IDs — tests compare these arrays directly. */
export const SCALING_CANONICAL_SECTION_IDS = Object.freeze([
  "opportunity_overview",
  "what_is_happening",
  "potential_business",
  "why_this_hotel",
  "why_act_now",
  "facts_and_uncertainties",
  "who_to_contact",
  "recommended_next_step",
  "supporting_evidence",
  "hotel_validation",
]);

export const SCALING_CANONICAL_SECTION_TITLES = Object.freeze({
  opportunity_overview: "Opportunity overview",
  what_is_happening: "What is happening",
  potential_business: "Potential business for this hotel",
  why_this_hotel: "Why this hotel",
  why_act_now: "Why act now",
  facts_and_uncertainties: "Established facts and uncertainties",
  who_to_contact: "Who to contact",
  recommended_next_step: "Recommended next step",
  supporting_evidence: "Supporting evidence",
  hotel_validation: "Hotel Validation / Action / Outcome",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function distinct(a, b) {
  const x = text(a);
  const y = text(b);
  if (!x) return "";
  if (!y) return x;
  if (x.toLowerCase() === y.toLowerCase()) return "";
  if (y.toLowerCase().includes(x.toLowerCase().slice(0, Math.min(80, x.length)))) {
    return "";
  }
  return x;
}

function cleanList(items) {
  if (!Array.isArray(items)) return [];
  return items
    .map((t) => text(t))
    .filter(Boolean)
    .filter((t) => !/^—+$/.test(t) && t !== "-" && t !== "•");
}

function approachText(brief) {
  if (!brief || brief.recommendedSalesApproach == null) return "";
  const a = brief.recommendedSalesApproach;
  if (typeof a === "string") return text(a);
  if (typeof a === "object") {
    return text(a.nextAction || a.openingApproach || "");
  }
  return "";
}

function qualificationQuestions(brief) {
  const a = brief?.recommendedSalesApproach;
  const fromApproach =
    a && typeof a === "object" && Array.isArray(a.qualificationQuestions)
      ? a.qualificationQuestions
      : [];
  const fromBrief = Array.isArray(brief?.qualificationQuestions)
    ? brief.qualificationQuestions
    : [];
  const merged = fromApproach.length ? fromApproach : fromBrief;
  return cleanList(merged).filter(
    (q) =>
      !/association staff|PCO|host\/overflow|housing provider|event edition|Peak rooms|Attendance/i.test(
        q,
      ),
  );
}

function dedupeSources(sources) {
  const seen = new Set();
  const out = [];
  for (const s of Array.isArray(sources) ? sources : []) {
    const url = text(typeof s === "string" ? s : s?.url || s?.href);
    if (!url) continue;
    const key = url.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) continue;
    seen.add(key);
    const obj = typeof s === "object" && s ? s : {};
    out.push({
      url,
      title: text(obj.title || obj.sourceTitle) || null,
      publisher: text(obj.publisher || obj.entityAttributed) || null,
      publicationDate: obj.publicationDate || obj.publishedDate || null,
      establishes: text(obj.establishes || obj.relevance) || null,
      evidenceGroup: text(obj.evidenceGroup) || null,
      evidenceGroupLabel: text(obj.evidenceGroupLabel) || null,
      sourceId: text(obj.sourceId) || null,
      sourceType: text(obj.sourceType) || null,
      claimIdsSupported: Array.isArray(obj.claimIdsSupported)
        ? obj.claimIdsSupported
        : [],
      verificationStatus: text(obj.verificationStatus) || null,
    });
  }
  return out;
}

/**
 * Build the authoritative Scaling drawer model from a (preferably sanitized) opportunity.
 */
/**
 * Normalize CIB field aliases so Segments 4/9/12 (and future shared-drawer
 * segments) feed the same Scaling sections 2–10 builder without a parallel CIB.
 */
function normalizeBriefAliases(brief = {}) {
  const b = { ...brief };
  if (!b.potentialBusinessForThisHotel) {
    b.potentialBusinessForThisHotel =
      b.potentialBusinessForHotel || b.potentialBusiness || null;
  }
  if (!b.evidenceAndUncertainty) {
    const factsBlock = b.establishedFactsAndUncertainties || {};
    b.evidenceAndUncertainty = {
      establishedFacts: factsBlock.facts || b.establishedFacts || [],
      uncertainties: factsBlock.uncertainties || b.uncertainties || [],
      sources: Array.isArray(b.supportingEvidence)
        ? b.supportingEvidence.map((s) => ({
            url: s?.url,
            title: s?.title || s?.claim || null,
            claim: s?.claim || null,
            establishes: s?.claim || null,
          }))
        : [],
    };
  }
  if (b.recommendedSalesApproach == null && b.recommendedNextStep) {
    b.recommendedSalesApproach = {
      nextAction: b.recommendedNextStep,
      qualificationQuestions: b.qualificationQuestions || [],
    };
  }
  if (
    (!b.whyThisHotelAdvantages || !b.whyThisHotelAdvantages.length) &&
    Array.isArray(b.advantages)
  ) {
    b.whyThisHotelAdvantages = b.advantages;
  }
  if (
    (!b.whyThisHotelLimitations || !b.whyThisHotelLimitations.length) &&
    Array.isArray(b.limitations)
  ) {
    b.whyThisHotelLimitations = b.limitations;
  }
  return b;
}

export function buildScalingDrawerNormalizedModel(opportunity = {}, opts = {}) {
  if (!isScalingCompaniesSegment(opportunity) && !opts.force) {
    return null;
  }

  const cib = opportunity.commercialIntelligenceBrief || null;
  const sb = opportunity.salesBrief || null;
  const brief = normalizeBriefAliases(cib || sb || {});
  const forcedSharedSegment = opts.force === true && !isScalingCompaniesSegment(opportunity);

  const org =
    text(
      opportunity.displayCompanyName ||
        opportunity.organizationName ||
        opportunity.company ||
        opportunity.companyName,
    ) || "Organization";
  const hotelName =
    text(
      opts.hotelDisplayName ||
        opportunity.hotelDisplayName ||
        opportunity.hotelName ||
        opportunity.targetHotelName,
    ) || "this hotel";

  // --- What is happening: same source as the opportunity tile ---
  // Prefer CIB companyAndLocalActivity; never Opportunity overview.
  // buildScalingTileCustomerSummary is the shared scrubber for tile + this section.
  let whatHappening = forcedSharedSegment
    ? text(
        opportunity.whatIsHappening ||
          brief.whatIsHappening ||
          brief.companyAndLocalActivity ||
          opportunity.summaryWhat ||
          "",
      )
    : text(
        buildScalingTileCustomerSummary({
          ...opportunity,
          commercialIntelligenceBrief: brief,
          displayCompanyName: org,
          whatIsHappening:
            opportunity.whatIsHappening || brief.companyAndLocalActivity || null,
          summaryWhat: opportunity.summaryWhat || brief.companyAndLocalActivity || null,
          tileCustomerSummary: null,
        }),
      );
  if (!whatHappening) {
    whatHappening = forcedSharedSegment
      ? `${org} has documented programme activity relevant to this market. Exact hotel demand still needs qualification.`
      : `${org} has documented commercial expansion relevant to this market. Exact travel patterns still need qualification with the account.`;
  }
  whatHappening = whatHappening
    .replace(/Selection signal:\s*/gi, "")
    .replace(/Dealality selected [^.]+?\./gi, "")
    .replace(/\blocal expansion\/activity\b/gi, "commercial expansion")
    .replace(/\b(local activity|local growth activity)\b/gi, "commercial expansion")
    .replace(/\s{2,}/g, " ")
    .trim();
  if (
    whatHappening.length < 80 ||
    /^.{0,120}—\s*EXHIBITOR BLOCK\s*$/i.test(whatHappening)
  ) {
    whatHappening = forcedSharedSegment
      ? `${org} has documented programme activity relevant to this market. Exact hotel demand still needs qualification.`
      : `${org} has documented commercial expansion relevant to this market. Exact travel patterns still need qualification with the account.`;
  }

  const activityDate = text(
    brief.evidenceAndUncertainty?.originalActivityDate ||
      opportunity.growthEvidenceDate ||
      opportunity.sourcePublicationDate ||
      opportunity.activityChronology?.originalEventDate ||
      brief.activityChronology?.originalEventDate ||
      "",
  );

  // --- Potential business ---
  let potential = text(
    brief.potentialBusinessForThisHotel ||
      opportunity.potentialHotelBusiness ||
      opportunity.summaryWhyMatters ||
      "",
  );
  if (
    hasAssociationProcurementBoilerplate(potential) ||
    hasAssociationDemandBoilerplate(potential)
  ) {
    potential = text(brief.potentialBusinessForThisHotel) || "";
  }
  // Repair truncated template lead from research packaging.
  if (/^sized\s+group\s+opportunity\.\s*/i.test(potential)) {
    potential = potential.replace(/^sized\s+group\s+opportunity\.\s*/i, "").trim();
    if (potential) potential = potential.charAt(0).toUpperCase() + potential.slice(1);
  }
  if (!potential) {
    potential =
      "Hotel demand (visiting staff, leadership travel, training, or meetings) is not yet verified from public research. Treat as a hypothesis until a travel or meetings owner confirms patterns.";
  }
  const inferred = cleanList(opportunity.inferredHotelDemand).filter(
    (t) => distinct(t, potential),
  );

  // --- Why this hotel ---
  let whyHotel = text(
    opportunity.whyThisHotel || brief.whyThisHotel || opportunity.summaryWhyHotel || "",
  );
  whyHotel = whyHotel
    .replace(/\bSize signal:[^.]*\.?/gi, "")
    .replace(/\bgdi_hotel_[a-z0-9_]+\b/gi, "this hotel")
    .replace(/\brec[A-Za-z0-9]{10,}\b/g, "this hotel")
    .replace(/\s{2,}/g, " ")
    .trim();
  const actionScale = text(
    opportunity.recommendedAction || opportunity.suggestedAction || ""
  );
  const priorityScale = text(
    opportunity.priority || opportunity.cardPriority || ""
  );
  if (
    (/disqualified/i.test(priorityScale) ||
      /Past cycle\s*[—–-]\s*do not pursue/i.test(actionScale) ||
      /wrong[- ]destination|Close as wrong-destination/i.test(actionScale)) &&
    /\b(?:attracts?\s+over\s+\d+|large\s+number\s+of\s+attendees|significant\s+(?:room\s+)?demand|draw\s+a\s+large\s+number|creating\s+(?:significant\s+)?demand\s+for\s+hotel)\b/i.test(
      whyHotel
    )
  ) {
    whyHotel =
      "Property fit is relationship / next-cycle context only — do not treat current-cycle attendance or room-demand claims as an active chase for this hotel.";
  }
  if (
    hasAssociationDemandBoilerplate(whyHotel) ||
    hasAssociationProcurementBoilerplate(whyHotel)
  ) {
    whyHotel = rebuildScalingWhyThisHotelFromBrief(opportunity) || whyHotel;
  }
  const advantages = cleanList(
    brief.whyThisHotelAdvantages || opportunity.whyThisHotelAdvantages,
  )
    // Drop unpublished peak-room size signals that contradict overview "not published"
    .map((t) =>
      String(t || "")
        .replace(/\bSize signal:[^.]*\.?/gi, "")
        .replace(/\bgdi_hotel_[a-z0-9_]+\b/gi, "this hotel")
        .replace(/\brec[A-Za-z0-9]{10,}\b/g, "this hotel")
        .replace(/\s{2,}/g, " ")
        .trim()
    )
    .filter(Boolean);
  const limitations = cleanList(
    brief.whyThisHotelLimitations || opportunity.whyThisHotelLimitations,
  );
  if (!whyHotel && (advantages.length || limitations.length)) {
    whyHotel = advantages[0] || limitations[0] || "";
  }
  // Avoid duplicating the same sentences in body AND advantages/limitations lists.
  if (advantages.length || limitations.length) {
    const pack = [...advantages, ...limitations].join(" ").toLowerCase();
    if (whyHotel && pack.includes(whyHotel.toLowerCase().slice(0, Math.min(80, whyHotel.length)))) {
      whyHotel = "";
    }
  }
  if (!whyHotel && !advantages.length && !limitations.length) {
    whyHotel = `Property-specific fit for ${hotelName} is not yet established beyond geography. Confirm accessibility and product fit with the account before pitching.`;
  }

  // --- Why act now ---
  let whyActNow = text(
    opportunity.whyEstablishContactNow ||
      opportunity.whyNow ||
      opportunity.whyActNow ||
      brief.whyActNow ||
      (typeof brief.recommendedSalesApproach === "object"
        ? brief.recommendedSalesApproach?.timingRationale
        : "") ||
      "",
  );
  if (/OPEN[ _]?ENDED|exploratory until sourcing/i.test(whyActNow)) {
    whyActNow = "";
  }
  // Why Act Now must not duplicate Potential Business verbatim.
  if (
    whyActNow &&
    potential &&
    whyActNow.toLowerCase() === potential.toLowerCase()
  ) {
    whyActNow = "";
  }
  if (!whyActNow) {
    if (/Monitor|Past cycle|Watch/i.test(text(opportunity.recommendedAction))) {
      whyActNow =
        "Timing is watch-only until a confirmed lodging path or open sourcing signal appears — do not treat potential demand as an active chase.";
    } else {
      whyActNow =
        "No time-critical trigger is established. Monitor for confirmed local activity or a travel/meetings owner before aggressive pursuit.";
    }
  }

  // --- Facts / uncertainties ---
  let facts = cleanList(
    brief.evidenceAndUncertainty?.establishedFacts ||
      opportunity.establishedFacts,
  );
  let uncertainties = cleanList(
    brief.evidenceAndUncertainty?.uncertainties ||
      opportunity.uncertainties ||
      opportunity.constraints,
  );
  // Drop empties that look like blank bullets
  uncertainties = uncertainties.filter((u) => u.length > 2);
  facts = facts.filter((f) => f.length > 2);
  if (!facts.length && whatHappening && !/not yet clearly established/i.test(whatHappening)) {
    // Do not invent facts — leave empty with honest status
  }
  const factsStatus = facts.length
    ? "HAS_FACTS"
    : "NO_ESTABLISHED_FACTS_YET";
  const uncStatus = uncertainties.length
    ? "HAS_UNCERTAINTIES"
    : "NO_LISTED_UNCERTAINTIES";

  // --- Recommended next step (ALWAYS present) ---
  // Prefer published/certified recommendedNextStep over empty/generic action text.
  const certifiedNext = text(
    opportunity.recommendedNextStep ||
      brief.recommendedNextStep ||
      opportunity.recommendedAction ||
      opportunity.recommendedNextAction,
  );
  let nextStep = certifiedNext || approachText(brief) || text(opportunity.recommendedAction);
  if (
    certifiedNext &&
    /meetings\s*\/\s*congress\s*\/\s*operations route|association staff\s*vs\s*PCO|Organizer TBD/i.test(
      nextStep,
    )
  ) {
    nextStep = certifiedNext;
  }
  // Never inject Scaling corporate-travel fallback into forced shared segments
  // (Government / Local Catering / Relocation / …).
  if (!forcedSharedSegment) {
    if (hasAssociationProcurementBoilerplate(nextStep)) {
      nextStep = buildScalingCorporateResearchGapAction({
        ...opportunity,
        hotelDisplayName: hotelName,
      });
    }
    if (!nextStep) {
      nextStep = buildScalingCorporateResearchGapAction({
        ...opportunity,
        hotelDisplayName: hotelName,
      });
    }
  } else if (!nextStep) {
    nextStep =
      "Confirm programme ownership, dates, and commercial accessibility before outreach. Do not invent lodging or catering commitments.";
  }
  const questions = qualificationQuestions(brief);

  // --- Demand stage / timing meta ---
  const demandStage =
    text(opportunity.demandStageLabel) ||
    (potential
      ? "Potential business opportunity; hotel needs not yet confirmed"
      : "Corporate account — qualify lodging and meeting needs");
  const timing =
    text(opportunity.eventDateDisplay || opportunity.timing) ||
    (activityDate ? `Activity dated ${activityDate}` : "Timing to be confirmed");

  const drawerEvidence = opportunity.supportingEvidenceDrawer || null;
  // Stage F: prefer editorially selected primary evidence (concise); full list retained separately.
  const sources = dedupeSources(
    drawerEvidence?.sources?.length
      ? drawerEvidence.sources
      : brief.evidenceAndUncertainty?.sources?.length
        ? brief.evidenceAndUncertainty.sources
        : opportunity.primaryEvidenceSources?.length
          ? opportunity.primaryEvidenceSources
          : opportunity.sources,
  );
  const sourcesFull = dedupeSources(
    brief.evidenceAndUncertainty?.sourcesFull ||
      opportunity.sourcesFull ||
      sources,
  );
  // Concise default: no evidence groups / long "establishes" dumps unless UI expands full list
  const evidenceGroups = [];

  const contactPathLabel =
    text(opportunity.commercialContactPathLabel) ||
    (opportunity.commercialContactPath === "NAMED_DIRECT"
      ? "Named person — direct business email"
      : opportunity.commercialContactPath === "ORGANIZATION_PATH"
        ? "Company contact path"
        : opportunity.commercialContactPath === "FUNCTIONAL_CONTACT"
          ? "Functional / department email"
          : null);

  // Opportunity overview must be hotel-independent (market-level). Legacy briefs that
  // still name the property are scrubbed here; hotel-specific copy stays in why_this_hotel.
  const hotelCtx = {
    displayName: hotelName !== "this hotel" ? hotelName : null,
    name: hotelName !== "this hotel" ? hotelName : null,
    city: text(opts.hotelCity || opportunity.hotelCity || opportunity.marketCity),
    address: text(opts.hotelAddress || opportunity.hotelAddress),
  };
  let overviewBody = text(brief.opportunityOverview) || null;
  // Shared-segment drawers use structured Section 1 (overviewModel) — keep a
  // short identity line here only as fallback narrative body.
  if (forcedSharedSegment) {
    overviewBody =
      overviewBody ||
      text(opportunity.programmeName || opportunity.eventDisplayName || opportunity.title) ||
      org;
  } else if (
    !overviewBody ||
    overviewContainsHotelIdentity(overviewBody, hotelCtx) ||
    /\ba hotel in the market\b/i.test(overviewBody)
  ) {
    overviewBody = buildHotelIndependentOpportunityOverview({
      displayName: org,
      marketCity: marketLabelForOverview(
        { ...hotelCtx, city: hotelCtx.city || text(opts.hotelCity) },
        opportunity,
      ),
      chrono: {
        originalEventDate: activityDate || null,
        activityStatus: brief.evidenceAndUncertainty?.activityStatus || null,
        hasContinuityEvidence:
          brief.evidenceAndUncertainty?.hasContinuityEvidence === true,
      },
    });
  } else {
    overviewBody = stripHotelSpecificFromOverview(overviewBody, hotelCtx);
  }

  const typeLabel = forcedSharedSegment
    ? text(
        opportunity.segmentDisplayLabel ||
          opportunity.opportunityTypeLabel ||
          brief.segmentDisplayLabel,
      ) || "Demand opportunity"
    : normalizeScalingCustomerClassification(
        opportunity.opportunityTypeLabel || opportunity.segmentDisplayLabel,
      );

  return {
    version: SCALING_DRAWER_NORMALIZED_MODEL_V1,
    sectionIds: [...SCALING_CANONICAL_SECTION_IDS],
    sectionTitles: { ...SCALING_CANONICAL_SECTION_TITLES },
    organizationName: org,
    displayCompanyName: org,
    segmentDisplayLabel: typeLabel,
    opportunityTypeLabel: typeLabel,
    title: text(opportunity.displayTitle || opportunity.title) || org,
    demandStage,
    timing,
    overviewBody,
    sections: {
      opportunity_overview: {
        id: "opportunity_overview",
        title: SCALING_CANONICAL_SECTION_TITLES.opportunity_overview,
        organizationName: org,
        typeLabel,
        body: overviewBody,
        demandStage,
        timing,
      },
      what_is_happening: {
        id: "what_is_happening",
        title: SCALING_CANONICAL_SECTION_TITLES.what_is_happening,
        body: whatHappening,
        activityDate: activityDate || null,
      },
      potential_business: {
        id: "potential_business",
        title: SCALING_CANONICAL_SECTION_TITLES.potential_business,
        body: potential,
        inferred,
      },
      why_this_hotel: {
        id: "why_this_hotel",
        title: SCALING_CANONICAL_SECTION_TITLES.why_this_hotel,
        body: whyHotel,
        // Nested only — never top-level section titles
        advantages,
        limitations,
      },
      why_act_now: {
        id: "why_act_now",
        title: SCALING_CANONICAL_SECTION_TITLES.why_act_now,
        body: whyActNow,
      },
      facts_and_uncertainties: {
        id: "facts_and_uncertainties",
        title: SCALING_CANONICAL_SECTION_TITLES.facts_and_uncertainties,
        facts,
        uncertainties,
        factsStatus,
        uncStatus,
        factsEmptyNote:
          facts.length === 0
            ? "No separately listed established facts beyond the activity narrative above."
            : null,
        uncEmptyNote:
          uncertainties.length === 0
            ? "Material unknowns remain (travel volume, budgets, buying authority) unless confirmed with the account."
            : null,
      },
      who_to_contact: {
        id: "who_to_contact",
        title: SCALING_CANONICAL_SECTION_TITLES.who_to_contact,
        contactPathLabel,
        primaryContact: opportunity.primaryContact
          ? {
              name: text(opportunity.primaryContact.name),
              role: text(
                opportunity.primaryContact.role ||
                  opportunity.primaryContact.title,
              ),
              company: text(
                opportunity.primaryContact.organization ||
                  opportunity.organizationName,
              ),
              commercialClass:
                text(opportunity.primaryContact.commercialClass) || null,
              contactRelevanceLabel:
                text(opportunity.primaryContact.contactRelevanceLabel) || null,
              contractingAuthorityStatus:
                text(opportunity.primaryContact.contractingAuthorityStatus) ||
                null,
              whyRelevant: text(opportunity.primaryContact.whyThisContact) || null,
              sourceUrl: text(opportunity.primaryContact.sourceUrl) || null,
            }
          : null,
        secondaryContact: opportunity.secondaryContact?.name
          ? {
              name: text(opportunity.secondaryContact.name),
              role: text(
                opportunity.secondaryContact.role ||
                  opportunity.secondaryContact.title,
              ),
              company: text(
                opportunity.secondaryContact.organization ||
                  opportunity.organizationName,
              ),
              commercialClass:
                text(opportunity.secondaryContact.commercialClass) || null,
              contactRelevanceLabel:
                text(opportunity.secondaryContact.contactRelevanceLabel) || null,
              contractingAuthorityStatus:
                text(opportunity.secondaryContact.contractingAuthorityStatus) ||
                null,
              whyRelevant:
                text(opportunity.secondaryContact.whyThisContact) || null,
              sourceUrl: text(opportunity.secondaryContact.sourceUrl) || null,
            }
          : null,
        organizationalRouteUrl: text(
          opportunity.organizationRouteUrl || opportunity.contactUrl,
        ) || null,
        // Contact HTML still rendered via shared whoShouldSalesContactHtml for retrieval controls
      },
      recommended_next_step: {
        id: "recommended_next_step",
        title: SCALING_CANONICAL_SECTION_TITLES.recommended_next_step,
        body: nextStep,
        qualificationQuestions: questions,
      },
      supporting_evidence: {
        id: "supporting_evidence",
        title: SCALING_CANONICAL_SECTION_TITLES.supporting_evidence,
        // Concise primary list — strip long establishes for default customer view
        sources: sources.map((s) => ({
          ...s,
          establishes:
            s.establishes && s.establishes.length <= 120 ? s.establishes : null,
        })),
        sourcesFull,
        evidenceGroups,
        primaryCount: sources.length,
        fullCount: sourcesFull.length,
        activityDate: activityDate || null,
        researchVerificationDate: text(
          opportunity.evidenceFreshnessAt ||
            opportunity.activityChronology?.researchVerificationDate ||
            opportunity.commercialIntelligenceBrief?.evidenceAndUncertainty
              ?.researchVerificationDate ||
            "",
        ) || null,
      },
      hotel_validation: {
        id: "hotel_validation",
        title: SCALING_CANONICAL_SECTION_TITLES.hotel_validation,
      },
    },
  };
}

/**
 * Extract top-level section IDs from rendered Scaling HTML (data-section-id).
 */
export function extractScalingSectionIdsFromHtml(html) {
  const ids = [];
  const re = /data-section-id=["']([a-z_]+)["']/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    ids.push(m[1]);
  }
  return ids;
}

export function assertCanonicalSectionOrder(sectionIds) {
  const expected = SCALING_CANONICAL_SECTION_IDS;
  const actual = Array.isArray(sectionIds) ? sectionIds : [];
  return {
    ok:
      actual.length === expected.length &&
      expected.every((id, i) => actual[i] === id),
    expected: [...expected],
    actual,
  };
}
