/**
 * Commercial Depth & Buyer Intelligence V1
 *
 * Evidence-driven retrieval valuation + replanning + public buyer WHO pass
 * for long-form ORS / deep-research — without a new agent framework.
 *
 * Rules:
 * - Generic hotel meetings/marketing pages do NOT corroborate a DMC–hotel relationship.
 * - Retrieval quota / maxIterations ≠ commercial research completeness.
 * - Surfe/PDL/Parallel remain out of scope unless separately authorized.
 */

import {
  assessRelationshipVerification,
  assessCommercialDimensionCoverage,
  buildStopRecord,
  STOPPING_TRIGGER,
  RELATIONSHIP_VERIFICATION,
  COMMERCIAL_RESEARCH_DIMENSION,
  isIndependentPublisherUrl,
} from "./commercial-source-selection-v1.js";
import { updateLeadRelationshipFromRetrieval } from "./autonomous-lead-following-v2.js";
import { assessValueOfInformation, RESEARCH_DISPOSITION } from "./dynamic-research-planner-v1.js";
import {
  extractPublicContactIntelligenceForOrs,
  classifyContactOwnership,
  CONTACT_OWNERSHIP_CLASS,
} from "./commercial-spine-restoration-v41.js";
import { classifyPersonResponsibility } from "../workflow/buyer-contact-enrichment-v1.js";

export const COMMERCIAL_DEPTH_BUYER_V1 = "gdi-commercial-depth-buyer-intelligence-v1";

export const RETRIEVAL_COMMERCIAL_VALUE = Object.freeze({
  SUBSTANTIVE_ADVANCE: "SUBSTANTIVE_ADVANCE",
  BACKGROUND_ONLY: "BACKGROUND_ONLY",
  REDUNDANT_HELD: "REDUNDANT_HELD",
  UNPRODUCTIVE_RELATIONSHIP_PROBE: "UNPRODUCTIVE_RELATIONSHIP_PROBE",
  BUYER_SIGNAL: "BUYER_SIGNAL",
  CONTACT_ROUTE_CONFIRMED: "CONTACT_ROUTE_CONFIRMED",
  TECHNICAL_OR_EMPTY: "TECHNICAL_OR_EMPTY",
});

export const RESEARCH_COMPLETENESS = Object.freeze({
  COMMERCIALLY_SUFFICIENT: "COMMERCIALLY_SUFFICIENT",
  PARTIALLY_COMPLETE_BUYER_ENGAGEMENT_NEXT: "PARTIALLY_COMPLETE_BUYER_ENGAGEMENT_NEXT",
  INCOMPLETE_JUSTIFIED_ALTERNATIVES_REMAIN: "INCOMPLETE_JUSTIFIED_ALTERNATIVES_REMAIN",
  INCOMPLETE_BUDGET_OR_PROVIDER_LIMIT: "INCOMPLETE_BUDGET_OR_PROVIDER_LIMIT",
  INCOMPLETE_ITERATION_CEILING: "INCOMPLETE_ITERATION_CEILING",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function normalizeUrlKey(url) {
  return String(url || "").trim().replace(/\/+$/, "").toLowerCase();
}

const GENERIC_HOTEL_MARKETING =
  /\b(?:discover|overview|offers|accommodation|dining|wellness|experiences|shop|what's on|family moments|holiday at|spaces made for|world class)\b/i;
const MEETINGS_CAPABILITY =
  /\b(?:meetings?\s+and\s+events?|event\s+spaces?|banquet|ballroom|conference\s+rooms?)\b/i;
const NAMES_BOTH_PARTIES = (orgHint, hotelHint, blob) => {
  const org = String(orgHint || "").split(/[.\s]/)[0];
  const hotel = String(hotelHint || "").split(/\s+/).slice(0, 2).join(" ");
  if (!org || org.length < 4) return false;
  const hasOrg = new RegExp(org.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob);
  const hasHotel = hotel
    ? new RegExp(hotel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob)
    : false;
  return hasOrg && hasHotel;
};

/**
 * Does this page actually corroborate a claimed DMC–hotel relationship?
 * Independent hotel marketing / meetings amenity pages alone → NO.
 */
export function assessIndependentRelationshipCorroboration({
  url = "",
  pageText = "",
  organizationName = "",
  organizationHost = "",
  claimedRelatedEntity = "",
  researchQuestion = "",
} = {}) {
  const blob = text(pageText);
  const independent = isIndependentPublisherUrl(url, organizationHost);
  const relationshipUpdate = updateLeadRelationshipFromRetrieval({
    lead: {
      entityName: claimedRelatedEntity || "related hotel",
      verificationStatus: RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
    },
    retrievedUrl: url,
    pageText: blob,
    organizationHost,
  });

  const namesBoth = NAMES_BOTH_PARTIES(organizationName || organizationHost, claimedRelatedEntity, blob);
  const genericMarketing = GENERIC_HOTEL_MARKETING.test(blob) && !namesBoth;
  const meetingsOnly = MEETINGS_CAPABILITY.test(blob) && !namesBoth;
  // Require independent publisher AND explicit connection (lead-following update OR both-party naming).
  const connectsParties = independent && (
    relationshipUpdate.independentlyVerified === true || namesBoth
  );

  const verification = assessRelationshipVerification({
    provenanceUrl: url,
    confirmingUrl: connectsParties ? url : null,
    organizationHost,
    relationshipMentionedOnOrganizer: false,
    independentPublisherConfirms: connectsParties,
    independentSearchAttempted: independent,
    independentEvidenceFound: connectsParties,
  });

  return {
    version: COMMERCIAL_DEPTH_BUYER_V1,
    url,
    independentPublisher: independent,
    independentlyCorroboratesRelationship: connectsParties,
    verificationStatus: verification.verificationStatus,
    genericHotelMarketingPage: genericMarketing || meetingsOnly,
    countsAsRelationshipCorroboration: connectsParties,
    note: connectsParties
      ? "Independent page explicitly connects organizer and related entity."
      : independent
        ? "Independent publisher page retrieved — does NOT corroborate the DMC–hotel relationship without naming both parties / connecting evidence."
        : "Not an independent publisher relative to the organizer.",
    researchQuestionAdvanced: connectsParties
      || (/decision-?maker|sourcing|contact|staff|who\s+owns/i.test(researchQuestion) && /\b[A-Z][a-z]+\s+[A-Z][a-z]+\b/.test(blob) && /director|manager|sourcing|events|meetings/i.test(blob)),
    redirectToward: connectsParties
      ? []
      : [
        "third_party_article_naming_both_parties",
        "named_programme_case_with_independent_press",
        "staff_directory_or_team_page_for_buyer",
        "avoid_further_generic_hotel_amenity_pages",
      ],
  };
}

/**
 * Score one retrieval against open commercial questions.
 */
export function assessRetrievalCommercialValue({
  url = "",
  pageText = "",
  researchQuestions = [],
  heldUrls = [],
  organizationName = "",
  organizationHost = "",
  claimedRelatedEntity = null,
  relatedEntityFollowUp = false,
} = {}) {
  const blob = text(pageText);
  const held = new Set((heldUrls || []).map(normalizeUrlKey));
  if (!blob || blob.length < 80) {
    return {
      url,
      valueClass: RETRIEVAL_COMMERCIAL_VALUE.TECHNICAL_OR_EMPTY,
      advancedQuestions: [],
      unansweredRemain: researchQuestions,
      materialAdvance: false,
      replanRecommended: true,
      note: "Empty or thin retrieval — does not advance commercial questions.",
    };
  }
  if (held.has(normalizeUrlKey(url))) {
    return {
      url,
      valueClass: RETRIEVAL_COMMERCIAL_VALUE.REDUNDANT_HELD,
      advancedQuestions: [],
      unansweredRemain: researchQuestions,
      materialAdvance: false,
      replanRecommended: true,
      note: "URL already in held corpus (ignoring trailing slash) — not new commercial depth.",
    };
  }

  const advanced = [];
  const unanswered = [];
  for (const q of researchQuestions || []) {
    const ql = String(q || "").toLowerCase();
    let answered = false;
    if (/decision-?maker|who\s+(?:at|owns)|sourcing\s+director|hotel\s+sourcing/i.test(ql)) {
      answered = /\b(?:director|head|manager|vp|vice\s+president)\b/i.test(blob)
        && /\b[A-Z][a-z]+\s+[A-Z][a-z]+\b/.test(blob)
        && /hotel|sourcing|contract|events?|meetings?|incentive/i.test(blob);
    } else if (/rosewood|accommodation\s+partner|relationship/i.test(ql)) {
      const corr = assessIndependentRelationshipCorroboration({
        url,
        pageText: blob,
        organizationName,
        organizationHost,
        claimedRelatedEntity: claimedRelatedEntity || "Rosewood",
        researchQuestion: q,
      });
      answered = corr.independentlyCorroboratesRelationship;
    } else if (/nobu|dated\s+london\s+programme|bedrooms?\s+at/i.test(ql)) {
      answered = /nobu/i.test(blob) && /\b(?:rfp|room\s*block|20\d{2})\b/i.test(blob);
    } else if (/shortlist|recommend\s+hotels|how\s+does\s+principal/i.test(ql)) {
      answered = /shortlist|prefer(?:red)?\s+hotel|hotel\s+sourcing|contract\s+negotiation/i.test(blob)
        && !GENERIC_HOTEL_MARKETING.test(blob.slice(0, 400));
    } else if (/contact|reach/i.test(ql)) {
      answered = /@[a-z0-9.-]+\.[a-z]{2,}/i.test(blob) || /\b0\d{2,}\s+\d+/i.test(blob);
    }
    if (answered) advanced.push(q);
    else unanswered.push(q);
  }

  let relationshipProbe = null;
  if (relatedEntityFollowUp || /rosewoodhotels\.com/i.test(url)) {
    relationshipProbe = assessIndependentRelationshipCorroboration({
      url,
      pageText: blob,
      organizationName,
      organizationHost,
      claimedRelatedEntity: claimedRelatedEntity || "Rosewood London",
    });
  }

  const buyerSignal = /\b[A-Z][a-z]+\s+[A-Z][a-z]+\b/.test(blob)
    && /\b(?:director|manager|head of|vp|sourcing|events?|meetings?)\b/i.test(blob);
  const contactConfirm = /@[a-z0-9.-]+\.[a-z]{2,}/i.test(blob)
    && /principal/i.test(hostnameOf(url) + organizationHost);

  let valueClass = RETRIEVAL_COMMERCIAL_VALUE.BACKGROUND_ONLY;
  if (relationshipProbe && !relationshipProbe.independentlyCorroboratesRelationship && relationshipProbe.independentPublisher) {
    valueClass = RETRIEVAL_COMMERCIAL_VALUE.UNPRODUCTIVE_RELATIONSHIP_PROBE;
  } else if (advanced.length) {
    valueClass = RETRIEVAL_COMMERCIAL_VALUE.SUBSTANTIVE_ADVANCE;
  } else if (buyerSignal) {
    valueClass = RETRIEVAL_COMMERCIAL_VALUE.BUYER_SIGNAL;
  } else if (contactConfirm) {
    valueClass = RETRIEVAL_COMMERCIAL_VALUE.CONTACT_ROUTE_CONFIRMED;
  }

  const materialAdvance = valueClass === RETRIEVAL_COMMERCIAL_VALUE.SUBSTANTIVE_ADVANCE
    || valueClass === RETRIEVAL_COMMERCIAL_VALUE.BUYER_SIGNAL;

  return {
    url,
    publisher: hostnameOf(url),
    valueClass,
    advancedQuestions: advanced,
    unansweredRemain: unanswered,
    materialAdvance,
    replanRecommended: !materialAdvance || unanswered.length > 0,
    relationshipProbe,
    note: valueClass === RETRIEVAL_COMMERCIAL_VALUE.UNPRODUCTIVE_RELATIONSHIP_PROBE
      ? "Independent hotel page does not corroborate DMC–hotel relationship — redirect research."
      : materialAdvance
        ? "Retrieval advanced at least one open commercial question."
        : "Background / directory / org-presence information — limited commercial advance.",
  };
}

/**
 * After an unproductive retrieval, propose the next justified strategy (existing planner primitives).
 */
export function buildEvidenceDrivenReplan({
  retrievalAssessment = null,
  unresolvedQuestions = [],
  investigatedUrls = [],
  organizationName = "",
  targetHotel = "",
  destination = "London",
  contactPresent = false,
  remainingBudgetUsd = 0,
  alternativeCandidates = [],
} = {}) {
  const unanswered = retrievalAssessment?.unansweredRemain?.length
    ? retrievalAssessment.unansweredRemain
    : unresolvedQuestions;

  const voi = assessValueOfInformation({
    targetHotel,
    destination,
    hotelMarket: destination,
    hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
    unresolvedFact: unanswered[0] || "hotel sourcing decision-maker",
    alreadyInvestigated: true,
    contactPresent,
    openHostQuestion: /host|hotel|sourcing|rosewood|decision/i.test(unanswered[0] || ""),
  });

  const redirect = retrievalAssessment?.relationshipProbe?.redirectToward || [];
  const nextStrategies = [];

  if (retrievalAssessment?.valueClass === RETRIEVAL_COMMERCIAL_VALUE.UNPRODUCTIVE_RELATIONSHIP_PROBE) {
    nextStrategies.push({
      strategy: "seek_independent_naming_both_parties",
      reason: "generic_hotel_page_is_not_relationship_corroboration",
      preferredSources: redirect,
      avoid: ["further_rosewood_amenity_or_meetings_marketing_pages"],
    });
  }
  if (/decision-?maker|who\s+owns|sourcing/i.test(unanswered.join(" "))) {
    nextStrategies.push({
      strategy: "public_buyer_who_discovery",
      reason: "named_hotel_sourcing_stakeholder_still_open",
      preferredSources: ["team", "who-we-are", "leadership", "staff directory", "LinkedIn public if permitted"],
      avoid: ["repeating_generic_contact_page"],
    });
  }
  if (/programme|nobu|dated/i.test(unanswered.join(" "))) {
    nextStrategies.push({
      strategy: "programme_or_press_search",
      reason: "dated_programme_or_target_hotel_need_unanswered",
      preferredSources: ["news", "case studies with dates", "third-party event coverage"],
      avoid: ["directory_exhibitor_listings_as_buying_proof"],
    });
  }

  const worthExpectedCost = voi.furtherPublicResearchJustified === true
    && Number(remainingBudgetUsd) > 0
    && nextStrategies.length > 0
    && voi.preferredDisposition !== RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;

  const pendingDiscoverySearches = (alternativeCandidates || [])
    .filter((c) => c?.suggestedUrl || c?.url)
    .filter((c) => !investigatedUrls.map(normalizeUrlKey).includes(normalizeUrlKey(c.suggestedUrl || c.url)))
    .slice(0, 4)
    .map((c) => ({
      entityName: c.entityName || c.name || "replan_candidate",
      suggestedUrl: c.suggestedUrl || c.url,
      independentPublisher: c.independentPublisher === true,
      entityType: c.entityType || "programme",
      researchQuestion: c.researchQuestion || unanswered[0] || "Advance unanswered commercial question",
      replanReason: retrievalAssessment?.valueClass || "unproductive_or_incomplete",
    }));

  return {
    version: COMMERCIAL_DEPTH_BUYER_V1,
    valueOfInformation: voi,
    unansweredQuestions: unanswered,
    nextStrategies,
    worthExpectedCost,
    preferredDisposition: worthExpectedCost
      ? RESEARCH_DISPOSITION.FURTHER_RESEARCH_JUSTIFIED
      : (contactPresent
        ? RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED
        : voi.preferredDisposition),
    pendingDiscoverySearches,
    doNotCountGenericHotelPageAsCorroboration: true,
    organizationName,
  };
}

/**
 * Public-only buyer / decision-maker intelligence pass (no Surfe/PDL).
 * Honest WHO metrics: never claim successful buyer research when zero pages had usable text.
 */
export function runPublicBuyerIntelligencePass({
  evidence = [],
  organizationName = "",
  organizationDomain = "",
  segmentId = "professional_planners_dmc",
  existingContactEmail = null,
} = {}) {
  const stakeholders = [];
  const pages = [];
  const skippedEmpty = [];
  const pagesProvided = (evidence || []).length;
  const scannedAt = new Date().toISOString();

  for (const e of evidence || []) {
    const blob = text(e.statement || e.quote || e.text || e.excerpt || "");
    const url = e.url || "";
    if (blob.length < 40) {
      skippedEmpty.push({
        url: url || null,
        reason: url ? "URL_WITHOUT_USABLE_PAGE_TEXT" : "EMPTY_EVIDENCE_ROW",
        chars: blob.length,
      });
      continue;
    }
    pages.push({
      url,
      chars: blob.length,
      publisher: e.publisher || e.sourceFamily || null,
      retrievedAt: e.retrievedAt || e.fetchedAt || null,
      organizationName: e.organizationName || organizationName || null,
      opportunityScoped: true,
    });
    try {
      const ci = extractPublicContactIntelligenceForOrs({
        textContent: blob,
        url,
        organizationName,
      });
      for (const p of ci.namedStakeholdersFound || []) {
        const resp = classifyPersonResponsibility(
          {
            name: p.name,
            title: p.title,
            email: p.email,
            relevanceEvidence: `${p.title || ""} hotel sourcing incentive contracting`,
          },
          { organizationName, segmentId },
          {},
        );
        stakeholders.push({
          name: p.name,
          title: text(p.title || "").replace(/\s*[–—\-]\s*[a-z0-9._%+-]*@?.*$/i, "").trim() || null,
          email: p.email || null,
          responsibilityClass: resp?.class || null,
          responsibilityRationale: resp?.rationale || null,
          sourceUrl: url,
          contactDataOrigin: "PUBLIC_SOURCE",
          surfeUsed: false,
          pdlUsed: false,
          verifiedAt: scannedAt,
          currentOrganization: organizationName,
          decisionMakerClass: /BOOKING_DECISION|CONFIRMED_RELEVANT|POTENTIALLY_RELEVANT/i.test(resp?.class || "")
            ? "POTENTIAL_DECISION_MAKER"
            : "INFLUENCER_OR_GENERAL",
          buyerClassification: /BOOKING_DECISION|CONFIRMED_RELEVANT/i.test(resp?.class || "")
            ? "PROBABLE_RELEVANT_BUYER"
            : /POTENTIALLY_RELEVANT/i.test(resp?.class || "")
              ? "INFLUENCER"
              : "UNVERIFIED_PERSON",
        });
      }
    } catch {
      /* continue — extraction failure must not invent people */
    }
  }

  const deduped = [...new Map(stakeholders.filter((s) => s.name).map((s) => [s.name.toLowerCase(), s])).values()];
  const ownership = classifyContactOwnership({
    email: existingContactEmail || deduped[0]?.email || null,
    pageUrl: pages.find((p) => /contact/i.test(p.url))?.url || pages[0]?.url || "",
    organizationName,
    organizationDomain: organizationDomain || "",
    personName: deduped[0]?.name || null,
    roleTitle: deduped[0]?.title || null,
    publisherKind: "ORGANIZATION_OWNED",
  });

  const pagesScanned = pages.length;
  let whoResearchStatus = "ZERO_PAGES_SCANNED";
  if (pagesScanned > 0 && deduped.length > 0) whoResearchStatus = "NAMED_STAKEHOLDERS_FOUND";
  else if (pagesScanned > 0) whoResearchStatus = "PAGES_SCANNED_NO_NAMED_STAKEHOLDER";

  const decisionMakerGap = pagesScanned === 0
    ? "WHO research did not examine any usable page text — cannot claim buyer intelligence success."
    : deduped.length === 0
      ? "Named hotel-sourcing / contracting decision-maker not established from public evidence — general organization route remains."
      : null;

  return {
    version: COMMERCIAL_DEPTH_BUYER_V1,
    namedStakeholders: deduped,
    namedDecisionMakerEstablished: deduped.some((s) => s.decisionMakerClass === "POTENTIAL_DECISION_MAKER"),
    contactOwnership: ownership,
    decisionMakerGap,
    pagesProvided,
    pagesScanned,
    pagesSkippedEmpty: skippedEmpty,
    whoResearchStatus,
    whoResearchSucceeded: whoResearchStatus === "NAMED_STAKEHOLDERS_FOUND",
    roleRelevantStakeholders: deduped.filter((s) =>
      /PROBABLE_RELEVANT_BUYER|POTENTIAL_DECISION_MAKER/i.test(`${s.buyerClassification} ${s.decisionMakerClass}`),
    ),
    scannedAt,
    surfeInvoked: false,
    pdlInvoked: false,
  };
}

/**
 * Research completeness independent of technical stopping trigger.
 */
export function assessResearchCompleteness({
  unresolvedQuestions = [],
  namedDecisionMakerEstablished = false,
  relationshipIndependentlyVerified = false,
  contactUsable = false,
  technicalStopReason = null,
  remainingBudgetUsd = null,
  justifiedAlternativesRemain = false,
  commerciallyUsefulInternal = false,
} = {}) {
  const openMaterial = (unresolvedQuestions || []).filter((q) =>
    /decision-?maker|who\s+owns|who\s+at|housing|hotel\s+sourc|contract|buyer|dated\s+programme|shortlist|room.?block|headquarter|overflow|tournament|washcon|memorial|independent|next.?cycle/i.test(String(q || "")),
  );

  let completeness = RESEARCH_COMPLETENESS.INCOMPLETE_JUSTIFIED_ALTERNATIVES_REMAIN;
  if (openMaterial.length === 0 && (namedDecisionMakerEstablished || contactUsable)) {
    completeness = RESEARCH_COMPLETENESS.COMMERCIALLY_SUFFICIENT;
  } else if (contactUsable && commerciallyUsefulInternal && !justifiedAlternativesRemain) {
    completeness = RESEARCH_COMPLETENESS.PARTIALLY_COMPLETE_BUYER_ENGAGEMENT_NEXT;
  } else if (Number(remainingBudgetUsd) <= 0 || /PROVIDER|BUDGET/i.test(String(technicalStopReason || ""))) {
    completeness = RESEARCH_COMPLETENESS.INCOMPLETE_BUDGET_OR_PROVIDER_LIMIT;
  } else if (/ITERATION_CEILING|RETRIEVAL_CAP|PUBLIC_PATH/i.test(String(technicalStopReason || ""))) {
    completeness = RESEARCH_COMPLETENESS.INCOMPLETE_ITERATION_CEILING;
  } else if (contactUsable && !namedDecisionMakerEstablished) {
    completeness = RESEARCH_COMPLETENESS.PARTIALLY_COMPLETE_BUYER_ENGAGEMENT_NEXT;
  }

  const stopRecord = buildStopRecord({
    researchDisposition: completeness === RESEARCH_COMPLETENESS.COMMERCIALLY_SUFFICIENT
      ? RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT
      : completeness === RESEARCH_COMPLETENESS.PARTIALLY_COMPLETE_BUYER_ENGAGEMENT_NEXT
        ? RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED
        : RESEARCH_DISPOSITION.FURTHER_RESEARCH_JUSTIFIED,
    commercialNextAction: namedDecisionMakerEstablished
      ? "Engage named hotel-sourcing contact"
      : contactUsable
        ? "Introduce via general organization route; request hotel-sourcing owner"
        : "Continue public buyer/path research if justified",
    stoppingTrigger: completeness === RESEARCH_COMPLETENESS.COMMERCIALLY_SUFFICIENT
      ? STOPPING_TRIGGER.RESEARCH_SUFFICIENT
      : completeness === RESEARCH_COMPLETENESS.PARTIALLY_COMPLETE_BUYER_ENGAGEMENT_NEXT
        ? STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL
        : completeness === RESEARCH_COMPLETENESS.INCOMPLETE_ITERATION_CEILING
          ? STOPPING_TRIGGER.ITERATION_CEILING_HIT
          : STOPPING_TRIGGER.RELATED_ENTITY_FOLLOWUPS_PENDING,
    unresolvedQuestions: openMaterial.length ? openMaterial : unresolvedQuestions,
    remainingAuthorizedBudgetUsd: remainingBudgetUsd,
    note: "researchCompleteness is independent of technical stopReason / retrieval quota.",
  });

  return {
    version: COMMERCIAL_DEPTH_BUYER_V1,
    researchCompleteness: completeness,
    technicalStopReason: technicalStopReason || null,
    technicalStopIsNotCommercialExhaustion: /ITERATION_CEILING|RETRIEVAL_CAP|PUBLIC_PATH/i.test(String(technicalStopReason || "")),
    namedDecisionMakerEstablished,
    relationshipIndependentlyVerified,
    contactUsable,
    openMaterialQuestions: openMaterial,
    stopRecord,
    dimensionCoverage: assessCommercialDimensionCoverage({
      acceptedClaims: [],
      unresolvedGaps: unresolvedQuestions,
    }),
  };
}

/**
 * Audit every live Principal retrieval for commercial value (offline, from saved trace).
 */
export function auditLivePrincipalRetrievals({
  retrievals = [],
  researchQuestions = [],
  heldUrls = [],
  organizationName = "Principal. London DMC",
  organizationHost = "principallondon.com",
} = {}) {
  return (retrievals || []).map((r) => {
    const assessment = assessRetrievalCommercialValue({
      url: r.url,
      pageText: r.excerpt || r.statement || r.text || "",
      researchQuestions,
      heldUrls,
      organizationName,
      organizationHost,
      claimedRelatedEntity: "Rosewood London",
      relatedEntityFollowUp: r.relatedEntity === true || /related_entity/i.test(r.strategy || ""),
    });
    return {
      ...assessment,
      strategy: r.strategy || null,
      provider: r.provider || null,
      chars: r.chars || null,
      liveTracePublisher: r.publisher || null,
    };
  });
}
