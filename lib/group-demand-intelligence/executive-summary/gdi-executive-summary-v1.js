/**
 * GDI Executive Summary V1 — read-only aggregation.
 * Auth: FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_PHASE_1_20261127
 *
 * Customer-facing path uses CURRENT publication only (never STAGED fallback).
 * No paid research / enrichment / Surfe / Findymail / Airtable writes.
 * No synthetic ADR economics (165/195/235 must not appear as hotel facts).
 */

import {
  WEEKLY_DELTA_STATE,
  weeklyDeltaSummaryCounts,
} from "../weekly-delta.js";
import {
  CONTACT_ACTIONABILITY_TIER,
  classifyContactActionability,
} from "../presentation/gdi-contact-actionability-v1.js";
import {
  RESEARCH_RECEIPT_AVAILABILITY,
  RESEARCH_RECEIPT_ATTRIBUTION,
  buildResearchReceiptMetric,
  emptyResearchReceiptShell,
  GDI_RESEARCH_RECEIPT_CONTRACT_ID,
} from "./gdi-research-receipt-contract-v1.js";

export const GDI_EXECUTIVE_SUMMARY_V1 = "gdi-executive-summary-v1";
export const GDI_EXECUTIVE_SUMMARY_AUTH =
  "FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_PHASE_1_20261127";

export const EXEC_SUMMARY_DATA_STATE = Object.freeze({
  CURRENT_AVAILABLE: "CURRENT_AVAILABLE",
  STAGED_ONLY_NOT_CUSTOMER: "STAGED_ONLY_NOT_CUSTOMER",
  NO_ELIGIBLE_PUBLICATION: "NO_ELIGIBLE_PUBLICATION",
  FOUNDER_PREVIEW: "FOUNDER_PREVIEW",
  ERROR: "ERROR",
});

export const INVESTMENT_RECOVERY_METRIC_KEYS = Object.freeze([
  "identifiedRevenuePotential",
  "activePipelineValue",
  "bookedRevenue",
  "actualizedRevenue",
  "hotelReportedContribution",
  "dealalityInvestment",
  "revenueToInvestmentMultiple",
  "contributionBasedInvestmentRecovery",
]);

const HARDCODED_ADR_FORBIDDEN = Object.freeze([165, 195, 235]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function oppId(opp) {
  return text(opp?.id || opp?.opportunityId || "");
}

function weeklyState(opp) {
  return text(
    opp?.weeklyDeltaState || opp?.weeklyChangeState || opp?.deltaState || "",
  ).toUpperCase();
}

function isNeedPeriodAligned(opp) {
  if (opp?.needDateMatch === true) return true;
  const status = text(
    opp?.needPeriodMatchStatus ||
      opp?.needDateTimingStatus ||
      opp?.needPeriodTimingStatus ||
      "",
  ).toUpperCase();
  return (
    status.includes("CONFIRMED_PERIOD_OVERLAP") ||
    status.includes("SUPPORTED_PROBABLE_OVERLAP") ||
    status === "MATCH" ||
    status === "ALIGNED"
  );
}

function hasDeepResearchMarkers(opp) {
  return Boolean(
    opp?.deepResearchCompleted === true ||
      opp?.dossierDepth === "DEEP" ||
      opp?.researchDepth === "DEEP" ||
      (Array.isArray(opp?.evidence) && opp.evidence.length >= 3) ||
      text(opp?.hotelOpportunityThesis) ||
      text(opp?.whyThisHotel),
  );
}

function collectEvidenceUrls(opp) {
  const urls = new Set();
  for (const u of opp?.evidenceUrls || []) {
    if (/^https?:\/\//i.test(String(u))) urls.add(String(u).split("?")[0]);
  }
  for (const e of opp?.evidence || []) {
    const u = e?.sourceUrl || e?.url;
    if (/^https?:\/\//i.test(String(u))) urls.add(String(u).split("?")[0]);
  }
  for (const u of opp?.seedUrls || []) {
    if (/^https?:\/\//i.test(String(u))) urls.add(String(u).split("?")[0]);
  }
  return urls;
}

function whoHowPresentation(opp) {
  return (
    opp?.whoHow ||
    opp?.whoHowPresentation ||
    opp?.contactIntelligence ||
    opp?.drawerWhoHow ||
    null
  );
}

function countWhoHowPeople(opportunities) {
  let buyers = 0;
  let influencers = 0;
  let entryPoints = 0;
  let whoHowDone = 0;
  let personChannels = 0;

  for (const opp of opportunities) {
    const wh = whoHowPresentation(opp);
    const hasNamed =
      text(opp?.primaryContact?.name) ||
      text(opp?.contactName) ||
      text(wh?.directBuyer?.fullName) ||
      (wh?.entryPoints || []).some((e) => text(e?.fullName));
    if (wh || hasNamed) whoHowDone += 1;

    if (text(wh?.directBuyer?.fullName) || text(opp?.primaryContact?.name)) {
      const status = text(wh?.whoHowStatus || "").toUpperCase();
      if (
        status.includes("DIRECT_BUYER") ||
        opp?.primaryContact?.role === "DIRECT_BUYER" ||
        wh?.directBuyer?.fullName
      ) {
        buyers += 1;
      }
    }
    if (text(wh?.buyerInfluencer?.fullName)) influencers += 1;
    for (const ep of wh?.entryPoints || []) {
      if (text(ep?.fullName) && ep.status !== "NOT_FOUND") entryPoints += 1;
    }

    const tier =
      opp?.contactActionabilityTier ||
      classifyContactActionability(opp)?.tier ||
      null;
    if (
      tier === CONTACT_ACTIONABILITY_TIER.SALES_READY ||
      tier === CONTACT_ACTIONABILITY_TIER.REACHABLE
    ) {
      personChannels += 1;
    } else if (
      opp?.primaryContact?.linkedinUrl ||
      opp?.primaryContact?.verifiedEmail ||
      opp?.outreachChannelType
    ) {
      personChannels += 1;
    }
  }

  return { buyers, influencers, entryPoints, whoHowDone, personChannels };
}

/**
 * Reuse existing commercial signals — not a competing ranking engine.
 * Prefer published priority/score fields, then contact readiness, need-period, weekly NEW.
 */
export function prioritizeCustomerOpportunities(opportunities = [], limit = 5) {
  const scored = (opportunities || []).map((opp, idx) => {
    let score = 0;
    const pub =
      Number(opp.priorityScore) ||
      Number(opp.commercialScore) ||
      Number(opp.score) ||
      Number(opp.gdiScore) ||
      0;
    score += pub;
    const tier =
      opp.contactActionabilityTier ||
      classifyContactActionability(opp)?.tier ||
      "";
    if (tier === CONTACT_ACTIONABILITY_TIER.SALES_READY) score += 40;
    else if (tier === CONTACT_ACTIONABILITY_TIER.REACHABLE) score += 25;
    else if (tier === CONTACT_ACTIONABILITY_TIER.CONTACT_RESEARCH_REQUIRED)
      score += 10;
    if (isNeedPeriodAligned(opp)) score += 20;
    const wd = weeklyState(opp);
    if (wd === WEEKLY_DELTA_STATE.NEW) score += 15;
    if (wd === WEEKLY_DELTA_STATE.UPDATED) score += 8;
    if (text(opp.recommendedSalesAction) || text(opp.recommendedNextAction))
      score += 8;
    if (hasDeepResearchMarkers(opp)) score += 5;
    return { opp, score, idx };
  });
  scored.sort((a, b) => b.score - a.score || a.idx - b.idx);
  return scored.slice(0, Math.max(0, Math.min(5, limit))).map(({ opp, score }) => {
    const id = oppId(opp);
    const tier =
      opp.contactActionabilityTier ||
      classifyContactActionability(opp)?.tier ||
      CONTACT_ACTIONABILITY_TIER.CONTACT_IDENTIFICATION_REQUIRED;
    return {
      opportunityId: id,
      name:
        text(opp.title) ||
        text(opp.organizationName) ||
        text(opp.name) ||
        id,
      segment:
        text(opp.segmentLabel) ||
        text(opp.demandSegment) ||
        text(opp.segmentId) ||
        text(opp.researchSegment) ||
        null,
      demandTiming:
        text(opp.demandPeriodLabel) ||
        text(opp.expectedDemandPeriod) ||
        text(opp.eventDates) ||
        text(opp.timingSummary) ||
        null,
      hotelRationale:
        text(opp.hotelOpportunityThesis) ||
        text(opp.whyThisHotel) ||
        text(opp.commercialRationale) ||
        text(opp.summary) ||
        null,
      recommendedNextAction:
        text(opp.recommendedSalesAction) ||
        text(opp.recommendedNextAction) ||
        text(opp.primaryContact?.recommendedContactAction) ||
        null,
      contactActionabilityTier: tier,
      deepLinkQuery: id ? `opportunityId=${encodeURIComponent(id)}` : null,
      prioritizationScoreUsed: score,
    };
  });
}

export function buildInvestmentRecoveryPlaceholder() {
  return {
    phase: 1,
    implemented: false,
    syntheticEconomicsProhibited: true,
    hardcodedAdrValuesForbidden: [...HARDCODED_ADR_FORBIDDEN],
    metrics: INVESTMENT_RECOVERY_METRIC_KEYS.map((key) => ({
      key,
      value: null,
      availability: RESEARCH_RECEIPT_AVAILABILITY.HOTEL_INPUT_REQUIRED,
      reason:
        "Phase 1 establishes contract only; no synthetic ADR or unverified financials",
      customerVisible: false,
    })),
  };
}

export function buildCommercialEngagementSection(input = {}) {
  const pursuits = Array.isArray(input.pursuits) ? input.pursuits : null;
  const feedback = input.feedback;
  const pursuitsAvailable = pursuits != null;
  const feedbackAvailable = feedback != null && typeof feedback === "object";

  const pursued = pursuitsAvailable
    ? pursuits.filter((p) => {
        const st = text(p.status || p.pursuitStatus).toUpperCase();
        return st && st !== "NONE";
      }).length
    : null;

  let useful = null;
  let won = null;
  let lost = null;
  let rfp = null;
  let reportedRoomNights = null;

  if (feedbackAvailable) {
    const rows = Array.isArray(feedback)
      ? feedback
      : Array.isArray(feedback.entries)
        ? feedback.entries
        : Array.isArray(feedback.items)
          ? feedback.items
          : Object.values(feedback.byOpportunityId || feedback.opportunities || {});
    useful = rows.filter((r) => {
      const v = text(r.useful || r.salesOutcome || r.validation || r.rating).toUpperCase();
      return (
        r.useful === true ||
        v.includes("USEFUL") ||
        v.includes("HELPFUL") ||
        v === "YES"
      );
    }).length;
    won = rows.filter((r) =>
      /WON|BOOKED|CONFIRMED/.test(
        text(r.salesOutcome || r.outcome || r.commercialStatus).toUpperCase(),
      ),
    ).length;
    lost = rows.filter((r) =>
      /LOST|DECLINED|PASSED/.test(
        text(r.salesOutcome || r.outcome || r.commercialStatus).toUpperCase(),
      ),
    ).length;
    rfp = rows.filter((r) =>
      /RFP|PROPOSAL|SUBMITTED/.test(
        text(r.salesOutcome || r.commercialStatus || r.activity).toUpperCase(),
      ),
    ).length;
    const nights = rows
      .map((r) => Number(r.reportedRoomNights || r.roomNights || r.bookedRoomNights))
      .filter((n) => Number.isFinite(n) && n > 0);
    reportedRoomNights = nights.length ? nights.reduce((a, b) => a + b, 0) : null;
  }

  const metric = (value, available) => ({
    value: available ? value : null,
    availability: available
      ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING
      : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
    zeroMeansNoActivity: available === true,
    reason: available
      ? null
      : "No pursuit/feedback records loaded for this hotel",
  });

  return {
    opportunitiesPursued: metric(pursued, pursuitsAvailable),
    opportunitiesMarkedUseful: metric(useful, feedbackAvailable),
    rfpOrProposalActivity: metric(rfp, feedbackAvailable),
    opportunitiesWon: metric(won, feedbackAvailable),
    opportunitiesLost: metric(lost, feedbackAvailable),
    reportedRoomNights: metric(reportedRoomNights, feedbackAvailable && reportedRoomNights != null),
    note:
      "Phase 1 read-only foundation. Zero confirmed activity ≠ unavailable data.",
  };
}

export function buildResearchReceiptFromInventory(input = {}) {
  const hotelId = text(input.hotelId) || null;
  const opportunities = Array.isArray(input.opportunities)
    ? input.opportunities
    : [];
  const reportingPeriod = input.reportingPeriod || null;
  const lastVerifiedAt = input.lastVerifiedAt || null;
  const publicationPointer = input.publicationPointer || null;

  if (!input.customerAuthorizedInventory) {
    return emptyResearchReceiptShell({
      hotelId,
      reportingPeriod,
      lastVerifiedAt,
      publicationPointer,
    });
  }

  const delta = weeklyDeltaSummaryCounts(opportunities);
  const people = countWhoHowPeople(opportunities);
  const evidence = new Set();
  let deep = 0;
  let needMatched = 0;
  for (const opp of opportunities) {
    for (const u of collectEvidenceUrls(opp)) evidence.add(u);
    if (hasDeepResearchMarkers(opp)) deep += 1;
    if (isNeedPeriodAligned(opp)) needMatched += 1;
  }

  const metrics = [
    buildResearchReceiptMetric("demandSignalsEvaluated", {
      value: null,
      availability: RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.SHARED_MARKET,
      reason: "Shared-market signal telemetry not hotel-attributable yet",
    }),
    buildResearchReceiptMetric("uniqueOpportunitiesDiscovered", {
      value: opportunities.length,
      availability: RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
      reason:
        "Customer-visible CURRENT inventory count — not raw discovery volume",
    }),
    buildResearchReceiptMetric("opportunitiesDeeplyResearched", {
      value: deep,
      availability: RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    }),
    buildResearchReceiptMetric("uniqueEvidenceSourcesEvaluated", {
      value: evidence.size,
      availability: RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.MIXED,
    }),
    buildResearchReceiptMetric("opportunitiesScreenedOut", {
      value: null,
      availability: RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
      reason: "Exclusion ledger with hotel attribution required",
    }),
    buildResearchReceiptMetric("whoHowResearchPerformed", {
      value: people.whoHowDone,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    }),
    buildResearchReceiptMetric("verifiedDirectBuyers", {
      value: people.buyers,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    }),
    buildResearchReceiptMetric("verifiedBuyerInfluencers", {
      value: people.influencers,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    }),
    buildResearchReceiptMetric("verifiedAlternativeEntryPoints", {
      value: people.entryPoints,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    }),
    buildResearchReceiptMetric("verifiedPersonSpecificOutreachRoutes", {
      value: people.personChannels,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    }),
    buildResearchReceiptMetric("opportunitiesMatchedToNeedPeriods", {
      value: needMatched,
      availability:
        needMatched > 0
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
      reason:
        needMatched > 0
          ? null
          : "Need-period markers may be absent on package DTOs",
    }),
    buildResearchReceiptMetric("newlyDiscoveredIntelligence", {
      value: Number(delta.newThisWeek) || 0,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    }),
    buildResearchReceiptMetric("materiallyChangedIntelligence", {
      value: (Number(delta.updated) || 0) + (Number(delta.reactivated) || 0),
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    }),
    buildResearchReceiptMetric("existingIntelligenceReused", {
      value: null,
      availability: RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.REUSED,
      reason: "Reuse provenance stamps not yet durable",
    }),
  ];

  return {
    contractId: GDI_RESEARCH_RECEIPT_CONTRACT_ID,
    hotelId,
    reportingPeriod,
    lastVerifiedAt,
    publicationPointer,
    metrics,
    provenance: {
      measuredVsDerived: "DERIVED_FROM_CUSTOMER_VISIBLE_INVENTORY",
      sharedMarketAttributionPolicy:
        "Do not attribute full shared-market research runs to every hotel",
      syntheticCountsProhibited: true,
      source: "CURRENT_PUBLICATION_PACKAGE_OR_AUTHORIZED_PREVIEW",
    },
  };
}

export function buildExecutiveCommercialSnapshot(opportunities = [], opts = {}) {
  const delta = weeklyDeltaSummaryCounts(opportunities);
  const segments = {};
  let actionable = 0;
  let needAligned = 0;
  for (const opp of opportunities) {
    const seg =
      text(opp.segmentLabel) ||
      text(opp.demandSegment) ||
      text(opp.segmentId) ||
      "Unspecified";
    segments[seg] = (segments[seg] || 0) + 1;
    const tier =
      opp.contactActionabilityTier ||
      classifyContactActionability(opp)?.tier ||
      "";
    if (
      tier === CONTACT_ACTIONABILITY_TIER.SALES_READY ||
      tier === CONTACT_ACTIONABILITY_TIER.REACHABLE
    ) {
      actionable += 1;
    }
    if (isNeedPeriodAligned(opp)) needAligned += 1;
  }
  const prioritySegments = Object.entries(segments)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([name, count]) => ({ name, count }));

  return {
    totalQualifiedOpportunities: opportunities.length,
    newOpportunitiesSincePriorPeriod: Number(delta.newThisWeek) || 0,
    materiallyChangedOpportunities:
      (Number(delta.updated) || 0) + (Number(delta.reactivated) || 0),
    actionableOpportunities: actionable,
    opportunitiesAlignedWithNeedPeriods: needAligned,
    priorityDemandSegments: prioritySegments,
    reportingPeriod: opts.reportingPeriod || null,
    lastResearchCompletionDate: opts.lastResearchAt || null,
    distinguishesOpportunitiesFromContacts: true,
  };
}

/**
 * Build full Executive Summary payload.
 *
 * @param {object} input
 * @param {string} input.hotelId
 * @param {object[]} [input.opportunities] — customer-authorized only
 * @param {boolean} input.customerAuthorizedInventory
 * @param {string} input.dataState — EXEC_SUMMARY_DATA_STATE
 * @param {object} [input.publicationMeta]
 * @param {object[]} [input.pursuits]
 * @param {object} [input.feedback]
 * @param {string} [input.lastResearchAt]
 * @param {object} [input.reportingPeriod]
 * @param {boolean} [input.founderPreview]
 */
export function buildGdiExecutiveSummaryV1(input = {}) {
  const hotelId = text(input.hotelId);
  const dataState =
    input.dataState || EXEC_SUMMARY_DATA_STATE.NO_ELIGIBLE_PUBLICATION;
  const customerAuthorized = input.customerAuthorizedInventory === true;
  const opportunities = customerAuthorized
    ? Array.isArray(input.opportunities)
      ? input.opportunities
      : []
    : [];

  const reportingPeriod = input.reportingPeriod || {
    label: "Latest published intelligence",
    kind: "CURRENT_PUBLICATION",
  };

  const snapshot = customerAuthorized
    ? buildExecutiveCommercialSnapshot(opportunities, {
        reportingPeriod,
        lastResearchAt: input.lastResearchAt || null,
      })
    : {
        totalQualifiedOpportunities: null,
        newOpportunitiesSincePriorPeriod: null,
        materiallyChangedOpportunities: null,
        actionableOpportunities: null,
        opportunitiesAlignedWithNeedPeriods: null,
        priorityDemandSegments: [],
        reportingPeriod,
        lastResearchCompletionDate: input.lastResearchAt || null,
        distinguishesOpportunitiesFromContacts: true,
        unavailableReason:
          dataState === EXEC_SUMMARY_DATA_STATE.STAGED_ONLY_NOT_CUSTOMER
            ? "STAGED publication is not customer-authorized; CURRENT required"
            : "No eligible CURRENT publication for this hotel",
      };

  const researchReceipt = buildResearchReceiptFromInventory({
    hotelId,
    opportunities,
    customerAuthorizedInventory: customerAuthorized,
    reportingPeriod,
    lastVerifiedAt: input.lastResearchAt || input.publicationMeta?.publishedAt || null,
    publicationPointer: input.publicationMeta?.previewSource || null,
  });

  const priorityOpportunities = customerAuthorized
    ? prioritizeCustomerOpportunities(opportunities, 5)
    : [];

  return {
    ok: true,
    version: GDI_EXECUTIVE_SUMMARY_V1,
    authorizationId: GDI_EXECUTIVE_SUMMARY_AUTH,
    hotelId,
    hotelName: input.hotelName || null,
    dataState,
    founderPreview: input.founderPreview === true,
    customerFacing: customerAuthorized && !input.founderPreview,
    sections: {
      executiveCommercialSnapshot: snapshot,
      researchReceipt,
      priorityCommercialOpportunities: {
        max: 5,
        count: priorityOpportunities.length,
        items: priorityOpportunities,
        rankingNote:
          "Reuses published scores, contact actionability, need-period alignment, and weekly-delta — not a new ranking engine",
      },
      commercialEngagementAndOutcomes: buildCommercialEngagementSection({
        pursuits: input.pursuits,
        feedback: input.feedback,
      }),
      investmentRecovery: buildInvestmentRecoveryPlaceholder(),
    },
    weeklyChange: {
      engine: "weekly-delta.js",
      secondEngineProhibited: true,
      counts: customerAuthorized
        ? weeklyDeltaSummaryCounts(opportunities)
        : null,
    },
    pdfPreparation: {
      implemented: false,
      pagesRequired: 2,
      page1: "Dealality branded cover (existing report-family)",
      page2: "Executive Commercial Briefing (one page)",
      thirdPageProhibited: true,
      validationPlan:
        "Phase 4: Playwright page-count assert === 2; fail closed on overflow",
      hardcodedAdrForbidden: [...HARDCODED_ADR_FORBIDDEN],
    },
    governance: {
      noPaidResearch: true,
      noPaidEnrichment: true,
      noFindymail: true,
      noSurfeAuto: true,
      noStagedAsCustomerFallback: true,
      noSyntheticEconomics: true,
    },
  };
}
