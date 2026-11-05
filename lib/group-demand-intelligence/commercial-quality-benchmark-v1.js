/**
 * GDI 2.0 Phase 2C — commercial investigation quality benchmark (offline).
 *
 * Separates programmatic checks from commercial-quality scoring.
 * Does not invent room-night or revenue estimates. Does not call providers.
 */

import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { planBuyerRecovery } from "./buyer-recovery-planner-v1.js";
import { evaluateGdi2ShadowPublication } from "./gdi2-shadow-publication-evaluation-v1.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";

export const COMMERCIAL_QUALITY_BENCHMARK_V1 = "gdi-commercial-quality-benchmark-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function score01(ok) {
  return ok ? 1 : 0;
}

/**
 * Score one opportunity case (0–1 dimensions).
 */
export function scoreCommercialQualityCase(opportunity = {}, opts = {}) {
  const shared = evaluateSharedCommercialEligibility(opportunity, opts);
  const shadow = evaluateGdi2ShadowPublication(opportunity, {
    ...opts,
    existingSalesReady: opts.existingSalesReady === true,
    existingResearchContinuation: opts.existingResearchContinuation !== false,
  });
  const recovery = planBuyerRecovery(opportunity, {
    hotel: opts.hotel,
    attempts: opts.attempts || opportunity.buyerResearchAttempts || [],
    providerFailed: opts.providerFailed === true,
    authorizedBudgetExhausted: opts.authorizedBudgetExhausted === true,
    paidResearchAuthorized: false,
  });

  const buyerName = text(
    opportunity.primaryContact?.name || opportunity.contactName || "",
  );
  const buyerRelevant =
    shared.buyer?.responsibility?.ok === true ||
    (shared.gates?.namedRelevantBuyer &&
      !shared.gaps?.includes("BUYER_ROLE_IRRELEVANT"));

  const demandEvidenceQuality =
    shared.demand?.ok === true
      ? shared.sources?.ok
        ? 1
        : 0.7
      : text(opportunity.eventName) && !shared.demand?.ok
        ? 0.2
        : 0;

  const hotelSpecificFit = score01(
    Boolean(
      text(opportunity.whyThisHotel) ||
        text(opportunity.proposedHotelUse) ||
        text(opportunity.hotelDemandThesis) ||
        shared.gates?.realisticHotelDemand,
    ),
  );

  const continuationQuality =
    shared.researchContinuation || shared.salesReady
      ? score01(shadow.continuationDisposition?.preserveOpportunity !== false)
      : score01(shared.status === "COMMERCIALLY_INELIGIBLE");

  const strategyNovelty =
    recovery.strategyNovelty === "has_untried_or_high_gain_approaches"
      ? 1
      : recovery.strategyNovelty === "no_attempts_yet"
        ? 0.6
        : 0.3;

  const expectedGain =
    recovery.nextApproach?.expectedInformationGain != null
      ? Math.min(1, Number(recovery.nextApproach.expectedInformationGain) / 15)
      : recovery.recommendation === "STOP_WITH_EVIDENCE"
        ? 0.2
        : 0.4;

  const costAwareness = score01(
    recovery.doesNotExecutePaidTools === true &&
      recovery.costLimits?.paidResearchAuthorized === false,
  );

  const unsupportedClaimRisk =
    /\$\d|\d+\s*room.?night|guaranteed revenue|will book/i.test(
      `${opportunity.potentialHotelBusiness || ""} ${opportunity.salesBrief?.potentialBusinessForThisHotel || ""}`,
    )
      ? 0.2
      : 1;

  const duplicateResearchRisk =
    (recovery.lowInformationRepeats || []).length > 0 ? 0.3 : 1;

  const dimensions = {
    commercialEligibilityPrecision: shared.salesReady
      ? score01(shared.gates?.buyerResponsibilitySupported && shared.gates?.realisticHotelDemand)
      : score01(!shared.salesReady),
    buyerRelevanceAccuracy: buyerName
      ? score01(looksLikePersonName(buyerName) && buyerRelevant !== false)
      : score01(shared.researchContinuation),
    demandThesisEvidenceQuality: demandEvidenceQuality,
    hotelSpecificFit,
    researchContinuationQuality: continuationQuality,
    strategyNovelty,
    expectedInformationGain: expectedGain,
    costAwareness,
    unsupportedClaimFrequencyInverse: unsupportedClaimRisk,
    duplicateOrUnnecessaryResearchInverse: duplicateResearchRisk,
  };

  const values = Object.values(dimensions);
  const overall = values.reduce((a, b) => a + b, 0) / values.length;

  return {
    version: COMMERCIAL_QUALITY_BENCHMARK_V1,
    offlineOnly: true,
    opportunityRef: {
      id: opportunity.id || null,
      organizationName: text(opportunity.organizationName) || null,
      hotelId: opportunity.hotelId || null,
    },
    sharedStatus: shared.status,
    recoveryRecommendation: recovery.recommendation,
    dimensions,
    overallScore: Number(overall.toFixed(3)),
    notes: [
      "Programmatic commercial-quality proxy — not a substitute for human commercial review.",
      "Does not invent room-night or revenue estimates.",
    ],
  };
}

/**
 * Run a fixed offline fixture suite for adaptive research + eligibility quality.
 */
export function runCommercialQualityBenchmarkSuite() {
  const cases = [
    {
      id: "missing_buyer",
      label: "Missing commercially relevant buyer",
      opportunity: {
        id: "bench_missing_buyer",
        researchSegment: "scaling_companies",
        organizationName: "Bench Co",
        growthEvidenceDate: "2026-07-01",
        activityHeadline: "Opened office",
        potentialHotelBusiness: "Teams need guestrooms near office",
        proposedHotelUse: "Transient lodging at this hotel",
        whyThisHotel: "Near office",
        organizationContactEmail: "info@bench.example",
        sources: [{ url: "https://news.example/bench-office" }],
        evidenceQuote: "Bench opened an office.",
      },
      expect: { salesReady: false, continuation: true },
    },
    {
      id: "unknown_responsibility",
      label: "Unknown buyer responsibility",
      opportunity: {
        id: "bench_unknown_role",
        researchSegment: "scaling_companies",
        organizationName: "Role Gap Inc",
        growthEvidenceDate: "2026-07-01",
        activityHeadline: "Expansion",
        potentialHotelBusiness: "Training cohorts need guestrooms and meeting rooms",
        proposedHotelUse: "Meetings + lodging",
        whyThisHotel: "Near HQ",
        primaryContact: { name: "Casey Nameonly", role: "", email: "casey@rolegap.example" },
        contactEmail: "casey@rolegap.example",
        sources: [{ url: "https://news.example/rolegap" }],
        evidenceQuote: "Role Gap expanded.",
      },
      expect: { salesReady: false, gapIncludes: "BUYER_RESPONSIBILITY_UNKNOWN" },
    },
    {
      id: "role_on_contactRole_field",
      label: "Role lives on contactRole (mapping calibration)",
      opportunity: {
        id: "bench_contact_role_field",
        researchSegment: "scaling_companies",
        organizationName: "Mapped Role Co",
        growthEvidenceDate: "2026-07-01",
        activityHeadline: "Opened site",
        potentialHotelBusiness: "Visiting teams need guestrooms",
        proposedHotelUse: "Lodging at this hotel",
        whyThisHotel: "Near site",
        primaryContact: { name: "Jordan Mapped", email: "jordan@mapped.example" },
        contactRole: "Facilities Director",
        contactEmail: "jordan@mapped.example",
        sources: [{ url: "https://news.example/mapped" }],
        evidenceQuote: "Mapped Role opened a site.",
      },
      expect: { salesReady: true },
    },
    {
      id: "missing_route",
      label: "Missing contact route",
      opportunity: {
        id: "bench_missing_route",
        researchSegment: "scaling_companies",
        organizationName: "No Route LLC",
        growthEvidenceDate: "2026-07-01",
        activityHeadline: "Opened office",
        potentialHotelBusiness: "Guestrooms for project teams",
        proposedHotelUse: "Transient",
        whyThisHotel: "Near office",
        primaryContact: { name: "Sam Facilities", role: "Facilities Manager" },
        sources: [{ url: "https://news.example/noroute" }],
        evidenceQuote: "No Route opened an office.",
      },
      expect: { salesReady: false, gapIncludes: "MISSING_CONTACT_ROUTE" },
    },
    {
      id: "event_no_hotel_need",
      label: "Event with no clear hotel need",
      opportunity: {
        id: "bench_event_no_demand",
        researchSegment: "associations_government",
        associationEvents: true,
        organizationName: "Abstract Society",
        eventName: "Annual Thought Forum 2027",
        hotelDemandThesis: "Interesting industry gathering downtown",
        contactName: "Morgan Meetings",
        contactRole: "Meetings Director",
        contactEmail: "meetings@abstract.example",
        sources: [{ url: "https://abstract.example/forum" }],
        evidenceQuote: "Forum announced.",
      },
      expect: { salesReady: false, gapIncludes: "MISSING_HOTEL_DEMAND_THESIS" },
    },
    {
      id: "project_temp_lodging",
      label: "Project with potential temporary lodging",
      opportunity: {
        id: "bench_project",
        researchSegment: "scaling_companies",
        organizationName: "DeployCo",
        growthEvidenceDate: "2026-06-01",
        activityHeadline: "Mobilizing project crew",
        potentialHotelBusiness: "Extended-stay guestrooms for visiting project teams",
        proposedHotelUse: "Project crew lodging at this hotel",
        whyThisHotel: "Near worksite",
        primaryContact: { name: "Alex Ops", role: "Project Operations Manager", email: "alex@deploy.example" },
        contactEmail: "alex@deploy.example",
        sources: [{ url: "https://news.example/deployco" }],
        evidenceQuote: "DeployCo mobilizing crew.",
      },
      expect: { salesReady: true },
    },
    {
      id: "provider_failure",
      label: "Provider failure incomplete",
      opportunity: {
        id: "bench_provider_fail",
        researchSegment: "scaling_companies",
        organizationName: "Provider Gap Co",
        growthEvidenceDate: "2026-05-01",
        activityHeadline: "Opened office",
        potentialHotelBusiness: "Guestrooms for visiting staff",
        proposedHotelUse: "Transient",
        whyThisHotel: "Near office",
        primaryContact: { name: "Pat", role: "" },
        organizationContactEmail: "info@providergap.example",
        sources: [{ url: "https://news.example/pg" }],
        evidenceQuote: "Opened office.",
      },
      opts: { providerFailed: true },
      expect: { salesReady: false, exhausted: false },
    },
    {
      id: "strategy_exhaustion",
      label: "Research strategy exhaustion",
      opportunity: {
        id: "bench_exhausted_strategies",
        researchSegment: "scaling_companies",
        organizationName: "Exhaust Co",
        growthEvidenceDate: "2026-05-01",
        activityHeadline: "Opened office",
        potentialHotelBusiness: "Guestrooms for staff",
        proposedHotelUse: "Transient",
        whyThisHotel: "Near office",
        primaryContact: { name: "Lee", role: "" },
        organizationContactEmail: "info@exhaust.example",
        sources: [{ url: "https://news.example/ex" }],
        evidenceQuote: "Opened office.",
        buyerResearchAttempts: [
          { strategyId: "OFFICIAL_LEADERSHIP_PAGES", informationFound: false },
          { strategyId: "OFFICIAL_LEADERSHIP_PAGES", informationFound: false },
          { strategyId: "WORKPLACE_FACILITIES_REAL_ESTATE", informationFound: false },
          { strategyId: "PEOPLE_HR_TRAINING", informationFound: false },
          { strategyId: "MEETINGS_INCENTIVES_SALES_OPS", informationFound: false },
          { strategyId: "REGIONAL_OPS_EXPANSION", informationFound: false },
          { strategyId: "PROCUREMENT_ADMIN_EA", informationFound: false },
          { strategyId: "PRESS_NAMED_THEN_VERIFY_AFFILIATION", informationFound: false },
          { strategyId: "PUBLIC_PROFESSIONAL_PROFILES", informationFound: false },
          { strategyId: "ORG_ROUTE_ASK_FOR_NAMED", informationFound: false },
        ],
      },
      expect: { salesReady: false },
    },
    {
      id: "international_multilingual",
      label: "International / multilingual investigation",
      opportunity: {
        id: "bench_intl",
        researchSegment: "associations_government",
        associationEvents: true,
        organizationName: "Sociedad Clínica Regional",
        eventName: "Congreso Anual 2027",
        market: "México",
        hotelDemandThesis: "Bloques de hospedaje y banquetes para delegados",
        proposedHotelUse: "Hospedaje overflow y reuniones",
        contactName: "María Soto",
        contactRole: "Directora de Eventos",
        contactEmail: "eventos@sociedad.example",
        sources: [{ url: "https://sociedad.example/congreso" }],
        evidenceQuote: "El congreso busca bloques hoteleros.",
      },
      expect: { salesReady: true },
    },
    {
      id: "qualified_baseline",
      label: "Fully qualified scaling baseline",
      opportunity: {
        id: "bench_qualified",
        researchSegment: "scaling_companies",
        organizationName: "Ready Co",
        growthEvidenceDate: "2026-08-01",
        activityHeadline: "Opened Bethesda office",
        potentialHotelBusiness: "Project teams need guestrooms and meetings",
        proposedHotelUse: "Transient + meetings at this hotel",
        whyThisHotel: "Walking distance to office",
        primaryContact: {
          name: "Riley Buyer",
          role: "Workplace Operations Manager",
          email: "riley@ready.example",
        },
        contactEmail: "riley@ready.example",
        sources: [{ url: "https://news.example/ready" }],
        evidenceQuote: "Ready Co opened a Bethesda office.",
      },
      expect: { salesReady: true },
    },
  ];

  const results = [];
  for (const c of cases) {
    const shared = evaluateSharedCommercialEligibility(c.opportunity);
    const recovery = planBuyerRecovery(c.opportunity, {
      ...(c.opts || {}),
      attempts: c.opportunity.buyerResearchAttempts || [],
    });
    const quality = scoreCommercialQualityCase(c.opportunity, c.opts || {});
    const checks = [];
    if (c.expect.salesReady != null) {
      checks.push({
        name: "salesReady",
        ok: shared.salesReady === c.expect.salesReady,
        expected: c.expect.salesReady,
        actual: shared.salesReady,
      });
    }
    if (c.expect.continuation != null) {
      checks.push({
        name: "continuation",
        ok: shared.researchContinuation === c.expect.continuation,
        expected: c.expect.continuation,
        actual: shared.researchContinuation,
      });
    }
    if (c.expect.gapIncludes) {
      checks.push({
        name: "gapIncludes",
        ok: (shared.gaps || []).includes(c.expect.gapIncludes),
        expected: c.expect.gapIncludes,
        actual: shared.gaps,
      });
    }
    if (c.expect.exhausted === false) {
      checks.push({
        name: "notExhausted",
        ok: recovery.continuationDisposition?.commerciallyExhausted !== true,
        expected: false,
        actual: recovery.continuationDisposition?.commerciallyExhausted,
      });
    }
    results.push({
      id: c.id,
      label: c.label,
      programmaticOk: checks.every((x) => x.ok),
      checks,
      sharedStatus: shared.status,
      recoveryRecommendation: recovery.recommendation,
      nextApproachId: recovery.nextApproach?.id || null,
      strategyNovelty: recovery.strategyNovelty,
      qualityOverall: quality.overallScore,
      qualityDimensions: quality.dimensions,
    });
  }

  return {
    version: COMMERCIAL_QUALITY_BENCHMARK_V1,
    generatedAt: new Date().toISOString(),
    caseCount: results.length,
    programmaticPassCount: results.filter((r) => r.programmaticOk).length,
    meanQualityScore: Number(
      (
        results.reduce((a, r) => a + Number(r.qualityOverall || 0), 0) /
        Math.max(1, results.length)
      ).toFixed(3),
    ),
    results,
    note:
      "Programmatic pass ≠ independent commercial-quality certification. Human review still required for production claims.",
  };
}
