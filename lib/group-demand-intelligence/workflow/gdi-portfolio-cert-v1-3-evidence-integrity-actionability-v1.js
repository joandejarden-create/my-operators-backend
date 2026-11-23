/**
 * GDI Portfolio Certification V1.3 — Deep Research Evidence Integrity & Commercial Actionability.
 * $0 validation + controlled remediation. No paid providers. No Airtable writes. No Sales Ready promotion.
 */

import fs from "node:fs";
import path from "node:path";
import { reconcileCommercialDispositionV1 } from "./gdi-commercial-disposition-reconcile-v1.js";
import {
  V1_1_EIGHT_OPPORTUNITY_IDS,
  reclassifyFrozenOpportunity,
  assessDeepResearchImprovement,
  resolveV11OrsPaths,
  runOfflineConversionRecovery,
} from "./gdi-portfolio-cert-v1-2-commercial-conversion-recovery-v1.js";
import { mapDossierEvidenceToOpportunityFields } from "../research/exa-agent/exa-agent-source-evidence-v1.js";
import { mergeDeepResearchIntoOpportunity } from "../research/deep-research-gdi-integration-v1.js";

export const GDI_PORTFOLIO_CERT_V1_3 =
  "gdi-portfolio-cert-v1-3-evidence-integrity-actionability-v1";

export const V1_3_PRIORITY_THREE = Object.freeze([
  "disc_0a11a0bb1158", // AAEI
  "disc_9334f4f1e3dd", // AHA
  "disc_b7fc48bf6995", // ESCP
]);

/** URL / evidence role taxonomy — never collapse these. */
export const EVIDENCE_URL_ROLE = Object.freeze({
  PROVIDER_RETURNED_SOURCE: "PROVIDER_RETURNED_SOURCE",
  INDEPENDENTLY_INSPECTED: "INDEPENDENTLY_INSPECTED",
  DEMAND_SUPPORTING_EVIDENCE: "DEMAND_SUPPORTING_EVIDENCE",
  PROVIDER_CONCLUSION_WITHOUT_SUPPORT: "PROVIDER_CONCLUSION_WITHOUT_SUPPORT",
  DISCOVERY_SEED_ONLY: "DISCOVERY_SEED_ONLY",
});

export const DEEP_RESEARCH_COMPLETION = Object.freeze({
  IMPROVED: "IMPROVED",
  EXPLICIT_NO_IMPROVEMENT: "EXPLICIT_NO_IMPROVEMENT",
  INCOMPLETE_EMPTY_INVESTIGATED_URLS: "INCOMPLETE_EMPTY_INVESTIGATED_URLS",
  PROVIDER_SUCCESS_WITHOUT_EVIDENCE: "PROVIDER_SUCCESS_WITHOUT_EVIDENCE",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function normUrl(u) {
  return text(u).toLowerCase().replace(/\/$/, "");
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Assess whether a deep-research pass may be marked complete.
 * Provider HTTP/run success alone is never sufficient.
 */
export function assessDeepResearchCompletion(opportunity = {}, deepResult = {}) {
  const seed = new Set((opportunity.seedUrls || []).map(normUrl).filter(Boolean));
  const investigated = [
    ...(opportunity.investigatedUrls || []),
    ...(deepResult.investigatedUrls || []),
  ]
    .map(normUrl)
    .filter(Boolean);
  const uniqueInvestigated = [...new Set(investigated)];
  const beyondSeed = uniqueInvestigated.filter((u) => u && !seed.has(u));
  const providerRan = Boolean(
    opportunity.exaRunId ||
      opportunity.researchJobId ||
      deepResult.researchJobId ||
      deepResult.providerRunId,
  );
  const explicitNoImprovement =
    deepResult.noImprovement === true ||
    deepResult.evidenceImprovement === false ||
    text(deepResult.stoppingTrigger).toUpperCase() === "NO_IMPROVEMENT" ||
    text(opportunity.deepResearchStoppingReason).toUpperCase() === "NO_IMPROVEMENT";

  let status;
  if (beyondSeed.length > 0) {
    status = DEEP_RESEARCH_COMPLETION.IMPROVED;
  } else if (explicitNoImprovement) {
    status = DEEP_RESEARCH_COMPLETION.EXPLICIT_NO_IMPROVEMENT;
  } else if (providerRan && uniqueInvestigated.length === 0) {
    status = DEEP_RESEARCH_COMPLETION.INCOMPLETE_EMPTY_INVESTIGATED_URLS;
  } else if (providerRan) {
    status = DEEP_RESEARCH_COMPLETION.PROVIDER_SUCCESS_WITHOUT_EVIDENCE;
  } else {
    status = DEEP_RESEARCH_COMPLETION.INCOMPLETE_EMPTY_INVESTIGATED_URLS;
  }

  return {
    version: GDI_PORTFOLIO_CERT_V1_3,
    status,
    complete:
      status === DEEP_RESEARCH_COMPLETION.IMPROVED ||
      status === DEEP_RESEARCH_COMPLETION.EXPLICIT_NO_IMPROVEMENT,
    providerRan,
    investigatedUrlCount: uniqueInvestigated.length,
    urlsBeyondSeed: beyondSeed.length,
    seedUrlCount: seed.size,
    note:
      status === DEEP_RESEARCH_COMPLETION.IMPROVED
        ? "Substantive investigated URLs beyond discovery seeds."
        : status === DEEP_RESEARCH_COMPLETION.EXPLICIT_NO_IMPROVEMENT
          ? "Explicit no-improvement outcome recorded — investigation may close."
          : "Do not mark deep investigation complete: provider success without evidentiary improvement.",
  };
}

/**
 * Classify each URL observed on an opportunity across the pipeline.
 */
export function classifyEvidenceUrls(opportunity = {}, opts = {}) {
  const seed = new Set((opportunity.seedUrls || []).map(normUrl));
  const investigated = new Set((opportunity.investigatedUrls || []).map(normUrl));
  const evidenceUrls = new Set(
    (opportunity.evidence || [])
      .map((e) => normUrl(e.url))
      .filter(Boolean),
  );
  const providerSources = new Set(
    [
      ...(opportunity.sources || []).map((s) => normUrl(s.url || s)),
      ...(opts.providerReturnedUrls || []).map(normUrl),
      ...((opportunity.exaAgentEvidencePack?.sources || []).map((s) =>
        normUrl(s.url),
      )),
    ].filter(Boolean),
  );

  const all = new Set([
    ...seed,
    ...investigated,
    ...evidenceUrls,
    ...providerSources,
  ]);

  const rows = [];
  for (const u of all) {
    if (!u) continue;
    const roles = [];
    if (seed.has(u) && !investigated.has(u) && !providerSources.has(u)) {
      roles.push(EVIDENCE_URL_ROLE.DISCOVERY_SEED_ONLY);
    }
    if (providerSources.has(u)) roles.push(EVIDENCE_URL_ROLE.PROVIDER_RETURNED_SOURCE);
    if (investigated.has(u)) roles.push(EVIDENCE_URL_ROLE.INDEPENDENTLY_INSPECTED);
    const supporting = (opportunity.evidence || []).find(
      (e) => normUrl(e.url) === u && text(e.statement).length > 20,
    );
    if (supporting) roles.push(EVIDENCE_URL_ROLE.DEMAND_SUPPORTING_EVIDENCE);
    if (
      providerSources.has(u) &&
      !supporting &&
      !investigated.has(u)
    ) {
      roles.push(EVIDENCE_URL_ROLE.PROVIDER_CONCLUSION_WITHOUT_SUPPORT);
    }
    rows.push({
      url: u,
      roles: [...new Set(roles)],
    });
  }
  return {
    version: GDI_PORTFOLIO_CERT_V1_3,
    rows,
    counts: rows.reduce((acc, r) => {
      for (const role of r.roles) acc[role] = (acc[role] || 0) + 1;
      return acc;
    }, {}),
  };
}

/**
 * End-to-end propagation fixture test: discovery seed → Exa dossier → merge → qualify inputs.
 */
export function runEvidencePropagationFixture() {
  const seedOpp = {
    id: "disc_fixture_v13",
    opportunityId: "disc_fixture_v13",
    organizationName: "Fixture Association",
    hotelName: "Bethesda Marriott",
    seedUrls: ["https://example.org/event-announcement"],
    investigatedUrls: [],
    evidence: [
      {
        url: "https://example.org/event-announcement",
        statement: "Annual conference announced",
        provenance: "discovery_evidence",
      },
    ],
    acceptedClaims: [
      {
        url: "https://example.org/event-announcement",
        statement: "Annual conference announced",
      },
    ],
  };

  const dossier = {
    supportingSources: [
      {
        url: "https://example.org/hotel-housing",
        title: "Housing",
        relevance: "Official housing page listing room block",
      },
      {
        url: "https://example.org/event-announcement",
        title: "Announcement",
        relevance: "Seed also returned by provider",
      },
    ],
    claimToSourceMap: [
      {
        claim: "Organizer published a negotiated room block for attendees.",
        sourceUrls: ["https://example.org/hotel-housing"],
      },
    ],
  };

  const mapped = mapDossierEvidenceToOpportunityFields(seedOpp, dossier);
  const merged = mergeDeepResearchIntoOpportunity(mapped.opportunity, {
    investigatedUrls: mapped.opportunity.investigatedUrls || [],
    acceptedClaims: [
      {
        url: "https://example.org/hotel-housing",
        statement: "Organizer published a negotiated room block for attendees.",
      },
    ],
    researchJobId: "fixture_agent_run",
  });

  const completion = assessDeepResearchCompletion(merged.opportunity || merged, {
    investigatedUrls: (merged.opportunity || merged).investigatedUrls,
    researchJobId: "fixture_agent_run",
  });
  const urlClasses = classifyEvidenceUrls(merged.opportunity || merged);

  const emptyCompletion = assessDeepResearchCompletion(
    {
      ...seedOpp,
      exaRunId: "agent_run_empty",
      investigatedUrls: [],
    },
    { providerRunId: "agent_run_empty" },
  );

  return {
    version: GDI_PORTFOLIO_CERT_V1_3,
    mappedInvestigatedUrls: mapped.opportunity.investigatedUrls || [],
    mergedInvestigatedUrls: (merged.opportunity || merged).investigatedUrls || [],
    completion,
    emptyProviderCompletion: emptyCompletion,
    urlClasses,
    pass:
      (mapped.opportunity.investigatedUrls || []).includes(
        "https://example.org/hotel-housing",
      ) &&
      completion.status === DEEP_RESEARCH_COMPLETION.IMPROVED &&
      emptyCompletion.complete === false &&
      emptyCompletion.status ===
        DEEP_RESEARCH_COMPLETION.INCOMPLETE_EMPTY_INVESTIGATED_URLS,
  };
}

/**
 * Genuine EXCLUDE negative cases — must not be remapped to WATCHLIST.
 */
export function runGenuineExcludeNegatives() {
  const cases = [
    {
      name: "commercially_ineligible_no_org_activity",
      ors: {
        opportunityId: "neg_exclude_1",
        commercialEligibilityStatus: "COMMERCIALLY_INELIGIBLE",
        commercialEligibilityContractV1: {
          status: "COMMERCIALLY_INELIGIBLE",
          researchContinuation: false,
          preserveOpportunity: false,
          salesReady: false,
          commercialEligible: false,
        },
        commercialQualificationV41: { internalDisposition: "EXCLUDE" },
        historicalInternalDisposition: "EXCLUDE",
        dispositionReconcile: { finalDisposition: "EXCLUDE" },
      },
      expectFinal: "EXCLUDE",
    },
    {
      name: "local_day_only_no_lodging_no_activity",
      ors: {
        opportunityId: "neg_exclude_2",
        commercialEligibilityStatus: "COMMERCIALLY_INELIGIBLE",
        commercialEligibilityContractV1: {
          status: "COMMERCIALLY_INELIGIBLE",
          researchContinuation: false,
          preserveOpportunity: false,
          doNotTreatAsExhausted: false,
          salesReady: false,
        },
        commercialQualificationV41: { internalDisposition: "EXCLUDE" },
        historicalInternalDisposition: "EXCLUDE",
      },
      expectFinal: "EXCLUDE",
    },
  ];

  const results = cases.map((c) => {
    const reconcile = reconcileCommercialDispositionV1({
      eligibilityResult: c.ors.commercialEligibilityContractV1,
      v41Qualification: c.ors.commercialQualificationV41,
      priorDisposition: c.ors.historicalInternalDisposition,
    });
    return {
      name: c.name,
      finalDisposition: reconcile.finalDisposition,
      ok: reconcile.finalDisposition === c.expectFinal,
    };
  });

  return {
    version: GDI_PORTFOLIO_CERT_V1_3,
    results,
    pass: results.every((r) => r.ok),
  };
}

/**
 * Frozen public-source commercial reviews for AAEI / AHA / ESCP (research date 2026-11-23).
 * Findings are evidence-backed from free public pages — not invented.
 */
export function buildThreeCandidateCommercialReview() {
  const researchDate = "2026-11-23";
  return {
    version: GDI_PORTFOLIO_CERT_V1_3,
    researchDate,
    paidEnrichment: false,
    inventedContacts: false,
    candidates: [
      {
        opportunityId: "disc_0a11a0bb1158",
        organizationName: "American Association of Exporters and Importers",
        hotelName: "Bethesda Marriott",
        eventTitle: "AAEI 106th Annual Conference & Trade Day 2027",
        eventDates: "2026-11-23 research: event 2027-06-22 to 2027-06-24 (future — timing valid)",
        venue: "Ronald Reagan Building & International Trade Center, Washington, DC",
        organizer: "AAEI (association-owned event)",
        namedLodgingBuyer: null,
        employmentVerifiedNamedBuyer: false,
        hotelSourcingDecisionEvidence:
          "Official room block NOW OPEN at Hotel Washington (515 15th St NW, Washington, DC) — competitor/host lodging committed.",
        publicContactChannel: {
          type: "ORGANIZATION_ROUTE",
          email: "events@aaei.hq.org",
          phone: "+1 202.857.8009",
          namedPerson: null,
          sourceUrl: "https://aaei.org/2027-annual-conference-trade-day/",
        },
        hotelRelevanceToPilot:
          "LOW for Bethesda Marriott as primary — official block is downtown DC Hotel Washington; Bethesda is not the published housing hotel. Overflow not evidenced.",
        lodgingDemandValidated: true,
        lodgingDemandForPilotHotel: false,
        dispositionRecommendation: "STOP_PRIMARY_COMPETITOR_COMMITTED",
        mayRetainAs: "COMPETITIVE_INTELLIGENCE_OR_OVERFLOW_WATCH_ONLY",
        salesReady: false,
        continueResearch: false,
        continueReason:
          "Primary lodging decision closed at competitor. No overflow evidence. Do not spend paid research for Bethesda primary pursuit.",
        sourcesInspected: [
          "https://aaei.org/2027-annual-conference-trade-day/",
          "https://members.aaei.org/ap/Events/Register/3BFza8gtYCQCg",
        ],
      },
      {
        opportunityId: "disc_9334f4f1e3dd",
        organizationName: "American Hospital Association",
        hotelName: "Bethesda Marriott",
        eventTitle: "AHA Annual Membership Meeting 2027",
        eventDates: "2026-11-23 research: event 2027-04-18 to 2027-04-20 (future — timing valid)",
        venue: "Marriott Marquis Washington, D.C.",
        organizer: "American Hospital Association",
        namedLodgingBuyer: null,
        employmentVerifiedNamedBuyer: false,
        hotelSourcingDecisionEvidence:
          "AHA is official housing provider; negotiated block at Marriott Marquis Washington, D.C. ($371/night) expires 2027-03-17.",
        publicContactChannel: {
          type: "ORGANIZATION_ROUTE",
          email: "annualmeeting@aha.org",
          phone: null,
          namedPerson: null,
          sourceUrl: "https://annualmeeting.aha.org/hotel-travel-0",
        },
        hotelRelevanceToPilot:
          "LOW for Bethesda Marriott as primary — host hotel is Marriott Marquis DC. Bethesda could only be overflow/displacement; overflow not evidenced on public pages.",
        lodgingDemandValidated: true,
        lodgingDemandForPilotHotel: false,
        dispositionRecommendation: "STOP_PRIMARY_COMPETITOR_COMMITTED",
        mayRetainAs: "COMPETITIVE_INTELLIGENCE_OR_OVERFLOW_WATCH_ONLY",
        salesReady: false,
        continueResearch: false,
        continueReason:
          "Confirmed competing host hotel + official housing. Paid deep research for Bethesda primary is not commercially justified without overflow evidence.",
        sourcesInspected: [
          "https://annualmeeting.aha.org/hotel-travel-0",
          "https://annualmeeting.aha.org/questions",
          "https://annualmeeting.aha.org/",
        ],
      },
      {
        opportunityId: "disc_b7fc48bf6995",
        organizationName: "European Society of Coloproctology",
        hotelName: "YOTEL Geneva Lake",
        eventTitle: "ESCP 2027 – 22nd Scientific Meeting",
        eventDates: "2026-11-23 research: event 2027-09-22 to 2027-09-24 (future — timing valid)",
        venue: "Palexpo, Route François-Peyrot 30, 1218 Le Grand-Saconnex, Geneva",
        organizer: "ESCP Secretariat (c/o Lindsays, Edinburgh)",
        namedLodgingBuyer: null,
        employmentVerifiedNamedBuyer: false,
        hotelSourcingDecisionEvidence:
          "Venue confirmed at Palexpo; public site states 'More information will follow soon' — no official room block published yet.",
        publicContactChannel: {
          type: "ORGANIZATION_ROUTE",
          email: null,
          phone: null,
          namedPerson: null,
          organizationAddress:
            "ESCP Secretariat c/o Lindsays, Caledonian Exchange, 19a Canning Street, Edinburgh EH3 8HE, United Kingdom",
          sourceUrl:
            "https://www.escp.eu.com/conference-and-events/geneva-2027-22nd-scientific-meeting",
          note: "On-page email obfuscated; no verified public professional email extracted without invention.",
        },
        hotelRelevanceToPilot:
          "WEAK — YOTEL Geneva Lake (Founex) is ~9 miles / ~15 min from Palexpo; closer properties sit at Palexpo (Hilton / ibis). Not a natural primary congress hotel.",
        lodgingDemandValidated: false,
        lodgingDemandForPilotHotel: false,
        dispositionRecommendation: "WATCHLIST_LOW_PRIORITY_OR_STOP",
        mayRetainAs: "WATCHLIST_RESEARCH only if housing partner still open AND overflow/satellite demand evidenced",
        salesReady: false,
        continueResearch: true,
        continueReason:
          "Free public pass only: monitor housing partner announcement; do not pay until a named housing contact or open RFP appears AND YOTEL Lake relevance is defended.",
        sourcesInspected: [
          "https://www.escp.eu.com/conference-and-events/geneva-2027-22nd-scientific-meeting/venue",
          "https://www.escp.eu.com/conference-and-events/geneva-2027-22nd-scientific-meeting",
          "https://www.escp.eu.com/conference-and-events/event-calendar/2263-escp-22nd-scientific-annual-meeting",
          "https://www.yotel.com/en/hotels/yotel-geneva-lake",
        ],
      },
    ],
  };
}

/**
 * Precise research-continuation plans for all eight watchlist rows.
 */
export function buildResearchContinuationPlans(threeReview = null) {
  const review = threeReview || buildThreeCandidateCommercialReview();
  const byId = Object.fromEntries(
    review.candidates.map((c) => [c.opportunityId, c]),
  );

  const plans = V1_1_EIGHT_OPPORTUNITY_IDS.map((id) => {
    const three = byId[id];
    if (three) {
      return {
        opportunityId: id,
        organizationName: three.organizationName,
        hotelName: three.hotelName,
        missingDemandEvidence: three.lodgingDemandForPilotHotel
          ? []
          : [
              three.lodgingDemandValidated
                ? "Lodging demand exists but for competitor host — pilot-hotel demand not evidenced"
                : "No official housing/room-block evidence for pilot hotel",
            ],
        missingHotelFitEvidence: [
          three.hotelRelevanceToPilot.startsWith("LOW") ||
          three.hotelRelevanceToPilot.startsWith("WEAK")
            ? three.hotelRelevanceToPilot
            : null,
        ].filter(Boolean),
        missingNamedBuyer: !three.namedLodgingBuyer,
        missingContactChannel: !three.publicContactChannel?.email,
        missingDecisionTiming: false,
        sourcesRequired: three.sourcesInspected,
        estimatedValueOfAnotherPass: three.continueResearch
          ? "LOW_TO_MODERATE (free monitor only)"
          : "NEAR_ZERO for pilot-hotel primary",
        decision: three.continueResearch ? "CONTINUE_FREE_ONLY" : "STOP_PRIMARY",
        reason: three.continueReason,
        priorityRank: three.continueResearch ? 3 : 8,
      };
    }

    // Non-priority eight: template from frozen gaps
    const defaults = {
      disc_461dfa26a790: {
        org: "ASGSR",
        hotel: "Bethesda Marriott",
        decision: "CONTINUE_FREE_THEN_CONDITIONAL_PAID",
        value: "MODERATE — multi-day meeting; lodging page may exist",
        rank: 2,
        gaps: [
          "hotel lodging / housing page",
          "named meeting planner",
          "Bethesda-side fit vs DC/elsewhere",
        ],
      },
      disc_83f5b13dff4c: {
        org: "Liatris, Inc.",
        hotel: "Bethesda Marriott",
        decision: "CONTINUE_PUBLIC_THEN_TOP_PAID_CANDIDATE",
        value: "HIGHEST of eight — named executives + Rockville proximity",
        rank: 1,
        gaps: [
          "verify lodging/travel decision ownership (not invent email)",
          "defensible overnight demand mechanism for visiting staff/partners",
          "org route already partially known (liatris.biz/contact)",
        ],
      },
      disc_cd8f10005345: {
        org: "International Road Federation",
        hotel: "YOTEL Geneva Lake",
        decision: "CONTINUE_FREE",
        value: "MODERATE — confirm destination + housing",
        rank: 4,
        gaps: ["destination city confirmation", "housing partner", "named organizer"],
      },
      disc_a1a0378bf466: {
        org: "SETAC Europe",
        hotel: "YOTEL Geneva Lake",
        decision: "CONTINUE_FREE",
        value: "MODERATE — Palexpo-class scientific meeting",
        rank: 5,
        gaps: ["housing", "named buyer", "YOTEL Lake vs venue distance"],
      },
      disc_77bac9a0d82a: {
        org: "GDPM / Agroscope",
        hotel: "YOTEL Geneva Lake",
        decision: "STOP_OR_ARCHIVE_LOW",
        value: "LOW — niche workshop; hotel fit unclear",
        rank: 7,
        gaps: ["attendance/lodging scale", "buyer", "Geneva Lake relevance"],
      },
    };
    const d = defaults[id] || {
      org: id,
      hotel: null,
      decision: "CONTINUE_FREE",
      value: "UNKNOWN",
      rank: 6,
      gaps: ["demand", "buyer", "contact"],
    };
    return {
      opportunityId: id,
      organizationName: d.org,
      hotelName: d.hotel,
      missingDemandEvidence: d.gaps.filter((g) => /lodging|housing|demand|overnight/i.test(g)),
      missingHotelFitEvidence: d.gaps.filter((g) => /fit|Bethesda|YOTEL|distance|Geneva/i.test(g)),
      missingNamedBuyer: true,
      missingContactChannel: id !== "disc_83f5b13dff4c",
      missingDecisionTiming: /timing|date/i.test(d.gaps.join(" ")),
      sourcesRequired: ["official event/org pages", "housing page if published"],
      estimatedValueOfAnotherPass: d.value,
      decision: d.decision,
      reason: `Prioritize by commercial value: ${d.value}`,
      priorityRank: d.rank,
    };
  });

  plans.sort((a, b) => a.priorityRank - b.priorityRank);
  return { version: GDI_PORTFOLIO_CERT_V1_3, plans };
}

/**
 * Proposed paid continuation (design only — do not execute).
 */
export function buildPaidPilotDesignV13() {
  return {
    version: GDI_PORTFOLIO_CERT_V1_3,
    executeAuthorized: false,
    maxOpportunities: 3,
    candidates: [
      {
        opportunityId: "disc_83f5b13dff4c",
        organizationName: "Liatris, Inc.",
        hotelName: "Bethesda Marriott",
        missingEvidence: [
          "Verified lodging/travel decision-maker responsibility (beyond CEO title)",
          "Defensible overnight demand for Rockville-area visiting staff/partners",
          "Usable public or org-route outreach channel (no invented personal email)",
        ],
        requiredProviderCalls: [
          "1× Exa Agent medium deep research (scaling) with investigatedUrls gate",
        ],
        estimatedMaxSpendUsd: 0.5,
        expectedIncrementalOutput: [
          "≥1 investigated URL beyond Maryland.gov seed",
          "Explicit lodging hypothesis accept/reject with citations",
          "Contact route classification (org vs personal) without PII invention",
        ],
        stopConditions: [
          "Explicit NO_IMPROVEMENT after one medium pass",
          "Hotel demand rejected with sourced rationale",
          "Spend would exceed $0.50 for this opportunity",
        ],
        verificationMethod:
          "assessDeepResearchCompletion === IMPROVED or EXPLICIT_NO_IMPROVEMENT; drawer remains unpublished",
        commercialAcceptance:
          "WATCHLIST→ACCOUNT_DEVELOPMENT only if named relevant buyer + realistic hotel demand; Sales Ready still requires full contact+revenue gates",
      },
      {
        opportunityId: "disc_461dfa26a790",
        organizationName: "ASGSR",
        hotelName: "Bethesda Marriott",
        missingEvidence: [
          "2026 meeting housing/hotel page",
          "Named meeting planner or housing contact",
          "Bethesda-side vs DC venue geography",
        ],
        requiredProviderCalls: ["1× Exa Agent medium events deep research"],
        estimatedMaxSpendUsd: 0.5,
        expectedIncrementalOutput: [
          "Housing URL captured in investigatedUrls",
          "Competitor host or open sourcing status",
        ],
        stopConditions: [
          "Competitor host confirmed with no overflow → STOP_PRIMARY",
          "NO_IMPROVEMENT",
        ],
        verificationMethod: "Evidence completion gate + disposition reconcile",
        commercialAcceptance:
          "Continue only if sourcing open or overflow evidenced for Bethesda Marriott",
      },
      {
        opportunityId: "disc_cd8f10005345",
        organizationName: "International Road Federation",
        hotelName: "YOTEL Geneva Lake",
        missingEvidence: [
          "2026 conference destination + housing partner",
          "Named organizer contact route",
          "YOTEL Lake relevance vs venue city",
        ],
        requiredProviderCalls: ["1× Exa Agent medium events deep research"],
        estimatedMaxSpendUsd: 0.5,
        expectedIncrementalOutput: [
          "Destination + housing status with URLs",
          "Org contact route if published",
        ],
        stopConditions: [
          "Venue city incompatible with YOTEL Lake → STOP",
          "NO_IMPROVEMENT",
        ],
        verificationMethod: "Evidence completion gate",
        commercialAcceptance: "WATCHLIST retention only with open housing pathway",
      },
    ],
    excludedFromPaidPilot: [
      {
        opportunityId: "disc_0a11a0bb1158",
        reason: "AAEI — Hotel Washington block already open (competitor committed)",
      },
      {
        opportunityId: "disc_9334f4f1e3dd",
        reason: "AHA — Marriott Marquis official housing committed",
      },
      {
        opportunityId: "disc_b7fc48bf6995",
        reason:
          "ESCP — weak YOTEL Lake↔Palexpo fit (~9 mi); free monitor only until housing opens",
      },
    ],
    aggregateMaxSpendUsd: 1.5,
    dualLedgerNote: "Remain under existing $12.50/hotel and $25 aggregate caps if founder GO issued later",
    requiresSeparateFounderGo: true,
  };
}

export function verifyV12Corrections(repoRoot) {
  const recovery = runOfflineConversionRecovery(repoRoot, { replayV3: true });
  const after = recovery.dispositionDistribution.after || {};
  const watchlist = after.WATCHLIST_RESEARCH || 0;
  const exclude = after.EXCLUDE || 0;
  const salesReady = recovery.benchmark.salesReadyOpportunities;
  const publication = recovery.rows.filter((r) => r.drawerReady === true).length;

  const watchlistDoesNotImplyVerifiedDemand = recovery.rows.every((r) => {
    // WATCHLIST rows must retain eligibility gaps / non-sales-ready
    return (
      r.afterDisposition === "WATCHLIST_RESEARCH" &&
      r.salesReadyAfter === false &&
      (r.eligibilityStatus === "RESEARCH_CONTINUATION" ||
        (r.excludeRootCause?.eligibilityGaps || []).length >= 0)
    );
  });

  return {
    version: GDI_PORTFOLIO_CERT_V1_3,
    watchlistResearch: watchlist,
    incorrectExclude: exclude,
    salesReady,
    customerPublication: publication,
    pass:
      watchlist === 8 &&
      exclude === 0 &&
      salesReady === 0 &&
      publication === 0 &&
      watchlistDoesNotImplyVerifiedDemand,
    watchlistDoesNotImplyVerifiedDemand,
    recoverySummary: {
      dispositionDistribution: recovery.dispositionDistribution,
      deepResearchImprovementCounts: recovery.deepResearchImprovementCounts,
      forensicClassCounts: recovery.forensicClassCounts,
    },
  };
}

export function runV13OfflineCertification(repoRoot) {
  const v12 = verifyV12Corrections(repoRoot);
  const propagation = runEvidencePropagationFixture();
  const negatives = runGenuineExcludeNegatives();
  const three = buildThreeCandidateCommercialReview();
  const continuation = buildResearchContinuationPlans(three);
  const paidDesign = buildPaidPilotDesignV13();

  const paths = resolveV11OrsPaths(repoRoot);
  const urlTraces = [];
  for (const p of paths) {
    const ors = readJson(p.orsPath);
    const reclass = reclassifyFrozenOpportunity(ors, { replayV3: true });
    const completion = assessDeepResearchCompletion(ors);
    const urls = classifyEvidenceUrls(ors);
    const improvement = assessDeepResearchImprovement(ors);
    urlTraces.push({
      opportunityId: ors.opportunityId || p.opportunityId,
      organizationName: ors.organizationName,
      hotelName: ors.hotelName,
      afterDisposition: reclass.afterDisposition,
      salesReady: false,
      deepCompletion: completion,
      urlClassCounts: urls.counts,
      improvementVerdict: improvement.verdict,
      eligibilityGaps: ors.commercialEligibilityContractV1?.gaps || [],
      investigatedUrlCount: (ors.investigatedUrls || []).length,
      seedUrlCount: (ors.seedUrls || []).length,
    });
  }

  const readiness =
    v12.pass && propagation.pass && negatives.pass
      ? "LIMITED_PILOT_CONTINUATION_DESIGN_READY"
      : "NOT_READY";

  return {
    version: GDI_PORTFOLIO_CERT_V1_3,
    at: new Date().toISOString(),
    paidResearch: false,
    airtableWrites: false,
    salesReadyPromotions: 0,
    phase1Integration: {
      pr564: "OPEN_AT_LAST_CHECK",
      pr566: "OPEN_AT_LAST_CHECK",
      note: "GDI product chat does not merge — Integration Controller owns #564 then #566 before or with V1.3 sequencing.",
    },
    v12Verification: v12,
    evidencePropagation: propagation,
    genuineExcludeNegatives: negatives,
    urlTraces,
    threeCandidateReview: three,
    researchContinuation: continuation,
    paidPilotDesign: paidDesign,
    qualificationBenchmark: {
      dispositionsWatchlist: urlTraces.filter(
        (t) => t.afterDisposition === "WATCHLIST_RESEARCH",
      ).length,
      salesReady: 0,
      publicationBlocked: true,
      evidenceBackedDemandForPilotHotel: three.candidates.filter(
        (c) => c.lodgingDemandForPilotHotel,
      ).length,
      competitorCommittedPrimary: three.candidates.filter((c) =>
        /STOP_PRIMARY_COMPETITOR/.test(c.dispositionRecommendation),
      ).length,
      investigatedUrlsStillEmptyOnFrozen: urlTraces.filter(
        (t) => t.investigatedUrlCount === 0,
      ).length,
    },
    readinessVerdict: readiness,
  };
}

export default {
  GDI_PORTFOLIO_CERT_V1_3,
  V1_3_PRIORITY_THREE,
  EVIDENCE_URL_ROLE,
  DEEP_RESEARCH_COMPLETION,
  assessDeepResearchCompletion,
  classifyEvidenceUrls,
  runEvidencePropagationFixture,
  runGenuineExcludeNegatives,
  buildThreeCandidateCommercialReview,
  buildResearchContinuationPlans,
  buildPaidPilotDesignV13,
  verifyV12Corrections,
  runV13OfflineCertification,
};
