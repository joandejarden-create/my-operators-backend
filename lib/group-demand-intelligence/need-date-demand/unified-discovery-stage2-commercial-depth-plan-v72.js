/**
 * GDI V7.2 — Stage 2 Commercial Depth plan (planning / preflight only).
 *
 * Diversifies beyond wedding-planner directory bias.
 * Tracks A (verified need-date) and B (qualified relationship) stay separate.
 * Does not authorize or execute paid Apify/Exa/Surfe/Firecrawl.
 */

import {
  OPTION_B_AGGREGATE_MAXIMUM_USD,
  OPTION_B_STAGE2_APPROVAL_PHRASE,
  buildOptionBStagePlansV7,
} from "./unified-discovery-option-b-stage-plans-v7.js";
import { evaluateFourteenStrategiesForNeedDateCampaign } from "./need-date-fourteen-strategy-relevance-v1.js";
import { buildFourteenWorkflowProviderRoutingV7 } from "./unified-discovery-provider-routing-v7.js";
import { buildV6WaveProviderIntegrationV7 } from "./unified-discovery-provider-routing-v7.js";
import {
  NEED_DATE_V6_WAVE,
  NEED_DATE_V6_SALES_READY_ACCESSIBILITY,
  NEED_DATE_V6_ACCESSIBILITY,
} from "./need-date-deep-discovery-v6-contract.js";
import { SEMANTIC_SECTION_CONTRACT } from "../demand-segments/segment-semantic-cib-contract-v1.js";
import { WATERSTONE_V7_HOTEL } from "./unified-discovery-engine-v7-contract.js";
import { APIFY_COVERAGE_CLASS } from "../research/apify/apify-fourteen-workflow-coverage-v1.js";

export const STAGE2_V72_PLAN_VERSION = "gdi-stage2-commercial-depth-plan-v7.2";

/** Stage 1 actual spend (ledger) — not assumed remaining. */
export const STAGE1_ACTUAL_SPEND_USD = Object.freeze({
  apify: 0.256,
  exa: 0.5,
  aggregate: 0.756,
});

export const STAGE2_PROPOSED_CEILING_USD = 36;

export const STAGE2_TRACK = Object.freeze({
  A_VERIFIED_NEED_DATE: "A_VERIFIED_NEED_DATE_OPPORTUNITIES",
  B_QUALIFIED_RELATIONSHIP: "B_QUALIFIED_RELATIONSHIP_OPPORTUNITIES",
});

/**
 * V7.1 planner seeds — starting points only, not the whole strategy.
 * Cap Deep Research volume; reject destination-skew / thin vendors early.
 */
export const V71_PLANNER_PRIORITY_QUEUE = Object.freeze([
  {
    rank: 1,
    title: "Boca Event Planning",
    sourceUrl: "http://www.bocaeventplanning.com/",
    why: "Explicit Boca identity; marina-resort social/group referral thesis",
    trackDefault: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
    deepResearchCapUsd: 1.5,
  },
  {
    rank: 2,
    title: "Ten Events Group",
    sourceUrl: "http://www.teneventsgroup.com/",
    why: "Established South Florida planner brand; lodging referral unknown — investigate",
    trackDefault: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
    deepResearchCapUsd: 1.5,
  },
  {
    rank: 3,
    title: "The Sutton Collective",
    sourceUrl: "https://thesuttoncollective.com/",
    why: "Collective/planner listing; buyer + lodging influence unverified",
    trackDefault: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
    deepResearchCapUsd: 1.25,
  },
  {
    rank: 4,
    title: "Keith Willard Events",
    sourceUrl: "https://www.keithwillardevents.com/",
    why: "Known Boca high-end social producer — hotel partnership path plausible",
    trackDefault: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
    deepResearchCapUsd: 1.5,
  },
  {
    rank: 5,
    title: "Palm Perfect Events",
    sourceUrl: "https://palmperfectevents.com/",
    why: "Local events brand; confirm lodging vs décor-only scope before more spend",
    trackDefault: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
    deepResearchCapUsd: 1.0,
  },
]);

export const V71_PLANNER_DEPRIORITIZE = Object.freeze([
  {
    title: "Chic Bahamas Weddings and Events",
    reason: "Destination-skew (Bahamas) despite Boca Knot city tag — HOLD unless Boca lodging evidence appears",
  },
  {
    title: "PARTY BUS TO BACKYARD",
    reason: "UNRELATED_ACTIVITY — nightlife transport; Stage 1 ORS NON-QUALIFY; never re-open without new hotel-demand evidence",
  },
]);

/** Remaining Knot leads — secondary relationship backlog after top-5 + diversified discovery. */
export const V71_PLANNER_BACKLOG = Object.freeze([
  "Orea Events LLC",
  "EventsByGabriella",
  "It's All in the Details by MB",
  "DTL Evnts",
  "Events By Robyn Von",
  "Delightful Planning",
  "The Timeless Edit",
  "EBJ Events",
  "Live N Lavish Events",
  "Ivory Palm Events",
  "Your Special Day By Joni",
]);

/**
 * Hard-stop verification given actual Stage 1 spend + proposed Stage 2 ceiling.
 */
export function verifyStage2BudgetHardStops({
  stage1SpentUsd = STAGE1_ACTUAL_SPEND_USD.aggregate,
  stage2ProposedCeilingUsd = STAGE2_PROPOSED_CEILING_USD,
  campaignMaximumUsd = OPTION_B_AGGREGATE_MAXIMUM_USD,
  stage2ApifyCeilingUsd = 4,
  stage2ExaCeilingUsd = 32,
} = {}) {
  const remainingCampaignUsd = Number(
    (campaignMaximumUsd - stage1SpentUsd).toFixed(4),
  );
  const authorizedStage2CeilingUsd = Math.min(
    stage2ProposedCeilingUsd,
    remainingCampaignUsd,
  );
  const apifyCeiling = Math.min(stage2ApifyCeilingUsd, authorizedStage2CeilingUsd);
  const exaCeiling = Math.min(
    stage2ExaCeilingUsd,
    Number((authorizedStage2CeilingUsd - 0.01).toFixed(4)),
  );
  const combinedWouldExceed =
    stage1SpentUsd + authorizedStage2CeilingUsd > campaignMaximumUsd + 1e-9;

  const checks = [
    {
      id: "CAMPAIGN_REMAINING_POSITIVE",
      ok: remainingCampaignUsd > 0,
      detail: `remaining=${remainingCampaignUsd}`,
    },
    {
      id: "STAGE2_CEILING_LEQ_REMAINING",
      ok: authorizedStage2CeilingUsd <= remainingCampaignUsd + 1e-9,
      detail: `authorizedStage2=${authorizedStage2CeilingUsd} remaining=${remainingCampaignUsd}`,
    },
    {
      id: "STAGE2_PLUS_STAGE1_LEQ_40",
      ok: !combinedWouldExceed,
      detail: `stage1=${stage1SpentUsd} + stage2Ceiling=${authorizedStage2CeilingUsd} <= ${campaignMaximumUsd}`,
    },
    {
      id: "PROVIDER_CEILINGS_SUM_RESPECTED",
      ok: apifyCeiling + exaCeiling >= authorizedStage2CeilingUsd - 0.5,
      detail: `apify=${apifyCeiling} exa=${exaCeiling} (soft sum; hard stop per-call against remaining)`,
    },
    {
      id: "STAGE2_NOT_AUTO_AUTHORIZED",
      ok: true,
      detail: "Remaining budget is NOT automatic authorization",
    },
  ];

  return {
    version: "gdi-stage2-budget-hard-stops-v7.2",
    stage1SpentUsd,
    campaignMaximumUsd,
    stage2ProposedCeilingUsd,
    remainingCampaignUsd,
    authorizedStage2CeilingUsd,
    providerCeilings: {
      apifyUsd: apifyCeiling,
      exaUsd: Math.min(exaCeiling, authorizedStage2CeilingUsd),
      aggregateUsd: authorizedStage2CeilingUsd,
    },
    hardStopRules: [
      "Refuse any paid call when authorizeOptionBStage2Live !== true",
      "Refuse without exact approval phrase match",
      "Stop before any Apify call that would exceed remaining Apify ceiling",
      "Stop before any Exa call that would exceed remaining Exa ceiling",
      "Stop before any call that would exceed remaining Stage 2 aggregate ceiling",
      "Stop before any call that would exceed campaign remaining (40 − Stage1 − Stage2Spent)",
      "Count retries and failed paid attempts against the same ceilings",
      "Publication and Airtable opportunity writes remain DENIED",
    ],
    checks,
    ok: checks.every((c) => c.ok),
  };
}

export function buildStage2BudgetAllocationV72(hardStops = verifyStage2BudgetHardStops()) {
  const ceil = hardStops.authorizedStage2CeilingUsd;
  // Diversified: more spend on dated demand + Exa discovery than planner directory depth
  const allocation = {
    waveA_inventoryReviewUsd: 0,
    waveB_apifyDiversifiedDiscoveryUsd: 3.5,
    waveB_exaBroadDiscoveryUsd: 9.0,
    waveC_qualificationUsd: 0,
    waveD_deepResearch_trackA_datedUsd: 12.0,
    waveD_deepResearch_trackB_relationshipUsd: 6.0,
    waveE_adaptiveGapUsd: 4.0,
    retryReserveUsd: 1.5,
  };
  const sum = Object.values(allocation).reduce((a, b) => a + b, 0);
  return {
    version: "gdi-stage2-budget-allocation-v7.2",
    authorizedStage2CeilingUsd: ceil,
    allocation,
    estimatedAggregateUsd: sum,
    withinCeiling: sum <= ceil + 1e-9,
    trackSpendIntent: {
      [STAGE2_TRACK.A_VERIFIED_NEED_DATE]: {
        discoveryPlusDeepUsd:
          allocation.waveB_apifyDiversifiedDiscoveryUsd * 0.75 +
          allocation.waveB_exaBroadDiscoveryUsd * 0.7 +
          allocation.waveD_deepResearch_trackA_datedUsd,
        note: "Majority of Stage 2 — find source-supported Aug–Oct 2027 demand",
      },
      [STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP]: {
        discoveryPlusDeepUsd:
          allocation.waveB_apifyDiversifiedDiscoveryUsd * 0.25 +
          allocation.waveB_exaBroadDiscoveryUsd * 0.3 +
          allocation.waveD_deepResearch_trackB_relationshipUsd,
        note: "Capped — top planners + preferred lodging; not 17 equal Deep Research burns",
      },
    },
    providerSplitEstimate: {
      apifyUsd: allocation.waveB_apifyDiversifiedDiscoveryUsd,
      exaUsd: Number(
        (
          allocation.waveB_exaBroadDiscoveryUsd +
          allocation.waveD_deepResearch_trackA_datedUsd +
          allocation.waveD_deepResearch_trackB_relationshipUsd +
          allocation.waveE_adaptiveGapUsd +
          allocation.retryReserveUsd
        ).toFixed(2),
      ),
    },
  };
}

/**
 * All-fourteen workflow Stage 2 discovery plan with Waterstone fit + provider.
 */
export function buildFourteenWorkflowStage2DiscoveryPlanV72() {
  const relevance = evaluateFourteenStrategiesForNeedDateCampaign({
    hotel: {
      hotelId: WATERSTONE_V7_HOTEL.hotelId,
      market: "Boca Raton / South Florida",
      facilities: {
        marinaResort: true,
        meetingSpace: true,
        catering: true,
        leisureSocial: true,
        extendedStayLikely: false,
      },
    },
    needPeriods: WATERSTONE_V7_HOTEL.needPeriods,
  });
  const routing = buildFourteenWorkflowProviderRoutingV7();

  const byId = Object.fromEntries(routing.workflows.map((w) => [w.strategyId, w]));

  const rows = relevance.strategies.map((s) => {
    const r = byId[s.strategyId] || {};
    const pursuit = resolvePursuitMode(s, r);
    return {
      workflowNumber: s.workflowNumber,
      strategyId: s.strategyId,
      powershell: s.powershell,
      relevance: s.relevance,
      relevanceReasons: s.reasons || [],
      providerClassification: r.classification,
      recommendedSource: r.recommendedSource,
      pursuitMode: pursuit.mode,
      primaryTrack: pursuit.track,
      discoveryActions: pursuit.actions,
      estimatedDiscoveryUsd: pursuit.estimateUsd,
      note: pursuit.note,
    };
  });

  rows.sort((a, b) => a.workflowNumber - b.workflowNumber);

  return {
    version: "gdi-stage2-fourteen-workflow-plan-v7.2",
    antiBiasRule:
      "Do not replace conference/event bias with wedding-planner directory bias. Planners are a capped Track B seed, not the research strategy.",
    workflows: rows,
    pursuitSummary: {
      ACTIVE_DATED_FOCUS: rows.filter((x) => x.pursuitMode === "ACTIVE_DATED_FOCUS").length,
      ACTIVE_RELATIONSHIP_CAPPED: rows.filter((x) => x.pursuitMode === "ACTIVE_RELATIONSHIP_CAPPED")
        .length,
      EXA_PRIMARY_ACTIVE: rows.filter((x) => x.pursuitMode === "EXA_PRIMARY_ACTIVE").length,
      SUPPLEMENTAL: rows.filter((x) => x.pursuitMode === "SUPPLEMENTAL").length,
      EVALUATE_DEFER: rows.filter((x) => x.pursuitMode === "EVALUATE_DEFER").length,
    },
  };
}

function resolvePursuitMode(strategy, routing) {
  const id = strategy.strategyId;
  // Diversified dated demand first
  if (
    id === "meetings_incentives" ||
    id === "association_events" ||
    id === "local_catering_day_meetings" ||
    id === "sports_entertainment" ||
    id === "corporate_celebrations" ||
    id === "education_scientific"
  ) {
    return {
      mode: id === "education_scientific" || id === "corporate_celebrations"
        ? "SUPPLEMENTAL"
        : "ACTIVE_DATED_FOCUS",
      track: STAGE2_TRACK.A_VERIFIED_NEED_DATE,
      estimateUsd: id === "meetings_incentives" || id === "association_events" ? 2.5 : 1.5,
      actions: [
        "Exa discovery for demand owners / programmes with Aug–Oct 2027 timing evidence",
        routing.classification === APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY ||
        routing.classification === APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY
          ? "Apify Eventbrite/Meetup/sports Actors with commercial prequal (reject nightlife)"
          : "Apify only if differentiated structured source exists",
        "ORS Deep Research only after prequal PASS on hotel-revenue thesis",
      ],
      note: "Prioritize demand owners over competing hotel venue calendars",
    };
  }
  if (id === "social_special_events") {
    return {
      mode: "ACTIVE_DATED_FOCUS",
      track: STAGE2_TRACK.A_VERIFIED_NEED_DATE,
      estimateUsd: 2.0,
      actions: [
        "Seek dated social/wedding-related GROUP demand with timing evidence",
        "Do not invent couple dates from Knot directory rows",
        "Public Eventbrite social with lodging thesis only after nightlife reject",
      ],
      note: "Wedding group business ≠ directory volume",
    };
  }
  if (id === "professional_event_planners" || id === "preferred_lodging_partnerships") {
    return {
      mode: "ACTIVE_RELATIONSHIP_CAPPED",
      track: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
      estimateUsd: id === "professional_event_planners" ? 6.0 : 2.0,
      actions: [
        "Investigate top-5 V7.1 planner seeds with question-led Deep Research",
        "Preferred lodging: venues without rooms that may need Waterstone blocks",
        "Do not Deep Research all 17 Knot rows",
      ],
      note: "Relationship track — timing unconfirmed until evidence",
    };
  }
  if (
    id === "scaling_companies" ||
    id === "government_public_sector" ||
    id === "projects_infrastructure" ||
    id === "relocation_extended_stay"
  ) {
    const defer =
      id === "projects_infrastructure" || id === "relocation_extended_stay";
    return {
      mode: defer ? "EVALUATE_DEFER" : "EXA_PRIMARY_ACTIVE",
      track: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
      estimateUsd: defer ? 0.5 : 2.0,
      actions: defer
        ? ["Evaluate hotel fit; defer unless Wave G finds justified gap"]
        : [
            "Exa-only discovery for Boca/Palm Beach corporate accounts and programmes",
            "Promote to Track A only with source-supported need-month timing",
          ],
      note: routing.classification || "EXA_PRIMARY",
    };
  }
  if (id === "travel_trade") {
    return {
      mode: "SUPPLEMENTAL",
      track: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
      estimateUsd: 1.0,
      actions: ["Exa for DMCs / group intermediaries serving Boca marina groups"],
      note: "Relationship / intermediary",
    };
  }
  return {
    mode: "EVALUATE_DEFER",
    track: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
    estimateUsd: 0,
    actions: ["Document relevance only"],
    note: "Hotel-fit evaluation without forced spend",
  };
}

/**
 * Deep investigation sequence — Track A first, then capped Track B.
 */
export function buildDeepInvestigationSequenceV72() {
  return {
    version: "gdi-stage2-deep-investigation-sequence-v7.2",
    principle:
      "Question-led ORS only. Stop when thesis collapses. Do not pay to re-summarize listings.",
    phases: [
      {
        phase: 1,
        name: "WAVE_A_INVENTORY",
        paid: false,
        actions: [
          "Load Waterstone GDI inventory (45+) + Stage 1 artifacts + V7.1 remediation",
          "Exclude AES/WIO/URA watch without new evidence",
          "Map existing coverage by need month and segment",
        ],
      },
      {
        phase: 2,
        name: "WAVE_B_DIVERSIFIED_DATED_DISCOVERY",
        paid: true,
        trackFocus: STAGE2_TRACK.A_VERIFIED_NEED_DATE,
        actions: [
          "Apify: Eventbrite absolute-date Boca + Delray/Deerfield/West Palm filters; commercial prequal",
          "Apify: Meetup / sports schedule Actors only with South Florida geo + lodging thesis",
          "Exa: association/conference programmes Aug–Oct 2027 Boca–Palm Beach corridor",
          "Exa: corporate meetings/training owners (not venue calendars first)",
          "Exa: sports travel demand owners; education/scientific public programmes",
          "Exa: local catering/day-meeting generators with banquet path",
        ],
        stopIf: "No commercially prequalified dated candidates after budget slice — do not fill with Knot",
      },
      {
        phase: 3,
        name: "WAVE_C_PREQUAL",
        paid: false,
        actions: [
          "V7.1 commercial prequalify + nightlife reject",
          "Split pool into Track A candidates vs Track B relationship",
          "Reject generic vendors / unrelated leisure",
        ],
      },
      {
        phase: 4,
        name: "WAVE_D_DEEP_TRACK_A",
        paid: true,
        trackFocus: STAGE2_TRACK.A_VERIFIED_NEED_DATE,
        maxInitialDeep: 6,
        actions: [
          "Deep Research each prequalified dated candidate with adaptive plan",
          "Require named buyer/entry point + Waterstone revenue thesis for qualification",
          "Source-supported timing required for Track A yield counts",
        ],
      },
      {
        phase: 5,
        name: "WAVE_D_DEEP_TRACK_B_PLANNERS",
        paid: true,
        trackFocus: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
        maxInitialDeep: 5,
        sequence: V71_PLANNER_PRIORITY_QUEUE,
        actions: [
          "Investigate services, lodging influence, Boca relevance, named contact, active demand, Waterstone win path, sales next step",
          "Do not invent wedding dates or room blocks",
          "Promote to Track A only if timing evidence appears",
        ],
        deprioritize: V71_PLANNER_DEPRIORITIZE,
        backlog: V71_PLANNER_BACKLOG,
      },
      {
        phase: 6,
        name: "WAVE_E_ADAPTIVE",
        paid: true,
        actions: [
          "Resolve blocker ledger gaps with new questions only",
          "No identical query burns",
          "Prefer unlocking Track A yield over more directory rows",
        ],
      },
      {
        phase: 7,
        name: "WAVE_F_G_CERTIFY_AND_COVERAGE",
        paid: false,
        actions: [
          "Separate yield: Track A vs Track B counts",
          "CIB completeness for qualified only",
          "Coverage assessment by need month — recommend follow-up only if justified",
        ],
      },
    ],
  };
}

export function buildCommercialQualificationGatesV72() {
  return {
    version: "gdi-stage2-commercial-qualification-gates-v7.2",
    requiredForAnyQualified: [
      "Verified named relevant commercial buyer OR legitimate activity-linked entry point",
      "Credible Waterstone-specific revenue thesis for guestrooms AND/OR meetings/catering/banquets/private dining/events (GDI_MULTI_REVENUE_QUALIFICATION)",
      "Commercial accessibility in sales-ready or continuation-eligible class — not speculative overflow",
      "ORS evidence preserved; uncertainties explicit",
      "Explicit revenueStreamClass: GUESTROOMS_ONLY | EVENT_FNB_ONLY | COMBINED_GUESTROOMS_EVENT_FNB (never guestrooms-only gate)",
    ],
    notRequired: [
      "Confirmed room nights",
      "Immediate RFP",
      "Signed contract",
      "Guestroom demand when EVENT_FNB_ONLY is evidenced",
      "Event/F&B demand when GUESTROOMS_ONLY is evidenced",
    ],
    reject: [
      "Nightlife / leisure transport / hobby listings",
      "Unrelated contacts",
      "Speculative overflow without evidence",
      "Competitor-committed with no contestable path",
      "Directory re-summaries without buyer/pathway progress",
      "Invented dates or room nights",
      "REVENUE_STREAM_UNCONFIRMED without a credible hotel revenue mechanism",
    ],
    multiRevenue: {
      gate: "GDI_MULTI_REVENUE_QUALIFICATION",
      classes: [
        "GUESTROOMS_ONLY",
        "EVENT_FNB_ONLY",
        "COMBINED_GUESTROOMS_EVENT_FNB",
        "REVENUE_STREAM_UNCONFIRMED",
      ],
      banAssumptions: [
        "Never assume a corporate meeting requires guestrooms",
        "Never assume a wedding necessarily requires an overnight room block",
        "Never assume a sporting event creates available group accommodation demand",
      ],
      doNotDemoteCateringOnlyForMissingGuestrooms: true,
    },
    trackA_additional: [
      "Source-supported timing relevant to Aug and/or Sep and/or Oct 2027",
      "countsTowardVerifiedNeedMonthYield = true only with timing evidence",
    ],
    trackB_additional: [
      "May lack need-month timing",
      "Must still clear buyer/entry + revenue thesis",
      "Accessibility typically TIMING_UNCONFIRMED_RELATIONSHIP until dates evidenced",
      "Never merge into Track A counts",
    ],
    salesReadyAccessibility: [...NEED_DATE_V6_SALES_READY_ACCESSIBILITY],
    continuationAccessibility: [
      NEED_DATE_V6_ACCESSIBILITY.TIMING_UNCONFIRMED_RELATIONSHIP,
    ],
  };
}

export function buildCibCompletenessStandardsV72() {
  return {
    version: "gdi-stage2-cib-completeness-v7.2",
    pipeline: "DISCOVERY→NORMALIZE→DEDUP→PREQUAL→ORS/EXA→QUALIFY→UNIVERSAL_RESEARCH_TO_DRAWER_V2",
    requiredSections: Object.keys(SEMANTIC_SECTION_CONTRACT),
    sectionMeanings: SEMANTIC_SECTION_CONTRACT,
    rules: [
      "No Apify-specific drawer — Universal Research-to-Drawer V2 only",
      "Qualified opportunities must carry evidence, uncertainties, timing, named contacts, next actions, hotel fit",
      "Track A vs Track B labeling preserved in opportunity presentation",
      "Need-Date Match explicit when timing evidenced; relationship otherwise",
    ],
  };
}

export function buildStage2AuthorizationRequirementsV72(hardStops = verifyStage2BudgetHardStops()) {
  return {
    version: "gdi-stage2-authorization-requirements-v7.2",
    approvalPhraseExact: OPTION_B_STAGE2_APPROVAL_PHRASE,
    requiredFlags: {
      authorizeOptionBStage2Live: true,
      authorizeApifyLive: true,
      authorizeExaLive: true,
      executeLive: true,
      authorizePublication: false,
      authorizeAirtableMutation: false,
      authorizeSurfe: false,
      authorizeFirecrawl: false,
    },
    requiredBudgets: {
      stage2ApifyBudgetUsd: {
        minExclusive: 0,
        max: hardStops.providerCeilings.apifyUsd,
      },
      stage2ExaBudgetUsd: {
        minExclusive: 0,
        max: hardStops.providerCeilings.exaUsd,
      },
      stage2AggregateBudgetUsd: {
        minExclusive: 0,
        max: hardStops.authorizedStage2CeilingUsd,
      },
    },
    envRequired: ["APIFY_TOKEN or GDI_APIFY_LIVE=1", "EXA_API_KEY"],
    explicitFounderGO:
      "Paste approval phrase + numeric ceilings in the same turn. Remaining campaign budget is NOT auto-authorized.",
    blockedUntilGO: [
      "All paid Apify Actor runs",
      "All paid Exa Agent / Deep Research",
      "Surfe",
      "Firecrawl",
      "Airtable opportunity writes",
      "External publication",
    ],
    preserves: [
      "Existing GDI inventory",
      "Historical ledgers",
      "Stage 1 raw research",
      "V7.1 remediation",
      "V6 waves A–G",
      "Fourteen workflows",
      "Canonical dedup",
      "Commercial accessibility V6",
      "Ten-section CIB V2",
    ],
  };
}

/**
 * Full Stage 2 commercial depth preflight package (zero-paid).
 */
export function buildStage2CommercialDepthPreflightV72() {
  const basePlans = buildOptionBStagePlansV7();
  const hardStops = verifyStage2BudgetHardStops();
  const allocation = buildStage2BudgetAllocationV72(hardStops);
  const fourteen = buildFourteenWorkflowStage2DiscoveryPlanV72();
  const sequence = buildDeepInvestigationSequenceV72();
  const gates = buildCommercialQualificationGatesV72();
  const cib = buildCibCompletenessStandardsV72();
  const auth = buildStage2AuthorizationRequirementsV72(hardStops);
  const waves = buildV6WaveProviderIntegrationV7();

  const revisedPriorities = {
    principle:
      "Qualified commercial yield across segments — not raw Eventbrite counts or Knot directory volume",
    trackA_priorities: [
      "Corporate meetings / training with Aug–Oct 2027 timing",
      "Associations / conferences (demand owners; contestable sourcing)",
      "Local catering / day meetings / banquets",
      "Sports travel with South Florida lodging thesis",
      "Dated social / wedding-related GROUP demand (evidence required)",
      "Corporate celebrations / public galas with lodging or banquet path",
      "Education / scientific programmes (supplemental)",
    ],
    trackB_priorities: [
      ...V71_PLANNER_PRIORITY_QUEUE.map((p) => `${p.rank}. ${p.title} — ${p.why}`),
      "Preferred lodging partnerships (venues needing rooms)",
      "Scaling / corporate accounts (Exa) without inventing need months",
      "Travel trade intermediaries (supplemental)",
    ],
    explicitlyNotStrategy: [
      "Deep Research all 17 Knot planners equally",
      "More wedding directory scrapes as primary success metric",
      "Re-opening PARTY BUS",
      "Conference calendar scraping without demand-owner investigation",
    ],
  };

  const datedVsRelationshipAllocation = {
    budgetIntentPct: { trackA_dated: 55, trackB_relationship: 25, adaptive_shared: 20 },
    measurement: {
      netNewCandidates: "Shared discovery pool size after inventory dedup (not success)",
      verifiedDatedOpportunities: "Track A count only — separate",
      qualifiedRelationshipOpportunities: "Track B count only — separate",
      neverCombine: true,
    },
    spend: allocation.trackSpendIntent,
  };

  return {
    version: STAGE2_V72_PLAN_VERSION,
    hotel: {
      name: WATERSTONE_V7_HOTEL.name,
      hotelId: WATERSTONE_V7_HOTEL.hotelId,
      needPeriods: WATERSTONE_V7_HOTEL.needPeriods,
    },
    stage1ActualSpendUsd: STAGE1_ACTUAL_SPEND_USD,
    zeroPaidPreflight: true,
    paidExecutionAuthorized: false,
    revisedPriorities,
    fourteenWorkflowPlan: fourteen,
    datedVsRelationshipAllocation,
    deepInvestigationSequence: sequence,
    budget: { hardStops, allocation },
    commercialQualificationGates: gates,
    cibCompletenessStandards: cib,
    authorizationRequirements: auth,
    v6Waves: waves,
    optionBStage2PlanRef: basePlans.stage2,
    publicationBlocked: true,
    airtableOpportunityWritesBlocked: true,
  };
}
