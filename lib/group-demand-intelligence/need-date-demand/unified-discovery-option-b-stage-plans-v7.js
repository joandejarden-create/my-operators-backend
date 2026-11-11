/**
 * Option B Stage 1 + Stage 2 plans and reconciled $40 ledger (planning only).
 */

import { WATERSTONE_V7_HOTEL, V7_BUDGET_RECONCILIATION } from "./unified-discovery-engine-v7-contract.js";

export const OPTION_B_AGGREGATE_MAXIMUM_USD = 40;
export const OPTION_B_PLANNED_USD = 36;

export const OPTION_B_STAGE1_APPROVAL_PHRASE =
  "AUTHORIZE WATERSTONE V7 OPTION B STAGE 1 LIVE";

export const OPTION_B_STAGE2_APPROVAL_PHRASE =
  "AUTHORIZE WATERSTONE V7 OPTION B STAGE 2 COMMERCIAL DEPTH";

export function buildOptionBStagePlansV7() {
  const stage1 = {
    id: "OPTION_B_STAGE_1_LIVE_INTEGRATION_VALIDATION",
    purpose:
      "Smallest useful live Apify→normalize→dedup→ORS/Exa path to prove the real pipeline",
    autoLaunchStage2: false,
    continuationCheckpointRequired: true,
    calls: {
      apify: [
        {
          actorId: "solidcode/eventbrite-scraper",
          purpose: "Dated public events (absolute YYYY-MM-DD for need months)",
          input: {
            country: "united-states",
            city: "Boca Raton",
            startDate: "2027-08-01",
            endDate: "2027-10-31",
            eventFormat: "in-person",
            maxResults: 25,
          },
          maxItems: 25,
          estimateUsd: 0.04,
          hardCapUsd: 0.5,
        },
        {
          actorId: "dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads",
          purpose: "Wedding/planner discovery (relationship track)",
          input: {
            startUrls: [
              "https://www.theknot.com/marketplace/wedding-planners-boca-raton-fl",
              "https://www.theknot.com/marketplace/wedding-venues-boca-raton-fl",
            ],
            maxPages: 2,
          },
          maxItems: 20,
          estimateUsd: 0.12,
          hardCapUsd: 0.5,
        },
      ],
      exa: [
        {
          purpose: "One promising net-new candidate → full Exa/ORS investigation",
          effort: "high",
          maxCostDollars: 1.5,
          estimateUsd: 1.5,
          selectionRule:
            "V7.1: commercial prequalify first — reject nightlife/generic listings; prefer plausible hotel-revenue thesis or Boca wedding/planner relationship; dates are secondary not dominant",
        },
      ],
    },
    estimatedUsd: { apify: 0.16, exa: 1.5, aggregate: 1.66 },
    hardCeilingUsd: 4,
    providerCeilings: { apifyUsd: 1.5, exaUsd: 2.5, aggregateUsd: 4 },
    inventoryFirst: true,
    excludeWatchWithoutNewEvidence: ["AES", "WIO", "URA"],
    publicationBlocked: true,
    airtableOpportunityWritesBlocked: true,
    surfeBlocked: true,
    firecrawlBlocked: true,
    successCriteria: [
      "At least one live Apify dataset retrieved (mode=live, not mock)",
      "Candidates normalized into shared pool with provenance",
      "Dedup against existing Waterstone GDI inventory",
      "One Exa/ORS deep investigation artifact on a real candidate",
      "Spend ledgers show Apify and Exa separately under ceilings",
      "No publication / no Airtable opportunity mutation",
    ],
  };

  const stage2 = {
    id: "OPTION_B_STAGE_2_COMMERCIAL_DEPTH",
    purpose:
      "V7.2 Commercial Depth after explicit Stage 1 continuation GO — diversified dated demand first; planners capped Track B; not auto-launched",
    planModule: "unified-discovery-stage2-commercial-depth-plan-v72.js",
    requiresStage1Pass: true,
    requiresFounderContinuationPhrase: OPTION_B_STAGE2_APPROVAL_PHRASE,
    antiBias:
      "Do not replace conference/event bias with wedding-planner directory bias",
    coverage: {
      evaluateAll14: true,
      activelyResearchTrackA_dated: [
        "meetings_incentives",
        "association_events",
        "local_catering_day_meetings",
        "sports_entertainment",
        "social_special_events (dated group demand — not directory volume)",
        "corporate_celebrations (supplemental)",
        "education_scientific (supplemental)",
      ],
      activelyResearchTrackB_relationship_capped: [
        "professional_event_planners (top-5 Deep Research seeds from V7.1)",
        "preferred_lodging_partnerships",
        "scaling_companies (Exa; promote to Track A only with timing)",
        "travel_trade (supplemental)",
      ],
      exaPrimary: [
        "scaling_companies",
        "government_public_sector",
        "projects_infrastructure",
        "relocation_extended_stay",
      ],
      deferUnlessJustified: ["projects_infrastructure", "relocation_extended_stay"],
    },
    allocationByNeedMonth: {
      "2027-08": {
        plannedUsd: 10.5,
        focus: [
          "Corporate meetings / training with Aug timing evidence",
          "Associations / demand owners",
          "Local catering day meetings",
          "Capped planner relationship (not primary)",
        ],
      },
      "2027-09": {
        plannedUsd: 10.5,
        focus: [
          "Associations/conferences dated Sep",
          "Sports South Florida lodging thesis",
          "Dated social / wedding-related GROUP demand",
        ],
      },
      "2027-10": {
        plannedUsd: 10.5,
        focus: [
          "Meetings + scaling accounts with Oct timing",
          "Preferred lodging partnerships",
          "Adaptive gap resolution for strongest Track A blockers",
        ],
      },
    },
    reserves: {
      adaptiveWaveE_usd: 4.0,
      retryAllowanceUsd: 1.5,
      note: "Inside Stage 2 ceiling; aggregate campaign still ≤ $40 including Stage 1 actual $0.756",
    },
    estimatedUsd: {
      apify: 3.5,
      exaDiscovery: 9,
      exaDeepResearchTrackA: 12,
      exaDeepResearchTrackB: 6,
      adaptiveFollowUp: 4,
      retries: 1.5,
      aggregate: 36,
    },
    hardCeilingUsd: 36,
    providerCeilings: { apifyUsd: 4, exaUsd: 32, aggregateUsd: 36 },
    stage1ActualSpendUsd: 0.756,
    campaignMaximumUsd: OPTION_B_AGGREGATE_MAXIMUM_USD,
    remainingCampaignAfterStage1Usd: Number((OPTION_B_AGGREGATE_MAXIMUM_USD - 0.756).toFixed(4)),
    targetQualifiedOpportunities: { min: 5, max: 10, notAQuota: true },
    tracks: {
      A_verifiedNeedDate: "A_VERIFIED_NEED_DATE_OPPORTUNITIES",
      B_qualifiedRelationship: "B_QUALIFIED_RELATIONSHIP_OPPORTUNITIES",
      neverCombineYieldCounts: true,
    },
    publicationBlocked: true,
    airtableOpportunityWritesBlocked: true,
  };

  const ledger = {
    version: "gdi-option-b-reconciled-ledger-v7",
    aggregateMaximumUsd: OPTION_B_AGGREGATE_MAXIMUM_USD,
    plannedAggregateUsd: OPTION_B_PLANNED_USD,
    historical: {
      v4SpentUsd: V7_BUDGET_RECONCILIATION.historicalV4SpentUsd,
      v4LeftoverShapedUsd: V7_BUDGET_RECONCILIATION.historicalV4LeftoverShapedUsd,
      reusableAsCredit: false,
      note: "Preserve $6.90 history; do not silently reuse $11.10 leftover",
    },
    stages: {
      stage1: {
        reservedUsd: stage1.hardCeilingUsd,
        estimatedUsd: stage1.estimatedUsd.aggregate,
        apifyCeilingUsd: stage1.providerCeilings.apifyUsd,
        exaCeilingUsd: stage1.providerCeilings.exaUsd,
      },
      stage2: {
        reservedUsd: stage2.hardCeilingUsd,
        estimatedUsd: stage2.estimatedUsd.aggregate,
        apifyCeilingUsd: stage2.providerCeilings.apifyUsd,
        exaCeilingUsd: stage2.providerCeilings.exaUsd,
        note: "Stage 2 ceiling $36 assumes Stage 1 ≤ $4 so combined ≤ $40",
      },
    },
    combined: {
      stage1PlusStage2Ceil: stage1.hardCeilingUsd + stage2.hardCeilingUsd,
      withinAggregate40: stage1.hardCeilingUsd + stage2.hardCeilingUsd <= 40,
      ifStage1SpendsMax4_stage2Max: 36,
    },
    hardStopRules: [
      "Stop before any provider call that would exceed that provider's remaining ceiling",
      "Stop before any call that would exceed aggregate remaining (40 − spent)",
      "Count retries against the same ceilings",
      "Stage 2 cannot start without explicit continuation phrase after Stage 1 review",
    ],
  };

  return {
    version: "gdi-option-b-stage-plans-v7",
    hotel: WATERSTONE_V7_HOTEL,
    stage1,
    stage2,
    ledger,
    approvalPhrases: {
      stage1: OPTION_B_STAGE1_APPROVAL_PHRASE,
      stage2: OPTION_B_STAGE2_APPROVAL_PHRASE,
    },
  };
}
