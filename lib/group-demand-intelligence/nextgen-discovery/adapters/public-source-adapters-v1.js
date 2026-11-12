/**
 * First-wave public / historical source adapters.
 * Reuses existing independent discovery + official historical notices.
 * Zero paid by default.
 */

import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import {
  DISCOVERY_EVIDENCE_CLASS,
  stampDiscoveryEvidenceClass,
} from "../shared/discovery-evidence-class-v1.js";
import { discoverGovernmentProcurementLiveOrHistorical } from "../hunters/government-procurement-hunter-v1.js";
import { runIndependentDemandDiscovery } from "../../research/independent-demand-discovery-v1.js";
import {
  getCpeIndependentDiscoverySegmentConfig,
  getCmtIndependentDiscoverySegmentConfig,
} from "../../research/corporate-projects-extended-stay-discovery-v1.js";
import { normalizeHunterDiscoveries } from "../orchestration/normalize-discovery-v1.js";

export const PUBLIC_SOURCE_ADAPTERS_V1 = "gdi-nextgen-public-source-adapters-v1";

/** USASpending public awards surface — independently verifiable official site. */
export const USASPENDING_PROJECT_HISTORICAL = Object.freeze({
  organizationName: "U.S. Government award recipients (USASpending)",
  domain: "usaspending.gov",
  demandTrigger: "federal award / contractor mobilization lodging adjacency",
  activityTitle:
    "USASpending.gov award explorer — contractor project travel demand (historical official public)",
  activityDate: null,
  activityDateVerified: false,
  activityDateKind: "ONGOING_PROGRAM_NO_SINGLE_EDITION",
  geographicScope: "US / hotel market catchment when award place of performance overlaps",
  evidenceUrls: ["https://www.usaspending.gov/"],
  contactName: null,
  contactRole: "Award recipient project office (unverified)",
  buyerVerified: false,
  intermediaryVerified: false,
  pursuitThesis:
    "Hotels near award places of performance can develop project-crew and inspector lodging packages when mobilization language appears on verified awards; USASpending proves the public award surface, not a specific hotel RFP.",
  confidence: 0.52,
  hotelDemandRelevance: 0.6,
  recordKind: "DEMAND_TRIGGER",
  uncertainties: [
    "Specific award + place-of-performance match to this hotel is not verified in this zero-paid pass.",
    "Named lodging buyer not identified.",
  ],
  sourceAuthority: "Tier_A",
  researchMethodId: "GDI-PROJECT-PUBLIC-01",
});

/** Meetings industry public surface — historical official/public program page. */
export const PCMA_MEETINGS_HISTORICAL = Object.freeze({
  organizationName: "PCMA (Professional Convention Management Association)",
  domain: "pcma.org",
  demandTrigger: "meetings industry education / corporate meeting planner adjacency",
  activityTitle:
    "PCMA education & events — meeting-planner demand adjacency (historical public)",
  activityDate: null,
  activityDateVerified: false,
  activityDateKind: "ONGOING_PROGRAM_NO_SINGLE_EDITION",
  geographicScope: "US / destination markets hosting PCMA education events",
  evidenceUrls: ["https://www.pcma.org/"],
  contactName: null,
  contactRole: "Meeting planner / education program contact (unverified)",
  buyerVerified: false,
  intermediaryVerified: false,
  pursuitThesis:
    "Hotels can pursue account development with planners active in PCMA education cycles when destination and timing fit; public association surface is evidenced, named buyer and specific room blocks are not.",
  confidence: 0.5,
  hotelDemandRelevance: 0.55,
  recordKind: "DEMAND_TRIGGER",
  uncertainties: [
    "Edition-specific event city/dates not verified in this pass.",
    "Named buyer not verified.",
  ],
  sourceAuthority: "Tier_B",
  researchMethodId: "GDI-CMT-PUBLIC-01",
});

/** Maryland SoccerPlex / travel-team housing — independently verifiable public page. */
export const MD_SOCCERPLEX_HOUSING_HISTORICAL = Object.freeze({
  organizationName: "Maryland SoccerPlex / Maryland Sports",
  domain: "mdsoccerplex.com",
  demandTrigger: "youth tournament stay-to-play housing adjacency",
  activityTitle:
    "Maryland SoccerPlex tournament calendar — visiting-team lodging demand (historical public program page)",
  activityDate: null,
  activityDateVerified: false,
  activityDateKind: "ONGOING_PROGRAM_NO_SINGLE_EDITION",
  geographicScope: "Germantown / Montgomery County MD / Bethesda lodging catchment",
  evidenceUrls: ["https://www.mdsoccerplex.com/"],
  contactName: null,
  contactRole: "Tournament / housing coordinator (unverified)",
  buyerVerified: false,
  intermediaryVerified: false,
  pursuitThesis:
    "Bethesda Marriott can pursue stay-to-play overflow and visiting-team packages for SoccerPlex events when primary Germantown inventory is constrained; program existence is public, edition-specific room blocks remain unverified.",
  confidence: 0.58,
  hotelDemandRelevance: 0.72,
  recordKind: "DEMAND_TRIGGER",
  uncertainties: [
    "Specific upcoming tournament editions and contracted hotels are not verified in this zero-paid pass.",
    "Named housing buyer not identified.",
  ],
  sourceAuthority: "Tier_B",
  researchMethodId: "GDI-SPORTS-PUBLIC-01",
});

/** Georgetown University public events / alumni demand adjacency. */
export const GEORGETOWN_PUBLIC_EVENTS_HISTORICAL = Object.freeze({
  organizationName: "Georgetown University",
  domain: "georgetown.edu",
  demandTrigger: "university campus events / alumni weekend lodging adjacency",
  activityTitle:
    "Georgetown University events & campus calendar — visitor lodging demand (historical public)",
  activityDate: null,
  activityDateVerified: false,
  activityDateKind: "ONGOING_PROGRAM_NO_SINGLE_EDITION",
  geographicScope: "Georgetown / Washington DC / Bethesda lodging catchment",
  evidenceUrls: ["https://www.georgetown.edu/events/"],
  contactName: null,
  contactRole: "University events / conference services (unverified)",
  buyerVerified: false,
  intermediaryVerified: false,
  pursuitThesis:
    "Bethesda Marriott can develop preferred lodging relationships for Georgetown-adjacent visitor and alumni demand when campus-proximate inventory is tight; calendar existence is public, named buyer and specific weekend room blocks remain unverified.",
  confidence: 0.55,
  hotelDemandRelevance: 0.7,
  recordKind: "DEMAND_TRIGGER",
  uncertainties: [
    "Edition-specific alumni/homecoming dates require further verification.",
    "Named lodging buyer not verified.",
  ],
  sourceAuthority: "Tier_B",
  researchMethodId: "GDI-UNIV-PUBLIC-01",
});

function mapIndependentCandidate(c, hunterId) {
  return stampDiscoveryEvidenceClass(
    {
      organizationName: c.organizationName || c.orgName,
      activityTitle: c.activityTitle || c.title || c.activity,
      activityDate: c.activityDate || c.dates || null,
      activityDateVerified: false,
      geographicScope: c.geography || c.city || null,
      demandTrigger: c.demandTrigger || c.activityTitle || c.title,
      pursuitThesis: c.hotelOpportunityThesis || c.defaultThesis || c.thesis,
      evidenceUrls: [
        ...(c.evidenceUrls || []),
        c.sourceUrl,
        c.url,
      ].filter(Boolean),
      sourceUrl: c.sourceUrl || c.url || null,
      confidence: c.confidence ?? 0.5,
      hotelDemandRelevance: c.hotelDemandRelevance ?? 0.55,
      recordKind: "DEMAND_TRIGGER",
      segmentId: c.segmentId,
      provider: "independent_public",
      researchMethodId: "GDI-INDEPENDENT-PUBLIC-01",
      sourceHunter: hunterId,
      uncertainties: c.reasons || c.uncertainties || [
        "Independent public extract — named buyer not verified",
      ],
    },
    DISCOVERY_EVIDENCE_CLASS.LIVE_PUBLIC_DISCOVERY
  );
}

async function discoverViaIndependentPublic({
  hunterId,
  hotel = {},
  segmentConfig,
  skipDirectPublicFetches = false,
  maxCandidates = 5,
}) {
  const result = await runIndependentDemandDiscovery({
    hotel,
    segmentConfig,
    skipDirectPublicFetches,
    limits: { maxExternalRetrievals: 8, maxCandidates },
    authorizeSearchIndexDiscovery: false,
    authorizeLiveMeteredSearch: false,
    budgetCeilingUsd: 0,
  });
  const candidates = (result.candidates || []).map((c) =>
    mapIndependentCandidate(c, hunterId)
  );
  return {
    discoveries: normalizeHunterDiscoveries(candidates, {
      hunterId,
      hotelId: hotel.hotelId || hotel.id,
      provider: "independent_public",
      segmentId: segmentConfig.segmentId,
    }),
    meta: {
      mode: result.ok
        ? "ZERO_PAID_INDEPENDENT_PUBLIC"
        : "ZERO_PAID_INDEPENDENT_PUBLIC_LIMITED",
      discoveryEvidenceClass: DISCOVERY_EVIDENCE_CLASS.LIVE_PUBLIC_DISCOVERY,
      paidAuthorized: false,
      budgetUsdSpent: 0,
      independent: {
        ok: result.ok,
        stagesCompleted: result.stagesCompleted,
        stopReasons: result.stopReasons,
        retrievalsUsed: result.retrievalsUsed,
        estimatedUsd: result.estimatedUsd,
      },
    },
  };
}

export async function runPublicSourceAdapter(hunterId, ctx = {}) {
  const hotel = {
    hotelId: ctx.hotelId,
    name: ctx.hotelName,
    city: ctx.city || ctx.territory || "",
    country: ctx.country || "US",
    ...(ctx.hotel || {}),
  };

  if (hunterId === SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT) {
    const gov = await discoverGovernmentProcurementLiveOrHistorical({
      ...ctx,
      attemptLivePublic: ctx.attemptLivePublic !== false,
    });
    return {
      discoveries: normalizeHunterDiscoveries(gov.discoveries || [], {
        hunterId,
        hotelId: ctx.hotelId,
        provider: "public_api",
        discoveryEvidenceClass: gov.meta?.discoveryEvidenceClass,
      }),
      meta: { ...gov.meta, adapter: PUBLIC_SOURCE_ADAPTERS_V1 },
    };
  }

  if (hunterId === SPECIALIST_HUNTER.PROJECT_WORKFORCE) {
    if (ctx.preferHistoricalOnly === true || ctx.skipDirectPublicFetches !== false) {
      const row = stampDiscoveryEvidenceClass(
        { ...USASPENDING_PROJECT_HISTORICAL, hotelId: ctx.hotelId },
        DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
      );
      return {
        discoveries: normalizeHunterDiscoveries([row], {
          hunterId,
          hotelId: ctx.hotelId,
          provider: "public_api",
        }),
        meta: {
          mode: "ZERO_PAID_HISTORICAL_OFFICIAL_PUBLIC",
          discoveryEvidenceClass:
            DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY,
          paidAuthorized: false,
          budgetUsdSpent: 0,
          adapter: PUBLIC_SOURCE_ADAPTERS_V1,
          note: "USASpending historical official surface; live independent fetches optional via skipDirectPublicFetches=false.",
        },
      };
    }
    const out = await discoverViaIndependentPublic({
      hunterId,
      hotel,
      segmentConfig: getCpeIndependentDiscoverySegmentConfig(),
      skipDirectPublicFetches: false,
    });
    out.meta.adapter = PUBLIC_SOURCE_ADAPTERS_V1;
    return out;
  }

  if (hunterId === SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS) {
    if (ctx.preferHistoricalOnly === true || ctx.skipDirectPublicFetches !== false) {
      const row = stampDiscoveryEvidenceClass(
        { ...PCMA_MEETINGS_HISTORICAL, hotelId: ctx.hotelId },
        DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
      );
      return {
        discoveries: normalizeHunterDiscoveries([row], {
          hunterId,
          hotelId: ctx.hotelId,
          provider: "public_page",
        }),
        meta: {
          mode: "ZERO_PAID_HISTORICAL_PUBLIC_PROGRAM",
          discoveryEvidenceClass:
            DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY,
          paidAuthorized: false,
          budgetUsdSpent: 0,
          adapter: PUBLIC_SOURCE_ADAPTERS_V1,
        },
      };
    }
    const out = await discoverViaIndependentPublic({
      hunterId,
      hotel,
      segmentConfig: getCmtIndependentDiscoverySegmentConfig(),
      skipDirectPublicFetches: false,
    });
    out.meta.adapter = PUBLIC_SOURCE_ADAPTERS_V1;
    return out;
  }

  if (hunterId === SPECIALIST_HUNTER.SPORTS_HOUSING) {
    const row = stampDiscoveryEvidenceClass(
      { ...MD_SOCCERPLEX_HOUSING_HISTORICAL, hotelId: ctx.hotelId },
      DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
    );
    return {
      discoveries: normalizeHunterDiscoveries([row], {
        hunterId,
        hotelId: ctx.hotelId,
        provider: "public_page",
      }),
      meta: {
        mode: "ZERO_PAID_HISTORICAL_PUBLIC_PROGRAM",
        discoveryEvidenceClass: DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY,
        paidAuthorized: false,
        budgetUsdSpent: 0,
        adapter: PUBLIC_SOURCE_ADAPTERS_V1,
        incomplete: [
          "Edition-specific tournament housing not verified.",
          "Named buyer not verified — research-stage only.",
        ],
      },
    };
  }

  if (hunterId === SPECIALIST_HUNTER.UNIVERSITY_DEMAND) {
    const row = stampDiscoveryEvidenceClass(
      { ...GEORGETOWN_PUBLIC_EVENTS_HISTORICAL, hotelId: ctx.hotelId },
      DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
    );
    return {
      discoveries: normalizeHunterDiscoveries([row], {
        hunterId,
        hotelId: ctx.hotelId,
        provider: "public_page",
      }),
      meta: {
        mode: "ZERO_PAID_HISTORICAL_PUBLIC_PROGRAM",
        discoveryEvidenceClass: DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY,
        paidAuthorized: false,
        budgetUsdSpent: 0,
        adapter: PUBLIC_SOURCE_ADAPTERS_V1,
        incomplete: [
          "Specific alumni/homecoming dates not verified in this pass.",
          "Named buyer not verified — research-stage only.",
        ],
      },
    };
  }

  return {
    discoveries: [],
    meta: {
      mode: "ADAPTER_NOT_IMPLEMENTED",
      hunterId,
      paidAuthorized: false,
      budgetUsdSpent: 0,
      adapter: PUBLIC_SOURCE_ADAPTERS_V1,
    },
  };
}
