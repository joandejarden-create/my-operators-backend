/**
 * GDI Hotel Operational Preflight V1
 *
 * Determines whether a hotel is ready for standardized GDI research
 * without hotel-specific engineering. Read-only. $0 by default.
 *
 * Does NOT require meeting facilities — room-night demand is equally valid.
 * Does NOT invent census fields. Uncertain affiliation is flagged.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveHotelIdentity,
  preflightHotelEligibility,
} from "./gdi-portfolio-processing-certification-v1.js";
import { preflightCanonicalFullE2e } from "./gdi-canonical-full-e2e-entry-v1.js";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";

export const GDI_HOTEL_OPERATIONAL_PREFLIGHT_V1 =
  "gdi-hotel-operational-preflight-v1";

export const HOTEL_PROCESSING_CLASS = Object.freeze({
  NEW_HOTEL: "NEW_HOTEL",
  EXISTING_HOTEL: "EXISTING_HOTEL",
  REFRESH_REQUIRED: "REFRESH_REQUIRED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  CONFIGURATION_INCOMPLETE: "CONFIGURATION_INCOMPLETE",
});

export const DEMAND_MIX_POSTURE = Object.freeze({
  URBAN_MEETINGS: "URBAN_MEETINGS",
  LEISURE_DESTINATION: "LEISURE_DESTINATION",
  BOUTIQUE_LIMITED_MEETINGS: "BOUTIQUE_LIMITED_MEETINGS",
  MIXED_OR_UNKNOWN: "MIXED_OR_UNKNOWN",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function readJsonSafe(p) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Load Design Hotels CALA census match index (local report — no Airtable).
 */
export function loadDesignHotelsCensusIndex(root = ROOT) {
  const auditPath = path.join(root, "reports/design-hotels-cala-census-audit.json");
  const amenitiesPath = path.join(
    root,
    "reports/design-hotels-census-marriott-amenities-backfill-plan.json"
  );
  const propertyIdPath = path.join(
    root,
    "reports/design-hotels-census-property-id-backfill-plan.json"
  );
  const audit = readJsonSafe(auditPath);
  const amenities = readJsonSafe(amenitiesPath);
  const propertyIds = readJsonSafe(propertyIdPath);
  const byId = new Map();
  for (const m of audit?.matched || []) {
    byId.set(m.censusRecordId, {
      censusRecordId: m.censusRecordId,
      censusName: m.censusName,
      country: m.censusCountry,
      propertyUrl: m.propertyUrl,
      affiliation: "Design Hotels",
      affiliationConfidence: "HIGH",
      affiliationSource: "design-hotels-cala-census-audit",
    });
  }
  for (const row of amenities?.planRows || []) {
    const cur = byId.get(row.censusRecordId) || {
      censusRecordId: row.censusRecordId,
      censusName: row.censusName,
    };
    const amenitiesText = text(row.applyFields?.Amenities || row.amenitiesTextSuggested);
    cur.marshaCode = row.marshaCode || null;
    cur.marriottUrl = row.marriottUrl || null;
    cur.amenitiesText = amenitiesText || null;
    cur.meetingFacilitiesSummary =
      /Conference & Meetings:\s*([^;]+)/i.exec(amenitiesText)?.[1]?.trim() || null;
    cur.hasMeetingFacilities = Boolean(
      cur.meetingFacilitiesSummary &&
        !/^n\/?a$/i.test(cur.meetingFacilitiesSummary)
    );
    byId.set(row.censusRecordId, cur);
  }
  for (const row of propertyIds?.planRows || []) {
    const cur = byId.get(row.censusRecordId) || {
      censusRecordId: row.censusRecordId,
      censusName: row.censusName,
    };
    cur.marshaCode = cur.marshaCode || row.marshaCode || null;
    cur.marriottUrl = cur.marriottUrl || row.marriottUrl || null;
    byId.set(row.censusRecordId, cur);
  }
  // Europe / non-CALA Design Hotels affiliation supplement (e.g. Memmo Lisbon)
  const supplementPath = path.join(
    root,
    "fixtures/gdi-design-hotels-operational-pilot-v1/design-hotels-affiliation-supplement-v1.json"
  );
  const supplement = readJsonSafe(supplementPath);
  for (const row of supplement?.hotels || []) {
    if (!row?.censusRecordId) continue;
    const cur = byId.get(row.censusRecordId) || {};
    byId.set(row.censusRecordId, {
      ...cur,
      censusRecordId: row.censusRecordId,
      censusName: row.censusName || cur.censusName || null,
      country: row.country || cur.country || null,
      propertyUrl: row.propertyUrl || cur.propertyUrl || null,
      affiliation: row.affiliation || cur.affiliation || "Design Hotels",
      affiliationConfidence:
        row.affiliationConfidence || cur.affiliationConfidence || "HIGH",
      affiliationSource:
        row.affiliationSource ||
        cur.affiliationSource ||
        "design-hotels-affiliation-supplement-v1",
      marshaCode: row.marshaCode || cur.marshaCode || null,
      marriottUrl: row.marriottUrl || cur.marriottUrl || null,
      amenitiesText: row.amenitiesText || cur.amenitiesText || null,
      meetingFacilitiesSummary:
        row.meetingFacilitiesSummary || cur.meetingFacilitiesSummary || null,
      hasMeetingFacilities:
        row.hasMeetingFacilities === true || cur.hasMeetingFacilities === true,
      roomsCount: row.roomsCount ?? cur.roomsCount ?? null,
      city: row.city || cur.city || null,
      market: row.market || cur.market || null,
      submarket: row.submarket || cur.submarket || null,
    });
  }
  return { byId, auditPath, matchedCount: byId.size, supplementPath };
}

/**
 * Infer demand-mix posture from amenities / geography (heuristic, not Sales Ready).
 */
export function inferDemandMixPosture(census = {}) {
  const blob = `${text(census.amenitiesText)} ${text(census.propertyUrl)} ${text(
    census.censusName
  )}`.toLowerCase();
  const meetingSpaces = text(census.meetingFacilitiesSummary);
  const multiEvent =
    /\d+\s+event|\d+\s+meeting|conference|banquet|boardroom/i.test(
      meetingSpaces || blob
    );
  const leisure =
    /beach|playa|resort|villa|lagoon|island|monteverde|jungle|eco|scuba|snorkel/i.test(
      blob
    );
  const urban =
    /mexico-city|guadalajara|medellin|quito|leon|centro|city|condesa|polanco|downtown/i.test(
      blob
    );

  // Leisure destinations first (villa/beach/island) — "Boutique" in a resort
  // name must not steal leisure posture.
  if (leisure && !multiEvent) return DEMAND_MIX_POSTURE.LEISURE_DESTINATION;
  if (leisure) return DEMAND_MIX_POSTURE.LEISURE_DESTINATION;
  if (
    /co-working|coworking/i.test(meetingSpaces || "") ||
    (!multiEvent &&
      !leisure &&
      /good hotel|hotelito|\bboutique\b/i.test(blob) &&
      !/villa|resort|playa|beach/i.test(blob))
  ) {
    return DEMAND_MIX_POSTURE.BOUTIQUE_LIMITED_MEETINGS;
  }
  if (multiEvent && urban) return DEMAND_MIX_POSTURE.URBAN_MEETINGS;
  if (urban && multiEvent) return DEMAND_MIX_POSTURE.URBAN_MEETINGS;
  if (urban) return DEMAND_MIX_POSTURE.URBAN_MEETINGS;
  return DEMAND_MIX_POSTURE.MIXED_OR_UNKNOWN;
}

/**
 * Prioritize 14 discovery segments without treating low-priority as failure.
 */
export function prioritizeDiscoverySegments(demandMix) {
  const all = listFourteenWorkflowSegmentIds();
  const boost = {
    [DEMAND_MIX_POSTURE.URBAN_MEETINGS]: [
      "corporate_meetings_incentives",
      "associations_conferences_events",
      "corporate_accounts_scaling",
      "government_public_sector",
      "projects_infrastructure",
      "local_catering_day_meetings",
    ],
    [DEMAND_MIX_POSTURE.LEISURE_DESTINATION]: [
      "travel_trade_group_intermediaries",
      "sports_entertainment",
      "social_special_events",
      "corporate_celebrations_private_events",
      "corporate_meetings_incentives",
      "relocation_extended_stay",
    ],
    [DEMAND_MIX_POSTURE.BOUTIQUE_LIMITED_MEETINGS]: [
      "corporate_meetings_incentives",
      "travel_trade_group_intermediaries",
      "social_special_events",
      "corporate_accounts_scaling",
      "associations_conferences_events",
      "preferred_lodging_partnerships",
    ],
    [DEMAND_MIX_POSTURE.MIXED_OR_UNKNOWN]: [
      "corporate_meetings_incentives",
      "associations_conferences_events",
      "corporate_accounts_scaling",
      "travel_trade_group_intermediaries",
      "projects_infrastructure",
    ],
  };
  const preferred = boost[demandMix] || boost[DEMAND_MIX_POSTURE.MIXED_OR_UNKNOWN] || [];
  const ordered = [
    ...preferred.filter((id) => all.includes(id)),
    ...all.filter((id) => !preferred.includes(id)),
  ];
  return {
    universeCount: all.length,
    prioritized: ordered,
    primaryFocus: preferred,
    note: "All 14 remain available; low allocation to secondary segments is not a failure.",
  };
}

function classifyProcessingState(identity, census) {
  const gaps = [];
  if (!identity.ok) {
    return {
      class: HOTEL_PROCESSING_CLASS.CONFIGURATION_INCOMPLETE,
      gaps: ["INVALID_HPC_HOTEL_ID"],
    };
  }
  if (!census) {
    gaps.push("CENSUS_AFFILIATION_NOT_IN_LOCAL_DESIGN_HOTELS_INDEX");
  }
  if (census && !census.country) gaps.push("COUNTRY_UNRESOLVED");
  if (identity.hasOpportunityStore && identity.opportunityCount > 0) {
    if (identity.opportunityCount < 5) {
      return {
        class: HOTEL_PROCESSING_CLASS.RESEARCH_CONTINUATION,
        gaps,
      };
    }
    return {
      class: HOTEL_PROCESSING_CLASS.EXISTING_HOTEL,
      gaps,
    };
  }
  if (!identity.hasProfile && !identity.hasOpportunityStore) {
    return {
      class: HOTEL_PROCESSING_CLASS.NEW_HOTEL,
      gaps,
    };
  }
  if (identity.hasProfile && !identity.hasOpportunityStore) {
    return {
      class: HOTEL_PROCESSING_CLASS.REFRESH_REQUIRED,
      gaps: [...gaps, "PROFILE_WITHOUT_OPPORTUNITY_INVENTORY"],
    };
  }
  return { class: HOTEL_PROCESSING_CLASS.NEW_HOTEL, gaps };
}

/**
 * Universal hotel preflight — ready for PowerShell Preview / Rehearsal.
 */
export function runHotelOperationalPreflight(hotelId, opts = {}) {
  const root = opts.root || ROOT;
  const identity = resolveHotelIdentity(hotelId, { root });
  const eligibility = preflightHotelEligibility(hotelId, { root });
  const canonical = preflightCanonicalFullE2e({
    hotelId: text(hotelId),
    mode: "preflight",
    dryRun: true,
    budgetCeilingUsd: 0,
  });
  const { byId } = loadDesignHotelsCensusIndex(root);
  const census = byId.get(text(hotelId)) || null;

  const uncertainAffiliation =
    !census &&
    /tribute|city express|banyan tree|hyatt|hilton/i.test(
      `${identity.hotelName || ""} ${identity.brand || ""}`
    );

  const demandMixHint = text(census?.demandMixHint).toUpperCase();
  const demandMixFromHint = Object.values(DEMAND_MIX_POSTURE).includes(demandMixHint)
    ? demandMixHint
    : null;
  const demandMix =
    demandMixFromHint ||
    (census ? inferDemandMixPosture(census) : DEMAND_MIX_POSTURE.MIXED_OR_UNKNOWN);
  const segments = prioritizeDiscoverySegments(demandMix);
  const processing = classifyProcessingState(identity, census);

  const checks = {
    hotelIdentity: identity.ok,
    marketGeography: Boolean(census?.country || identity.country),
    hotelPositioningDemandMix: Boolean(census || identity.hotelName),
    roomsAndMeetingSpace: {
      roomsKnown: census?.roomsCount != null && Number(census.roomsCount) > 0,
      roomsCount: census?.roomsCount != null ? Number(census.roomsCount) : null,
      meetingFacilitiesRequired: false,
      meetingFacilitiesPresent: census?.hasMeetingFacilities === true,
      meetingFacilitiesSummary: census?.meetingFacilitiesSummary || null,
      note: "Meeting facilities are optional — room-night demand remains GDI-eligible.",
    },
    demandGenerators: "DEFERRED_TO_RESEARCH_RUN",
    existingResearchInventory: {
      hasProfile: identity.hasProfile === true,
      hasOpportunityStore: identity.hasOpportunityStore === true,
      opportunityCount: identity.opportunityCount || 0,
    },
    existingContactIntelligence: "DEFERRED_TO_RESEARCH_RUN",
    availableResearchBudgetUsd: Number(opts.budgetUsd || 0) || 0,
    sourceCoverage: census?.propertyUrl || census?.marriottUrl ? "PARTIAL_DIRECTORY" : "UNKNOWN",
    publicationAuthorization: false,
  };

  const blockers = [];
  if (!identity.ok) blockers.push(identity.error || "INVALID_HOTEL");
  if (!canonical.ok) {
    for (const e of canonical.errors || []) blockers.push(e.code || String(e));
  }
  if (uncertainAffiliation) {
    blockers.push("AFFILIATION_UNCERTAIN_FLAG_FOR_FOUNDER");
  }

  const warnings = [...(eligibility.warnings || [])];
  for (const g of processing.gaps) warnings.push({ code: g });
  if (!checks.roomsAndMeetingSpace.roomsKnown) {
    warnings.push({
      code: "ROOMS_COUNT_UNKNOWN_LOCALLY",
      note: "Do not invent room counts; resolve from census/HPC when available.",
    });
  }

  const readyForDryRun = blockers.length === 0;
  const readyForPaidResearch =
    readyForDryRun &&
    opts.authorizePaidResearch === true &&
    Number(opts.budgetUsd || 0) > 0;

  return {
    contract: GDI_HOTEL_OPERATIONAL_PREFLIGHT_V1,
    hotelId: text(hotelId),
    ok: readyForDryRun,
    readyForDryRun,
    readyForPaidResearch,
    processingClass: processing.class,
    demandMixPosture: demandMix,
    hotel: {
      hpcId: text(hotelId),
      name: census?.censusName || identity.hotelName || null,
      country: census?.country || identity.country || null,
      city: census?.city || identity.city || null,
      market: census?.market || census?.country || identity.country || null,
      submarket: census?.submarket || null,
      affiliation: census?.affiliation || null,
      affiliationConfidence: census?.affiliationConfidence || null,
      marshaCode: census?.marshaCode || null,
      propertyUrl: census?.propertyUrl || null,
      marriottUrl: census?.marriottUrl || null,
      meetingFacilitiesSummary: census?.meetingFacilitiesSummary || null,
      roomsCount:
        census?.roomsCount != null ? Number(census.roomsCount) : null,
    },
    checks,
    discoverySegments: segments,
    eligibility,
    canonicalPreflight: {
      ok: canonical.ok,
      errors: canonical.errors || [],
    },
    blockers,
    warnings,
    existingGdi: {
      hasLocalArtifacts: identity.hasProfile || identity.hasOpportunityStore,
      opportunityCount: identity.opportunityCount || 0,
    },
    publication: {
      authorized: false,
      note: "Customer publication requires separate founder authorization.",
    },
    persistence: {
      airtableWritesAuthorized: false,
      note: "Research-inventory apply requires separate founder GO.",
    },
  };
}

/**
 * Batch preflight for a hotel list (Design Hotels pilot / any portfolio).
 */
export function runHotelOperationalPreflightBatch(hotelIds = [], opts = {}) {
  const results = (hotelIds || []).map((id) =>
    runHotelOperationalPreflight(id, opts)
  );
  return {
    contract: GDI_HOTEL_OPERATIONAL_PREFLIGHT_V1,
    count: results.length,
    readyCount: results.filter((r) => r.readyForDryRun).length,
    blockedCount: results.filter((r) => !r.readyForDryRun).length,
    byClass: Object.fromEntries(
      Object.values(HOTEL_PROCESSING_CLASS).map((c) => [
        c,
        results.filter((r) => r.processingClass === c).length,
      ])
    ),
    results,
  };
}
