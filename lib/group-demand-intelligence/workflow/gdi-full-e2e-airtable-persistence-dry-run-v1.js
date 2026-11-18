/**
 * Dry-run Full E2E Airtable persistence planner.
 * Plans idempotent upserts against EXISTING adapters; refuses live writes.
 */

import {
  AIRTABLE_PRODUCTION_WRITE_GUARD,
  GDI_FULL_E2E_AIRTABLE_PERSISTENCE_CONTRACT_V1,
  WRITE_OUTCOME,
  routeResearchObjectToAirtable,
} from "./gdi-full-e2e-airtable-persistence-contract-v1.js";
import { MAP_DEMAND_GENERATOR } from "../demand-generators/airtable-field-map.js";
import { MAP_GDI_OPPORTUNITY } from "../opportunity-field-map.js";
import { MAP_PE_VENUE } from "../private-events/airtable-field-map.js";
import { normalizeOrgIdentityKey } from "./reuse-prior-intelligence-v1.js";
import { computeDemandGeneratorId } from "../demand-generators/entities.js";
import { computeVenueId } from "../private-events/venue-entity.js";
import { resolveAuthoritativeVenueId } from "./gdi-multi-hotel-airtable-reconciliation-v2.js";
import { evaluateFullE2eAirtableWriteEligibility } from "./gdi-full-e2e-airtable-persistence-eligibility-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function classifyCandidateKind(candidate = {}) {
  const segmentId = text(candidate.segmentId);
  const disposition = text(candidate.disposition || candidate.evaluationLabel);
  if (
    segmentId === "preferred_lodging_partnerships" ||
    /mansion|venue|estate|museum|hall/i.test(candidate.organizationName || "")
  ) {
    if (segmentId === "preferred_lodging_partnerships") return "VENUE";
  }
  if (
    segmentId === "professional_event_planners" ||
    segmentId === "travel_trade_group_intermediaries"
  ) {
    return "PLANNER";
  }
  if (
    disposition === "NEW_UNIQUE" ||
    disposition === "WATCHLIST_RESEARCH" ||
    candidate.hotelQualified === true
  ) {
    return "HOTEL_OPPORTUNITY";
  }
  // Credible reusable global org (e.g. out-of-market retain) — not raw noise.
  if (
    candidate.retainGlobally === true ||
    candidate.credibleGlobal === true ||
    candidate.outOfMarket === true
  ) {
    return "ORGANIZATION";
  }
  if (disposition === "EXCLUDE" || disposition === "REJECTED") {
    return "RAW_DISCOVERY";
  }
  return "ORGANIZATION";
}

/**
 * Build a planned field payload for dry-run (never writes).
 */
export function planAirtableFieldsForCandidate(candidate = {}, hotel = {}) {
  const kind = classifyCandidateKind(candidate);
  const hotelQualified =
    candidate.hotelQualified === true ||
    text(candidate.disposition) === "WATCHLIST_RESEARCH" ||
    text(candidate.evaluationLabel) === "NEW_UNIQUE";
  const unsupported =
    text(candidate.disposition) === "EXCLUDE" ||
    text(candidate.evaluationLabel) === "REJECTED";
  const outOfMarket = candidate.outOfMarket === true;
  const routing = routeResearchObjectToAirtable({
    kind,
    segmentId: candidate.segmentId,
    hotelQualified,
    unsupported,
    outOfMarket,
    credibleGlobal: !unsupported || Boolean(candidate.retainGlobally),
  });

  const orgKey = normalizeOrgIdentityKey(
    candidate.organizationName || candidate.company || "",
  );
  const planned = [];

  const eligibility = evaluateFullE2eAirtableWriteEligibility({
    kind,
    candidate: {
      ...candidate,
      hotelQualified,
      disposition: candidate.disposition || candidate.evaluationLabel,
    },
  });

  for (const target of routing.airtable || []) {
    const tableName = target.table?.name;
    if (!eligibility.allowedTables.includes(tableName)) {
      planned.push({
        outcome: WRITE_OUTCOME.DEFERRED,
        tableId: target.table.tableId,
        tableName,
        role: target.role,
        identityKey: orgKey,
        fields: {},
        readBackRequired: false,
        eligibilityDeferred: true,
        eligibilityReasons: eligibility.reasons,
      });
      continue;
    }
    let fields = {};
    let identityKey = orgKey;
    if (tableName === "Demand Generators") {
      identityKey = computeDemandGeneratorId({
        organizationName: candidate.organizationName || candidate.company,
        website: candidate.website || candidate.sourceUrl || null,
        officialDomain: candidate.officialDomain || null,
      });
      fields = {
        [MAP_DEMAND_GENERATOR.demandGeneratorId]: identityKey,
        [MAP_DEMAND_GENERATOR.organizationName]:
          candidate.organizationName || null,
        [MAP_DEMAND_GENERATOR.generatorStatus]: unsupported
          ? "WATCH"
          : "DISCOVERED",
        [MAP_DEMAND_GENERATOR.primaryMarkets]: hotel.city || null,
        [MAP_DEMAND_GENERATOR.researchReason]: text(
          candidate.segmentId || candidate.matchReason,
        ),
        [MAP_DEMAND_GENERATOR.sourceUrls]: Array.isArray(candidate.sourceUrls)
          ? candidate.sourceUrls.join("\n")
          : candidate.sourceUrl || null,
      };
    } else if (tableName === "Private Event Venues") {
      // Prefer domain-stable venue IDs. Never let geo-only hashes silently
      // replace an existing domain-backed identity (Rock Creek class defect).
      const venueResolution = resolveAuthoritativeVenueId({
        venueName: candidate.organizationName,
        city: hotel.city || candidate.city || null,
        region: hotel.region || candidate.region || null,
        website: candidate.website || candidate.sourceUrl || null,
        officialDomain: candidate.officialDomain || null,
        existingStructuredVenueId: candidate.existingVenueId || null,
        existingPayloadVenueId: candidate.payloadVenueId || null,
      });
      identityKey =
        venueResolution.authoritativeId ||
        computeVenueId({
          venueName: candidate.organizationName,
          city: hotel.city || candidate.city || null,
          website: candidate.website || candidate.sourceUrl || null,
          officialDomain: candidate.officialDomain || null,
        });
      fields = {
        [MAP_PE_VENUE.venueId]: identityKey,
        [MAP_PE_VENUE.venueName]: candidate.organizationName || null,
        [MAP_PE_VENUE.onSiteLodgingStatus]:
          candidate.onSiteLodgingStatus || "UNKNOWN",
        [MAP_PE_VENUE.city]: hotel.city || null,
        [MAP_PE_VENUE.website]:
          candidate.website || candidate.sourceUrl || null,
        [MAP_PE_VENUE.officialDomain]: candidate.officialDomain || null,
      };
    } else if (tableName === "Group Demand Opportunities") {
      identityKey = text(
        candidate.opportunityId || candidate.id || `opp_${orgKey}`,
      );
      fields = {
        [MAP_GDI_OPPORTUNITY.opportunityId]: identityKey,
        [MAP_GDI_OPPORTUNITY.hotelId]: hotel.hotelId || candidate.hotelId,
        [MAP_GDI_OPPORTUNITY.organizationName]:
          candidate.organizationName || null,
        [MAP_GDI_OPPORTUNITY.segment]: candidate.segmentId || null,
        [MAP_GDI_OPPORTUNITY.opportunityName]:
          candidate.opportunityName ||
          candidate.title ||
          candidate.organizationName ||
          null,
      };
    } else {
      identityKey = `${tableName}:${orgKey}`;
      fields = {
        Organization: candidate.organizationName || null,
        Segment: candidate.segmentId || null,
        HotelId: hotel.hotelId || candidate.hotelId || null,
      };
    }

    planned.push({
      outcome: WRITE_OUTCOME.DEFERRED,
      tableId: target.table.tableId,
      tableName,
      role: target.role,
      identityKey,
      fields,
      readBackRequired: true,
      eligibilityReasons: eligibility.reasons,
      commercialQualificationStatus: eligibility.commercialQualificationStatus,
    });
  }

  return {
    organizationName: candidate.organizationName || null,
    organizationKey: orgKey,
    kind,
    routing,
    plannedWrites: planned,
    nonAirtable: routing.nonAirtable || [],
  };
}

/**
 * Dry-run persistence for a batch of candidates.
 * apply/liveWrite always refused.
 */
export function dryRunFullE2eAirtablePersistence(input = {}) {
  if (input.apply === true || input.liveWrite === true) {
    return {
      ok: false,
      ...AIRTABLE_PRODUCTION_WRITE_GUARD,
      version: GDI_FULL_E2E_AIRTABLE_PERSISTENCE_CONTRACT_V1,
    };
  }

  const hotel = {
    hotelId: text(input.hotelId),
    city: text(input.city),
    displayName: text(input.displayName || input.hotelName),
  };
  const candidates = Array.isArray(input.candidates) ? input.candidates : [];
  const plans = candidates.map((c) => planAirtableFieldsForCandidate(c, hotel));

  const byTable = {};
  let plannedWriteCount = 0;
  let deferredAuditOnly = 0;
  for (const p of plans) {
    if (!p.plannedWrites.length) deferredAuditOnly += 1;
    for (const w of p.plannedWrites) {
      plannedWriteCount += 1;
      byTable[w.tableName] = (byTable[w.tableName] || 0) + 1;
    }
  }

  return {
    ok: true,
    dryRun: true,
    version: GDI_FULL_E2E_AIRTABLE_PERSISTENCE_CONTRACT_V1,
    hotelId: hotel.hotelId,
    runId: text(input.runId) || null,
    candidateCount: candidates.length,
    plannedWriteCount,
    deferredAuditOnly,
    byTable,
    plans,
    mutationGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
    readBackContract: {
      required: true,
      rule: "API 200 alone is insufficient; identity field must read back equal to planned identityKey before CREATED/UPDATED is certified.",
      outcomes: WRITE_OUTCOME,
    },
  };
}

export default {
  planAirtableFieldsForCandidate,
  dryRunFullE2eAirtablePersistence,
};
