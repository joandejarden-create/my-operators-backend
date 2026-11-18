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

  for (const target of routing.airtable || []) {
    const tableName = target.table?.name;
    let fields = {};
    let identityKey = orgKey;
    if (tableName === "Demand Generators") {
      identityKey = `dg_${orgKey || "unknown"}`;
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
      identityKey = `pev_${orgKey || "unknown"}`;
      fields = {
        [MAP_PE_VENUE.venueId]: identityKey,
        [MAP_PE_VENUE.venueName]: candidate.organizationName || null,
        [MAP_PE_VENUE.onSiteLodgingStatus]:
          candidate.onSiteLodgingStatus || "UNKNOWN",
        [MAP_PE_VENUE.city]: hotel.city || null,
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
