/**
 * Import Webhound GDI discovery output → seedCandidates for runGroupDemandResearch.
 * REUSABLE_PRODUCT_LOGIC — hotel-agnostic mapper; hotelId passed in.
 */

import {
  BOOKING_WINDOW,
  CLAIM_KIND,
  DEMAND_STATUS,
  OPPORTUNITY_TYPE,
  ROOM_DEMAND_STATUS,
  VENUE_SOURCING_STATUS,
} from "./claim-types.js";
import { TERRITORY_CLASS } from "./demand-territory.js";
import { normalizeVenueStatus } from "./provider-normalization.js";

const OPP_TYPE_MAP = Object.freeze({
  PRIMARY_PURSUIT: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
  OVERFLOW_HOUSING: OPPORTUNITY_TYPE.OVERFLOW_HOUSING,
  OVERFLOW: OPPORTUNITY_TYPE.OVERFLOW_HOUSING,
  REACTIVATION: OPPORTUNITY_TYPE.REACTIVATION,
  FUTURE_CYCLE: OPPORTUNITY_TYPE.FUTURE_CYCLE,
  CLOSED_DISQUALIFIED: OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED,
  CLOSED: OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED,
});

const VENUE_MAP = Object.freeze({
  OPEN_UNRESOLVED: VENUE_SOURCING_STATUS.OPEN_UNRESOLVED,
  RFP_ACTIVE: VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING,
  RFP_ACTIVE_SOURCING: VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING,
  HOTEL_VENUE_TBD: VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD,
  PARTIALLY_PLACED: VENUE_SOURCING_STATUS.PARTIALLY_PLACED,
  PRIMARY_SELECTED_OVERFLOW_POSSIBLE:
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
  PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE:
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
  PRIMARY_SELECTED_NO_OVERFLOW:
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
  PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE:
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
  FULLY_PLACED: VENUE_SOURCING_STATUS.FULLY_PLACED,
  CURRENT_CYCLE_CLOSED: VENUE_SOURCING_STATUS.CURRENT_CYCLE_CLOSED,
  UNKNOWN: VENUE_SOURCING_STATUS.UNKNOWN,
});

const TERRITORY_MAP = Object.freeze({
  CORE: TERRITORY_CLASS.TERRITORY_CORE,
  TERRITORY_CORE: TERRITORY_CLASS.TERRITORY_CORE,
  NEARBY: TERRITORY_CLASS.TERRITORY_NEARBY,
  TERRITORY_NEARBY: TERRITORY_CLASS.TERRITORY_NEARBY,
  COMPETITIVE: TERRITORY_CLASS.TERRITORY_COMPETITIVE,
  TERRITORY_COMPETITIVE: TERRITORY_CLASS.TERRITORY_COMPETITIVE,
  STRETCH: TERRITORY_CLASS.TERRITORY_STRETCH,
  TERRITORY_STRETCH: TERRITORY_CLASS.TERRITORY_STRETCH,
  OUTSIDE: TERRITORY_CLASS.OUTSIDE_REALISTIC_TERRITORY,
  OUTSIDE_REALISTIC_TERRITORY: TERRITORY_CLASS.OUTSIDE_REALISTIC_TERRITORY,
});

const ROOM_DEMAND_MAP = Object.freeze({
  VerifiedRoomBlock: ROOM_DEMAND_STATUS.VERIFIED_ROOM_BLOCK,
  VERIFIED_ROOM_BLOCK: ROOM_DEMAND_STATUS.VERIFIED_ROOM_BLOCK,
  VerifiedHousingProgram: ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM,
  VERIFIED_HOUSING_PROGRAM: ROOM_DEMAND_STATUS.VERIFIED_HOUSING_PROGRAM,
  StrongRoomDemandEvidence: ROOM_DEMAND_STATUS.STRONG_ROOM_DEMAND_EVIDENCE,
  STRONG_ROOM_DEMAND_EVIDENCE: ROOM_DEMAND_STATUS.STRONG_ROOM_DEMAND_EVIDENCE,
  EstimatedRoomDemand: ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND,
  ESTIMATED_ROOM_DEMAND: ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND,
  LocalLimitedRoomDemand: ROOM_DEMAND_STATUS.LOCAL_LIMITED_ROOM_DEMAND,
  LOCAL_LIMITED_ROOM_DEMAND: ROOM_DEMAND_STATUS.LOCAL_LIMITED_ROOM_DEMAND,
  OverflowOnly: ROOM_DEMAND_STATUS.OVERFLOW_ONLY,
  OVERFLOW_ONLY: ROOM_DEMAND_STATUS.OVERFLOW_ONLY,
  Unknown: ROOM_DEMAND_STATUS.UNKNOWN,
  UNKNOWN: ROOM_DEMAND_STATUS.UNKNOWN,
});

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function mapEvidence(sources = []) {
  return (sources || [])
    .map((s) => {
      if (typeof s === "string") {
        return {
          field: "source",
          value: s,
          claimKind: CLAIM_KIND.FACT,
          sourceUrl: s.startsWith("http") ? s : null,
          sourceTitle: s,
          sourceAuthority: "Tier_B",
          researchProvider: "webhound",
          researchLevel: "L5_WEBHOUND",
        };
      }
      return {
        field: s.field || "source",
        value: s.title || s.url || s.value || null,
        claimKind: CLAIM_KIND.FACT,
        sourceUrl: s.url || s.sourceUrl || null,
        sourceTitle: s.title || s.sourceTitle || null,
        sourceDomain: s.domain || null,
        sourceAuthority: s.authority || "Tier_B",
        researchProvider: "webhound",
        researchLevel: "L5_WEBHOUND",
      };
    })
    .filter((e) => e.sourceUrl || e.sourceTitle);
}

/**
 * Normalize one Webhound-structured opportunity row into a GDI raw candidate.
 */
export function mapWebhoundOpportunityRow(row, hotelId, index = 0) {
  const title = row.title || row.opportunity || row.name;
  if (!title) return null;

  const oppTypeRaw = String(row.opportunityType || row.type || "PRIMARY_PURSUIT")
    .toUpperCase()
    .replace(/[\s/-]+/g, "_");
  // Prefer explicit sourcingStatus; do not treat venue *name* as status.
  const venueRawInput =
    row.venueSourcingStatus ||
    row.sourcingStatus ||
    row.venueStatus ||
    "UNKNOWN";
  const venueNormalized = normalizeVenueStatus(venueRawInput);
  const terrRaw = String(row.demandTerritoryFit || row.territory || "COMPETITIVE")
    .toUpperCase()
    .replace(/[\s/-]+/g, "_");
  const roomRaw = String(row.roomDemandStatus || "ESTIMATED_ROOM_DEMAND").replace(
    /\s+/g,
    ""
  );

  const winThesis =
    row.hotelWinThesis ||
    row.hotelOpportunityThesis ||
    row.winThesis ||
    row.whyHotel ||
    null;

  const contact = row.primaryContactCandidate || row.primaryContact || null;
  const estimatedPeakRooms =
    row.estimatedPeakRooms ?? row.peakRooms ?? row.publishedPeakRooms ?? null;
  const estimatedAttendance =
    row.estimatedAttendance ?? row.attendance ?? row.publishedAttendance ?? null;

  const disqualified =
    oppTypeRaw.includes("CLOSED") ||
    terrRaw.includes("OUTSIDE") ||
    Boolean(row.disqualifyReason);

  return {
    id: row.id || `gdi_opp_${slugify(title)}_${index}`,
    hotelId,
    title,
    organizationName: row.organizationName || row.organization || null,
    segment: row.segment || row.demandSegment || "Discovered",
    demandType: row.demandType || "meeting",
    demandStatus: disqualified ? DEMAND_STATUS.DISQUALIFIED : DEMAND_STATUS.CONFIRMED_DEMAND,
    opportunityType: OPP_TYPE_MAP[oppTypeRaw] || OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
    venueSourcingStatus:
      venueNormalized ||
      VENUE_MAP[String(venueRawInput).toUpperCase().replace(/[\s/-]+/g, "_")] ||
      VENUE_SOURCING_STATUS.UNKNOWN,
    venueStatus: row.venueStatus || row.venueSourcingStatus || null,
    destinationStatus: row.destinationStatus || row.location || row.city || null,
    eventStartDate: row.eventStartDate || row.startDate || null,
    eventEndDate: row.eventEndDate || row.endDate || null,
    eventDateClaimKind: row.eventStartDate ? CLAIM_KIND.FACT : CLAIM_KIND.UNKNOWN,
    estimatedPeakRooms,
    estimatedPeakRoomsClaimKind: estimatedPeakRooms != null ? CLAIM_KIND.ESTIMATED : CLAIM_KIND.UNKNOWN,
    estimatedAttendance,
    estimatedAttendanceClaimKind:
      estimatedAttendance != null ? CLAIM_KIND.ESTIMATED : CLAIM_KIND.UNKNOWN,
    roomDemandStatus:
      ROOM_DEMAND_MAP[roomRaw] ||
      ROOM_DEMAND_MAP[String(row.roomDemandStatus || "").toUpperCase()] ||
      ROOM_DEMAND_STATUS.ESTIMATED_ROOM_DEMAND,
    demandTerritoryFit: TERRITORY_MAP[terrRaw] || TERRITORY_CLASS.TERRITORY_COMPETITIVE,
    demandTerritoryFitLocked: Boolean(row.demandTerritoryFit || row.territory),
    demandTerritoryRationale: row.demandTerritoryRationale || row.territoryReason || null,
    hotelOpportunityThesis: winThesis,
    bethesdaWinThesis: winThesis,
    whyNow: row.whyNow || null,
    recommendedAction: row.recommendedAction || null,
    summaryWhat: row.summaryWhat || row.summary || title,
    summaryWhyMatters: row.summaryWhyMatters || row.whyNow || null,
    summaryWhyHotel: winThesis,
    bookingWindowStatus: row.bookingWindowStatus || BOOKING_WINDOW.QUALIFY_NOW,
    disqualifyReasons: row.disqualifyReason
      ? [row.disqualifyReason]
      : disqualified && terrRaw.includes("OUTSIDE")
        ? ["Outside realistic demand territory"]
        : [],
    primaryContact: contact
      ? {
          name: contact.name || contact.personName || null,
          role: contact.role || contact.title || null,
          email: contact.email || null,
          phone: contact.phone || null,
          organization: contact.organization || contact.org || null,
          sourceUrl: contact.sourceUrl || contact.url || null,
        }
      : null,
    evidence: mapEvidence(row.evidenceSources || row.sources || row.evidence || []),
    fitComponents: row.fitComponents || {
      physicalFit: row.physicalFitScore ?? 70,
      geographyFit: row.geographyFitScore ?? 70,
      timing: row.timingScore ?? 65,
      commercialValue: row.commercialValueScore ?? 65,
      historicalFit: row.historicalFitScore ?? 40,
      competitiveAccessibility: row.competitiveAccessibilityScore ?? 55,
      contactability: contact?.name ? 60 : 30,
    },
    confidenceInput: row.confidenceInput || {
      sourceAuthority: 72,
      independentSourceCount: Math.min(
        5,
        (row.evidenceSources || row.sources || []).length || 1
      ),
      directness: 65,
      recency: 72,
      verifiedFieldRatio: Math.min(
        0.7,
        0.35 + 0.1 * Math.min(4, (row.evidenceSources || row.sources || []).length || 1)
      ),
      firstPartyShare: 55,
    },
    researchProvider: "webhound",
    discoverySource: "webhound_gdi_first_run",
  };
}

/**
 * @param {object[]} rows - structured opportunity objects from Webhound output
 * @param {string} hotelId
 * @returns {{ candidates: object[], rejected: object[], parseErrors: string[] }}
 */
export function mapWebhoundOpportunityUniverse(rows, hotelId) {
  const candidates = [];
  const rejected = [];
  const parseErrors = [];

  for (let i = 0; i < (rows || []).length; i += 1) {
    try {
      const mapped = mapWebhoundOpportunityRow(rows[i], hotelId, i);
      if (!mapped) {
        parseErrors.push(`row_${i}_missing_title`);
        continue;
      }
      if (
        mapped.opportunityType === OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED ||
        (mapped.disqualifyReasons || []).length
      ) {
        rejected.push(mapped);
      }
      candidates.push(mapped);
    } catch (err) {
      parseErrors.push(`row_${i}:${err.message}`);
    }
  }

  return { candidates, rejected, parseErrors };
}

/**
 * Extract JSON-ish opportunity arrays from a Webhound markdown/report string.
 * Best-effort — prefers fenced ```json blocks containing opportunity arrays.
 */
export function extractOpportunityRowsFromWebhoundText(text) {
  const out = [];
  const src = String(text || "");

  const fenceRe = /```(?:json)?\s*([\s\S]*?)```/gi;
  let m;
  while ((m = fenceRe.exec(src))) {
    const body = m[1].trim();
    try {
      const parsed = JSON.parse(body);
      if (Array.isArray(parsed)) {
        out.push(...parsed);
      } else if (Array.isArray(parsed?.opportunities)) {
        out.push(...parsed.opportunities);
      } else if (Array.isArray(parsed?.qualified)) {
        out.push(...parsed.qualified);
        if (Array.isArray(parsed.rejected)) {
          for (const r of parsed.rejected) {
            out.push({
              ...r,
              opportunityType: "CLOSED_DISQUALIFIED",
              disqualifyReason: r.disqualifyReason || r.reason || r.reasonRejected,
            });
          }
        }
      }
    } catch {
      // ignore non-JSON fences
    }
  }

  return out;
}
