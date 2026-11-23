/**
 * GDI Competitive Intelligence Integration V1 —
 * Map verified research dispositions onto existing canonical competitor fields.
 *
 * Does NOT rebuild Market Scope, UI, or CMI classifier.
 * Does NOT invent competitor commitments from event-venue mentions alone.
 * Does NOT add rows to the Bethesda 67 sales baseline.
 * Does NOT write Airtable / customer publication (planner is dry-run by default).
 */

import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  COMMERCIAL_ACTIONABILITY,
} from "./commercial-status-dimensions-v1.js";

export const GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1 =
  "gdi-research-competitor-commitment-mapping-v1";

export const RESEARCH_COMPETITOR_COMMITMENT_DISPOSITION = Object.freeze({
  STOP_PRIMARY_COMPETITOR_COMMITTED: "STOP_PRIMARY_COMPETITOR_COMMITTED",
});

/** Commitment scope for the competitor win (existing commercial vocabulary). */
export const COMPETITOR_COMMITMENT_SCOPE = Object.freeze({
  PRIMARY_ROOM_BLOCK: "PRIMARY_ROOM_BLOCK",
  PRIMARY_EVENT_VENUE_AND_HOUSING: "PRIMARY_EVENT_VENUE_AND_HOUSING",
  PRIMARY_HOST_HOTEL: "PRIMARY_HOST_HOTEL",
  PARTIAL_WITH_REMAINING_DEMAND: "PARTIAL_WITH_REMAINING_DEMAND",
});

const BOOKING_LANGUAGE_RE =
  /room block|hotel block|negotiated block|host hotel|conference hotel|contracted|published (room )?rates|official (housing|accommodations|housing provider)/i;

const COMPETITOR_HOTEL_FROM_EVIDENCE_RES = [
  /\bat\s+(Hotel\s+Washington)\b/i,
  /\bat\s+(Marriott\s+Marquis\s+Washington(?:,?\s*D\.?C\.?)?)/i,
  /(?:room block|hotel block|negotiated block|official block|housing)\s+(?:now\s+open\s+)?at\s+([A-Z][A-Za-z0-9 &.'’-]{2,80}?)(?:\s*[—(,]|\s+for\b|\s*$)/i,
  /(?:host hotel|housing hotel|official housing)\s*(?:is|=|:)?\s*([A-Z][A-Za-z0-9 &.'’-]{2,80}?)(?:\s*[—(,]|\s*$)/i,
];

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function uniq(arr) {
  return [...new Set((arr || []).map((x) => text(x)).filter(Boolean))];
}

/**
 * Event venue alone is never treated as a competitor hotel commitment.
 */
export function isEventVenueOnlyMention(research = {}, competitorHotel = "") {
  const venue = text(research.venue || research.eventVenue);
  const hotel = text(competitorHotel);
  if (!venue || !hotel) return false;
  const evidence = text(
    research.hotelSourcingDecisionEvidence ||
      research.competitorBookingClaim ||
      research.lodgingEvidence ||
      ""
  );
  if (BOOKING_LANGUAGE_RE.test(evidence)) return false;
  const venueNorm = venue.toLowerCase().replace(/[^a-z0-9]+/g, " ");
  const hotelNorm = hotel.toLowerCase().replace(/[^a-z0-9]+/g, " ");
  return venueNorm.includes(hotelNorm) || hotelNorm.includes(venueNorm);
}

/**
 * Extract a named competing hotel from verified lodging evidence — not venue alone.
 */
export function extractCompetitorHotelFromResearchEvidence(research = {}) {
  const explicit = text(
    research.competitorHotel ||
      research.competingHotel ||
      research.hostHotel ||
      research.cmiCompetitorHotel ||
      ""
  );
  if (explicit) return explicit;

  const evidence = text(
    research.hotelSourcingDecisionEvidence ||
      research.competitorBookingClaim ||
      research.lodgingEvidence ||
      ""
  );
  if (!evidence) return null;

  for (const re of COMPETITOR_HOTEL_FROM_EVIDENCE_RES) {
    const m = evidence.match(re);
    if (m && m[1]) {
      const name = text(m[1]).replace(/\s+/g, " ");
      // Reject convention-center / non-hotel venues commonly mistaken for hotels.
      if (
        /ronald reagan|convention center|trade center|palexpo|arena|stadium/i.test(
          name
        )
      ) {
        continue;
      }
      return name;
    }
  }
  return null;
}

function inferVenueStatus(evidence, competitorHotel) {
  const blob = `${text(evidence)} ${text(competitorHotel)}`;
  if (
    /room block|hotel block|negotiated block|official block|housing provider|published (room )?rates/i.test(
      blob
    )
  ) {
    return COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK;
  }
  if (/host hotel|conference hotel|event venue|all (conference )?events will take place/i.test(blob)) {
    return COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE;
  }
  return COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK;
}

function inferCommitmentScope(venueStatus, remainingDemand) {
  if (remainingDemand) return COMPETITOR_COMMITMENT_SCOPE.PARTIAL_WITH_REMAINING_DEMAND;
  if (venueStatus === COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE) {
    return COMPETITOR_COMMITMENT_SCOPE.PRIMARY_EVENT_VENUE_AND_HOUSING;
  }
  if (/host hotel/i.test(venueStatus)) {
    return COMPETITOR_COMMITMENT_SCOPE.PRIMARY_HOST_HOTEL;
  }
  return COMPETITOR_COMMITMENT_SCOPE.PRIMARY_ROOM_BLOCK;
}

function collectSourceUrls(research = {}) {
  const out = [];
  const push = (v) => {
    const s = text(v);
    if (s && /^https?:\/\//i.test(s)) out.push(s);
  };
  for (const s of research.sourcesInspected || research.sources || []) {
    if (typeof s === "string") push(s);
    else if (s && typeof s === "object") push(s.url || s.sourceUrl || s.href);
  }
  push(research.publicContactChannel?.sourceUrl);
  push(research.sourceUrl);
  push(research.primarySourceUrl);
  push(research.lodgingEvidenceUrl);
  return uniq(out);
}

function hasIndependentRemainingDemand(research = {}) {
  if (research.overflowVerified === true) return true;
  if (research.lodgingDemandForPilotHotel === true) return true;
  const sourcing = upper(
    research.targetHotelSourcingStatus || research.remainingDemandStatus || ""
  );
  return (
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED ||
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED
  );
}

/**
 * Map a research disposition row onto existing canonical competitor fields.
 *
 * @returns {{
 *   ok: boolean,
 *   skipReason?: string,
 *   disposition?: string,
 *   canonicalFields?: object,
 *   warnings?: string[],
 * }}
 */
export function mapResearchCompetitorCommitmentToCanonical(research = {}, opts = {}) {
  const disposition = upper(
    research.dispositionRecommendation ||
      research.researchDisposition ||
      research.disposition ||
      ""
  );
  const warnings = [];

  if (
    disposition !==
    RESEARCH_COMPETITOR_COMMITMENT_DISPOSITION.STOP_PRIMARY_COMPETITOR_COMMITTED
  ) {
    return {
      ok: false,
      skipReason: "DISPOSITION_NOT_COMPETITOR_COMMITTED",
      disposition: disposition || null,
      contract: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
    };
  }

  const evidence = text(
    research.hotelSourcingDecisionEvidence ||
      research.competitorBookingClaim ||
      research.lodgingEvidence ||
      ""
  );
  if (!evidence) {
    return {
      ok: false,
      skipReason: "MISSING_LODGING_DECISION_EVIDENCE",
      disposition,
      contract: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
    };
  }
  if (!BOOKING_LANGUAGE_RE.test(evidence)) {
    return {
      ok: false,
      skipReason: "NO_BOOKING_OR_HOUSING_LANGUAGE_IN_EVIDENCE",
      disposition,
      contract: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
      warnings: [
        "Event venue mentions alone are insufficient; require room/hotel/negotiated block or official housing language.",
      ],
    };
  }

  const competitorHotel = extractCompetitorHotelFromResearchEvidence(research);
  if (!competitorHotel) {
    return {
      ok: false,
      skipReason: "NAMED_COMPETITOR_HOTEL_NOT_EXTRACTABLE",
      disposition,
      contract: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
    };
  }
  if (isEventVenueOnlyMention(research, competitorHotel) && opts.allowVenueOnly !== true) {
    return {
      ok: false,
      skipReason: "EVENT_VENUE_ONLY_NOT_COMPETITOR_COMMITMENT",
      disposition,
      contract: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
    };
  }

  const urls = collectSourceUrls(research);
  if (!urls.length) {
    return {
      ok: false,
      skipReason: "MISSING_SOURCE_URL",
      disposition,
      contract: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
    };
  }

  const remainingDemand = hasIndependentRemainingDemand(research);
  const venueStatus = inferVenueStatus(evidence, competitorHotel);
  const commitmentScope = inferCommitmentScope(venueStatus, remainingDemand);
  const targetSourcing = remainingDemand
    ? TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED
    : TARGET_HOTEL_SOURCING_STATUS.SOURCING_CLOSED;

  if (research.salesReady === true) {
    warnings.push("salesReady ignored — competitor-committed research is never Sales Ready");
  }

  const eventStartDate = text(research.eventStartDate) || null;
  const eventEndDate = text(research.eventEndDate) || null;
  const title =
    text(research.eventTitle || research.title) ||
    `${text(research.organizationName)} competitor-committed event`;

  const canonicalFields = {
    id: text(research.opportunityId || research.id) || undefined,
    opportunityId: text(research.opportunityId || research.id) || undefined,
    hotelId: text(research.hotelId) || undefined,
    title,
    organizationName: text(research.organizationName) || undefined,
    eventStartDate,
    eventEndDate,
    competitorHotel,
    cmiCompetitorHotel: competitorHotel,
    competitorVenueStatus: venueStatus,
    competitorVenueConfirmed: true,
    competitorBookingEvidenceClass: "VERIFIED_BOOKING",
    competitorBookingClaim: evidence,
    lodgingEvidence: evidence,
    evidence: urls.map((url) => ({
      url,
      statement: evidence,
      accepted: true,
      kind: "COMPETITOR_COMMITMENT",
    })),
    sources: urls.map((url) => ({ url })),
    sourceUrl: urls[0],
    primarySourceUrl: urls[0],
    targetHotelSourcingStatus: targetSourcing,
    commercialActionability:
      COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
    competitorCommitmentScope: commitmentScope,
    researchDisposition: disposition,
    dispositionRecommendation: disposition,
    mayRetainAs:
      text(research.mayRetainAs) ||
      "COMPETITIVE_INTELLIGENCE_OR_OVERFLOW_WATCH_ONLY",
    // Persistence / publication boundary stamps — research inventory only.
    customerVisible: false,
    salesReady: false,
    bethesdaInternalResearchInventory: true,
    gdiInternalUnionOnly: true,
    researchCompetitorIntelligenceV1: true,
    gdiCompetitiveIntelligenceIntegrationV1: true,
    // Explicitly outside Bethesda authorized sales baseline membership.
    salesBaselineMembership: "OUTSIDE_67_BASELINE",
    doNotAddToBethesda67SalesBaseline: true,
    publicationState: "RESEARCH_ONLY",
    commercialInventoryClass: "COMPETITIVE_MARKET_INTELLIGENCE",
  };

  return {
    ok: true,
    disposition,
    canonicalFields,
    warnings,
    contract: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
    remainingDemand,
  };
}

/**
 * Non-destructive merge of mapped competitor fields onto an opportunity shell.
 * Preserves prior values under competitiveIntelligenceMappingPrior.
 */
export function applyResearchCompetitorCommitmentMapping(opp = {}, research = {}, opts = {}) {
  const mapped = mapResearchCompetitorCommitmentToCanonical(research, opts);
  if (!mapped.ok) {
    return {
      ok: false,
      opportunity: { ...opp },
      mapping: mapped,
    };
  }
  const fields = mapped.canonicalFields;
  const next = {
    ...opp,
    ...fields,
    id: text(opp.id || opp.opportunityId || fields.id),
    opportunityId: text(opp.opportunityId || opp.id || fields.opportunityId),
    hotelId: text(opp.hotelId || fields.hotelId),
    competitiveIntelligenceMappingPrior: {
      competitorHotel: opp.competitorHotel ?? null,
      competitorVenueStatus: opp.competitorVenueStatus ?? null,
      competitorBookingClaim: opp.competitorBookingClaim ?? null,
      targetHotelSourcingStatus: opp.targetHotelSourcingStatus ?? null,
      customerVisible: opp.customerVisible ?? null,
    },
    competitiveIntelligenceMappedAt:
      opts.mappedAt || new Date().toISOString(),
    competitiveIntelligenceMappingContract:
      GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
  };
  return { ok: true, opportunity: next, mapping: mapped };
}

/**
 * Non-destructive, idempotent persistence plan (dry-run by default).
 * Layers: research discovery → verified evidence → canonical mapping →
 * research inventory persistence → customer eligibility → Market Scope.
 *
 * Never writes Airtable / GDO / publication unless explicit founder gates are set
 * (still returns plan only — this module does not execute writes).
 */
export function planResearchCompetitorCommitmentPersistence(
  researchRows = [],
  opts = {}
) {
  const rows = [];
  for (const research of researchRows || []) {
    const mapped = mapResearchCompetitorCommitmentToCanonical(research, opts);
    const id = text(research.opportunityId || research.id);
    if (!mapped.ok) {
      rows.push({
        opportunityId: id,
        operation: "SKIP",
        reason: mapped.skipReason,
        layer: "canonical_field_mapping",
        airtableWrite: false,
        customerPublication: false,
        bethesda67Membership: false,
      });
      continue;
    }
    rows.push({
      opportunityId: id,
      operation: "UPSERT_RESEARCH_INVENTORY_ONLY",
      reason: "MAPPED_STOP_PRIMARY_COMPETITOR_COMMITTED",
      layer: "research_inventory_persistence",
      canonicalFields: mapped.canonicalFields,
      warnings: mapped.warnings || [],
      airtableWrite: false,
      customerPublication: false,
      bethesda67Membership: false,
      customerVisible: false,
      marketScopeExpected: mapped.remainingDemand
        ? ["SALES_OPPORTUNITIES", "COMPETITIVE_MARKET_INTELLIGENCE"]
        : ["COMPETITIVE_MARKET_INTELLIGENCE"],
      requiresSeparateApprovalFor: [
        "AIRTABLE_RESEARCH_INVENTORY_APPLY",
        "CUSTOMER_PUBLICATION",
        "BETHESDA_67_BASELINE_MEMBERSHIP",
      ],
      idempotencyKey: `cmi-map-v1:${id}:${mapped.canonicalFields.competitorHotel}`,
    });
  }

  return {
    contract: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
    mode: "DRY_RUN",
    executed: false,
    airtableWrites: 0,
    customerPublications: 0,
    bethesda67Additions: 0,
    rowCount: rows.length,
    mappedCount: rows.filter((r) => r.operation === "UPSERT_RESEARCH_INVENTORY_ONLY")
      .length,
    skippedCount: rows.filter((r) => r.operation === "SKIP").length,
    rows,
    persistenceBoundary: {
      researchDiscovery: "input research dispositions (read-only)",
      verifiedEvidence: "hotelSourcingDecisionEvidence + sourcesInspected",
      canonicalFieldMapping: GDI_RESEARCH_COMPETITOR_COMMITMENT_MAPPING_V1,
      researchInventoryPersistence:
        "planned only — Hotel Research Inventory / FS mirror; no apply in this module",
      customerOpportunityEligibility:
        "customerVisible=false; outside Bethesda 67; not Sales Ready",
      marketScopePresentation:
        "annotateInventoryPopulations after canonical fields present",
    },
    stopForFounderReview: true,
    note:
      "STOP — do not execute live persistence or publication without separate founder approval.",
  };
}
