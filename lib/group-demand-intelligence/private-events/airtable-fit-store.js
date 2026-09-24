/**
 * Hotel Venue Fit — hotel-specific relationship to global Private Event Venues.
 * Fail closed if hotelId cannot resolve to a canonical hotel id.
 */

import { createHash } from "node:crypto";
import {
  HOTEL_VENUE_FIT_TABLE_NAME,
  MAP_HOTEL_VENUE_FIT as F,
  MAP_PE_VENUE,
  PE_VENUES_TABLE_NAME,
  VAL_LODGING_CATCHMENT_FIT,
  VAL_PRODUCT_FIT,
  VAL_PARTNERSHIP_POTENTIAL,
  VAL_LODGING_CAPTURE_POTENTIAL,
} from "./airtable-field-map.js";
import {
  getPeBase,
  isPeAirtableConfigured,
  omitEmpty,
  asSelect,
  jsonText,
  nowIso,
  schemaVersion,
  findByField,
  createOrUpdate,
} from "./airtable-client.js";
import {
  buildHotelContext,
  buildHotelVenueRelationship,
} from "./hotel-context.js";
import { classifyVenuePriority } from "./venue-priority.js";

function fitTable() {
  return getPeBase()(HOTEL_VENUE_FIT_TABLE_NAME);
}

export function computeFitId(hotelId, venueId) {
  const seed = `${String(hotelId || "").trim()}|${String(venueId || "").trim()}`;
  const hash = createHash("sha256").update(seed).digest("hex").slice(0, 14);
  return `hvf_${hash}`;
}

function mapPartnershipPotential(rel, priority) {
  if (rel.potentialPartnershipFit === "HIGH" || priority === "HIGH_POTENTIAL_PARTNER") {
    return "HIGH";
  }
  if (rel.potentialPartnershipFit === "MODERATE") return "MEDIUM";
  if (rel.potentialPartnershipFit === "LOW") return "LOW";
  return "UNKNOWN";
}

function mapLodgingCapture(rel, venue) {
  if (rel.lodgingCatchmentFit === "OUTSIDE") return "LOW";
  if (venue?.onSiteLodgingStatus === "ADEQUATE_LODGING") return "LOW";
  if (
    (venue?.onSiteLodgingStatus === "NO_LODGING" ||
      venue?.onSiteLodgingStatus === "LIMITED_LODGING") &&
    (rel.lodgingCatchmentFit === "CORE" || rel.lodgingCatchmentFit === "COMPETITIVE")
  ) {
    return "HIGH";
  }
  if (rel.lodgingCatchmentFit === "STRETCH") return "MODERATE";
  return "UNKNOWN";
}

/**
 * Validate hotel id is present and looks like a canonical census / GDI hotel id.
 * Fail closed — no orphan fits.
 */
export function assertCanonicalHotelId(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) {
    const err = new Error("hotel_id_unresolved");
    err.code = "hotel_id_unresolved";
    throw err;
  }
  // Accept Airtable rec… census IDs or gdi_hotel_* synthetic canaries
  if (!/^(rec[a-zA-Z0-9]{14}|gdi_hotel_[a-z0-9_]+|hotel_[a-z0-9_]+)$/.test(id)) {
    const err = new Error(`hotel_id_unresolved:${id}`);
    err.code = "hotel_id_unresolved";
    throw err;
  }
  return id;
}

export function fitToAirtableFields({
  hotelCtx,
  venue,
  relationship,
  priorityResult,
  meta = {},
}) {
  const hotelId = assertCanonicalHotelId(hotelCtx.hotelId || meta.hotelId);
  const venueId = String(venue.venueId || "").trim();
  if (!venueId) {
    const err = new Error("venue_id_required_for_fit");
    err.code = "venue_id_required_for_fit";
    throw err;
  }
  const fitId = meta.fitId || computeFitId(hotelId, venueId);
  const pri = priorityResult || classifyVenuePriority(venue, relationship);

  return omitEmpty({
    [F.fitId]: fitId,
    [F.hotelId]: hotelId,
    [F.hotelName]: hotelCtx.displayName || meta.hotelName || null,
    [F.venueId]: venueId,
    [F.venueName]: venue.venueName || null,
    [F.distanceMiles]: relationship.distanceMiles,
    [F.distanceKm]: relationship.distanceKm,
    [F.driveTimeMinutes]: relationship.driveTimeMinutes,
    [F.lodgingCatchmentFit]: asSelect(
      relationship.lodgingCatchmentFit,
      VAL_LODGING_CATCHMENT_FIT
    ),
    [F.productFit]: asSelect(relationship.productFit, VAL_PRODUCT_FIT),
    [F.partnershipPotential]: asSelect(
      mapPartnershipPotential(relationship, pri.priority),
      VAL_PARTNERSHIP_POTENTIAL
    ),
    [F.lodgingCapturePotential]: asSelect(
      mapLodgingCapture(relationship, venue),
      VAL_LODGING_CAPTURE_POTENTIAL
    ),
    [F.existingHotelRelationshipStatus]:
      relationship.existingRelationshipStatus || "NONE_KNOWN",
    [F.preferredPartnerStatus]: venue.preferredHotelListed
      ? "PREFERRED_LISTED"
      : "NONE_KNOWN",
    [F.fitRationale]: (pri.reasons || []).join("; ") || null,
    [F.whyHotelCouldWin]: meta.whyHotelCouldWin || null,
    [F.constraints]: meta.constraints || null,
    [F.currentGdiOpportunityId]: meta.currentGdiOpportunityId || null,
    [F.currentGdiOpportunityStatus]: meta.currentGdiOpportunityStatus || null,
    [F.firstEvaluated]: meta.firstEvaluated || nowIso(),
    [F.lastEvaluated]: nowIso(),
    [F.lastChanged]: nowIso(),
    [F.evidenceUrls]: (venue.sourceUrls || []).join("\n") || undefined,
    [F.confidence]: meta.confidence != null ? Number(meta.confidence) : undefined,
    [F.schemaVersion]: schemaVersion(),
    [F.fitPayloadJson]: jsonText({
      fitId,
      hotelId,
      venueId,
      relationship,
      priority: pri.priority,
      factors: pri.factors,
    }),
  });
}

export function airtableRecordToFit(rec) {
  if (!rec) return null;
  const f = rec.fields || {};
  return {
    airtableRecordId: rec.id,
    fitId: f[F.fitId],
    hotelId: f[F.hotelId],
    hotelName: f[F.hotelName],
    venueId: f[F.venueId],
    venueName: f[F.venueName],
    distanceMiles: f[F.distanceMiles],
    lodgingCatchmentFit: f[F.lodgingCatchmentFit],
    productFit: f[F.productFit],
    partnershipPotential: f[F.partnershipPotential],
    lodgingCapturePotential: f[F.lodgingCapturePotential],
    currentGdiOpportunityId: f[F.currentGdiOpportunityId],
  };
}

export async function upsertHotelVenueFit({
  hotel,
  venue,
  dryRun = true,
  meta = {},
} = {}) {
  const hotelCtx = buildHotelContext(hotel);
  assertCanonicalHotelId(hotelCtx.hotelId);

  if (!venue?.venueId) {
    return { ok: false, error: "venue_id_required", orphanPrevented: true };
  }

  const relationship = buildHotelVenueRelationship(hotelCtx, venue);
  const priorityResult = classifyVenuePriority(venue, relationship);
  const fitId = computeFitId(hotelCtx.hotelId, venue.venueId);

  let prior = null;
  if (isPeAirtableConfigured() && !dryRun) {
    const hits = await findByField(fitTable(), F.fitId, fitId, 1);
    if (hits[0]) prior = airtableRecordToFit(hits[0]);
  }

  const fields = fitToAirtableFields({
    hotelCtx,
    venue,
    relationship,
    priorityResult,
    meta: {
      ...meta,
      fitId,
      firstEvaluated: prior?.firstEvaluated || nowIso(),
    },
  });

  // Always populate Airtable linked-record Venue when resolvable
  // (text Venue ID alone is not graph-visible in the Airtable UI).
  if (!dryRun && isPeAirtableConfigured()) {
    let venueRecId = meta.venueAirtableRecordId || null;
    if (!venueRecId && venue.venueId) {
      const hits = await findByField(
        getPeBase()(PE_VENUES_TABLE_NAME),
        MAP_PE_VENUE.venueId,
        venue.venueId,
        1
      );
      venueRecId = hits[0]?.id || null;
    }
    if (venueRecId) {
      fields[F.venueLink] = [venueRecId];
    }
  }

  const result = await createOrUpdate(dryRun ? null : fitTable(), {
    recordId: prior?.airtableRecordId || null,
    fields,
    dryRun,
  });

  return {
    ok: true,
    fitId,
    hotelId: hotelCtx.hotelId,
    venueId: venue.venueId,
    lodgingCatchmentFit: relationship.lodgingCatchmentFit,
    partnershipPotential: fields[F.partnershipPotential],
    fields,
    priority: priorityResult.priority,
    ...result,
  };
}

export { HOTEL_VENUE_FIT_TABLE_NAME, F as HOTEL_VENUE_FIT_FIELDS };
