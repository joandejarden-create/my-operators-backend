/**
 * Generate Census hotel descriptions from existing fields only.
 * Never invent amenities, phone, rooms, or location claims.
 */

import { isIncorrectCanonicalPropertyName } from "./universal-hotel-record-inspector.js";
import {
  evaluateDescriptionQuality,
} from "./census-description-quality-gate.js";

export const HOTEL_DESCRIPTION_GENERATOR_VERSION =
  "census-hotel-description-generator-v1";

/** Schema mapping — confirmed Hotel Property Census fields. */
export const DESCRIPTION_FIELD_MAP = Object.freeze({
  /** Official extracted narrative (do not invent into this field). */
  sourceText: "Hotel Description - Source Text",
  /** Governed generated summary from Census fields (owner-facing style). */
  aiSummary: "Hotel Description - AI Summary",
});

/** Schema gaps vs founder request (do not invent Airtable fields). */
export const DESCRIPTION_SCHEMA_GAPS = Object.freeze([
  "Public Description",
  "Internal Description",
  "Description Status",
  "Description Source Type",
  "Description Reviewed Date",
  "Description Notes",
]);

export const DESCRIPTION_STATUS = Object.freeze({
  GENERATED: "generated_from_census_fields",
  HELD_DIRTY: "held_dirty_identity",
  HELD_MISSING: "held_missing_core_fields",
  STEWARD: "steward_review_required",
});

function isBlank(v) {
  return v == null || !String(v).trim();
}

function cleanName(fields) {
  return String(
    fields["Canonical Property Name"] || fields["Property Name"] || ""
  ).trim();
}

function isDirtyPartnerBrand(brand) {
  return /^(choice hotels|partner\s*\/\s*spnd|unconfirmed|unknown|sam)$/i.test(
    String(brand || "").trim()
  );
}

function locationClaimSupported(fields) {
  const hay = [
    fields.Address,
    fields.Submarket,
    fields.Market,
    fields["Canonical Property Name"],
    fields["Property Name"],
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return /zona hotelera|hotel zone|aeropuerto|airport|centro hist[oó]rico|casco antiguo|financial district|piantini|naco|colonial/i.test(
    hay
  );
}

/**
 * Eligibility for description generation.
 */
export function evaluateDescriptionEligibility(fields = {}) {
  const name = cleanName(fields);
  const city = String(fields.City || "").trim();
  const country = String(fields.Country || "").trim();
  const brand = String(fields["Current Brand"] || "").trim();
  const propertyType = String(fields["Property Type"] || "").trim();
  const nameCheck = isIncorrectCanonicalPropertyName(fields);

  if (!name || nameCheck.incorrect) {
    return {
      ok: false,
      status: DESCRIPTION_STATUS.HELD_DIRTY,
      reason: nameCheck.reason || "missing_property_name",
    };
  }
  if (!city || !country) {
    return {
      ok: false,
      status: DESCRIPTION_STATUS.HELD_MISSING,
      reason: "missing_city_or_country",
    };
  }
  if (!brand && !propertyType) {
    return {
      ok: false,
      status: DESCRIPTION_STATUS.HELD_MISSING,
      reason: "missing_brand_and_property_type",
    };
  }
  if (isDirtyPartnerBrand(brand)) {
    return {
      ok: false,
      status: DESCRIPTION_STATUS.STEWARD,
      reason: "dirty_partner_brand",
      allow_internal_only: true,
    };
  }
  return { ok: true, status: DESCRIPTION_STATUS.GENERATED, reason: null };
}

/**
 * Build public + internal description strings from Census fields.
 */
export function generateHotelDescriptions(fields = {}) {
  const eligibility = evaluateDescriptionEligibility(fields);
  const name = cleanName(fields);
  const brand = String(fields["Current Brand"] || "").trim();
  const family = String(fields["Brand Family"] || "").trim();
  const city = String(fields.City || "").trim();
  const country = String(fields.Country || "").trim();
  const market = String(fields.Market || "").trim();
  const submarket = String(fields.Submarket || "").trim();
  const rooms = fields["Rooms / Keys"];
  const roomsOk =
    rooms != null &&
    rooms !== "" &&
    Number.isFinite(Number(rooms)) &&
    Number(rooms) > 0 &&
    String(fields["Rooms Confidence"] || "").toLowerCase() === "high";
  const governance = String(fields["Brand Governance Status"] || "").trim();
  const censusOnly = /census only|not owner/i.test(governance);

  if (!eligibility.ok && !eligibility.allow_internal_only) {
    return {
      ok: false,
      eligibility,
      public_description: null,
      internal_description: null,
      schema_gaps: DESCRIPTION_SCHEMA_GAPS,
      field_map: DESCRIPTION_FIELD_MAP,
    };
  }

  let publicText = "";
  let internalText = "";

  if (eligibility.allow_internal_only) {
    internalText = `${name} is a hotel record in ${city}, ${country} included in the Dealality Census. Brand identity requires steward review before owner-facing copy.`;
    publicText = null;
  } else if (censusOnly) {
    publicText = `${name} is a hotel in ${city}, ${country}${
      market ? `, within the ${market} market` : ""
    }.`;
    internalText = `${name} is an evidence-backed hotel in ${city}, ${country}${
      market ? `, within the ${market} market` : ""
    }. The property is included in the Dealality Census as Census Only / Not Owner-Facing pending brand governance review.`;
  } else if (roomsOk && brand && city && country) {
    publicText = `${name} is a ${Number(rooms)}-key ${brand} hotel in ${city}, ${country}${
      market ? `, within the ${market} market` : ""
    }.`;
    internalText = `${name} is a ${Number(rooms)}-key ${brand} hotel in ${city}, ${country}${
      market ? `, within the ${market} market` : ""
    }. The property is part of ${
      family || brand
    }'s official hotel inventory and is tracked with source-supported room count evidence.`;
  } else if (submarket && market && brand) {
    publicText = `${name} is a ${brand} hotel in ${submarket}, within the ${market} market.`;
    internalText = `${name} is a ${brand} hotel located in ${submarket}, within the ${market} market. The property is part of ${
      family || brand
    }'s official hotel inventory and has source-supported address information in the Dealality Census.`;
  } else if (brand && city && country) {
    publicText = `${name} is a ${brand} hotel in ${city}, ${country}${
      market ? `, within the ${market} market` : ""
    }.`;
    internalText = `${name} is a ${brand} hotel in ${city}, ${country}${
      market ? `, within the ${market} market` : ""
    }. The property is part of ${
      family || brand
    }'s official hotel inventory and is tracked in the Dealality Census for market, brand, and owner-decision analysis.`;
  } else {
    publicText = `${name} is a hotel in ${city}, ${country}.`;
    internalText = `${name} is a hotel record in ${city}, ${country} included in the Dealality Census based on official source evidence. Additional operating details remain under source review.`;
  }

  const locOk = locationClaimSupported(fields);
  const publicGate = publicText
    ? evaluateDescriptionQuality(publicText, {
        allowInternalTerms: false,
        locationSupported: locOk,
      })
    : { ok: true, failures: [] };
  const internalGate = evaluateDescriptionQuality(internalText, {
    allowInternalTerms: true,
    locationSupported: locOk,
  });

  if (publicText && !publicGate.ok) {
    return {
      ok: false,
      eligibility: {
        ...eligibility,
        status: DESCRIPTION_STATUS.STEWARD,
        reason: `public_quality_gate:${publicGate.failures.join(",")}`,
      },
      public_description: null,
      internal_description: internalGate.ok ? internalText : null,
      public_gate: publicGate,
      internal_gate: internalGate,
      schema_gaps: DESCRIPTION_SCHEMA_GAPS,
      field_map: DESCRIPTION_FIELD_MAP,
    };
  }

  return {
    ok: Boolean(publicText || internalText),
    eligibility,
    public_description: publicText,
    internal_description: internalText,
    public_gate: publicGate,
    internal_gate: internalGate,
    schema_gaps: DESCRIPTION_SCHEMA_GAPS,
    field_map: DESCRIPTION_FIELD_MAP,
    write_fields: {
      // Public-style text → AI Summary (Source Text reserved for official extracted copy)
      [DESCRIPTION_FIELD_MAP.aiSummary]: publicText || undefined,
    },
  };
}
