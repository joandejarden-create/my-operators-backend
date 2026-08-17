/**
 * Design Hotels census content backfill — field contract and fill-blank patches.
 */

import { CENSUS_FIELDS } from "./fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../hilton-amenity-map.js";
import { CENSUS_DESCRIPTION_FIELD } from "./hilton-description-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "./hilton-property-id-contract.js";
import { isBlankCensusValue, buildEnrichmentFieldActions } from "./brand-directory-enrichment-contract.js";
import {
  formatMarriottAmenitiesText,
  parseMarriottAmenitiesText,
} from "../marriott-amenity-format.js";

export const MAP_DESIGN_HOTELS_CENSUS_CONTENT = {
  hotelDescription: CENSUS_DESCRIPTION_FIELD,
  amenities: CENSUS_AMENITIES_TEXT_FIELD,
  rooms: CENSUS_FIELDS.rooms,
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  restaurantYn: "Restaurant (Y/N)",
  spaYn: "Spa (Y/N)",
  golfYn: "Golf (Y/N)",
  conferenceYn: "Conference (Y/N)",
  resortYn: "Resort (Y/N)",
  boutiqueYn: "Boutique (Y/N)",
};

/** @type {{ field: string, patterns: RegExp[] }[]} */
export const DESIGN_HOTELS_AMENITY_YN_RULES = [
  {
    field: MAP_DESIGN_HOTELS_CENSUS_CONTENT.restaurantYn,
    patterns: [/restaurant/i, /\bdining\b/i, /\bbistro\b/i, /coffee shop/i, /on-site restaurant/i],
  },
  {
    field: MAP_DESIGN_HOTELS_CENSUS_CONTENT.spaYn,
    patterns: [/\bspa\b/i, /hammam/i, /sauna/i, /wellness center/i, /infrared sauna/i],
  },
  { field: MAP_DESIGN_HOTELS_CENSUS_CONTENT.golfYn, patterns: [/\bgolf\b/i] },
  {
    field: MAP_DESIGN_HOTELS_CENSUS_CONTENT.conferenceYn,
    patterns: [/meeting room/i, /conference room/i, /business center/i, /catering/i],
  },
  { field: MAP_DESIGN_HOTELS_CENSUS_CONTENT.resortYn, patterns: [/\bresort\b/i] },
  { field: MAP_DESIGN_HOTELS_CENSUS_CONTENT.boutiqueYn, patterns: [/\bboutique\b/i] },
];

/**
 * @param {string[]} amenities
 * @param {string} amenitiesText
 */
export function inferDesignHotelsAmenityYnFlags(amenities, amenitiesText) {
  const blob = `${(amenities || []).join("; ")}; ${amenitiesText || ""}`.toLowerCase();
  /** @type {Record<string, "Y">} */
  const flags = {};
  for (const rule of DESIGN_HOTELS_AMENITY_YN_RULES) {
    if (rule.patterns.some((re) => re.test(blob))) flags[rule.field] = "Y";
  }
  return flags;
}

/**
 * @param {import('airtable').FieldSet} censusFields
 */
export function designHotelsRowNeedsContent(censusFields) {
  const F = MAP_DESIGN_HOTELS_CENSUS_CONTENT;
  return (
    isBlankCensusValue(censusFields?.[F.hotelDescription]) ||
    isBlankCensusValue(censusFields?.[F.amenities]) ||
    isBlankCensusValue(censusFields?.[F.rooms]) ||
    isBlankCensusValue(censusFields?.[F.restaurantYn]) ||
    isBlankCensusValue(censusFields?.[F.spaYn])
  );
}

/**
 * @param {import('airtable').FieldSet} censusFields
 * @param {ReturnType<import("../design-hotels-hotel-content-fetch.js").parseDesignHotelsOverviewHtml>} content
 * @param {string[]} presentFields
 */
export function buildDesignHotelsContentPatch(censusFields, content, presentFields) {
  const F = MAP_DESIGN_HOTELS_CENSUS_CONTENT;
  /** @type {Record<string, unknown>} */
  const proposed = {};

  if (content.description) proposed[F.hotelDescription] = content.description;
  if (content.amenitiesText) proposed[F.amenities] = content.amenitiesText;
  if (Number.isFinite(content.rooms) && content.rooms > 0) proposed[F.rooms] = content.rooms;
  if (content.marshaCode) proposed[F.propertyId] = content.marshaCode;

  const yn = inferDesignHotelsAmenityYnFlags(content.amenities, content.amenitiesText);
  for (const [field, val] of Object.entries(yn)) {
    proposed[field] = val;
  }

  const present = new Set(presentFields);
  const filtered = {};
  for (const [k, v] of Object.entries(proposed)) {
    if (!present.has(k)) continue;
    filtered[k] = v;
  }

  const { applyFields } = buildEnrichmentFieldActions(censusFields, filtered, {
    fillBlankOnly: true,
    includeBrandPropertyCode: false,
  });
  return applyFields;
}

/**
 * Merge existing census amenities with Marriott subpage chips (deduped, preserve order).
 * @param {string} existingText
 * @param {string[]} marriottAmenities
 */
export function mergeDesignHotelsAmenitiesText(existingText, marriottAmenities) {
  const existing = parseMarriottAmenitiesText(existingText);
  const merged = formatMarriottAmenitiesText([...existing, ...(marriottAmenities || [])]);
  return { merged, addedCount: Math.max(0, parseMarriottAmenitiesText(merged).length - existing.length) };
}

/**
 * @param {import('airtable').FieldSet} censusFields
 * @param {{ amenities?: string[], amenitiesText?: string, description?: string }} marriottContent
 * @param {string[]} presentFields
 * @param {{ refreshAmenities?: boolean }} [opts]
 */
export function buildDesignHotelsMarriottAmenitiesPatch(
  censusFields,
  marriottContent,
  presentFields,
  opts = {}
) {
  const F = MAP_DESIGN_HOTELS_CENSUS_CONTENT;
  /** @type {Record<string, unknown>} */
  const proposed = {};

  const marriottLabels = marriottContent.amenities?.length
    ? marriottContent.amenities
    : parseMarriottAmenitiesText(marriottContent.amenitiesText || "");

  if (marriottLabels.length) {
    const existing = String(censusFields?.[F.amenities] || "").trim();
    if (opts.refreshAmenities || isBlankCensusValue(existing)) {
      proposed[F.amenities] = formatMarriottAmenitiesText(marriottLabels);
    } else {
      const { merged, addedCount } = mergeDesignHotelsAmenitiesText(existing, marriottLabels);
      if (addedCount > 0) proposed[F.amenities] = merged;
    }
  }

  if (marriottContent.description && isBlankCensusValue(censusFields?.[F.hotelDescription])) {
    proposed[F.hotelDescription] = marriottContent.description;
  }

  const amenitiesForYn = parseMarriottAmenitiesText(
    String(proposed[F.amenities] || censusFields?.[F.amenities] || marriottContent.amenitiesText || "")
  );
  const yn = inferDesignHotelsAmenityYnFlags(amenitiesForYn, amenitiesForYn.join("; "));
  for (const [field, val] of Object.entries(yn)) {
    proposed[field] = val;
  }

  const present = new Set(presentFields);
  const filtered = {};
  for (const [k, v] of Object.entries(proposed)) {
    if (!present.has(k)) continue;
    filtered[k] = v;
  }

  /** @type {Record<string, unknown>} */
  const applyFields = {};
  const amenityPatch = {};
  const ynPatch = {};
  for (const [k, v] of Object.entries(filtered)) {
    if (k === F.amenities || k === F.hotelDescription) amenityPatch[k] = v;
    else ynPatch[k] = v;
  }

  if (Object.keys(amenityPatch).length) {
    const { applyFields: amenApply } = buildEnrichmentFieldActions(censusFields, amenityPatch, {
      fillBlankOnly: !(opts.refreshAmenities || Boolean(amenityPatch[F.amenities])),
      includeBrandPropertyCode: false,
    });
    Object.assign(applyFields, amenApply);
  }

  if (Object.keys(ynPatch).length) {
    const { applyFields: ynApply } = buildEnrichmentFieldActions(censusFields, ynPatch, {
      fillBlankOnly: true,
      includeBrandPropertyCode: false,
    });
    Object.assign(applyFields, ynApply);
  }

  return applyFields;
}
