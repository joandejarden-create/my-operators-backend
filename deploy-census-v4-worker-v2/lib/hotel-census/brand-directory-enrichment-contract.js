/**
 * Cross-brand Hotel Census directory-tier enrichment contract.
 * Hilton pilot — maps reliable directory fields to existing census columns.
 */

import {
  CENSUS_FIELDS,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "./fields.js";
import {
  CENSUS_AMENITIES_TEXT_FIELD,
  CENSUS_AMENITY_YN_COLUMNS,
  directoryAmenityIdsToCensusFields,
  summarizeAmenityYnFlags,
} from "../hilton-amenity-map.js";

export { summarizeAmenityYnFlags as summarizeAmenityFlags };

export const ENRICHMENT_SOURCE_HILTON_DIRECTORY = "hilton_locations_directory";

/** Fields written by directory enrichment (existing census columns). */
export const MAP_DIRECTORY_ENRICHMENT = {
  name: CENSUS_FIELDS.name,
  affiliation: CENSUS_FIELDS.affiliation,
  parentCompany: CENSUS_FIELDS.parentCompany,
  status: CENSUS_FIELDS.status,
  openDate: "Open Date",
  projectedOpenDate: "projected_open_date",
  lat: "Latitude",
  lng: "Longitude",
  city: CENSUS_FIELDS.city,
  country: CENSUS_FIELDS.country,
  state: "State",
  address1: "Address 1",
  postalCode: "Postal Code",
  telephone: "Telephone",
  website: "Website",
  dataConfidence: CENSUS_FIELDS.dataConfidence,
  brandPropertyCode:
    process.env.AIRTABLE_CENSUS_BRAND_PROPERTY_CODE_FIELD || "Brand Property Code",
  amenities: CENSUS_AMENITIES_TEXT_FIELD,
};

export const DIRECTORY_TIER_LABELS = {
  identity: ["name", "affiliation", "parentCompany", "brandPropertyCode"],
  status: ["status", "openDate", "projectedOpenDate"],
  geo: ["lat", "lng", "city", "country", "state", "address1", "postalCode"],
  contact: ["telephone", "website"],
  amenities: ["amenities", ...CENSUS_AMENITY_YN_COLUMNS],
  governance: ["dataConfidence"],
};

export function isBlankCensusValue(value) {
  if (value == null) return true;
  if (typeof value === "string") return value.trim() === "";
  if (typeof value === "number" && !Number.isFinite(value)) return true;
  return false;
}

function finiteCoord(n) {
  const v = Number(n);
  return Number.isFinite(v) ? v : null;
}

/**
 * @param {object} row normalized directory row (Hilton or future sources)
 * @param {{ affiliation?: string, parentCompany?: string }} [defaults]
 */
export function directoryRowToCensusFields(row, defaults = {}) {
  const fields = {};
  const open = row.status === STATUS_OPEN;
  const openDate = row.openDate || null;

  if (defaults.affiliation) fields[MAP_DIRECTORY_ENRICHMENT.affiliation] = defaults.affiliation;
  if (defaults.parentCompany) fields[MAP_DIRECTORY_ENRICHMENT.parentCompany] = defaults.parentCompany;

  if (row.status) fields[MAP_DIRECTORY_ENRICHMENT.status] = row.status;

  if (open && openDate) {
    fields[MAP_DIRECTORY_ENRICHMENT.openDate] = openDate;
  } else if (!open && openDate) {
    fields[MAP_DIRECTORY_ENRICHMENT.projectedOpenDate] = openDate;
  }

  const lat = finiteCoord(row.latitude);
  const lng = finiteCoord(row.longitude);
  if (lat != null) fields[MAP_DIRECTORY_ENRICHMENT.lat] = lat;
  if (lng != null) fields[MAP_DIRECTORY_ENRICHMENT.lng] = lng;

  if (row.city) fields[MAP_DIRECTORY_ENRICHMENT.city] = row.city;
  if (row.country) fields[MAP_DIRECTORY_ENRICHMENT.country] = row.country;
  if (row.state) fields[MAP_DIRECTORY_ENRICHMENT.state] = row.state;
  if (row.addressLine1) fields[MAP_DIRECTORY_ENRICHMENT.address1] = row.addressLine1;
  if (row.postalCode) fields[MAP_DIRECTORY_ENRICHMENT.postalCode] = row.postalCode;
  if (row.phone) fields[MAP_DIRECTORY_ENRICHMENT.telephone] = row.phone;
  if (row.website) fields[MAP_DIRECTORY_ENRICHMENT.website] = row.website;
  if (row.brandPropertyCode) {
    fields[MAP_DIRECTORY_ENRICHMENT.brandPropertyCode] = row.brandPropertyCode;
  }

  if (Array.isArray(row.amenityIds) && row.amenityIds.length) {
    Object.assign(fields, directoryAmenityIdsToCensusFields(row.amenityIds));
  }

  return fields;
}

/**
 * @param {Record<string, unknown>} currentFields census row fields
 * @param {Record<string, unknown>} proposed Airtable patch
 * @param {{ fillBlankOnly?: boolean, force?: boolean, includeBrandPropertyCode?: boolean }} [opts]
 */
const PROTECTED_AIRTABLE_FIELDS = new Set([
  MAP_DIRECTORY_ENRICHMENT.name,
  MAP_DIRECTORY_ENRICHMENT.affiliation,
  MAP_DIRECTORY_ENRICHMENT.parentCompany,
]);

export function buildEnrichmentFieldActions(currentFields, proposed, opts = {}) {
  const { fillBlankOnly = true, force = false, includeBrandPropertyCode = false } = opts;
  const F = MAP_DIRECTORY_ENRICHMENT;
  const actions = [];
  const applyFields = {};

  const proposedEntries = Object.entries(proposed).filter(([airtableField]) => {
    if (airtableField === F.brandPropertyCode && !includeBrandPropertyCode) return false;
    return true;
  });

  for (const [airtableField, proposedValue] of proposedEntries) {
    const logicalKey =
      Object.entries(F).find(([, col]) => col === airtableField)?.[0] ||
      (CENSUS_AMENITY_YN_COLUMNS.includes(airtableField) ? `yn:${airtableField}` : airtableField);
    const currentValue = currentFields?.[airtableField];
    const currentBlank = isBlankCensusValue(currentValue);
    const proposedBlank = isBlankCensusValue(proposedValue);

    if (proposedBlank) {
      actions.push({
        logicalKey,
        airtableField,
        action: "skip_empty_proposed",
        current: currentValue ?? "",
        proposed: proposedValue ?? "",
      });
      continue;
    }

    const same =
      String(currentValue ?? "").trim() === String(proposedValue ?? "").trim() ||
      (typeof currentValue === "number" &&
        typeof proposedValue === "number" &&
        currentValue === proposedValue);

    if (same) {
      actions.push({
        logicalKey,
        airtableField,
        action: "skip_unchanged",
        current: currentValue ?? "",
        proposed: proposedValue ?? "",
      });
      continue;
    }

    if (!currentBlank && fillBlankOnly && !force) {
      actions.push({
        logicalKey,
        airtableField,
        action: PROTECTED_AIRTABLE_FIELDS.has(airtableField) ? "skip_protected" : "skip_has_value",
        current: currentValue ?? "",
        proposed: proposedValue ?? "",
      });
      continue;
    }

    if (PROTECTED_AIRTABLE_FIELDS.has(airtableField) && !currentBlank && !force) {
      actions.push({
        logicalKey,
        airtableField,
        action: "skip_protected",
        current: currentValue ?? "",
        proposed: proposedValue ?? "",
      });
      continue;
    }

    applyFields[airtableField] = proposedValue;
    actions.push({
      logicalKey,
      airtableField,
      action: currentBlank ? "fill_blank" : "overwrite",
      current: currentValue ?? "",
      proposed: proposedValue ?? "",
    });
  }

  const hasWritable = Object.keys(applyFields).length > 0;
  if (hasWritable && F.dataConfidence in MAP_DIRECTORY_ENRICHMENT) {
    const dcField = MAP_DIRECTORY_ENRICHMENT.dataConfidence;
    const currentDc = currentFields?.[dcField];
    if (isBlankCensusValue(currentDc) || force) {
      applyFields[dcField] = "Medium";
      actions.push({
        logicalKey: "dataConfidence",
        airtableField: dcField,
        action: isBlankCensusValue(currentDc) ? "fill_blank" : "overwrite",
        current: currentDc ?? "",
        proposed: "Medium",
      });
    }
  }

  return { actions, applyFields, hasWritable };
}

/**
 * @param {object} planRow
 */
export function validateEnrichmentPlanRow(planRow) {
  const errors = [];
  if (!planRow?.censusRecordId) errors.push("missing censusRecordId");
  if (!planRow?.directoryBrandPropertyCode) errors.push("missing directoryBrandPropertyCode");
  if (!planRow?.matchConfidence || planRow.matchConfidence === "none") {
    errors.push("match confidence too low for apply");
  }
  if (!planRow?.applyFields || !Object.keys(planRow.applyFields).length) {
    errors.push("no applyFields");
  }
  return { pass: errors.length === 0, errors };
}
