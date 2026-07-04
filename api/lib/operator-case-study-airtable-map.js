/**
 * Operator Setup — Case Studies child table field mapping (new-base + legacy).
 * UI / API canonical keys are snake_case; Airtable column titles vary by table.
 */
import { formatListValue } from "./third-party-operator-value-utils.js";
import { normalizeCaseStudySituationForForm } from "./third-party-operator-select-prefill-normalize.js";

/** @typedef {Record<string, unknown>} AirtableFieldsRow */

export const map_operatorCaseStudyFields = {
  property_name: {
    newBase: "property_name",
    legacy: "Property Name",
  },
  hotel_type: {
    newBase: "hotel_type",
    legacy: "Hotel Type",
  },
  region: {
    newBase: "region",
    legacy: "Region",
  },
  branded_independent: {
    newBase: "branded_independent",
    legacy: "Branded / Independent",
  },
  situation: {
    newBase: "situation",
    legacy: "Situation",
  },
  challenge: {
    newBase: "challenge",
    legacy: "Challenge",
  },
  services: {
    newBase: "services",
    legacy: "Services",
  },
  outcome: {
    newBase: "outcome",
    legacy: "Outcome",
  },
  owner_relevance: {
    newBase: "owner_relevance",
    legacy: "Owner Relevance",
  },
  data_status: {
    newBase: "data_status",
    legacy: "Data Status",
  },
  image_url: {
    newBase: "image_url",
    legacy: "Image URL",
  },
  display_order: {
    newBase: "display_order",
    legacy: "Display Order",
  },
};

function pickField(row, key) {
  const spec = map_operatorCaseStudyFields[key];
  if (!spec || !row) return "";
  return formatListValue(row[spec.newBase]) || formatListValue(row[spec.legacy]);
}

function resolveImageUrl(rawImage, fallbackText) {
  if (typeof rawImage === "string" && /^https?:\/\//i.test(rawImage.trim())) {
    return rawImage.trim();
  }
  if (Array.isArray(rawImage) && rawImage[0] && rawImage[0].url) {
    return String(rawImage[0].url);
  }
  const text = formatListValue(fallbackText);
  return /^https?:\/\//i.test(text) ? text : "";
}

/**
 * Normalize one Airtable case study row → API / prefill shape.
 * @param {AirtableFieldsRow} row
 * @returns {import("./operator-case-study-airtable-map.js").OperatorCaseStudyDetailRow}
 */
export function mapAirtableRowToCaseStudyDetail(row) {
  row = row || {};
  const rawImage = row.image_url ?? row["Image URL"];
  return {
    property_name: pickField(row, "property_name"),
    hotel_type: pickField(row, "hotel_type"),
    region: pickField(row, "region"),
    branded_independent: pickField(row, "branded_independent"),
    situation: normalizeCaseStudySituationForForm(pickField(row, "situation")),
    challenge: pickField(row, "challenge"),
    services: pickField(row, "services"),
    outcome: pickField(row, "outcome"),
    owner_relevance: pickField(row, "owner_relevance"),
    data_status: pickField(row, "data_status"),
    image_url: resolveImageUrl(rawImage, pickField(row, "image_url")),
  };
}

/**
 * @param {import("./operator-case-study-airtable-map.js").OperatorCaseStudyDetailRow} item
 * @param {{ operatorRecordId?: string, companyName?: string }} [ctx]
 * @returns {Record<string, string>}
 */
export function mapCaseStudyDetailToLegacyAirtableRow(item, ctx = {}) {
  const o = item || {};
  return {
    "Operator Record ID": ctx.operatorRecordId || "",
    "Company Name": ctx.companyName || "",
    "Property Name": String(o.property_name || "").trim(),
    "Hotel Type": String(o.hotel_type || "").trim(),
    Region: String(o.region || "").trim(),
    "Branded / Independent": String(o.branded_independent || "").trim(),
    Situation: String(o.situation || "").trim(),
    Challenge: String(o.challenge || "").trim(),
    Services: String(o.services || "").trim(),
    Outcome: String(o.outcome || "").trim(),
    "Owner Relevance": String(o.owner_relevance || "").trim(),
    "Data Status": String(o.data_status || "").trim(),
    "Image URL": String(o.image_url || "").trim(),
  };
}

/**
 * New-base child table write payload (snake_case columns).
 * @param {import("./operator-case-study-airtable-map.js").OperatorCaseStudyDetailRow} item
 * @param {number} idx
 */
export function mapCaseStudyDetailToNewBaseChildRow(item, idx, opts = {}) {
  const o = item || {};
  const row = {
    display_order: idx + 1,
    property_name: String(o.property_name || "").trim(),
    hotel_type: String(o.hotel_type || "").trim(),
    region: String(o.region || "").trim(),
    branded_independent: String(o.branded_independent || "").trim(),
    situation: String(o.situation || "").trim(),
    challenge: String(o.challenge || "").trim(),
    services: String(o.services || "").trim(),
    outcome: String(o.outcome || "").trim(),
    owner_relevance: String(o.owner_relevance || "").trim(),
    data_status: String(o.data_status || "").trim(),
    image_url: String(o.image_url || "").trim(),
  };
  if (opts.omitExtendedFields) {
    delete row.challenge;
    delete row.data_status;
  }
  return row;
}

/**
 * @typedef {{
 *   property_name?: string;
 *   hotel_type?: string;
 *   region?: string;
 *   branded_independent?: string;
 *   situation?: string;
 *   challenge?: string;
 *   services?: string;
 *   outcome?: string;
 *   owner_relevance?: string;
 *   data_status?: string;
 *   image_url?: string;
 * }} OperatorCaseStudyDetailRow
 */
