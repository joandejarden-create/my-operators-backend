/**
 * Footprint table writes for third-party operator intake.
 * Intake builds one canonical `fields` object (third-party-operator-intake.js); geo / footprint
 * columns may exist only on "3rd Party Operator - Footprint". This module maps names and
 * builds a create payload from the full compact intake fields — not from Basics-filtered rows.
 */

import {
  filterFieldsToAirtableSchema,
  remapBasicsFieldsForAirtableSchema,
} from "./third-party-operator-basics-airtable-column-aliases.js";

/**
 * Intake uses Basics-style "%" location columns; many Footprint bases use shorter names.
 * [intakeKey, footprintKey] — applied only when intakeKey is missing from schema and footprintKey exists.
 */
const LOCATION_INTAKE_TO_FOOTPRINT = [
  ["Location Type % Urban", "Location Type Urban"],
  ["Location Type % Suburban", "Location Type Suburban"],
  ["Location Type % Resort", "Location Type Resort"],
  ["Location Type % Airport", "Location Type Airport"],
  ["Location Type % Total", "Location Type Total"],
  ["Location Type % Small Metro/Town", "Location Type Highway"],
  ["Location Type % Interstate", "Location Type Other"],
];

/**
 * Dynamic brand table detail can live in different long-text fields on Footprint.
 * [intakeKey, footprintKey] — move only when intakeKey is missing and footprintKey exists.
 */
const DYNAMIC_BRAND_DETAIL_INTAKE_TO_FOOTPRINT = [
  ["Brands Portfolio Detail", "Brand Units & Staffing Detail"],
  ["Brands Portfolio Detail", "Brand Units and Staffing Detail"],
];

const FOOTPRINT_HIGHWAY = "Location Type Highway";
const LOC_SMALL = "Location Type % Small Metro/Town";
const LOC_INTERSTATE = "Location Type % Interstate";

function compactAirtableFieldPayload(obj) {
  return Object.fromEntries(
    Object.entries(obj).filter(([, value]) => {
      if (value == null) return false;
      if (typeof value === "string") return value.trim() !== "";
      if (Array.isArray(value)) return value.length > 0;
      return true;
    })
  );
}

function toFiniteNumber(v) {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = parseFloat(v.replace(/,/g, ""));
    return Number.isFinite(n) ? n : 0;
  }
  return 0;
}

function sumFromKeys(obj, keys) {
  return keys.reduce((acc, key) => acc + toFiniteNumber(obj[key]), 0);
}

/**
 * Derive Regions multi-select from geo distribution:
 * include a region when any Existing/Pipeline Hotels/Rooms value > 0.
 */
function deriveRegionsFromGeoDistribution(fields) {
  const regionKeySets = [
    {
      label: "NA",
      keys: [
        "Geo NA Existing Hotels",
        "Geo NA Existing Rooms",
        "Geo NA Pipeline Hotels",
        "Geo NA Pipeline Rooms",
        "NA Existing Hotels",
        "NA Existing Rooms",
        "NA Pipeline Hotels",
        "NA Pipeline Rooms",
      ],
    },
    {
      label: "CALA",
      keys: [
        "Geo CALA Existing Hotels",
        "Geo CALA Existing Rooms",
        "Geo CALA Pipeline Hotels",
        "Geo CALA Pipeline Rooms",
        "CALA Existing Hotels",
        "CALA Existing Rooms",
        "CALA Pipeline Hotels",
        "CALA Pipeline Rooms",
      ],
    },
    {
      label: "EU",
      keys: [
        "Geo EU Existing Hotels",
        "Geo EU Existing Rooms",
        "Geo EU Pipeline Hotels",
        "Geo EU Pipeline Rooms",
        "EU Existing Hotels",
        "EU Existing Rooms",
        "EU Pipeline Hotels",
        "EU Pipeline Rooms",
      ],
    },
    {
      label: "MEA",
      keys: [
        "Geo MEA Existing Hotels",
        "Geo MEA Existing Rooms",
        "Geo MEA Pipeline Hotels",
        "Geo MEA Pipeline Rooms",
        "MEA Existing Hotels",
        "MEA Existing Rooms",
        "MEA Pipeline Hotels",
        "MEA Pipeline Rooms",
      ],
    },
    {
      label: "APAC",
      keys: [
        "Geo APAC Existing Hotels",
        "Geo APAC Existing Rooms",
        "Geo APAC Pipeline Hotels",
        "Geo APAC Pipeline Rooms",
        "APAC Existing Hotels",
        "APAC Existing Rooms",
        "APAC Pipeline Hotels",
        "APAC Pipeline Rooms",
      ],
    },
  ];

  return regionKeySets
    .filter(({ keys }) => sumFromKeys(fields, keys) > 0)
    .map(({ label }) => label);
}

/**
 * Rename location keys to match Footprint; optionally merge Small Metro + Interstate into Highway
 * when the base has no separate columns for them (common Footprint seed shape).
 */
function remapLocationKeysForFootprint(fields, schemaSet) {
  const f = { ...fields };

  for (const [from, to] of LOCATION_INTAKE_TO_FOOTPRINT) {
    if (!Object.prototype.hasOwnProperty.call(f, from)) continue;
    if (schemaSet.has(from)) continue;
    if (!schemaSet.has(to)) continue;
    f[to] = f[from];
    delete f[from];
  }

  const hasSmallCol = schemaSet.has(LOC_SMALL);
  const hasInterCol = schemaSet.has(LOC_INTERSTATE);
  const hasHighwayCol = schemaSet.has(FOOTPRINT_HIGHWAY);

  if (!hasSmallCol && !hasInterCol && hasHighwayCol) {
    const combined =
      toFiniteNumber(f[LOC_SMALL]) + toFiniteNumber(f[LOC_INTERSTATE]);
    if (combined > 0 && f[FOOTPRINT_HIGHWAY] == null) {
      f[FOOTPRINT_HIGHWAY] = combined;
    }
    if (Object.prototype.hasOwnProperty.call(f, LOC_SMALL)) delete f[LOC_SMALL];
    if (Object.prototype.hasOwnProperty.call(f, LOC_INTERSTATE)) delete f[LOC_INTERSTATE];
  }

  return f;
}

function remapDynamicBrandDetailForFootprint(fields, schemaSet) {
  const f = { ...fields };
  for (const [from, to] of DYNAMIC_BRAND_DETAIL_INTAKE_TO_FOOTPRINT) {
    if (!Object.prototype.hasOwnProperty.call(f, from)) continue;
    if (schemaSet.has(from)) continue;
    if (!schemaSet.has(to)) continue;
    f[to] = f[from];
    delete f[from];
    break;
  }
  return f;
}

/**
 * @param {Record<string, unknown>} compactIntakeFields Populated intake fields (same keys as Airtable intake payload).
 * @param {string} basicsRecordId
 * @param {Set<string>} footprintSchema Field names on Footprint table
 * @param {string} basicsLinkField e.g. "Operator (Basics Link)"
 * @returns {Record<string, unknown> | null}
 */
export function buildFootprintRowPayloadFromIntake(compactIntakeFields, basicsRecordId, footprintSchema, basicsLinkField) {
  if (!footprintSchema || footprintSchema.size === 0) return null;

  let merged = { ...compactIntakeFields };
  merged = remapBasicsFieldsForAirtableSchema(merged, footprintSchema);
  merged = remapLocationKeysForFootprint(merged, footprintSchema);
  merged = remapDynamicBrandDetailForFootprint(merged, footprintSchema);
  merged = filterFieldsToAirtableSchema(merged, footprintSchema);

  const out = { ...merged };

  const derivedRegions = deriveRegionsFromGeoDistribution(compactIntakeFields);
  if (derivedRegions.length > 0) {
    if (footprintSchema.has("Regions")) {
      out.Regions = derivedRegions;
    }
    if (footprintSchema.has("Regions Supported")) {
      out["Regions Supported"] = derivedRegions;
    }
  }

  if (footprintSchema.has(basicsLinkField)) {
    out[basicsLinkField] = [basicsRecordId];
  }
  const companyName = compactIntakeFields["Company Name"];
  if (footprintSchema.has("Operator") && companyName) {
    out.Operator = String(companyName).trim();
  }

  const compact = compactAirtableFieldPayload(out);
  if (!compact[basicsLinkField]) return null;
  return compact;
}
