/**
 * Sample opportunity / sample deal — two-layer model and per-field source tagging.
 * @see docs/sample-opportunity-deal-governance.md
 */

export const SOURCE_TYPES = Object.freeze({
  PUBLIC_REFERENCE: "public_reference",
  INFERRED_FROM_REFERENCE: "inferred_from_reference",
  FICTIONAL_SAMPLE_ASSUMPTION: "fictional_sample_assumption",
  NEEDS_VALIDATION: "needs_validation",
});

export const FIELD_LAYERS = Object.freeze({
  REFERENCE_PROPERTY: "reference_property",
  FICTIONAL_DEAL: "fictional_deal",
});

/** Fields that should default to reference_property layer when tagging. */
export const REFERENCE_LAYER_FIELD_NAMES = new Set([
  "Property Name",
  "Full Address",
  "City & State",
  "Country",
  "Hotel Submarket & Location",
  "Hotel Chain Scale",
  "Hotel Type",
  "Hotel Service Model",
  "Total Number of Rooms/Keys",
  "Number of Standard Rooms",
  "Number of Suites",
  "Building Type",
  "Number of Stories",
  "Meeting Space",
  "Meeting Space Unit",
  "F&B Outlets?",
  "Outlet Names / Concepts",
  "Number of F&B Outlets",
  "F&B Program Type",
  "Additional Amenities",
  "Parking Amenities?",
  "Primary Demand Drivers",
  "Key Competitors",
  "Current Brand Affiliation",
  "Parent Company Name",
  "Operator Name Current",
  "Is the hotel currently branded?",
  "Is the hotel currently managed by a third-party operator?",
  "Total Site Size",
  "Total Site Size Unit",
  "Zoning Status",
  "Zoned for Hotel Development",
]);

/** Fields that should default to fictional_deal layer when tagging. */
export const FICTIONAL_LAYER_FIELD_NAMES = new Set([
  "Project Name",
  "Stage of Development",
  "Expected Opening or Rebranding Date",
  "Who should receive bids for this project?",
  "Ownership Structure",
  "Preferred Deal Structure",
  "PIP / CapEx Status",
  "Total Project Cost Range",
  "Equity vs Debt Split",
  "PIP Budget Range (if conversion)",
  "IRR/Yield Goals",
  "Soft vs Hard Brand Preference",
  "Plan to Self-Manage or Hire Third Party?",
  "Preferred Brands (up to 4)",
  "Preferred Chain Scales",
  "Top 3 Success Metrics",
  "Top Priorities for Project",
  "Top Concerns for this Project",
  "Top 3 Deal Breakers",
  "Top 3 Deal Breakers Other",
  "Must-haves From Brand or Operator",
  "Must-haves From Brand or Operator Other",
  "Planned Hold Period",
  "Primary Goal for the Hotel",
  "Brand Flexibility vs Prestige",
  "Decision Timeline for Brand/Operator",
  "Proposal Deadline",
  "Company Executive Summary",
  "Ownership/Brand History or Track Record",
  "Portfolio Size",
  "Main Contact Name",
  "Entity or Company Name",
  "Email Address",
  "Company HQ Location",
  "Working with Broker/Advisor?",
  "Are you open to considering other brands with favorable terms?",
]);

export const PROHIBITED_REFERENCE_IMPLICATIONS = Object.freeze([
  "for sale",
  "distressed",
  "seeking a brand",
  "seeking an operator",
  "participating in dealality",
  "available for conversion",
  "available for acquisition",
  "on dealality",
  "listed on dealality",
]);

export const SAMPLE_DISCLAIMER =
  "Sample deal for product demonstration only. Reference property is a public comp for factual context; it is not offered for sale and is not participating in Dealality.";

/**
 * @param {string} fieldName
 * @returns {typeof FIELD_LAYERS[keyof typeof FIELD_LAYERS]}
 */
export function defaultLayerForField(fieldName) {
  if (FICTIONAL_LAYER_FIELD_NAMES.has(fieldName)) return FIELD_LAYERS.FICTIONAL_DEAL;
  if (REFERENCE_LAYER_FIELD_NAMES.has(fieldName)) return FIELD_LAYERS.REFERENCE_PROPERTY;
  return FIELD_LAYERS.FICTIONAL_DEAL;
}

/**
 * @param {unknown} record
 * @returns {{ ok: boolean, errors: string[], warnings: string[] }}
 */
export function validateSampleDealRecord(record) {
  const errors = [];
  const warnings = [];

  if (!record || typeof record !== "object") {
    return { ok: false, errors: ["Record must be an object"], warnings };
  }

  const meta = record.meta || {};
  if (!meta.isSample) warnings.push("meta.isSample should be true for sample records");

  const ref = record.referenceProperty;
  const fic = record.fictionalDeal;
  if (!ref || typeof ref !== "object") errors.push("referenceProperty object is required");
  if (!fic || typeof fic !== "object") errors.push("fictionalDeal object is required");

  const projectName = fic?.fields?.["Project Name"] || fic?.fields?.["Property Name"];
  const refPublicName = ref?.publicName || ref?.displayLabel || "";
  if (projectName && refPublicName && String(projectName).trim() === String(refPublicName).trim()) {
    errors.push("fictional project name must not equal reference public name");
  }

  const fields = {
    ...(ref?.fields || {}),
    ...(fic?.fields || {}),
    ...(record.mergedFields || {}),
    ...(record.fields || {}),
  };
  const fieldSources = buildDefaultFieldSources(record);
  for (const [key, val] of Object.entries(fields)) {
    if (val == null || val === "") continue;
    if (!fieldSources[key]) {
      warnings.push(`Missing fieldSources tag for populated field: ${key}`);
    } else {
      const tag = fieldSources[key];
      if (!Object.values(SOURCE_TYPES).includes(tag.sourceType)) {
        errors.push(`Invalid sourceType for ${key}: ${tag.sourceType}`);
      }
      if (!Object.values(FIELD_LAYERS).includes(tag.layer)) {
        errors.push(`Invalid layer for ${key}: ${tag.layer}`);
      }
    }
  }

  const scan = { ...record };
  delete scan.disclaimer;
  delete scan.meta;
  const blob = JSON.stringify(scan).toLowerCase();
  for (const phrase of PROHIBITED_REFERENCE_IMPLICATIONS) {
    if (blob.includes(phrase)) errors.push(`Prohibited implication phrase found: "${phrase}"`);
  }

  if (!record.disclaimer && !meta.disclaimer) {
    warnings.push("Missing disclaimer string on record");
  }

  return { ok: errors.length === 0, errors, warnings };
}

/**
 * Flatten reference + fictional layers into Deal Setup fields with internal metadata.
 * @param {object} record
 * @returns {{ fields: Record<string, unknown>, meta: object }}
 */
/**
 * Build fieldSources entries for all populated fields using defaults.
 * @param {object} record
 * @returns {Record<string, { sourceType: string, layer: string, note?: string }>}
 */
export function buildDefaultFieldSources(record) {
  const refFields = record.referenceProperty?.fields || {};
  const ficFields = record.fictionalDeal?.fields || {};
  const out = { ...(record.fieldSources || {}) };
  for (const key of Object.keys(refFields)) {
    if (refFields[key] == null || refFields[key] === "" || out[key]) continue;
    out[key] = {
      sourceType: SOURCE_TYPES.PUBLIC_REFERENCE,
      layer: FIELD_LAYERS.REFERENCE_PROPERTY,
      note: "Auto-tagged from referenceProperty.fields",
    };
  }
  for (const key of Object.keys(ficFields)) {
    if (ficFields[key] == null || ficFields[key] === "" || out[key]) continue;
    out[key] = {
      sourceType: SOURCE_TYPES.FICTIONAL_SAMPLE_ASSUMPTION,
      layer: FIELD_LAYERS.FICTIONAL_DEAL,
      note: "Auto-tagged from fictionalDeal.fields",
    };
  }
  return out;
}

/**
 * Build Airtable-ready row descriptors from a sample deal record.
 * @param {object} record
 * @returns {Array<{ table: string, field: string, value: unknown, sourceType: string, layer: string, notes?: string, sourceUrl?: string }>}
 */
export function buildAirtableFieldMap(record) {
  const ref = record.referenceProperty?.fields || {};
  const fic = record.fictionalDeal?.fields || {};
  const sources = buildDefaultFieldSources(record);
  const rows = [];

  const push = (table, field, value, layerOverride) => {
    if (value == null || value === "") return;
    const tag = sources[field] || {
      sourceType:
        layerOverride === FIELD_LAYERS.REFERENCE_PROPERTY
          ? SOURCE_TYPES.PUBLIC_REFERENCE
          : SOURCE_TYPES.FICTIONAL_SAMPLE_ASSUMPTION,
      layer: layerOverride || defaultLayerForField(field),
    };
    rows.push({
      table: table,
      field: field,
      value: value,
      sourceType: tag.sourceType,
      layer: tag.layer,
      notes: tag.note || "",
      sourceUrl: tag.sourceUrl || "",
    });
  };

  const merged = { ...ref, ...fic };
  for (const [field, value] of Object.entries(merged)) {
    const ficVal = fic[field];
    const layer =
      ficVal != null && ficVal !== ""
        ? FIELD_LAYERS.FICTIONAL_DEAL
        : FIELD_LAYERS.REFERENCE_PROPERTY;
    push("Deals", field, value, layer);
  }

  for (const row of record.airtableRows || []) {
    if (!row?.field) continue;
    rows.push({
      table: row.table || "Deals",
      field: row.field,
      value: row.value,
      sourceType: row.sourceType || SOURCE_TYPES.FICTIONAL_SAMPLE_ASSUMPTION,
      layer: row.layer || FIELD_LAYERS.FICTIONAL_DEAL,
      notes: row.notes || "",
      sourceUrl: row.sourceUrl || "",
    });
  }

  return rows;
}

/**
 * @param {ReturnType<typeof buildAirtableFieldMap>} rows
 * @returns {string}
 */
export function formatAirtableFieldMapMarkdown(rows) {
  const lines = [
    "| Airtable table | Field | Value | Source type | Layer | Notes |",
    "| --- | --- | --- | --- | --- | --- |",
  ];
  for (const r of rows) {
    const val = Array.isArray(r.value) ? r.value.join("; ") : String(r.value ?? "");
    const safe = val.replace(/\|/g, "\\|").replace(/\n/g, " ");
    lines.push(
      `| ${r.table} | ${r.field} | ${safe.slice(0, 120)}${safe.length > 120 ? "…" : ""} | ${r.sourceType} | ${r.layer} | ${r.notes || ""} |`
    );
  }
  return lines.join("\n");
}

export function flattenSampleDealToIntakeFields(record) {
  const refFields = record.referenceProperty?.fields || {};
  const ficFields = record.fictionalDeal?.fields || {};
  const fields = { ...refFields, ...ficFields };
  return {
    fields,
    meta: {
      isSample: true,
      sampleId: record.meta?.sampleId,
      referenceProperty: record.referenceProperty,
      fictionalDeal: record.fictionalDeal,
      fieldSources: record.fieldSources,
      disclaimer: record.disclaimer || SAMPLE_DISCLAIMER,
    },
  };
}
