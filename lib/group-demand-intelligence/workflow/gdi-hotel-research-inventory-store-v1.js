/**
 * GDI Hotel Research Inventory Airtable store V1.
 * Composite key (hotelId, opportunityId). Fail-closed on oversized payload.
 * Never writes GDO or publication packages.
 */

import Airtable from "airtable";
import { escapeAirtableFormulaValue } from "../../airtable-utils.js";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
} from "../../decision-outcomes/airtable-base.js";
import { airtableRequestTimeoutMs } from "../../http/with-timeout.js";
import {
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
  GDI_HOTEL_RESEARCH_OPPORTUNITY_SCHEMA_VERSION,
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
  VAL_RESEARCH_LIFECYCLE_STATUS,
  VAL_COMMERCIAL_ELIGIBILITY_STATUS,
  buildHotelResearchCompositeKey,
} from "../hotel-research-opportunity-field-map.js";
import {
  classifyOpportunityLifecycleLayer,
  extractDemandThesisEnum,
} from "./gdi-complete-research-persistence-v1.js";

export const GDI_HOTEL_RESEARCH_INVENTORY_STORE_V1 =
  "gdi-hotel-research-inventory-store-v1";

/** Airtable multiline safe ceiling — exceed → fail closed (no truncate). */
export const RESEARCH_PAYLOAD_MAX_CHARS = 95_000;

const TELEMETRY_DROP_KEYS = Object.freeze([
  "orsRaw",
  "orsTranscript",
  "firecrawlRaw",
  "exaRawHits",
  "retrievalLog",
  "plannerTrace",
  "ledgerEntries",
  "deepResearchEnrichment",
  "supervisorV2",
]);

function text(v) {
  // Never coerce structured objects via String(obj) → "[object Object]".
  if (v == null) return "";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
    return String(v).replace(/\s+/g, " ").trim();
  }
  return "";
}

/**
 * Extract one human-readable source evidence line from a source entry.
 * Prefers URL, then label/title/name, then relative path. Never fabricates.
 */
export function formatResearchSourceEvidencePart(source) {
  if (source == null) return "";
  if (typeof source === "string" || typeof source === "number") {
    return text(source);
  }
  if (typeof source !== "object") return "";

  const url = text(source.url || source.sourceUrl || source.href);
  if (url) return url;

  const label = text(
    source.label ||
      source.title ||
      source.name ||
      source.sourceName ||
      source.citation ||
      source.description,
  );
  const pathRel = text(source.pathRel || source.path || source.file);
  if (label && pathRel) return `${label} (${pathRel})`;
  if (label) return label;
  if (pathRel) return pathRel;
  return "";
}

/**
 * Build evidence-grounded sourceSummary for research inventory rows.
 * Rejects accidental "[object Object]" placeholders from prior bad coercion.
 */
export function buildResearchSourceSummary(opp = {}) {
  const rawSummary = opp.sourceSummary;
  let fromField = "";
  if (typeof rawSummary === "string" || typeof rawSummary === "number") {
    fromField = text(rawSummary);
  } else if (Array.isArray(rawSummary)) {
    fromField = rawSummary
      .map((s) => formatResearchSourceEvidencePart(s))
      .filter(Boolean)
      .slice(0, 5)
      .join(" | ");
  } else if (rawSummary && typeof rawSummary === "object") {
    fromField = formatResearchSourceEvidencePart(rawSummary);
  }
  if (fromField && !/\[object Object\]/i.test(fromField)) {
    return fromField;
  }

  const sources = Array.isArray(opp.sources) ? opp.sources : [];
  const fromSources = sources
    .map((s) => formatResearchSourceEvidencePart(s))
    .filter(Boolean)
    .slice(0, 5)
    .join(" | ");
  if (fromSources && !/\[object Object\]/i.test(fromSources)) {
    return fromSources;
  }
  return null;
}

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function getBase() {
  const apiKey = getToken();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  if (!apiKey || !baseId) {
    const err = new Error("gdi_hotel_research_airtable_not_configured");
    err.code = "gdi_hotel_research_airtable_not_configured";
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, {
    surface: "gdi_hotel_research_inventory",
  });
  return new Airtable({
    apiKey,
    requestTimeout: airtableRequestTimeoutMs(),
  }).base(baseId);
}

function table() {
  return getBase()(GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME);
}

function asSelect(value, allowed) {
  const v = text(value);
  if (!v) return undefined;
  return allowed.includes(v) ? v : undefined;
}

/**
 * Serialize research payload without silent research loss.
 * Drops only pure telemetry keys. If still oversized → fail closed.
 */
export function serializeResearchPayload(opp = {}) {
  const next = { ...opp };
  for (const k of TELEMETRY_DROP_KEYS) {
    if (k in next) delete next[k];
  }
  // Omit empty demandThesis so classifier null overlays do not churn payload.
  if (next.demandThesis == null || next.demandThesis === "") {
    delete next.demandThesis;
  }
  let json;
  try {
    json = JSON.stringify(next);
  } catch (err) {
    return {
      ok: false,
      code: "RESEARCH_PAYLOAD_SERIALIZE_FAILED",
      error: String(err?.message || err),
      json: null,
      length: 0,
    };
  }
  if (json.length > RESEARCH_PAYLOAD_MAX_CHARS) {
    return {
      ok: false,
      code: "RESEARCH_PAYLOAD_TOO_LARGE",
      error: `payload ${json.length} chars exceeds ${RESEARCH_PAYLOAD_MAX_CHARS}; refuse truncate`,
      json: null,
      length: json.length,
    };
  }
  return { ok: true, code: null, json, length: json.length };
}

export function inferResearchLifecycleStatus(opp = {}) {
  const d = text(opp.disposition || opp.evaluationLabel).toUpperCase();
  const pop = text(opp.inventoryPopulationPrimary).toUpperCase();
  if (d === "WATCHLIST_RESEARCH" || pop === "WATCHLIST") return "WATCHLIST";
  if (d === "EXCLUDE" || d === "REJECTED" || text(opp.priority) === "DISQUALIFIED") {
    return "ARCHIVED_RESEARCH";
  }
  if (pop === "SALES_OPPORTUNITIES") return "ACTIVE_RESEARCH";
  if (!extractDemandThesisEnum(opp) && !opp.primaryContact) return "EXPLORATORY";
  return "ACTIVE_RESEARCH";
}

export function buildResearchInventoryFields(opp = {}, hotel = {}) {
  const hotelId = text(hotel.hotelId || opp.hotelId);
  const opportunityId = text(opp.opportunityId || opp.id);
  const compositeKey = buildHotelResearchCompositeKey(hotelId, opportunityId);
  if (!compositeKey) {
    return {
      ok: false,
      code: "MISSING_COMPOSITE_IDENTITY",
      fields: null,
    };
  }

  const payload = serializeResearchPayload(opp);
  if (!payload.ok) {
    return { ok: false, code: payload.code, detail: payload.error, fields: null };
  }

  const cls = classifyOpportunityLifecycleLayer(
    {
      ...opp,
      opportunityId,
      hotelId,
      demandThesis: extractDemandThesisEnum(opp),
    },
    { hotelId },
  );

  const sourceSummary = buildResearchSourceSummary(opp);

  const fields = {
    [F.compositeKey]: compositeKey,
    [F.opportunityId]: opportunityId,
    [F.hotelId]: hotelId,
    [F.organizationName]: text(opp.organizationName || opp.company) || null,
    [F.researchLifecycleStatus]: asSelect(
      inferResearchLifecycleStatus(opp),
      VAL_RESEARCH_LIFECYCLE_STATUS,
    ),
    [F.commercialEligibilityStatus]: asSelect(
      cls.eligibility?.commercialQualificationStatus || "RESEARCH_ONLY",
      VAL_COMMERCIAL_ELIGIBILITY_STATUS,
    ),
    [F.demandThesis]: extractDemandThesisEnum(opp) || null,
    [F.priority]: text(opp.priority) || null,
    [F.title]: text(opp.title || opp.displayTitle || opp.opportunityName) || null,
    [F.segment]: text(opp.segment || opp.segmentDisplayLabel) || null,
    [F.sourceSummary]: sourceSummary,
    [F.firstDiscoveredAt]:
      text(opp.firstDiscoveredAt || opp.firstSeenAt) || null,
    [F.firstDiscoveredRunId]:
      text(opp.firstDiscoveredRunId || opp.firstSeenRunId || opp.runId) || null,
    [F.lastResearchedAt]:
      text(opp.lastResearchedAt || opp.updatedAt || opp.lastSeenAt) || null,
    [F.lastMaterialChangeAt]: text(opp.lastMaterialChangeAt) || null,
    [F.dgGeneratorId]: text(opp.dgGeneratorId || opp.demandGeneratorId) || null,
    [F.researchPayloadJson]: payload.json,
    [F.schemaVersion]: GDI_HOTEL_RESEARCH_OPPORTUNITY_SCHEMA_VERSION,
  };

  // Drop null/undefined/empty
  const clean = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") continue;
    clean[k] = v;
  }

  return {
    ok: true,
    code: null,
    compositeKey,
    hotelId,
    opportunityId,
    fields: clean,
    payloadLength: payload.length,
    commerciallyQualified: cls.commerciallyQualified === true,
  };
}

/**
 * Fields compared for material research change detection.
 * firstDiscovered* excluded — preserved operational provenance.
 */
export const MATERIAL_RESEARCH_FIELDS = Object.freeze([
  F.opportunityId,
  F.hotelId,
  F.organizationName,
  F.researchLifecycleStatus,
  F.commercialEligibilityStatus,
  F.demandThesis,
  F.priority,
  F.title,
  F.segment,
  F.sourceSummary,
  F.lastResearchedAt,
  F.lastMaterialChangeAt,
  F.dgGeneratorId,
  F.researchPayloadJson,
  F.schemaVersion,
  F.compositeKey,
]);

function normalizeComparableValue(fieldName, value) {
  if (value === undefined || value === null || value === "") return null;
  if (fieldName === F.researchPayloadJson && typeof value === "string") {
    try {
      return canonicalizeJson(JSON.parse(value));
    } catch {
      return String(value).trim();
    }
  }
  if (typeof value === "string") return value.replace(/\s+/g, " ").trim();
  if (typeof value === "object") return canonicalizeJson(value);
  return value;
}

/** Deterministic JSON for stable fingerprint/compare (sorted keys, drop null/empty). */
export function canonicalizeJson(value) {
  if (value === undefined || value === null || value === "") return null;
  if (Array.isArray(value)) {
    return value.map((v) => canonicalizeJson(v));
  }
  if (typeof value === "object") {
    const out = {};
    for (const k of Object.keys(value).sort()) {
      const v = canonicalizeJson(value[k]);
      if (v === null || v === undefined || v === "") continue;
      out[k] = v;
    }
    return out;
  }
  if (typeof value === "string") {
    const t = value.replace(/\s+/g, " ").trim();
    return t === "" ? null : t;
  }
  return value;
}

export function fingerprintResearchFields(fields = {}) {
  const slice = {};
  for (const name of MATERIAL_RESEARCH_FIELDS) {
    slice[name] = normalizeComparableValue(name, fields[name]);
  }
  return JSON.stringify(canonicalizeJson(slice));
}

/**
 * Compare proposed write fields against existing Airtable record fields.
 * Returns materialChanged + changed field names.
 */
export function diffMaterialResearchFields(proposed = {}, existing = {}) {
  const changed = [];
  for (const name of MATERIAL_RESEARCH_FIELDS) {
    const a = normalizeComparableValue(name, proposed[name]);
    const b = normalizeComparableValue(name, existing[name]);
    if (JSON.stringify(a) !== JSON.stringify(b)) {
      changed.push({
        field: name,
        beforeFingerprint: fingerprintResearchFields({ [name]: existing[name] }),
        afterFingerprint: fingerprintResearchFields({ [name]: proposed[name] }),
      });
    }
  }
  return {
    materialChanged: changed.length > 0,
    changedFields: changed.map((c) => c.field),
    changes: changed,
    beforeFingerprint: fingerprintResearchFields(existing),
    afterFingerprint: fingerprintResearchFields(proposed),
  };
}

export async function findResearchInventoryByCompositeKey(compositeKey) {
  const key = text(compositeKey);
  if (!key) return null;
  const formula = `{${F.compositeKey}} = "${escapeAirtableFormulaValue(key)}"`;
  const records = await table()
    .select({ filterByFormula: formula, maxRecords: 5 })
    .firstPage();
  if (!records.length) return null;
  if (records.length > 1) {
    const err = new Error(`duplicate_research_inventory_key:${key}`);
    err.code = "DUPLICATE_COMPOSITE_KEY";
    err.records = records.map((r) => r.id);
    throw err;
  }
  return records[0];
}

export async function listResearchInventoryForHotel(hotelId) {
  const id = text(hotelId);
  if (!id) return [];
  const formula = `{${F.hotelId}} = "${escapeAirtableFormulaValue(id)}"`;
  const out = [];
  await table()
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((page, next) => {
      out.push(...page);
      next();
    });
  return out;
}

/**
 * Upsert one research inventory row. Preserves firstDiscovered* on update.
 */
export async function upsertResearchInventoryRecord({
  opportunity,
  hotel,
  dryRun = true,
} = {}) {
  const built = buildResearchInventoryFields(opportunity, hotel);
  if (!built.ok) {
    return {
      ok: false,
      action: "FAILED",
      code: built.code,
      detail: built.detail || null,
      compositeKey: null,
    };
  }

  let existing = null;
  try {
    existing = await findResearchInventoryByCompositeKey(built.compositeKey);
  } catch (err) {
    if (err.code === "DUPLICATE_COMPOSITE_KEY") {
      return {
        ok: false,
        action: "FAILED",
        code: err.code,
        detail: err.records,
        compositeKey: built.compositeKey,
      };
    }
    throw err;
  }

  const fields = { ...built.fields };
  if (existing) {
    // Preserve first discovery history
    const prev = existing.fields || {};
    if (prev[F.firstDiscoveredAt]) {
      fields[F.firstDiscoveredAt] = prev[F.firstDiscoveredAt];
    }
    if (prev[F.firstDiscoveredRunId]) {
      fields[F.firstDiscoveredRunId] = prev[F.firstDiscoveredRunId];
    }
  }

  let materialDiff = null;
  if (existing) {
    materialDiff = diffMaterialResearchFields(fields, existing.fields || {});
  }

  if (dryRun) {
    let action = "WOULD_CREATE";
    if (existing) {
      action = materialDiff.materialChanged ? "WOULD_UPDATE" : "UNCHANGED";
    }
    return {
      ok: true,
      action,
      compositeKey: built.compositeKey,
      hotelId: built.hotelId,
      opportunityId: built.opportunityId,
      airtableRecordId: existing?.id || null,
      fields,
      payloadLength: built.payloadLength,
      materialDiff,
    };
  }

  if (existing) {
    if (!materialDiff.materialChanged) {
      return {
        ok: true,
        action: "UNCHANGED",
        compositeKey: built.compositeKey,
        hotelId: built.hotelId,
        opportunityId: built.opportunityId,
        airtableRecordId: existing.id,
        fields,
        payloadLength: built.payloadLength,
        materialDiff,
      };
    }
    const updated = await table().update(existing.id, fields);
    return {
      ok: true,
      action: "UPDATED",
      compositeKey: built.compositeKey,
      hotelId: built.hotelId,
      opportunityId: built.opportunityId,
      airtableRecordId: updated.id,
      fields,
      payloadLength: built.payloadLength,
      materialDiff,
    };
  }

  const created = await table().create(fields);
  return {
    ok: true,
    action: "CREATED",
    compositeKey: built.compositeKey,
    hotelId: built.hotelId,
    opportunityId: built.opportunityId,
    airtableRecordId: created.id,
    fields,
    payloadLength: built.payloadLength,
    materialDiff: null,
  };
}

export async function readBackResearchInventory(airtableRecordId) {
  const rec = await table().find(airtableRecordId);
  return {
    id: rec.id,
    fields: rec.fields,
    compositeKey: rec.fields?.[F.compositeKey] || null,
    opportunityId: rec.fields?.[F.opportunityId] || null,
    hotelId: rec.fields?.[F.hotelId] || null,
  };
}

export default {
  GDI_HOTEL_RESEARCH_INVENTORY_STORE_V1,
  RESEARCH_PAYLOAD_MAX_CHARS,
  MATERIAL_RESEARCH_FIELDS,
  serializeResearchPayload,
  canonicalizeJson,
  fingerprintResearchFields,
  diffMaterialResearchFields,
  formatResearchSourceEvidencePart,
  buildResearchSourceSummary,
  buildResearchInventoryFields,
  findResearchInventoryByCompositeKey,
  listResearchInventoryForHotel,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
};
