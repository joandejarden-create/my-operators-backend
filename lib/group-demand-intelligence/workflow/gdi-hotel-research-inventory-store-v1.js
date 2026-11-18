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
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
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

  const sources = Array.isArray(opp.sources) ? opp.sources : [];
  const sourceSummary =
    text(opp.sourceSummary) ||
    sources
      .slice(0, 5)
      .map((s) => text(s.url || s.sourceUrl || s))
      .filter(Boolean)
      .join(" | ") ||
    null;

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

  if (dryRun) {
    return {
      ok: true,
      action: existing ? "WOULD_UPDATE" : "WOULD_CREATE",
      compositeKey: built.compositeKey,
      hotelId: built.hotelId,
      opportunityId: built.opportunityId,
      airtableRecordId: existing?.id || null,
      fields,
      payloadLength: built.payloadLength,
    };
  }

  if (existing) {
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
  serializeResearchPayload,
  buildResearchInventoryFields,
  findResearchInventoryByCompositeKey,
  listResearchInventoryForHotel,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
};
