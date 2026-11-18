/**
 * Ensure GDI Hotel Research Opportunities table (founder-approved schema).
 * Base: appa2cE7FTRmIbB32. Does not modify GDO or other tables.
 */

import {
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
  GDI_HOTEL_RESEARCH_PRIMARY_FIELD,
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
  VAL_RESEARCH_LIFECYCLE_STATUS,
  VAL_COMMERCIAL_ELIGIBILITY_STATUS,
} from "../hotel-research-opportunity-field-map.js";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../../decision-outcomes/airtable-base.js";

export const GDI_HOTEL_RESEARCH_OPPORTUNITIES_SCHEMA_V1 =
  "gdi-hotel-research-opportunities-schema-v1";

export const APPROVED_BASE_ID = CANONICAL_INTELLIGENCE_BASE_ID;

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function singleLine(name, description) {
  const field = { name, type: "singleLineText" };
  if (description) field.description = description;
  return field;
}

function multiline(name, description) {
  const field = { name, type: "multilineText" };
  if (description) field.description = description;
  return field;
}

function singleSelect(name, optionNames, description) {
  const field = {
    name,
    type: "singleSelect",
    options: choices(optionNames),
  };
  if (description) field.description = description;
  return field;
}

export function buildResearchOpportunityFields() {
  return [
    singleLine(
      F.compositeKey,
      "Primary identity = hotelId::opportunityId (adapter-enforced unique).",
    ),
    singleLine(F.opportunityId, "Canonical opportunity id (hotel-scoped)."),
    singleLine(F.hotelId, "Canonical Airtable Hotels record id."),
    singleLine(F.organizationName),
    singleSelect(
      F.researchLifecycleStatus,
      VAL_RESEARCH_LIFECYCLE_STATUS,
      "Research inventory lifecycle — not GDO qualification.",
    ),
    singleSelect(
      F.commercialEligibilityStatus,
      VAL_COMMERCIAL_ELIGIBILITY_STATUS,
      "Commercial eligibility snapshot; research may remain RESEARCH_ONLY.",
    ),
    singleLine(F.demandThesis),
    singleLine(F.priority),
    singleLine(F.title),
    singleLine(F.segment),
    multiline(F.sourceSummary),
    singleLine(F.firstDiscoveredAt, "ISO timestamp; preserved across updates."),
    singleLine(F.firstDiscoveredRunId),
    singleLine(F.lastResearchedAt),
    singleLine(F.lastMaterialChangeAt),
    singleLine(F.dgGeneratorId, "Shared Demand Generator identity when known."),
    multiline(
      F.researchPayloadJson,
      "Complete supported research JSON. Oversized payloads fail closed (no truncate).",
    ),
    singleLine(F.schemaVersion),
  ];
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

/**
 * Create or verify the research inventory table.
 * @param {{ apply?: boolean }} opts
 */
export async function ensureHotelResearchOpportunitiesSchema(opts = {}) {
  const apply = opts.apply === true;
  const token = getToken();
  const baseId = getGdiOpportunitiesAirtableBaseId() || APPROVED_BASE_ID;
  if (!token) {
    const err = new Error("AIRTABLE_API_KEY / AIRTABLE_PAT required");
    err.code = "AIRTABLE_TOKEN_MISSING";
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, {
    surface: "gdi_hotel_research_opportunities_schema",
  });
  if (baseId !== APPROVED_BASE_ID) {
    const err = new Error(
      `base_mismatch: expected ${APPROVED_BASE_ID}, got ${baseId}`,
    );
    err.code = "WRONG_AIRTABLE_BASE";
    throw err;
  }

  const report = {
    version: GDI_HOTEL_RESEARCH_OPPORTUNITIES_SCHEMA_V1,
    mode: apply ? "apply" : "dry-run",
    baseId,
    tableName: GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
    primaryField: GDI_HOTEL_RESEARCH_PRIMARY_FIELD,
    tableId: null,
    createdTable: false,
    wouldCreateTable: false,
    fields: [],
    fieldIdsByName: {},
    errors: [],
  };

  const { res: listRes, json: listJson } = await metaFetch(
    baseId,
    token,
    "/tables",
  );
  if (!listRes.ok) {
    report.errors.push({ listTables: true, status: listRes.status, error: listJson });
    return { ok: false, report };
  }

  const tables = listJson.tables || [];
  let table = tables.find(
    (t) => t.name === GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
  );
  const allFields = buildResearchOpportunityFields();

  if (!table) {
    if (!apply) {
      report.wouldCreateTable = true;
      report.fields = allFields.map((f) => ({ name: f.name, type: f.type }));
      return { ok: true, report };
    }
    const { res, json } = await metaFetch(baseId, token, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
        description:
          "Durable hotel research inventory (exploratory/watchlist/continuation). Not qualified GDO. Not customer publication.",
        fields: allFields,
      }),
    });
    if (!res.ok) {
      report.errors.push({ createTable: true, status: res.status, error: json });
      return { ok: false, report };
    }
    table = json;
    report.createdTable = true;
  }

  report.tableId = table.id;
  for (const f of table.fields || []) {
    report.fieldIdsByName[f.name] = f.id;
    report.fields.push({ name: f.name, id: f.id, type: f.type });
  }

  // Add any missing fields if table pre-existed
  const existing = new Set((table.fields || []).map((f) => f.name));
  for (const field of allFields) {
    if (existing.has(field.name)) continue;
    if (!apply) {
      report.fields.push({ name: field.name, type: field.type, wouldCreate: true });
      continue;
    }
    const { res, json } = await metaFetch(
      baseId,
      token,
      `/tables/${table.id}/fields`,
      { method: "POST", body: JSON.stringify(field) },
    );
    if (!res.ok) {
      report.errors.push({
        createField: field.name,
        status: res.status,
        error: json,
      });
      continue;
    }
    report.fieldIdsByName[field.name] = json.id;
    report.fields.push({
      name: field.name,
      id: json.id,
      type: field.type,
      created: true,
    });
    await new Promise((r) => setTimeout(r, 220));
  }

  return { ok: report.errors.length === 0, report };
}

export default {
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_SCHEMA_V1,
  APPROVED_BASE_ID,
  buildResearchOpportunityFields,
  ensureHotelResearchOpportunitiesSchema,
};
