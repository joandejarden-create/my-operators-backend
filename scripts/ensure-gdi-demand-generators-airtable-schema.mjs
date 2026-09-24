/**
 * Ensure Demand Generator V1 Airtable tables on canonical GDI base.
 *
 * Tables:
 *   - Demand Generators
 *   - Demand Programs
 *   - Hotel Demand Generator Fit
 *   - Demand Generator Signals
 *   - Group Demand Opportunities (extend link text fields)
 *
 * Usage:
 *   node scripts/ensure-gdi-demand-generators-airtable-schema.mjs
 *   node scripts/ensure-gdi-demand-generators-airtable-schema.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";
import { GDI_OPPORTUNITIES_TABLE_NAME } from "../lib/group-demand-intelligence/opportunity-field-map.js";
import {
  DEMAND_GENERATORS_TABLE_NAME,
  DEMAND_PROGRAMS_TABLE_NAME,
  HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
  DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
  MAP_DEMAND_GENERATOR,
  MAP_DEMAND_PROGRAM,
  MAP_HOTEL_GENERATOR_FIT,
  MAP_DEMAND_GENERATOR_SIGNAL,
  MAP_GDI_DG_LINK,
  VAL_ORGANIZATION_TYPE,
  VAL_GENERATOR_STATUS,
  VAL_PROGRAM_TYPE,
  VAL_RECURRENCE_STATUS,
  VAL_GENERATOR_PRIORITY,
  VAL_RELEVANCE_CLASS,
  VAL_SIGNAL_STATUS,
  VAL_TRIGGER_TYPE,
  VAL_SOURCE_AUTHORITY,
  VAL_FIT_BAND,
  DG_SCHEMA_VERSION,
} from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "demand-generators-v1"
);

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}
function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}
function checkbox(name, description) {
  const field = {
    name,
    type: "checkbox",
    options: { color: "greenBright", icon: "check" },
  };
  if (description) field.description = description;
  return field;
}
function dateTimeField(name, description) {
  const field = {
    name,
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  };
  if (description) field.description = description;
  return field;
}
function dateField(name, description) {
  const field = { name, type: "date", options: { dateFormat: { name: "iso" } } };
  if (description) field.description = description;
  return field;
}
function numberField(name, precision = 0, description) {
  const field = { name, type: "number", options: { precision } };
  if (description) field.description = description;
  return field;
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
function urlField(name, description) {
  const field = { name, type: "url" };
  if (description) field.description = description;
  return field;
}
function linkField(name, linkedTableId, description) {
  const field = {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
  if (description) field.description = description;
  return field;
}

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
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

function findTable(tables, nameOrId) {
  return (tables || []).find((t) => t.name === nameOrId || t.id === nameOrId) || null;
}

function existingFieldNames(table) {
  return new Set((table.fields || []).map((f) => f.name));
}

function generatorFields() {
  const G = MAP_DEMAND_GENERATOR;
  return [
    singleLine(G.demandGeneratorId, "Stable dg_… id (primary)"),
    singleLine(G.organizationName),
    multiline(G.organizationAliases),
    singleSelect(G.organizationType, VAL_ORGANIZATION_TYPE),
    singleLine(G.officialDomain),
    urlField(G.website),
    singleLine(G.headquartersLocation),
    multiline(G.primaryMarkets),
    singleLine(G.industry),
    singleSelect(G.generatorStatus, VAL_GENERATOR_STATUS),
    multiline(G.sourceUrls),
    singleSelect(G.sourceAuthority, VAL_SOURCE_AUTHORITY),
    dateTimeField(G.firstSeenAt),
    dateTimeField(G.lastSeenAt),
    dateTimeField(G.lastVerifiedAt),
    numberField(G.confidence, 2),
    dateTimeField(G.nextResearchAt),
    multiline(G.researchReason),
    multiline(G.lastResearchResult),
    dateTimeField(G.lastMaterialSignalAt),
    singleLine(G.schemaVersion),
    multiline(G.payloadJson),
  ];
}

function programFields(generatorTableId) {
  const P = MAP_DEMAND_PROGRAM;
  const fields = [
    singleLine(P.programId, "Stable dgp_… id (primary)"),
    singleLine(P.seriesId),
    singleLine(P.demandGeneratorId),
    singleLine(P.programName),
    multiline(P.programAliases),
    singleSelect(P.programType, VAL_PROGRAM_TYPE),
    singleLine(P.market),
    singleLine(P.typicalTiming),
    singleSelect(P.recurrenceStatus, VAL_RECURRENCE_STATUS),
    singleLine(P.recurrenceFrequency),
    multiline(P.historicalCyclesJson),
    multiline(P.confirmedFutureCycleJson),
    multiline(P.sourceUrls),
    dateTimeField(P.firstSeenAt),
    dateTimeField(P.lastSeenAt),
    dateTimeField(P.lastVerifiedAt),
    singleLine(P.schemaVersion),
    multiline(P.payloadJson),
  ];
  if (generatorTableId) {
    fields.splice(3, 0, linkField(P.generatorLink, generatorTableId));
  }
  return fields;
}

function fitFields(generatorTableId) {
  const F = MAP_HOTEL_GENERATOR_FIT;
  const fields = [
    singleLine(F.fitId, "Stable hdgf_… id (primary)"),
    singleLine(F.hotelId),
    singleLine(F.hotelName),
    singleLine(F.demandGeneratorId),
    singleLine(F.organizationName),
    singleSelect(F.marketRelevance, VAL_FIT_BAND),
    singleSelect(F.productFit, VAL_FIT_BAND),
    singleSelect(F.travelDemandPotential, VAL_FIT_BAND),
    singleLine(F.recurrencePotential),
    singleSelect(F.buyerAccessibility, VAL_FIT_BAND),
    singleSelect(F.generatorPriority, VAL_GENERATOR_PRIORITY),
    singleSelect(F.relevanceClass, VAL_RELEVANCE_CLASS),
    multiline(F.fitRationale),
    multiline(F.factorsJson),
    dateTimeField(F.lastEvaluatedAt),
    dateTimeField(F.firstEvaluatedAt),
    singleLine(F.schemaVersion),
    multiline(F.payloadJson),
  ];
  if (generatorTableId) {
    fields.splice(4, 0, linkField(F.generatorLink, generatorTableId));
  }
  return fields;
}

function signalFields(generatorTableId, programTableId, fitTableId, oppTableId) {
  const S = MAP_DEMAND_GENERATOR_SIGNAL;
  const fields = [
    singleLine(S.signalId, "Stable dgs_… id (primary)"),
    singleLine(S.demandGeneratorId),
    singleLine(S.programId),
    singleLine(S.seriesId),
    singleLine(S.cycleId),
    singleLine(S.hotelId),
    singleLine(S.hotelGeneratorFitId),
    singleSelect(S.triggerType, VAL_TRIGGER_TYPE),
    singleLine(S.title),
    dateField(S.eventStartDate),
    dateField(S.eventEndDate),
    singleLine(S.market),
    multiline(S.hotelDemandThesis),
    urlField(S.sourceUrl),
    multiline(S.sourceUrls),
    singleSelect(S.signalStatus, VAL_SIGNAL_STATUS),
    multiline(S.recommendedAction),
    singleLine(S.gdiOpportunityId),
    multiline(S.failReasonsJson),
    dateTimeField(S.firstSeenAt),
    dateTimeField(S.lastSeenAt),
    singleLine(S.schemaVersion),
    multiline(S.payloadJson),
  ];
  if (generatorTableId) fields.splice(2, 0, linkField(S.generatorLink, generatorTableId));
  if (programTableId) fields.splice(4, 0, linkField(S.programLink, programTableId));
  if (fitTableId) fields.splice(8, 0, linkField(S.fitLink, fitTableId));
  if (oppTableId) fields.push(linkField(S.gdiOpportunityLink, oppTableId));
  return fields;
}

async function ensureTable(baseId, token, name, description, fields, decision) {
  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`list_tables_failed ${listRes.status}`);
  const existing = findTable(listJson.tables, name);
  if (existing) {
    decision.tables[name] = { action: "REUSED", tableId: existing.id };
    const have = existingFieldNames(existing);
    const missing = fields.filter((f) => !have.has(f.name));
    if (missing.length && APPLY) {
      for (const field of missing) {
        const { res, json } = await metaFetch(
          baseId,
          token,
          `/tables/${existing.id}/fields`,
          { method: "POST", body: JSON.stringify(field) }
        );
        if (!res.ok) {
          decision.fieldErrors = decision.fieldErrors || [];
          decision.fieldErrors.push({ table: name, field: field.name, error: json });
        }
      }
    }
    decision.tables[name].missingFields = missing.map((f) => f.name);
    return existing;
  }
  decision.tables[name] = { action: APPLY ? "CREATE" : "WOULD_CREATE" };
  if (!APPLY) return null;
  const body = {
    name,
    description,
    fields: fields.length ? fields : [singleLine("Name")],
  };
  // Primary field must be first
  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    decision.tables[name].error = json;
    throw new Error(`create_table_failed ${name} ${res.status}`);
  }
  decision.tables[name].tableId = json.id;
  return json;
}

async function extendOppLinks(baseId, token, tables, decision) {
  const opp = findTable(tables, GDI_OPPORTUNITIES_TABLE_NAME);
  if (!opp) {
    decision.gdiOpportunityExtend = { action: "TABLE_MISSING" };
    return;
  }
  const have = existingFieldNames(opp);
  const textLinks = [
    singleLine(MAP_GDI_DG_LINK.dgGeneratorId, "Demand Generator stable id"),
    singleLine(MAP_GDI_DG_LINK.dgProgramId, "Demand Program stable id"),
    singleLine(MAP_GDI_DG_LINK.dgSignalId, "Demand Generator Signal stable id"),
    singleLine(MAP_GDI_DG_LINK.hotelGeneratorFitId, "Hotel Generator Fit stable id"),
  ];
  const gen = findTable(tables, DEMAND_GENERATORS_TABLE_NAME);
  const prog = findTable(tables, DEMAND_PROGRAMS_TABLE_NAME);
  const sig = findTable(tables, DEMAND_GENERATOR_SIGNALS_TABLE_NAME);
  if (gen?.id && !have.has(MAP_GDI_DG_LINK.dgGeneratorLink)) {
    textLinks.push(linkField(MAP_GDI_DG_LINK.dgGeneratorLink, gen.id));
  }
  if (prog?.id && !have.has(MAP_GDI_DG_LINK.dgProgramLink)) {
    textLinks.push(linkField(MAP_GDI_DG_LINK.dgProgramLink, prog.id));
  }
  if (sig?.id && !have.has(MAP_GDI_DG_LINK.dgSignalLink)) {
    textLinks.push(linkField(MAP_GDI_DG_LINK.dgSignalLink, sig.id));
  }
  const missing = textLinks.filter((f) => !have.has(f.name));
  decision.gdiOpportunityExtend = {
    action: missing.length ? (APPLY ? "EXTEND" : "WOULD_EXTEND") : "ALREADY_PRESENT",
    missing: missing.map((f) => f.name),
  };
  if (!APPLY || !missing.length) return;
  for (const field of missing) {
    const { res, json } = await metaFetch(baseId, token, `/tables/${opp.id}/fields`, {
      method: "POST",
      body: JSON.stringify(field),
    });
    if (!res.ok) {
      decision.fieldErrors = decision.fieldErrors || [];
      decision.fieldErrors.push({
        table: GDI_OPPORTUNITIES_TABLE_NAME,
        field: field.name,
        error: json,
      });
    }
  }
}

async function main() {
  const baseId = getGdiOpportunitiesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "gdi_demand_generators_schema" });
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
  if (!token) throw new Error("AIRTABLE_API_KEY required");

  const decision = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    schemaVersion: DG_SCHEMA_VERSION,
    tables: {},
  };

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`meta_list_failed ${res.status}`);
  let tables = json.tables || [];

  // Inspect reuse first
  for (const name of [
    DEMAND_GENERATORS_TABLE_NAME,
    DEMAND_PROGRAMS_TABLE_NAME,
    HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
    DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
  ]) {
    const t = findTable(tables, name);
    if (t) decision.tables[name] = { action: "EXISTS", tableId: t.id };
  }

  const genTable = await ensureTable(
    baseId,
    token,
    DEMAND_GENERATORS_TABLE_NAME,
    "Persistent organizations that repeatedly create hotel demand (research intelligence, not customer cards).",
    generatorFields(),
    decision
  );

  const { json: json2 } = await metaFetch(baseId, token, "/tables");
  tables = json2.tables || tables;
  const genId =
    genTable?.id || findTable(tables, DEMAND_GENERATORS_TABLE_NAME)?.id || null;

  await ensureTable(
    baseId,
    token,
    DEMAND_PROGRAMS_TABLE_NAME,
    "Programs / series under demand generators. Historical recurrence does not invent futures.",
    programFields(genId),
    decision
  );
  await ensureTable(
    baseId,
    token,
    HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
    "Hotel-specific relevance / fit for a demand generator.",
    fitFields(genId),
    decision
  );

  const { json: json3 } = await metaFetch(baseId, token, "/tables");
  tables = json3.tables || tables;
  const progId = findTable(tables, DEMAND_PROGRAMS_TABLE_NAME)?.id || null;
  const fitId = findTable(tables, HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME)?.id || null;
  const oppId = findTable(tables, GDI_OPPORTUNITIES_TABLE_NAME)?.id || null;

  await ensureTable(
    baseId,
    token,
    DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
    "Future triggers / signals from monitored generators. Trigger ≠ opportunity until lodging thesis qualifies.",
    signalFields(genId, progId, fitId, oppId),
    decision
  );

  const { json: json4 } = await metaFetch(baseId, token, "/tables");
  tables = json4.tables || tables;
  await extendOppLinks(baseId, token, tables, decision);

  // Final IDs
  decision.tableIds = {
    demandGenerators: findTable(tables, DEMAND_GENERATORS_TABLE_NAME)?.id || null,
    demandPrograms: findTable(tables, DEMAND_PROGRAMS_TABLE_NAME)?.id || null,
    hotelDemandGeneratorFit:
      findTable(tables, HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME)?.id || null,
    demandGeneratorSignals:
      findTable(tables, DEMAND_GENERATOR_SIGNALS_TABLE_NAME)?.id || null,
    gdiOpportunities: findTable(tables, GDI_OPPORTUNITIES_TABLE_NAME)?.id || null,
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, "SCHEMA_DECISION.json");
  fs.writeFileSync(outPath, JSON.stringify(decision, null, 2));
  console.log(JSON.stringify(decision, null, 2));
  console.log("WROTE", outPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
