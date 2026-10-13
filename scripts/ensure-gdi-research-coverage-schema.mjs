/**
 * Ensure GDI Research Coverage V1 tables on canonical base appa2cE7FTRmIbB32.
 *
 * Tables:
 *   - GDI Research Targets
 *   - GDI Research Runs
 *   - GDI Research Target Runs
 * Extends Group Demand Opportunities with provenance text fields.
 *
 * Usage:
 *   node scripts/ensure-gdi-research-coverage-schema.mjs
 *   node scripts/ensure-gdi-research-coverage-schema.mjs --apply
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
} from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import { PE_VENUES_TABLE_NAME } from "../lib/group-demand-intelligence/private-events/airtable-field-map.js";
import {
  RESEARCH_TARGETS_TABLE_NAME,
  RESEARCH_RUNS_TABLE_NAME,
  RESEARCH_TARGET_RUNS_TABLE_NAME,
  RESEARCH_COVERAGE_VERSION,
  MAP_RESEARCH_TARGET,
  MAP_RESEARCH_RUN,
  MAP_RESEARCH_TARGET_RUN,
  MAP_GDI_PROVENANCE_EXTEND,
  VAL_TARGET_TYPE,
  VAL_TARGET_STATUS,
  VAL_TARGET_PRIORITY,
  VAL_RESEARCH_CADENCE,
  VAL_RUN_TYPE,
  VAL_RUN_STATUS,
  VAL_EXECUTION_STATUS,
  VAL_RESULT_TYPE,
} from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "research-coverage-v1"
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

function targetFields(genId, progId, venueId) {
  const T = MAP_RESEARCH_TARGET;
  const fields = [
    singleLine(T.targetId, "Stable gdirt_… id (primary)"),
    singleLine(T.hotelId),
    singleLine(T.hotelName),
    singleSelect(T.targetType, VAL_TARGET_TYPE),
    singleLine(T.entityType),
    singleLine(T.canonicalName),
    singleLine(T.entityId),
    singleLine(T.programId),
    singleLine(T.venueId),
    singleLine(T.organizationId),
    singleLine(T.seriesId),
    singleLine(T.officialDomain),
    urlField(T.primarySourceUrl),
    singleLine(T.researchPlaybook),
    singleSelect(T.priority, VAL_TARGET_PRIORITY),
    singleSelect(T.status, VAL_TARGET_STATUS),
    multiline(T.reasonMonitored),
    singleSelect(T.researchCadence, VAL_RESEARCH_CADENCE),
    dateTimeField(T.nextResearchAt),
    dateTimeField(T.lastResearchedAt),
    dateTimeField(T.lastSuccessfulResearchAt),
    dateTimeField(T.lastMaterialChangeAt),
    singleLine(T.lastResult),
    singleLine(T.lastRunId),
    numberField(T.consecutiveNoChangeRuns, 0),
    multiline(T.sourceFamilies),
    dateTimeField(T.firstSeenAt),
    dateTimeField(T.createdAt),
    dateTimeField(T.updatedAt),
    numberField(T.confidence, 2),
    numberField(T.signalsFound, 0),
    numberField(T.opportunitiesCreated, 0),
    numberField(T.opportunitiesUpdated, 0),
    numberField(T.successfulRuns, 0),
    numberField(T.noChangeRuns, 0),
    singleLine(T.schemaVersion),
    multiline(T.payloadJson),
  ];
  if (genId) fields.splice(11, 0, linkField(T.demandGeneratorLink, genId));
  if (progId) fields.splice(12, 0, linkField(T.demandProgramLink, progId));
  if (venueId) fields.splice(13, 0, linkField(T.privateEventVenueLink, venueId));
  return fields;
}

function runFields() {
  const R = MAP_RESEARCH_RUN;
  return [
    singleLine(R.runId, "Stable gdir_… id (primary)"),
    singleLine(R.hotelId),
    singleLine(R.hotelName),
    singleSelect(R.runType, VAL_RUN_TYPE),
    dateTimeField(R.startedAt),
    dateTimeField(R.completedAt),
    singleSelect(R.status, VAL_RUN_STATUS),
    numberField(R.targetsDue, 0),
    numberField(R.targetsAttempted, 0),
    numberField(R.targetsCompleted, 0),
    numberField(R.targetsMissed, 0),
    numberField(R.targetsFailed, 0),
    numberField(R.targetsNoChange, 0),
    numberField(R.newSignals, 0),
    numberField(R.updatedSignals, 0),
    numberField(R.newOpportunities, 0),
    numberField(R.updatedOpportunities, 0),
    numberField(R.queries, 0),
    numberField(R.fetches, 0),
    numberField(R.estimatedCost, 2),
    singleLine(R.codeVersion),
    singleLine(R.gitSha),
    singleLine(R.researchVersion),
    multiline(R.notes),
    singleLine(R.schemaVersion),
    multiline(R.payloadJson),
  ];
}

function targetRunFields(runTableId, targetTableId) {
  const TR = MAP_RESEARCH_TARGET_RUN;
  const fields = [
    singleLine(TR.targetRunId, "Stable gditr_… id (primary)"),
    singleLine(TR.hotelId),
    singleLine(TR.targetId),
    singleLine(TR.runId),
    dateTimeField(TR.scheduledAt),
    dateTimeField(TR.startedAt),
    dateTimeField(TR.completedAt),
    singleSelect(TR.executionStatus, VAL_EXECUTION_STATUS),
    singleSelect(TR.resultType, VAL_RESULT_TYPE),
    numberField(TR.queriesUsed, 0),
    numberField(TR.fetchesUsed, 0),
    numberField(TR.sourceCount, 0),
    numberField(TR.officialSourceCount, 0),
    checkbox(TR.materialChange),
    multiline(TR.failureReason),
    numberField(TR.newSignalCount, 0),
    numberField(TR.updatedSignalCount, 0),
    numberField(TR.newOpportunityCount, 0),
    numberField(TR.updatedOpportunityCount, 0),
    multiline(TR.lastResultSummary),
    multiline(TR.sourceUrls),
    singleLine(TR.schemaVersion),
    multiline(TR.payloadJson),
  ];
  if (runTableId) fields.splice(1, 0, linkField(TR.researchRunLink, runTableId));
  if (targetTableId) fields.splice(2, 0, linkField(TR.researchTargetLink, targetTableId));
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
  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify({ name, description, fields }),
  });
  if (!res.ok) {
    decision.tables[name].error = json;
    throw new Error(`create_table_failed ${name} ${res.status}`);
  }
  decision.tables[name].tableId = json.id;
  return json;
}

async function extendOppProvenance(baseId, token, tables, decision) {
  const opp = findTable(tables, GDI_OPPORTUNITIES_TABLE_NAME);
  if (!opp) {
    decision.gdiOpportunityExtend = { action: "TABLE_MISSING" };
    return;
  }
  const have = existingFieldNames(opp);
  const toAdd = Object.values(MAP_GDI_PROVENANCE_EXTEND)
    .filter((name) => !have.has(name))
    .map((name) => singleLine(name, "GDI research coverage provenance"));
  decision.gdiOpportunityExtend = {
    action: toAdd.length ? (APPLY ? "EXTEND" : "WOULD_EXTEND") : "OK",
    missing: toAdd.map((f) => f.name),
  };
  if (!APPLY || !toAdd.length) return;
  for (const field of toAdd) {
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
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
  const baseId = getGdiOpportunitiesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "ensure_gdi_research_coverage" });
  if (!token) throw new Error("AIRTABLE_API_KEY / AIRTABLE_PAT required");

  const decision = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    schemaVersion: RESEARCH_COVERAGE_VERSION,
    tables: {},
  };

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`list_tables ${listRes.status}`);
  let tables = listJson.tables || [];

  const gen = findTable(tables, DEMAND_GENERATORS_TABLE_NAME);
  const prog = findTable(tables, DEMAND_PROGRAMS_TABLE_NAME);
  const venue = findTable(tables, PE_VENUES_TABLE_NAME);

  const targetsTable = await ensureTable(
    baseId,
    token,
    RESEARCH_TARGETS_TABLE_NAME,
    "Hotel-scoped GDI research monitoring registry (V1)",
    targetFields(gen?.id, prog?.id, venue?.id),
    decision
  );

  const runsTable = await ensureTable(
    baseId,
    token,
    RESEARCH_RUNS_TABLE_NAME,
    "Immutable GDI research run ledger (V1)",
    runFields(),
    decision
  );

  // refresh table list for links
  const { json: list2 } = await metaFetch(baseId, token, "/tables");
  tables = list2.tables || tables;
  const targetsId =
    targetsTable?.id ||
    findTable(tables, RESEARCH_TARGETS_TABLE_NAME)?.id ||
    decision.tables[RESEARCH_TARGETS_TABLE_NAME]?.tableId;
  const runsId =
    runsTable?.id ||
    findTable(tables, RESEARCH_RUNS_TABLE_NAME)?.id ||
    decision.tables[RESEARCH_RUNS_TABLE_NAME]?.tableId;

  await ensureTable(
    baseId,
    token,
    RESEARCH_TARGET_RUNS_TABLE_NAME,
    "Per-target research execution audit (target × run)",
    targetRunFields(runsId, targetsId),
    decision
  );

  const { json: list3 } = await metaFetch(baseId, token, "/tables");
  await extendOppProvenance(baseId, token, list3.tables || [], decision);

  decision.tableIds = {
    researchTargets: decision.tables[RESEARCH_TARGETS_TABLE_NAME]?.tableId || null,
    researchRuns: decision.tables[RESEARCH_RUNS_TABLE_NAME]?.tableId || null,
    researchTargetRuns: decision.tables[RESEARCH_TARGET_RUNS_TABLE_NAME]?.tableId || null,
    gdiOpportunities: findTable(list3.tables, GDI_OPPORTUNITIES_TABLE_NAME)?.id || null,
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, "SCHEMA_DECISION.json");
  fs.writeFileSync(outPath, JSON.stringify(decision, null, 2));
  console.log(JSON.stringify({ ok: true, apply: APPLY, outPath, decision }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
