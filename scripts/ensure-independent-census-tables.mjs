/**
 * Phase 2A — Create or verify independent census staging tables (schema only).
 *
 * Does NOT ingest data, query external sources, or modify Hotel Census / Brand Alias Mapping.
 * Does NOT delete or rename fields.
 *
 * Default: dry-run (report only). Use --apply to create missing tables/fields.
 * Requires INDEPENDENT_CENSUS_PIPELINE_ENABLED unless --force (when using --apply).
 *
 * Usage:
 *   node scripts/ensure-independent-census-tables.mjs
 *   node scripts/ensure-independent-census-tables.mjs --apply
 *   node scripts/ensure-independent-census-tables.mjs --apply --force
 *
 * Report: reports/independent-census-schema-check.json
 */
import "../load-env.js";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  CANDIDATES_TABLE,
  EVIDENCE_TABLE,
  VERIFIED_TABLE,
  CANDIDATE_FIELDS,
  EVIDENCE_FIELDS,
  VERIFIED_FIELDS,
  SOURCE_TYPES,
  REVIEW_STATUS,
  MATCH_CONFIDENCE,
  RECOMMENDED_ACTION,
  EVIDENCE_TYPES,
  RECONCILIATION_STATUS,
} from "../lib/independent-census/fields.js";
import { isIndependentCensusPipelineEnabled } from "../lib/independent-census/platform-base.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_PATH = join(__dirname, "..", "reports", "independent-census-schema-check.json");

/** Only these tables may receive schema POSTs from this script. */
const STAGING_TABLE_NAMES = new Set([CANDIDATES_TABLE, EVIDENCE_TABLE, VERIFIED_TABLE]);

/**
 * Link fields pointing at Hotel Census cannot be created via API without Airtable
 * adding an inverse link column on Hotel Census (schema change). Ops must add manually
 * if desired, or store census record ids in text fields in a later phase.
 */
const HOTEL_CENSUS_LINK_FIELDS = [
  { table: CANDIDATES_TABLE, field: CANDIDATE_FIELDS.possibleMatchInCurrentCensus },
  { table: EVIDENCE_TABLE, field: EVIDENCE_FIELDS.comparesToCensusRecord },
  { table: VERIFIED_TABLE, field: VERIFIED_FIELDS.linkedCensusRecord },
];

const BRAND_ALIAS_TABLE =
  process.env.AIRTABLE_BRAND_ALIAS_TABLE || "Brand Alias Mapping";

function parseArgs() {
  return {
    apply: process.argv.includes("--apply"),
    force: process.argv.includes("--force"),
  };
}

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function dateTimeField(name, description) {
  return {
    name,
    type: "dateTime",
    description,
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  };
}

/** Airtable Metadata API requires `options.precision` on number fields. */
function numberField(name, precision = 0, description) {
  const field = { name, type: "number", options: { precision } };
  if (description) field.description = description;
  return field;
}

function buildCandidatesScalarFields() {
  return [
    {
      name: CANDIDATE_FIELDS.sourceName,
      type: "singleLineText",
      description: "Dataset label (e.g. OpenStreetMap, Wikidata).",
    },
    {
      name: CANDIDATE_FIELDS.sourceType,
      type: "singleSelect",
      description: "Open/public source category.",
      options: choices(Object.values(SOURCE_TYPES)),
    },
    {
      name: CANDIDATE_FIELDS.sourceLicense,
      type: "singleLineText",
      description: "SPDX or license URL.",
    },
    { name: CANDIDATE_FIELDS.sourceUrl, type: "url", description: "Source record or dataset URL." },
    {
      name: CANDIDATE_FIELDS.sourceRecordId,
      type: "singleLineText",
      description: "External id (OSM node, Wikidata Q-id, registry id).",
    },
    { name: CANDIDATE_FIELDS.rawHotelName, type: "singleLineText" },
    { name: CANDIDATE_FIELDS.rawAddress, type: "singleLineText" },
    { name: CANDIDATE_FIELDS.rawCity, type: "singleLineText" },
    { name: CANDIDATE_FIELDS.rawCountry, type: "singleLineText" },
    numberField(CANDIDATE_FIELDS.rawLatitude, 6),
    numberField(CANDIDATE_FIELDS.rawLongitude, 6),
    { name: CANDIDATE_FIELDS.rawWebsite, type: "url" },
    { name: CANDIDATE_FIELDS.rawPhone, type: "singleLineText" },
    { name: CANDIDATE_FIELDS.rawBrand, type: "singleLineText" },
    {
      name: CANDIDATE_FIELDS.rawPayloadJson,
      type: "multilineText",
      description: "Full source JSON payload.",
    },
    { name: CANDIDATE_FIELDS.importBatchId, type: "singleLineText" },
    dateTimeField(CANDIDATE_FIELDS.importedAt, "When this candidate row was imported."),
    {
      name: CANDIDATE_FIELDS.reviewStatus,
      type: "singleSelect",
      options: choices(Object.values(REVIEW_STATUS)),
    },
    {
      name: CANDIDATE_FIELDS.possibleMatchConfidence,
      type: "singleSelect",
      options: choices(Object.values(MATCH_CONFIDENCE)),
    },
    {
      name: CANDIDATE_FIELDS.recommendedAction,
      type: "singleSelect",
      options: choices(Object.values(RECOMMENDED_ACTION)),
    },
    {
      name: CANDIDATE_FIELDS.candidateDedupeKey,
      type: "singleLineText",
      description: "Normalized name|city|country or geohash.",
    },
  ];
}

function buildEvidenceScalarFields() {
  return [
    {
      name: "Name",
      type: "singleLineText",
      description: "Airtable primary field; optional short label for this evidence row.",
    },
    {
      name: EVIDENCE_FIELDS.evidenceType,
      type: "singleSelect",
      options: choices(Object.values(EVIDENCE_TYPES)),
    },
    { name: EVIDENCE_FIELDS.evidenceUrl, type: "url" },
    { name: EVIDENCE_FIELDS.evidenceText, type: "multilineText" },
    dateTimeField(EVIDENCE_FIELDS.capturedAt, "When evidence was captured."),
    { name: EVIDENCE_FIELDS.capturedBy, type: "singleLineText" },
    numberField(EVIDENCE_FIELDS.matchScore, 0, "Match score 0–100 for census comparison."),
    { name: EVIDENCE_FIELDS.matchReason, type: "multilineText" },
  ];
}

function buildVerifiedScalarFields() {
  return [
    { name: VERIFIED_FIELDS.verifiedHotelName, type: "singleLineText" },
    { name: VERIFIED_FIELDS.verifiedAddress, type: "singleLineText" },
    { name: VERIFIED_FIELDS.verifiedCity, type: "singleLineText" },
    { name: VERIFIED_FIELDS.verifiedState, type: "singleLineText" },
    { name: VERIFIED_FIELDS.verifiedCountry, type: "singleLineText" },
    { name: VERIFIED_FIELDS.verifiedPostalCode, type: "singleLineText" },
    numberField(VERIFIED_FIELDS.verifiedLatitude, 6),
    numberField(VERIFIED_FIELDS.verifiedLongitude, 6),
    { name: VERIFIED_FIELDS.verifiedWebsite, type: "url" },
    { name: VERIFIED_FIELDS.verifiedPhone, type: "singleLineText" },
    {
      name: VERIFIED_FIELDS.verifiedBrandLabel,
      type: "singleLineText",
      description: "Open-source brand label; not Hotel Census Affiliation.",
    },
    dateTimeField(VERIFIED_FIELDS.approvedAt, "Human approval timestamp."),
    { name: VERIFIED_FIELDS.approvedBy, type: "singleLineText" },
    { name: VERIFIED_FIELDS.approvalNotes, type: "multilineText" },
    {
      name: VERIFIED_FIELDS.censusReconciliationStatus,
      type: "singleSelect",
      options: choices(Object.values(RECONCILIATION_STATUS)),
    },
    { name: VERIFIED_FIELDS.verifiedDedupeKey, type: "singleLineText" },
    {
      name: VERIFIED_FIELDS.active,
      type: "checkbox",
      options: { icon: "check", color: "greenBright" },
    },
  ];
}

const TABLE_SPECS = [
  {
    key: "candidates",
    name: CANDIDATES_TABLE,
    description:
      "Staging ingest for open/public hotel sources. Never auto-promoted. Never writes Hotel Census.",
    scalarFields: buildCandidatesScalarFields,
    stagingLinkFields: (ids) => [
      {
        name: CANDIDATE_FIELDS.linkedEvidence,
        type: "multipleRecordLinks",
        options: { linkedTableId: ids.evidence },
        description: "Optional evidence rows for this candidate.",
      },
    ],
  },
  {
    key: "evidence",
    name: EVIDENCE_TABLE,
    description: "Audit trail for independent census candidate review.",
    scalarFields: buildEvidenceScalarFields,
    stagingLinkFields: (ids) => [
      {
        name: EVIDENCE_FIELDS.candidate,
        type: "multipleRecordLinks",
        options: { linkedTableId: ids.candidates },
        description: "Parent candidate row.",
      },
    ],
  },
  {
    key: "verified",
    name: VERIFIED_TABLE,
    description:
      "Human-approved independent census records. Promotion requires explicit approval.",
    scalarFields: buildVerifiedScalarFields,
    stagingLinkFields: (ids) => [
      {
        name: VERIFIED_FIELDS.primarySourceCandidate,
        type: "multipleRecordLinks",
        options: { linkedTableId: ids.candidates },
        description: "Source candidate promoted to verified.",
      },
    ],
  },
];

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
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

function findTable(tables, name) {
  return (tables || []).find((t) => t.name === name);
}

function fieldNameSet(table) {
  return new Set((table?.fields || []).map((f) => f.name));
}

function assertStagingTableName(tableName) {
  if (!STAGING_TABLE_NAMES.has(tableName)) {
    throw new Error(`Refusing to modify non-staging table: ${tableName}`);
  }
}

function assertStagingTableId(tableId, tableNameById) {
  const name = tableNameById.get(tableId);
  if (!name || !STAGING_TABLE_NAMES.has(name)) {
    throw new Error(`Refusing to POST to non-staging table id: ${tableId}`);
  }
}

async function createTable(baseId, token, spec, dryRun, tableReport) {
  assertStagingTableName(spec.name);
  const fields = spec.scalarFields();
  if (!fields.length) {
    throw new Error(`Table "${spec.name}" has no scalar fields defined`);
  }

  if (dryRun) {
    console.log(`[dry-run] Would create table "${spec.name}" with ${fields.length} scalar fields.`);
    tableReport.wouldCreateTable = true;
    tableReport.wouldCreateFields.push(...fields.map((f) => f.name));
    return null;
  }

  const body = {
    name: spec.name,
    description: spec.description,
    fields,
  };
  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`Create table "${spec.name}" failed ${res.status}: ${JSON.stringify(json)}`);
  }
  tableReport.createdTable = true;
  tableReport.tableId = json.id;
  console.log(`Created table "${spec.name}" (${json.id}).`);
  return json;
}

async function addField(baseId, token, tableId, tableName, fieldDef, dryRun, tableReport) {
  assertStagingTableName(tableName);
  assertStagingTableId(tableId, tableReport._nameById);

  if (dryRun) {
    console.log(`[dry-run] Would add field "${fieldDef.name}" on "${tableName}"`);
    tableReport.wouldCreateFields.push(fieldDef.name);
    return;
  }

  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldDef),
  });
  if (!res.ok) {
    throw new Error(
      `Add field "${fieldDef.name}" on "${tableName}" failed ${res.status}: ${JSON.stringify(json)}`
    );
  }
  tableReport.fieldsCreated.push(fieldDef.name);
  console.log(`Added field "${fieldDef.name}" on "${tableName}" (${json.id}).`);
}

async function ensureScalarFields(baseId, token, table, spec, dryRun, tableReport) {
  const existing = fieldNameSet(table);
  const expected = spec.scalarFields();

  for (const fieldDef of expected) {
    if (existing.has(fieldDef.name)) {
      tableReport.fieldsPresent.push(fieldDef.name);
      continue;
    }
    tableReport.missingFields.push({ field: fieldDef.name, kind: "scalar" });
    await addField(baseId, token, table.id, spec.name, fieldDef, dryRun, tableReport);
  }

  const expectedNames = new Set(expected.map((f) => f.name));
  for (const name of existing) {
    if (!expectedNames.has(name)) {
      tableReport.extraFields.push(name);
    }
  }
}

async function ensureStagingLinkFields(baseId, token, table, spec, stagingIds, dryRun, tableReport) {
  if (!table?.id) return;
  const existing = fieldNameSet(table);
  const linkDefs = spec.stagingLinkFields(stagingIds);

  for (const fieldDef of linkDefs) {
    if (existing.has(fieldDef.name)) {
      tableReport.fieldsPresent.push(fieldDef.name);
      continue;
    }
    tableReport.missingFields.push({ field: fieldDef.name, kind: "staging_link" });
    await addField(baseId, token, table.id, spec.name, fieldDef, dryRun, tableReport);
  }
}

async function main() {
  const { apply, force } = parseArgs();
  const dryRun = !apply;

  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!token) throw new Error("Set AIRTABLE_API_KEY (schema.bases:read and schema.bases:write)");
  if (!baseId) throw new Error("Set AIRTABLE_BASE_ID_ALT");

  if (apply && !isIndependentCensusPipelineEnabled() && !force) {
    throw new Error(
      "INDEPENDENT_CENSUS_PIPELINE_ENABLED is false. Use --force to apply schema anyway, or enable the flag in .env."
    );
  }

  console.log("=== Independent census schema check (Phase 2A) ===\n");
  console.log(`Mode: ${dryRun ? "DRY-RUN (no Airtable changes)" : "APPLY"}`);
  console.log(`Pipeline enabled: ${isIndependentCensusPipelineEnabled()}`);
  console.log(`Staging tables only: ${[...STAGING_TABLE_NAMES].join(", ")}\n`);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "apply",
    baseId,
    pipelineEnabled: isIndependentCensusPipelineEnabled(),
    force,
    protectedTables: {
      hotelCensus: HOTEL_CENSUS_TABLE,
      brandAliasMapping: BRAND_ALIAS_TABLE,
      hotelCensusWrites: false,
      brandAliasWrites: false,
    },
    tables: {},
    hotelCensusLinkFieldsManualOnly: HOTEL_CENSUS_LINK_FIELDS.map(({ table, field }) => ({
      table,
      field,
      reason:
        "Airtable API link creation adds an inverse field on the linked table; auto-create would modify Hotel Census schema.",
    })),
    summary: {
      tablesMissing: [],
      tablesPresent: [],
      totalMissingScalarFields: 0,
      totalWouldCreate: 0,
      totalCreated: 0,
    },
  };

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);
  }

  const tables = listJson.tables || [];
  const tableNameById = new Map(tables.map((t) => [t.id, t.name]));

  const hotelCensus = findTable(tables, HOTEL_CENSUS_TABLE);
  if (hotelCensus) {
    report.protectedTables.hotelCensusTableId = hotelCensus.id;
    report.protectedTables.hotelCensusFieldCount = (hotelCensus.fields || []).length;
    console.log(`Found "${HOTEL_CENSUS_TABLE}" (${hotelCensus.id}) — read-only reference, no schema writes.`);
  } else {
    console.warn(`Warning: "${HOTEL_CENSUS_TABLE}" not found in base (census link fields are manual-only anyway).`);
  }

  const brandAlias = findTable(tables, BRAND_ALIAS_TABLE);
  if (brandAlias) {
    report.protectedTables.brandAliasTableId = brandAlias.id;
    console.log(`Found "${BRAND_ALIAS_TABLE}" (${brandAlias.id}) — not modified.`);
  }

  const stagingIds = { candidates: null, evidence: null, verified: null };

  for (const spec of TABLE_SPECS) {
    const tableReport = {
      tableName: spec.name,
      tableId: null,
      exists: false,
      createdTable: false,
      wouldCreateTable: false,
      fieldsPresent: [],
      missingFields: [],
      fieldsCreated: [],
      wouldCreateFields: [],
      extraFields: [],
      _nameById: tableNameById,
    };
    report.tables[spec.key] = tableReport;

    let table = findTable(tables, spec.name);
    if (table) {
      tableReport.exists = true;
      tableReport.tableId = table.id;
      stagingIds[spec.key] = table.id;
      report.summary.tablesPresent.push(spec.name);
      console.log(`\nTable "${spec.name}" exists (${table.id}).`);
      await ensureScalarFields(baseId, token, table, spec, dryRun, tableReport);
    } else {
      report.summary.tablesMissing.push(spec.name);
      console.log(`\nTable "${spec.name}" missing.`);
      const created = await createTable(baseId, token, spec, dryRun, tableReport);
      if (created) {
        table = created;
        stagingIds[spec.key] = created.id;
        tableNameById.set(created.id, spec.name);
        tables.push(created);
        for (const f of spec.scalarFields()) tableReport.fieldsPresent.push(f.name);
      }
    }
  }

  // Staging link fields (second pass — needs table ids)
  const idsReady = stagingIds.candidates && stagingIds.evidence && stagingIds.verified;
  if (!idsReady && !dryRun) {
    console.warn("\nSkipping staging link fields: not all staging table ids are available.");
  } else if (!idsReady && dryRun) {
    console.log("\n[dry-run] Staging link fields deferred until all three tables exist.");
    for (const spec of TABLE_SPECS) {
      const tr = report.tables[spec.key];
      for (const fieldDef of spec.stagingLinkFields({
        candidates: stagingIds.candidates || "tbl_candidates_pending",
        evidence: stagingIds.evidence || "tbl_evidence_pending",
        verified: stagingIds.verified || "tbl_verified_pending",
      })) {
        if (!tr.exists) continue;
        const table = findTable(tables, spec.name);
        if (table && !fieldNameSet(table).has(fieldDef.name)) {
          tr.missingFields.push({ field: fieldDef.name, kind: "staging_link" });
          tr.wouldCreateFields.push(fieldDef.name);
        }
      }
    }
  } else {
    console.log("\n--- Staging link fields (Candidates ↔ Evidence, Verified → Candidates) ---");
    for (const spec of TABLE_SPECS) {
      const table = findTable(tables, spec.name);
      const tr = report.tables[spec.key];
      if (table) {
        await ensureStagingLinkFields(baseId, token, table, spec, stagingIds, dryRun, tr);
      }
    }
  }

  // Hotel Census link fields — report only, never auto-create
  console.log("\n--- Hotel Census link fields (manual setup only) ---");
  for (const { table, field } of HOTEL_CENSUS_LINK_FIELDS) {
    const t = findTable(tables, table);
    const present = t && fieldNameSet(t).has(field);
    const entry = report.hotelCensusLinkFieldsManualOnly.find((e) => e.field === field);
    entry.present = !!present;
    console.log(
      `  ${table} → "${field}": ${present ? "present" : "missing (add manually in Airtable if needed)"}`
    );
  }

  for (const spec of TABLE_SPECS) {
    const tr = report.tables[spec.key];
    report.summary.totalMissingScalarFields += tr.missingFields.filter((m) => m.kind === "scalar").length;
    report.summary.totalWouldCreate += tr.wouldCreateFields.length;
    report.summary.totalCreated += tr.fieldsCreated.length + (tr.createdTable ? 1 : 0);
  }

  writeJson(REPORT_PATH, report);

  console.log(`\nReport: ${REPORT_PATH}`);
  console.log(JSON.stringify(report.summary, null, 2));

  if (dryRun) {
    console.log("\nDry-run complete — no Airtable changes. Use --apply to create missing schema.");
  } else {
    console.log("\nApply complete — only independent staging tables may have been modified.");
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
