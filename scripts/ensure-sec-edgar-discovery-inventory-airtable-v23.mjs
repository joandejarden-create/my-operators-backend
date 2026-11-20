#!/usr/bin/env node
/**
 * SEC EDGAR V2.3 — ensure HI Hotel Discovery Inventory + HI Discovery Findings
 * on canonical HPC base appCCUsuGsE1ifoLk.
 *
 *   node scripts/ensure-sec-edgar-discovery-inventory-airtable-v23.mjs --dry-run
 *   node scripts/ensure-sec-edgar-discovery-inventory-airtable-v23.mjs --apply
 *
 * Does NOT mutate HPC core columns, HI Graph Evidence, or ADP evidence tables.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  DISCOVERY_INVENTORY_CONTRACT_V23,
  DISCOVERY_SCHEMA_VERSION,
  GRAPH_BASE_ID_CANONICAL,
  GRAPH_EVIDENCE_REUSE_DECISION,
  HPC_TABLE_ID,
  HI_ORGANIZATIONS_TABLE_ID,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  IDENTITY_CLASS_OPTIONS,
  BRAND_FAMILY_OPTIONS,
  EVIDENCE_CONFIDENCE_OPTIONS,
  HPC_MATCH_STATUS_OPTIONS,
  LIFECYCLE_STATUS_OPTIONS,
  PERSISTENCE_STATUS_OPTIONS,
  PUBLICATION_STATE_OPTIONS,
  FINDING_TYPE_OPTIONS,
  AGREEMENT_TYPE_OPTIONS,
  HISTORICAL_APPLICABILITY_OPTIONS,
  FINDING_VERIFICATION_OPTIONS,
  HPC_PROMOTION_STATUS_OPTIONS,
  assertCanonicalDiscoveryBase,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [
  path.join(ROOT, ".env"),
  "C:/Dev/dealality/my-operators-backend/.env",
]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}
function singleSelect(name, optionNames) {
  return { name, type: "singleSelect", options: { choices: choices(optionNames) } };
}
function checkbox(name) {
  return { name, type: "checkbox", options: { icon: "check", color: "greenBright" } };
}
function dateField(name) {
  return { name, type: "date", options: { dateFormat: { name: "iso" } } };
}
function dateTimeField(name) {
  return {
    name,
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  };
}
function linkField(name, tableId) {
  return {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId: tableId },
  };
}

async function metaFetch(baseId, token, pathSuffix, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${pathSuffix}`;
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

function inventoryFields() {
  const M = MAP_DISCOVERY_INVENTORY;
  return [
    { name: M.discoveryPropertyId, type: "singleLineText" },
    { name: M.hotelNameNormalized, type: "singleLineText" },
    { name: M.hotelNameRaw, type: "singleLineText" },
    { name: M.historicalAliases, type: "multilineText" },
    { name: M.city, type: "singleLineText" },
    { name: M.stateRegion, type: "singleLineText" },
    { name: M.country, type: "singleLineText" },
    { name: M.address, type: "singleLineText" },
    singleSelect(M.brandFamily, BRAND_FAMILY_OPTIONS),
    { name: M.brandLabel, type: "singleLineText" },
    { name: M.historicalBrand, type: "singleLineText" },
    { name: M.currentBrand, type: "singleLineText" },
    singleSelect(M.identityClass, IDENTITY_CLASS_OPTIONS),
    singleSelect(M.evidenceConfidence, EVIDENCE_CONFIDENCE_OPTIONS),
    { name: M.secRegistrant, type: "singleLineText" },
    { name: M.registrantCik, type: "singleLineText" },
    { name: M.sourceFilingUrl, type: "url" },
    dateField(M.filingDate),
    { name: M.sourceDocumentLedger, type: "multilineText" },
    { name: M.sourceAccessionPrimary, type: "singleLineText" },
    { name: M.discoveryMethod, type: "singleLineText" },
    singleSelect(M.hpcMatchStatus, HPC_MATCH_STATUS_OPTIONS),
    linkField(M.hpcHotel, HPC_TABLE_ID),
    singleSelect(M.lifecycleStatus, LIFECYCLE_STATUS_OPTIONS),
    singleSelect(M.persistenceStatus, PERSISTENCE_STATUS_OPTIONS),
    dateTimeField(M.firstDiscoveredAt),
    dateTimeField(M.lastResearchedAt),
    { name: M.propertyKey, type: "singleLineText" },
    { name: M.notesQuarantine, type: "multilineText" },
    { name: M.schemaVersion, type: "singleLineText" },
    singleSelect(M.publicationState, PUBLICATION_STATE_OPTIONS),
  ];
}

function findingsFields(discoveryTableId) {
  const M = MAP_DISCOVERY_FINDINGS;
  const fields = [
    { name: M.findingId, type: "singleLineText" },
  ];
  if (discoveryTableId && !String(discoveryTableId).startsWith("tbl_PENDING")) {
    fields.push(linkField(M.discoveryProperty, discoveryTableId));
  } else {
    // Dry-run placeholder — real link created on apply after inventory table exists
    fields.push({
      name: M.discoveryProperty,
      type: "multipleRecordLinks",
      options: { linkedTableId: discoveryTableId || "tbl_PENDING_DISCOVERY_INVENTORY" },
      _dryRunNote: "linkedTableId resolved at apply time",
    });
  }
  fields.push(
    singleSelect(M.findingType, FINDING_TYPE_OPTIONS),
    { name: M.fieldName, type: "singleLineText" },
    { name: M.valueObserved, type: "multilineText" },
    singleSelect(M.agreementType, AGREEMENT_TYPE_OPTIONS),
    singleSelect(M.historicalApplicability, HISTORICAL_APPLICABILITY_OPTIONS),
    dateField(M.effectiveDate),
    dateField(M.reportingDate),
    { name: M.sourceUrl, type: "url" },
    { name: M.secAccession, type: "singleLineText" },
    { name: M.registrantCik, type: "singleLineText" },
    { name: M.evidenceSnippet, type: "multilineText" },
    singleSelect(M.confidence, EVIDENCE_CONFIDENCE_OPTIONS),
    singleSelect(M.verificationStatus, FINDING_VERIFICATION_OPTIONS),
    { name: M.relatedOrgName, type: "singleLineText" },
    linkField(M.relatedOrganization, HI_ORGANIZATIONS_TABLE_ID),
    { name: M.supersededBy, type: "singleLineText" },
    singleSelect(M.hpcPromotionStatus, HPC_PROMOTION_STATUS_OPTIONS),
    { name: M.schemaVersion, type: "singleLineText" },
    { name: M.notes, type: "multilineText" },
  );
  return fields;
}

async function ensureTable(baseId, token, tables, report, { tableName, description, primaryName, buildFields }) {
  const entry = {
    tableName,
    tableId: null,
    createdTable: false,
    reused: false,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };
  let table = tables.find((t) => t.name === tableName);
  const fieldSpecs = buildFields(table?.id || null);

  if (!table) {
    if (DRY) {
      entry.wouldCreateTable = true;
      entry.fieldsWouldCreate = fieldSpecs.map((f) => ({ name: f.name, type: f.type }));
      console.log(`[dry-run] WOULD CREATE TABLE ${tableName} (${fieldSpecs.length} fields)`);
    } else {
      const primary = fieldSpecs.find((f) => f.name === primaryName) || fieldSpecs[0];
      const rest = fieldSpecs.filter((f) => f.name !== primary.name);
      // Airtable create-table: link fields to tables that already exist are OK;
      // Discovery→Findings link needs discovery table id — create inventory first without findings reverse.
      const { res, json } = await metaFetch(baseId, token, "/tables", {
        method: "POST",
        body: JSON.stringify({
          name: tableName,
          description,
          fields: [primary, ...rest],
        }),
      });
      if (!res.ok) throw new Error(`Create table ${tableName} failed: ${JSON.stringify(json)}`);
      table = json;
      entry.createdTable = true;
      entry.tableId = json.id;
      entry.fieldsCreated = (json.fields || []).map((f) => f.name);
      tables.push(json);
      console.log(`CREATED TABLE ${json.name} ${json.id}`);
    }
  } else {
    entry.tableId = table.id;
    entry.reused = true;
    console.log(`TABLE EXISTS ${table.name} ${table.id}`);
    const existing = new Set((table.fields || []).map((f) => f.name));
    for (const fieldSpec of fieldSpecs) {
      if (existing.has(fieldSpec.name)) {
        entry.fieldsSkipped.push(fieldSpec.name);
        continue;
      }
      if (DRY) {
        entry.fieldsWouldCreate.push({ name: fieldSpec.name, type: fieldSpec.type });
        continue;
      }
      const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
        method: "POST",
        body: JSON.stringify({
          name: fieldSpec.name,
          type: fieldSpec.type,
          ...(fieldSpec.options ? { options: fieldSpec.options } : {}),
        }),
      });
      if (!res.ok) {
        entry.fieldsFailed.push({ name: fieldSpec.name, error: json });
        console.error(`FIELD FAIL ${tableName}.${fieldSpec.name}`, json);
        continue;
      }
      entry.fieldsCreated.push(fieldSpec.name);
      existing.add(fieldSpec.name);
      console.log(`CREATED FIELD ${tableName}.${fieldSpec.name}`);
    }
  }
  report.tables.push(entry);
  return entry;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!token) throw new Error("Set AIRTABLE_API_KEY (schema.bases:read/write)");

  const baseGate = assertCanonicalDiscoveryBase(GRAPH_BASE_ID_CANONICAL);
  if (!baseGate.ok) throw new Error(JSON.stringify(baseGate));

  const baseId = GRAPH_BASE_ID_CANONICAL;
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`Schema read-back failed: ${res.status} ${JSON.stringify(json)}`);

  const tables = json.tables || [];
  const existingNames = tables.map((t) => t.name).sort();
  const collisionCheck = {
    discoveryInventoryExists: existingNames.includes(TABLE_HI_HOTEL_DISCOVERY_INVENTORY),
    discoveryFindingsExists: existingNames.includes(TABLE_HI_DISCOVERY_FINDINGS),
    hpcExists: Boolean(tables.find((t) => t.id === HPC_TABLE_ID || t.name === "Hotel Property Census")),
    graphEvidenceExists: Boolean(tables.find((t) => t.name === "HI Graph Evidence")),
    relatedEvidenceTables: existingNames.filter((n) =>
      /Source Evidence|Discovery|Finding/i.test(n),
    ),
  };

  const report = {
    contract: DISCOVERY_INVENTORY_CONTRACT_V23,
    schemaVersion: DISCOVERY_SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    mode: DRY ? "DRY_RUN" : "APPLY",
    baseId,
    graphEvidenceReuseDecision: GRAPH_EVIDENCE_REUSE_DECISION,
    schemaReadBack: {
      tableCount: tables.length,
      existingNames,
      collisionCheck,
      customerFacingNote:
        "New tables are INTERNAL_ONLY / CUSTOMER_BLOCKED by default; no HE/Radar/customer UI reads them yet.",
    },
    tables: [],
    guarantees: [
      "No HPC core column mutations",
      "No HI Graph Evidence schema changes",
      "No ADP evidence table mutations",
      "Registrant is not auto-classified as legal owner",
    ],
  };

  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        collisionCheck,
        graphEvidenceReuse: GRAPH_EVIDENCE_REUSE_DECISION.decision,
      },
      null,
      2,
    ),
  );

  const inv = await ensureTable(baseId, token, tables, report, {
    tableName: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
    description:
      "Permanent SEC/hotel discovery inventory for properties independent of HPC coverage. INTERNAL_ONLY.",
    primaryName: MAP_DISCOVERY_INVENTORY.discoveryPropertyId,
    buildFields: () => inventoryFields(),
  });

  // Refresh tables list after create so findings can link
  let discoveryTableId = inv.tableId;
  if (!DRY && inv.createdTable) {
    const refresh = await metaFetch(baseId, token, "/tables");
    if (refresh.res.ok) {
      tables.length = 0;
      tables.push(...(refresh.json.tables || []));
      discoveryTableId = tables.find((t) => t.name === TABLE_HI_HOTEL_DISCOVERY_INVENTORY)?.id || discoveryTableId;
    }
  } else if (DRY && !discoveryTableId) {
    discoveryTableId = "tbl_PENDING_DISCOVERY_INVENTORY";
  }

  await ensureTable(baseId, token, tables, report, {
    tableName: TABLE_HI_DISCOVERY_FINDINGS,
    description:
      "General-purpose discovery findings (contract, ownership, management, brand, transaction) linked to HI Hotel Discovery Inventory — not HPC Graph Evidence.",
    primaryName: MAP_DISCOVERY_FINDINGS.findingId,
    buildFields: () => findingsFields(DRY && !inv.reused ? null : discoveryTableId),
  });

  // Read-back after apply
  if (!DRY) {
    const rb = await metaFetch(baseId, token, "/tables");
    if (!rb.res.ok) throw new Error(`Post-apply read-back failed: ${JSON.stringify(rb.json)}`);
    const after = rb.json.tables || [];
    const invT = after.find((t) => t.name === TABLE_HI_HOTEL_DISCOVERY_INVENTORY);
    const findT = after.find((t) => t.name === TABLE_HI_DISCOVERY_FINDINGS);
    report.postApplyReadBack = {
      ok: Boolean(invT && findT),
      inventory: invT
        ? { id: invT.id, fieldCount: invT.fields?.length, fields: invT.fields?.map((f) => f.name) }
        : null,
      findings: findT
        ? { id: findT.id, fieldCount: findT.fields?.length, fields: findT.fields?.map((f) => f.name) }
        : null,
    };
    if (!report.postApplyReadBack.ok) {
      throw new Error("Post-apply schema validation failed — tables missing on read-back");
    }

    // Persist generated table ids beside HI graph registry
    const outIds = {
      at: new Date().toISOString(),
      contract: DISCOVERY_INVENTORY_CONTRACT_V23,
      schemaVersion: DISCOVERY_SCHEMA_VERSION,
      baseId,
      tableIds: {
        [TABLE_HI_HOTEL_DISCOVERY_INVENTORY]: invT.id,
        [TABLE_HI_DISCOVERY_FINDINGS]: findT.id,
      },
      hpcTableId: HPC_TABLE_ID,
      hiOrganizationsTableId: HI_ORGANIZATIONS_TABLE_ID,
    };
    const idsPath = path.join(
      ROOT,
      "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json",
    );
    fs.writeFileSync(idsPath, JSON.stringify(outIds, null, 2));
    report.generatedIdsPath = idsPath;
  }

  const outDir = path.join(ROOT, "reports", "sec-edgar-v23-discovery-inventory");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, DRY ? "SCHEMA_DRY_RUN.json" : "SCHEMA_APPLY.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  // Exact schema-change diff summary
  const diff = {
    mode: report.mode,
    additions: report.tables.map((t) => ({
      table: t.tableName,
      action: t.wouldCreateTable || t.createdTable ? "CREATE_TABLE" : "REUSE_TABLE",
      tableId: t.tableId,
      fieldsAdded: t.fieldsCreated?.length ? t.fieldsCreated : t.fieldsWouldCreate,
      fieldsSkippedExisting: t.fieldsSkipped,
      fieldsFailed: t.fieldsFailed,
    })),
    hpcSchemaMutations: [],
    graphEvidenceMutations: [],
  };
  fs.writeFileSync(path.join(outDir, DRY ? "SCHEMA_DIFF_DRY_RUN.json" : "SCHEMA_DIFF.json"), JSON.stringify(diff, null, 2));

  console.log(JSON.stringify({ ok: true, outPath, diff }, null, 2));
  if (DRY) {
    console.error("\nDry-run complete. Re-run with --apply after reviewing SCHEMA_DIFF_DRY_RUN.json");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
