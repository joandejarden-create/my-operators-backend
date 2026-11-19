#!/usr/bin/env node
/**
 * HE-INTEL-001 P2 — ensure Airtable intelligence graph schema on HPC base.
 *
 * Canonical base: appCCUsuGsE1ifoLk (Hotel Property Census).
 * FORBIDDEN: appa2cE7FTRmIbB32 (ADP/intelligence) — refuse writes there.
 *
 *   node scripts/ensure-he-intel-001-p2-airtable-graph-schema.mjs --dry-run
 *   node scripts/ensure-he-intel-001-p2-airtable-graph-schema.mjs --apply
 *
 * Creates five HI graph tables + HI Graph Evidence; links relationships/runs/evidence to HPC.
 * Does NOT mutate ADP Hotel Intelligence Evidence.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  GRAPH_BASE_ID_CANONICAL,
  GRAPH_BASE_ID_FORBIDDEN,
  GRAPH_CONTRACT_ID,
  GRAPH_CONTRACT_VERSION,
  GRAPH_SCHEMA_VERSION,
  HPC_TABLE_ID,
  HPC_TABLE_NAME,
  HI_GRAPH_TABLES,
  MAP_HI_ORGANIZATION,
  MAP_HI_HOTEL_ORG_RELATIONSHIP,
  MAP_HI_ORG_CONTACT,
  MAP_HI_CONTACT_CHANNEL,
  MAP_HI_OWNERSHIP_RESEARCH_RUN,
  MAP_HI_GRAPH_EVIDENCE,
  ORG_TYPES,
  RELATIONSHIP_TYPES,
  VERIFICATION_STATES,
  CONFIDENCE_LEVELS,
  CHANNEL_KINDS,
  CHANNEL_CLASSIFICATIONS,
  CHANNEL_VERIFICATION,
  RUN_STATUSES,
  RECORD_PUBLICATION_STATE,
  CONFLICT_STATUS,
  SUBJECT_TYPES,
  GRAPH_EVIDENCE_ENTITY_TYPES,
  assertCanonicalGraphBase,
  resolveGraphBaseId,
  writeGuaranteesP2,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

for (const p of [
  path.join(ROOT, ".env"),
  path.join(ROOT, "..", "my-operators-backend", ".env"),
  "C:/Dev/dealality/my-operators-backend/.env",
]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;

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
function numberField(name, precision = 2) {
  return { name, type: "number", options: { precision } };
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

function orgFields() {
  const M = MAP_HI_ORGANIZATION;
  return [
    { name: M.orgId, type: "singleLineText" },
    { name: M.legalName, type: "singleLineText" },
    { name: M.tradingName, type: "singleLineText" },
    singleSelect(M.organizationType, ORG_TYPES),
    { name: M.jurisdiction, type: "singleLineText" },
    { name: M.registryId, type: "singleLineText" },
    { name: M.officialWebsite, type: "url" },
    { name: M.parentOrgId, type: "singleLineText" },
    singleSelect(M.verificationState, VERIFICATION_STATES),
    singleSelect(M.confidence, CONFIDENCE_LEVELS),
    { name: M.dedupeKey, type: "singleLineText" },
    { name: M.provenanceSummary, type: "multilineText" },
    singleSelect(M.publicationState, Object.values(RECORD_PUBLICATION_STATE)),
    { name: M.evidenceRefs, type: "multilineText" },
    { name: M.schemaVersion, type: "singleLineText" },
    { name: M.notes, type: "multilineText" },
  ];
}

function relFields() {
  const M = MAP_HI_HOTEL_ORG_RELATIONSHIP;
  return [
    { name: M.relationshipId, type: "singleLineText" },
    { name: M.hpcHotelId, type: "singleLineText" },
    { name: M.organizationId, type: "singleLineText" },
    singleSelect(M.relationshipType, RELATIONSHIP_TYPES),
    checkbox(M.current),
    dateField(M.effectiveFrom),
    dateField(M.effectiveTo),
    singleSelect(M.confidence, CONFIDENCE_LEVELS),
    singleSelect(M.verificationState, VERIFICATION_STATES),
    singleSelect(M.conflictStatus, CONFLICT_STATUS),
    { name: M.dedupeKey, type: "singleLineText" },
    { name: M.evidenceRefs, type: "multilineText" },
    singleSelect(M.publicationState, Object.values(RECORD_PUBLICATION_STATE)),
    { name: M.schemaVersion, type: "singleLineText" },
    { name: M.notes, type: "multilineText" },
  ];
}

function contactFields() {
  const M = MAP_HI_ORG_CONTACT;
  return [
    { name: M.personId, type: "singleLineText" },
    { name: M.displayName, type: "singleLineText" },
    { name: M.organizationId, type: "singleLineText" },
    { name: M.title, type: "singleLineText" },
    { name: M.functionRole, type: "singleLineText" },
    { name: M.hotelResponsibility, type: "multilineText" },
    checkbox(M.affiliationVerified),
    { name: M.roleCurrency, type: "singleLineText" },
    singleSelect(M.confidence, CONFIDENCE_LEVELS),
    singleSelect(M.verificationState, VERIFICATION_STATES),
    { name: M.personDedupeKey, type: "singleLineText" },
    { name: M.affiliationDedupeKey, type: "singleLineText" },
    { name: M.evidenceRefs, type: "multilineText" },
    singleSelect(M.publicationState, Object.values(RECORD_PUBLICATION_STATE)),
    { name: M.schemaVersion, type: "singleLineText" },
    { name: M.notes, type: "multilineText" },
  ];
}

function channelFields() {
  const M = MAP_HI_CONTACT_CHANNEL;
  return [
    { name: M.channelId, type: "singleLineText" },
    singleSelect(M.subjectType, SUBJECT_TYPES),
    { name: M.subjectId, type: "singleLineText" },
    singleSelect(M.channelKind, CHANNEL_KINDS),
    { name: M.channelValue, type: "singleLineText" },
    singleSelect(M.classification, CHANNEL_CLASSIFICATIONS),
    singleSelect(M.verificationStatus, CHANNEL_VERIFICATION),
    { name: M.sourceName, type: "singleLineText" },
    { name: M.sourceUrl, type: "url" },
    dateTimeField(M.observedAt),
    { name: M.provider, type: "singleLineText" },
    { name: M.usageProvenance, type: "multilineText" },
    { name: M.accessRestriction, type: "singleLineText" },
    { name: M.dedupeKey, type: "singleLineText" },
    singleSelect(M.publicationState, Object.values(RECORD_PUBLICATION_STATE)),
    { name: M.schemaVersion, type: "singleLineText" },
    { name: M.notes, type: "multilineText" },
  ];
}

function runFields() {
  const M = MAP_HI_OWNERSHIP_RESEARCH_RUN;
  return [
    { name: M.runId, type: "singleLineText" },
    { name: M.caseId, type: "singleLineText" },
    { name: M.contractId, type: "singleLineText" },
    { name: M.contractVersion, type: "singleLineText" },
    { name: M.hpcHotelId, type: "singleLineText" },
    { name: M.organizationScope, type: "multilineText" },
    { name: M.executionMode, type: "singleLineText" },
    { name: M.currentStage, type: "singleLineText" },
    singleSelect(M.runStatus, RUN_STATUSES),
    { name: M.checkpointRef, type: "singleLineText" },
    { name: M.evidenceVersion, type: "singleLineText" },
    { name: M.providerOperations, type: "multilineText" },
    numberField(M.costUsd, 4),
    { name: M.costUsdStatus, type: "singleLineText" },
    numberField(M.credits, 0),
    { name: M.validationState, type: "singleLineText" },
    singleSelect(M.publicationState, Object.values(RECORD_PUBLICATION_STATE)),
    { name: M.failureInfo, type: "multilineText" },
    { name: M.inputFingerprint, type: "singleLineText" },
    { name: M.schemaVersion, type: "singleLineText" },
    { name: M.notes, type: "multilineText" },
  ];
}

function evidenceFields() {
  const M = MAP_HI_GRAPH_EVIDENCE;
  return [
    { name: M.evidenceId, type: "singleLineText" },
    { name: M.hpcHotelId, type: "singleLineText" },
    singleSelect(M.entityType, GRAPH_EVIDENCE_ENTITY_TYPES),
    { name: M.entityRecordId, type: "singleLineText" },
    { name: M.fieldName, type: "singleLineText" },
    { name: M.valueObserved, type: "multilineText" },
    { name: M.sourceName, type: "singleLineText" },
    { name: M.sourceType, type: "singleLineText" },
    { name: M.sourceUrl, type: "url" },
    dateField(M.sourcePublishedDate),
    dateTimeField(M.retrievedAt),
    { name: M.evidenceSnippet, type: "multilineText" },
    singleSelect(M.confidence, CONFIDENCE_LEVELS),
    { name: M.evidenceStrength, type: "singleLineText" },
    checkbox(M.current),
    { name: M.supersededBy, type: "singleLineText" },
    { name: M.notes, type: "multilineText" },
    { name: M.schemaVersion, type: "singleLineText" },
  ];
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
  const fieldSpecs = buildFields();

  if (!table) {
    if (DRY) {
      entry.wouldCreateTable = true;
      entry.fieldsWouldCreate = fieldSpecs.map((f) => f.name);
      console.log(`[dry-run] WOULD CREATE TABLE ${tableName} (${fieldSpecs.length} fields)`);
    } else {
      const primary = fieldSpecs.find((f) => f.name === primaryName) || fieldSpecs[0];
      const rest = fieldSpecs.filter((f) => f.name !== primary.name);
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
        entry.fieldsWouldCreate.push(fieldSpec.name);
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
        console.error("FIELD FAILED", tableName, fieldSpec.name, JSON.stringify(json));
      } else {
        entry.fieldsCreated.push(fieldSpec.name);
        console.log("CREATED FIELD", tableName, fieldSpec.name);
      }
    }
  }
  report.tables.push(entry);
  return table;
}

async function ensureLinks(baseId, token, tables, report, hpcTableId) {
  if (DRY) {
    report.links = {
      dry_run: true,
      would_create: [
        "Parent Organization self-link",
        "Relationship→Org",
        "Relationship→HPC",
        "Contact→Org",
        "Channel→Person",
        "Channel→Org",
        "Run→HPC",
        "GraphEvidence→HPC",
      ],
    };
    console.log("[dry-run] WOULD CREATE cross-table + HPC link fields after table creation");
    return;
  }
  const org = tables.find((t) => t.name === HI_GRAPH_TABLES.organizations);
  const rel = tables.find((t) => t.name === HI_GRAPH_TABLES.hotelOrgRelationships);
  const contact = tables.find((t) => t.name === HI_GRAPH_TABLES.orgContacts);
  const channel = tables.find((t) => t.name === HI_GRAPH_TABLES.contactChannels);
  const run = tables.find((t) => t.name === HI_GRAPH_TABLES.ownershipResearchRuns);
  const evidence = tables.find((t) => t.name === HI_GRAPH_TABLES.graphEvidence);
  const linkReport = [];

  async function ensureLink(table, fieldName, linkedTableId) {
    if (!table?.id || !linkedTableId) return;
    const existing = new Set((table.fields || []).map((f) => f.name));
    if (existing.has(fieldName)) {
      linkReport.push({ table: table.name, field: fieldName, skipped: true });
      return;
    }
    const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(linkField(fieldName, linkedTableId)),
    });
    if (!res.ok) {
      linkReport.push({ table: table.name, field: fieldName, error: json });
      console.error("LINK FAILED", table.name, fieldName, JSON.stringify(json));
    } else {
      linkReport.push({ table: table.name, field: fieldName, created: true, linkedTableId });
      table.fields = [...(table.fields || []), json];
      console.log("CREATED LINK", table.name, fieldName, "→", linkedTableId);
    }
  }

  if (org) await ensureLink(org, MAP_HI_ORGANIZATION.parentOrgLink, org.id);
  if (rel && org) await ensureLink(rel, MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink, org.id);
  if (rel && hpcTableId) await ensureLink(rel, MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelLink, hpcTableId);
  if (contact && org) await ensureLink(contact, MAP_HI_ORG_CONTACT.organizationLink, org.id);
  if (channel && contact) await ensureLink(channel, MAP_HI_CONTACT_CHANNEL.personLink, contact.id);
  if (channel && org) await ensureLink(channel, MAP_HI_CONTACT_CHANNEL.organizationLink, org.id);
  if (run && hpcTableId) await ensureLink(run, MAP_HI_OWNERSHIP_RESEARCH_RUN.hpcHotelLink, hpcTableId);
  if (evidence && hpcTableId) await ensureLink(evidence, MAP_HI_GRAPH_EVIDENCE.hpcHotelLink, hpcTableId);
  report.links = linkReport;
}

function writeRegistry(report) {
  const ids = {};
  for (const t of report.tables || []) {
    if (t.tableId) ids[t.tableName] = t.tableId;
  }
  const registry = {
    at: new Date().toISOString(),
    graph_contract: GRAPH_CONTRACT_ID,
    graph_contract_version: GRAPH_CONTRACT_VERSION,
    baseId: report.baseId,
    forbiddenBaseId: GRAPH_BASE_ID_FORBIDDEN,
    hpcTableId: HPC_TABLE_ID,
    hpcTableName: HPC_TABLE_NAME,
    tableIds: ids,
    evidenceArchitecture: {
      decision: "HI_GRAPH_EVIDENCE_ON_HPC_BASE",
      graphEvidenceTable: HI_GRAPH_TABLES.graphEvidence,
      adpEvidencePreserved: {
        baseId: GRAPH_BASE_ID_FORBIDDEN,
        tableId: "tblHxS8x0niDEGVsh",
        tableName: "Hotel Intelligence Evidence",
        action: "DO_NOT_DELETE_OR_MUTATE",
      },
      rationale:
        "Cross-base evidence links undermine HPC consistency; ownership/org/person/channel/run evidence belongs beside HPC.",
    },
  };
  const outDir = path.join(ROOT, "reports", "hotel-explorer");
  fs.mkdirSync(outDir, { recursive: true });
  const regPath = path.join(outDir, "HE-INTEL-001-P2-live-schema-ids-v1.json");
  fs.writeFileSync(regPath, `${JSON.stringify(registry, null, 2)}\n`, "utf8");
  const libPath = path.join(
    ROOT,
    "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json"
  );
  fs.writeFileSync(libPath, `${JSON.stringify(registry, null, 2)}\n`, "utf8");
  console.log("Wrote registry", regPath);
  return registry;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  if (!token) {
    console.error("Missing AIRTABLE_API_KEY / AIRTABLE_PAT");
    process.exit(2);
  }

  const baseId = resolveGraphBaseId(process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) {
    console.error("STOP — base gate failed:", JSON.stringify(gate));
    process.exit(3);
  }
  if (baseId === GRAPH_BASE_ID_FORBIDDEN) {
    console.error("STOP — refusing forbidden ADP/intelligence base");
    process.exit(3);
  }

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    console.error("Failed to list tables", res.status, JSON.stringify(json).slice(0, 400));
    process.exit(1);
  }
  const tables = json.tables || [];
  const hpc = tables.find((t) => t.id === HPC_TABLE_ID || t.name === HPC_TABLE_NAME);
  if (!hpc) {
    console.error("STOP — Hotel Property Census not found on canonical base");
    process.exit(3);
  }
  console.log("HPC verified", hpc.id, hpc.name, "base", baseId);

  const blockers = [];
  for (const t of tables) {
    if (/^HI\s+Organi/i.test(t.name) && t.name !== HI_GRAPH_TABLES.organizations) {
      blockers.push({ reason: "NAME_VARIANT", table: t.name, id: t.id });
    }
  }
  if (blockers.length) {
    console.error("STOP — unexpected conflicting tables:", JSON.stringify(blockers));
    process.exit(3);
  }

  const report = {
    at: new Date().toISOString(),
    mode: DRY ? "DRY_RUN" : "APPLY",
    baseId,
    expectedBaseId: GRAPH_BASE_ID_CANONICAL,
    forbiddenBaseId: GRAPH_BASE_ID_FORBIDDEN,
    hpcTableId: hpc.id,
    graph_contract: GRAPH_CONTRACT_ID,
    graph_contract_version: GRAPH_CONTRACT_VERSION,
    schema_version: GRAPH_SCHEMA_VERSION,
    write_guarantees: writeGuaranteesP2(),
    tables: [],
  };

  await ensureTable(baseId, token, tables, report, {
    tableName: HI_GRAPH_TABLES.organizations,
    description: "Canonical hotel-intelligence organizations (P2). Deduped by Dedupe Key — never name-only merge.",
    primaryName: MAP_HI_ORGANIZATION.orgId,
    buildFields: orgFields,
  });
  await ensureTable(baseId, token, tables, report, {
    tableName: HI_GRAPH_TABLES.hotelOrgRelationships,
    description: "Hotel–organization relationships linked to HPC. Brand/operator/owner are distinct types.",
    primaryName: MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipId,
    buildFields: relFields,
  });
  await ensureTable(baseId, token, tables, report, {
    tableName: HI_GRAPH_TABLES.orgContacts,
    description: "Person × organization affiliation. Person identity separate from employment history.",
    primaryName: MAP_HI_ORG_CONTACT.personId,
    buildFields: contactFields,
  });
  await ensureTable(baseId, token, tables, report, {
    tableName: HI_GRAPH_TABLES.contactChannels,
    description: "Emails/phones with classification + verification. Inferred ≠ verified direct.",
    primaryName: MAP_HI_CONTACT_CHANNEL.channelId,
    buildFields: channelFields,
  });
  await ensureTable(baseId, token, tables, report, {
    tableName: HI_GRAPH_TABLES.ownershipResearchRuns,
    description: "Research run ledger: stage, checkpoints, provider ops, cost, publication state.",
    primaryName: MAP_HI_OWNERSHIP_RESEARCH_RUN.runId,
    buildFields: runFields,
  });
  await ensureTable(baseId, token, tables, report, {
    tableName: HI_GRAPH_TABLES.graphEvidence,
    description:
      "Field-level evidence for HI graph entities on the HPC base. ADP Hotel Intelligence Evidence is separate and preserved.",
    primaryName: MAP_HI_GRAPH_EVIDENCE.evidenceId,
    buildFields: evidenceFields,
  });

  if (!DRY) {
    const refreshed = await metaFetch(baseId, token, "/tables");
    if (refreshed.res.ok) {
      tables.length = 0;
      tables.push(...(refreshed.json.tables || []));
    }
  }

  await ensureLinks(baseId, token, tables, report, hpc.id);

  const outDir = path.join(ROOT, "reports", "hotel-explorer");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(
    outDir,
    DRY
      ? "HE-INTEL-001-P2-schema-migration-hpc-base-dry-run-v1.json"
      : "HE-INTEL-001-P2-schema-migration-hpc-base-apply-v1.json"
  );
  fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  console.log("\nWrote", outPath);

  if (!DRY) {
    writeRegistry(report);
  }

  console.log(DRY ? "DRY-RUN complete — re-run with --apply to create schema on HPC base" : "APPLY complete on HPC base");
  console.log("Guarantees:", JSON.stringify(writeGuaranteesP2()));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
