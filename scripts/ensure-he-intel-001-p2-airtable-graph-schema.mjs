#!/usr/bin/env node
/**
 * HE-INTEL-001 P2 — ensure Airtable intelligence graph schema.
 *
 * Creates five approved tables + extends Hotel Intelligence Evidence entity types.
 * Non-destructive: never deletes/renames fields. Stops on unexpected conflicts.
 *
 *   node scripts/ensure-he-intel-001-p2-airtable-graph-schema.mjs --dry-run
 *   node scripts/ensure-he-intel-001-p2-airtable-graph-schema.mjs --apply
 *
 * Env: AIRTABLE_API_KEY (preferred) or AIRTABLE_PAT; base via AIRTABLE_INTELLIGENCE_BASE_ID
 * or ADP_AIRTABLE_BASE_ID (default appa2cE7FTRmIbB32).
 *
 * Schema creation only — no bulk hotel research data writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  GRAPH_BASE_ID_CANONICAL,
  GRAPH_CONTRACT_ID,
  GRAPH_CONTRACT_VERSION,
  GRAPH_SCHEMA_VERSION,
  HI_GRAPH_TABLES,
  MAP_HI_ORGANIZATION,
  MAP_HI_HOTEL_ORG_RELATIONSHIP,
  MAP_HI_ORG_CONTACT,
  MAP_HI_CONTACT_CHANNEL,
  MAP_HI_OWNERSHIP_RESEARCH_RUN,
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
  EVIDENCE_ENTITY_TYPE_EXTENSIONS,
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
function linkField(name, tableId, description) {
  return {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId: tableId },
    ...(description ? { description } : {}),
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
      // Guard: do not replace existing field types
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
          ...(fieldSpec.description ? { description: fieldSpec.description } : {}),
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

async function ensureEvidenceEntityTypes(baseId, token, tables, report) {
  const table = tables.find((t) => t.name === HI_GRAPH_TABLES.evidence);
  const entry = {
    tableName: HI_GRAPH_TABLES.evidence,
    tableId: table?.id || null,
    reused: Boolean(table),
    choicesWouldAdd: [],
    choicesAdded: [],
    choicesSkipped: [],
    error: null,
  };
  if (!table) {
    entry.error = "EVIDENCE_TABLE_MISSING";
    report.evidenceExtension = entry;
    console.error("STOP: Hotel Intelligence Evidence table missing — unexpected");
    return entry;
  }
  const field = (table.fields || []).find((f) => f.name === "Entity Type");
  if (!field || field.type !== "singleSelect") {
    entry.error = "ENTITY_TYPE_FIELD_UNEXPECTED";
    report.evidenceExtension = entry;
    console.error("STOP: Entity Type field missing or wrong type — founder review required");
    return entry;
  }
  const existing = new Set((field.options?.choices || []).map((c) => c.name));
  const toAdd = EVIDENCE_ENTITY_TYPE_EXTENSIONS.filter((n) => !existing.has(n));
  entry.choicesSkipped = EVIDENCE_ENTITY_TYPE_EXTENSIONS.filter((n) => existing.has(n));
  if (!toAdd.length) {
    console.log("Evidence Entity Type already has all P2 extensions");
    report.evidenceExtension = entry;
    return entry;
  }
  if (DRY) {
    entry.choicesWouldAdd = toAdd;
    console.log(`[dry-run] WOULD ADD Entity Type choices: ${toAdd.join(", ")}`);
    report.evidenceExtension = entry;
    return entry;
  }
  // Airtable PATCH for select options — append by updating field options
  const nextChoices = [
    ...(field.options?.choices || []),
    ...toAdd.map((name) => ({ name })),
  ];
  const { res, json } = await metaFetch(
    baseId,
    token,
    `/tables/${table.id}/fields/${field.id}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        options: { choices: nextChoices },
      }),
    }
  );
  if (!res.ok) {
    // Airtable Meta API often refuses select-option mutations on existing fields
    // ("Changing a field's type..."). Non-destructive: leave existing choices intact
    // and record a manual founder step — do not attempt destructive field recreation.
    entry.error = json;
    entry.manual_required = true;
    entry.manual_choices_to_add = toAdd;
    entry.note =
      "Meta API refused Entity Type option append. Add choices manually in Airtable UI; do not recreate the field.";
    console.warn(
      "Evidence Entity Type extend deferred (manual):",
      toAdd.join(", "),
      JSON.stringify(json)
    );
  } else {
    entry.choicesAdded = toAdd;
    console.log("ADDED Evidence Entity Type choices:", toAdd.join(", "));
  }
  report.evidenceExtension = entry;
  return entry;
}

async function ensureLinks(baseId, token, tables, report) {
  if (DRY) {
    report.links = { dry_run: true, would_create: ["Parent Organization self-link", "Relationship→Org", "Contact→Org", "Channel→Person", "Channel→Org"] };
    console.log("[dry-run] WOULD CREATE cross-table link fields after table creation");
    return;
  }
  const org = tables.find((t) => t.name === HI_GRAPH_TABLES.organizations);
  const rel = tables.find((t) => t.name === HI_GRAPH_TABLES.hotelOrgRelationships);
  const contact = tables.find((t) => t.name === HI_GRAPH_TABLES.orgContacts);
  const channel = tables.find((t) => t.name === HI_GRAPH_TABLES.contactChannels);
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
      linkReport.push({ table: table.name, field: fieldName, created: true });
      table.fields = [...(table.fields || []), json];
      console.log("CREATED LINK", table.name, fieldName);
    }
  }

  if (org) await ensureLink(org, MAP_HI_ORGANIZATION.parentOrgLink, org.id);
  if (rel && org) await ensureLink(rel, MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink, org.id);
  if (contact && org) await ensureLink(contact, MAP_HI_ORG_CONTACT.organizationLink, org.id);
  if (channel && contact) await ensureLink(channel, MAP_HI_CONTACT_CHANNEL.personLink, contact.id);
  if (channel && org) await ensureLink(channel, MAP_HI_CONTACT_CHANNEL.organizationLink, org.id);
  report.links = linkReport;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  const baseId = (
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    process.env.ADP_AIRTABLE_BASE_ID ||
    GRAPH_BASE_ID_CANONICAL
  ).trim();
  if (!token) {
    console.error("Missing AIRTABLE_API_KEY / AIRTABLE_PAT");
    process.exit(2);
  }

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    console.error("Failed to list tables", res.status, JSON.stringify(json).slice(0, 400));
    process.exit(1);
  }
  const tables = json.tables || [];

  // Conflict detection: unexpected same-name variants
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
    description: "Hotel–organization relationships. Brand/operator/owner are distinct relationship types.",
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

  // Refresh tables after creates for links
  if (!DRY) {
    const refreshed = await metaFetch(baseId, token, "/tables");
    if (refreshed.res.ok) {
      tables.length = 0;
      tables.push(...(refreshed.json.tables || []));
    }
  }

  await ensureEvidenceEntityTypes(baseId, token, tables, report);
  await ensureLinks(baseId, token, tables, report);

  const outDir = path.join(ROOT, "reports", "hotel-explorer");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(
    outDir,
    DRY
      ? "HE-INTEL-001-P2-schema-migration-dry-run-v1.json"
      : "HE-INTEL-001-P2-schema-migration-apply-v1.json"
  );
  fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  console.log("\nWrote", outPath);
  console.log(DRY ? "DRY-RUN complete — re-run with --apply to create schema" : "APPLY complete");
  console.log("Guarantees:", JSON.stringify(writeGuaranteesP2()));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
