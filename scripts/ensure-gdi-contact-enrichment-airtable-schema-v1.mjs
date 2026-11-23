#!/usr/bin/env node
/**
 * Ensure GDI Contact Enrichment Airtable Schema V1 (additive only).
 *
 *   node scripts/ensure-gdi-contact-enrichment-airtable-schema-v1.mjs
 *   node scripts/ensure-gdi-contact-enrichment-airtable-schema-v1.mjs --apply
 *   node scripts/ensure-gdi-contact-enrichment-airtable-schema-v1.mjs --apply --confirm-founder-schema-go
 *
 * Creates (if missing):
 *   - appa2cE7FTRmIbB32 / GDI Contact Enrichment Usage Events
 * Adds (if missing) on appCCUsuGsE1ifoLk / HI Contact Channels:
 *   - Last Checked At, License / Retention Class, Retention Expires At, Provider Request ID
 *
 * Never deletes/renames. Never writes contact records. No Findymail/Surfe API.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CONTACT_ENRICHMENT_AIRTABLE_SCHEMA_V1,
  GDI_OPS_BASE,
  HI_BASE,
  USAGE_EVENTS_TABLE_NAME,
  MAP_USAGE_EVENT,
  MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1,
  USAGE_PROVIDER_CHOICES,
  USAGE_LOOKUP_OUTCOME_CHOICES,
  USAGE_REQUEST_ORIGIN_CHOICES,
  USAGE_DATA_ENVIRONMENT_CHOICES,
  LICENSE_RETENTION_CLASS_CHOICES,
  USAGE_EVENTS_FORBIDDEN_FIELD_PATTERNS,
} from "../lib/group-demand-intelligence/contact-intelligence/gdi-contact-enrichment-airtable-schema-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const CONFIRM = process.argv.includes("--confirm-founder-schema-go");
const REPORT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-contact-enrichment-airtable-schema-v1",
);

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
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
  const field = {
    name,
    type: "date",
    options: { dateFormat: { name: "iso" } },
  };
  if (description) field.description = description;
  return field;
}

function currencyField(name, description) {
  const field = {
    name,
    type: "currency",
    options: { precision: 2, symbol: "$" },
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

function fieldNames(table) {
  return new Set((table?.fields || []).map((f) => f.name));
}

function getPrimary(table) {
  if (!table) return null;
  return (
    (table.fields || []).find((f) => f.id === table.primaryFieldId) ||
    (table.fields || [])[0] ||
    null
  );
}

function assertNoForbiddenUsageFields(fields, report) {
  for (const f of fields || []) {
    for (const re of USAGE_EVENTS_FORBIDDEN_FIELD_PATTERNS) {
      if (re.test(f.name)) {
        report.blocked = true;
        report.blockReasons.push(
          `Forbidden PII/secret field name in usage ledger plan: ${f.name}`,
        );
      }
    }
  }
}

function buildUsagePrimary() {
  return singleLine(
    MAP_USAGE_EVENT.lookupEventId,
    "Idempotent usage-event identity (no PII)",
  );
}

function buildUsageNonPrimary() {
  return [
    singleSelect(MAP_USAGE_EVENT.provider, USAGE_PROVIDER_CHOICES),
    singleSelect(MAP_USAGE_EVENT.lookupOutcome, USAGE_LOOKUP_OUTCOME_CHOICES),
    numberField(MAP_USAGE_EVENT.creditsCharged, 0),
    currencyField(MAP_USAGE_EVENT.costEstimateUsd, "Estimated provider cost USD"),
    singleLine(
      MAP_USAGE_EVENT.personIdentityHash,
      "Opaque personIdentityKey hash — never readable PII",
    ),
    singleLine(MAP_USAGE_EVENT.hotelId),
    singleLine(MAP_USAGE_EVENT.opportunityId),
    singleSelect(MAP_USAGE_EVENT.requestOrigin, USAGE_REQUEST_ORIGIN_CHOICES),
    dateTimeField(MAP_USAGE_EVENT.eventAt),
    singleSelect(MAP_USAGE_EVENT.dataEnvironment, USAGE_DATA_ENVIRONMENT_CHOICES),
    singleLine(MAP_USAGE_EVENT.schemaVersion),
    singleLine(MAP_USAGE_EVENT.providerRequestReference),
    multiline(MAP_USAGE_EVENT.notes),
  ];
}

function buildHiChannelAdditions() {
  return [
    dateTimeField(
      MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1.lastCheckedAt,
      "Most recent email / channel verification check",
    ),
    singleSelect(
      MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1.licenseRetentionClass,
      LICENSE_RETENTION_CLASS_CHOICES,
      "License / retention class for durable channel storage",
    ),
    dateField(
      MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1.retentionExpiresAt,
      "License or retention expiration date",
    ),
    singleLine(
      MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1.providerRequestId,
      "Internal provider audit reference (not a secret)",
    ),
  ];
}

async function createField(baseId, token, tableId, fieldSpec, entry) {
  if (!APPLY) {
    entry.wouldCreateFields.push(fieldSpec.name);
    return { ok: true, dryRun: true };
  }
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldSpec),
  });
  if (!res.ok) {
    entry.errors.push({ field: fieldSpec.name, status: res.status, error: json });
    return { ok: false, json };
  }
  entry.createdFields.push({
    name: fieldSpec.name,
    id: json.id,
    type: json.type,
  });
  return { ok: true, json };
}

async function ensureUsageTable(baseId, token, tables, report) {
  const entry = {
    baseId,
    tableName: USAGE_EVENTS_TABLE_NAME,
    tableId: null,
    createdTable: false,
    wouldCreateTable: false,
    wouldCreateFields: [],
    createdFields: [],
    skippedExisting: [],
    errors: [],
    primaryField: MAP_USAGE_EVENT.lookupEventId,
  };
  report.usageEvents = entry;

  // Duplicate / alias detection
  const aliases = (tables || []).filter((t) =>
    /contact enrichment usage|surfe usage events/i.test(t.name),
  );
  entry.preexistingUsageLikeTables = aliases.map((t) => ({
    id: t.id,
    name: t.name,
  }));

  let table = findTable(tables, USAGE_EVENTS_TABLE_NAME);
  if (!table) {
    // If a Surfe-specific table already exists with compatible primary, STOP —
    // founder asked for one provider-agnostic ledger, not a duplicate.
    const surfe = findTable(tables, "GDI Surfe Usage Events");
    if (surfe) {
      report.blocked = true;
      report.blockReasons.push(
        `Found existing "GDI Surfe Usage Events" (${surfe.id}). Refusing to create a second usage ledger — IC must rename/promote or explicitly authorize dual tables.`,
      );
      entry.errors.push({ conflict: "SURFE_USAGE_TABLE_EXISTS", tableId: surfe.id });
      return entry;
    }
  }

  const allFields = [buildUsagePrimary(), ...buildUsageNonPrimary()];
  assertNoForbiddenUsageFields(allFields, report);
  if (report.blocked) return entry;

  if (table) {
    entry.tableId = table.id;
    const primary = getPrimary(table);
    if (primary?.name !== MAP_USAGE_EVENT.lookupEventId) {
      report.blocked = true;
      report.blockReasons.push(
        `Usage table primary is "${primary?.name}", expected "${MAP_USAGE_EVENT.lookupEventId}"`,
      );
      return entry;
    }
    const existing = fieldNames(table);
    for (const f of buildUsageNonPrimary()) {
      if (existing.has(f.name)) entry.skippedExisting.push(f.name);
      else {
        console.log(
          `${APPLY ? "Creating" : "[dry-run] Would create"} ${USAGE_EVENTS_TABLE_NAME}.${f.name}`,
        );
        await createField(baseId, token, table.id, f, entry);
      }
    }
    entry.skippedExisting.push(MAP_USAGE_EVENT.lookupEventId);
    return entry;
  }

  if (!APPLY) {
    entry.wouldCreateTable = true;
    entry.wouldCreateFields = allFields.map((f) => f.name);
    console.log(
      `[dry-run] Would create table "${USAGE_EVENTS_TABLE_NAME}" with ${allFields.length} fields`,
    );
    return entry;
  }

  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify({
      name: USAGE_EVENTS_TABLE_NAME,
      description:
        "Provider-agnostic contact enrichment usage/cost ledger. No PII (no names/emails/phones/raw payloads).",
      fields: allFields,
    }),
  });
  if (!res.ok) {
    entry.errors.push({ createTable: true, status: res.status, error: json });
    report.blocked = true;
    report.blockReasons.push(`Create usage table failed: HTTP ${res.status}`);
    console.error("Create usage table failed:", JSON.stringify(json));
    return entry;
  }
  entry.createdTable = true;
  entry.tableId = json.id;
  entry.createdFields = (json.fields || []).map((f) => ({
    name: f.name,
    id: f.id,
    type: f.type,
  }));
  tables.push(json);
  console.log(`Created "${USAGE_EVENTS_TABLE_NAME}" (${json.id})`);
  return entry;
}

async function ensureHiChannelFields(baseId, token, tables, report) {
  const entry = {
    baseId,
    tableName: HI_BASE.contactChannelsTableName,
    tableId: null,
    wouldCreateFields: [],
    createdFields: [],
    skippedExisting: [],
    errors: [],
    recordCountBefore: null,
    recordCountAfter: null,
  };
  report.hiContactChannels = entry;

  const table = findTable(tables, HI_BASE.contactChannelsTableId);
  if (!table || table.name !== HI_BASE.contactChannelsTableName) {
    report.blocked = true;
    report.blockReasons.push(
      `HI Contact Channels identity mismatch: expected ${HI_BASE.contactChannelsTableName} (${HI_BASE.contactChannelsTableId}), got ${table?.name || "null"} (${table?.id || "null"})`,
    );
    return entry;
  }
  entry.tableId = table.id;

  // Record count (no mutation) — for before/after confirmation
  const listUrl = `https://api.airtable.com/v0/${baseId}/${table.id}?pageSize=1&fields%5B%5D=${encodeURIComponent("Channel ID")}`;
  const listRes = await fetch(listUrl, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const listJson = await listRes.json().catch(() => ({}));
  // Airtable list doesn't return total; capture offset presence + first id fingerprint
  entry.sampleRecordIdBefore = listJson.records?.[0]?.id || null;
  entry.listOkBefore = listRes.ok;

  const existing = fieldNames(table);
  for (const f of buildHiChannelAdditions()) {
    if (existing.has(f.name)) {
      entry.skippedExisting.push(f.name);
      continue;
    }
    console.log(
      `${APPLY ? "Creating" : "[dry-run] Would create"} ${table.name}.${f.name}`,
    );
    await createField(baseId, token, table.id, f, entry);
  }

  const listRes2 = await fetch(listUrl, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const listJson2 = await listRes2.json().catch(() => ({}));
  entry.sampleRecordIdAfter = listJson2.records?.[0]?.id || null;
  entry.listOkAfter = listRes2.ok;
  entry.sampleRecordUnchanged =
    entry.sampleRecordIdBefore === entry.sampleRecordIdAfter;

  return entry;
}

async function readBack(baseId, token, tableIdOrName) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) return { ok: false, error: json };
  const table = findTable(json.tables, tableIdOrName);
  if (!table) return { ok: false, error: "TABLE_NOT_FOUND", tables: json.tables?.length };
  return {
    ok: true,
    table: {
      id: table.id,
      name: table.name,
      primaryFieldId: table.primaryFieldId,
      fields: (table.fields || []).map((f) => ({
        id: f.id,
        name: f.name,
        type: f.type,
        choices: f.options?.choices?.map((c) => c.name) || undefined,
      })),
    },
  };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!token) {
    throw new Error("Set AIRTABLE_API_KEY or AIRTABLE_PAT");
  }
  if (APPLY && !CONFIRM) {
    throw new Error(
      "Refusing --apply without --confirm-founder-schema-go (additive schema only).",
    );
  }

  const report = {
    contract: GDI_CONTACT_ENRICHMENT_AIRTABLE_SCHEMA_V1,
    mode: APPLY ? "apply" : "dry-run",
    at: new Date().toISOString(),
    blocked: false,
    blockReasons: [],
    safety: {
      findymailApi: false,
      surfeApi: false,
      contactRecordWrites: false,
      syntheticUsageRecords: false,
      deletions: false,
      renames: false,
    },
    verification: {},
  };

  // --- GDI ops base ---
  const gdiMeta = await metaFetch(GDI_OPS_BASE.baseId, token, "/tables");
  if (!gdiMeta.res.ok) {
    throw new Error(`GDI base meta failed: ${JSON.stringify(gdiMeta.json)}`);
  }
  const gdiTables = gdiMeta.json.tables || [];
  const anchor = findTable(gdiTables, GDI_OPS_BASE.referenceTableId);
  const gdo = findTable(gdiTables, GDI_OPS_BASE.groupDemandOpportunitiesTableId);
  report.verification.gdiBase = {
    baseId: GDI_OPS_BASE.baseId,
    tableCount: gdiTables.length,
    referenceTable: anchor
      ? { id: anchor.id, name: anchor.name }
      : null,
    groupDemandOpportunities: gdo
      ? { id: gdo.id, name: gdo.name }
      : null,
  };
  if (!anchor || anchor.id !== GDI_OPS_BASE.referenceTableId) {
    report.blocked = true;
    report.blockReasons.push(
      `GDI base reference table ${GDI_OPS_BASE.referenceTableId} not found — fail closed`,
    );
  }
  if (!gdo || gdo.name !== "Group Demand Opportunities") {
    report.blocked = true;
    report.blockReasons.push(
      "Group Demand Opportunities identity could not be verified on GDI ops base",
    );
  }
  if (report.blocked) {
    fs.mkdirSync(REPORT_DIR, { recursive: true });
    fs.writeFileSync(
      path.join(REPORT_DIR, APPLY ? "APPLY_BLOCKED.json" : "DRY_RUN_BLOCKED.json"),
      JSON.stringify(report, null, 2),
    );
    console.error("BLOCKED:", report.blockReasons.join("; "));
    process.exit(2);
  }

  await ensureUsageTable(GDI_OPS_BASE.baseId, token, gdiTables, report);
  if (report.blocked) {
    fs.mkdirSync(REPORT_DIR, { recursive: true });
    fs.writeFileSync(
      path.join(REPORT_DIR, "BLOCKED.json"),
      JSON.stringify(report, null, 2),
    );
    console.error("BLOCKED:", report.blockReasons.join("; "));
    process.exit(2);
  }

  // --- HI base ---
  const hiMeta = await metaFetch(HI_BASE.baseId, token, "/tables");
  if (!hiMeta.res.ok) {
    throw new Error(`HI base meta failed: ${JSON.stringify(hiMeta.json)}`);
  }
  await ensureHiChannelFields(HI_BASE.baseId, token, hiMeta.json.tables || [], report);
  if (report.blocked) {
    fs.mkdirSync(REPORT_DIR, { recursive: true });
    fs.writeFileSync(
      path.join(REPORT_DIR, "BLOCKED.json"),
      JSON.stringify(report, null, 2),
    );
    console.error("BLOCKED:", report.blockReasons.join("; "));
    process.exit(2);
  }

  // --- Read-back ---
  if (APPLY || report.usageEvents?.tableId) {
    const usageId =
      report.usageEvents.tableId || USAGE_EVENTS_TABLE_NAME;
    report.readBack = {
      usageEvents: await readBack(GDI_OPS_BASE.baseId, token, usageId),
      hiContactChannels: await readBack(
        HI_BASE.baseId,
        token,
        HI_BASE.contactChannelsTableId,
      ),
    };
  } else {
    // Dry-run: still snapshot HI + confirm usage absent
    report.readBack = {
      usageEvents: await readBack(
        GDI_OPS_BASE.baseId,
        token,
        USAGE_EVENTS_TABLE_NAME,
      ),
      hiContactChannels: await readBack(
        HI_BASE.baseId,
        token,
        HI_BASE.contactChannelsTableId,
      ),
    };
  }

  // Validate select choices on read-back when table exists
  const rbUsage = report.readBack?.usageEvents;
  if (rbUsage?.ok) {
    const byName = Object.fromEntries(
      (rbUsage.table.fields || []).map((f) => [f.name, f]),
    );
    const provider = byName[MAP_USAGE_EVENT.provider];
    const outcome = byName[MAP_USAGE_EVENT.lookupOutcome];
    report.readBack.choiceValidation = {
      providerOk:
        USAGE_PROVIDER_CHOICES.every((c) =>
          (provider?.choices || []).includes(c),
        ),
      outcomeOk: USAGE_LOOKUP_OUTCOME_CHOICES.every((c) =>
        (outcome?.choices || []).includes(c),
      ),
      fieldNamesPresent: Object.values(MAP_USAGE_EVENT).every((n) => byName[n]),
      noPiiFields: !(rbUsage.table.fields || []).some((f) =>
        USAGE_EVENTS_FORBIDDEN_FIELD_PATTERNS.some((re) => re.test(f.name)),
      ),
    };
  }

  const hiRb = report.readBack?.hiContactChannels;
  if (hiRb?.ok) {
    const names = new Set((hiRb.table.fields || []).map((f) => f.name));
    report.readBack.hiEnrichmentFieldsPresent = Object.fromEntries(
      Object.entries(MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1).map(([k, n]) => [
        k,
        {
          name: n,
          present: names.has(n),
          id: (hiRb.table.fields || []).find((f) => f.name === n)?.id || null,
        },
      ]),
    );
  }

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  const outName = APPLY ? "APPLY_REPORT.json" : "DRY_RUN_REPORT.json";
  fs.writeFileSync(path.join(REPORT_DIR, outName), JSON.stringify(report, null, 2));
  console.log(`\nWrote ${path.join(REPORT_DIR, outName)}`);
  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        usageTableId: report.usageEvents?.tableId,
        usageCreated: report.usageEvents?.createdTable,
        hiCreatedFields: report.hiContactChannels?.createdFields,
        hiSkipped: report.hiContactChannels?.skippedExisting,
        blocked: report.blocked,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
