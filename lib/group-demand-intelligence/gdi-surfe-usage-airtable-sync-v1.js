/**
 * Surfe Usage V1.2 — Airtable persistence design + bounded sync.
 *
 * No suitable existing usage-events table was found in AI Demand Positioning.
 * This module proposes a minimal "GDI Surfe Usage Events" table and implements
 * idempotent sync against it ONLY when table ID + founder enable flags are set.
 *
 * Default: dry-run / disabled. Local filesystem ledger remains SoT for recovery.
 * Never writes emails, phones, raw Surfe payloads, or secrets.
 */
import { GDI_AIRTABLE_BASE } from "./workflow/gdi-full-e2e-airtable-persistence-contract-v1.js";
import {
  GDI_SURFE_USAGE_TRACKING_V1_2,
  classifyUsageClass,
} from "./gdi-surfe-usage-context-v1.js";

export const GDI_SURFE_USAGE_AIRTABLE_SYNC_V1 = "gdi-surfe-usage-airtable-sync-v1";

/**
 * Surfe usage persists into the provider-agnostic ledger
 * (GDI Contact Enrichment Usage Events) — not a Surfe-only table.
 * Surfe emails/phones remain session-only; ledger stores no PII.
 */
export const PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE = Object.freeze({
  baseId: GDI_AIRTABLE_BASE.baseId,
  baseName: GDI_AIRTABLE_BASE.name,
  tableName: "GDI Contact Enrichment Usage Events",
  tableId: "tblvUJlik63pETitY",
  identityField: "Lookup Event ID",
  status: "SCHEMA_CREATED_PROVIDER_AGNOSTIC_V1",
  reuseAudit:
    "Founder GO 2026-11-23: one shared usage ledger for Findymail/Surfe/future providers. Do not create GDI Surfe Usage Events.",
  envTableIdKey: "GDI_CONTACT_ENRICHMENT_USAGE_AIRTABLE_TABLE_ID",
  legacyEnvTableIdKey: "GDI_SURFE_USAGE_AIRTABLE_TABLE_ID",
});

/**
 * Proposed field map (Airtable display names → local ledger keys).
 * Single-line / number / select only. No PII contact fields.
 */
export const PROPOSED_FIELD_MAP = Object.freeze({
  "Lookup Event ID": "lookupEventId",
  "Client Request ID": "clientRequestId",
  "Enrichment ID": "enrichmentId",
  "Event At": "at",
  "Data Environment": "dataEnvironment",
  "Usage Class": "usageClass",
  "Opportunity Hotel ID": "hotelId",
  "Opportunity Hotel Name": "hotelName",
  "Hotel Market": "hotelMarket",
  "Hotel Country": "hotelCountry",
  "Opportunity ID": "opportunityId",
  "Opportunity Title": "opportunityTitle",
  "Opportunity Organization": "opportunityOrganizationName",
  "Requesting Portfolio ID": "requestingPortfolioId",
  "Requesting Hotel Or Office ID": "requestingHotelOrOfficeId",
  "Requesting Organization Name": "requestingOrganizationName",
  "Requesting Office Market": "requestingOfficeMarket",
  "Requesting User Hash": "requestingUserHash",
  "Lookup Type": "lookupType",
  Result: "status",
  "Email Found": "emailFound",
  "Phone Found": "phoneFound",
  "Credits Charged": "creditsCharged",
  "Credit Certainty": "creditCertainty",
  "Credits Confirmed": "creditsConfirmed",
  "Credits Estimated": "creditsEstimated",
  "Cost Estimate USD": "costEstimateUsd",
  "Cost Certainty": "costCertainty",
  Surface: "surface",
  "Provider Latency Ms": "providerLatencyMs",
  "Airtable Sync Status": "airtableSyncStatus",
  "Schema Version": "schemaVersion",
});

export function isSurfeUsageAirtableSyncEnabled(env = process.env) {
  return String(env.GDI_SURFE_USAGE_AIRTABLE_SYNC_ENABLE || "") === "1";
}

export function isSurfeUsageAirtableDryRun(env = process.env) {
  // Default dry-run ON. Explicit "0" required for apply.
  return String(env.GDI_SURFE_USAGE_AIRTABLE_DRY_RUN ?? "1") !== "0";
}

export function resolveSurfeUsageAirtableTableId(env = process.env) {
  const id = String(
    env.GDI_CONTACT_ENRICHMENT_USAGE_AIRTABLE_TABLE_ID ||
      env.GDI_SURFE_USAGE_AIRTABLE_TABLE_ID ||
      PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE.tableId ||
      "",
  ).trim();
  return id || null;
}

export function buildAirtableFieldsFromUsageEvent(entry = {}) {
  const usageClass = classifyUsageClass(entry);
  const fields = {};
  for (const [airtableName, localKey] of Object.entries(PROPOSED_FIELD_MAP)) {
    let value = entry[localKey];
    if (localKey === "usageClass") value = usageClass;
    if (localKey === "schemaVersion") {
      value = entry.schemaVersion || GDI_SURFE_USAGE_TRACKING_V1_2;
    }
    if (localKey === "airtableSyncStatus") {
      value = entry.airtableSyncStatus || "pending";
    }
    if (localKey === "emailFound" || localKey === "phoneFound") {
      value = entry[localKey] === true;
    }
    if (value === undefined || value === null || value === "") continue;
    // Never allow accidental PII keys
    if (
      /email|phone|mobile|contact|payload|raw|api.?key/i.test(airtableName) &&
      !/Email Found|Phone Found/.test(airtableName)
    ) {
      continue;
    }
    fields[airtableName] = value;
  }
  return fields;
}

/**
 * Plan idempotent upserts. Does not call Airtable.
 * Skips FIXTURE_TEST by default unless includeFixtures=true.
 */
export function planSurfeUsageAirtableSync(entries = [], opts = {}) {
  const includeFixtures = opts.includeFixtures === true;
  const planned = [];
  const skipped = [];
  const seen = new Set();

  for (const e of entries || []) {
    if (!e || typeof e !== "object") continue;
    const id = String(e.lookupEventId || "").trim();
    if (!id) {
      skipped.push({ reason: "missing_lookupEventId", entry: e });
      continue;
    }
    if (seen.has(id)) {
      skipped.push({ reason: "duplicate_in_batch", lookupEventId: id });
      continue;
    }
    seen.add(id);
    const usageClass = classifyUsageClass(e);
    if (!includeFixtures && usageClass === "FIXTURE_TEST") {
      skipped.push({ reason: "fixture_excluded", lookupEventId: id, usageClass });
      continue;
    }
    planned.push({
      identity: id,
      identityField: PROPOSED_FIELD_MAP["Lookup Event ID"]
        ? "Lookup Event ID"
        : "Lookup Event ID",
      filterFormula: `{Lookup Event ID}='${id.replace(/'/g, "\\'")}'`,
      fields: buildAirtableFieldsFromUsageEvent(e),
      usageClass,
      operation: "upsert_by_lookup_event_id",
    });
  }

  return {
    ok: true,
    schema: GDI_SURFE_USAGE_AIRTABLE_SYNC_V1,
    proposedTable: PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE,
    plannedCount: planned.length,
    skippedCount: skipped.length,
    planned,
    skipped,
    doubleCountGuard:
      "Idempotent on Lookup Event ID; local ledger remains authoritative; sync marks airtableSyncStatus.",
  };
}

/**
 * Execute sync. Refuses without enable flag + table ID.
 * Dry-run by default. Apply requires GDI_SURFE_USAGE_AIRTABLE_DRY_RUN=0.
 * Airtable outages must not be thrown into Surfe reveal — callers use Safe wrapper.
 */
export async function syncSurfeUsageEventsToAirtable(entries = [], opts = {}) {
  const env = opts.env || process.env;
  const dryRun = opts.dryRun != null ? opts.dryRun : isSurfeUsageAirtableDryRun(env);
  const enabled = isSurfeUsageAirtableSyncEnabled(env);
  const tableId = resolveSurfeUsageAirtableTableId(env);
  const plan = planSurfeUsageAirtableSync(entries, opts);

  if (!enabled) {
    return {
      ...plan,
      applied: false,
      blocked: true,
      reason: "SYNC_DISABLED",
      message:
        "Set GDI_SURFE_USAGE_AIRTABLE_SYNC_ENABLE=1 after founder approves table creation.",
    };
  }
  if (!tableId) {
    return {
      ...plan,
      applied: false,
      blocked: true,
      reason: "TABLE_ID_MISSING",
      message:
        "Propose/create GDI Surfe Usage Events table, then set GDI_SURFE_USAGE_AIRTABLE_TABLE_ID.",
      persistenceClassification: "PERSISTENCE_DESIGNED",
    };
  }
  if (dryRun) {
    return {
      ...plan,
      applied: false,
      dryRun: true,
      tableId,
      message: "Dry-run only — no Airtable writes.",
      persistenceClassification: "PERSISTENCE_DESIGNED",
    };
  }

  // Live apply path — only when explicitly enabled. Uses Airtable REST via existing token.
  const apiKey = String(env.AIRTABLE_API_KEY || "").trim();
  const baseId = String(env.GDI_SURFE_USAGE_AIRTABLE_BASE_ID || GDI_AIRTABLE_BASE.baseId).trim();
  if (!apiKey || !baseId) {
    return {
      ...plan,
      applied: false,
      blocked: true,
      reason: "AIRTABLE_CREDENTIALS_MISSING",
    };
  }

  const results = [];
  for (const row of plan.planned) {
    try {
      const listUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableId)}?filterByFormula=${encodeURIComponent(row.filterFormula)}&maxRecords=1`;
      const listRes = await fetch(listUrl, {
        headers: { Authorization: `Bearer ${apiKey}` },
      });
      if (!listRes.ok) {
        results.push({
          identity: row.identity,
          ok: false,
          status: listRes.status,
          error: "list_failed",
        });
        continue;
      }
      const listJson = await listRes.json();
      const existing = Array.isArray(listJson.records) ? listJson.records[0] : null;
      const method = existing ? "PATCH" : "POST";
      const url = existing
        ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableId)}`
        : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableId)}`;
      const body = existing
        ? { records: [{ id: existing.id, fields: row.fields }] }
        : { records: [{ fields: row.fields }] };
      const writeRes = await fetch(url, {
        method,
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });
      results.push({
        identity: row.identity,
        ok: writeRes.ok,
        status: writeRes.status,
        operation: existing ? "update" : "create",
      });
    } catch (err) {
      results.push({
        identity: row.identity,
        ok: false,
        error: String(err?.message || err).slice(0, 160),
      });
    }
  }

  return {
    ...plan,
    applied: true,
    dryRun: false,
    tableId,
    results,
    persistenceClassification: "PERSISTENCE_VALIDATED_PENDING_REVIEW",
  };
}

/** Never throws — safe to call after ledger append. */
export async function syncSurfeUsageEventsToAirtableSafe(entries = [], opts = {}) {
  try {
    return await syncSurfeUsageEventsToAirtable(entries, opts);
  } catch (err) {
    return {
      ok: false,
      applied: false,
      error: "AIRTABLE_SYNC_FAILED",
      message: String(err?.message || err).slice(0, 160),
      note: "Local ledger retained; Surfe lookup path unaffected.",
    };
  }
}

export function airtableSyncHealthFromLedger(doc = {}) {
  const entries = Array.isArray(doc.entries) ? doc.entries : [];
  let pending = 0;
  let synced = 0;
  let failed = 0;
  let fixtures = 0;
  for (const e of entries) {
    const cls = classifyUsageClass(e);
    if (cls === "FIXTURE_TEST") {
      fixtures += 1;
      continue;
    }
    const st = String(e.airtableSyncStatus || "pending");
    if (st === "synced") synced += 1;
    else if (st === "failed") failed += 1;
    else pending += 1;
  }
  return {
    pending,
    synced,
    failed,
    fixturesExcluded: fixtures,
    tableStatus: PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE.status,
    proposedTableName: PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE.tableName,
    proposedBaseId: PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE.baseId,
  };
}
