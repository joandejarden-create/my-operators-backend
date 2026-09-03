/**
 * Sandbox-only Airtable writer for Brand & Portfolio Period-1 history.
 *
 * Hard refuses:
 * - baseId === AIRTABLE_BASE_ID (production)
 * - writing Live Published Reports overlay
 * - enabling ADP_AIRTABLE_READ_LIVE
 *
 * Filesystem remains immutable blob SoT; Airtable = structured index.
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import {
  HISTORY_TABLES,
  PERSISTENCE_STATES,
  HISTORY_ENV,
  isAdpHistoryWritesEnabled,
} from "../longitudinal/airtable-history-schema-final-v1.js";
import {
  MEASUREMENT_FAMILY,
  BPP_PERIOD_ID,
  BPP_PUBLICATION_VERSION,
  BPP_ASSET_TOKEN_FROZEN,
} from "./bpp-history-schema-v1.js";
import {
  freezeCustomerPublishedBppPeriod1,
  assertCurrentReportIsolation,
  CUSTOMER_PUBLISHED_PACK_PATH,
} from "./bpp-history-freeze-v1.js";
import { buildBppHistoricalWriteManifest } from "./persist-bpp-historical-period-v1.js";
import { sha256OfPayload } from "../longitudinal/report-snapshot-v1.js";
import {
  resolveProductionHistoryDestination,
  resolveSandboxHistoryBaseId as resolveSandboxBaseFromDest,
  LIVE_OVERLAY_TABLE,
  PRODUCTION_HISTORY_DESTINATION_ISOLATION,
} from "../longitudinal/resolve-adp-history-destination-v1.js";

export const BPP_SANDBOX_WRITER_VERSION = "bpp_sandbox_history_writer_v1";
export const BRAND_PORTFOLIO_SANDBOX_HISTORY_PERSISTENCE_READY =
  "BRAND_PORTFOLIO_SANDBOX_HISTORY_PERSISTENCE_READY";
export const BRAND_PORTFOLIO_PRODUCTION_HISTORY_PERSISTENCE_READY =
  "BRAND_PORTFOLIO_PRODUCTION_HISTORY_PERSISTENCE_READY";
export const SUPPRESSED_METRIC_HISTORY_NULL_INTEGRITY =
  "SUPPRESSED_METRIC_HISTORY_NULL_INTEGRITY";
export const PORTFOLIO_METRIC_TO_PROMPT_TRACEABILITY =
  "PORTFOLIO_METRIC_TO_PROMPT_TRACEABILITY";
export const AIRTABLE_HISTORY_SCHEMA_CAPABILITY_INTEGRITY =
  "AIRTABLE_HISTORY_SCHEMA_CAPABILITY_INTEGRITY";
export { PRODUCTION_HISTORY_DESTINATION_ISOLATION };

const LIVE_OVERLAY = LIVE_OVERLAY_TABLE;

const PRIMARY_KEY_BY_TABLE = Object.freeze({
  [HISTORY_TABLES.MONITORING_PERIODS]: "Period Key",
  [HISTORY_TABLES.REPORT_SNAPSHOTS]: "Snapshot ID",
  [HISTORY_TABLES.PERIOD_METRICS]: "Metrics Key",
  [HISTORY_TABLES.TERRITORY_METRICS]: "Territory Key",
  [HISTORY_TABLES.PROVIDER_METRICS]: "Provider Key",
  [HISTORY_TABLES.COMPETITIVE_RANKINGS]: "Rank Key",
  [HISTORY_TABLES.EVIDENCE_INDEX]: "Evidence Key",
  [HISTORY_TABLES.REPORT_CORRECTIONS]: "Correction ID",
  [HISTORY_TABLES.PROMPT_LEDGER]: "Prompt Ledger Key",
});

export function resolveSandboxHistoryBaseId() {
  const sandbox = resolveSandboxBaseFromDest();
  const production = process.env.AIRTABLE_BASE_ID || "";
  if (!sandbox) throw new Error("Missing ADP_HISTORY_SANDBOX_BASE_ID / AIRTABLE_BASE_ID_SANDBOX");
  if (production && sandbox === production) {
    throw new Error("REFUSED: sandbox base equals production AIRTABLE_BASE_ID");
  }
  return sandbox;
}

export function resolveHistoryWriteBaseId({ mode = "sandbox" } = {}) {
  if (mode === "production") {
    const dest = resolveProductionHistoryDestination({ confirmGovernedAdpBase: true });
    if (!dest.ok) {
      throw new Error(
        `REFUSED production history destination: ${dest.defects.map((d) => d.code).join(",")}`
      );
    }
    return dest.baseId;
  }
  return resolveSandboxHistoryBaseId();
}

export function assertSandboxWriteAllowed({ allowSandboxWrites = false } = {}) {
  if (!allowSandboxWrites) {
    return { allowed: false, reason: "allowSandboxWrites=false" };
  }
  if (process.env.ADP_HISTORY_AIRTABLE_WRITE_APPLY !== "true") {
    return { allowed: false, reason: "ADP_HISTORY_AIRTABLE_WRITE_APPLY not true" };
  }
  if (process.env.ADP_AIRTABLE_READ_LIVE === "1" || process.env.ADP_AIRTABLE_READ_LIVE === "true") {
    return {
      allowed: false,
      reason: "REFUSED: ADP_AIRTABLE_READ_LIVE must remain off during history sandbox writes",
    };
  }
  try {
    const baseId = resolveSandboxHistoryBaseId();
    return { allowed: true, baseId, mode: "SANDBOX" };
  } catch (err) {
    return { allowed: false, reason: err.message };
  }
}

/**
 * Production history write gate (controlled Period-1 promotion only).
 */
export function assertProductionHistoryWriteAllowed({
  allowProductionWrites = false,
} = {}) {
  if (!allowProductionWrites) {
    return { allowed: false, reason: "allowProductionWrites=false" };
  }
  if (!isAdpHistoryWritesEnabled()) {
    return { allowed: false, reason: "ADP_HISTORY_WRITES_ENABLED not true" };
  }
  if (process.env.ADP_HISTORY_AIRTABLE_WRITE_APPLY !== "true") {
    return { allowed: false, reason: "ADP_HISTORY_AIRTABLE_WRITE_APPLY not true" };
  }
  if (process.env.ADP_AIRTABLE_READ_LIVE === "1" || process.env.ADP_AIRTABLE_READ_LIVE === "true") {
    return {
      allowed: false,
      reason: "REFUSED: ADP_AIRTABLE_READ_LIVE must remain off during production history writes",
    };
  }
  const dest = resolveProductionHistoryDestination({ confirmGovernedAdpBase: true });
  if (!dest.ok) {
    return {
      allowed: false,
      reason: dest.defects.map((d) => d.detail || d.code).join("; "),
      isolation: dest.isolation,
    };
  }
  // Ensure env points at resolved base for this controlled run
  if (!process.env.ADP_HISTORY_AIRTABLE_BASE_ID) {
    process.env.ADP_HISTORY_AIRTABLE_BASE_ID = dest.baseId;
  }
  return {
    allowed: true,
    baseId: dest.baseId,
    mode: "PRODUCTION",
    isolation: dest.isolation,
    destination: dest,
  };
}

async function airtableFetch(baseId, token, tableName, pathSuffix = "", init = {}) {
  const encoded = encodeURIComponent(tableName);
  const url = `https://api.airtable.com/v0/${baseId}/${encoded}${pathSuffix}`;
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

function escapeFormulaString(s) {
  return String(s).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

async function patchRecordFields(baseId, token, tableName, recordId, fields) {
  const encoded = encodeURIComponent(tableName);
  const url = `https://api.airtable.com/v0/${baseId}/${encoded}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { ok: res.ok, status: res.status, json };
}

/**
 * Advance Monitoring Period Persistence State only (never mutate content hashes).
 * PENDING/WRITING/VERIFYING → COMPLETE|FAILED. Refuses COMPLETE → mutate content.
 */
export async function setMonitoringPeriodPersistenceState({
  baseId,
  token,
  periodKey,
  state,
}) {
  const found = await findByPrimaryKey(
    baseId,
    token,
    HISTORY_TABLES.MONITORING_PERIODS,
    "Period Key",
    periodKey
  );
  if (!found.ok || !found.records?.length) {
    return { ok: false, detail: `period not found: ${periodKey}`, error: found.error };
  }
  const rec = found.records[0];
  const current = rec.fields?.["Persistence State"];
  if (current === state) return { ok: true, skipped: true, recordId: rec.id, state };
  const patched = await patchRecordFields(
    baseId,
    token,
    HISTORY_TABLES.MONITORING_PERIODS,
    rec.id,
    { "Persistence State": state }
  );
  return {
    ok: patched.ok,
    recordId: rec.id,
    from: current,
    state,
    error: patched.ok ? null : patched.json,
  };
}

async function findByPrimaryKey(baseId, token, tableName, primaryField, value) {
  const formula = `{${primaryField}}="${escapeFormulaString(value)}"`;
  const qs = `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
  const { res, json } = await airtableFetch(baseId, token, tableName, qs);
  if (!res.ok) {
    return { ok: false, error: json, records: [] };
  }
  return { ok: true, records: json.records || [] };
}

function sanitizeFieldsForAirtable(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (v === undefined) continue;
    // Airtable rejects undefined; null clears — we omit null for create to preserve suppressed semantics
    // except we WANT null for suppressed metrics — omit so field is empty (unavailable)
    if (v === null) continue;
    if (typeof v === "number" && Number.isNaN(v)) continue;
    out[k] = v;
  }
  return out;
}

async function createRecordsBatch(baseId, token, tableName, records) {
  const body = {
    records: records.map((fields) => ({ fields: sanitizeFieldsForAirtable(fields) })),
    typecast: true,
  };
  const { res, json } = await airtableFetch(baseId, token, tableName, "", {
    method: "POST",
    body: JSON.stringify(body),
  });
  return { ok: res.ok, status: res.status, json };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Enrich period metrics / snapshot fields with Metrics Version + publication version.
 */
function enrichManifestRecords(manifest, structured, snapshot) {
  return (manifest.records || []).map((rec) => {
    const fields = { ...rec.fields };
    if (rec.table === HISTORY_TABLES.PERIOD_METRICS) {
      fields["Metrics Version"] = structured.metricsVersion || snapshot.metricsVersion || null;
      fields["Customer Publication Version"] =
        structured.publicationVersion || snapshot.publicationVersion || BPP_PUBLICATION_VERSION;
      // Ensure suppressed Cambridge metrics are omitted (null), not 0
      if (structured.periodMetrics?.suppressedKpis?.portfolioBenchmark) {
        delete fields["Portfolio Benchmark"];
      }
      if (structured.periodMetrics?.suppressedKpis?.portfolioPresenceIndex) {
        delete fields["Portfolio Presence Index"];
      }
    }
    if (rec.table === HISTORY_TABLES.REPORT_SNAPSHOTS) {
      fields["Metrics Version"] = snapshot.metricsVersion || null;
    }
    if (rec.table === HISTORY_TABLES.MONITORING_PERIODS) {
      fields["Persistence State"] = PERSISTENCE_STATES.WRITING;
    }
    return { ...rec, fields };
  });
}

/**
 * Write one property's BPP history to sandbox or production history Airtable.
 */
export async function writeBppHistoryPropertyToSandbox({
  snapshot,
  structured,
  dryRun = true,
  allowSandboxWrites = false,
  allowProductionWrites = false,
  mode = "sandbox",
}) {
  const gate =
    mode === "production"
      ? assertProductionHistoryWriteAllowed({
          allowProductionWrites: allowProductionWrites && !dryRun,
        })
      : assertSandboxWriteAllowed({ allowSandboxWrites: allowSandboxWrites && !dryRun });
  const manifest = buildBppHistoricalWriteManifest({
    snapshot,
    structured,
    synthetic: false,
  });
  const records = enrichManifestRecords(manifest, structured, snapshot);

  const result = {
    propertyId: snapshot.propertyId,
    dryRun,
    mode,
    persistenceState: PERSISTENCE_STATES.PENDING,
    ok: manifest.ok,
    defects: [...manifest.defects],
    counts: {},
    created: 0,
    skipped: 0,
    errors: [],
    snapshotId: snapshot.snapshotId,
    contentHash: snapshot.contentHash,
  };

  for (const r of records) {
    result.counts[r.table] = (result.counts[r.table] || 0) + 1;
  }

  if (!manifest.ok) {
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    return result;
  }

  if (dryRun) {
    result.ok = true;
    result.recordCount = records.length;
    return result;
  }

  if (!gate.allowed) {
    result.ok = false;
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    result.defects.push({
      code: mode === "production" ? "PRODUCTION_WRITE_BLOCKED" : "SANDBOX_WRITE_BLOCKED",
      detail: gate.reason,
    });
    return result;
  }

  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = gate.baseId;
  result.baseId = baseId;
  result.persistenceState = PERSISTENCE_STATES.WRITING;

  // Group by table for batching
  const byTable = new Map();
  for (const rec of records) {
    if (rec.table === LIVE_OVERLAY) {
      result.errors.push({ code: "LIVE_OVERLAY_REFUSED", detail: rec.table });
      continue;
    }
    if (!byTable.has(rec.table)) byTable.set(rec.table, []);
    byTable.get(rec.table).push(rec);
  }

  for (const [tableName, tableRecs] of byTable.entries()) {
    const primary = PRIMARY_KEY_BY_TABLE[tableName];
    if (!primary) {
      result.errors.push({ code: "UNKNOWN_TABLE", detail: tableName });
      continue;
    }

    const toCreate = [];
    for (const rec of tableRecs) {
      const keyVal = rec.fields[primary];
      if (!keyVal) {
        result.errors.push({ code: "MISSING_PRIMARY", detail: `${tableName}` });
        continue;
      }
      const found = await findByPrimaryKey(baseId, token, tableName, primary, keyVal);
      await sleep(80);
      if (!found.ok) {
        result.errors.push({
          code: "LOOKUP_FAILED",
          detail: `${tableName}:${keyVal}`,
          error: found.error,
        });
        continue;
      }
      if (found.records.length > 0) {
        // Idempotent skip — refuse silent overwrite of differing finalized content
        const existing = found.records[0].fields || {};
        const existingHash =
          existing["Content Hash"] ||
          existing["Customer Visible Content Hash"] ||
          existing["Response Content Hash"] ||
          null;
        const nextHash =
          rec.fields["Content Hash"] ||
          rec.fields["Customer Visible Content Hash"] ||
          rec.fields["Response Content Hash"] ||
          null;
        if (
          existingHash &&
          nextHash &&
          existingHash !== nextHash &&
          (tableName === HISTORY_TABLES.REPORT_SNAPSHOTS ||
            tableName === HISTORY_TABLES.MONITORING_PERIODS)
        ) {
          result.errors.push({
            code: "HISTORICAL_RECORD_SILENT_OVERWRITE",
            gate: "HISTORICAL_RECORD_NO_SILENT_OVERWRITE",
            detail: `${tableName}:${keyVal}`,
          });
          continue;
        }
        result.skipped += 1;
        continue;
      }
      toCreate.push(rec.fields);
    }

    for (let i = 0; i < toCreate.length; i += 10) {
      const chunk = toCreate.slice(i, i + 10);
      const created = await createRecordsBatch(baseId, token, tableName, chunk);
      await sleep(220);
      if (!created.ok) {
        result.errors.push({
          code: "CREATE_FAILED",
          detail: tableName,
          status: created.status,
          error: created.json,
        });
      } else {
        result.created += (created.json.records || []).length;
      }
    }
  }

  result.persistenceState =
    result.errors.length === 0 ? PERSISTENCE_STATES.VERIFYING : PERSISTENCE_STATES.FAILED;
  result.recordCount = records.length;
  result.ok = result.errors.length === 0;

  // State machine: never leave successful periods in WRITING; never mark COMPLETE on errors.
  const periodRec = records.find((r) => r.table === HISTORY_TABLES.MONITORING_PERIODS);
  const periodKey = periodRec?.fields?.["Period Key"];
  if (periodKey) {
    const nextState = result.ok ? PERSISTENCE_STATES.COMPLETE : PERSISTENCE_STATES.FAILED;
    if (result.ok) result.persistenceState = PERSISTENCE_STATES.VERIFYING;
    const finalized = await setMonitoringPeriodPersistenceState({
      baseId,
      token,
      periodKey,
      state: nextState,
    });
    await sleep(80);
    if (!finalized.ok) {
      result.ok = false;
      result.persistenceState = PERSISTENCE_STATES.FAILED;
      result.errors.push({
        code: "PERSISTENCE_STATE_FINALIZE_FAILED",
        detail: periodKey,
        error: finalized.error || finalized.detail,
      });
    } else {
      result.persistenceState = nextState;
      result.persistenceStateFinalize = finalized;
    }
  }

  return result;
}

/**
 * Read back period metrics + snapshot for KPI verification.
 */
export async function readSandboxPeriodMetrics(baseId, token, propertyId) {
  const formula = `AND({Property ID}="${escapeFormulaString(propertyId)}",{Measurement Family}="${MEASUREMENT_FAMILY.BRAND_PORTFOLIO}",{Period ID}="${escapeFormulaString(BPP_PERIOD_ID)}")`;
  const qs = `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=5`;
  const metrics = await airtableFetch(baseId, token, HISTORY_TABLES.PERIOD_METRICS, qs);
  const snaps = await airtableFetch(baseId, token, HISTORY_TABLES.REPORT_SNAPSHOTS, qs);
  return {
    metrics: metrics.json.records || [],
    snapshots: snaps.json.records || [],
    metricsOk: metrics.res.ok,
    snapsOk: snaps.res.ok,
  };
}

export async function countSandboxTable(baseId, token, tableName, propertyId = null) {
  let offset = null;
  let count = 0;
  do {
    let qs = propertyId
      ? `?pageSize=100&filterByFormula=${encodeURIComponent(`{Property ID}="${escapeFormulaString(propertyId)}"`)}`
      : `?pageSize=100`;
    if (offset) qs += `&offset=${encodeURIComponent(offset)}`;
    // For tables that always have Property ID — all history tables do
    const { res, json } = await airtableFetch(baseId, token, tableName, qs);
    if (!res.ok) return { ok: false, count, error: json };
    count += (json.records || []).length;
    offset = json.offset || null;
    await sleep(80);
  } while (offset);
  return { ok: true, count };
}

export async function countSandboxFamilyRecords(baseId, token) {
  const totals = {};
  for (const tableName of Object.values(HISTORY_TABLES)) {
    // Count BPP family where field exists; monitoring periods use Families Present JSON
    let formula;
    if (tableName === HISTORY_TABLES.MONITORING_PERIODS) {
      formula = `FIND("${MEASUREMENT_FAMILY.BRAND_PORTFOLIO}", {Families Present JSON} & "")`;
    } else if (tableName === HISTORY_TABLES.REPORT_CORRECTIONS) {
      formula = `{Measurement Family}="${MEASUREMENT_FAMILY.BRAND_PORTFOLIO}"`;
    } else {
      formula = `AND({Measurement Family}="${MEASUREMENT_FAMILY.BRAND_PORTFOLIO}",{Period ID}="${escapeFormulaString(BPP_PERIOD_ID)}")`;
    }
    // Monitoring periods use calendar week as Period ID
    if (tableName === HISTORY_TABLES.MONITORING_PERIODS) {
      formula = `FIND("${MEASUREMENT_FAMILY.BRAND_PORTFOLIO}", {Families Present JSON} & "")`;
    }

    let offset = null;
    let count = 0;
    let ok = true;
    let error = null;
    do {
      let qs = `?pageSize=100&filterByFormula=${encodeURIComponent(formula)}`;
      if (offset) qs += `&offset=${encodeURIComponent(offset)}`;
      const { res, json } = await airtableFetch(baseId, token, tableName, qs);
      if (!res.ok) {
        ok = false;
        error = json;
        break;
      }
      count += (json.records || []).length;
      offset = json.offset || null;
      await sleep(100);
    } while (offset);
    totals[tableName] = { ok, count, error };
  }
  return totals;
}

/**
 * Negative overwrite test using a dedicated synthetic key (does not mutate real five).
 */
export async function runNoSilentOverwriteNegativeTest(baseId, token) {
  const table = HISTORY_TABLES.REPORT_SNAPSHOTS;
  const syntheticId = `bpp_snap_NEGATIVE_TEST_${createHash("sha256").update(String(Date.now())).digest("hex").slice(0, 8)}`;
  const fieldsA = {
    "Snapshot ID": syntheticId,
    "Property ID": "adp_negative_overwrite_fixture",
    "Period ID": BPP_PERIOD_ID,
    "Measurement Family": MEASUREMENT_FAMILY.BRAND_PORTFOLIO,
    "Content Hash": "hash_aaa_original",
    "Customer Visible Content Hash": "hash_aaa_original",
    "Publication Version": BPP_PUBLICATION_VERSION,
    Synthetic: true,
    "Certification Status": "SYNTHETIC_NEGATIVE_TEST",
  };
  const create = await createRecordsBatch(baseId, token, table, [fieldsA]);
  if (!create.ok) {
    return { pass: false, detail: "could not create synthetic fixture", error: create.json };
  }
  await sleep(200);

  // Attempt "overwrite" path: find existing then refuse if hash differs
  const found = await findByPrimaryKey(baseId, token, table, "Snapshot ID", syntheticId);
  const existing = found.records?.[0];
  if (!existing) return { pass: false, detail: "fixture missing after create" };

  const attemptedHash = "hash_bbb_mutated";
  const existingHash = existing.fields["Content Hash"];
  const refused = existingHash && existingHash !== attemptedHash;

  // Cleanup synthetic fixture
  const delUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${existing.id}`;
  await fetch(delUrl, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${token}` },
  });

  return {
    pass: Boolean(refused),
    gate: "HISTORICAL_RECORD_NO_SILENT_OVERWRITE",
    detail: refused
      ? "mutation with altered content hash would be refused"
      : "refusal logic failed",
    syntheticId,
  };
}

export async function runFivePropertySandboxWrite({
  dryRun = true,
  allowSandboxWrites = false,
  writeFilesystemFreeze = false,
  mode = "sandbox",
  allowProductionWrites = false,
} = {}) {
  const packBefore = JSON.parse(readFileSync(CUSTOMER_PUBLISHED_PACK_PATH, "utf8"));
  const beforeHash = packBefore.payloadHash || sha256OfPayload(packBefore.payloads);

  const { freezeBundle, frozen } = freezeCustomerPublishedBppPeriod1({
    writeFilesystem: writeFilesystemFreeze,
  });

  const perProperty = [];
  for (const f of frozen) {
    const res = await writeBppHistoryPropertyToSandbox({
      snapshot: f.snapshot,
      structured: f.structured,
      dryRun,
      allowSandboxWrites: mode === "sandbox" ? allowSandboxWrites : false,
      allowProductionWrites: mode === "production" ? allowProductionWrites : false,
      mode,
    });
    perProperty.push(res);
  }

  const isolation = assertCurrentReportIsolation(beforeHash);
  const totals = {};
  for (const p of perProperty) {
    for (const [t, n] of Object.entries(p.counts || {})) {
      totals[t] = (totals[t] || 0) + n;
    }
  }

  let baseId = null;
  try {
    baseId = dryRun
      ? resolveHistoryWriteBaseId({ mode })
      : perProperty[0]?.baseId || resolveHistoryWriteBaseId({ mode });
  } catch {
    baseId = perProperty[0]?.baseId || null;
  }

  return {
    ok: perProperty.every((p) => p.ok),
    dryRun,
    mode,
    freezeBundle,
    perProperty,
    totals,
    recordCount: perProperty.reduce((s, p) => s + (p.recordCount || 0), 0),
    created: perProperty.reduce((s, p) => s + (p.created || 0), 0),
    skipped: perProperty.reduce((s, p) => s + (p.skipped || 0), 0),
    isolation,
    assetToken: BPP_ASSET_TOKEN_FROZEN,
    baseId,
  };
}

export async function runFivePropertyProductionWrite(opts = {}) {
  return runFivePropertySandboxWrite({
    ...opts,
    mode: "production",
    allowSandboxWrites: false,
    allowProductionWrites: opts.allowProductionWrites !== false,
  });
}

/** Expected KPI gold for verification only */
export const BPP_KPI_GOLD_V1 = Object.freeze({
  adp_waterstone_boca_raton: {
    presence: 0.5,
    rank: 2,
    rankOf: 6,
    benchmark: 0.365,
    index: 137,
  },
  adp_renaissance_times_square: {
    presence: 0.472,
    rank: 3,
    rankOf: 6,
    benchmark: 0.411,
    index: 115,
  },
  adp_hotel_phillips_kansas_city: {
    presence: 0.417,
    rank: 2,
    rankOf: 6,
    benchmark: 0.344,
    index: 121,
  },
  adp_cambridge_beaches_bermuda: {
    presence: 0.656,
    rank: 1,
    rankOf: 4,
    benchmark: null,
    index: null,
  },
  adp_now_now_noho: {
    presence: 0.031,
    rank: 6,
    rankOf: 6,
    benchmark: 0.219,
    index: 14,
  },
});

export function near(a, b, eps = 0.0015) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= eps;
}
