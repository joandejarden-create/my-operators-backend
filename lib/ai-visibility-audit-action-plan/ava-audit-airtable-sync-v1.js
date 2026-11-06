/**
 * AVA historical Airtable sync — indexes immutable local archives into the
 * existing AI Demand Positioning (ADP) governed Airtable base.
 *
 * Base: appa2cE7FTRmIbB32 (CANONICAL_INTELLIGENCE / ADP)
 *
 * Reuses existing tables (no new base, no hotel master duplication):
 *   - ADP Report Snapshots      → audit run + report version index
 *   - ADP Monitoring Periods    → execution / period index
 *   - ADP Period Metrics        → ADP-derived KPI references (Measurement Family omitted;
 *                                 Schema/key prefixes isolate from CORE/BPP)
 *   - Hotel Intelligence Evidence → findings, actions, research evidence (Entity Type=Other)
 *
 * NEVER writes to "AI Demand Positioning - Published Reports" (Live ADP overlay).
 * NEVER overwrites CORE/BRAND_PORTFOLIO rows (unique AVA primary keys).
 *
 * Full JSON/PDF remain in durable local archive; Airtable stores hashes + refs.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getAvaAirtableMirrorRoot,
  AVA_AUDIT_EXECUTION_CONTRACT,
} from "./ava-audit-execution-contract-v1.js";
import { loadAuditRun } from "./ava-audit-run-archive-v1.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../decision-outcomes/airtable-base.js";
import { resolveAdpCanonicalBaseId } from "../group-demand-intelligence/canonical-airtable-base.js";
import { HISTORY_TABLES } from "../ai-demand-positioning/longitudinal/airtable-history-schema-final-v1.js";

const MODULE_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function atomicWriteJson(filePath, obj) {
  ensureDir(path.dirname(filePath));
  const tmp = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
  fs.renameSync(tmp, filePath);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function truncate(s, n) {
  const t = String(s ?? "");
  return t.length <= n ? t : `${t.slice(0, n - 1)}…`;
}

function loadEnvFile(abs) {
  if (!fs.existsSync(abs)) return {};
  const out = {};
  for (const line of fs.readFileSync(abs, "utf8").split(/\r?\n/)) {
    if (!line || line.trim().startsWith("#")) continue;
    const i = line.indexOf("=");
    if (i < 0) continue;
    out[line.slice(0, i).trim()] = line.slice(i + 1).trim();
  }
  return out;
}

/** Merge parent repo .env when worktree lacks credentials (never log secrets). */
export function resolveAvaAirtableEnv(env = process.env) {
  const candidates = [
    path.join(process.cwd(), ".env"),
    path.join(process.cwd(), "..", "my-operators-backend", ".env"),
    path.join(MODULE_ROOT, ".env"),
    path.join(MODULE_ROOT, "..", "my-operators-backend", ".env"),
  ];
  let fileEnv = {};
  for (const c of candidates) {
    if (fs.existsSync(c)) fileEnv = { ...fileEnv, ...loadEnvFile(c) };
  }
  return { ...fileEnv, ...env };
}

/** Logical entity → existing ADP table (compatibility map). */
export const AVA_AIRTABLE_ENTITY_TABLES = Object.freeze({
  auditRuns: HISTORY_TABLES.REPORT_SNAPSHOTS, // "ADP Report Snapshots"
  monitoring: HISTORY_TABLES.MONITORING_PERIODS,
  metrics: HISTORY_TABLES.PERIOD_METRICS,
  evidence: "Hotel Intelligence Evidence",
  // Findings + actions also land in Hotel Intelligence Evidence (Entity Type=Other)
  findings: "Hotel Intelligence Evidence",
  actions: "Hotel Intelligence Evidence",
  reportVersions: HISTORY_TABLES.REPORT_SNAPSHOTS,
});

/** Forbidden table — Live ADP publish overlay. */
export const AVA_AIRTABLE_FORBIDDEN_TABLES = Object.freeze([
  "AI Demand Positioning - Published Reports",
]);

export const AVA_AIRTABLE_PRODUCT_MARK = "AVA_AUDIT_V1";

export function resolveAvaAdpAirtableBase(env = process.env) {
  const merged = resolveAvaAirtableEnv(env);
  try {
    const resolved = resolveAdpCanonicalBaseId(merged, {
      allowGenericFallback: false,
    });
    return {
      ok: true,
      baseId: resolved.baseId || CANONICAL_INTELLIGENCE_BASE_ID,
      source: resolved.source,
      status: resolved.status,
    };
  } catch {
    // Explicit governed default when ADP_AIRTABLE_BASE_ID unset but credentials exist
    if (merged.AIRTABLE_API_KEY || merged.AIRTABLE_PAT) {
      return {
        ok: true,
        baseId: CANONICAL_INTELLIGENCE_BASE_ID,
        source: "CANONICAL_INTELLIGENCE_BASE_ID_default",
        status: "SET_CANONICAL",
      };
    }
    return { ok: false, error: "AIRTABLE_CREDENTIALS_OR_BASE_MISSING" };
  }
}

/**
 * Build rows mapped onto existing ADP / Hotel Intelligence tables.
 */
export function buildAirtableSyncBundle(auditRunId, env = process.env) {
  const loaded = loadAuditRun(auditRunId, env);
  if (!loaded.ok) return { ok: false, ...loaded };

  const identity = loaded.identity || {};
  const payload = loaded.payload || {};
  const propertyId = loaded.propertyId || identity.propertyId;
  const adpPeriod = identity.adpPeriodId || "unknown_period";
  const snapshotId = auditRunId; // primary key on ADP Report Snapshots
  const periodKey = `ava|${propertyId}|${auditRunId}`;
  const storeRef = path
    .relative(process.cwd(), loaded.dir)
    .replace(/\\/g, "/");

  const snapshotRow = {
    table: HISTORY_TABLES.REPORT_SNAPSHOTS,
    primaryField: "Snapshot ID",
    primaryValue: snapshotId,
    idempotencyKey: `snap:${snapshotId}`,
    fields: {
      "Snapshot ID": snapshotId,
      "Property ID": propertyId,
      "Period ID": adpPeriod,
      "Monitoring Date": (identity.createdAt || "").slice(0, 10) || undefined,
      "Schema Version": AVA_AUDIT_EXECUTION_CONTRACT,
      "Content Hash": identity.contentSha256 || null,
      "Envelope Hash": identity.identityFingerprintSha256 || null,
      "Renderer Version": identity.avaTemplateVersion || null,
      "Measurement Contract Version": identity.adpMethodologyVersion || null,
      "Certification Status": identity.certificationStatus || null,
      "Is Current Historical Version": true,
      "Snapshot Store Ref": storeRef,
      "Synthetic": false,
      // Measurement Family intentionally omitted — select only allows CORE|BRAND_PORTFOLIO.
      // Isolation via Snapshot ID prefix AVA-RUN- + Schema Version.
      "Publication Version": identity.crossReportStatus || null,
      "Report Edition": AVA_AIRTABLE_PRODUCT_MARK,
      "Customer Visible Content Hash": identity.pdfSha256 || null,
      "Metrics Version": identity.adpPackageId || null,
      "Assurance Version": identity.adpCertificationFingerprint || null,
    },
  };

  const monitoringRow = {
    table: HISTORY_TABLES.MONITORING_PERIODS,
    primaryField: "Period Key",
    primaryValue: periodKey,
    idempotencyKey: `mon:${periodKey}`,
    fields: {
      "Period Key": periodKey,
      "Property ID": propertyId,
      "Period ID": adpPeriod,
      "Snapshot ID": snapshotId,
      "Monitoring Date": (identity.createdAt || "").slice(0, 10) || undefined,
      "Certification Timestamp": identity.createdAt || undefined,
      "Certification Status": identity.certificationStatus || null,
      "Report Schema Version": AVA_AUDIT_EXECUTION_CONTRACT,
      "Persistence State": "COMPLETE",
      "Content Hash": identity.contentSha256 || null,
      "Envelope Hash": identity.pdfSha256 || null,
      "Historical Version": 1,
      "Is Current Historical Version": true,
      "Synthetic": false,
      "Families Present JSON": JSON.stringify([AVA_AIRTABLE_PRODUCT_MARK]),
    },
  };

  const metricRows = (payload.kpis || []).map((k) => {
    const metricId = String(k.label || "KPI")
      .toUpperCase()
      .replace(/\s+/g, "_");
    const metricsKey = `ava|${auditRunId}|${metricId}`;
    const valueStr = String(k.value ?? "");
    const pct = Number(String(valueStr).replace(/[^\d.-]/g, ""));
    const fields = {
      "Metrics Key": metricsKey,
      "Property ID": propertyId,
      "Period ID": adpPeriod,
      "Historical Version": 1,
      "Certification Status": identity.certificationStatus || null,
      "Disclosures JSON": JSON.stringify({
        product: AVA_AIRTABLE_PRODUCT_MARK,
        auditRunId,
        label: k.label,
        value: valueStr,
        meta: k.meta || null,
        adpPackageId: identity.adpPackageId,
        adpFingerprint: identity.adpCertificationFingerprint,
        measurementStatus: /^0(\.0+)?%?$/.test(valueStr.trim())
          ? "MEASURED_ZERO"
          : "MEASURED_VALUE",
      }),
      "Metrics Version": identity.adpPackageId || null,
      "Customer Publication Version": identity.crossReportStatus || null,
    };
    if (/AI Consideration/i.test(k.label) && Number.isFinite(pct)) {
      fields["Consideration Rate"] = pct;
    }
    if (/Scenario Presence/i.test(k.label) && Number.isFinite(pct)) {
      fields["Scenario Presence"] = pct;
    }
    if (/Reality Coverage/i.test(k.label) && Number.isFinite(pct)) {
      fields["Reality Coverage"] = pct;
    }
    return {
      table: HISTORY_TABLES.PERIOD_METRICS,
      primaryField: "Metrics Key",
      primaryValue: metricsKey,
      idempotencyKey: `metric:${metricsKey}`,
      fields,
    };
  });

  const evidenceRows = [];
  const findings = payload.priorityFindings || payload.findings || [];
  findings.forEach((f, i) => {
    const stableId =
      f.stableFindingId ||
      f.findingId ||
      f.id ||
      `finding_${i}_${String(f.title || "").slice(0, 24)}`;
    const evidenceId = `ava|${auditRunId}|finding|${stableId}`;
    evidenceRows.push({
      table: "Hotel Intelligence Evidence",
      primaryField: "Evidence ID",
      primaryValue: evidenceId,
      idempotencyKey: `finding:${evidenceId}`,
      fields: {
        "Evidence ID": evidenceId,
        "Entity Type": "Other",
        "Entity Record ID": auditRunId,
        "Field Name": truncate(f.category || f.claimType || "AVA_FINDING", 80),
        "Value Observed": truncate(f.title || f.headline || "", 500),
        "Evidence Snippet": truncate(
          f.body || f.what || f.whatWeFound || f.description || "",
          1200
        ),
        "Source Type": "Public Research",
        "Source Name": AVA_AIRTABLE_PRODUCT_MARK,
        "Source URL": Array.isArray(f.evidence)
          ? truncate(String(f.evidence[0] || ""), 500)
          : truncate(String(f.evidenceUrl || ""), 500) || undefined,
        "Retrieved At": identity.createdAt || undefined,
        Confidence: f.confidence || undefined,
        "Current?": true,
        Notes: truncate(
          JSON.stringify({
            kind: "AVA_FINDING",
            priority: f.priority || null,
            status: f.status || "OBSERVED",
            propertyId,
            adpPeriodId: adpPeriod,
            storeRef,
          }),
          5000
        ),
        "Schema Version": AVA_AUDIT_EXECUTION_CONTRACT,
      },
    });
  });

  const actions =
    payload.customerActionRegister?.actions || payload.actions || [];
  actions.forEach((a, i) => {
    const stableId = a.stableActionId || a.actionId || a.id || `action_${i}`;
    const evidenceId = `ava|${auditRunId}|action|${stableId}`;
    evidenceRows.push({
      table: "Hotel Intelligence Evidence",
      primaryField: "Evidence ID",
      primaryValue: evidenceId,
      idempotencyKey: `action:${evidenceId}`,
      fields: {
        "Evidence ID": evidenceId,
        "Entity Type": "Other",
        "Entity Record ID": auditRunId,
        "Field Name": truncate(a.priority || "AVA_ACTION", 80),
        "Value Observed": truncate(
          a.actionTitle || a.headline || a.action || a.title || "",
          500
        ),
        "Evidence Snippet": truncate(
          Array.isArray(a.implementationSteps)
            ? a.implementationSteps.join(" | ")
            : a.desiredOutcome || a.whatWeFound || "",
          1200
        ),
        "Source Type": "Model Derived",
        "Source Name": AVA_AIRTABLE_PRODUCT_MARK,
        "Retrieved At": identity.createdAt || undefined,
        "Current?": true,
        Notes: truncate(
          JSON.stringify({
            kind: "AVA_ACTION",
            workstream: a.workstreamTheme || a.owner || null,
            propertyId,
            adpPeriodId: adpPeriod,
            storeRef,
          }),
          5000
        ),
        "Schema Version": AVA_AUDIT_EXECUTION_CONTRACT,
      },
    });
  });

  return {
    ok: true,
    auditRunId,
    propertyId,
    contract: AVA_AUDIT_EXECUTION_CONTRACT,
    mappingMode: "EXISTING_ADP_TABLES_V1",
    forbiddenTables: AVA_AIRTABLE_FORBIDDEN_TABLES,
    rows: [snapshotRow, monitoringRow, ...metricRows, ...evidenceRows],
    counts: {
      snapshots: 1,
      monitoring: 1,
      metrics: metricRows.length,
      evidence: evidenceRows.length,
      findings: findings.length,
      actions: actions.length,
    },
    certification: loaded.certification || {},
  };
}

async function airtableFetch(baseId, token, tableName, pathSuffix = "", init = {}) {
  const encoded = encodeURIComponent(tableName);
  const url = `https://api.airtable.com/v0/${baseId}/${encoded}${pathSuffix}`;
  let attempt = 0;
  while (attempt < 5) {
    attempt++;
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
      json = { raw: text.slice(0, 400) };
    }
    if (res.status === 429) {
      await sleep(500 * attempt);
      continue;
    }
    return { ok: res.ok, status: res.status, json };
  }
  return { ok: false, status: 429, json: { error: "rate_limited" } };
}

function escapeFormulaString(s) {
  return String(s).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

async function findByPrimary(baseId, token, tableName, primaryField, keyVal) {
  const formula = `{${primaryField}}="${escapeFormulaString(keyVal)}"`;
  return airtableFetch(
    baseId,
    token,
    tableName,
    `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=3`
  );
}

function sanitizeFields(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return out;
}

/**
 * Idempotent upsert: find by primary key → patch or create.
 */
async function upsertRow(baseId, token, row) {
  if (AVA_AIRTABLE_FORBIDDEN_TABLES.includes(row.table)) {
    return {
      ok: false,
      error: "FORBIDDEN_TABLE",
      table: row.table,
      message: "Refusing to write AVA history into ADP Live Published Reports",
    };
  }
  const found = await findByPrimary(
    baseId,
    token,
    row.table,
    row.primaryField,
    row.primaryValue
  );
  if (!found.ok) {
    return {
      ok: false,
      error: "LOOKUP_FAILED",
      status: found.status,
      detail: found.json,
      idempotencyKey: row.idempotencyKey,
    };
  }
  const existing = (found.json.records || [])[0];
  const fields = sanitizeFields(row.fields);
  if (existing) {
    const patched = await airtableFetch(
      baseId,
      token,
      row.table,
      `/${existing.id}`,
      { method: "PATCH", body: JSON.stringify({ fields, typecast: false }) }
    );
    return {
      ok: patched.ok,
      action: "PATCH",
      recordId: existing.id,
      status: patched.status,
      detail: patched.ok ? null : patched.json,
      idempotencyKey: row.idempotencyKey,
      duplicatePrevented: true,
    };
  }
  const created = await airtableFetch(baseId, token, row.table, "", {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: false }),
  });
  const rec = created.json?.records?.[0];
  return {
    ok: created.ok,
    action: "CREATE",
    recordId: rec?.id || null,
    status: created.status,
    detail: created.ok ? null : created.json,
    idempotencyKey: row.idempotencyKey,
    duplicatePrevented: false,
  };
}

export async function synchronizeAuditRunToAirtable(auditRunId, opts = {}) {
  const env = resolveAvaAirtableEnv(opts.env || process.env);
  const bundle = buildAirtableSyncBundle(auditRunId, env);
  if (!bundle.ok) return bundle;

  const mirrorRoot = getAvaAirtableMirrorRoot(env);
  const mirrorDir = path.join(mirrorRoot, bundle.propertyId, auditRunId);
  ensureDir(mirrorDir);
  atomicWriteJson(path.join(mirrorDir, "sync-bundle.json"), bundle);

  const liveEnabled =
    opts.forceLive === true ||
    String(env.AVA_AUDIT_AIRTABLE_SYNC_ENABLED || "").trim() === "1";

  if (!liveEnabled) {
    const status = {
      auditRunId,
      status: "LOCAL_MIRROR_ONLY",
      archiveComplete: false,
      airtableIndexed: false,
      airtableReadAccess: null,
      schemaCompatible: true,
      mappingMode: bundle.mappingMode,
      reason:
        "Live ADP Airtable sync disabled. Set AVA_AUDIT_AIRTABLE_SYNC_ENABLED=1 to write into existing ADP tables.",
      mirroredAt: new Date().toISOString(),
      mirrorPath: mirrorDir,
    };
    atomicWriteJson(path.join(mirrorDir, "sync-status.json"), status);
    return { ok: true, ...status, bundle };
  }

  const apiKey = env.AIRTABLE_API_KEY || env.AIRTABLE_PAT;
  const baseInfo = resolveAvaAdpAirtableBase(env);
  if (!apiKey || !baseInfo.ok) {
    const status = {
      auditRunId,
      status: "PENDING_CREDENTIALS",
      archiveComplete: false,
      airtableIndexed: false,
      reason: baseInfo.error || "Missing AIRTABLE_API_KEY/PAT",
      mirroredAt: new Date().toISOString(),
      mirrorPath: mirrorDir,
    };
    atomicWriteJson(path.join(mirrorDir, "sync-status.json"), status);
    return { ok: false, ...status, bundle };
  }

  // Read-access probe
  const probe = await airtableFetch(
    baseInfo.baseId,
    apiKey,
    HISTORY_TABLES.REPORT_SNAPSHOTS,
    "?maxRecords=1"
  );
  if (!probe.ok) {
    const status = {
      auditRunId,
      status: "AIRTABLE_READ_ACCESS_FAIL",
      archiveComplete: false,
      airtableIndexed: false,
      reason: `Cannot read ${HISTORY_TABLES.REPORT_SNAPSHOTS}`,
      httpStatus: probe.status,
      baseIdPrefix: String(baseInfo.baseId).slice(0, 10) + "…",
      mirroredAt: new Date().toISOString(),
      mirrorPath: mirrorDir,
    };
    atomicWriteJson(path.join(mirrorDir, "sync-status.json"), status);
    return { ok: false, ...status, bundle };
  }

  const results = [];
  for (const row of bundle.rows) {
    const r = await upsertRow(baseInfo.baseId, apiKey, row);
    results.push({ table: row.table, primaryValue: row.primaryValue, ...r });
    await sleep(220);
  }

  // Read-after-write: snapshot must exist
  const verify = await findByPrimary(
    baseInfo.baseId,
    apiKey,
    HISTORY_TABLES.REPORT_SNAPSHOTS,
    "Snapshot ID",
    auditRunId
  );
  const verifiedRec = (verify.json?.records || [])[0];
  const writeFails = results.filter((r) => !r.ok);
  const airtableIndexed =
    verify.ok && verifiedRec && writeFails.length === 0;

  const status = {
    auditRunId,
    status: airtableIndexed ? "AIRTABLE_INDEXED" : "AIRTABLE_PARTIAL_FAILURE",
    archiveComplete: airtableIndexed,
    airtableIndexed,
    airtableReadAccess: true,
    schemaCompatible: true,
    mappingMode: bundle.mappingMode,
    baseIdPrefix: String(baseInfo.baseId).slice(0, 10) + "…",
    baseSource: baseInfo.source,
    counts: bundle.counts,
    writeResults: results.map((r) => ({
      table: r.table,
      action: r.action,
      ok: r.ok,
      recordId: r.recordId,
      idempotencyKey: r.idempotencyKey,
      duplicatePrevented: r.duplicatePrevented,
      error: r.error || null,
      status: r.status,
    })),
    readAfterWrite: {
      ok: Boolean(verifiedRec),
      recordId: verifiedRec?.id || null,
      contentHash: verifiedRec?.fields?.["Content Hash"] || null,
      storeRef: verifiedRec?.fields?.["Snapshot Store Ref"] || null,
      certificationStatus: verifiedRec?.fields?.["Certification Status"] || null,
    },
    mirroredAt: new Date().toISOString(),
    mirrorPath: mirrorDir,
  };
  atomicWriteJson(path.join(mirrorDir, "sync-status.json"), status);
  return { ok: airtableIndexed, ...status, bundle };
}

export async function retryAirtableSync(auditRunId, opts = {}) {
  return synchronizeAuditRunToAirtable(auditRunId, opts);
}

/**
 * Idempotency certification: sync twice, assert single snapshot record.
 */
export async function certifyAirtableIdempotency(auditRunId, opts = {}) {
  const first = await synchronizeAuditRunToAirtable(auditRunId, {
    ...opts,
    forceLive: true,
  });
  const second = await synchronizeAuditRunToAirtable(auditRunId, {
    ...opts,
    forceLive: true,
  });
  const env = resolveAvaAirtableEnv(opts.env || process.env);
  const apiKey = env.AIRTABLE_API_KEY || env.AIRTABLE_PAT;
  const baseInfo = resolveAvaAdpAirtableBase(env);
  const listed = await findByPrimary(
    baseInfo.baseId,
    apiKey,
    HISTORY_TABLES.REPORT_SNAPSHOTS,
    "Snapshot ID",
    auditRunId
  );
  const count = (listed.json?.records || []).length;
  return {
    ok: first.ok && second.ok && count === 1,
    firstOk: first.ok,
    secondOk: second.ok,
    secondWasPatch: (second.writeResults || []).some(
      (r) => r.table === HISTORY_TABLES.REPORT_SNAPSHOTS && r.action === "PATCH"
    ),
    snapshotRecordCount: count,
    first,
    second,
  };
}
