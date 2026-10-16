/**
 * AI Demand Positioning — Airtable published report validation + upsert/read.
 */

import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  map_adp_published_report as MAP,
  isAllowedPublishStatus,
  isValidCensusRecordId,
} from "./airtable-field-map.js";
import { resolveAdpCanonicalBaseId } from "../group-demand-intelligence/canonical-airtable-base.js";
import { getAdpPersistencePolicy } from "./adp-persistence-policy.js";

const MAX_JSON_FIELD_CHARS = 95000;
const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "../..");
const PUBLISHED_ROOT = join(REPO_ROOT, "data/ai-demand-positioning/published");

/** Stable incomplete/error codes for compacted Airtable overlay reads. */
export const ADP_AIRTABLE_PAYLOAD_READ = Object.freeze({
  COMPLETE: "complete",
  RESOLVED_FROM_PRIMARY_SOT: "resolved_from_primary_sot",
  INCOMPLETE_STORAGE_STUB: "incomplete_storage_stub",
  INVALID_STORE_REF: "invalid_store_ref",
  PROPERTY_MISMATCH: "property_mismatch",
  PERIOD_MISMATCH: "period_mismatch",
  FIELD_MISMATCH: "field_mismatch",
  PRIMARY_SOT_UNAVAILABLE: "primary_sot_unavailable",
  PRIMARY_SOT_UNREADABLE: "primary_sot_unreadable",
});

/** Nested payload keys eligible for storage-only stubs when the Airtable overlay is over limit. */
const PAYLOAD_OVERLAY_STUB_KEYS = Object.freeze([
  "evidence",
  "competitiveRankingByTerritory",
  "executiveRead",
  "intentPresenceIndex",
  "lostDemand",
  "realityGap",
  "competitiveSet",
  "whiteSpace",
  "metricScopeLabels",
  "trends",
  "brief",
]);

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!apiKey) {
    const err = new Error("Set AIRTABLE_API_KEY (or AIRTABLE_PAT) and ADP_AIRTABLE_BASE_ID");
    err.statusCode = 500;
    throw err;
  }
  // Fail-closed: refuse silent fallback to non-canonical AIRTABLE_BASE_ID
  const { baseId } = resolveAdpCanonicalBaseId();
  // Live overlay is optional relative to filesystem PRIMARY_SOT — policy is explicit
  getAdpPersistencePolicy();
  return new Airtable({ apiKey }).base(baseId);
}

function truncateJson(value, label) {
  const json = typeof value === "string" ? value : JSON.stringify(value);
  if (json.length <= MAX_JSON_FIELD_CHARS) return json;
  const err = new Error(`${label} exceeds Airtable field limit (${json.length} chars)`);
  err.statusCode = 400;
  throw err;
}

/**
 * Compact customer report payload for the optional Airtable Live overlay.
 * Full payload remains on filesystem PRIMARY_SOT (payloadStoreRef).
 * Never truncates mid-string; stubs whole nested blobs with stable refs.
 */
export function compactPayloadForAirtableOverlay(payload, opts = {}) {
  const storeRef = String(opts.payloadStoreRef || "").trim();
  const periodId = payload?.period?.periodId || payload?.periodId || null;
  const propertyId = payload?.property?.propertyId || payload?.propertyId || null;
  if (!payload || typeof payload !== "object") {
    return { payload, compacted: false, stubbedKeys: [] };
  }
  let working = payload;
  const stubbedKeys = [];
  let json = JSON.stringify(working);
  if (json.length <= MAX_JSON_FIELD_CHARS) {
    return { payload: working, compacted: false, stubbedKeys };
  }
  working = { ...working };
  for (const key of PAYLOAD_OVERLAY_STUB_KEYS) {
    if (working[key] == null) continue;
    const before = JSON.stringify(working[key]).length;
    if (before < 400) continue;
    working[key] = {
      ok: true,
      storageOnly: true,
      payloadStoreRef: storeRef,
      field: key,
      periodId,
      propertyId,
    };
    stubbedKeys.push(key);
    json = JSON.stringify(working);
    if (json.length <= MAX_JSON_FIELD_CHARS) break;
  }
  if (json.length > MAX_JSON_FIELD_CHARS) {
    const err = new Error(
      `Payload JSON exceeds Airtable field limit after compaction (${json.length} chars; stubbed=${stubbedKeys.join(",")})`
    );
    err.statusCode = 400;
    err.stubbedKeys = stubbedKeys;
    throw err;
  }
  return { payload: working, compacted: stubbedKeys.length > 0, stubbedKeys };
}

export function isStorageOnlyStub(value) {
  return Boolean(value && typeof value === "object" && value.storageOnly === true);
}

export function listStorageOnlyStubKeys(payload) {
  if (!payload || typeof payload !== "object") return [];
  return Object.keys(payload).filter((k) => isStorageOnlyStub(payload[k]));
}

/**
 * Validate payloadStoreRef / evidenceStoreRef against PRIMARY_SOT published root.
 * Rejects path traversal, absolute paths outside published/, and cross-hotel refs.
 */
export function validateAdpPublishedStoreRef(storeRef, { propertyId, periodId, kind = "report" } = {}) {
  const raw = String(storeRef || "").trim().replace(/\\/g, "/");
  if (!raw) {
    return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF, error: "store_ref_empty" };
  }
  if (raw.includes("\0") || raw.includes("..")) {
    return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF, error: "path_traversal_rejected" };
  }
  const expectedProp = String(propertyId || "").trim();
  if (!expectedProp) {
    return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF, error: "property_id_required" };
  }
  const prefix = `published/${expectedProp}/`;
  if (!raw.startsWith(prefix)) {
    return {
      ok: false,
      code: ADP_AIRTABLE_PAYLOAD_READ.PROPERTY_MISMATCH,
      error: "store_ref_outside_property",
    };
  }
  const fileName = raw.slice(prefix.length);
  if (!fileName || fileName.includes("/") || fileName.includes("\\")) {
    return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF, error: "nested_path_rejected" };
  }
  const expectedPeriod = String(periodId || "").trim();
  if (kind === "report") {
    const m = /^report-(.+)\.json$/.exec(fileName);
    if (!m) {
      return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF, error: "report_filename_invalid" };
    }
    if (expectedPeriod && m[1] !== expectedPeriod) {
      return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.PERIOD_MISMATCH, error: "period_mismatch" };
    }
  } else if (kind === "evidence") {
    const m = /^evidence-(.+)\.json$/.exec(fileName);
    if (!m) {
      return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF, error: "evidence_filename_invalid" };
    }
    if (expectedPeriod && m[1] !== expectedPeriod) {
      return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.PERIOD_MISMATCH, error: "period_mismatch" };
    }
  } else {
    return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF, error: "unknown_kind" };
  }

  const abs = resolve(join(PUBLISHED_ROOT, expectedProp, fileName));
  const publishedRootResolved = resolve(PUBLISHED_ROOT) + sep;
  if (!abs.startsWith(publishedRootResolved) && abs !== resolve(PUBLISHED_ROOT)) {
    return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF, error: "resolved_outside_published_root" };
  }
  return { ok: true, absolutePath: abs, relativePath: raw, fileName };
}

function readPrimarySotJson(absPath) {
  if (!existsSync(absPath)) {
    return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.PRIMARY_SOT_UNAVAILABLE, error: "file_missing" };
  }
  try {
    const parsed = JSON.parse(readFileSync(absPath, "utf8"));
    return { ok: true, doc: parsed };
  } catch (err) {
    return {
      ok: false,
      code: ADP_AIRTABLE_PAYLOAD_READ.PRIMARY_SOT_UNREADABLE,
      error: err?.message || "unreadable",
    };
  }
}

/**
 * Resolve compacted Airtable overlay stubs from the authoritative filesystem report.
 * Never returns a customer payload that still contains storageOnly stubs.
 *
 * @returns {{ ok: true, payload, status, stubbedKeysResolved } | { ok: false, code, error, stubbedKeys }}
 */
export function resolveCompactedAirtablePayload(airtablePayload, opts = {}) {
  if (!airtablePayload || typeof airtablePayload !== "object") {
    return { ok: false, code: ADP_AIRTABLE_PAYLOAD_READ.INCOMPLETE_STORAGE_STUB, error: "payload_missing" };
  }
  const stubbedKeys = listStorageOnlyStubKeys(airtablePayload);
  if (stubbedKeys.length === 0) {
    return {
      ok: true,
      payload: airtablePayload,
      status: ADP_AIRTABLE_PAYLOAD_READ.COMPLETE,
      stubbedKeysResolved: [],
    };
  }

  const expectedPropertyId = String(
    opts.propertyId || airtablePayload.property?.propertyId || airtablePayload.propertyId || ""
  ).trim();
  const expectedPeriodId = String(
    opts.periodId || airtablePayload.period?.periodId || airtablePayload.periodId || ""
  ).trim();

  // All stubs must agree on store ref / property / period / field identity.
  let storeRef = String(opts.payloadStoreRef || "").trim();
  for (const key of stubbedKeys) {
    const stub = airtablePayload[key];
    if (!isStorageOnlyStub(stub)) continue;
    if (stub.field && stub.field !== key) {
      return {
        ok: false,
        code: ADP_AIRTABLE_PAYLOAD_READ.FIELD_MISMATCH,
        error: `stub_field_mismatch:${key}`,
        stubbedKeys,
      };
    }
    if (stub.propertyId && expectedPropertyId && stub.propertyId !== expectedPropertyId) {
      return {
        ok: false,
        code: ADP_AIRTABLE_PAYLOAD_READ.PROPERTY_MISMATCH,
        error: `stub_property_mismatch:${key}`,
        stubbedKeys,
      };
    }
    if (stub.periodId && expectedPeriodId && stub.periodId !== expectedPeriodId) {
      return {
        ok: false,
        code: ADP_AIRTABLE_PAYLOAD_READ.PERIOD_MISMATCH,
        error: `stub_period_mismatch:${key}`,
        stubbedKeys,
      };
    }
    if (!storeRef && stub.payloadStoreRef) storeRef = String(stub.payloadStoreRef).trim();
    if (stub.payloadStoreRef && storeRef && stub.payloadStoreRef !== storeRef) {
      return {
        ok: false,
        code: ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF,
        error: "conflicting_payload_store_refs",
        stubbedKeys,
      };
    }
  }

  const validated = validateAdpPublishedStoreRef(storeRef, {
    propertyId: expectedPropertyId,
    periodId: expectedPeriodId,
    kind: "report",
  });
  if (!validated.ok) {
    return { ok: false, code: validated.code, error: validated.error, stubbedKeys };
  }

  const primary = readPrimarySotJson(validated.absolutePath);
  if (!primary.ok) {
    return { ok: false, code: primary.code, error: primary.error, stubbedKeys };
  }
  const full =
    primary.doc?.payload && typeof primary.doc.payload === "object"
      ? primary.doc.payload
      : primary.doc;
  if (!full || typeof full !== "object") {
    return {
      ok: false,
      code: ADP_AIRTABLE_PAYLOAD_READ.PRIMARY_SOT_UNREADABLE,
      error: "primary_payload_missing",
      stubbedKeys,
    };
  }
  const fullPropertyId = String(full.property?.propertyId || full.propertyId || "").trim();
  const fullPeriodId = String(full.period?.periodId || full.periodId || "").trim();
  if (expectedPropertyId && fullPropertyId && fullPropertyId !== expectedPropertyId) {
    return {
      ok: false,
      code: ADP_AIRTABLE_PAYLOAD_READ.PROPERTY_MISMATCH,
      error: "primary_property_mismatch",
      stubbedKeys,
    };
  }
  if (expectedPeriodId && fullPeriodId && fullPeriodId !== expectedPeriodId) {
    return {
      ok: false,
      code: ADP_AIRTABLE_PAYLOAD_READ.PERIOD_MISMATCH,
      error: "primary_period_mismatch",
      stubbedKeys,
    };
  }

  const resolved = { ...airtablePayload };
  for (const key of stubbedKeys) {
    if (full[key] === undefined) {
      return {
        ok: false,
        code: ADP_AIRTABLE_PAYLOAD_READ.PRIMARY_SOT_UNAVAILABLE,
        error: `primary_missing_field:${key}`,
        stubbedKeys,
      };
    }
    if (isStorageOnlyStub(full[key])) {
      return {
        ok: false,
        code: ADP_AIRTABLE_PAYLOAD_READ.INCOMPLETE_STORAGE_STUB,
        error: `primary_also_stubbed:${key}`,
        stubbedKeys,
      };
    }
    resolved[key] = full[key];
  }
  if (listStorageOnlyStubKeys(resolved).length > 0) {
    return {
      ok: false,
      code: ADP_AIRTABLE_PAYLOAD_READ.INCOMPLETE_STORAGE_STUB,
      error: "unresolved_stubs_remain",
      stubbedKeys: listStorageOnlyStubKeys(resolved),
    };
  }
  return {
    ok: true,
    payload: resolved,
    status: ADP_AIRTABLE_PAYLOAD_READ.RESOLVED_FROM_PRIMARY_SOT,
    stubbedKeysResolved: stubbedKeys,
    payloadStoreRef: storeRef,
  };
}

export function validatePublishedReportUpsert(input) {
  const errors = [];
  const manifest = input?.manifest || {};
  const report = input?.report || {};
  const evidenceIndex = input?.evidenceIndex || {};

  if (!manifest.propertyId) errors.push("manifest.propertyId required");
  if (!manifest.latestPeriodId) errors.push("manifest.latestPeriodId required");
  if (!manifest.propertyName) errors.push("manifest.propertyName required");
  if (!report?.payload?.ok) errors.push("report.payload must be ok:true");
  // Evidence index is stored on disk when too large; Airtable gets stub or compact copy.
  if (!evidenceIndex?.ok) errors.push("evidenceIndex must be ok:true");

  const censusRecordId = String(manifest.censusRecordId || input.censusRecordId || "").trim();
  if (censusRecordId && !isValidCensusRecordId(censusRecordId)) {
    errors.push(`invalid Census Record ID format: ${censusRecordId}`);
  }
  const publishStatus = input.publishStatus || manifest.publishStatus || "Live";
  if (!isAllowedPublishStatus(publishStatus)) {
    errors.push(`invalid publishStatus: ${publishStatus}`);
  }

  let payloadJson = null;
  let evidenceIndexJson = null;
  let payloadCompaction = null;
  if (errors.length === 0) {
    try {
      const storeRef =
        input.payloadStoreRef ||
        `published/${manifest.propertyId}/${manifest.reportFile || `report-${manifest.latestPeriodId}.json`}`;
      payloadCompaction = compactPayloadForAirtableOverlay(report.payload, {
        payloadStoreRef: storeRef,
      });
      payloadJson = truncateJson(payloadCompaction.payload, "Payload JSON");
      const fullEvidenceJson = JSON.stringify(evidenceIndex);
      if (fullEvidenceJson.length <= MAX_JSON_FIELD_CHARS) {
        evidenceIndexJson = fullEvidenceJson;
      } else {
        evidenceIndexJson = JSON.stringify({
          ok: true,
          storageOnly: true,
          evidenceStoreRef: input.evidenceStoreRef || storeRef.replace("report-", "evidence-") || "",
          periodId: evidenceIndex.periodId,
          propertyId: evidenceIndex.propertyId,
        });
      }
    } catch (err) {
      errors.push(err.message);
    }
  }

  const payloadStoreRef =
    input.payloadStoreRef ||
    `published/${manifest.propertyId}/${manifest.reportFile || `report-${manifest.latestPeriodId}.json`}`;

  const sanitizedFields = errors.length
    ? null
    : {
        [MAP.reportName]: `${manifest.propertyName} — ${(manifest.latestPublishedAt || "").slice(0, 10)}`,
        [MAP.adpPropertyId]: manifest.propertyId,
        [MAP.periodId]: manifest.latestPeriodId,
        [MAP.propertyName]: manifest.propertyName,
        [MAP.city]: manifest.city || "",
        [MAP.state]: manifest.state || "",
        [MAP.market]: manifest.market || "",
        [MAP.executionDate]: report.payload?.period?.executionDate || manifest.latestPublishedAt,
        [MAP.publishedAt]: manifest.latestPublishedAt,
        [MAP.publishStatus]: publishStatus,
        [MAP.productVersion]: manifest.productVersion || report.productVersion || "",
        [MAP.demandCaptureRate]: manifest.demandCaptureRate ?? report.payload?.demandCapture?.overallRate ?? null,
        [MAP.providerCount]: manifest.providerCount ?? report.payload?.period?.providerCount ?? null,
        [MAP.payloadJson]: payloadJson,
        [MAP.evidenceIndexJson]: evidenceIndexJson,
        [MAP.payloadStoreRef]: payloadStoreRef,
        [MAP.censusRecordId]: censusRecordId,
      };

  return {
    ok: errors.length === 0,
    errors,
    publishStatus,
    sanitizedFields,
    fieldMapping: MAP,
    payloadCompaction: payloadCompaction
      ? {
          compacted: payloadCompaction.compacted === true,
          stubbedKeys: payloadCompaction.stubbedKeys || [],
          payloadStoreRef,
        }
      : null,
  };
}

export async function findPublishedReportByPropertyId(propertyId, { publishStatus = "Live" } = {}) {
  const base = getBase();
  const formula = `AND({${MAP.adpPropertyId}}='${String(propertyId).replace(/'/g, "\\'")}',{${MAP.publishStatus}}='${publishStatus}')`;
  const rows = await base(MAP.table)
    .select({ filterByFormula: formula, maxRecords: 1, sort: [{ field: MAP.publishedAt, direction: "desc" }] })
    .firstPage();
  return rows[0] || null;
}

export async function upsertPublishedReportToAirtable(bundle, options = {}) {
  const validation = validatePublishedReportUpsert({
    manifest: bundle.manifest,
    report: bundle.report,
    evidenceIndex: bundle.evidenceIndex,
    publishStatus: options.publishStatus,
    censusRecordId: options.censusRecordId,
    payloadStoreRef: options.payloadStoreRef,
  });

  if (!validation.ok) {
    const err = new Error(`Published report validation failed: ${validation.errors.join("; ")}`);
    err.statusCode = 400;
    err.validation = validation;
    throw err;
  }

  if (options.dryRun) {
    return {
      ok: true,
      dryRun: true,
      validation,
      sanitizedFieldsPreview: validation.sanitizedFields,
      fieldMapping: MAP,
    };
  }

  const existing = await findPublishedReportByPropertyId(bundle.manifest.propertyId, {
    publishStatus: validation.publishStatus,
  });

  const fields = { ...validation.sanitizedFields };
  // Native linked-record field only when ADP base shares base with Hotel Property Census.
  if (options.linkedCensusRecordIds?.length && process.env.ADP_CENSUS_SAME_BASE === "1") {
    fields[MAP.linkedCensusProperty] = options.linkedCensusRecordIds;
  }

  let record;
  if (existing) {
    record = await baseUpdate(getBase(), MAP.table, existing.id, fields);
  } else {
    record = await getBase()(MAP.table).create(fields);
  }

  return {
    ok: true,
    dryRun: false,
    recordId: record.id,
    validation,
    fieldMapping: MAP,
  };
}

async function baseUpdate(base, table, recordId, fields) {
  return new Promise((resolve, reject) => {
    base(table).update(recordId, fields, (err, record) => {
      if (err) reject(err);
      else resolve(record);
    });
  });
}

/**
 * Load Airtable Live overlay payload for a property.
 *
 * Contract:
 * - Complete payloads return as-is.
 * - Compacted storageOnly stubs are resolved from PRIMARY_SOT via validated payloadStoreRef.
 * - Incomplete / invalid / cross-hotel / unreadable refs return null so callers fall back
 *   to the verified filesystem published snapshot — never customer-visible stubs.
 */
export async function loadPublishedReportFromAirtable(propertyId, { publishStatus = "Live" } = {}) {
  const record = await findPublishedReportByPropertyId(propertyId, { publishStatus });
  if (!record) return null;

  const payloadJson = record.get(MAP.payloadJson);
  if (!payloadJson) return null;

  let payload;
  try {
    payload = typeof payloadJson === "string" ? JSON.parse(payloadJson) : payloadJson;
  } catch (err) {
    console.error("[ADP Airtable] invalid Payload JSON for", propertyId, err.message);
    return null;
  }

  const storeRef = record.get(MAP.payloadStoreRef) || null;
  const periodId = record.get(MAP.periodId) || null;
  const resolved = resolveCompactedAirtablePayload(payload, {
    propertyId: String(propertyId || "").trim(),
    periodId,
    payloadStoreRef: storeRef,
  });
  if (!resolved.ok) {
    console.warn(
      `[ADP Airtable] rejecting incomplete overlay for ${propertyId}: ${resolved.code} (${resolved.error}) — use PRIMARY_SOT fallback`
    );
    return null;
  }
  return resolved.payload;
}

export async function loadPublishedEvidenceFromAirtable(propertyId, { publishStatus = "Live" } = {}) {
  const record = await findPublishedReportByPropertyId(propertyId, { publishStatus });
  if (!record) return null;

  const evidenceJson = record.get(MAP.evidenceIndexJson);
  if (!evidenceJson) return null;

  try {
    const parsed = typeof evidenceJson === "string" ? JSON.parse(evidenceJson) : evidenceJson;
    if (!isStorageOnlyStub(parsed)) return parsed;

    // Evidence stub → resolve from PRIMARY_SOT evidence file; never return stub to callers.
    const property = String(propertyId || "").trim();
    const periodId = String(parsed.periodId || record.get(MAP.periodId) || "").trim();
    const storeRef =
      String(parsed.evidenceStoreRef || "").trim() ||
      (periodId ? `published/${property}/evidence-${periodId}.json` : "");
    const validated = validateAdpPublishedStoreRef(storeRef, {
      propertyId: property,
      periodId,
      kind: "evidence",
    });
    if (!validated.ok) {
      console.warn(
        `[ADP Airtable] evidence stub rejected for ${propertyId}: ${validated.code} — PRIMARY_SOT fallback`
      );
      return null;
    }
    const primary = readPrimarySotJson(validated.absolutePath);
    if (!primary.ok || isStorageOnlyStub(primary.doc)) {
      console.warn(
        `[ADP Airtable] evidence PRIMARY_SOT unavailable for ${propertyId}: ${primary.code || "stub"}`
      );
      return null;
    }
    return primary.doc;
  } catch (err) {
    console.error("[ADP Airtable] invalid Evidence Index JSON for", propertyId, err.message);
    return null;
  }
}

export async function loadPublishedManifestFromAirtable(propertyId, { publishStatus = "Live" } = {}) {
  const record = await findPublishedReportByPropertyId(propertyId, { publishStatus });
  if (!record) return null;

  return {
    propertyId: record.get(MAP.adpPropertyId),
    propertyName: record.get(MAP.propertyName),
    latestPeriodId: record.get(MAP.periodId),
    latestPublishedAt: record.get(MAP.publishedAt),
    publishStatus: record.get(MAP.publishStatus),
    censusRecordId: record.get(MAP.censusRecordId) || null,
    airtableRecordId: record.id,
    demandCaptureRate: record.get(MAP.demandCaptureRate),
    providerCount: record.get(MAP.providerCount),
    payloadStoreRef: record.get(MAP.payloadStoreRef),
  };
}
