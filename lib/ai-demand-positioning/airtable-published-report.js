/**
 * AI Demand Positioning — Airtable published report validation + upsert/read.
 */

import Airtable from "airtable";
import {
  map_adp_published_report as MAP,
  isAllowedPublishStatus,
  isValidCensusRecordId,
} from "./airtable-field-map.js";
import { resolveAdpCanonicalBaseId } from "../group-demand-intelligence/canonical-airtable-base.js";
import { getAdpPersistencePolicy } from "./adp-persistence-policy.js";

const MAX_JSON_FIELD_CHARS = 95000;

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

export async function loadPublishedReportFromAirtable(propertyId, { publishStatus = "Live" } = {}) {
  const record = await findPublishedReportByPropertyId(propertyId, { publishStatus });
  if (!record) return null;

  const payloadJson = record.get(MAP.payloadJson);
  if (!payloadJson) return null;

  try {
    const payload = typeof payloadJson === "string" ? JSON.parse(payloadJson) : payloadJson;
    return payload;
  } catch (err) {
    console.error("[ADP Airtable] invalid Payload JSON for", propertyId, err.message);
    return null;
  }
}

export async function loadPublishedEvidenceFromAirtable(propertyId, { publishStatus = "Live" } = {}) {
  const record = await findPublishedReportByPropertyId(propertyId, { publishStatus });
  if (!record) return null;

  const evidenceJson = record.get(MAP.evidenceIndexJson);
  if (!evidenceJson) return null;

  try {
    const parsed = typeof evidenceJson === "string" ? JSON.parse(evidenceJson) : evidenceJson;
    if (parsed?.storageOnly) return null;
    return parsed;
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
