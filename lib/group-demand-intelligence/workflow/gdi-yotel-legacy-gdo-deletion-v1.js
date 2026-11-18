/**
 * YOTEL legacy GDO deletion V1 — exact allowlist only.
 * Founder-authorized for recrPQcZg7SFARRb2 Group Demand Opportunities.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

export const GDI_YOTEL_LEGACY_GDO_DELETION_V1 = "gdi-yotel-legacy-gdo-deletion-v1";

export const YOTEL_LEGACY_DELETE_AUTH = Object.freeze({
  hotelId: "recrPQcZg7SFARRb2",
  hotelName: "YOTEL Geneva Lake",
  baseId: "appa2cE7FTRmIbB32",
  tableName: "Group Demand Opportunities",
  tableId: "tblRuReslJMwsfRQj",
  authorizationId: "yotel-legacy-gdo-delete-108-v1",
  expectedCount: 108,
});

function text(v) {
  return String(v || "").trim();
}

export function loadDeleteAllowlist(manifestPath) {
  const doc = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const ids = [...(doc.airtableRecordIdsExact || [])].map(text).filter(Boolean);
  const unique = [...new Set(ids)].sort();
  if (unique.length !== ids.length) {
    const err = new Error("yotel_delete_manifest_duplicate_ids");
    err.code = "DELETE_MANIFEST_DUPLICATE_IDS";
    throw err;
  }
  return {
    manifestPath,
    hotelId: doc.hotel?.hotelId || YOTEL_LEGACY_DELETE_AUTH.hotelId,
    airtableRecordIdsExact: unique,
    opportunityIdsExact: [...(doc.opportunityIdsExact || [])].map(text).filter(Boolean),
    expectedCount: doc.counts?.proposedDeletes ?? unique.length,
  };
}

/**
 * Compare live YOTEL GDO rows to the exact allowlist.
 * Fail closed on any mismatch that would widen or miss scope.
 */
export function preflightYotelLegacyDeletion({
  allowlist,
  liveRawRecords = [],
  localOpportunityCount = null,
} = {}) {
  const allow = new Set(allowlist.airtableRecordIdsExact);
  const liveById = new Map();
  const outOfScope = [];
  const hotelMismatches = [];

  for (const rec of liveRawRecords) {
    const id = text(rec.id);
    const hotelId = text(rec.fields?.hotelId);
    liveById.set(id, rec);
    if (hotelId !== YOTEL_LEGACY_DELETE_AUTH.hotelId) {
      hotelMismatches.push({ airtableRecordId: id, hotelId });
    }
    if (!allow.has(id)) {
      outOfScope.push({
        airtableRecordId: id,
        opportunityId: rec.fields?.opportunityId || null,
        reason: "LIVE_NOT_IN_ALLOWLIST",
      });
    }
  }

  const missingFromLive = [];
  for (const id of allowlist.airtableRecordIdsExact) {
    if (!liveById.has(id)) {
      missingFromLive.push({ airtableRecordId: id, reason: "ALLOWLIST_ID_NOT_IN_LIVE" });
    }
  }

  const liveCount = liveRawRecords.length;
  const allowCount = allowlist.airtableRecordIdsExact.length;
  const exactMatch =
    hotelMismatches.length === 0 &&
    outOfScope.length === 0 &&
    missingFromLive.length === 0 &&
    liveCount === allowCount &&
    allowCount === YOTEL_LEGACY_DELETE_AUTH.expectedCount;

  const localOk =
    localOpportunityCount == null
      ? true
      : Number(localOpportunityCount) === 42;

  return {
    ok: exactMatch && localOk,
    code: exactMatch && localOk ? "PREFLIGHT_PASS" : "PREFLIGHT_BLOCKED",
    liveCount,
    allowCount,
    expectedCount: YOTEL_LEGACY_DELETE_AUTH.expectedCount,
    hotelMismatches,
    liveNotInAllowlist: outOfScope,
    allowlistMissingFromLive: missingFromLive,
    localOpportunityCount,
    localBaselineOk: localOk,
    deletableIds: exactMatch
      ? [...allowlist.airtableRecordIdsExact]
      : [],
  };
}

export function chunkIds(ids, size = 10) {
  const out = [];
  for (let i = 0; i < ids.length; i += size) out.push(ids.slice(i, i + size));
  return out;
}

export function buildDeletionLedgerSkeleton({
  allowlist,
  preflight,
  backupPath,
  generatedAt = new Date().toISOString(),
} = {}) {
  return {
    version: GDI_YOTEL_LEGACY_GDO_DELETION_V1,
    authorizationId: YOTEL_LEGACY_DELETE_AUTH.authorizationId,
    generatedAt,
    hotel: YOTEL_LEGACY_DELETE_AUTH,
    allowlistCount: allowlist.airtableRecordIdsExact.length,
    backupPath,
    preflight,
    batches: [],
    deletedIds: [],
    failed: [],
    completed: false,
  };
}

export function applyBatchResultToLedger(ledger, batch) {
  const next = {
    ...ledger,
    batches: [...(ledger.batches || []), batch],
    deletedIds: [...(ledger.deletedIds || [])],
    failed: [...(ledger.failed || [])],
  };
  if (batch.ok) {
    for (const id of batch.ids || []) {
      if (!next.deletedIds.includes(id)) next.deletedIds.push(id);
    }
  } else {
    next.failed.push(batch);
  }
  return next;
}

export function remainingDeleteIds(allowlistIds, deletedIds) {
  const done = new Set(deletedIds || []);
  return (allowlistIds || []).filter((id) => !done.has(id));
}

export function fingerprintIds(ids = []) {
  return createHash("sha256").update([...ids].sort().join("\n")).digest("hex");
}

export function assertAuthorizedYotelDelete(opts = {}) {
  if (opts.authorizeYotelLegacyDelete !== true) {
    const err = new Error("yotel_legacy_delete_not_authorized");
    err.code = "YOTEL_LEGACY_DELETE_NOT_AUTHORIZED";
    throw err;
  }
  if (text(opts.exactWriteSetAuthorization) !== YOTEL_LEGACY_DELETE_AUTH.authorizationId) {
    const err = new Error("yotel_legacy_delete_authorization_id_mismatch");
    err.code = "YOTEL_DELETE_AUTH_ID_MISMATCH";
    throw err;
  }
  if (opts.apply !== true) {
    const err = new Error("yotel_legacy_delete_apply_flag_required");
    err.code = "YOTEL_DELETE_APPLY_REQUIRED";
    throw err;
  }
  return true;
}

export function writeJson(filePath, doc) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(doc, null, 2));
  return filePath;
}
