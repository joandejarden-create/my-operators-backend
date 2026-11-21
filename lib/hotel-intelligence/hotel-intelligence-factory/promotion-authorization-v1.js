/**
 * Canonical Promotion authorization — fail closed on any binding mismatch.
 *
 * Bound fields:
 *   - manifestSha256 (exact file hash)
 *   - allowedActions
 *   - canonicalBaseId
 *   - recordIdentities (discovery / candidate ids authorized)
 *   - maxBatchSize
 *   - runId
 *   - expiresAt (ISO)
 */

import crypto from "node:crypto";
import fs from "node:fs";

export const HIF_PROMOTION_AUTHORIZATION_V1 = "hotel-intelligence-factory-promotion-authorization-v1";

export const CANONICAL_BASE_ID = "appCCUsuGsE1ifoLk";

export const ALLOWED_PROMOTION_ACTIONS = Object.freeze([
  "LINK_EXISTING",
  "CREATE",
  "ENRICH_NON_DESTRUCTIVE",
  "LINK_CONTRACT",
  "LINK_MILESTONE",
  "QUEUE_HE_ENRICHMENT",
]);

export function sha256Buffer(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

export function sha256File(filePath) {
  return sha256Buffer(fs.readFileSync(filePath));
}

/**
 * Build an authorization object (founder-issued). Not an apply by itself.
 */
export function buildPromotionAuthorization(opts = {}) {
  return {
    version: HIF_PROMOTION_AUTHORIZATION_V1,
    manifestSha256: String(opts.manifestSha256 || "").toLowerCase(),
    allowedActions: [...(opts.allowedActions || [])],
    canonicalBaseId: opts.canonicalBaseId || CANONICAL_BASE_ID,
    recordIdentities: [...(opts.recordIdentities || [])],
    maxBatchSize: Number(opts.maxBatchSize || 0),
    runId: String(opts.runId || ""),
    expiresAt: String(opts.expiresAt || ""),
    issuedAt: opts.issuedAt || new Date().toISOString(),
    issuer: opts.issuer || "FOUNDER",
  };
}

function fail(error, detail = {}) {
  return { ok: false, error, code: 2, ...detail };
}

/**
 * Validate live apply attempt against authorization + manifest file + env.
 */
export function assertPromotionAuthorization(opts = {}) {
  const {
    authorization,
    manifestPath,
    runId,
    requestedActions = [],
    candidateIdentities = [],
    batchSize = 0,
    now = new Date(),
    expectedBaseId = CANONICAL_BASE_ID,
  } = opts;

  if (!authorization || typeof authorization !== "object") {
    return fail("AUTHORIZATION_OBJECT_REQUIRED");
  }
  if (!manifestPath || !fs.existsSync(manifestPath)) {
    return fail("MANIFEST_FILE_MISSING", { manifestPath });
  }

  const actualHash = sha256File(manifestPath).toLowerCase();
  const expectedHash = String(authorization.manifestSha256 || "").toLowerCase();
  if (!expectedHash || actualHash !== expectedHash) {
    return fail("MANIFEST_HASH_MISMATCH", { expectedHash, actualHash });
  }

  const envHash = String(process.env.HIF_FOUNDER_MANIFEST_SHA256 || "").trim().toLowerCase();
  if (!envHash || envHash !== expectedHash) {
    return fail("ENV_MANIFEST_HASH_MISMATCH", { envHash, expectedHash });
  }

  if (String(process.env.HIF_APPLY_PROMOTION || "") !== "1") {
    return fail("HIF_APPLY_PROMOTION_ENV_REQUIRED");
  }

  if (String(authorization.canonicalBaseId || "") !== String(expectedBaseId)) {
    return fail("CANONICAL_BASE_MISMATCH", {
      authorized: authorization.canonicalBaseId,
      expected: expectedBaseId,
    });
  }

  if (!authorization.runId || String(authorization.runId) !== String(runId)) {
    return fail("RUN_ID_MISMATCH", {
      authorizedRunId: authorization.runId,
      actualRunId: runId,
    });
  }

  const expiresAt = Date.parse(authorization.expiresAt || "");
  if (!Number.isFinite(expiresAt)) {
    return fail("AUTHORIZATION_EXPIRY_INVALID");
  }
  if (now.getTime() > expiresAt) {
    return fail("AUTHORIZATION_EXPIRED", { expiresAt: authorization.expiresAt });
  }

  const maxBatch = Number(authorization.maxBatchSize);
  if (!Number.isFinite(maxBatch) || maxBatch < 1) {
    return fail("MAX_BATCH_SIZE_INVALID");
  }
  if (Number(batchSize) > maxBatch) {
    return fail("BATCH_SIZE_EXCEEDS_AUTHORIZATION", { batchSize, maxBatch });
  }

  const allowed = new Set(authorization.allowedActions || []);
  for (const a of allowed) {
    if (!ALLOWED_PROMOTION_ACTIONS.includes(a)) {
      return fail("UNKNOWN_ALLOWED_ACTION", { action: a });
    }
  }
  for (const a of requestedActions) {
    if (!allowed.has(a)) {
      return fail("ACTION_NOT_AUTHORIZED", { action: a, allowed: [...allowed] });
    }
  }

  const authorizedIds = new Set((authorization.recordIdentities || []).map(String));
  if (authorizedIds.size < 1) {
    return fail("RECORD_IDENTITIES_EMPTY");
  }
  for (const id of candidateIdentities) {
    if (!authorizedIds.has(String(id))) {
      return fail("RECORD_IDENTITY_NOT_AUTHORIZED", { id });
    }
  }

  // Per-row create still requires founderApproval in manifest content when action CREATE
  if (requestedActions.includes("CREATE")) {
    try {
      const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
      const rows = manifest.candidates || manifest.rows || [];
      for (const row of rows) {
        if (row.disposition === "CREATE" || row.founderApproval?.approvedForCreate) {
          if (row.founderApproval?.approvedForCreate !== true) {
            return fail("CREATE_ROW_MISSING_FOUNDER_APPROVAL", {
              candidateId: row.candidateId || row.discoveryPropertyId,
            });
          }
        }
      }
    } catch (err) {
      return fail("MANIFEST_PARSE_FAILED", { message: String(err.message || err) });
    }
  }

  return {
    ok: true,
    manifestSha256: actualHash,
    runId,
    maxBatchSize: maxBatch,
    expiresAt: authorization.expiresAt,
  };
}

/**
 * Load authorization JSON from durable auth dir or explicit path.
 */
export function loadPromotionAuthorization(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    return null;
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export default {
  buildPromotionAuthorization,
  assertPromotionAuthorization,
  loadPromotionAuthorization,
  sha256File,
  CANONICAL_BASE_ID,
  ALLOWED_PROMOTION_ACTIONS,
};
