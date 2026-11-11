/**
 * ADP_MEASUREMENT_HISTORY_WRITE_AUTHORIZATION_V1
 *
 * Durable founder authorization for governed Airtable MEASUREMENT_HISTORY writes.
 * Mirrors HPC create authorization pattern — chat text alone does not unlock writes.
 *
 * Required for live apply:
 *   1. ACTIVE ledger record with matching manifestSha256 + scope
 *   2. ADP_HISTORY_WRITES_ENABLED=true|1
 *   3. ADP_HISTORY_AIRTABLE_WRITE_APPLY=true|1
 *   4. allowGovernedProductionHistoryBase / GOVERNED_PRODUCTION destination
 *
 * Does NOT authorize: paid research, publication, deploy, external share, attributes.
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { isAdpHistoryWritesEnabled } from "./airtable-history-schema-final-v1.js";
import { HISTORY_ENV } from "./airtable-history-schema-final-v1.js";
import { writesAllowed } from "./persist-adp-historical-period-v1.js";
import {
  ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1,
  listFullE2eMeasurementHistoryTables,
  resolveLiveTableName,
  PERSISTENCE_AUTH_SCOPE,
} from "./adp-airtable-ecosystem-contract-v1.js";
import { ADP_GOVERNED_BASE_ID } from "./resolve-adp-history-destination-v1.js";
import { resolveAuthenticatedFounderIdentity } from "../../hotel-census/hpc-authorized-create-reuse-v1.js";

export const ADP_MEASUREMENT_HISTORY_WRITE_AUTHORIZATION_V1 =
  "ADP_MEASUREMENT_HISTORY_WRITE_AUTHORIZATION_V1";

export const MEASUREMENT_HISTORY_AUTH_SCOPE =
  PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LEDGER_DIR = path.join(
  ROOT,
  "data/ai-demand-positioning/measurement-history-authorization"
);
const AUTH_LEDGER = path.join(LEDGER_DIR, "write-authorization-ledger.json");
const LOCK_DIR = path.join(LEDGER_DIR, "locks");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function readJson(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJson(p, body) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(body, null, 2) + "\n");
}

export function sha256File(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

export function listAuthorizedMeasurementHistoryTableNames() {
  return listFullE2eMeasurementHistoryTables().map((t) => resolveLiveTableName(t));
}

/**
 * Load ACTIVE authorization matching manifest hash / authorizationId.
 */
export function loadMeasurementHistoryWriteAuthorization({
  authorizationId = null,
  manifestSha256 = null,
  requireActive = true,
} = {}) {
  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  const records = ledger.records || [];
  return (
    records.find((r) => {
      if (requireActive && r.status !== "ACTIVE") return false;
      if (r.scope !== MEASUREMENT_HISTORY_AUTH_SCOPE) return false;
      if (authorizationId && r.authorizationId !== authorizationId) return false;
      if (manifestSha256 && r.manifestSha256 !== manifestSha256) return false;
      if (authorizationId || manifestSha256) return true;
      return false;
    }) || null
  );
}

/**
 * Evaluate whether live MEASUREMENT_HISTORY writes may proceed.
 * Never manufactures an authorization record.
 */
export function assessMeasurementHistoryWriteAuthorization({
  manifestSha256 = null,
  authorizationId = null,
  allowGovernedProductionHistoryBase = true,
} = {}) {
  const missing = [];
  const tables = listAuthorizedMeasurementHistoryTableNames();
  const auth = loadMeasurementHistoryWriteAuthorization({
    authorizationId,
    manifestSha256,
  });

  if (!auth) {
    missing.push({
      code: "MISSING_DURABLE_AUTHORIZATION_LEDGER_RECORD",
      detail:
        "No ACTIVE record in data/ai-demand-positioning/measurement-history-authorization/write-authorization-ledger.json matching manifestSha256/authorizationId",
      howToCreate:
        "Founder must record an ACTIVE authorization with scope=MEASUREMENT_HISTORY, baseId=appa2cE7FTRmIbB32, manifestSha256=<dry-run hash>, tablesAllowlist=nine S9B tables, approverIdentity=<founder>, authorizationId=<unique>",
    });
  } else if (manifestSha256 && auth.manifestSha256 !== manifestSha256) {
    missing.push({
      code: "MANIFEST_HASH_MISMATCH",
      detail: `ledger ${auth.manifestSha256} !== current ${manifestSha256}`,
    });
  }

  if (!isAdpHistoryWritesEnabled()) {
    missing.push({
      code: "ADP_HISTORY_WRITES_ENABLED_UNSET",
      detail: "Set ADP_HISTORY_WRITES_ENABLED=true for this process only",
    });
  }
  const apply = process.env[HISTORY_ENV.ADP_HISTORY_AIRTABLE_WRITE_APPLY];
  if (apply !== "true" && apply !== "1") {
    missing.push({
      code: "ADP_HISTORY_AIRTABLE_WRITE_APPLY_UNSET",
      detail: "Set ADP_HISTORY_AIRTABLE_WRITE_APPLY=true for this process only",
    });
  }

  const gate = writesAllowed({
    allowProductionWrites: true,
    useMock: false,
    allowGovernedProductionHistoryBase,
  });
  if (!gate.allowed) {
    missing.push({
      code: "WRITES_ALLOWED_REFUSED",
      detail: gate.reason,
      mode: gate.mode,
    });
  }

  if (auth && auth.baseId && auth.baseId !== ADP_GOVERNED_BASE_ID) {
    missing.push({
      code: "WRONG_BASE_IN_AUTHORIZATION",
      detail: auth.baseId,
    });
  }

  return {
    ok: missing.length === 0,
    contract: ADP_MEASUREMENT_HISTORY_WRITE_AUTHORIZATION_V1,
    ecosystemContract: ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1,
    scope: MEASUREMENT_HISTORY_AUTH_SCOPE,
    baseId: ADP_GOVERNED_BASE_ID,
    tablesAllowlist: tables,
    authorization: auth
      ? {
          authorizationId: auth.authorizationId,
          status: auth.status,
          manifestSha256: auth.manifestSha256,
          approverIdentity: auth.approverIdentity,
          authorizedAt: auth.authorizedAt,
          insertCountAuthorized: auth.insertCountAuthorized,
        }
      : null,
    writesGate: gate,
    missing,
    blocked: missing.length > 0,
    note: "Chat directives do not create ledger records. Do not manufacture approval.",
  };
}

export function acquireMeasurementHistoryWriteLock(lockKey, runId) {
  ensureDir(LOCK_DIR);
  const safe = String(lockKey).replace(/[^a-zA-Z0-9_-]/g, "_");
  const lp = path.join(LOCK_DIR, `${safe}.lock`);
  if (fs.existsSync(lp)) {
    const existing = readJson(lp, {});
    if (existing.runId && existing.runId !== runId) {
      const age = Date.now() - Date.parse(existing.acquiredAt || 0);
      if (age < 6 * 60 * 60 * 1000) {
        return { ok: false, error: "MEASUREMENT_HISTORY_WRITE_LOCK_HELD", existing };
      }
    }
  }
  const body = {
    runId,
    lockKey,
    acquiredAt: new Date().toISOString(),
    pid: process.pid,
  };
  writeJson(lp, body);
  return { ok: true, lock: body, path: lp };
}

export function releaseMeasurementHistoryWriteLock(lockKey, runId) {
  const safe = String(lockKey).replace(/[^a-zA-Z0-9_-]/g, "_");
  const lp = path.join(LOCK_DIR, `${safe}.lock`);
  if (!fs.existsSync(lp)) return { ok: true };
  const existing = readJson(lp, {});
  if (existing.runId && existing.runId !== runId) {
    return { ok: false, error: "LOCK_OWNED_BY_OTHER", existing };
  }
  fs.unlinkSync(lp);
  return { ok: true };
}

/** Template for founder to populate — never auto-written as ACTIVE by agents. */
export function buildAuthorizationRecordTemplate({
  manifestSha256,
  insertCountAuthorized,
  approverIdentity = "FOUNDER_REQUIRED",
} = {}) {
  return {
    authorizationId: `auth_adp_measurement_history_${new Date()
      .toISOString()
      .slice(0, 10)
      .replace(/-/g, "")}_v1`,
    status: "DRAFT_NOT_ACTIVE",
    scope: MEASUREMENT_HISTORY_AUTH_SCOPE,
    baseId: ADP_GOVERNED_BASE_ID,
    tablesAllowlist: listAuthorizedMeasurementHistoryTableNames(),
    manifestPackage:
      "reports/dealality-adp-complete-airtable-ecosystem-and-persistence-v1",
    manifestFile: "COMPLETE_BACKFILL_DRY_RUN.csv",
    approvalFile: "SUPERSEDING_FOUNDER_WRITE_APPROVAL_MANIFEST.csv",
    manifestSha256,
    insertCountAuthorized,
    excludePublication: true,
    excludePaidResearch: true,
    excludeAttributes: true,
    excludeDeploy: true,
    excludeExternalShare: true,
    approverIdentity,
    authorizedAt: null,
    note: "Set status=ACTIVE and authorizedAt=ISO only after founder signs; place in write-authorization-ledger.json records[]",
  };
}

/**
 * Record ACTIVE MEASUREMENT_HISTORY authorization only when:
 * - local git identity matches the trusted consumed HPC founder identity
 * - founderDirectiveRef is an explicit durable directive string from this session
 * - manifestSha256 and insertCountAuthorized are provided
 *
 * Does not invent identity. authorizedAt is the wall-clock moment of this recording.
 */
export function recordMeasurementHistoryWriteAuthorization({
  manifestSha256,
  insertCountAuthorized,
  founderDirectiveRef,
  gitName = "",
  gitEmail = "",
  envValue = "",
  manifestPackage =
    "reports/dealality-adp-complete-airtable-ecosystem-and-persistence-v1",
  manifestFile = "COMPLETE_BACKFILL_DRY_RUN.csv",
  approvalFile = "SUPERSEDING_FOUNDER_WRITE_APPROVAL_MANIFEST.csv",
  allScopesManifestSha256 = null,
} = {}) {
  const identity = resolveAuthenticatedFounderIdentity({
    gitName,
    gitEmail,
    envValue,
  });
  if (!identity.ok || !identity.authenticated) {
    return {
      ok: false,
      error: "FOUNDER_IDENTITY_NOT_AUTHENTICATED",
      detail: identity.reason,
    };
  }
  const directive = String(founderDirectiveRef || "").trim();
  if (!directive || directive.length < 24) {
    return {
      ok: false,
      error: "MISSING_FOUNDER_DIRECTIVE_REF",
      detail: "Explicit founder PROCEED directive required",
    };
  }
  if (!manifestSha256 || !/^[a-f0-9]{64}$/i.test(manifestSha256)) {
    return { ok: false, error: "INVALID_MANIFEST_SHA256" };
  }
  const inserts = Number(insertCountAuthorized);
  if (!Number.isFinite(inserts) || inserts < 0) {
    return { ok: false, error: "INVALID_INSERT_COUNT" };
  }

  const authorizationId = `auth_adp_measurement_history_${new Date()
    .toISOString()
    .slice(0, 10)
    .replace(/-/g, "")}_${manifestSha256.slice(0, 8)}`;
  const authorizedAt = new Date().toISOString();
  const record = {
    authorizationId,
    status: "ACTIVE",
    scope: MEASUREMENT_HISTORY_AUTH_SCOPE,
    baseId: ADP_GOVERNED_BASE_ID,
    tablesAllowlist: listAuthorizedMeasurementHistoryTableNames(),
    manifestPackage,
    manifestFile,
    approvalFile,
    manifestSha256,
    allScopesManifestSha256: allScopesManifestSha256 || null,
    insertCountAuthorized: inserts,
    excludePublication: true,
    excludePaidResearch: true,
    excludeAttributes: true,
    excludeDeploy: true,
    excludeExternalShare: true,
    approverIdentity: identity.approverIdentity,
    authorizedAt,
    founderDirectiveRef: directive.slice(0, 500),
    authenticationMechanism: identity.authenticationMechanism,
    trustedAuthorizationId: identity.trustedAuthorizationId,
    contract: ADP_MEASUREMENT_HISTORY_WRITE_AUTHORIZATION_V1,
  };

  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  const records = Array.isArray(ledger.records) ? [...ledger.records] : [];
  const existingIdx = records.findIndex(
    (r) =>
      r.authorizationId === authorizationId ||
      (r.status === "ACTIVE" &&
        r.scope === MEASUREMENT_HISTORY_AUTH_SCOPE &&
        r.manifestSha256 === manifestSha256)
  );
  if (existingIdx >= 0) {
    records[existingIdx] = {
      ...records[existingIdx],
      ...record,
      supersededPrior: records[existingIdx].authorizationId,
    };
  } else {
    records.push(record);
  }
  writeJson(AUTH_LEDGER, { version: 1, records, updatedAt: authorizedAt });
  return { ok: true, record, ledgerPath: AUTH_LEDGER };
}

/**
 * Record ACTIVE authorization for verified field-level UPDATEs (not inserts).
 * Same founder authentication requirements as insert authorization.
 */
export function recordMeasurementHistoryFieldPatchAuthorization({
  manifestSha256,
  updateCountAuthorized,
  founderDirectiveRef,
  gitName = "",
  gitEmail = "",
  envValue = "",
  manifestPackage =
    "reports/dealality-adp-airtable-field-completeness-recovery-v1/final-validated-updates-v1",
  manifestFile = "VERIFIED_FIELD_UPDATE_MANIFEST.csv",
  approvalScopeCode = "MEASUREMENT_HISTORY_VERIFIED_FIELD_PATCH",
  excludeHashBackfill = true,
} = {}) {
  const identity = resolveAuthenticatedFounderIdentity({
    gitName,
    gitEmail,
    envValue,
  });
  if (!identity.ok || !identity.authenticated) {
    return {
      ok: false,
      error: "FOUNDER_IDENTITY_NOT_AUTHENTICATED",
      detail: identity.reason,
    };
  }
  const directive = String(founderDirectiveRef || "").trim();
  if (!directive || directive.length < 24) {
    return { ok: false, error: "MISSING_FOUNDER_DIRECTIVE_REF" };
  }
  if (!manifestSha256 || !/^[a-f0-9]{64}$/i.test(manifestSha256)) {
    return { ok: false, error: "INVALID_MANIFEST_SHA256" };
  }
  const updates = Number(updateCountAuthorized);
  if (!Number.isFinite(updates) || updates <= 0) {
    return { ok: false, error: "INVALID_UPDATE_COUNT" };
  }

  const authorizationId = `auth_adp_mh_field_patch_${new Date()
    .toISOString()
    .slice(0, 10)
    .replace(/-/g, "")}_${manifestSha256.slice(0, 8)}`;
  const authorizedAt = new Date().toISOString();
  const record = {
    authorizationId,
    status: "ACTIVE",
    scope: MEASUREMENT_HISTORY_AUTH_SCOPE,
    operationKind: "FIELD_PATCH_UPDATE",
    approvalScopeCode,
    baseId: ADP_GOVERNED_BASE_ID,
    tablesAllowlist: listAuthorizedMeasurementHistoryTableNames(),
    manifestPackage,
    manifestFile,
    manifestSha256,
    updateCountAuthorized: updates,
    insertCountAuthorized: 0,
    excludePublication: true,
    excludePaidResearch: true,
    excludeAttributes: true,
    excludeDeploy: true,
    excludeExternalShare: true,
    excludeHashBackfill: Boolean(excludeHashBackfill),
    excludeSyntheticCheckboxNoop: true,
    excludeInvalidPublicationCommit: true,
    approverIdentity: identity.approverIdentity,
    authorizedAt,
    founderDirectiveRef: directive.slice(0, 800),
    authenticationMechanism: identity.authenticationMechanism,
    trustedAuthorizationId: identity.trustedAuthorizationId,
    contract: ADP_MEASUREMENT_HISTORY_WRITE_AUTHORIZATION_V1,
  };

  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  const records = Array.isArray(ledger.records) ? [...ledger.records] : [];
  const existingIdx = records.findIndex(
    (r) =>
      r.authorizationId === authorizationId ||
      (r.status === "ACTIVE" &&
        r.operationKind === "FIELD_PATCH_UPDATE" &&
        r.manifestSha256 === manifestSha256)
  );
  if (existingIdx >= 0) {
    records[existingIdx] = { ...records[existingIdx], ...record };
  } else {
    records.push(record);
  }
  writeJson(AUTH_LEDGER, { version: 1, records, updatedAt: authorizedAt });
  return { ok: true, record, ledgerPath: AUTH_LEDGER };
}

export { AUTH_LEDGER, LEDGER_DIR };
