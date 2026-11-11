/**
 * PUBLICATION_HISTORY overlay recovery authorization.
 *
 * Scope: reconcile Airtable "AI Demand Positioning – Published Reports" Live overlay
 * to already-published filesystem PRIMARY_SOT for production-share hotels.
 *
 * Does NOT authorize: new customer publication, external-share issuance, deploy,
 * paid research, or staging→Live promotion of unpublished hotels.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ADP_GOVERNED_BASE_ID } from "./resolve-adp-history-destination-v1.js";
import { PERSISTENCE_AUTH_SCOPE } from "./adp-airtable-ecosystem-contract-v1.js";
import { resolveAuthenticatedFounderIdentity } from "../../hotel-census/hpc-authorized-create-reuse-v1.js";
import { ADP_PUBLISHED_REPORTS_TABLE } from "../airtable-field-map.js";

export const ADP_PUBLICATION_HISTORY_OVERLAY_RECOVERY_AUTHORIZATION_V1 =
  "ADP_PUBLICATION_HISTORY_OVERLAY_RECOVERY_AUTHORIZATION_V1";

export const PUBLICATION_HISTORY_AUTH_SCOPE = PERSISTENCE_AUTH_SCOPE.PUBLICATION_HISTORY;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LEDGER_DIR = path.join(
  ROOT,
  "data/ai-demand-positioning/publication-history-authorization"
);
const AUTH_LEDGER = path.join(LEDGER_DIR, "overlay-recovery-authorization-ledger.json");

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

export function loadPublicationHistoryOverlayRecoveryAuthorization({
  authorizationId = null,
  manifestSha256 = null,
  requireActive = true,
} = {}) {
  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  return (
    (ledger.records || []).find((r) => {
      if (requireActive && r.status !== "ACTIVE") return false;
      if (r.scope !== PUBLICATION_HISTORY_AUTH_SCOPE) return false;
      if (authorizationId && r.authorizationId !== authorizationId) return false;
      if (manifestSha256 && r.manifestSha256 !== manifestSha256) return false;
      if (authorizationId || manifestSha256) return true;
      return false;
    }) || null
  );
}

export function assessPublicationHistoryOverlayRecoveryAuthorization({
  manifestSha256 = null,
  authorizationId = null,
} = {}) {
  const missing = [];
  const auth = loadPublicationHistoryOverlayRecoveryAuthorization({
    authorizationId,
    manifestSha256,
  });
  if (!auth) {
    missing.push({
      code: "MISSING_DURABLE_PUBLICATION_OVERLAY_AUTHORIZATION",
      detail: `No ACTIVE record in ${AUTH_LEDGER}`,
    });
  } else if (manifestSha256 && auth.manifestSha256 !== manifestSha256) {
    missing.push({
      code: "MANIFEST_HASH_MISMATCH",
      detail: `ledger ${auth.manifestSha256} !== current ${manifestSha256}`,
    });
  }
  const apply = process.env.ADP_AIRTABLE_PUBLISH_APPLY;
  if (apply !== "true" && apply !== "1") {
    missing.push({
      code: "ADP_AIRTABLE_PUBLISH_APPLY_UNSET",
      detail: "Set ADP_AIRTABLE_PUBLISH_APPLY=true for overlay recovery process only",
    });
  }
  return {
    ok: missing.length === 0,
    contract: ADP_PUBLICATION_HISTORY_OVERLAY_RECOVERY_AUTHORIZATION_V1,
    scope: PUBLICATION_HISTORY_AUTH_SCOPE,
    baseId: ADP_GOVERNED_BASE_ID,
    table: ADP_PUBLISHED_REPORTS_TABLE,
    authorization: auth
      ? {
          authorizationId: auth.authorizationId,
          status: auth.status,
          manifestSha256: auth.manifestSha256,
          approverIdentity: auth.approverIdentity,
          authorizedAt: auth.authorizedAt,
          insertCountAuthorized: auth.insertCountAuthorized,
          updateCountAuthorized: auth.updateCountAuthorized,
        }
      : null,
    missing,
    blocked: missing.length > 0,
  };
}

export function recordPublicationHistoryOverlayRecoveryAuthorization({
  manifestSha256,
  insertCountAuthorized,
  updateCountAuthorized,
  founderDirectiveRef,
  gitName = "",
  gitEmail = "",
  envValue = "",
  manifestPackage =
    "reports/dealality-adp-complete-airtable-all-scopes-reconciliation-v1",
  manifestFile = "PUBLISHED_REPORTS_BACKFILL_MANIFEST.csv",
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
  const inserts = Number(insertCountAuthorized);
  const updates = Number(updateCountAuthorized);
  if (!Number.isFinite(inserts) || inserts < 0 || !Number.isFinite(updates) || updates < 0) {
    return { ok: false, error: "INVALID_COUNTS" };
  }

  const authorizationId = `auth_adp_publication_overlay_${new Date()
    .toISOString()
    .slice(0, 10)
    .replace(/-/g, "")}_${manifestSha256.slice(0, 8)}`;
  const authorizedAt = new Date().toISOString();
  const record = {
    authorizationId,
    status: "ACTIVE",
    scope: PUBLICATION_HISTORY_AUTH_SCOPE,
    recoveryKind: "EXISTING_PUBLISHED_OVERLAY_RECONCILIATION",
    baseId: ADP_GOVERNED_BASE_ID,
    tablesAllowlist: [ADP_PUBLISHED_REPORTS_TABLE],
    tableId: "tblGSsAV8ctCuS5A0",
    manifestPackage,
    manifestFile,
    manifestSha256,
    insertCountAuthorized: inserts,
    updateCountAuthorized: updates,
    excludeNewCustomerPublication: true,
    excludeExternalShareIssuance: true,
    excludeUMusicAndIntentionalUnpublished: true,
    excludePaidResearch: true,
    excludeDeploy: true,
    excludeMeasurementHistory: true,
    approverIdentity: identity.approverIdentity,
    authorizedAt,
    founderDirectiveRef: directive.slice(0, 500),
    authenticationMechanism: identity.authenticationMechanism,
    trustedAuthorizationId: identity.trustedAuthorizationId,
    contract: ADP_PUBLICATION_HISTORY_OVERLAY_RECOVERY_AUTHORIZATION_V1,
  };

  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  const records = Array.isArray(ledger.records) ? [...ledger.records] : [];
  const existingIdx = records.findIndex(
    (r) =>
      r.authorizationId === authorizationId ||
      (r.status === "ACTIVE" &&
        r.scope === PUBLICATION_HISTORY_AUTH_SCOPE &&
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
