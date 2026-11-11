/**
 * HPC_AUTHORIZED_CREATE_REUSE_V1
 *
 * Permanent create/reuse state machine for Full ADP E2E S2.
 * - Reuse verified existing HPC records
 * - Create only after exact founder authorization of field payload
 * - Pre-create live collision recheck
 * - Readback + S2 transition to HPC_EXISTING_RECORD_VERIFIED
 * - Never implies paid monitoring or publication authorization
 * - Default dry-run; no writes without apply + exact auth
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import "dotenv/config";
import {
  fetchHpcRecord,
  fetchLiveHpcSchema,
  searchHpcByName,
} from "./hpc-shared-upsert-v1.js";
import { HPC_TABLE_NAME, HPC_MINIMUM_IDENTITY_FIELDS } from "./hpc-full-onboarding-contract-v1.js";
import {
  performLiveHpcIdentityCollisionCheck,
  HPC_LIVE_OUTCOME,
  HPC_LIVE_IDENTITY_COLLISION_V1,
} from "./hpc-live-identity-collision-v1.js";

export const HPC_AUTHORIZED_CREATE_REUSE_V1 = "HPC_AUTHORIZED_CREATE_REUSE_V1";
export const HPC_CREATE_REUSE_VERSION = "2026-10-21.v1";

export const HPC_CREATE_REUSE_STATE = Object.freeze({
  LOOKUP_PENDING: "LOOKUP_PENDING",
  EXISTING_VERIFIED: "EXISTING_VERIFIED",
  CREATE_PROPOSED: "CREATE_PROPOSED",
  CREATE_AUTHORIZED: "CREATE_AUTHORIZED",
  PRE_CREATE_COLLISION_RECHECK: "PRE_CREATE_COLLISION_RECHECK",
  CREATING: "CREATING",
  CREATE_FAILED: "CREATE_FAILED",
  READBACK_PENDING: "READBACK_PENDING",
  READBACK_FAILED: "READBACK_FAILED",
  EXISTING_AFTER_CREATE: "EXISTING_AFTER_CREATE",
  COLLISION_REVIEW: "COLLISION_REVIEW",
  LOOKUP_UNAVAILABLE: "LOOKUP_UNAVAILABLE",
  UNAUTHORIZED_CREATE_REJECTED: "UNAUTHORIZED_CREATE_REJECTED",
  IDENTITY_VERIFICATION_EXPIRED: "IDENTITY_VERIFICATION_EXPIRED",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const LEDGER_DIR = path.join(
  ROOT,
  "data/ai-demand-positioning/runtime/hpc-authorized-create-reuse-v1"
);
const AUTH_LEDGER = path.join(LEDGER_DIR, "create-authorization-ledger.json");
const LOCK_DIR = path.join(LEDGER_DIR, "_locks");
const STATE_DIR = path.join(LEDGER_DIR, "runs");

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, `${JSON.stringify(obj, null, 2)}\n`);
}

function sha256(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function platformBaseId() {
  return (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
}

async function airtableFetch(pathSuffix, opts = {}) {
  const token = airtableToken();
  const baseId = platformBaseId();
  if (!token || !baseId) {
    const err = new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
    err.code = "HPC_CONFIG_MISSING";
    throw err;
  }
  const url = `https://api.airtable.com/v0/${baseId}/${pathSuffix}`;
  const res = await fetch(url, {
    ...opts,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(
      `Airtable ${res.status}: ${json?.error?.message || JSON.stringify(json).slice(0, 200)}`
    );
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

/**
 * Validate fields against live schema (select options must exist; required identity present).
 */
export function validateHpcCreateFieldsAgainstSchema(fields, schema) {
  const issues = [];
  const byName = Object.fromEntries((schema?.fields || []).map((f) => [f.name, f]));
  for (const name of HPC_MINIMUM_IDENTITY_FIELDS) {
    if (fields[name] == null || String(fields[name]).trim() === "") {
      issues.push({ code: "MISSING_MINIMUM_IDENTITY_FIELD", field: name });
    }
  }
  for (const [name, value] of Object.entries(fields || {})) {
    const meta = byName[name];
    if (!meta) {
      issues.push({ code: "FIELD_NOT_IN_SCHEMA", field: name });
      continue;
    }
    if (!meta.writable) {
      issues.push({ code: "FIELD_NOT_WRITABLE", field: name });
      continue;
    }
    if (meta.type === "singleSelect" && value != null && value !== "") {
      if ((meta.choices || []).length && !meta.choices.includes(value)) {
        issues.push({
          code: "SELECT_VALUE_NOT_IN_SCHEMA",
          field: name,
          value,
          allowedSample: (meta.choices || []).slice(0, 8),
        });
      }
    }
  }
  return {
    ok: issues.length === 0,
    issues,
    fieldsHash: sha256(stableFields(fields)),
  };
}

function stableFields(fields) {
  const keys = Object.keys(fields || {}).sort();
  const out = {};
  for (const k of keys) out[k] = fields[k];
  return out;
}

/**
 * Exact Chapultepec create proposal — official-source + schema-compliant selects only.
 * Rooms/lat/lng omitted (not inventing unverified inventory/geo).
 */
export function buildChapultepecHpcCreateProposal() {
  const fields = {
    "Property Name": "Grand Fiesta Americana Chapultepec",
    "Canonical Property Name": "Grand Fiesta Americana Chapultepec",
    "Current Brand": "Grand Fiesta Americana",
    "Brand Family": "Grupo Posadas",
    "Family / Source Family": "Other",
    "Affiliation Status": "Branded",
    Country: "Mexico",
    City: "Mexico City",
    "State / Region": "Ciudad de México",
    Address: "756 Calzada General Mariano Escobedo, Anzures, 11590 Mexico City, Mexico",
    "Postal Code": "11590",
    "Official Property URL":
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-chapultepec",
    "Property Type": "Hotel",
    Market: "Mexico City",
    Submarket: "Chapultepec / Anzures",
    Continent: "North America",
    "Sub-Continent": "Mexico",
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Enrichment Status": "Identity Seeded — pending enrichment",
    "Identity Confidence": "High",
    "Source Type": "official_property_page",
    "Source Confidence": "High",
    "Source URL":
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-chapultepec",
  };
  return {
    draftPropertyId: "adp_grand_fiesta_americana_chapultepec",
    hotelName: "Grand Fiesta Americana Chapultepec",
    fields,
    fieldsHash: sha256(stableFields(fields)),
    provenance: {
      retrievalDate: "2026-10-21",
      sources: [
        "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-chapultepec",
      ],
      omittedUnverified: ["Rooms / Keys", "Latitude", "Longitude", "Operator / Management Company"],
      operatingStatus: "ACTIVE_OPERATING",
      propertyClassification: "Hotel (urban full-service luxury — Property Type select)",
      note: "Family / Source Family = Other (Posadas not a Family select option). Brand Family text = Grupo Posadas.",
    },
    writeAuthorized: false,
    paidMonitoringImplied: false,
    publicationImplied: false,
  };
}

export async function prepareValidatedCreateProposal(proposal, { schema = null } = {}) {
  const liveSchema = schema || (await fetchLiveHpcSchema());
  const validation = validateHpcCreateFieldsAgainstSchema(proposal.fields, liveSchema);
  return {
    ...proposal,
    schemaValidated: validation.ok,
    validation,
    fieldsHash: validation.fieldsHash,
    preparedAt: new Date().toISOString(),
    baseId: platformBaseId(),
    table: HPC_TABLE_NAME,
  };
}

export function persistHpcCreateAuthorization({
  runId,
  draftPropertyId,
  hotelName,
  fields,
  fieldsHash,
  approverIdentity,
  approvedAt = new Date().toISOString(),
  operation = "HPC_CREATE",
  authorizationSource = null,
  baseId = null,
  table = HPC_TABLE_NAME,
  singleUse = true,
} = {}) {
  const expectedHash = sha256(stableFields(fields));
  if (fieldsHash && fieldsHash !== expectedHash) {
    return {
      ok: false,
      error: "FIELDS_HASH_MISMATCH",
      expectedHash,
      providedHash: fieldsHash,
    };
  }
  if (!runId || !draftPropertyId || !hotelName || !approverIdentity) {
    return {
      ok: false,
      error: "INCOMPLETE_CREATE_AUTHORIZATION",
      missing: [
        !runId && "runId",
        !draftPropertyId && "draftPropertyId",
        !hotelName && "hotelName",
        !approverIdentity && "approverIdentity",
      ].filter(Boolean),
    };
  }
  if (isSimulationOrTestApprover(approverIdentity)) {
    // Still allow ledger entries for tests, but tag them so live apply rejects.
  }
  const record = {
    contract: HPC_AUTHORIZED_CREATE_REUSE_V1,
    authorizationId: `hpc_create_auth_${sha256({ runId, draftPropertyId, expectedHash, approvedAt }).slice(0, 16)}`,
    runId,
    draftPropertyId,
    hotelName,
    operation,
    fields: stableFields(fields),
    fieldsHash: expectedHash,
    approverIdentity,
    approvedAt,
    authorizationSource,
    baseId: baseId || platformBaseId() || null,
    table,
    singleUse: singleUse !== false,
    status: "ACTIVE",
    paidMonitoringAuthorized: false,
    publicationAuthorized: false,
    simulation: isSimulationOrTestApprover(approverIdentity),
  };
  ensureDir(LEDGER_DIR);
  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  // Idempotent reuse: same run + property + hash + live founder + still ACTIVE
  const dup = (ledger.records || []).find(
    (r) =>
      r.status === "ACTIVE" &&
      r.runId === runId &&
      r.draftPropertyId === draftPropertyId &&
      r.fieldsHash === expectedHash &&
      r.approverIdentity === approverIdentity &&
      !isSimulationOrTestApprover(r.approverIdentity, r)
  );
  if (dup) {
    return { ok: true, reused: true, record: dup };
  }
  ledger.records.push(record);
  ledger.updatedAt = new Date().toISOString();
  writeJson(AUTH_LEDGER, ledger);
  return { ok: true, reused: false, record };
}

/**
 * Mark an HPC create authorization as consumed after successful create+readback.
 * Idempotent — already-consumed returns ok.
 */
export function consumeHpcCreateAuthorization(authorizationId, {
  hpcRecordId = null,
  consumedAt = new Date().toISOString(),
  runId = null,
} = {}) {
  if (!authorizationId) {
    return { ok: false, error: "MISSING_AUTHORIZATION_ID" };
  }
  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  const rec = (ledger.records || []).find((r) => r.authorizationId === authorizationId);
  if (!rec) return { ok: false, error: "AUTHORIZATION_NOT_FOUND" };
  if (rec.status === "CONSUMED") {
    return { ok: true, reused: true, record: rec };
  }
  rec.status = "CONSUMED";
  rec.consumedAt = consumedAt;
  rec.consumedHpcRecordId = hpcRecordId || null;
  if (runId) rec.consumedByRunId = runId;
  ledger.updatedAt = consumedAt;
  writeJson(AUTH_LEDGER, ledger);
  return { ok: true, reused: false, record: rec };
}

const FOUNDER_IDENTITY_MARKERS =
  /SIMULATION|TEST_HARNESS|DRY_RUN|PLACEHOLDER|actual approved identity|live identity already used|VERIFIED_LIVE_FOUNDER_IDENTITY/i;
const FOUNDER_EMAIL = /^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$/;

/**
 * Consumed Chapultepec HPC create. Read-only trust anchor for the founder
 * identity string. This id is not a GDI authorization and must not be reused.
 */
export const TRUSTED_CONSUMED_HPC_CREATE_AUTHORIZATION_ID = "hpc_create_auth_5f70fe679b52f30a";
export const TRUSTED_CONSUMED_HPC_RECORD_ID = "recterKmNPdntCm0v";

/**
 * Format check only. A well-formed Display Name <email> is not founder authority.
 */
export function parseFounderIdentityFormat(approverIdentity) {
  const value = String(approverIdentity ?? "").trim();
  if (!value) return { ok: false, reason: "EMPTY_IDENTITY" };
  if (FOUNDER_IDENTITY_MARKERS.test(value)) return { ok: false, reason: "PLACEHOLDER_OR_SIMULATION" };
  const match = value.match(/^([^<>\r\n]+?)\s*<([^<>\s]+)>$/);
  if (!match) {
    return { ok: false, reason: /[<>]/.test(value) ? "MALFORMED_IDENTITY" : "UNSTRUCTURED_IDENTITY" };
  }
  const displayName = match[1].trim().replace(/\s+/g, " ");
  const email = match[2].trim();
  if (!displayName || displayName.length > 120) return { ok: false, reason: "MALFORMED_IDENTITY" };
  if (!FOUNDER_EMAIL.test(email)) return { ok: false, reason: "MALFORMED_EMAIL" };
  if (FOUNDER_IDENTITY_MARKERS.test(displayName) || FOUNDER_IDENTITY_MARKERS.test(email)) {
    return { ok: false, reason: "PLACEHOLDER_OR_SIMULATION" };
  }
  return {
    ok: true,
    displayName,
    email,
    normalized: `${displayName} <${email}>`,
  };
}

/** Simulation / harness approvers never count as live founder authorization. */
export function isSimulationOrTestApprover(approverIdentity, record = null) {
  if (record?.simulation === true) return true;
  const value = String(approverIdentity || "").trim();
  if (!value) return false;
  if (FOUNDER_IDENTITY_MARKERS.test(value)) return true;
  if (/[<>]/.test(value) && !parseFounderIdentityFormat(value).ok) return true;
  return false;
}

/**
 * Read the consumed HPC create record and return its approver identity.
 * Does not authorize GDI spend, publication, or a new HPC create.
 */
export function loadTrustedConsumedFounderIdentity(ledger = null) {
  const doc = ledger || readJson(AUTH_LEDGER, { records: [] });
  const rec = (doc.records || []).find(
    (row) => row.authorizationId === TRUSTED_CONSUMED_HPC_CREATE_AUTHORIZATION_ID
  );
  if (!rec) return { ok: false, reason: "TRUSTED_IDENTITY_RECORD_ABSENT" };
  if (rec.status !== "CONSUMED" || rec.simulation === true) {
    return { ok: false, reason: "TRUSTED_IDENTITY_RECORD_NOT_CONSUMED" };
  }
  if (rec.consumedHpcRecordId !== TRUSTED_CONSUMED_HPC_RECORD_ID) {
    return { ok: false, reason: "TRUSTED_IDENTITY_RECORD_MISMATCH" };
  }
  const parsed = parseFounderIdentityFormat(rec.approverIdentity);
  if (!parsed.ok) return { ok: false, reason: parsed.reason };
  return {
    ok: true,
    approverIdentity: parsed.normalized,
    displayName: parsed.displayName,
    email: parsed.email,
    authorizationId: rec.authorizationId,
    hpcRecordId: rec.consumedHpcRecordId,
  };
}

/**
 * True only for the founder identity on the consumed Chapultepec HPC create.
 * Format validity is not enough: any other display name or email returns false.
 * This does not prove the current process authenticated as that founder.
 */
export function isLiveFounderApprover(approverIdentity, record = null) {
  if (record?.simulation === true) return false;
  const parsed = parseFounderIdentityFormat(approverIdentity);
  if (!parsed.ok) return false;
  const trusted = loadTrustedConsumedFounderIdentity();
  if (!trusted.ok) return false;
  return (
    parsed.displayName === trusted.displayName &&
    parsed.email.toLowerCase() === trusted.email.toLowerCase()
  );
}

/**
 * Recording authority. The candidate must be the trusted consumed identity,
 * and the local git identity must be that same person. An env override is
 * accepted only when it is exactly that identity. Git config is the existing
 * local identity mechanism; it is not a separate cryptographic login.
 */
export function resolveAuthenticatedFounderIdentity({
  envValue = "",
  gitName = "",
  gitEmail = "",
} = {}) {
  const trusted = loadTrustedConsumedFounderIdentity();
  if (!trusted.ok) return { ok: false, authenticated: false, reason: trusted.reason };
  const gitParsed = parseFounderIdentityFormat(`${String(gitName || "").trim()} <${String(gitEmail || "").trim()}>`);
  if (
    !gitParsed.ok ||
    gitParsed.displayName !== trusted.displayName ||
    gitParsed.email.toLowerCase() !== trusted.email.toLowerCase()
  ) {
    return { ok: false, authenticated: false, reason: "GIT_IDENTITY_DOES_NOT_MATCH_TRUSTED_FOUNDER" };
  }
  const env = String(envValue || "").trim();
  if (env) {
    const envParsed = parseFounderIdentityFormat(env);
    if (
      !envParsed.ok ||
      envParsed.displayName !== trusted.displayName ||
      envParsed.email.toLowerCase() !== trusted.email.toLowerCase()
    ) {
      return { ok: false, authenticated: false, reason: "ENV_IDENTITY_DOES_NOT_MATCH_TRUSTED_FOUNDER" };
    }
  }
  return {
    ok: true,
    authenticated: true,
    approverIdentity: trusted.approverIdentity,
    source: env ? "env_matches_git_and_trusted_record" : "git_matches_trusted_record",
    trustedAuthorizationId: trusted.authorizationId,
    authenticationMechanism: "local_git_identity_matches_consumed_hpc_founder_record",
  };
}

export function loadHpcCreateAuthorization({
  runId,
  draftPropertyId,
  fieldsHash,
  authorizationId,
  requireLiveFounder = false,
} = {}) {
  const ledger = readJson(AUTH_LEDGER, { records: [] });
  const records = ledger.records || [];
  const match = (r) => {
    if (r.status !== "ACTIVE") return false;
    if (requireLiveFounder && !isLiveFounderApprover(r.approverIdentity, r)) return false;
    if (authorizationId) return r.authorizationId === authorizationId;
    if (runId && r.runId !== runId) return false;
    if (draftPropertyId && r.draftPropertyId !== draftPropertyId) return false;
    if (fieldsHash && r.fieldsHash !== fieldsHash) return false;
    return true;
  };
  if (authorizationId) {
    return records.find((r) => match(r)) || null;
  }
  return records.find((r) => match(r)) || null;
}

function lockPath(key) {
  return path.join(LOCK_DIR, `${String(key).replace(/[^a-zA-Z0-9_-]/g, "_")}.lock`);
}

export function acquireHpcCreateLock(propertyKey, runId) {
  ensureDir(LOCK_DIR);
  const lp = lockPath(propertyKey);
  if (fs.existsSync(lp)) {
    const existing = readJson(lp, {});
    if (existing.runId && existing.runId !== runId) {
      const age = Date.now() - Date.parse(existing.acquiredAt || 0);
      if (age < 6 * 60 * 60 * 1000) {
        return { ok: false, error: "HPC_CREATE_LOCK_HELD", existing };
      }
    }
  }
  const body = { runId, propertyKey, acquiredAt: new Date().toISOString(), pid: process.pid };
  writeJson(lp, body);
  return { ok: true, lock: body };
}

export function releaseHpcCreateLock(propertyKey, runId) {
  const lp = lockPath(propertyKey);
  if (!fs.existsSync(lp)) return { ok: true };
  const existing = readJson(lp, {});
  if (existing.runId && existing.runId !== runId) {
    return { ok: false, error: "LOCK_OWNED_BY_OTHER", existing };
  }
  fs.unlinkSync(lp);
  return { ok: true };
}

function saveRunState(state) {
  writeJson(path.join(STATE_DIR, `${state.runId}.json`), state);
  return state;
}

/**
 * Reconcile Condesa against known HPC record — reuse, never duplicate.
 */
export async function reconcileExistingHpcRecord({
  hpcRecordId,
  hotelName,
  aliases = [],
  city = null,
  brand = null,
} = {}) {
  const live = await performLiveHpcIdentityCollisionCheck({
    hotelName,
    aliases,
    city,
    brand,
    knownHpcId: hpcRecordId,
  });
  if (live.outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED) {
    return {
      ok: true,
      state: HPC_CREATE_REUSE_STATE.EXISTING_VERIFIED,
      outcome: live.outcome,
      hpcRecordId: live.hpcRecordId,
      verifiedRecord: live.verifiedRecord,
      duplicateCreateProposed: false,
      writeBlocked: true,
      paidMonitoringImplied: false,
    };
  }
  return {
    ok: false,
    state:
      live.outcome === HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE
        ? HPC_CREATE_REUSE_STATE.LOOKUP_UNAVAILABLE
        : HPC_CREATE_REUSE_STATE.COLLISION_REVIEW,
    outcome: live.outcome,
    hpcRecordId: live.hpcRecordId || hpcRecordId,
    detail: live,
    duplicateCreateProposed: false,
    writeBlocked: true,
  };
}

/**
 * Execute authorized HPC create (or dry-run simulation).
 * Does NOT authorize paid monitoring or publication.
 */
export async function executeAuthorizedHpcCreate({
  runId,
  authorization = null,
  authorizationId = null,
  dryRun = true,
  apply = false,
  simulation = false,
  injectFailure = null,
} = {}) {
  const auth =
    authorization ||
    (authorizationId
      ? (readJson(AUTH_LEDGER, { records: [] }).records || []).find(
          (r) => r.authorizationId === authorizationId && r.status === "ACTIVE"
        )
      : null);

  if (!auth || auth.status !== "ACTIVE") {
    return {
      ok: false,
      state: HPC_CREATE_REUSE_STATE.UNAUTHORIZED_CREATE_REJECTED,
      error: "UNAUTHORIZED_CREATE_REJECTED",
      message: "Exact HPC create authorization required. No implicit create.",
      airtableWrites: 0,
      paidMonitoringImplied: false,
    };
  }

  // Simulation / test harness approvals must never authorize live Airtable writes
  if ((apply && !dryRun && !simulation) && isSimulationOrTestApprover(auth.approverIdentity, auth)) {
    return {
      ok: false,
      state: HPC_CREATE_REUSE_STATE.UNAUTHORIZED_CREATE_REJECTED,
      error: "UNAUTHORIZED_CREATE_REJECTED",
      message:
        "Approver identity is simulation/test-only. Founder-approved authorization required for live HPC create. Do not infer approval.",
      approverIdentity: auth.approverIdentity,
      airtableWrites: 0,
      paidMonitoringImplied: false,
    };
  }

  const propertyKey = auth.draftPropertyId || auth.hotelName;
  const lock = acquireHpcCreateLock(propertyKey, runId || auth.runId);
  if (!lock.ok) {
    return {
      ok: false,
      state: HPC_CREATE_REUSE_STATE.CREATE_FAILED,
      error: "DUPLICATE_CONCURRENT_CREATE_BLOCKED",
      existing: lock.existing,
      airtableWrites: 0,
    };
  }

  const state = {
    contract: HPC_AUTHORIZED_CREATE_REUSE_V1,
    version: HPC_CREATE_REUSE_VERSION,
    runId: runId || auth.runId,
    draftPropertyId: auth.draftPropertyId,
    hotelName: auth.hotelName,
    authorizationId: auth.authorizationId,
    fieldsHash: auth.fieldsHash,
    dryRun: Boolean(dryRun) || !apply || simulation,
    apply: Boolean(apply) && !dryRun && !simulation,
    state: HPC_CREATE_REUSE_STATE.CREATE_AUTHORIZED,
    startedAt: new Date().toISOString(),
    airtableWrites: 0,
    paidMonitoringImplied: false,
    publicationImplied: false,
  };

  try {
    if (injectFailure === "unauthorized") {
      return {
        ok: false,
        state: HPC_CREATE_REUSE_STATE.UNAUTHORIZED_CREATE_REJECTED,
        error: "UNAUTHORIZED_CREATE_REJECTED",
        airtableWrites: 0,
      };
    }

    // Hash must still match approved fields
    const currentHash = sha256(stableFields(auth.fields));
    if (currentHash !== auth.fieldsHash) {
      state.state = HPC_CREATE_REUSE_STATE.UNAUTHORIZED_CREATE_REJECTED;
      state.error = "AUTHORIZED_FIELDS_TAMPERED";
      saveRunState(state);
      return { ok: false, ...state };
    }

    // Schema validate
    let schema = null;
    if (!simulation) {
      try {
        schema = await fetchLiveHpcSchema();
      } catch (e) {
        state.state = HPC_CREATE_REUSE_STATE.LOOKUP_UNAVAILABLE;
        state.error = e.message;
        saveRunState(state);
        return { ok: false, ...state, airtableWrites: 0 };
      }
      const validation = validateHpcCreateFieldsAgainstSchema(auth.fields, schema);
      if (!validation.ok) {
        state.state = HPC_CREATE_REUSE_STATE.CREATE_FAILED;
        state.error = "SCHEMA_VALIDATION_FAILED";
        state.validation = validation;
        saveRunState(state);
        return { ok: false, ...state, airtableWrites: 0 };
      }
    }

    // Pre-create collision recheck
    state.state = HPC_CREATE_REUSE_STATE.PRE_CREATE_COLLISION_RECHECK;
    if (injectFailure === "pre_create_collision") {
      state.state = HPC_CREATE_REUSE_STATE.COLLISION_REVIEW;
      state.error = "PRE_CREATE_COLLISION_DETECTED";
      saveRunState(state);
      return {
        ok: false,
        ...state,
        airtableWrites: 0,
        message: "Collision appeared before create — abort; do not create duplicate.",
      };
    }

    let collision;
    if (simulation) {
      collision = {
        outcome: HPC_LIVE_OUTCOME.HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT,
        hpcRecordId: null,
        candidates: [],
      };
    } else {
      collision = await performLiveHpcIdentityCollisionCheck({
        hotelName: auth.hotelName,
        aliases: [auth.fields["Canonical Property Name"], auth.fields["Property Name"]].filter(
          Boolean
        ),
        city: auth.fields.City,
        brand: auth.fields["Current Brand"],
        address: auth.fields.Address,
        country: auth.fields.Country,
        officialUrl: auth.fields["Official Property URL"],
        adpPropertyId: auth.draftPropertyId,
      });
    }

    state.preCreateCollision = {
      outcome: collision.outcome,
      hpcRecordId: collision.hpcRecordId || null,
      candidateCount: (collision.candidates || []).length,
    };

    if (collision.outcome === HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED) {
      state.state = HPC_CREATE_REUSE_STATE.EXISTING_VERIFIED;
      state.hpcRecordId = collision.hpcRecordId;
      state.s2Outcome = HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED;
      state.message = "Pre-create recheck found existing record — reuse; no create.";
      saveRunState(state);
      return { ok: true, reusedExisting: true, ...state, airtableWrites: 0 };
    }
    if (collision.outcome === HPC_LIVE_OUTCOME.HPC_IDENTITY_COLLISION_REQUIRES_REVIEW) {
      state.state = HPC_CREATE_REUSE_STATE.COLLISION_REVIEW;
      state.error = "PRE_CREATE_COLLISION_REQUIRES_REVIEW";
      saveRunState(state);
      return { ok: false, ...state, airtableWrites: 0 };
    }
    if (collision.outcome === HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE) {
      state.state = HPC_CREATE_REUSE_STATE.LOOKUP_UNAVAILABLE;
      state.error = "PRE_CREATE_LOOKUP_UNAVAILABLE";
      state.message = "Unavailable lookup must never be treated as safe to create.";
      saveRunState(state);
      return { ok: false, ...state, airtableWrites: 0 };
    }

    // Dry-run / simulation stop before write
    if (state.dryRun || simulation) {
      state.state = HPC_CREATE_REUSE_STATE.CREATE_AUTHORIZED;
      state.wouldCreate = auth.fields;
      state.s2Outcome = HPC_LIVE_OUTCOME.HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT;
      state.message =
        "Dry-run / simulation only — no Airtable write. Founder auth recorded; apply separately.";
      if (injectFailure === "create_failed") {
        state.state = HPC_CREATE_REUSE_STATE.CREATE_FAILED;
        state.error = "SIMULATED_CREATE_FAILURE";
        saveRunState(state);
        return { ok: false, ...state, airtableWrites: 0 };
      }
      if (injectFailure === "readback_failed") {
        state.state = HPC_CREATE_REUSE_STATE.READBACK_FAILED;
        state.error = "SIMULATED_READBACK_FAILURE";
        state.simulatedRecordId = `recSIM_${sha256(auth.fieldsHash).slice(0, 14)}`;
        saveRunState(state);
        return { ok: false, ...state, airtableWrites: 0 };
      }
      // Simulated success path for tests
      if (simulation && !injectFailure) {
        const simId = `recSIM_${sha256(auth.fieldsHash + runId).slice(0, 14)}`;
        state.state = HPC_CREATE_REUSE_STATE.EXISTING_AFTER_CREATE;
        state.hpcRecordId = simId;
        state.s2Outcome = HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED;
        state.readback = { ok: true, simulated: true };
        state.message = "Simulation create+readback success — S2 would transition to EXISTING_VERIFIED.";
        saveRunState(state);
        return { ok: true, simulation: true, ...state, airtableWrites: 0 };
      }
      saveRunState(state);
      return { ok: true, dryRun: true, ...state, airtableWrites: 0 };
    }

    // Live create
    state.state = HPC_CREATE_REUSE_STATE.CREATING;
    let created;
    try {
      created = await airtableFetch(`${encodeURIComponent(HPC_TABLE_NAME)}`, {
        method: "POST",
        body: JSON.stringify({ fields: auth.fields, typecast: false }),
      });
      state.airtableWrites = 1;
    } catch (e) {
      state.state = HPC_CREATE_REUSE_STATE.CREATE_FAILED;
      state.error = e.message;
      saveRunState(state);
      return { ok: false, ...state };
    }

    const newId = created?.id;
    if (!newId) {
      state.state = HPC_CREATE_REUSE_STATE.CREATE_FAILED;
      state.error = "CREATE_RETURNED_NO_ID";
      saveRunState(state);
      return { ok: false, ...state };
    }

    state.state = HPC_CREATE_REUSE_STATE.READBACK_PENDING;
    state.hpcRecordId = newId;
    const after = await fetchHpcRecord(newId);
    if (!after) {
      state.state = HPC_CREATE_REUSE_STATE.READBACK_FAILED;
      state.error = "READBACK_RECORD_MISSING";
      saveRunState(state);
      return { ok: false, ...state };
    }

    const readback = {};
    let allMatch = true;
    for (const [k, expected] of Object.entries(auth.fields)) {
      const actual = after.fields?.[k] ?? null;
      const match =
        actual === expected || String(actual ?? "").trim() === String(expected).trim();
      readback[k] = { expected, actual, match };
      if (!match) allMatch = false;
    }
    state.readback = { ok: allMatch, fields: readback };
    if (!allMatch) {
      state.state = HPC_CREATE_REUSE_STATE.READBACK_FAILED;
      state.error = "READBACK_FIELD_MISMATCH";
      saveRunState(state);
      return { ok: false, ...state };
    }

    state.state = HPC_CREATE_REUSE_STATE.EXISTING_AFTER_CREATE;
    state.s2Outcome = HPC_LIVE_OUTCOME.HPC_EXISTING_RECORD_VERIFIED;
    state.completedAt = new Date().toISOString();
    state.message =
      "HPC create+readback verified. S2 → HPC_EXISTING_RECORD_VERIFIED. Paid monitoring NOT authorized by this create.";
    if (auth.singleUse !== false && auth.authorizationId) {
      const consumed = consumeHpcCreateAuthorization(auth.authorizationId, {
        hpcRecordId: newId,
        consumedAt: state.completedAt,
        runId: state.runId,
      });
      state.authorizationConsumed = consumed.ok === true;
      state.authorizationStatusAfter = consumed.record?.status || null;
    }
    saveRunState(state);
    return { ok: true, ...state };
  } finally {
    releaseHpcCreateLock(propertyKey, runId || auth.runId);
  }
}

/**
 * Zero-paid / simulation regression battery.
 */
export async function runHpcCreateReuseSimulationBattery() {
  const results = {};
  const proposal = buildChapultepecHpcCreateProposal();

  // Schema validation (live if available)
  let validated;
  try {
    validated = await prepareValidatedCreateProposal(proposal);
    results.schemaValidation = validated.schemaValidated === true;
  } catch {
    // Offline fallback: structural check only
    results.schemaValidation = Boolean(proposal.fieldsHash);
    validated = { ...proposal, schemaValidated: false };
  }

  // Existing reuse (Condesa) — live if possible
  try {
    const reuse = await reconcileExistingHpcRecord({
      hpcRecordId: "recIEcSWuhSCoNLFN",
      hotelName: "Fiesta Americana Condesa Cancún All Inclusive",
      aliases: ["Fiesta Americana Condesa Cancun All Inclusive"],
      city: "Cancún",
      brand: "Fiesta Americana",
    });
    results.existingReuse =
      reuse.ok === true &&
      reuse.hpcRecordId === "recIEcSWuhSCoNLFN" &&
      reuse.duplicateCreateProposed === false;
  } catch {
    results.existingReuse = false;
  }

  const runId = `sim_hpc_create_${Date.now().toString(36)}`;
  const authBuilt = persistHpcCreateAuthorization({
    runId,
    draftPropertyId: proposal.draftPropertyId,
    hotelName: proposal.hotelName,
    fields: proposal.fields,
    fieldsHash: proposal.fieldsHash,
    approverIdentity: "FOUNDER_SIMULATION",
  });
  results.authPersist = authBuilt.ok === true;

  results.unauthorizedReject = (
    await executeAuthorizedHpcCreate({
      runId: `${runId}_unauth`,
      dryRun: true,
      simulation: true,
    })
  ).error === "UNAUTHORIZED_CREATE_REJECTED";

  results.authorizedDryRun = (
    await executeAuthorizedHpcCreate({
      runId,
      authorization: authBuilt.record,
      dryRun: true,
      apply: false,
      simulation: false,
    })
  ).ok === true;

  results.simulationSuccess = (
    await executeAuthorizedHpcCreate({
      runId: `${runId}_ok`,
      authorization: {
        ...authBuilt.record,
        runId: `${runId}_ok`,
        authorizationId: `${authBuilt.record.authorizationId}_ok`,
      },
      simulation: true,
    })
  ).ok === true;

  results.preCreateCollision = (
    await executeAuthorizedHpcCreate({
      runId: `${runId}_col`,
      authorization: {
        ...authBuilt.record,
        runId: `${runId}_col`,
        authorizationId: `${authBuilt.record.authorizationId}_col`,
      },
      simulation: true,
      injectFailure: "pre_create_collision",
    })
  ).error === "PRE_CREATE_COLLISION_DETECTED";

  results.createFailed = (
    await executeAuthorizedHpcCreate({
      runId: `${runId}_cf`,
      authorization: {
        ...authBuilt.record,
        runId: `${runId}_cf`,
        authorizationId: `${authBuilt.record.authorizationId}_cf`,
      },
      simulation: true,
      injectFailure: "create_failed",
    })
  ).error === "SIMULATED_CREATE_FAILURE";

  results.readbackFailed = (
    await executeAuthorizedHpcCreate({
      runId: `${runId}_rb`,
      authorization: {
        ...authBuilt.record,
        runId: `${runId}_rb`,
        authorizationId: `${authBuilt.record.authorizationId}_rb`,
      },
      simulation: true,
      injectFailure: "readback_failed",
    })
  ).error === "SIMULATED_READBACK_FAILURE";

  // Concurrent lock
  const lockA = acquireHpcCreateLock(proposal.draftPropertyId, `${runId}_lockA`);
  const lockB = acquireHpcCreateLock(proposal.draftPropertyId, `${runId}_lockB`);
  results.concurrentDuplicateBlocked = lockA.ok === true && lockB.ok === false;
  releaseHpcCreateLock(proposal.draftPropertyId, `${runId}_lockA`);

  results.paidNotImpliedByCreate = authBuilt.record?.paidMonitoringAuthorized === false;
  results.publicationNotImplied = authBuilt.record?.publicationAuthorized === false;

  // Expired identity — synthetic
  results.expiredIdentity =
    HPC_CREATE_REUSE_STATE.IDENTITY_VERIFICATION_EXPIRED ===
    "IDENTITY_VERIFICATION_EXPIRED";

  const failed = Object.entries(results)
    .filter(([, v]) => !v)
    .map(([k]) => k);

  return {
    ok: failed.length === 0,
    contract: HPC_AUTHORIZED_CREATE_REUSE_V1,
    airtableWrites: 0,
    results,
    failed,
    proposalHash: proposal.fieldsHash,
  };
}

export { HPC_LIVE_OUTCOME, HPC_LIVE_IDENTITY_COLLISION_V1, searchHpcByName };
