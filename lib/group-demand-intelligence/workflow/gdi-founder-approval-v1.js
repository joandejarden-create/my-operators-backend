/**
 * Authenticated founder approval for GDI persistence and publication.
 * A Memberstack-verified session is required. Git identity, env strings, and caller flags are ignored.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { verifyMemberstackToken } from "../../memberstack/verify-token.js";

const TRUSTED_CONSUMED_HPC_CREATE_AUTHORIZATION_ID = "hpc_create_auth_5f70fe679b52f30a";
const TRUSTED_CONSUMED_HPC_RECORD_ID = "recterKmNPdntCm0v";
const FOUNDER_IDENTITY_MARKERS =
  /SIMULATION|TEST_HARNESS|DRY_RUN|PLACEHOLDER|actual approved identity|live identity already used|VERIFIED_LIVE_FOUNDER_IDENTITY/i;
const FOUNDER_EMAIL = /^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$/;

function parseFounderIdentityFormat(approverIdentity) {
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
  return { ok: true, displayName, email };
}

/** Read-only founder email from the consumed HPC create ledger. Not a publication approval. */
export function loadTrustedConsumedFounderIdentity() {
  const file = path.join(
    path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../.."),
    "data/ai-demand-positioning/runtime/hpc-authorized-create-reuse-v1/create-authorization-ledger.json"
  );
  if (!fs.existsSync(file)) return { ok: false, reason: "TRUSTED_IDENTITY_RECORD_ABSENT" };
  const doc = JSON.parse(fs.readFileSync(file, "utf8"));
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
  return { ok: true, email: parsed.email, displayName: parsed.displayName };
}

export const GDI_FOUNDER_APPROVAL_VERSION = "gdi-founder-approval-v1";
export const APPROVAL_OPERATION = Object.freeze({
  PERSISTENCE: "PERSISTENCE",
  PUBLICATION: "PUBLICATION",
  RESEARCH_CANARY: "RESEARCH_CANARY",
  BETHESDA_WORKSTREAM_RESEARCH: "BETHESDA_WORKSTREAM_RESEARCH",
  RESEARCH_PREVIEW: "RESEARCH_PREVIEW",
});

function emptyLedger() {
  return { version: GDI_FOUNDER_APPROVAL_VERSION, records: [] };
}

function readLedger(ledgerPath) {
  if (!ledgerPath || !fs.existsSync(ledgerPath)) return emptyLedger();
  return JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
}

function writeLedger(ledgerPath, ledger) {
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  fs.writeFileSync(ledgerPath, JSON.stringify(ledger, null, 2));
}

function scopeKey(input) {
  return JSON.stringify({
    operation: input.operation,
    hotelIds: [...input.hotelIds].sort(),
    sourceHash: input.sourceHash,
    opportunityIds: [...(input.opportunityIds || [])].sort(),
    provider: input.provider || null,
    maxRequests: input.maxRequests ?? null,
    maxRequestsPerHotel: input.maxRequestsPerHotel ?? null,
    queries: input.queries || [],
    subsetHash: input.subsetHash || null,
    parentSnapshotVersion: input.parentSnapshotVersion || null,
    customerAccessScope: input.customerAccessScope || null,
  });
}

export async function requestFounderApproval({
  token = "",
  operation,
  hotelIds = [],
  sourceHash = "",
  opportunityIds = [],
  restrictions = [],
  ledgerPath,
  verifier = verifyMemberstackToken,
  trustedEmail = "",
  provider = null,
  maxRequests = null,
  maxRequestsPerHotel = null,
  maxCredits = null,
  pathways = [],
  queries = [],
  expiresAt = null,
  subsetHash = null,
  parentSnapshotVersion = null,
  customerAccessScope = null,
} = {}) {
  if (![APPROVAL_OPERATION.PERSISTENCE, APPROVAL_OPERATION.PUBLICATION, APPROVAL_OPERATION.RESEARCH_CANARY, APPROVAL_OPERATION.BETHESDA_WORKSTREAM_RESEARCH, APPROVAL_OPERATION.RESEARCH_PREVIEW].includes(operation)) {
    return { ok: false, reason: "UNKNOWN_OPERATION", written: false };
  }
  if (!String(token || "").trim()) {
    return { ok: false, reason: "MISSING_AUTHENTICATED_SESSION", written: false };
  }
  if (!ledgerPath) return { ok: false, reason: "LEDGER_PATH_REQUIRED", written: false };
  let session;
  try {
    session = await verifier(token);
  } catch (error) {
    return { ok: false, reason: "SESSION_NOT_VERIFIED", detail: String(error.message || error).slice(0, 120), written: false };
  }
  const email = String(session?.email || "").trim().toLowerCase();
  const memberId = String(session?.id || "").trim();
  if (!email || !memberId || session?.verifiedVia == null) {
    return { ok: false, reason: "SESSION_NOT_VERIFIED", written: false };
  }
  const trusted = trustedEmail
    ? { ok: true, email: trustedEmail }
    : loadTrustedConsumedFounderIdentity();
  if (!trusted.ok || email !== String(trusted.email || "").trim().toLowerCase()) {
    return { ok: false, reason: "SESSION_EMAIL_NOT_FOUNDER", written: false };
  }
  const ledger = readLedger(ledgerPath);
  const key = scopeKey({ operation, hotelIds, sourceHash, opportunityIds, provider, maxRequests, maxRequestsPerHotel, queries, subsetHash, parentSnapshotVersion, customerAccessScope });
  const existing = (ledger.records || []).find((row) => row.scopeKey === key && row.status === "APPROVED");
  if (existing) return { ok: true, reused: true, approvalId: existing.approvalId, written: false };
  const now = new Date().toISOString();
  const record = {
    approvalId: `gdi_appr_${crypto.randomBytes(8).toString("hex")}`,
    version: GDI_FOUNDER_APPROVAL_VERSION,
    operation,
    hotelIds: [...hotelIds],
    sourceHash,
    opportunityIds: [...opportunityIds],
    restrictions: [...restrictions],
    memberId,
    verifiedVia: session.verifiedVia,
    provider,
    maxRequests,
    maxRequestsPerHotel,
    maxCredits,
    pathways,
    queries,
    expiresAt,
    subsetHash,
    parentSnapshotVersion,
    customerAccessScope,
    status: "APPROVED",
    approvedAt: now,
    scopeKey: key,
    singleUse: true,
    audit: [{ event: "CREATED", at: now }],
  };
  ledger.records.push(record);
  writeLedger(ledgerPath, ledger);
  return { ok: true, reused: false, approvalId: record.approvalId, written: true };
}

export function consumeFounderApproval({ ledgerPath, approvalId } = {}) {
  const ledger = readLedger(ledgerPath);
  const record = (ledger.records || []).find((row) => row.approvalId === approvalId);
  if (!record) return { ok: false, reason: "APPROVAL_NOT_FOUND" };
  if (record.status === "REVOKED") return { ok: false, reason: "APPROVAL_REVOKED" };
  if (record.status === "CONSUMED") return { ok: false, reason: "APPROVAL_ALREADY_CONSUMED" };
  record.status = "CONSUMED";
  record.consumedAt = new Date().toISOString();
  record.audit.push({ event: "CONSUMED", at: record.consumedAt });
  writeLedger(ledgerPath, ledger);
  return { ok: true, approval: record };
}

export function revokeFounderApproval({ ledgerPath, approvalId } = {}) {
  const ledger = readLedger(ledgerPath);
  const record = (ledger.records || []).find((row) => row.approvalId === approvalId);
  if (!record) return { ok: false, reason: "APPROVAL_NOT_FOUND" };
  if (record.status === "CONSUMED") return { ok: false, reason: "CONSUMED_APPROVAL_CANNOT_BE_REVOKED" };
  record.status = "REVOKED";
  record.revokedAt = new Date().toISOString();
  record.audit.push({ event: "REVOKED", at: record.revokedAt });
  writeLedger(ledgerPath, ledger);
  return { ok: true };
}

export function executeApprovedPersistence({ ledgerPath, approvalId, hotelId, sourceHash, write } = {}) {
  const ledger = readLedger(ledgerPath);
  const record = (ledger.records || []).find((row) => row.approvalId === approvalId);
  if (!record || record.status !== "CONSUMED" || record.operation !== APPROVAL_OPERATION.PERSISTENCE) {
    return { written: false, reason: "PERSISTENCE_APPROVAL_NOT_CONSUMED" };
  }
  if (!record.hotelIds.includes(hotelId) || record.sourceHash !== sourceHash) {
    return { written: false, reason: "PERSISTENCE_SCOPE_MISMATCH" };
  }
  if (record.executedAt) return { written: false, reason: "ALREADY_EXECUTED" };
  if (typeof write !== "function") return { written: false, reason: "WRITE_NOT_PROVIDED" };
  write();
  record.executedAt = new Date().toISOString();
  record.audit.push({ event: "EXECUTED", at: record.executedAt });
  writeLedger(ledgerPath, ledger);
  return { written: true };
}

export const RESEARCH_CANARY_SCOPE = Object.freeze({
  operation: "RESEARCH_CANARY",
  provider: "serpapi",
  hotelIds: ["recRkGGNKslJptFIL", "recterKmNPdntCm0v"],
  maxRequests: 2,
  maxRequestsPerHotel: 1,
  sourceHash: "gdi-research-canary-v5-5",
  queries: [
    { hotelId: "recRkGGNKslJptFIL", workstream: "corporate_meetings_training", pathway: "corporate_institutional_programs", language: "en", query: "Northern Virginia corporate training program group hotel" },
    { hotelId: "recterKmNPdntCm0v", workstream: "corporate_meetings_training", pathway: "corporate_institutional_programs", language: "es", query: "capacitación empresarial Ciudad de México programa de grupos" },
  ],
  restrictions: ["no_publication", "no_canonical_write", "no_automatic_retry", "historical_serpapi_exceptions_remain_open"],
});

export function assertFounderCanarySession({ verifiedVia = "", email = "", trustedEmail = "" } = {}) {
  if (verifiedVia !== "rest" && verifiedVia !== "jwks") return { ok: false, reason: "SESSION_NOT_LIVE" };
  if (!email || email.toLowerCase() !== String(trustedEmail || "").toLowerCase()) {
    return { ok: false, reason: "SESSION_EMAIL_NOT_FOUNDER" };
  }
  return { ok: true };
}

export function loadLiveResearchCanaryApproval(ledgerPath = "") {
  const file = ledgerPath || path.join(path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../.."), "data/group-demand-intelligence/gdi-founder-approval-v1/approval-ledger-v1.json");
  if (!fs.existsSync(file)) return { ok: false, reason: "MEMBERSTACK_CANARY_APPROVAL_REQUIRED" };
  const ledger = readLedger(file);
  const record = (ledger.records || []).find((row) => row.operation === APPROVAL_OPERATION.RESEARCH_CANARY && row.status === "APPROVED");
  if (!record) return { ok: false, reason: "MEMBERSTACK_CANARY_APPROVAL_REQUIRED" };
  if (!record.memberId || (record.verifiedVia !== "rest" && record.verifiedVia !== "jwks")) {
    return { ok: false, reason: "SESSION_NOT_LIVE" };
  }
  if (record.provider !== "serpapi" || Number(record.maxRequests) !== 2 || Number(record.maxRequestsPerHotel) !== 1) {
    return { ok: false, reason: "CANARY_SCOPE_INVALID" };
  }
  const hotels = new Set(record.hotelIds || []);
  if (hotels.size !== 2 || !hotels.has("recRkGGNKslJptFIL") || !hotels.has("recterKmNPdntCm0v")) {
    return { ok: false, reason: "CANARY_SCOPE_INVALID" };
  }
  const queryTexts = (record.queries || []).map((row) => (typeof row === "string" ? row : row?.query || ""));
  if (queryTexts.length !== 2 || queryTexts.some((query) => !query || /\bcongreso\b/i.test(query))) {
    return { ok: false, reason: "CANARY_SCOPE_INVALID" };
  }
  const restrictions = record.restrictions || [];
  if (!restrictions.includes("no_publication") || !restrictions.includes("no_canonical_write") || !restrictions.includes("no_automatic_retry")) {
    return { ok: false, reason: "CANARY_SCOPE_INVALID" };
  }
  if (record.expiresAt && Date.parse(record.expiresAt) < Date.now()) {
    return { ok: false, reason: "CANARY_EXPIRED" };
  }
  return { ok: true, approval: record };
}
