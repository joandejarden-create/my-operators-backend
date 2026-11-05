/**
 * File-based AVA store.
 *
 * Durable mode (AVA_STORAGE_MODE=local|required):
 *   {AVA_STORAGE_ROOT}/audits/{propertyId}/AUDIT_META.json
 *   {AVA_STORAGE_ROOT}/audits/{propertyId}/versions/{versionId}.json
 *   {AVA_STORAGE_ROOT}/audits/{propertyId}/events/{eventId}.json
 *
 * Legacy mode (AVA_STORAGE_MODE=off):
 *   data/ai-visibility-audit-action-plan/production/store/...
 *
 * API surface stays compatible. Writes in durable mode fail closed when the
 * volume is missing — no silent ephemeral fallback.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AVA_LIFECYCLE_STATUSES,
  assertAvaLifecycleTransition,
} from "./lifecycle-v1.js";
import { AVA_PRODUCTION_STORE_ROOT } from "./certified-baseline-paths-v1.js";
import {
  assertAvaDurableWritable,
  ensureAvaDurableStorage,
  isAvaDurableStorageEnabled,
  resolveAvaStorageMode,
  AVA_STORAGE_MODE,
} from "./resolve-ava-storage-root-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const LEGACY_STORE_ROOT = path.join(
  ROOT,
  "data/ai-visibility-audit-action-plan/store"
);

function resolveLegacyStoreRoot() {
  if (process.env.AVA_STORE_ROOT) {
    return path.resolve(process.env.AVA_STORE_ROOT);
  }
  if (
    fs.existsSync(path.join(AVA_PRODUCTION_STORE_ROOT, "adp_spice_island_beach_resort"))
  ) {
    return AVA_PRODUCTION_STORE_ROOT;
  }
  if (fs.existsSync(AVA_PRODUCTION_STORE_ROOT)) {
    const kids = fs
      .readdirSync(AVA_PRODUCTION_STORE_ROOT)
      .filter((n) => n.startsWith("adp_"));
    if (kids.length) return AVA_PRODUCTION_STORE_ROOT;
  }
  return LEGACY_STORE_ROOT;
}

function activeStoreRoot() {
  if (isAvaDurableStorageEnabled()) {
    const state = assertAvaDurableWritable();
    return state.layout.audits;
  }
  return resolveLegacyStoreRoot();
}

/** Public export — resolves dynamically so tests can flip env mid-process. */
export function getAvaStoreRoot() {
  try {
    return activeStoreRoot();
  } catch (_err) {
    return resolveLegacyStoreRoot();
  }
}

/** @deprecated Prefer getAvaStoreRoot(); kept for existing importers. */
export const AVA_STORE_ROOT = resolveLegacyStoreRoot();

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function atomicWriteJson(filePath, obj) {
  ensureDir(path.dirname(filePath));
  const tmp = `${filePath}.${process.pid}.${Date.now().toString(36)}.tmp`;
  const body = `${JSON.stringify(obj, null, 2)}\n`;
  fs.writeFileSync(tmp, body, "utf8");
  fs.renameSync(tmp, filePath);
}

function auditDir(propertyId) {
  return path.join(activeStoreRoot(), propertyId);
}

function metaPath(propertyId) {
  return path.join(auditDir(propertyId), "AUDIT_META.json");
}

function versionsDir(propertyId) {
  return path.join(auditDir(propertyId), "versions");
}

function eventsDir(propertyId) {
  return path.join(auditDir(propertyId), "events");
}

function requireWritableStore() {
  if (isAvaDurableStorageEnabled()) {
    assertAvaDurableWritable();
  }
}

export function listStoredAudits() {
  let root;
  try {
    root = activeStoreRoot();
  } catch (err) {
    if (resolveAvaStorageMode() === AVA_STORAGE_MODE.REQUIRED) throw err;
    root = resolveLegacyStoreRoot();
  }
  if (!fs.existsSync(root)) return [];
  return fs
    .readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isDirectory() && !d.name.startsWith("."))
    .map((d) => {
      const metaFile = path.join(root, d.name, "AUDIT_META.json");
      if (!fs.existsSync(metaFile)) {
        return { propertyId: d.name, status: "NOT_STARTED" };
      }
      return JSON.parse(fs.readFileSync(metaFile, "utf8"));
    })
    .sort((a, b) =>
      String(a.name || a.propertyId).localeCompare(String(b.name || b.propertyId))
    );
}

export function getAuditMeta(propertyId) {
  let p;
  try {
    p = metaPath(propertyId);
  } catch (_err) {
    p = path.join(resolveLegacyStoreRoot(), propertyId, "AUDIT_META.json");
  }
  if (!fs.existsSync(p)) {
    // Read fallback: seed packaged under production/store when durable empty
    const seed = path.join(AVA_PRODUCTION_STORE_ROOT, propertyId, "AUDIT_META.json");
    if (fs.existsSync(seed)) {
      return JSON.parse(fs.readFileSync(seed, "utf8"));
    }
    return null;
  }
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function upsertAuditMeta(propertyId, patch = {}) {
  requireWritableStore();
  ensureDir(auditDir(propertyId));
  const prev = getAuditMeta(propertyId) || {
    propertyId,
    status: "NOT_STARTED",
    createdAt: new Date().toISOString(),
    versions: [],
    decisionEvents: [],
    approvalEvents: [],
    researchCostUsd: 0,
    authorizedBudgetUsd: 0,
    paidResearchAuthorized: false,
    publicationAuthorized: false,
    customerReadyAuthorized: false,
  };
  if (patch.status && patch.status !== prev.status && patch.forceStatus !== true) {
    assertAvaLifecycleTransition(prev.status, patch.status);
  }
  if (patch.status && !AVA_LIFECYCLE_STATUSES.includes(patch.status)) {
    throw new Error(`INVALID_AVA_STATUS: ${patch.status}`);
  }
  const next = {
    ...prev,
    ...patch,
    propertyId,
    updatedAt: new Date().toISOString(),
  };
  atomicWriteJson(metaPath(propertyId), next);
  return next;
}

export function saveAuditVersion(propertyId, versionId, payload, extras = {}) {
  requireWritableStore();
  const dir = versionsDir(propertyId);
  ensureDir(dir);
  const file = path.join(dir, `${versionId}.json`);
  const record = {
    versionId,
    propertyId,
    kind: extras.kind || "REPORT_VERSION",
    savedAt: new Date().toISOString(),
    status: extras.status || payload.status || "ACTION_PLAN_STAGED",
    payload,
    evidenceRegister: extras.evidenceRegister || null,
    validation: extras.validation || null,
    pdfRelativePath: extras.pdfRelativePath || null,
    researchCostUsd: extras.researchCostUsd ?? 0,
  };
  atomicWriteJson(file, record);
  const meta = getAuditMeta(propertyId) || { propertyId, versions: [] };
  const versions = Array.isArray(meta.versions) ? [...meta.versions] : [];
  const idx = versions.findIndex((v) => v.versionId === versionId);
  const entry = {
    versionId,
    kind: record.kind,
    savedAt: record.savedAt,
    status: record.status,
    pdfRelativePath: record.pdfRelativePath,
    actionCount: Array.isArray(payload.actions) ? payload.actions.length : 0,
  };
  if (idx >= 0) versions[idx] = entry;
  else versions.push(entry);
  return upsertAuditMeta(propertyId, {
    ...meta,
    name: payload.property?.name || meta.name,
    market:
      payload.property?.market || payload.property?.coverLocation || meta.market,
    latestVersionId: versionId,
    versions,
    status: extras.metaStatus || record.status,
    forceStatus: extras.forceStatus === true,
    acceptedRecommendationCount: entry.actionCount,
    researchCostUsd: extras.researchCostUsd ?? meta.researchCostUsd ?? 0,
    adpBaselinePeriodId:
      payload.reporting?.adpPeriodId || meta.adpBaselinePeriodId || null,
    pdfStatus: record.pdfRelativePath ? "STAGED" : meta.pdfStatus || "MISSING",
    evidenceReviewStatus:
      extras.evidenceReviewStatus || meta.evidenceReviewStatus || "PENDING",
    customerReadinessStatus:
      meta.customerReadinessStatus || "NOT_CUSTOMER_READY",
    publicationAuthorizationStatus:
      meta.publicationAuthorizationStatus || "NOT_AUTHORIZED",
  });
}

/**
 * Record a recommendation decision WITHOUT creating a new report/PDF version.
 * Updates the latest version payload in place and appends an event.
 */
export function recordRecommendationDecision(
  propertyId,
  actionId,
  decisionPayload
) {
  requireWritableStore();
  const latest = getLatestAuditVersion(propertyId);
  if (!latest) throw new Error("NO_AUDIT_VERSION");
  const payload = latest.payload;
  const action = (payload.actions || []).find((a) => a.actionId === actionId);
  if (!action) throw new Error("ACTION_NOT_FOUND");

  const at = decisionPayload.at || new Date().toISOString();
  action.adminDecision = {
    decision: decisionPayload.decision,
    note: decisionPayload.note || null,
    at,
    actor: decisionPayload.actor || "ai-admin",
  };
  if (decisionPayload.decision === "REJECT") {
    action.implementationStatus = "NOT_STARTED";
  }

  // In-place update of the report version record (same versionId / PDF).
  const versionFile = path.join(
    versionsDir(propertyId),
    `${latest.versionId}.json`
  );
  const updated = {
    ...latest,
    payload,
    savedAt: latest.savedAt,
    decisionUpdatedAt: at,
  };
  // If latest only existed in seed (read-only path), write into durable store.
  ensureDir(versionsDir(propertyId));
  atomicWriteJson(versionFile, updated);

  const eventId = `decision_${actionId}_${Date.now().toString(36)}`;
  const event = {
    eventId,
    kind: "RECOMMENDATION_DECISION",
    propertyId,
    versionId: latest.versionId,
    actionId,
    decision: decisionPayload.decision,
    note: decisionPayload.note || null,
    actor: decisionPayload.actor || "ai-admin",
    at,
    pdfRelativePath: latest.pdfRelativePath || null,
  };
  ensureDir(eventsDir(propertyId));
  atomicWriteJson(path.join(eventsDir(propertyId), `${eventId}.json`), event);

  const meta = getAuditMeta(propertyId) || { propertyId };
  const decisionEvents = Array.isArray(meta.decisionEvents)
    ? [...meta.decisionEvents]
    : [];
  decisionEvents.push({
    eventId,
    actionId,
    decision: event.decision,
    at,
    versionId: latest.versionId,
  });

  const priorEvidence = String(meta.evidenceReviewStatus || latest.status || "");
  const preserveEvidence = new Set([
    "AUDIT_VALIDATED",
    "FOUNDER_REVIEWED",
    "FOUNDER_REVIEWED_STAGED",
    "FOUNDER_APPROVED",
    "FOUNDER_APPROVED_FOR_CUSTOMER",
  ]);

  return upsertAuditMeta(propertyId, {
    ...meta,
    latestVersionId: latest.versionId,
    decisionEvents,
    evidenceReviewStatus: preserveEvidence.has(priorEvidence)
      ? priorEvidence
      : "IN_REVIEW",
    // Do not push a new versions[] entry — decisions are events.
    forceStatus: true,
  });
}

/**
 * Append founder approval / status event without creating a PDF version.
 */
export function recordApprovalEvent(propertyId, eventPayload) {
  requireWritableStore();
  const at = eventPayload.at || new Date().toISOString();
  const eventId =
    eventPayload.eventId ||
    `approval_${Date.now().toString(36)}_${crypto.randomBytes(3).toString("hex")}`;
  const event = {
    eventId,
    kind: eventPayload.kind || "FOUNDER_APPROVAL",
    propertyId,
    status: eventPayload.status || null,
    note: eventPayload.note || null,
    actor: eventPayload.actor || "ai-admin",
    at,
    simulated: eventPayload.simulated === true,
  };
  ensureDir(eventsDir(propertyId));
  atomicWriteJson(path.join(eventsDir(propertyId), `${eventId}.json`), event);

  const meta = getAuditMeta(propertyId) || { propertyId };
  const approvalEvents = Array.isArray(meta.approvalEvents)
    ? [...meta.approvalEvents]
    : [];
  approvalEvents.push({
    eventId,
    kind: event.kind,
    status: event.status,
    at,
    simulated: event.simulated === true,
  });
  return upsertAuditMeta(propertyId, {
    ...meta,
    approvalEvents,
    forceStatus: true,
  });
}

export function listAuditEvents(propertyId) {
  let dir;
  try {
    dir = eventsDir(propertyId);
  } catch (_err) {
    return [];
  }
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), "utf8")))
    .sort((a, b) => String(a.at || "").localeCompare(String(b.at || "")));
}

function resolveVersionFile(propertyId, versionId) {
  try {
    const durable = path.join(versionsDir(propertyId), `${versionId}.json`);
    if (fs.existsSync(durable)) return durable;
  } catch (_err) {
    /* fall through */
  }
  const seed = path.join(
    AVA_PRODUCTION_STORE_ROOT,
    propertyId,
    "versions",
    `${versionId}.json`
  );
  if (fs.existsSync(seed)) return seed;
  const legacy = path.join(
    resolveLegacyStoreRoot(),
    propertyId,
    "versions",
    `${versionId}.json`
  );
  if (fs.existsSync(legacy)) return legacy;
  return null;
}

export function loadAuditVersion(propertyId, versionId) {
  const file = resolveVersionFile(propertyId, versionId);
  if (!file) return null;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

export function getLatestAuditVersion(propertyId) {
  const meta = getAuditMeta(propertyId);
  if (!meta?.latestVersionId) return null;
  return loadAuditVersion(propertyId, meta.latestVersionId);
}

export function getAvaStorageStatus() {
  return ensureAvaDurableStorage();
}
