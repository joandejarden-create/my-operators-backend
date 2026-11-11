/**
 * Canonical ADP publication / review registry (file-backed durable ledger).
 * Authoritative for local review → approval → publication state transitions.
 * Airtable sync is optional and read-back verified when credentials exist.
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "fs";
import { join } from "path";
import { createHash } from "crypto";

export const ADP_PUBLICATION_REGISTRY_VERSION = "adp_publication_registry_v1";

export const ADP_PUBLICATION_STATES = Object.freeze([
  "RESEARCH_IN_PROGRESS",
  "STAGED",
  "CERTIFICATION_FAILED",
  "READY_FOR_REVIEW",
  "CHANGES_REQUESTED",
  "APPROVED_FOR_PUBLICATION",
  "WAITING_PUBLICATION_AUTHORIZATION",
  "PUBLISHING",
  "PUBLISHED",
  "DELIVERY_VERIFIED",
  "PUBLICATION_FAILED",
  "ROLLED_BACK",
]);

const ALLOWED_TRANSITIONS = Object.freeze({
  RESEARCH_IN_PROGRESS: ["STAGED", "CERTIFICATION_FAILED"],
  STAGED: ["READY_FOR_REVIEW", "CERTIFICATION_FAILED"],
  CERTIFICATION_FAILED: ["STAGED", "RESEARCH_IN_PROGRESS"],
  READY_FOR_REVIEW: ["CHANGES_REQUESTED", "APPROVED_FOR_PUBLICATION", "STAGED"],
  CHANGES_REQUESTED: ["STAGED", "READY_FOR_REVIEW"],
  APPROVED_FOR_PUBLICATION: [
    "WAITING_PUBLICATION_AUTHORIZATION",
    "PUBLISHING",
    "STAGED", // invalidated by new staged version
  ],
  WAITING_PUBLICATION_AUTHORIZATION: ["PUBLISHING", "APPROVED_FOR_PUBLICATION"],
  PUBLISHING: ["PUBLISHED", "PUBLICATION_FAILED"],
  PUBLISHED: ["DELIVERY_VERIFIED", "ROLLED_BACK", "STAGED"],
  DELIVERY_VERIFIED: ["ROLLED_BACK", "STAGED"],
  PUBLICATION_FAILED: ["WAITING_PUBLICATION_AUTHORIZATION", "READY_FOR_REVIEW"],
  ROLLED_BACK: ["STAGED", "READY_FOR_REVIEW"],
});

function registryPath(cwd = process.cwd()) {
  return join(cwd, "data/ai-demand-positioning/publication-registry-v1.json");
}

function emptyRegistry() {
  return {
    registryVersion: ADP_PUBLICATION_REGISTRY_VERSION,
    updatedAt: null,
    hotels: {},
  };
}

export function loadPublicationRegistry(cwd = process.cwd()) {
  const p = registryPath(cwd);
  if (!existsSync(p)) return emptyRegistry();
  try {
    const doc = JSON.parse(readFileSync(p, "utf8"));
    if (!doc.hotels || typeof doc.hotels !== "object") return emptyRegistry();
    return doc;
  } catch {
    return emptyRegistry();
  }
}

export function savePublicationRegistry(doc, cwd = process.cwd()) {
  const p = registryPath(cwd);
  mkdirSync(join(cwd, "data/ai-demand-positioning"), { recursive: true });
  const next = {
    ...doc,
    registryVersion: ADP_PUBLICATION_REGISTRY_VERSION,
    updatedAt: new Date().toISOString(),
  };
  writeFileSync(p, JSON.stringify(next, null, 2) + "\n", "utf8");
  return next;
}

function emptyHotelRecord(propertyId) {
  return {
    propertyId,
    hpcId: null,
    hotelName: null,
    market: null,
    measurementPeriod: null,
    runId: null,
    workflowVersion: null,
    stagedReportVersion: null,
    stagedArtifactHash: null,
    publishedReportVersion: null,
    publishedArtifactHash: null,
    previousPublishedVersion: null,
    certificationResult: null,
    publicationState: "RESEARCH_IN_PROGRESS",
    reviewStatus: null,
    publicationApprovalStatus: null,
    publishedStatus: false,
    approvedBy: null,
    approvedAt: null,
    approvedReportVersion: null,
    publishedBy: null,
    publishedAt: null,
    verificationResult: null,
    rollbackReference: null,
    reviewComments: [],
    history: [],
  };
}

export function getHotelPublicationRecord(propertyId, cwd = process.cwd()) {
  const reg = loadPublicationRegistry(cwd);
  return reg.hotels[String(propertyId || "").trim()] || null;
}

export function upsertHotelPublicationRecord(propertyId, patch = {}, cwd = process.cwd()) {
  const id = String(propertyId || "").trim();
  if (!id.startsWith("adp_")) {
    return { ok: false, error: "invalid_property_id" };
  }
  const reg = loadPublicationRegistry(cwd);
  const prior = reg.hotels[id] || emptyHotelRecord(id);
  const next = {
    ...prior,
    ...patch,
    propertyId: id,
  };
  if (patch.publicationState && patch.publicationState !== prior.publicationState) {
    next.history = [
      ...(prior.history || []),
      {
        at: new Date().toISOString(),
        from: prior.publicationState,
        to: patch.publicationState,
        actor: patch._actor || null,
        note: patch._note || null,
      },
    ].slice(-50);
  }
  delete next._actor;
  delete next._note;
  reg.hotels[id] = next;
  savePublicationRegistry(reg, cwd);
  return { ok: true, record: next };
}

export function canTransitionPublicationState(from, to) {
  const allowed = ALLOWED_TRANSITIONS[from] || [];
  return allowed.includes(to);
}

/**
 * Transition publication state with validation.
 * Material staged version change invalidates prior approval.
 */
export function transitionPublicationState({
  propertyId,
  toState,
  actor = null,
  note = null,
  requireAuth = true,
  cwd = process.cwd(),
  extra = {},
} = {}) {
  if (requireAuth && !actor) {
    return { ok: false, error: "actor_required" };
  }
  if (!ADP_PUBLICATION_STATES.includes(toState)) {
    return { ok: false, error: "invalid_state" };
  }
  const prior = getHotelPublicationRecord(propertyId, cwd) || emptyHotelRecord(propertyId);
  const from = prior.publicationState || "RESEARCH_IN_PROGRESS";
  if (!canTransitionPublicationState(from, toState)) {
    return {
      ok: false,
      error: "illegal_transition",
      from,
      to: toState,
    };
  }
  return upsertHotelPublicationRecord(
    propertyId,
    {
      ...extra,
      publicationState: toState,
      _actor: actor,
      _note: note,
    },
    cwd
  );
}

/**
 * Approve a specific immutable staged report version.
 */
export function approveStagedReportVersion({
  propertyId,
  reportVersion,
  artifactHash,
  actor,
  cwd = process.cwd(),
} = {}) {
  if (!actor || typeof actor !== "object" || !actor.id) {
    return { ok: false, error: "authenticated_actor_required" };
  }
  const prior = getHotelPublicationRecord(propertyId, cwd);
  if (!prior || prior.stagedReportVersion !== reportVersion) {
    return { ok: false, error: "report_version_mismatch" };
  }
  if (artifactHash && prior.stagedArtifactHash && artifactHash !== prior.stagedArtifactHash) {
    return { ok: false, error: "artifact_hash_mismatch" };
  }
  if (!["READY_FOR_REVIEW", "CHANGES_REQUESTED", "STAGED"].includes(prior.publicationState)) {
    // Allow ready path: STAGED may need READY_FOR_REVIEW first
    if (prior.publicationState === "STAGED") {
      transitionPublicationState({
        propertyId,
        toState: "READY_FOR_REVIEW",
        actor: actor.id,
        note: "auto_ready_before_approve",
        requireAuth: false,
        cwd,
      });
    }
  }
  return upsertHotelPublicationRecord(
    propertyId,
    {
      publicationState: "APPROVED_FOR_PUBLICATION",
      publicationApprovalStatus: "APPROVED",
      reviewStatus: "APPROVED",
      approvedBy: {
        id: actor.id,
        email: actor.email || null,
        name: actor.name || null,
      },
      approvedAt: new Date().toISOString(),
      approvedReportVersion: reportVersion,
      _actor: actor.id,
      _note: "approve_for_publication",
    },
    cwd
  );
}

export function requestChangesOnStagedReport({
  propertyId,
  reportVersion,
  comment,
  actor,
  cwd = process.cwd(),
} = {}) {
  if (!actor?.id) return { ok: false, error: "authenticated_actor_required" };
  const prior = getHotelPublicationRecord(propertyId, cwd) || emptyHotelRecord(propertyId);
  if (reportVersion && prior.stagedReportVersion && reportVersion !== prior.stagedReportVersion) {
    return { ok: false, error: "report_version_mismatch" };
  }
  const comments = [
    ...(prior.reviewComments || []),
    {
      at: new Date().toISOString(),
      by: { id: actor.id, email: actor.email || null },
      reportVersion: reportVersion || prior.stagedReportVersion,
      comment: String(comment || "").trim(),
    },
  ].slice(-100);
  return upsertHotelPublicationRecord(
    propertyId,
    {
      publicationState: "CHANGES_REQUESTED",
      reviewStatus: "CHANGES_REQUESTED",
      reviewComments: comments,
      // Invalidate approval for this version
      publicationApprovalStatus: null,
      approvedBy: null,
      approvedAt: null,
      approvedReportVersion: null,
      _actor: actor.id,
      _note: "request_changes",
    },
    cwd
  );
}

/**
 * Mark that a new staged version landed — invalidates prior approval.
 */
export function recordStagedVersion({
  propertyId,
  hotelName = null,
  hpcId = null,
  market = null,
  measurementPeriod = null,
  runId = null,
  workflowVersion = null,
  stagedReportVersion,
  stagedArtifactHash,
  certificationResult = null,
  actor = "system",
  cwd = process.cwd(),
} = {}) {
  const prior = getHotelPublicationRecord(propertyId, cwd);
  const approvalInvalidated =
    prior?.approvedReportVersion &&
    prior.approvedReportVersion !== stagedReportVersion;

  return upsertHotelPublicationRecord(
    propertyId,
    {
      hotelName,
      hpcId,
      market,
      measurementPeriod,
      runId,
      workflowVersion,
      stagedReportVersion,
      stagedArtifactHash,
      certificationResult,
      publicationState: certificationResult?.passed === false ? "CERTIFICATION_FAILED" : "STAGED",
      reviewStatus: "PENDING_REVIEW",
      publicationApprovalStatus: approvalInvalidated ? null : prior?.publicationApprovalStatus || null,
      approvedBy: approvalInvalidated ? null : prior?.approvedBy || null,
      approvedAt: approvalInvalidated ? null : prior?.approvedAt || null,
      approvedReportVersion: approvalInvalidated ? null : prior?.approvedReportVersion || null,
      publishedStatus: prior?.publishedStatus || false,
      publishedReportVersion: prior?.publishedReportVersion || null,
      publishedArtifactHash: prior?.publishedArtifactHash || null,
      previousPublishedVersion: prior?.previousPublishedVersion || null,
      _actor: actor,
      _note: approvalInvalidated ? "new_staged_invalidates_approval" : "staged",
    },
    cwd
  );
}

export function recordPublishedVersion({
  propertyId,
  publishedReportVersion,
  publishedArtifactHash,
  actor,
  verificationResult = null,
  cwd = process.cwd(),
} = {}) {
  if (!actor?.id) return { ok: false, error: "authenticated_actor_required" };
  const prior = getHotelPublicationRecord(propertyId, cwd);
  if (!prior) return { ok: false, error: "no_registry_record" };
  if (prior.publicationState !== "APPROVED_FOR_PUBLICATION" &&
      prior.publicationState !== "WAITING_PUBLICATION_AUTHORIZATION" &&
      prior.publicationState !== "PUBLISHING") {
    return { ok: false, error: "not_approved_for_publication" };
  }
  if (prior.approvedReportVersion !== publishedReportVersion) {
    return { ok: false, error: "approved_version_mismatch" };
  }
  return upsertHotelPublicationRecord(
    propertyId,
    {
      publicationState: verificationResult?.ok === false ? "PUBLICATION_FAILED" : "PUBLISHED",
      publishedStatus: verificationResult?.ok !== false,
      previousPublishedVersion: prior.publishedReportVersion || null,
      publishedReportVersion,
      publishedArtifactHash,
      publishedBy: { id: actor.id, email: actor.email || null, name: actor.name || null },
      publishedAt: new Date().toISOString(),
      verificationResult,
      _actor: actor.id,
      _note: "publish",
    },
    cwd
  );
}

export function hashPayload(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

export function listAllPublicationRecords(cwd = process.cwd()) {
  const reg = loadPublicationRegistry(cwd);
  return Object.values(reg.hotels || {});
}
