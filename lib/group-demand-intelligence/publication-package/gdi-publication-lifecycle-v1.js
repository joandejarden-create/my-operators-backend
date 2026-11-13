/**
 * GDI Customer Publication lifecycle gates V1.
 *
 * RESEARCH_READY → CUSTOMER_RENDERED → SEMANTIC_VALIDATED → PARITY_VALIDATED
 * → FOUNDER_REVIEWED → FOUNDER_APPROVED → PRODUCTION_STAGED → PRODUCTION_VERIFIED
 * → CUSTOMER_PUBLISHED
 *
 * Publishing requires all mandatory gates. Content changes mint a new version.
 */

export const GDI_PUBLICATION_LIFECYCLE_V1 = "gdi-publication-lifecycle-v1";

export const GDI_PUBLICATION_LIFECYCLE_STATE = Object.freeze({
  RESEARCH_READY: "RESEARCH_READY",
  CUSTOMER_RENDERED: "CUSTOMER_RENDERED",
  SEMANTIC_VALIDATED: "SEMANTIC_VALIDATED",
  PARITY_VALIDATED: "PARITY_VALIDATED",
  FOUNDER_REVIEWED: "FOUNDER_REVIEWED",
  FOUNDER_APPROVED: "FOUNDER_APPROVED",
  PRODUCTION_STAGED: "PRODUCTION_STAGED",
  PRODUCTION_VERIFIED: "PRODUCTION_VERIFIED",
  CUSTOMER_PUBLISHED: "CUSTOMER_PUBLISHED",
  PUBLICATION_FAILED: "PUBLICATION_FAILED",
  WITHHELD: "WITHHELD",
  SUPERSEDED: "SUPERSEDED",
});

export const GDI_PUBLICATION_LIFECYCLE_ORDER = Object.freeze([
  GDI_PUBLICATION_LIFECYCLE_STATE.RESEARCH_READY,
  GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
  GDI_PUBLICATION_LIFECYCLE_STATE.SEMANTIC_VALIDATED,
  GDI_PUBLICATION_LIFECYCLE_STATE.PARITY_VALIDATED,
  GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_APPROVED,
  GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
  GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_VERIFIED,
  GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED,
]);

/** Gates that must pass before CUSTOMER_PUBLISHED. */
export const MANDATORY_PUBLICATION_GATES = Object.freeze([
  "inventory_parity",
  "content_parity",
  "presentation_parity",
  "quality",
  "fingerprint_integrity",
  "founder_approval_fingerprint_match",
  "production_verification",
]);

function text(v) {
  return String(v == null ? "" : v).trim();
}

export function lifecycleIndex(state) {
  return GDI_PUBLICATION_LIFECYCLE_ORDER.indexOf(String(state || ""));
}

export function canAdvanceLifecycle(fromState, toState) {
  const from = lifecycleIndex(fromState);
  const to = lifecycleIndex(toState);
  if (to < 0) return false;
  if (
    toState === GDI_PUBLICATION_LIFECYCLE_STATE.PUBLICATION_FAILED ||
    toState === GDI_PUBLICATION_LIFECYCLE_STATE.WITHHELD ||
    toState === GDI_PUBLICATION_LIFECYCLE_STATE.SUPERSEDED
  ) {
    return true;
  }
  if (from < 0) return to === 0;
  return to === from + 1 || to === from;
}

/**
 * Founder approval must bind exact publication version + content fingerprint.
 */
export function validateFounderApprovalReceipt(receipt = {}, packageMeta = {}) {
  const fails = [];
  if (receipt?.approved !== true) fails.push("approved:not_true");
  const pubId = text(receipt.publicationId || receipt.publication_id);
  const pubVer = text(receipt.publicationVersion || receipt.publication_version);
  const fp = text(
    receipt.contentFingerprintSha256 ||
      receipt.content_fingerprint_sha256 ||
      receipt.fingerprint,
  );
  if (!pubId) fails.push("publicationId:missing");
  if (!pubVer) fails.push("publicationVersion:missing");
  if (!/^[a-f0-9]{64}$/i.test(fp)) fails.push("contentFingerprintSha256:invalid");
  if (packageMeta.publicationId && pubId && pubId !== packageMeta.publicationId) {
    fails.push("publicationId:mismatch");
  }
  if (
    packageMeta.publicationVersion &&
    pubVer &&
    pubVer !== packageMeta.publicationVersion
  ) {
    fails.push("publicationVersion:mismatch");
  }
  if (
    packageMeta.contentFingerprintSha256 &&
    fp &&
    fp.toLowerCase() !== String(packageMeta.contentFingerprintSha256).toLowerCase()
  ) {
    fails.push("contentFingerprintSha256:mismatch");
  }
  return {
    ok: fails.length === 0,
    fails,
    publicationId: pubId || null,
    publicationVersion: pubVer || null,
    contentFingerprintSha256: fp ? fp.toLowerCase() : null,
  };
}

/**
 * Block CUSTOMER_PUBLISHED unless every mandatory gate is recorded as pass.
 */
export function assertPublicationGatesPassed(gateResults = {}) {
  const missing = [];
  const failed = [];
  for (const gate of MANDATORY_PUBLICATION_GATES) {
    const row = gateResults[gate];
    if (!row || row.ok !== true) {
      if (!row) missing.push(gate);
      else failed.push(gate);
    }
  }
  return {
    ok: missing.length === 0 && failed.length === 0,
    missing,
    failed,
    mandatoryGates: MANDATORY_PUBLICATION_GATES,
  };
}

export function isCustomerVisibleLifecycleState(state) {
  return state === GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED;
}
