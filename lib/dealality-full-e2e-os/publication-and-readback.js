/**
 * Publication lifecycle + Airtable read-back contract.
 * Research may stage without publication auth; customer visibility requires explicit approval.
 */

import { PUBLICATION_LIFECYCLE } from "./execution-states.js";

export const PUBLICATION_CONTRACT_VERSION = "dealality-publication-readback-v1";

export const PUBLICATION_STATE_ORDER = Object.freeze([
  PUBLICATION_LIFECYCLE.RESEARCH_STAGED,
  PUBLICATION_LIFECYCLE.QUALITY_APPROVED,
  PUBLICATION_LIFECYCLE.PUBLICATION_APPROVED,
  PUBLICATION_LIFECYCLE.PUBLISHED,
  PUBLICATION_LIFECYCLE.DELIVERY_VERIFIED,
]);

/**
 * Build a dry-run publication plan (no writes).
 */
export function buildPublicationDryRunPlan({
  product,
  hotelId,
  recordsToCreate = [],
  recordsToUpdate = [],
  recordsUnchanged = [],
  recordsRequiringReview = [],
  customerVisibilityChanges = [],
  expectedReadback = {},
  rollbackStrategy = null,
} = {}) {
  return Object.freeze({
    contractVersion: PUBLICATION_CONTRACT_VERSION,
    product,
    hotelId,
    mode: "dry_run",
    recordsToCreate: Object.freeze([...recordsToCreate]),
    recordsToUpdate: Object.freeze([...recordsToUpdate]),
    recordsUnchanged: Object.freeze([...recordsUnchanged]),
    recordsRequiringReview: Object.freeze([...recordsRequiringReview]),
    customerVisibilityChanges: Object.freeze([...customerVisibilityChanges]),
    expectedReadback: Object.freeze({ ...expectedReadback }),
    rollbackStrategy:
      rollbackStrategy ||
      "Restore previous published pointer / Airtable snapshot; do not delete customer-visible IDs",
    invariants: Object.freeze([
      "Never auto-publish solely because Ready",
      "Do not demote customer-visible opportunities without approved change",
      "Preserve customer feedback and stable IDs",
      "Prefer versioned replacement over in-place overwrite for ADP reports",
      "API success ≠ persistence proof — read-back required",
    ]),
  });
}

/**
 * Compare intended write set vs Airtable/FS read-back.
 * A successful API response alone is insufficient.
 */
export function reconcileAirtableReadback({
  intended = {},
  readback = {},
} = {}) {
  const mismatches = [];
  const checks = [
    ["recordIds", "ids"],
    ["recordCount", "count"],
    ["hashes", "hashes"],
    ["hotelId", "hotelId"],
    ["publicationState", "publicationState"],
    ["foreignKeys", "foreignKeys"],
  ];

  for (const [intendKey, readKey] of checks) {
    const a = intended[intendKey] ?? intended[readKey];
    const b = readback[readKey] ?? readback[intendKey];
    if (a === undefined || b === undefined) continue;
    if (JSON.stringify(a) !== JSON.stringify(b)) {
      mismatches.push({
        field: intendKey,
        intended: a,
        readback: b,
      });
    }
  }

  if (
    intended.hotelId &&
    readback.hotelId &&
    String(intended.hotelId) !== String(readback.hotelId)
  ) {
    mismatches.push({
      field: "hotel_identity",
      intended: intended.hotelId,
      readback: readback.hotelId,
    });
  }

  return {
    ok: mismatches.length === 0,
    contractVersion: PUBLICATION_CONTRACT_VERSION,
    mismatches: Object.freeze(mismatches),
    verifiedAt: new Date().toISOString(),
  };
}

/**
 * Map Ready / eligibility into publication lifecycle — they are not the same.
 */
export function classifyOpportunityPublicationState({
  researchPersisted = false,
  qualityApproved = false,
  publicationAuthorized = false,
  customerVisible = false,
  deliveryVerified = false,
  readyOk = false,
} = {}) {
  // Ready alone never implies published.
  if (deliveryVerified && customerVisible) {
    return PUBLICATION_LIFECYCLE.DELIVERY_VERIFIED;
  }
  if (customerVisible && publicationAuthorized) {
    return PUBLICATION_LIFECYCLE.PUBLISHED;
  }
  if (publicationAuthorized && qualityApproved) {
    return PUBLICATION_LIFECYCLE.PUBLICATION_APPROVED;
  }
  if (qualityApproved || readyOk) {
    return qualityApproved
      ? PUBLICATION_LIFECYCLE.QUALITY_APPROVED
      : PUBLICATION_LIFECYCLE.RESEARCH_STAGED;
  }
  if (researchPersisted) return PUBLICATION_LIFECYCLE.RESEARCH_STAGED;
  return null;
}

export function assertPublicationAuth({
  authorizePublish = false,
  dryRun = true,
  targetLifecycle = PUBLICATION_LIFECYCLE.PUBLISHED,
} = {}) {
  if (
    targetLifecycle === PUBLICATION_LIFECYCLE.PUBLISHED ||
    targetLifecycle === PUBLICATION_LIFECYCLE.DELIVERY_VERIFIED
  ) {
    if (dryRun) {
      return {
        ok: false,
        code: "DRY_RUN_BLOCKS_PUBLISH",
        message: "Dry-run blocks customer publication writes",
      };
    }
    if (!authorizePublish) {
      return {
        ok: false,
        code: "PUBLICATION_AUTH_REQUIRED",
        message: "Explicit authorizePublish required",
      };
    }
  }
  return { ok: true, code: "OK" };
}
