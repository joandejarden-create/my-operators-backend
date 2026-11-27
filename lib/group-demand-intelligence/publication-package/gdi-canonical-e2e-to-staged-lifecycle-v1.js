/**
 * Canonical GDI publication lifecycle (permanent):
 *   FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT
 *
 * Successfully qualified results from an authorized Full E2E run progress
 * automatically to STAGED for local founder review. CURRENT changes only after
 * explicit founder approval. HOLD / continuation / discovery are never staged
 * as qualified.
 *
 * Contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1
 */

import { createHash } from "node:crypto";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
} from "./gdi-customer-publication-package-v1.js";
import { createGdiPublicationPackageStore } from "./index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "./gdi-publication-lifecycle-v1.js";

export const GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1 =
  "gdi-canonical-e2e-to-staged-lifecycle-v1";

export const GDI_CANONICAL_PUBLICATION_LIFECYCLE = Object.freeze({
  sequence: ["FULL_E2E", "STAGED", "FOUNDER_APPROVAL", "CURRENT"],
  autoStageAfterAuthorizedE2e: true,
  proposedQualifiedIsInternalOnly: true,
  proposedQualifiedIsNotFounderGate: true,
  currentRequiresExplicitFounderApproval: true,
  stagedNeverAutoPublishesCurrent: true,
  holdAndContinuationNotQualifiedStaged: true,
});

function text(v) {
  return String(v == null ? "" : v).trim();
}

/**
 * Strip founder-preview metadata before immutable package write.
 */
export function sanitizeOpportunityForStagedPackage(row = {}) {
  const id = text(row.id || row.opportunityId);
  if (!id) return null;
  const {
    founderProposedPreview,
    founderReviewBucket,
    founderVisibilityState,
    _founderPreviewSource,
    customerFacing,
    publicationStatus,
    experimentalLabel,
    ...rest
  } = row;
  return {
    ...rest,
    id,
    opportunityId: id,
    hotelMarketScopeEligible:
      rest.hotelMarketScopeEligible !== false &&
      rest.marketScope !== "COMPETITIVE",
    primaryCommercialEligible: rest.primaryCommercialEligible !== false,
    commercialSurfaceDisposition:
      rest.commercialSurfaceDisposition || "COMMERCIAL_OPPORTUNITY",
  };
}

/**
 * Build + optionally activate a STAGED package from qualified opportunity rows.
 * Never writes CURRENT. Never writes Airtable.
 *
 * @param {{
 *   hotelId: string,
 *   hotelName?: string,
 *   opportunities: object[],
 *   activate?: boolean,
 *   dryRun?: boolean,
 *   runId?: string,
 *   notes?: string|object,
 *   preservePriorStagedVersion?: boolean,
 * }} input
 */
export function stageQualifiedInventoryFromE2e(input = {}) {
  const hotelId = text(input.hotelId);
  const opportunitiesIn = Array.isArray(input.opportunities)
    ? input.opportunities
    : [];
  if (!hotelId) {
    return { ok: false, error: "hotelId_required" };
  }
  const sanitized = opportunitiesIn
    .map(sanitizeOpportunityForStagedPackage)
    .filter(Boolean);
  if (!sanitized.length) {
    return {
      ok: false,
      error: "NO_QUALIFIED_OPPORTUNITIES_TO_STAGE",
      contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
    };
  }

  const runId =
    text(input.runId) ||
    `e2e_stage_${Date.now().toString(36)}_${createHash("sha256")
      .update(hotelId)
      .digest("hex")
      .slice(0, 6)}`;
  const publicationVersion =
    text(input.publicationVersion) ||
    `gdi_hotel_wide_fqo_${hotelId}_${new Date()
      .toISOString()
      .slice(0, 10)
      .replace(/-/g, "")}_${createHash("sha256")
      .update(runId)
      .digest("hex")
      .slice(0, 8)}`;

  const pkg = buildCustomerPublicationPackage({
    hotelId,
    hotelName: text(input.hotelName) || hotelId,
    opportunities: sanitized,
    sortOrder: sanitized.map((o) => o.id),
    publicationVersion,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
    approvalStatus: "CANDIDATE_STAGED_FOR_FOUNDER_REVIEW",
    notes: input.notes || {
      at: new Date().toISOString(),
      contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
      runId,
      note: "Automatic STAGED progression after authorized Full E2E qualification — CURRENT unchanged",
    },
  });

  const integrity = verifyPublicationPackageIntegrity(pkg);
  if (!integrity.ok) {
    return {
      ok: false,
      error: "PACKAGE_INTEGRITY_FAILED",
      fails: integrity.fails,
      contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
    };
  }

  const store = createGdiPublicationPackageStore({});
  const priorStaged = store.readPointer(hotelId, "STAGED");
  const priorCurrent = store.readPointer(hotelId, "CURRENT");

  if (input.dryRun === true || input.activate === false) {
    return {
      ok: true,
      dryRun: true,
      activated: false,
      contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
      lifecycle: GDI_CANONICAL_PUBLICATION_LIFECYCLE,
      publicationVersion: pkg.publicationVersion,
      opportunityCount: pkg.opportunityCount,
      opportunityIds: pkg.sortOrder.slice(),
      priorStagedVersion: priorStaged?.publicationVersion || null,
      priorCurrentVersion: priorCurrent?.publicationVersion || null,
      package: pkg,
    };
  }

  const staged = store.stageForProduction(pkg);
  if (!staged.ok) {
    return {
      ok: false,
      error: staged.error || "STAGE_FOR_PRODUCTION_FAILED",
      fails: staged.fails || null,
      contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
    };
  }

  const afterStaged = store.readPointer(hotelId, "STAGED");
  const afterCurrent = store.readPointer(hotelId, "CURRENT");
  const currentUnchanged =
    (priorCurrent?.publicationVersion || null) ===
      (afterCurrent?.publicationVersion || null) &&
    (priorCurrent?.contentFingerprintSha256 || null) ===
      (afterCurrent?.contentFingerprintSha256 || null);

  return {
    ok: true,
    dryRun: false,
    activated: true,
    contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
    lifecycle: GDI_CANONICAL_PUBLICATION_LIFECYCLE,
    publicationVersion: pkg.publicationVersion,
    publicationId: pkg.publicationId,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    opportunityCount: pkg.opportunityCount,
    opportunityIds: pkg.sortOrder.slice(),
    path: staged.path || null,
    priorStagedVersion: priorStaged?.publicationVersion || null,
    stagedVersion: afterStaged?.publicationVersion || null,
    priorCurrentVersion: priorCurrent?.publicationVersion || null,
    currentVersion: afterCurrent?.publicationVersion || null,
    currentUnchanged,
    package: pkg,
  };
}
