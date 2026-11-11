/**
 * ADP_ADMIN_PUBLICATION_PARITY_AUTHORITY_V1
 *
 * Single-source projection for Admin / customer / PDF / share surfaces.
 * Durable published-current registry is authoritative for period, Consideration,
 * methodology, and publication identity. Operational statuses (PDF Ready,
 * Customer Access) are evidence-backed and fail closed.
 *
 * Does not mint shares, remesure, or mutate published packages.
 */

export const ADP_ADMIN_PUBLICATION_PARITY_AUTHORITY_V1 =
  "ADP_ADMIN_PUBLICATION_PARITY_AUTHORITY_V1";

export const ADP_PUBLICATION_SURFACE_PARITY_CONTRACT_V1 =
  "ADP_PUBLICATION_SURFACE_PARITY_CONTRACT_V1";

/**
 * Canonical field set that must match across:
 * approved package ↔ durable current ↔ Admin ↔ customer report ↔ share report
 */
export const ADP_CANONICAL_PUBLICATION_FIELDS_V1 = Object.freeze([
  "propertyId",
  "measurementPeriodId",
  "considerationRate",
  "methodologyFingerprintSha256",
  "pdfArtifactSha256",
  "publicationManifestSha256",
]);

export const ADP_FUTURE_PUBLICATION_WORKFLOW_V1 = Object.freeze({
  steps: Object.freeze([
    "CERTIFIED_MEASUREMENT",
    "APPROVED_IMMUTABLE_PACKAGE",
    "PUBLISHED_DURABLE_CURRENT",
    "PERSIST_PUBLICATION_TIMESTAMP",
    "PDF_ARTIFACT_REGISTRATION",
    "AUTHORIZED_SHARE_RESOLUTION",
    "SURFACE_PARITY_VERIFICATION",
    "ADMIN_STATUS_REFRESH",
  ]),
  /**
   * Admin field authority for future publications:
   *   Published  ← durable latestPublishedAt (publication event ISO)
   *   Measured   ← certified measurementPeriodId month/year
   *   Consideration ← certified published report
   *   Customer Access Verified ← live-verify ledger only
   *   PDF Ready ← retrievable + SHA-matched published artifact
   */
  adminTimestampFields: Object.freeze({
    publishedAt: "durable_registry.latestPublishedAt",
    measuredFrom: "durable_registry.periodId / measurementPeriodId",
    customerAccessVerifiedAt: "external_share_live_verify_ledger.verifiedAt",
  }),
  forbidden: Object.freeze([
    "manual_admin_consideration_recalc",
    "ephemeral_published_as_authority",
    "static_deploy_pack_substitution",
    "silent_stale_fallback",
    "token_rotation_without_authorization",
    "new_share_mint_without_authorization",
    "infer_published_month_from_measurement_period_id",
    "infer_published_month_from_deploy_or_mtime",
  ]),
});

/**
 * Compare two canonical publication identities (fail closed on mismatch).
 * @param {object} left
 * @param {object} right
 * @param {{ considerationTolerance?: number }} [opts]
 */
export function compareCanonicalPublicationIdentity(left, right, opts = {}) {
  const tol = Number(opts.considerationTolerance ?? 0.05);
  const mismatches = [];
  const l = left || {};
  const r = right || {};

  if (String(l.propertyId || "") !== String(r.propertyId || "")) {
    mismatches.push({
      field: "propertyId",
      left: l.propertyId || null,
      right: r.propertyId || null,
    });
  }
  if (String(l.measurementPeriodId || "") !== String(r.measurementPeriodId || "")) {
    mismatches.push({
      field: "measurementPeriodId",
      left: l.measurementPeriodId || null,
      right: r.measurementPeriodId || null,
    });
  }
  const lc = l.considerationRate == null ? null : Number(l.considerationRate);
  const rc = r.considerationRate == null ? null : Number(r.considerationRate);
  if (lc == null || rc == null || !Number.isFinite(lc) || !Number.isFinite(rc)) {
    if (lc !== rc) {
      mismatches.push({
        field: "considerationRate",
        left: lc,
        right: rc,
      });
    }
  } else if (Math.abs(lc - rc) > tol) {
    mismatches.push({
      field: "considerationRate",
      left: lc,
      right: rc,
    });
  }
  const lFp = String(l.methodologyFingerprintSha256 || "").toLowerCase() || null;
  const rFp = String(r.methodologyFingerprintSha256 || "").toLowerCase() || null;
  if (lFp && rFp && lFp !== rFp) {
    mismatches.push({
      field: "methodologyFingerprintSha256",
      left: lFp,
      right: rFp,
    });
  }
  const lPdf = String(l.pdfArtifactSha256 || "").toLowerCase() || null;
  const rPdf = String(r.pdfArtifactSha256 || "").toLowerCase() || null;
  if (lPdf && rPdf && lPdf !== rPdf) {
    mismatches.push({
      field: "pdfArtifactSha256",
      left: lPdf,
      right: rPdf,
    });
  }
  const lMan = String(l.publicationManifestSha256 || "").toLowerCase() || null;
  const rMan = String(r.publicationManifestSha256 || "").toLowerCase() || null;
  if (lMan && rMan && lMan !== rMan) {
    mismatches.push({
      field: "publicationManifestSha256",
      left: lMan,
      right: rMan,
    });
  }

  return {
    ok: mismatches.length === 0,
    mismatches,
    contract: ADP_PUBLICATION_SURFACE_PARITY_CONTRACT_V1,
  };
}

/**
 * Build canonical identity from an approved package JSON.
 */
export function canonicalIdentityFromApprovedPackage(pkg) {
  return {
    propertyId: pkg?.propertyId || null,
    measurementPeriodId: pkg?.measurementPeriodId || null,
    considerationRate: pkg?.certifiedMeasurement?.considerationRate ?? null,
    methodologyFingerprintSha256:
      pkg?.methodologyFingerprintSha256 ||
      pkg?.certifiedMeasurement?.methodologyFingerprintSha256 ||
      null,
    pdfArtifactSha256: pkg?.pdfArtifactSha256 || null,
    publicationManifestSha256: pkg?.publicationManifestSha256 || null,
  };
}

/**
 * Build canonical identity from Admin / durable authoritative column result.
 */
export function canonicalIdentityFromAuthoritativePublished(auth) {
  return {
    propertyId: auth?.propertyId || null,
    measurementPeriodId: auth?.publishedPeriodId || null,
    considerationRate: auth?.publishedConsideration ?? null,
    methodologyFingerprintSha256: auth?.methodologyFingerprintSha256 || null,
    pdfArtifactSha256: auth?.pdfArtifactSha256 || null,
    publicationManifestSha256: auth?.publicationManifestSha256 || null,
  };
}
