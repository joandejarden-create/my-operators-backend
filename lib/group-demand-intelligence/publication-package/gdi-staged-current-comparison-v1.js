/**
 * Compare STAGED vs CURRENT customer-visible publication content.
 * Uses canonical contentFingerprintSha256 — not version IDs or counts alone.
 * Read-only. No registry invention / no writes.
 */

import { createGdiPublicationPackageStore } from "./gdi-publication-package-store-v1.js";
import { verifyPublicationPackageIntegrity } from "./gdi-customer-publication-package-v1.js";

export const GDI_STAGED_CURRENT_COMPARISON_V1 =
  "gdi-staged-current-comparison-v1";

export const PUBLICATION_COMPARISON_STATUS = Object.freeze({
  SYNCED: "SYNCED",
  DIFFERENT: "DIFFERENT",
  UNAVAILABLE: "UNAVAILABLE",
  INTEGRITY_ERROR: "INTEGRITY_ERROR",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isSha256(fp) {
  return /^[a-f0-9]{64}$/i.test(text(fp));
}

function packageSummary(ptr, pkgLoad) {
  const version =
    text(ptr?.publicationVersion) ||
    text(pkgLoad?.package?.publicationVersion) ||
    null;
  const fingerprint =
    text(ptr?.contentFingerprintSha256) ||
    text(pkgLoad?.package?.contentFingerprintSha256) ||
    null;
  const opportunityCount = Array.isArray(pkgLoad?.package?.opportunities)
    ? pkgLoad.package.opportunities.length
    : pkgLoad?.package?.opportunityCount ?? null;
  return {
    available: Boolean(version),
    publicationVersion: version,
    contentFingerprintSha256: fingerprint,
    opportunityCount,
  };
}

function verifyPackageIntegrity(pkgLoad) {
  if (!pkgLoad?.ok || !pkgLoad.package) {
    return { ok: false, fails: [pkgLoad?.error || "PACKAGE_UNAVAILABLE"] };
  }
  try {
    const v = verifyPublicationPackageIntegrity(pkgLoad.package);
    if (!v?.ok) {
      return { ok: false, fails: v?.fails || ["VALIDATION_FAILED"] };
    }
  } catch (err) {
    return { ok: false, fails: [String(err?.message || err)] };
  }
  const fp = text(pkgLoad.package.contentFingerprintSha256);
  if (fp && !isSha256(fp)) {
    return { ok: false, fails: ["contentFingerprintSha256:invalid"] };
  }
  return { ok: true, fails: [] };
}

/**
 * Compare STAGED and CURRENT for one hotel.
 */
export function compareStagedAndCurrentPublications(input = {}) {
  const hotelId = text(input.hotelId);
  const store =
    input.store ||
    createGdiPublicationPackageStore({
      env: input.env || process.env,
      root: input.root,
    });

  if (!hotelId) {
    return {
      version: GDI_STAGED_CURRENT_COMPARISON_V1,
      hotelId: "",
      status: PUBLICATION_COMPARISON_STATUS.UNAVAILABLE,
      color: "gray",
      label: "Unavailable",
      detail: "Hotel identity required.",
      staged: packageSummary(null, null),
      current: packageSummary(null, null),
      unpublishedStagedChanges: null,
      fingerprintsMatch: null,
    };
  }

  const stagedPtr = store.readPointer(hotelId, "STAGED");
  const currentPtr = store.readPointer(hotelId, "CURRENT");
  const stagedLoad = stagedPtr?.publicationVersion
    ? store.loadPackage(hotelId, stagedPtr.publicationVersion)
    : { ok: false, error: "NO_STAGED_PUBLICATION" };
  const currentLoad = store.loadCustomerVisiblePackage(hotelId);

  const staged = packageSummary(stagedPtr, stagedLoad?.ok ? stagedLoad : null);
  const current = packageSummary(
    currentPtr,
    currentLoad?.ok ? currentLoad : null,
  );

  const stagedExists = staged.available;
  const currentExists = current.available;

  if (!stagedExists || !currentExists) {
    const missing = [];
    if (!stagedExists) missing.push("STAGED");
    if (!currentExists) missing.push("CURRENT");
    return {
      version: GDI_STAGED_CURRENT_COMPARISON_V1,
      hotelId,
      status: PUBLICATION_COMPARISON_STATUS.UNAVAILABLE,
      color: "gray",
      label: "Unavailable",
      detail:
        missing.length === 2
          ? "STAGED and CURRENT packages are not available for this hotel."
          : `${missing[0]} package is not available for this hotel.`,
      missingPackages: missing,
      staged,
      current,
      unpublishedStagedChanges: null,
      fingerprintsMatch: null,
    };
  }

  const stagedIntegrity = verifyPackageIntegrity(
    stagedLoad?.ok ? stagedLoad : { ok: false, error: "STAGED_LOAD_FAILED" },
  );
  const currentIntegrity = verifyPackageIntegrity(
    currentLoad?.ok
      ? currentLoad
      : { ok: false, error: "CURRENT_LOAD_FAILED" },
  );

  if (!stagedIntegrity.ok || !currentIntegrity.ok) {
    return {
      version: GDI_STAGED_CURRENT_COMPARISON_V1,
      hotelId,
      status: PUBLICATION_COMPARISON_STATUS.INTEGRITY_ERROR,
      color: "red",
      label: "Integrity error",
      detail: "A publication package or required fingerprint failed verification.",
      staged,
      current,
      stagedIntegrityFails: stagedIntegrity.fails,
      currentIntegrityFails: currentIntegrity.fails,
      unpublishedStagedChanges: null,
      fingerprintsMatch: null,
    };
  }

  const stagedFp = text(staged.contentFingerprintSha256).toLowerCase();
  const currentFp = text(current.contentFingerprintSha256).toLowerCase();

  if (!isSha256(stagedFp) || !isSha256(currentFp)) {
    return {
      version: GDI_STAGED_CURRENT_COMPARISON_V1,
      hotelId,
      status: PUBLICATION_COMPARISON_STATUS.INTEGRITY_ERROR,
      color: "red",
      label: "Integrity error",
      detail: "Comparable content fingerprint is missing or invalid.",
      staged,
      current,
      unpublishedStagedChanges: null,
      fingerprintsMatch: null,
    };
  }

  if (stagedFp === currentFp) {
    return {
      version: GDI_STAGED_CURRENT_COMPARISON_V1,
      hotelId,
      status: PUBLICATION_COMPARISON_STATUS.SYNCED,
      color: "green",
      label: "Synced",
      detail:
        "STAGED and CURRENT share the same customer-visible content fingerprint.",
      staged,
      current,
      unpublishedStagedChanges: false,
      fingerprintsMatch: true,
    };
  }

  return {
    version: GDI_STAGED_CURRENT_COMPARISON_V1,
    hotelId,
    status: PUBLICATION_COMPARISON_STATUS.DIFFERENT,
    color: "amber",
    label: "Different",
    detail:
      "STAGED customer-visible content differs from CURRENT. Staged changes are not reflected in the published inventory.",
    staged,
    current,
    unpublishedStagedChanges: true,
    fingerprintsMatch: false,
    note: "Version IDs alone are not used as proof of difference; fingerprints are.",
  };
}

export default {
  GDI_STAGED_CURRENT_COMPARISON_V1,
  PUBLICATION_COMPARISON_STATUS,
  compareStagedAndCurrentPublications,
};
