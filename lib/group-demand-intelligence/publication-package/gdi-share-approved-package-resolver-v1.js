/**
 * Resolve customer-share inventory from an approved publication package.
 *
 * When a hotel has CUSTOMER_PUBLISHED package:
 *   - Serve frozen opportunity DTOs from the package (no reinterpretation).
 *   - Merge mutable feedback from validation store separately.
 *   - Never silently fall back to Airtable/live inventory for that hotel.
 *
 * When no published package exists:
 *   - Legacy path remains available unless GDI_REQUIRE_APPROVED_PUBLICATION_PACKAGE=1.
 */

import {
  createGdiPublicationPackageStore,
} from "./gdi-publication-package-store-v1.js";
import {
  publicationPackageServeMeta,
  verifyPublicationPackageIntegrity,
} from "./gdi-customer-publication-package-v1.js";
import {
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "./gdi-publication-lifecycle-v1.js";

export const GDI_SHARE_APPROVED_PACKAGE_RESOLVER_V1 =
  "gdi-share-approved-package-resolver-v1";

function text(v) {
  return String(v == null ? "" : v).trim();
}

function truthyEnv(v) {
  const s = String(v == null ? "" : v).trim().toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "on";
}

export function isApprovedPublicationPackageRequired(env = process.env) {
  return truthyEnv(env.GDI_REQUIRE_APPROVED_PUBLICATION_PACKAGE);
}

/**
 * Attempt to resolve share opportunities from the durable approved package.
 *
 * @returns {{
 *   mode: 'PACKAGE'|'LEGACY'|'FAIL_CLOSED',
 *   ok: boolean,
 *   opportunities?: object[],
 *   meta?: object,
 *   error?: string,
 *   detail?: object,
 * }}
 */
export function resolveShareOpportunitiesFromApprovedPackage(input = {}) {
  const hotelId = text(input.hotelId);
  const env = input.env || process.env;
  const store =
    input.store ||
    createGdiPublicationPackageStore({ env, root: input.root });

  if (!hotelId) {
    return { ok: false, mode: "FAIL_CLOSED", error: "HOTEL_ID_REQUIRED" };
  }

  const loaded = store.loadCustomerVisiblePackage(hotelId);
  const requirePackage = isApprovedPublicationPackageRequired(env);

  if (!loaded.ok) {
    if (requirePackage) {
      return {
        ok: false,
        mode: "FAIL_CLOSED",
        error: loaded.error || "APPROVED_PACKAGE_UNAVAILABLE",
        detail: { pointer: loaded.pointer || null, fails: loaded.fails || null },
      };
    }
    // Soft: hotel has a CURRENT pointer that is broken → still fail closed for that hotel.
    if (
      loaded.pointer &&
      loaded.error &&
      loaded.error !== "NO_CURRENT_PUBLICATION" &&
      loaded.error !== "PUBLICATION_WITHHELD"
    ) {
      return {
        ok: false,
        mode: "FAIL_CLOSED",
        error: loaded.error,
        detail: {
          pointer: loaded.pointer,
          fails: loaded.fails || null,
          note: "Broken CURRENT publication pointer must not fall back to Airtable",
        },
      };
    }
    return {
      ok: true,
      mode: "LEGACY",
      opportunities: null,
      meta: {
        publicationPackageMode: "LEGACY",
        reason: loaded.error || "NO_CURRENT_PUBLICATION",
      },
    };
  }

  const integrity = verifyPublicationPackageIntegrity(loaded.package);
  if (!integrity.ok) {
    return {
      ok: false,
      mode: "FAIL_CLOSED",
      error: "PACKAGE_INTEGRITY_FAILED",
      detail: { fails: integrity.fails },
    };
  }

  const opportunities = (loaded.package.opportunities || []).map((o) => ({
    ...o,
    // Preserve package provenance for serve meta / debugging (stripped for customers by sanitize if needed).
    _publicationPackageId: loaded.package.publicationId,
    _publicationVersion: loaded.package.publicationVersion,
    _contentFingerprintSha256: loaded.package.contentFingerprintSha256,
  }));

  return {
    ok: true,
    mode: "PACKAGE",
    opportunities,
    package: loaded.package,
    meta: {
      ...publicationPackageServeMeta(loaded.package),
      publicationPackageMode: "PACKAGE",
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED,
      inventoryPopulationCounts: loaded.package.inventoryCounts || null,
    },
  };
}

/**
 * Founder / authenticated preview: same immutable package contract as share.
 * Prefer CUSTOMER_PUBLISHED CURRENT; else STAGED founder-review candidate.
 * Never silently recompute commercial/need-date content when a package exists.
 *
 * @returns {{ mode: 'PACKAGE'|'LEGACY', ok: boolean, opportunities?: object[], meta?: object, error?: string }}
 */
export function resolvePreviewOpportunitiesFromPublicationPackage(input = {}) {
  const hotelId = text(input.hotelId);
  const env = input.env || process.env;
  const store =
    input.store ||
    createGdiPublicationPackageStore({ env, root: input.root });
  if (!hotelId) {
    return { ok: false, mode: "LEGACY", error: "HOTEL_ID_REQUIRED" };
  }

  // Allow explicit live research browse when debugging (never for share).
  if (truthyEnv(input.forceLive) || truthyEnv(env.GDI_FOUNDER_PREVIEW_FORCE_LIVE)) {
    return {
      ok: true,
      mode: "LEGACY",
      opportunities: null,
      meta: { publicationPackageMode: "LEGACY", reason: "FORCE_LIVE" },
    };
  }

  const current = store.loadCustomerVisiblePackage(hotelId);
  if (current.ok) {
    const integrity = verifyPublicationPackageIntegrity(current.package);
    if (!integrity.ok) {
      return {
        ok: false,
        mode: "FAIL_CLOSED",
        error: "PACKAGE_INTEGRITY_FAILED",
        detail: { fails: integrity.fails, pointer: "CURRENT" },
      };
    }
    return {
      ok: true,
      mode: "PACKAGE",
      opportunities: (current.package.opportunities || []).map((o) => ({
        ...o,
        _publicationPackageId: current.package.publicationId,
        _publicationVersion: current.package.publicationVersion,
        _contentFingerprintSha256: current.package.contentFingerprintSha256,
      })),
      package: current.package,
      meta: {
        ...publicationPackageServeMeta(current.package),
        publicationPackageMode: "PACKAGE",
        previewSource: "CURRENT",
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED,
        inventoryPopulationCounts: current.package.inventoryCounts || null,
      },
    };
  }

  const stagedPtr = store.readPointer(hotelId, "STAGED");
  if (stagedPtr?.publicationVersion) {
    const staged = store.loadPackage(hotelId, stagedPtr.publicationVersion);
    if (staged.ok) {
      const integrity = verifyPublicationPackageIntegrity(staged.package);
      if (integrity.ok) {
        return {
          ok: true,
          mode: "PACKAGE",
          opportunities: (staged.package.opportunities || []).map((o) => ({
            ...o,
            _publicationPackageId: staged.package.publicationId,
            _publicationVersion: staged.package.publicationVersion,
            _contentFingerprintSha256: staged.package.contentFingerprintSha256,
          })),
          package: staged.package,
          meta: {
            ...publicationPackageServeMeta(staged.package),
            publicationPackageMode: "PACKAGE",
            previewSource: "STAGED",
            lifecycleState:
              staged.package.lifecycleState ||
              GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
            inventoryPopulationCounts: staged.package.inventoryCounts || null,
          },
        };
      }
    }
  }

  return {
    ok: true,
    mode: "LEGACY",
    opportunities: null,
    meta: {
      publicationPackageMode: "LEGACY",
      reason: current.error || "NO_PUBLICATION_PACKAGE",
    },
  };
}

/**
 * Merge mutable hotel validation / feedback onto immutable package opportunities.
 * Feedback never mutates the package document.
 */
export function mergeShareFeedbackOntoPackageOpportunities(
  opportunities = [],
  validationDoc = {},
) {
  const byVal = new Map(
    (validationDoc.items || []).map((v) => [String(v.opportunityId), v]),
  );
  return opportunities.map((o) => {
    const v = byVal.get(String(o.id));
    if (!v) {
      return {
        ...o,
        shareValidation: null,
      };
    }
    return {
      ...o,
      shareValidation: {
        familiarityStatus: v.familiarityStatus,
        commercialValue: v.commercialValue,
        contactPersonAssessment: v.contactPersonAssessment,
        emailAssessment: v.emailAssessment,
        phoneAssessment: v.phoneAssessment,
        note: v.note || "",
        validatedAt: v.validatedAt,
        hotelValidationUpdatedAt:
          v.hotelValidationUpdatedAt || v.validatedAt || null,
      },
    };
  });
}
