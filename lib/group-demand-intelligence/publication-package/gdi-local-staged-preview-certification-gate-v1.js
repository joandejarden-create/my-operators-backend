/**
 * GDI local staged-preview certification gate V1.
 *
 * After a Full E2E campaign stages a new package, local founder preview must
 * resolve that exact STAGED package (fingerprint + opportunity IDs + counts)
 * before the campaign may be marked locally browser-verified.
 *
 * Observability / integration gate only — no publication, Airtable, or Exa.
 */

import fs from "node:fs";
import path from "node:path";
import { createGdiPublicationPackageStore } from "./gdi-publication-package-store-v1.js";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "./gdi-share-approved-package-resolver-v1.js";

export const GDI_LOCAL_STAGED_PREVIEW_CERTIFICATION_GATE_V1 =
  "gdi-local-staged-preview-certification-gate-v1";

function text(v) {
  return String(v == null ? "" : v).trim();
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function opportunityIds(opportunities) {
  return (opportunities || [])
    .map((o) => text(o?.id || o?.opportunityId))
    .filter(Boolean)
    .sort();
}

function inventoryCounts(opportunities) {
  const ops = opportunities || [];
  return {
    all: ops.length,
    hotel: ops.filter((o) => o?.hotelMarketScopeEligible === true).length,
    competitive: ops.filter((o) => o?.competitiveMarketScopeEligible === true)
      .length,
  };
}

/**
 * Certify that local STAGED preview resolution matches the hotel's STAGED pointer.
 *
 * @param {{
 *   hotelId: string,
 *   root?: string,
 *   expectedFingerprintSha256?: string,
 *   expectedOpportunityIds?: string[],
 *   requireDrawerBodies?: boolean,
 * }} input
 */
export function certifyLocalStagedPreviewResolution(input = {}) {
  const hotelId = text(input.hotelId);
  const fails = [];
  const push = (code, detail = {}) => fails.push({ code, ...detail });

  if (!hotelId) {
    return {
      ok: false,
      gate: GDI_LOCAL_STAGED_PREVIEW_CERTIFICATION_GATE_V1,
      fails: [{ code: "HOTEL_ID_REQUIRED" }],
    };
  }

  const store = createGdiPublicationPackageStore({
    root: input.root,
  });
  const stagedPath = path.join(store.root, hotelId, "STAGED.json");
  const stagedPointer = store.readPointer(hotelId, "STAGED");
  if (!stagedPointer) {
    push("STAGED_POINTER_MISSING", { hotelId, stagedPath });
    return {
      ok: false,
      gate: GDI_LOCAL_STAGED_PREVIEW_CERTIFICATION_GATE_V1,
      hotelId,
      fails,
      browserVerifiedEligible: false,
    };
  }

  const pointerFp = text(stagedPointer.contentFingerprintSha256);
  const pointerVersion = text(stagedPointer.publicationVersion);
  if (input.expectedFingerprintSha256) {
    const expected = text(input.expectedFingerprintSha256);
    if (pointerFp !== expected) {
      push("STAGED_POINTER_FINGERPRINT_MISMATCH", {
        expected,
        actual: pointerFp,
      });
    }
  }

  const resolved = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId,
    preferStaged: true,
    root: input.root,
    store,
  });

  if (!resolved.ok || resolved.mode !== "PACKAGE") {
    push("STAGED_PACKAGE_RESOLVE_FAILED", {
      mode: resolved.mode || null,
      error: resolved.error || null,
    });
  }

  const resolvedFp = text(resolved.meta?.contentFingerprintSha256);
  const previewSource = text(resolved.meta?.previewSource).toUpperCase();
  if (previewSource !== "STAGED") {
    push("PREVIEW_SOURCE_NOT_STAGED", { previewSource });
  }
  if (pointerFp && resolvedFp && pointerFp !== resolvedFp) {
    push("RESOLVED_FINGERPRINT_MISMATCH_POINTER", {
      pointerFp,
      resolvedFp,
    });
  }

  const ops = resolved.opportunities || [];
  const counts = inventoryCounts(ops);
  const ids = opportunityIds(ops);
  const unique = new Set(ids);
  if (unique.size !== ids.length) {
    push("DUPLICATE_OPPORTUNITY_IDS", {
      total: ids.length,
      unique: unique.size,
    });
  }

  if (input.expectedOpportunityIds) {
    const expectedIds = opportunityIds(
      (input.expectedOpportunityIds || []).map((id) => ({ id })),
    );
    if (expectedIds.join("\n") !== ids.join("\n")) {
      push("OPPORTUNITY_ID_SET_MISMATCH", {
        expectedCount: expectedIds.length,
        actualCount: ids.length,
      });
    }
  }

  // Load immutable package bytes and confirm ID set + fingerprint identity.
  if (pointerVersion) {
    const pkgFile = path.join(
      path.dirname(stagedPath),
      pointerVersion,
      "package.json",
    );
    if (!fs.existsSync(pkgFile)) {
      push("STAGED_PACKAGE_BYTES_MISSING", { publicationVersion: pointerVersion });
    } else {
      const pkg = readJson(pkgFile);
      const pkgFp = text(pkg.contentFingerprintSha256);
      const pkgOps = pkg.opportunities || pkg.payload?.opportunities || [];
      const pkgIds = opportunityIds(pkgOps);
      if (pkgFp !== pointerFp) {
        push("PACKAGE_BYTES_FINGERPRINT_MISMATCH", { pkgFp, pointerFp });
      }
      if (pkgIds.join("\n") !== ids.join("\n")) {
        push("RESOLVED_IDS_NOT_PACKAGE_IDS", {
          packageCount: pkgIds.length,
          resolvedCount: ids.length,
        });
      }
      if (counts.all !== pkgOps.length) {
        push("COUNT_MISMATCH_PACKAGE", {
          resolved: counts.all,
          package: pkgOps.length,
        });
      }
    }
  }

  // Optional: every opportunity must carry enough body for a drawer open.
  if (input.requireDrawerBodies !== false) {
    const thin = ops.filter((o) => {
      const desc = text(
        o.whatIsHappening || o.commercialDescription || o.description || o.summary,
      );
      return desc.length < 40 || /\bIncomplete\b/i.test(desc);
    });
    if (thin.length) {
      push("DRAWER_BODY_INCOMPLETE", {
        count: thin.length,
        sampleIds: thin.slice(0, 5).map((o) => o.id),
      });
    }
  }

  return {
    ok: fails.length === 0,
    gate: GDI_LOCAL_STAGED_PREVIEW_CERTIFICATION_GATE_V1,
    hotelId,
    publicationVersion: pointerVersion || null,
    contentFingerprintSha256: pointerFp || resolvedFp || null,
    previewSource: previewSource || null,
    inventoryCounts: counts,
    opportunityIds: ids,
    fails,
    browserVerifiedEligible: fails.length === 0,
    note:
      "A Full E2E campaign must not be marked locally browser-verified until this gate passes for the staged hotel package.",
  };
}

export function assertLocalStagedPreviewCertified(input = {}) {
  const result = certifyLocalStagedPreviewResolution(input);
  if (!result.ok) {
    const err = new Error(
      `LOCAL_STAGED_PREVIEW_CERT_FAILED:${(result.fails || [])
        .map((f) => f.code)
        .join(",")}`,
    );
    err.result = result;
    throw err;
  }
  return result;
}
