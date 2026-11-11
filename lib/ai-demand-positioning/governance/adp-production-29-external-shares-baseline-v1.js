/**
 * ADP_PRODUCTION_29_EXTERNAL_SHARES_20261023_V1
 *
 * Immutable production baseline for the 29 verified external client shares
 * frozen after the three-hotel production release. Content-addressed; no
 * bearer tokens stored in the baseline manifest.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

export const ADP_PRODUCTION_29_EXTERNAL_SHARES_BASELINE_ID =
  "ADP_PRODUCTION_29_EXTERNAL_SHARES_20261023_V1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
export const BASELINE_DIR = path.join(
  ROOT,
  "data/ai-demand-positioning/baselines/adp-production-29-external-shares-20261023-v1"
);
export const BASELINE_MANIFEST_PATH = path.join(
  BASELINE_DIR,
  "PRODUCTION_29_SHARE_BASELINE_MANIFEST.json"
);

export const BASELINE_DEPLOYMENT = Object.freeze({
  productionDeploymentId: "5b2ab218-ae08-46fe-ba91-5ffc0496a75b",
  artifactReleaseDeploymentId: "62fed299-22bc-45d6-96f0-e01b2996f6c5",
  releaseBranch: "release/adp-three-hotel-external-share-v1",
  productionHost: "https://my-operators-backend-production.up.railway.app",
  evidencePackage:
    "reports/dealality-adp-three-hotel-production-external-share-release-v1/",
});

export const INTENTIONALLY_UNPUBLISHED = Object.freeze([
  "adp_falls_church_marriott_fairview_park",
  "adp_nobu_london_portman",
  "adp_renaissance_arlington_capital_view",
  "adp_tampa_airport_marriott",
]);

export function sha256Json(obj) {
  const canonical = JSON.stringify(obj);
  return crypto.createHash("sha256").update(canonical).digest("hex");
}

export function fingerprintToken(token) {
  if (!token) return null;
  return crypto.createHash("sha256").update(String(token)).digest("hex").slice(0, 16);
}

export function loadBaselineManifest() {
  if (!fs.existsSync(BASELINE_MANIFEST_PATH)) return null;
  return JSON.parse(fs.readFileSync(BASELINE_MANIFEST_PATH, "utf8"));
}

/**
 * Verify baseline integrity (content hash + structural invariants).
 */
export function verifyBaselineIntegrity(manifest = loadBaselineManifest()) {
  const failures = [];
  if (!manifest) {
    return { ok: false, failures: ["BASELINE_MANIFEST_MISSING"] };
  }
  if (manifest.baselineId !== ADP_PRODUCTION_29_EXTERNAL_SHARES_BASELINE_ID) {
    failures.push("BASELINE_ID_MISMATCH");
  }
  if (manifest.counts?.verifiedExternalShares !== 29) {
    failures.push(`EXPECTED_29_SHARES_GOT_${manifest.counts?.verifiedExternalShares}`);
  }
  if (manifest.counts?.commissionedHotels !== 33) {
    failures.push(`EXPECTED_33_COMMISSIONED_GOT_${manifest.counts?.commissionedHotels}`);
  }
  if (manifest.counts?.intentionallyUnpublished !== 4) {
    failures.push("EXPECTED_4_UNPUBLISHED");
  }
  const hotels = manifest.hotels || [];
  if (hotels.length !== 33) failures.push(`HOTEL_ROWS_${hotels.length}`);
  const withShare = hotels.filter((h) => h.classification === "VALID_PRODUCTION_LINK");
  if (withShare.length !== 29) failures.push(`VALID_ROWS_${withShare.length}`);
  for (const h of withShare) {
    if (!h.tokenFingerprint || String(h.tokenFingerprint).length < 8) {
      failures.push(`MISSING_FP:${h.propertyId}`);
    }
    if (h.token || (h.shareUrl && /adpshare\.v1\.eyJ/i.test(h.shareUrl))) {
      failures.push(`TOKEN_LEAK:${h.propertyId}`);
    }
  }
  for (const id of INTENTIONALLY_UNPUBLISHED) {
    const row = hotels.find((h) => h.propertyId === id);
    if (!row) failures.push(`UNPUBLISHED_MISSING:${id}`);
    else if (row.classification === "VALID_PRODUCTION_LINK") {
      failures.push(`UNPUBLISHED_MARKED_VALID:${id}`);
    }
  }
  const { contentHash, ...rest } = manifest;
  const recomputed = sha256Json(rest);
  if (contentHash && contentHash !== recomputed) {
    failures.push("CONTENT_HASH_MISMATCH");
  }
  return {
    ok: failures.length === 0,
    failures,
    contentHash: contentHash || recomputed,
    recomputedContentHash: recomputed,
    baselineId: ADP_PRODUCTION_29_EXTERNAL_SHARES_BASELINE_ID,
  };
}
