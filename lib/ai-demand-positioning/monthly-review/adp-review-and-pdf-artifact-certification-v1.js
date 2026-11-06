/**
 * ADP_REVIEW_AND_PDF_ARTIFACT_CERTIFICATION_V1
 *
 * Shared contract: staged/published source → review artifact → PDF → manifest → admin status.
 * Never hardcodes READY. Never treats synthetic fixtures as operational hotels.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildPublishedAdpReviewCoverageV1 } from "./admin/published-review-coverage-v1.js";
import { loadReviewPayload } from "./admin/archive-store-v1.js";
import { resolveAdpReportPdf } from "./resolve-adp-report-pdf-v1.js";
import { loadPublishedManifest, loadPublishedReport } from "../published-snapshot.js";
import {
  assertOperationalRegistryHygiene,
  isAdpSyntheticOrTestPropertyId,
  listCanonicalAdp31PropertyIds,
} from "../governance/adp-operational-registry-hygiene-v1.js";

export const ADP_REVIEW_AND_PDF_ARTIFACT_CERTIFICATION_V1 =
  "ADP_REVIEW_AND_PDF_ARTIFACT_CERTIFICATION_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

/**
 * @param {string} propertyId
 * @param {object} [coverageRow]
 */
export function certifyPropertyReviewAndPdfArtifactsV1(propertyId, coverageRow = null) {
  const id = String(propertyId || "").trim();
  const synthetic = isAdpSyntheticOrTestPropertyId(id);
  const row =
    coverageRow ||
    buildPublishedAdpReviewCoverageV1().properties.find((p) => p.propertyId === id) ||
    null;

  const manifest = loadPublishedManifest(id);
  const report = loadPublishedReport(id);
  const reviewId = row?.reviewId || null;
  const pack = reviewId ? loadReviewPayload(reviewId) : null;
  const pdfPath = pack?.paths?.reportPdf || null;
  const pdfExists = Boolean(pdfPath && fs.existsSync(pdfPath));
  const pdfBytes = pdfExists ? fs.statSync(pdfPath).size : 0;
  const pdfResolution = resolveAdpReportPdf({
    reviewId,
    propertyId: id,
    publishedPeriodId:
      row?.currentPeriodId ||
      manifest?.latestPeriodId ||
      report?.period?.periodId ||
      null,
    requirePublishedPeriodMatch: Boolean(
      row?.currentPeriodId || manifest?.latestPeriodId || report?.period?.periodId
    ),
  });

  const reviewPresent = Boolean(pack?.review && pack?.meta);
  const reviewMatchesHotel =
    !pack?.meta?.propertyId || pack.meta.propertyId === id;
  const reviewPeriod =
    pack?.meta?.sourceCurrentPeriodId ||
    pack?.review?.period?.periodId ||
    null;
  const publishedPeriod =
    row?.currentPeriodId ||
    manifest?.latestPeriodId ||
    report?.period?.periodId ||
    null;
  const periodMatch =
    !reviewPeriod || !publishedPeriod || reviewPeriod === publishedPeriod;

  const ARTIFACT_PRESENT = reviewPresent && pdfExists;
  const ARTIFACT_CURRENT = Boolean(periodMatch && pdfResolution.matchesPublishedPeriod !== false);
  const ARTIFACT_VALID =
    ARTIFACT_PRESENT &&
    reviewMatchesHotel &&
    pdfBytes > 1024 &&
    (row?.pdfStatus === "READY" || pdfResolution.available);
  const ARTIFACT_ACCESSIBLE = Boolean(
    ARTIFACT_VALID && (pdfResolution.available || pdfExists)
  );
  const ARTIFACT_CONTENT_CERTIFIED = Boolean(
    ARTIFACT_VALID &&
      ARTIFACT_CURRENT &&
      row?.coverageStatus === "READY" &&
      row?.eligible
  );

  // Unpublished + founder-eligible is not a false BLOCKED — dual readiness fields required.
  const accurateBlocker =
    row?.coverageStatus === "BLOCKED" &&
    Boolean(row?.coverageReason) &&
    row?.coverageReason !== "not_in_published_adp_universe";

  return {
    gate: ADP_REVIEW_AND_PDF_ARTIFACT_CERTIFICATION_V1,
    propertyId: id,
    synthetic,
    operationalEligible: !synthetic,
    coverageStatus: row?.coverageStatus || null,
    coverageReason: row?.coverageReason || null,
    founderReviewStatus: row?.founderReviewStatus || null,
    publishedReadyStatus: row?.publishedReadyStatus || null,
    publishedReady: row?.publishedReady === true,
    claimsCoreCertified: row?.claimsCoreCertified === true,
    reviewId,
    publishedPeriod,
    reviewPeriod,
    periodMatch,
    pdfPath: pdfPath ? path.relative(ROOT, pdfPath).replace(/\\/g, "/") : null,
    pdfBytes,
    ARTIFACT_PRESENT,
    ARTIFACT_CURRENT,
    ARTIFACT_VALID,
    ARTIFACT_ACCESSIBLE,
    ARTIFACT_CONTENT_CERTIFIED,
    pass:
      !synthetic &&
      (accurateBlocker
        ? true
        : ARTIFACT_CONTENT_CERTIFIED ||
          (row?.coverageStatus === "NEEDS_REBUILD" && Boolean(row?.coverageReason)) ||
          (row?.coverageStatus === "READY" && row?.eligible === true) ||
          (row?.founderReviewStatus === "FOUNDER_REVIEW_NEEDS_ARTIFACT" &&
            row?.eligible === true)),
  };
}

/**
 * Portfolio certification for the canonical 31.
 */
export function certifyCanonical31ReviewAndPdfArtifactsV1() {
  const canonical = listCanonicalAdp31PropertyIds();
  const coverage = buildPublishedAdpReviewCoverageV1();
  const hygiene = assertOperationalRegistryHygiene(
    coverage.properties.map((p) => p.propertyId)
  );
  const byId = new Map(coverage.properties.map((p) => [p.propertyId, p]));
  const rows = canonical.map((id) =>
    certifyPropertyReviewAndPdfArtifactsV1(id, byId.get(id) || null)
  );

  const syntheticInCoverage = coverage.properties.filter((p) =>
    isAdpSyntheticOrTestPropertyId(p.propertyId)
  );
  const ready = rows.filter((r) => r.coverageStatus === "READY");
  const needsRebuild = rows.filter((r) => r.coverageStatus === "NEEDS_REBUILD");
  const blocked = rows.filter((r) => r.coverageStatus === "BLOCKED");
  const contentCertified = rows.filter((r) => r.ARTIFACT_CONTENT_CERTIFIED);

  return {
    gate: ADP_REVIEW_AND_PDF_ARTIFACT_CERTIFICATION_V1,
    hygieneGate: hygiene.gate,
    hygienePass: hygiene.pass && syntheticInCoverage.length === 0,
    canonicalCount: canonical.length,
    coverageUniverseCount: coverage.properties.length,
    readyCount: ready.length,
    needsRebuildCount: needsRebuild.length,
    blockedCount: blocked.length,
    contentCertifiedCount: contentCertified.length,
    syntheticLeaks: syntheticInCoverage.map((p) => p.propertyId),
    bethesdaPresent: canonical.includes("adp_bethesda_marriott"),
    rows,
    pass:
      hygiene.pass &&
      syntheticInCoverage.length === 0 &&
      coverage.properties.length === 31 &&
      canonical.length === 31,
  };
}
