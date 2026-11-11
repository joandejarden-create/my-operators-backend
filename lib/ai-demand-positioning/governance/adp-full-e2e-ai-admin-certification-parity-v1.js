/**
 * ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1
 *
 * AI Admin must reflect certified Full ADP E2E staged evidence without requiring
 * a legacy monthly-review archive as the sole readiness signal.
 *
 * Does NOT publish. Does NOT re-run paid monitoring. Does NOT invent prior periods.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { loadPropertyProfile } from "../data-model.js";
import {
  loadStagedManifest,
  loadStagedOwnerPayload,
} from "../review/adp-staged-report-store-v1.js";
import { listArchiveReviews } from "../monthly-review/admin/archive-store-v1.js";
import {
  generateNewDraft,
  attachPdfToReview,
} from "../monthly-review/admin/generate-review-v1.js";
import { GENERATION_REASON, REVIEW_STATUS } from "../monthly-review/admin/report-status-v1.js";
import { resolveAdpFounderReviewReadinessV1 } from "../monthly-review/resolve-adp-founder-review-readiness-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "./adp-operational-registry-hygiene-v1.js";
import { isBppResearchOnlyPeer } from "./adp-property-registry-scope-v1.js";

export const ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1 =
  "ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function sha256File(filePath) {
  return createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

/**
 * Prefer immutable Full ADP E2E PDF artifact store, then content-addressed
 * greenfield baselines / STAGED_PDFS. Never treat Monthly Executive Review as
 * the Full ADP report source of truth.
 */
export function resolveCertifiedStagedPdfPath(propertyId, opts = {}) {
  const id = String(propertyId || "").trim();
  const cwd = opts.cwd || process.cwd();
  const stagedVersionId = opts.stagedVersionId || null;
  const candidates = [];
  if (stagedVersionId) {
    candidates.push(
      path.join(
        cwd,
        "data/ai-demand-positioning/full-adp-e2e-pdf",
        id,
        stagedVersionId,
        "report.pdf"
      )
    );
  }
  // LATEST pointer for Full ADP store
  const latestPointer = path.join(
    cwd,
    "data/ai-demand-positioning/full-adp-e2e-pdf",
    id,
    "LATEST.json"
  );
  if (fs.existsSync(latestPointer)) {
    try {
      const ptr = JSON.parse(fs.readFileSync(latestPointer, "utf8"));
      if (ptr?.stagedVersionId) {
        candidates.push(
          path.join(
            cwd,
            "data/ai-demand-positioning/full-adp-e2e-pdf",
            id,
            ptr.stagedVersionId,
            "report.pdf"
          )
        );
      }
    } catch {
      /* ignore */
    }
  }

  candidates.push(
    path.join(
      cwd,
      "reports/dealality-adp-two-greenfield-final-acceptance-and-baseline-v1/CONTENT_ADDRESSED_ARCHIVES",
      id.includes("chapultepec")
        ? "ADP_GREENFIELD_GRAND_FIESTA_AMERICANA_CHAPULTEPEC_20261022_V1"
        : id.includes("condesa")
          ? "ADP_GREENFIELD_FIESTA_AMERICANA_CONDESA_CANCUN_20261022_V1"
          : "",
      `${id}.pdf`
    ),
    path.join(
      cwd,
      "reports/dealality-adp-autonomous-competitor-resolution-and-two-hotel-certification-v1/STAGED_PDFS",
      `${id}.pdf`
    ),
    path.join(
      cwd,
      "reports/dealality-adp-chapultepec-full-greenfield-e2e-v1/STAGED_PDFS",
      `${id}.pdf`
    ),
    path.join(
      cwd,
      "reports/dealality-adp-condesa-cancun-full-greenfield-e2e-v1/STAGED_PDFS",
      `${id}.pdf`
    ),
    path.join(
      cwd,
      "reports/dealality-adp-full-e2e-pdf-canonical-fidelity-v1/CORRECTED_PDFS",
      `${id}.pdf`
    ),
    path.join(
      cwd,
      "reports/ai-demand-positioning/current-report-pdf",
      id,
      "report.pdf"
    )
  );

  for (const p of candidates.filter((x) => x && !x.includes(`${path.sep}${path.sep}`))) {
    if (fs.existsSync(p) && fs.statSync(p).size > 100) return p;
  }
  return null;
}

function bppSummaryFromPayload(payload) {
  const bpp = payload?.brandPortfolioPosition || null;
  if (!bpp) return { present: false };
  const bppNa =
    bpp.customerState === "BPP_NOT_APPLICABLE" ||
    bpp.status === "BPP_NOT_APPLICABLE" ||
    bpp.measuredRank === "BPP_NOT_APPLICABLE" ||
    bpp.disclosureReason === "LENS_NOT_APPLICABLE";
  const kpis = Array.isArray(bpp.kpis) ? bpp.kpis : [];
  const byId = Object.fromEntries(kpis.map((k) => [k.id, k]));
  return {
    present: bppNa || kpis.length > 0 || Boolean(bpp.ranking?.rows?.length),
    notApplicable: bppNa,
    portfolioAiPresence: byId.portfolioAiPresence?.value || null,
    portfolioRank: byId.portfolioRank?.value || null,
    numberOneAppearance: byId.numberOneAppearance?.value || null,
    top3Appearance: byId.top3Appearance?.value || null,
    certificationStatus: bpp.certification?.status || bpp.status || null,
  };
}

/**
 * Assess whether Full ADP E2E certified evidence exists for AI Admin parity.
 */
export function assessFullAdpE2eAiAdminCertificationParity(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const id = String(propertyId || "").trim();
  const failures = [];

  if (!id) {
    return {
      ok: false,
      gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
      failures: ["propertyId_required"],
    };
  }
  if (isAdpSyntheticOrTestPropertyId(id)) {
    return {
      ok: false,
      gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
      propertyId: id,
      failures: ["synthetic_excluded"],
      published: false,
    };
  }

  const profile = loadPropertyProfile(id) || { propertyId: id };
  if (isBppResearchOnlyPeer(profile, { hasStaged: true, hasPublished: false })) {
    return {
      ok: false,
      gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
      propertyId: id,
      failures: ["research_only_bpp_peer"],
      published: false,
    };
  }

  const staged = loadStagedOwnerPayload(id, cwd);
  const manifest = loadStagedManifest(id, cwd);
  const versionId =
    staged?.versionId ||
    manifest?.latestStagedVersionId ||
    manifest?.latestVersionId ||
    manifest?.versionId ||
    null;
  if (!staged || staged.ok === false || !versionId) {
    failures.push("staged_report_missing");
  }
  if (
    opts.expectedStagedVersionId &&
    versionId &&
    versionId !== opts.expectedStagedVersionId
  ) {
    failures.push("staged_version_mismatch");
  }

  const periodId =
    staged?.periodId ||
    staged?.payload?.period?.periodId ||
    manifest?.periodId ||
    null;
  if (!periodId) failures.push("core_period_missing");

  const bpp = bppSummaryFromPayload(staged?.payload);
  if (!bpp.present) failures.push("bpp_section_missing");

  const pdfPath = resolveCertifiedStagedPdfPath(id, {
    cwd,
    stagedVersionId: opts.expectedStagedVersionId || versionId,
  });
  const pdfSha256 = pdfPath ? sha256File(pdfPath) : null;
  if (!pdfPath) failures.push("certified_staged_pdf_missing");

  const archiveRows = listArchiveReviews({
    propertyId: id,
    includeArchived: false,
  });
  const latestArchive = archiveRows[0] || null;
  const archivePresent = Boolean(latestArchive?.reviewId);
  const archivePdfReady =
    latestArchive?.pdfStatus === "READY" || latestArchive?.pdfStatus === "READY";

  const founder = resolveAdpFounderReviewReadinessV1(id, {
    hasReviewArtifact: archivePresent,
    hasPdfReady: archivePdfReady || Boolean(pdfPath),
  });

  const publicationStatus =
    manifest?.publicationStatus === "PUBLISHED" ||
    manifest?.publicationStatus === "Live"
      ? "PUBLISHED"
      : "NOT_AUTHORIZED";

  const evidenceOk = failures.length === 0;
  // Parity can be READY for admin when staged+PDF certified even if archive not yet materialised.
  const adminReadyWithoutLegacyArchive = evidenceOk;
  const adminReadyWithArchive = evidenceOk && archivePresent && (archivePdfReady || Boolean(pdfPath));

  return {
    ok: evidenceOk,
    gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
    propertyId: id,
    hotelName: profile?.name || staged?.payload?.property?.name || id,
    hpcId: profile?.censusRecordId || profile?.hpcHotelId || null,
    stagedVersionId: versionId,
    corePeriodId: periodId,
    loyaltyPeriodId:
      staged?.payload?.period?.loyaltyPeriodId ||
      staged?.payload?.loyalty?.periodId ||
      null,
    bpp,
    pdfPath: pdfPath ? path.relative(cwd, pdfPath).replace(/\\/g, "/") : null,
    pdfAbsolutePath: pdfPath,
    pdfSha256,
    archivePresent,
    archiveReviewId: latestArchive?.reviewId || null,
    archivePdfReady,
    founderReviewEligible: founder.eligibleForFounderReview === true || evidenceOk,
    founderReviewStatus: founder.founderReviewStatus,
    customerDeliveryCertified: evidenceOk,
    memberstackAuthenticatedQa: "AUTHENTICATED_QA_PENDING",
    publicationStatus,
    published: false,
    adminReadyWithoutLegacyArchive,
    adminReadyWithArchive,
    failures,
    note:
      "Full E2E staged certification is authoritative for AI Admin founder-review readiness; monthly-review archive is recovered from staged when missing.",
  };
}

/**
 * Generate founder-review archive from certified staged report + attach staged PDF.
 * Zero paid. Never publishes. Never invents prior periods.
 */
export function recoverFounderReviewArtifactFromCertifiedStaged(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const id = String(propertyId || "").trim();
  const assessment = assessFullAdpE2eAiAdminCertificationParity(id, {
    cwd,
    expectedStagedVersionId: opts.expectedStagedVersionId || null,
  });
  if (!assessment.ok) {
    return {
      ok: false,
      gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
      propertyId: id,
      error: "certification_evidence_incomplete",
      assessment,
      published: false,
    };
  }

  if (assessment.archivePresent && assessment.archivePdfReady) {
    return {
      ok: true,
      gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
      propertyId: id,
      alreadyPresent: true,
      reviewId: assessment.archiveReviewId,
      stagedVersionId: assessment.stagedVersionId,
      published: false,
      assessment,
    };
  }

  let reviewId = assessment.archiveReviewId;
  let generated = null;
  if (!assessment.archivePresent) {
    generated = generateNewDraft(id, {
      purpose: "founder_review",
      generatedBy: opts.actor || "adp_full_e2e_ai_admin_certification_parity_v1",
      generationReason: GENERATION_REASON.GENERATE_NEW_DRAFT,
      reviewStatus: REVIEW_STATUS.READY_FOR_REVIEW,
      warnings: [
        "Recovered from certified Full ADP E2E staged report — not a new monitoring run.",
        `Staged version: ${assessment.stagedVersionId}`,
        `Core period: ${assessment.corePeriodId}`,
        "Publication NOT_AUTHORIZED.",
      ],
    });
    if (!generated?.ok) {
      return {
        ok: false,
        gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
        propertyId: id,
        error: generated?.error || "generate_new_draft_failed",
        generated,
        assessment,
        published: false,
      };
    }
    reviewId = generated.newReviewId;
  }

  let pdfAttach = null;
  if (assessment.pdfAbsolutePath && reviewId) {
    try {
      pdfAttach = attachPdfToReview(reviewId, assessment.pdfAbsolutePath);
    } catch (e) {
      if (String(e.message || "").includes("pdf_already_exists")) {
        pdfAttach = { alreadyAttached: true };
      } else {
        return {
          ok: false,
          gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
          propertyId: id,
          error: `pdf_attach_failed:${e.message}`,
          reviewId,
          generated,
          assessment,
          published: false,
        };
      }
    }
  }

  const after = assessFullAdpE2eAiAdminCertificationParity(id, {
    cwd,
    expectedStagedVersionId: assessment.stagedVersionId,
  });

  return {
    ok: after.archivePresent === true && after.ok === true,
    gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
    propertyId: id,
    alreadyPresent: false,
    reviewId,
    stagedVersionId: assessment.stagedVersionId,
    corePeriodId: assessment.corePeriodId,
    pdfSha256: assessment.pdfSha256,
    generated,
    pdfAttach: pdfAttach
      ? {
          pdfStatus: pdfAttach.pdfStatus || "READY",
          pdfFingerprint: pdfAttach.pdfFingerprint || assessment.pdfSha256,
          alreadyAttached: pdfAttach.alreadyAttached === true,
        }
      : null,
    assessment: after,
    published: false,
    publicationStatus: "NOT_AUTHORIZED",
  };
}

/**
 * Full E2E gate: register discoverability already handled separately; this ensures
 * AI Admin coverage/status parity (periods, staged version, founder-review artifact).
 */
export function ensureFullAdpE2eAiAdminCertificationParity(propertyId, opts = {}) {
  const recovery = recoverFounderReviewArtifactFromCertifiedStaged(propertyId, opts);
  const assessment =
    recovery.assessment ||
    assessFullAdpE2eAiAdminCertificationParity(propertyId, opts);

  const ok =
    recovery.ok === true &&
    assessment.ok === true &&
    assessment.adminReadyWithoutLegacyArchive === true &&
    assessment.publicationStatus === "NOT_AUTHORIZED";

  return {
    ok,
    gate: ADP_FULL_E2E_AI_ADMIN_CERTIFICATION_PARITY_V1,
    blocksCustomerDeliveryCertification: !ok,
    recovery,
    assessment,
    published: false,
    publicationStatus: "NOT_AUTHORIZED",
    memberstackAuthenticatedQa: "AUTHENTICATED_QA_PENDING",
  };
}
