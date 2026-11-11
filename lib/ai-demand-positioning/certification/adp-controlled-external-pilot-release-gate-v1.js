/**
 * ADP_CONTROLLED_EXTERNAL_PILOT_RELEASE_GATE_V1
 * Fail-closed gate for controlled five-hotel external pilot release.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { CONDITIONAL_APPROVED_METHODOLOGY_ID } from "../intelligence/reality-gap-methodology-v2.js";
import { loadStagedOwnerPayloadByVersionId } from "../review/adp-staged-report-store-v1.js";
import { independentlyRecalculateRealityGapV1V2 } from "./adp-rg-v2-cross-surface-parity-certification-v1.js";
import { verifyBaselineIntegrity } from "./adp-certified-31-hotel-baseline-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../governance/adp-operational-registry-hygiene-v1.js";

export const ADP_CONTROLLED_EXTERNAL_PILOT_RELEASE_GATE_V1 =
  "ADP_CONTROLLED_EXTERNAL_PILOT_RELEASE_GATE_V1";

export const EXTERNAL_RELEASE_STATUS_AWAITING = "AWAITING_FOUNDER_APPROVAL";
export const EXTERNAL_RELEASE_STATUS_APPROVED = "FOUNDER_APPROVED";

function shaFile(fp) {
  return crypto.createHash("sha256").update(fs.readFileSync(fp)).digest("hex");
}

function bppRank(payload) {
  const kpi = (payload?.brandPortfolioPosition?.kpis || []).find(
    (k) => k.id === "portfolioRank"
  );
  if (!kpi) return null;
  if (kpi.available === false || kpi.value === "—") return "SUBJECT_MEASURED_RANK_BLOCKED";
  return kpi.value || null;
}

/**
 * Evaluate one hotel against an exact immutable release row.
 * Does not publish. Requires explicit founderApproval === true on the manifest row
 * AND matching immutable fields to authorize release execution.
 */
export function evaluatePilotHotelReleaseRow(row, opts = {}) {
  const issues = [];
  const propertyId = row.propertyId;
  if (!propertyId || isAdpSyntheticOrTestPropertyId(propertyId)) {
    issues.push("synthetic_or_missing_property");
  }
  const staged = loadStagedOwnerPayloadByVersionId(propertyId, row.stagedVersionId);
  if (!staged?.ok) issues.push(`staged_unavailable:${staged?.error || "missing"}`);
  if (staged?.ok && staged.versionId !== row.stagedVersionId) {
    issues.push("staged_version_mismatch");
  }
  const meth =
    staged?.payload?.realityGap?.methodologyId ||
    row.realityGapMethodologyId ||
    "";
  if (meth !== CONDITIONAL_APPROVED_METHODOLOGY_ID) {
    issues.push(`methodology:${meth}`);
  }
  const liveRg = independentlyRecalculateRealityGapV1V2(propertyId);
  if (liveRg.ok && row.realityGapV2 != null && Number(liveRg.gapScoreV2) !== Number(row.realityGapV2)) {
    issues.push(`rg_v2_drift:${liveRg.gapScoreV2}!=${row.realityGapV2}`);
  }
  if (row.pdfPath) {
    const abs = path.isAbsolute(row.pdfPath)
      ? row.pdfPath
      : path.join(process.cwd(), row.pdfPath);
    if (!fs.existsSync(abs)) issues.push("pdf_missing");
    else if (row.pdfSha256 && shaFile(abs) !== row.pdfSha256) {
      issues.push("pdf_hash_mismatch");
    }
  } else {
    issues.push("pdf_path_required");
  }
  if (row.bppRank && staged?.ok) {
    const liveBpp = bppRank(staged.payload);
    if (liveBpp && String(liveBpp) !== String(row.bppRank)) {
      issues.push(`bpp_mismatch:${liveBpp}!=${row.bppRank}`);
    }
  }
  if (row.shareScope !== "pinned_report_version") {
    issues.push("share_scope_must_be_pinned_report_version");
  }
  if (String(row.externalReleaseStatus) !== EXTERNAL_RELEASE_STATUS_AWAITING &&
      String(row.externalReleaseStatus) !== EXTERNAL_RELEASE_STATUS_APPROVED) {
    issues.push(`bad_external_status:${row.externalReleaseStatus}`);
  }

  const authenticatedOk =
    row.authenticatedFounderQaStatus === "PASS" ||
    (opts.allowDocumentedAuthException === true &&
      row.authenticatedFounderQaStatus === "AUTHENTICATED_QA_PENDING" &&
      row.authExceptionDocumented === "Y");

  if (!authenticatedOk) {
    issues.push("authenticated_founder_qa_required_or_documented_exception");
  }

  const founderApproved =
    row.founderApproved === true ||
    String(row.founderApproved).toUpperCase() === "Y" ||
    String(row.externalReleaseStatus) === EXTERNAL_RELEASE_STATUS_APPROVED;

  const contentReady = issues.filter(
    (i) =>
      !i.startsWith("authenticated_") &&
      i !== "authenticated_founder_qa_required_or_documented_exception"
  );

  return {
    propertyId,
    stagedVersionId: row.stagedVersionId,
    gateId: ADP_CONTROLLED_EXTERNAL_PILOT_RELEASE_GATE_V1,
    contentOk: contentReady.length === 0,
    authenticatedOk,
    founderApproved,
    releaseExecutable:
      contentReady.length === 0 && authenticatedOk && founderApproved,
    failClosedWithoutApproval: !founderApproved,
    issues,
    blockers: issues,
  };
}

export function evaluatePilotReleaseManifest(rows, opts = {}) {
  const hotelResults = (rows || []).map((r) => evaluatePilotHotelReleaseRow(r, opts));
  const baseline = opts.skipBaselineVerify
    ? { ok: true, skipped: true }
    : verifyBaselineIntegrity(opts.baselineManifestPath || null);
  const backupOk = opts.backupIntegrityOk === true;
  const anyExecutable = hotelResults.some((h) => h.releaseExecutable);
  const allContent = hotelResults.every((h) => h.contentOk);
  return {
    gateId: ADP_CONTROLLED_EXTERNAL_PILOT_RELEASE_GATE_V1,
    ok:
      allContent &&
      baseline.ok &&
      backupOk &&
      hotelResults.every((h) => h.authenticatedOk),
    releaseExecutableAny: anyExecutable,
    releaseExecutableAll: hotelResults.every((h) => h.releaseExecutable),
    baselineOk: baseline.ok,
    backupOk,
    hotelResults,
    doctrine: {
      noAutoPublishOnGreen: true,
      versionChangeInvalidatesApproval: true,
      failClosedWithoutExactManifestApproval: true,
    },
  };
}

/** Regression helpers for negative tests (pure). */
export function assertStalePublishedShareNotEligible(publishedRg, stagedRgV2) {
  return Number(publishedRg) !== Number(stagedRgV2);
}

export function assertWrongHotelTokenRejected(verifyResult) {
  return verifyResult?.ok === false;
}
