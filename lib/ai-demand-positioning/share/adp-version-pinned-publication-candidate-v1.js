/**
 * Version-pinned external publication candidates for controlled ADP pilot.
 * Preparation only — never marks Live / PUBLISHED_READY.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { loadStagedOwnerPayloadByVersionId } from "../review/adp-staged-report-store-v1.js";
import { CONDITIONAL_APPROVED_METHODOLOGY_ID } from "../intelligence/reality-gap-methodology-v2.js";
import {
  REPORT_SCOPE_PINNED_REPORT_VERSION,
  issueShareCapability,
  verifyShareCapability,
} from "./adp-signed-share-capability-v1.js";

export const ADP_VERSION_PINNED_PUBLICATION_CANDIDATE_V1 =
  "ADP_VERSION_PINNED_PUBLICATION_CANDIDATE_V1";

const CANDIDATE_DIR = path.join(
  process.cwd(),
  "data/ai-demand-positioning/publication-candidates"
);

function shaFile(fp) {
  return crypto.createHash("sha256").update(fs.readFileSync(fp)).digest("hex");
}

function candidatePath(candidateId) {
  return path.join(CANDIDATE_DIR, `${candidateId}.json`);
}

/**
 * Build an immutable publication candidate bound to exact staged version + PDF hash.
 * Does NOT publish and does NOT mark PUBLISHED_READY.
 */
export function prepareVersionPinnedPublicationCandidate({
  propertyId,
  stagedVersionId,
  pdfPath,
  expectedPdfSha256 = null,
  corePeriodId = null,
  loyaltyPeriodId = null,
  bppRank = null,
  surfaces = ["report", "evidence", "properties", "publication_meta"],
  label = null,
} = {}) {
  const staged = loadStagedOwnerPayloadByVersionId(propertyId, stagedVersionId);
  if (!staged?.ok) {
    return { ok: false, error: staged?.error || "staged_version_unavailable", propertyId, stagedVersionId };
  }
  if (!pdfPath || !fs.existsSync(pdfPath)) {
    return { ok: false, error: "pdf_missing", propertyId, pdfPath };
  }
  const pdfSha256 = shaFile(pdfPath);
  if (expectedPdfSha256 && pdfSha256 !== expectedPdfSha256) {
    return {
      ok: false,
      error: "pdf_hash_mismatch",
      propertyId,
      expectedPdfSha256,
      actualPdfSha256: pdfSha256,
    };
  }
  const methodologyId =
    staged.payload?.realityGap?.methodologyId || CONDITIONAL_APPROVED_METHODOLOGY_ID;
  if (methodologyId !== CONDITIONAL_APPROVED_METHODOLOGY_ID) {
    return {
      ok: false,
      error: "methodology_not_v2",
      propertyId,
      methodologyId,
    };
  }
  const candidateId = `cand_${propertyId}_${stagedVersionId}`;
  const candidate = {
    schema: ADP_VERSION_PINNED_PUBLICATION_CANDIDATE_V1,
    candidateId,
    propertyId,
    stagedVersionId,
    corePeriodId: corePeriodId || staged.periodId || staged.payload?.period?.periodId || null,
    loyaltyPeriodId: loyaltyPeriodId || null,
    realityGapMethodologyId: methodologyId,
    realityGapV2: staged.payload?.realityGap?.gapScore ?? null,
    bppRank: bppRank || null,
    pdfPath: path.relative(process.cwd(), pdfPath).replace(/\\/g, "/"),
    pdfSha256,
    artifactHash: staged.artifactHash || null,
    surfaces: [...surfaces],
    shareScope: REPORT_SCOPE_PINNED_REPORT_VERSION,
    publicationStatus: "CANDIDATE_PREPARED_NOT_LIVE",
    publishedReady: false,
    externalReleaseStatus: "AWAITING_FOUNDER_APPROVAL",
    label: label || `pilot-candidate:${propertyId}`,
    preparedAt: new Date().toISOString(),
  };
  fs.mkdirSync(CANDIDATE_DIR, { recursive: true });
  fs.writeFileSync(candidatePath(candidateId), `${JSON.stringify(candidate, null, 2)}\n`, "utf8");
  return { ok: true, candidate };
}

export function loadPublicationCandidate(candidateId) {
  const p = candidatePath(candidateId);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Issue a local/test-only version-pinned share for a prepared candidate.
 * Never marks Live. Caller must not treat as production customer link.
 */
export function issueLocalTestPinnedShareForCandidate(candidate, { expiresHours = 2 } = {}) {
  if (!candidate?.ok && !candidate?.candidateId) {
    throw new Error("candidate_required");
  }
  const c = candidate.candidate || candidate;
  process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET =
    process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET || "1";
  const issued = issueShareCapability({
    propertyId: c.propertyId,
    label: `LOCAL_TEST_ONLY:${c.candidateId}`,
    reportScope: REPORT_SCOPE_PINNED_REPORT_VERSION,
    reportVersionId: c.stagedVersionId,
    pdfSha256: c.pdfSha256,
    candidateId: c.candidateId,
    surfaces: c.surfaces,
    expiresAt: new Date(Date.now() + expiresHours * 60 * 60 * 1000).toISOString(),
  });
  const verified = verifyShareCapability(issued.token, {
    expectedPropertyId: c.propertyId,
    requiredSurface: "report",
  });
  return { issued, verified, candidate: c };
}
