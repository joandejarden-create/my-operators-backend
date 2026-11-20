/**
 * Factory 2.0 batch approval — per-candidate tokens only.
 * Never a generic token that bypasses candidate-specific verification.
 */
import { createHash, randomBytes } from "crypto";
import {
  BE_FACTORY_2_0_BATCH_V1,
  FACTORY_2_STATUSES,
  PROTECTED_BRAND_IDS,
} from "./be-factory-2-contracts-v1.js";
import {
  loadBrandStage,
  saveBrandStage,
  saveBatchApprovalRecord,
  buildPortfolioDashboard,
} from "./be-factory-2-staging-store-v1.js";

export const BE_FACTORY_2_0_BATCH_APPROVAL_V1 = "BE_FACTORY_2_0_BATCH_APPROVAL_V1";

function mintCandidateApprovalToken({ brandId, candidateHash, approvedBy }) {
  const nonce = randomBytes(8).toString("hex");
  const material = [brandId, candidateHash, approvedBy || "", nonce, Date.now()].join("|");
  const digest = createHash("sha256").update(material).digest("hex");
  return {
    tokenId: `be-f2-appr-${digest.slice(0, 20)}`,
    brandId,
    candidateHash,
    nonce,
    approvedBy: approvedBy || null,
    issuedAt: new Date().toISOString(),
  };
}

/**
 * Record founder batch approval for selected brands.
 * Does NOT execute production writeback — only records exact per-candidate approvals.
 */
export function recordBatchApprovals(repoRoot, input = {}) {
  const approvedBy = String(input.approvedBy || "").trim() || "founder";
  const selections = Array.isArray(input.selections) ? input.selections : [];
  if (!selections.length) {
    return { ok: false, error: "No brand selections provided" };
  }

  const results = [];
  const approvalEntries = [];

  for (const sel of selections) {
    const brandId = String(sel.brandId || "").trim();
    const expectedHash = String(sel.candidateHash || "").trim();
    if (!brandId || !expectedHash) {
      results.push({
        brandId: brandId || null,
        ok: false,
        error: "brandId and candidateHash required per selection",
      });
      continue;
    }
    if (PROTECTED_BRAND_IDS.includes(brandId)) {
      results.push({ brandId, ok: false, error: "Protected brand — approval refused" });
      continue;
    }

    const stage = loadBrandStage(repoRoot, brandId);
    if (!stage) {
      results.push({ brandId, ok: false, error: "No staged candidate" });
      continue;
    }
    if (stage.factoryStatus !== FACTORY_2_STATUSES.READY_FOR_REVIEW) {
      results.push({
        brandId,
        ok: false,
        error: `factoryStatus=${stage.factoryStatus} (require READY_FOR_REVIEW)`,
      });
      continue;
    }
    const liveHash = String(stage.candidateHash || "").trim();
    if (liveHash !== expectedHash) {
      results.push({
        brandId,
        ok: false,
        error: "candidateHash mismatch — exact candidate verification failed",
        expectedHash,
        liveHash,
      });
      continue;
    }

    const token = mintCandidateApprovalToken({ brandId, candidateHash: liveHash, approvedBy });
    const writePlanFingerprint = createHash("sha256")
      .update(JSON.stringify(stage.writePlan || {}))
      .digest("hex")
      .slice(0, 24);

    stage.factoryStatus = FACTORY_2_STATUSES.APPROVED;
    stage.approvalStatus = "APPROVED_STAGED_ONLY";
    stage.writebackAuthorized = false; // production write still requires activation + publish path
    stage.approval = {
      tokenId: token.tokenId,
      candidateHash: liveHash,
      assembledHash: stage.assembledHash || null,
      writePlanFingerprint,
      approvedBy,
      approvedAt: token.issuedAt,
      writebackPending: true,
      note: "Approval recorded; production Presentation writeback not executed in this step",
    };
    saveBrandStage(repoRoot, brandId, stage);

    approvalEntries.push({
      brandId,
      brandName: stage.brandName,
      candidateHash: liveHash,
      assembledHash: stage.assembledHash || null,
      tokenId: token.tokenId,
      writePlanFingerprint,
      writePlanCounts: stage.writePlan?.counts || null,
    });
    results.push({ brandId, ok: true, tokenId: token.tokenId, writebackAuthorized: false });
  }

  const approvalDoc = {
    contract: BE_FACTORY_2_0_BATCH_APPROVAL_V1,
    parentContract: BE_FACTORY_2_0_BATCH_V1,
    approvalId: `batch-${Date.now()}`,
    approvedBy,
    recordedAt: new Date().toISOString(),
    productionWritebackExecuted: false,
    genericTokenForbidden: true,
    entries: approvalEntries,
    results,
  };
  const path = saveBatchApprovalRecord(repoRoot, approvalDoc);
  const dashboard = buildPortfolioDashboard(repoRoot);

  return {
    ok: results.every((r) => r.ok),
    approvalId: approvalDoc.approvalId,
    path,
    approvedCount: approvalEntries.length,
    failedCount: results.filter((r) => !r.ok).length,
    results,
    dashboardSummary: dashboard.summary,
  };
}

/**
 * Incremental refresh: detect material change vs prior staged candidate.
 */
export function detectIncrementalChange(priorStage, nextCandidateBlocks, hashFn) {
  const priorHash = priorStage?.candidateHash || null;
  const nextHash = hashFn(nextCandidateBlocks || []);
  const materialChange = Boolean(priorHash && nextHash && priorHash !== nextHash);
  return {
    priorHash,
    nextHash,
    materialChange,
    reviewDeltaOnly: materialChange,
    unchanged: Boolean(priorHash && priorHash === nextHash),
  };
}
