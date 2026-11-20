/**
 * Durable Factory 2.0 staging store — survives restarts/merges.
 * Root: data/brand-explorer/factory-2-0/
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync, readdirSync } from "fs";
import { join } from "path";
import {
  BE_FACTORY_2_0_BATCH_V1,
  FACTORY_2_STAGING_ROOT_REL,
  FACTORY_2_REPORTS_ROOT_REL,
} from "./be-factory-2-contracts-v1.js";

export function stagingRoot(repoRoot) {
  return join(repoRoot, FACTORY_2_STAGING_ROOT_REL);
}

export function reportsRoot(repoRoot) {
  return join(repoRoot, FACTORY_2_REPORTS_ROOT_REL);
}

export function brandDir(repoRoot, brandId) {
  return join(stagingRoot(repoRoot), "brands", brandId);
}

export function ensureDirs(repoRoot) {
  mkdirSync(join(stagingRoot(repoRoot), "brands"), { recursive: true });
  mkdirSync(join(stagingRoot(repoRoot), "approvals"), { recursive: true });
  mkdirSync(join(stagingRoot(repoRoot), "ledger"), { recursive: true });
  mkdirSync(reportsRoot(repoRoot), { recursive: true });
}

export function writeJson(absPath, obj) {
  mkdirSync(join(absPath, ".."), { recursive: true });
  writeFileSync(absPath, JSON.stringify(obj, null, 2) + "\n");
  return absPath;
}

export function readJson(absPath, fallback = null) {
  if (!existsSync(absPath)) return fallback;
  return JSON.parse(readFileSync(absPath, "utf8"));
}

export function saveQueue(repoRoot, queueDoc) {
  ensureDirs(repoRoot);
  const a = writeJson(join(stagingRoot(repoRoot), "queue.json"), queueDoc);
  writeJson(join(reportsRoot(repoRoot), "queue.json"), queueDoc);
  return a;
}

export function loadQueue(repoRoot) {
  return readJson(join(stagingRoot(repoRoot), "queue.json"), null);
}

export function saveBrandStage(repoRoot, brandId, stageDoc) {
  ensureDirs(repoRoot);
  const dir = brandDir(repoRoot, brandId);
  mkdirSync(dir, { recursive: true });
  writeJson(join(dir, "stage.json"), stageDoc);
  if (stageDoc.candidate) {
    writeJson(join(dir, "candidate.json"), stageDoc.candidate);
  }
  if (stageDoc.certification) {
    writeJson(join(dir, "certification.json"), stageDoc.certification);
  }
  if (stageDoc.writePlan) {
    writeJson(join(dir, "write-plan-dry-run.json"), stageDoc.writePlan);
  }
  if (stageDoc.researchSpend) {
    writeJson(join(dir, "research-spend.json"), stageDoc.researchSpend);
  }
  return dir;
}

export function loadBrandStage(repoRoot, brandId) {
  return readJson(join(brandDir(repoRoot, brandId), "stage.json"), null);
}

export function listStagedBrandIds(repoRoot) {
  const dir = join(stagingRoot(repoRoot), "brands");
  if (!existsSync(dir)) return [];
  return readdirSync(dir, { withFileTypes: true })
    .filter((d) => d.isDirectory() && d.name.startsWith("rec"))
    .map((d) => d.name);
}

export function buildPortfolioDashboard(repoRoot) {
  const queue = loadQueue(repoRoot) || { brands: [] };
  const rows = [];
  for (const b of queue.brands || []) {
    const stage = loadBrandStage(repoRoot, b.brandId);
    rows.push({
      brandId: b.brandId,
      brandName: b.brandName,
      parentCompany: b.parentCompany,
      priority: b.priority,
      queueStatus: b.status,
      factoryStatus: stage?.factoryStatus || b.status,
      researchCompletion: stage?.researchCompletion || null,
      tenTabQualityScore: stage?.certification?.tenTabQualityScore ?? null,
      researchGaps: stage?.certification?.gaps || b.researchRequirements || [],
      dataFreshness: stage?.dataFreshness || null,
      verifiedSources: stage?.sources || [],
      candidateHash: stage?.candidateHash || stage?.candidate?.presentationBlocksHashSha256 || null,
      assembledHash: stage?.assembledHash || null,
      compareStudioUrl: `/app#/brand-explorer-compare?id=${b.brandId}&beInternalPreview=1`,
      proposedProductUrl: `/app#/brand-explorer-combined?id=${b.brandId}&beInternalPreview=1&beProfileMode=proposed`,
      expectedAirtableWrites: stage?.writePlan?.mutatingOps ?? null,
      researchSpendUsd: stage?.researchSpend?.totalUsd ?? b.estimatedResearchSpendUsd ?? 0,
      approvalStatus: stage?.approvalStatus || "NOT_APPROVED",
      writebackAuthorized: false,
    });
  }
  const summary = {
    totalQueued: rows.length,
    readyForReview: rows.filter((r) => r.factoryStatus === "READY_FOR_REVIEW").length,
    needsRemediation: rows.filter((r) => r.factoryStatus === "NEEDS_REMEDIATION").length,
    researchInProgress: rows.filter((r) => r.factoryStatus === "RESEARCH_IN_PROGRESS").length,
    approved: rows.filter((r) => r.factoryStatus === "APPROVED").length,
    published: rows.filter((r) => r.factoryStatus === "PUBLISHED").length,
    blocked: rows.filter((r) => r.factoryStatus === "BLOCKED").length,
    skippedProtected: rows.filter((r) => r.factoryStatus === "SKIPPED_PROTECTED").length,
    researchSpendUsdTotal: rows.reduce((n, r) => n + (Number(r.researchSpendUsd) || 0), 0),
    expectedPublicationWritesTotal: rows.reduce(
      (n, r) => n + (Number(r.expectedAirtableWrites) || 0),
      0
    ),
  };
  const doc = {
    contract: BE_FACTORY_2_0_BATCH_V1,
    generatedAt: new Date().toISOString(),
    summary,
    rows,
  };
  writeJson(join(stagingRoot(repoRoot), "portfolio-dashboard.json"), doc);
  writeJson(join(reportsRoot(repoRoot), "portfolio-dashboard.json"), doc);
  return doc;
}

export function saveBatchApprovalRecord(repoRoot, approvalDoc) {
  ensureDirs(repoRoot);
  const id = approvalDoc.approvalId || `batch-${Date.now()}`;
  const path = join(stagingRoot(repoRoot), "approvals", `${id}.json`);
  writeJson(path, { ...approvalDoc, approvalId: id });
  return path;
}
