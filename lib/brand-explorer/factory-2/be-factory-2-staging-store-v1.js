/**
 * Durable Factory 2.0 staging store — survives restarts/merges.
 * Root: data/brand-explorer/factory-2-0/
 */
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  readdirSync,
  renameSync,
  unlinkSync,
  copyFileSync,
} from "fs";
import { join } from "path";
import {
  BE_FACTORY_2_0_BATCH_V1,
  FACTORY_2_STAGING_ROOT_REL,
  FACTORY_2_REPORTS_ROOT_REL,
  FACTORY_2_STATUSES,
  normalizeFactory2Status,
} from "./be-factory-2-contracts-v1.js";

function sleepSync(ms) {
  const start = Date.now();
  while (Date.now() - start < ms) {
    /* brief backoff for antivirus / OneDrive locks */
  }
}

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
  const payload = JSON.stringify(obj, null, 2) + "\n";
  const tmp = `${absPath}.${process.pid}.tmp`;
  let lastErr = null;
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      writeFileSync(tmp, payload);
      try {
        renameSync(tmp, absPath);
      } catch {
        copyFileSync(tmp, absPath);
        try {
          unlinkSync(tmp);
        } catch {
          /* ignore tmp cleanup */
        }
      }
      return absPath;
    } catch (err) {
      lastErr = err;
      sleepSync(50 * attempt);
    }
  }
  // Final direct overwrite attempt
  try {
    writeFileSync(absPath, payload);
    try {
      unlinkSync(tmp);
    } catch {
      /* ignore */
    }
    return absPath;
  } catch {
    throw lastErr;
  }
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

function statusEquals(raw, ...canonical) {
  const n = normalizeFactory2Status(raw);
  return canonical.some((c) => n === c || raw === c);
}

export function buildPortfolioDashboard(repoRoot) {
  const queue = loadQueue(repoRoot) || { brands: [] };
  const rows = [];
  for (const b of queue.brands || []) {
    const stage = loadBrandStage(repoRoot, b.brandId);
    const factoryStatus = normalizeFactory2Status(stage?.factoryStatus || b.status);
    rows.push({
      brandId: b.brandId,
      brandName: b.brandName,
      parentCompany: b.parentCompany,
      priority: b.priority,
      batchId: b.batchId || stage?.batchTag || null,
      batchIds: b.batchIds || [],
      researchClass: b.researchClass || null,
      queueStatus: normalizeFactory2Status(b.status),
      factoryStatus,
      persistence: b.persistence || stage?.persistence || null,
      researchCompletion: stage?.researchCompletion || null,
      tenTabQualityScore: stage?.certification?.tenTabQualityScore ?? null,
      researchGaps: stage?.certification?.gaps || b.researchRequirements || [],
      dataFreshness: stage?.dataFreshness || null,
      verifiedSources: stage?.sources || [],
      candidateHash: stage?.candidateHash || stage?.candidate?.presentationBlocksHashSha256 || b.candidateHash || null,
      assembledHash: stage?.assembledHash || b.assembledHash || null,
      compareStudioUrl: `/app#/brand-explorer-compare?id=${b.brandId}&beInternalPreview=1`,
      proposedProductUrl: `/app#/brand-explorer-combined?id=${b.brandId}&beInternalPreview=1&beProfileMode=proposed`,
      expectedAirtableWrites: stage?.writePlan?.mutatingOps ?? null,
      researchSpendUsd: stage?.researchSpend?.totalUsd ?? b.estimatedResearchSpendUsd ?? 0,
      approvalStatus: stage?.approvalStatus || "NOT_APPROVED",
      writebackAuthorized: false,
    });
  }

  const count = (...preds) =>
    rows.filter((r) => preds.some((p) => statusEquals(r.factoryStatus, p))).length;

  const existingQueued = rows.filter(
    (r) =>
      (r.researchClass === "EXISTING_ACTIVE" || !r.researchClass) &&
      statusEquals(r.factoryStatus, FACTORY_2_STATUSES.QUEUED)
  ).length;
  const existingCanonicalQueued = rows.filter(
    (r) =>
      r.researchClass === "EXISTING_CANONICAL" &&
      statusEquals(
        r.factoryStatus,
        FACTORY_2_STATUSES.QUEUED,
        FACTORY_2_STATUSES.NEEDS_REMEDIATION,
        FACTORY_2_STATUSES.RESEARCHING,
        FACTORY_2_STATUSES.CERTIFIED_READY_FOR_REVIEW
      )
  ).length;
  const newBrandsProposed = rows.filter((r) => r.researchClass === "NEW_BRAND_PROPOSED").length;

  const nextTenScheduled = rows
    .filter(
      (r) =>
        !statusEquals(
          r.factoryStatus,
          FACTORY_2_STATUSES.SKIPPED_PROTECTED,
          FACTORY_2_STATUSES.PUBLISHED,
          FACTORY_2_STATUSES.PERSISTED_VERIFIED
        )
    )
    .slice(0, 10)
    .map((r) => ({
      priority: r.priority,
      brandId: r.brandId,
      brandName: r.brandName,
      factoryStatus: r.factoryStatus,
      batchId: r.batchId,
      researchClass: r.researchClass,
    }));

  const summary = {
    totalBrandsInMasterQueue: rows.length,
    totalQueued: rows.length, // backward-compatible Admin label
    existingBrandsQueued: existingQueued,
    existingCanonicalInQueue: existingCanonicalQueued,
    newBrandsProposed,
    brandsResearching: count(
      FACTORY_2_STATUSES.RESEARCHING,
      FACTORY_2_STATUSES.RESEARCH_IN_PROGRESS
    ),
    brandsCertified: count(
      FACTORY_2_STATUSES.CERTIFIED_READY_FOR_REVIEW,
      FACTORY_2_STATUSES.READY_FOR_REVIEW
    ),
    brandsAwaitingApproval: count(
      FACTORY_2_STATUSES.CERTIFIED_READY_FOR_REVIEW,
      FACTORY_2_STATUSES.READY_FOR_REVIEW
    ),
    brandsPersisted: count(FACTORY_2_STATUSES.PERSISTED_VERIFIED),
    brandsPublished: count(FACTORY_2_STATUSES.PUBLISHED),
    brandsBlocked: count(FACTORY_2_STATUSES.BLOCKED),
    brandsNeedsRemediation: count(FACTORY_2_STATUSES.NEEDS_REMEDIATION),
    // legacy keys
    readyForReview: count(
      FACTORY_2_STATUSES.CERTIFIED_READY_FOR_REVIEW,
      FACTORY_2_STATUSES.READY_FOR_REVIEW
    ),
    needsRemediation: count(FACTORY_2_STATUSES.NEEDS_REMEDIATION),
    researchInProgress: count(
      FACTORY_2_STATUSES.RESEARCHING,
      FACTORY_2_STATUSES.RESEARCH_IN_PROGRESS
    ),
    approved: count(
      FACTORY_2_STATUSES.APPROVED_FOR_WRITEBACK,
      FACTORY_2_STATUSES.APPROVED
    ),
    published: count(FACTORY_2_STATUSES.PUBLISHED),
    blocked: count(FACTORY_2_STATUSES.BLOCKED),
    skippedProtected: count(FACTORY_2_STATUSES.SKIPPED_PROTECTED),
    researchSpendUsdTotal: rows.reduce((n, r) => n + (Number(r.researchSpendUsd) || 0), 0),
    expectedPublicationWritesTotal: rows.reduce(
      (n, r) => n + (Number(r.expectedAirtableWrites) || 0),
      0
    ),
    authorizedBatchCount: (queue.authorizedBatches || []).length || null,
    authorizedBrandCount: (queue.universe && queue.universe.authorizedBrandCount) || null,
  };
  const doc = {
    contract: BE_FACTORY_2_0_BATCH_V1,
    generatedAt: new Date().toISOString(),
    summary,
    nextTenScheduled,
    rows,
  };
  writeJson(join(stagingRoot(repoRoot), "portfolio-dashboard.json"), doc);
  writeJson(join(reportsRoot(repoRoot), "portfolio-dashboard.json"), doc);
  writeJson(join(reportsRoot(repoRoot), "04-portfolio-dashboard.json"), doc);
  return doc;
}

export function saveBatchApprovalRecord(repoRoot, approvalDoc) {
  ensureDirs(repoRoot);
  const id = approvalDoc.approvalId || `batch-${Date.now()}`;
  const path = join(stagingRoot(repoRoot), "approvals", `${id}.json`);
  writeJson(path, { ...approvalDoc, approvalId: id });
  return path;
}
