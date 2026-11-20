/**
 * Factory 2.0 batch orchestrator — research/stage/cert/dry-run per brand.
 * Never writes production Presentation without per-brand founder approval.
 */
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  fetchPresentationBlocksForBrand,
  buildCardinalityAwareWritePlan,
  assembleProposedBlocks,
} from "../be-presentation-writeback-apply-v1.js";
import { hashPresentationBlocks } from "../be-founder-approval-publication-v1.js";
import { evaluateFactoryPermanentStandardsGate } from "../be-factory-permanent-loyalty-region-standards-v1.js";
import {
  BE_FACTORY_2_0_BATCH_V1,
  FACTORY_2_STATUSES,
  PROTECTED_BRAND_IDS,
  AUTOGRAPH_BRAND_ID,
} from "./be-factory-2-contracts-v1.js";
import {
  saveBrandStage,
  loadBrandStage,
  loadQueue,
  saveQueue,
  buildPortfolioDashboard,
} from "./be-factory-2-staging-store-v1.js";

const AUTOGRAPH_CANDIDATE_REL =
  "brand-explorer-phase2e-preview/autograph-collection-phase2h-candidate.json";
const AUTOGRAPH_EXPECTED_OVERLAY =
  "d7510b9b6844e46c66dbf40f06cb14bd85b164983388024a956673d698e90979";

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function assessGaps(blocks) {
  const bySlot = new Map();
  for (const b of blocks || []) {
    const k = String(b.slotKey || "").trim();
    if (!k) continue;
    if (!bySlot.has(k)) bySlot.set(k, []);
    bySlot.get(k).push(b);
  }
  const gaps = [];
  const loyaltyProof = (bySlot.get("loyalty.proof") || []).filter(
    (b) => String(b.body || "").trim().length >= 24
  );
  if (loyaltyProof.length > 0 && loyaltyProof.length !== 6) {
    gaps.push(`loyalty.proof count ${loyaltyProof.length} (require 6)`);
  }
  if (loyaltyProof.length === 0) {
    gaps.push("loyalty.proof missing — static six may render; stage explicit six for publication");
  }
  const commercial = [...bySlot.keys()].filter((k) => k.startsWith("commercial."));
  if (commercial.length < 4) gaps.push("commercial engine thin");
  const econFee = [...bySlot.keys()].filter((k) => /^economics\.fee/.test(k));
  if (econFee.length < 2) gaps.push("economics fee buckets incomplete");
  const gallery = [1, 2, 3, 4, 5, 6].filter((n) => {
    const b = (bySlot.get(`materials.gallery.${n}`) || [])[0];
    return b && (b.imageUrl || /^https?:\/\//i.test(String(b.body || "")));
  });
  if (gallery.length < 6) gaps.push(`gallery ${gallery.length}/6`);
  return gaps;
}

function tenTabScore(blocks, gaps, standardsOk) {
  const n = (blocks || []).length;
  let score = Math.min(100, Math.round(n / 1.7));
  score -= gaps.length * 8;
  if (!standardsOk) score -= 25;
  return Math.max(0, Math.min(100, score));
}

function loadAutographCandidate(repoRoot) {
  const abs = join(repoRoot, "public", AUTOGRAPH_CANDIDATE_REL);
  if (!existsSync(abs)) return null;
  return JSON.parse(readFileSync(abs, "utf8"));
}

/**
 * Process a single brand into durable staging (no production writeback).
 */
export async function processBrand(repoRoot, brandMeta, opts = {}) {
  const brandId = brandMeta.brandId;
  const brandName = brandMeta.brandName || brandId;

  if (PROTECTED_BRAND_IDS.includes(brandId)) {
    const stage = {
      contract: BE_FACTORY_2_0_BATCH_V1,
      brandId,
      brandName,
      factoryStatus: FACTORY_2_STATUSES.SKIPPED_PROTECTED,
      approvalStatus: "NOT_APPLICABLE",
      note: "Protected Curio/Ascend — no reprocessing",
      processedAt: new Date().toISOString(),
      researchSpend: { totalUsd: 0, paidCalls: 0 },
    };
    saveBrandStage(repoRoot, brandId, stage);
    return stage;
  }

  const live = await fetchPresentationBlocksForBrand(brandId, brandName);
  let candidate = null;
  let overlayHash = null;
  let proposed = null;

  if (brandId === AUTOGRAPH_BRAND_ID) {
    candidate = loadAutographCandidate(repoRoot);
    if (candidate) {
      const overlayBlocks =
        (candidate.brandExplorer && candidate.brandExplorer.blocks) || candidate.blocks || [];
      overlayHash = hashPresentationBlocks(overlayBlocks);
      proposed = assembleProposedBlocks(live.blocks, {
        ...candidate,
        contract: "BE_PHASE2H_DEPTH_COMPOSITION_V1",
      });
    }
  }

  // Default path: stage CURRENT as baseline candidate (incremental refresh later).
  // Do not invent fees or loyalty cards — gap report drives remediation.
  if (!proposed) {
    candidate = {
      contract: "BE_FACTORY_2_0_STAGED_CURRENT_BASELINE_V1",
      candidateVersion: "factory-2-0-current-baseline",
      brandId,
      brandName,
      slug: slugify(brandName),
      generatedAt: new Date().toISOString(),
      founderApprovalStatus: "NOT_FOUNDER_APPROVED",
      researchSpendUsd: 0,
      researchNotes: [
        "Factory 2.0 staged CURRENT Presentation as baseline for portfolio review.",
        "Gap-fill research recorded in certification.gaps — no fabricated fees/loyalty cards.",
        "Production Presentation unchanged until exact-candidate founder approval.",
      ],
      brandExplorer: { version: 1, blocks: live.blocks },
    };
    overlayHash = hashPresentationBlocks(live.blocks);
    proposed = {
      blocks: live.blocks,
      contentHash: overlayHash,
      textHash: overlayHash,
      assembly: { summary: { currentBaseline: true, assembledBlockCount: live.blocks.length } },
    };
  }

  const standards = evaluateFactoryPermanentStandardsGate({
    blocks: proposed.blocks,
    loyaltyParticipates: true,
    requireLoyaltyProof: brandId === AUTOGRAPH_BRAND_ID,
  });
  const gaps = assessGaps(proposed.blocks);
  if (!standards.ok) {
    gaps.push(...(standards.loyalty?.reasons || []), ...(standards.region?.badHeadings || []).map(() => "region_acronym"));
  }

  const writePlan = buildCardinalityAwareWritePlan({
    brandId,
    brandName,
    currentBlocks: live.blocks,
    proposedBlocks: proposed.blocks,
  });

  const score = tenTabScore(proposed.blocks, gaps, standards.ok);
  const isAutographExact =
    brandId === AUTOGRAPH_BRAND_ID &&
    overlayHash === AUTOGRAPH_EXPECTED_OVERLAY &&
    Boolean(candidate?.brandExplorer?.blocks?.length);
  const isCurrentBaseline =
    candidate?.contract === "BE_FACTORY_2_0_STAGED_CURRENT_BASELINE_V1";

  let factoryStatus = FACTORY_2_STATUSES.RESEARCH_IN_PROGRESS;
  if (isAutographExact && standards.ok) {
    factoryStatus = FACTORY_2_STATUSES.READY_FOR_REVIEW;
  } else if (brandId === AUTOGRAPH_BRAND_ID) {
    factoryStatus = FACTORY_2_STATUSES.NEEDS_REMEDIATION;
  } else if (isCurrentBaseline) {
    // CURRENT baseline is durable staging only — not Curio-parity READY until
    // Factory candidate research completes permanent standards + gap closure.
    factoryStatus =
      gaps.length > 0 || !standards.ok
        ? FACTORY_2_STATUSES.NEEDS_REMEDIATION
        : FACTORY_2_STATUSES.RESEARCH_IN_PROGRESS;
    if (!gaps.includes("awaiting_factory_candidate_depth")) {
      gaps.push("awaiting_factory_candidate_depth");
    }
    // Re-evaluate after adding depth gap
    factoryStatus = FACTORY_2_STATUSES.NEEDS_REMEDIATION;
  } else if (gaps.length > 0 || !standards.ok) {
    factoryStatus = FACTORY_2_STATUSES.NEEDS_REMEDIATION;
  } else {
    factoryStatus = FACTORY_2_STATUSES.READY_FOR_REVIEW;
  }

  const stage = {
    contract: BE_FACTORY_2_0_BATCH_V1,
    brandId,
    brandName,
    parentCompany: brandMeta.parentCompany || null,
    factoryStatus,
    approvalStatus: "NOT_APPROVED",
    writebackAuthorized: false,
    processedAt: new Date().toISOString(),
    dataFreshness: new Date().toISOString().slice(0, 10),
    researchCompletion: {
      liveBlocks: live.blocks.length,
      proposedBlocks: proposed.blocks.length,
      overlayHash,
      assembledHash: proposed.contentHash,
    },
    candidateHash: overlayHash,
    assembledHash: proposed.contentHash,
    candidate,
    certification: {
      permanentStandards: standards,
      gaps,
      tenTabQualityScore: score,
      browserCert: null,
      localProductParity: null,
      note:
        brandId === AUTOGRAPH_BRAND_ID
          ? "Autograph Factory 1.0 candidate preserved"
          : "CURRENT baseline staged; remediation/gap-fill required before Curio-parity READY",
    },
    writePlan: {
      counts: writePlan.counts,
      mutatingOps:
        writePlan.counts.update + writePlan.counts.create + writePlan.counts.deactivate,
      applied: false,
      airtableWrites: 0,
    },
    researchSpend: { totalUsd: 0, paidCalls: 0, budgetUsd: 0 },
    sources: candidate?.sources || candidate?.researchNotes || [],
  };

  saveBrandStage(repoRoot, brandId, stage);
  return stage;
}

/**
 * Process next N eligible brands from queue (skip protected). Continues on noncritical failures.
 */
export async function processBatch(
  repoRoot,
  { limit = 5, concurrency = 1, force = false, brandIds = null } = {}
) {
  const queue = loadQueue(repoRoot);
  if (!queue?.brands?.length) throw new Error("Factory 2.0 queue missing — run queue build first");

  const eligible = queue.brands.filter(
    (b) =>
      !PROTECTED_BRAND_IDS.includes(b.brandId) &&
      b.status !== FACTORY_2_STATUSES.SKIPPED_PROTECTED &&
      b.status !== FACTORY_2_STATUSES.PUBLISHED
  );

  const batchList = [];

  // Explicit brand id list (force reprocess / targeted batch)
  if (Array.isArray(brandIds) && brandIds.length) {
    for (const id of brandIds) {
      const row = eligible.find((b) => b.brandId === id);
      if (row) batchList.push(row);
    }
  } else {
    // Prefer Autograph + next brands not yet READY/APPROVED (unless force)
    const pending = eligible.filter((b) => {
      if (force) return true;
      const st = loadBrandStage(repoRoot, b.brandId);
      if (!st) return true;
      return ![
        FACTORY_2_STATUSES.READY_FOR_REVIEW,
        FACTORY_2_STATUSES.APPROVED,
        FACTORY_2_STATUSES.PUBLISHED,
      ].includes(st.factoryStatus);
    });

    const auto = eligible.find((b) => b.brandId === AUTOGRAPH_BRAND_ID);
    if (auto) batchList.push(auto);
    for (const b of pending) {
      if (batchList.length >= limit) break;
      if (batchList.some((x) => x.brandId === b.brandId)) continue;
      batchList.push(b);
    }
    if (batchList.length < limit) {
      for (const b of eligible) {
        if (batchList.length >= limit) break;
        if (batchList.some((x) => x.brandId === b.brandId)) continue;
        batchList.push(b);
      }
    }
  }

  const results = [];
  for (const brand of batchList.slice(0, limit)) {
    try {
      const stage = await processBrand(repoRoot, brand, { concurrency });
      // Update queue row status
      const row = queue.brands.find((b) => b.brandId === brand.brandId);
      if (row) row.status = stage.factoryStatus;
      results.push({ ok: true, brandId: brand.brandId, factoryStatus: stage.factoryStatus });
    } catch (err) {
      const stage = {
        contract: BE_FACTORY_2_0_BATCH_V1,
        brandId: brand.brandId,
        brandName: brand.brandName,
        factoryStatus: FACTORY_2_STATUSES.BLOCKED,
        approvalStatus: "NOT_APPROVED",
        error: String(err?.message || err),
        processedAt: new Date().toISOString(),
        researchSpend: { totalUsd: 0, paidCalls: 0 },
      };
      saveBrandStage(repoRoot, brand.brandId, stage);
      const row = queue.brands.find((b) => b.brandId === brand.brandId);
      if (row) row.status = FACTORY_2_STATUSES.BLOCKED;
      results.push({ ok: false, brandId: brand.brandId, error: stage.error });
    }
  }

  queue.updatedAt = new Date().toISOString();
  saveQueue(repoRoot, queue);
  const dashboard = buildPortfolioDashboard(repoRoot);
  return { results, dashboard };
}
