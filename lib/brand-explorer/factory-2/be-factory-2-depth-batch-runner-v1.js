/**
 * Shared Factory 2.0 depth-batch runner (no production writeback).
 */
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  fetchPresentationBlocksForBrand,
  buildCardinalityAwareWritePlan,
} from "../be-presentation-writeback-apply-v1.js";
import { buildDepthCandidate } from "./be-factory-2-depth-candidate-builder-v1.js";
import {
  FACTORY_2_STATUSES,
  BE_FACTORY_2_0_BATCH_V1,
  AUTOGRAPH_BRAND_ID,
  PROTECTED_BRAND_IDS,
} from "./be-factory-2-contracts-v1.js";
import {
  saveBrandStage,
  loadBrandStage,
  loadQueue,
  saveQueue,
  buildPortfolioDashboard,
  writeJson,
  stagingRoot,
} from "./be-factory-2-staging-store-v1.js";

function assessGaps(proposedBlocks, standards, incompleteDrafts) {
  const gaps = [];
  const bySlot = new Map();
  for (const b of proposedBlocks || []) {
    const k = String(b.slotKey || "").trim();
    if (!k) continue;
    if (!bySlot.has(k)) bySlot.set(k, []);
    bySlot.get(k).push(b);
  }
  const loyaltyProof = (bySlot.get("loyalty.proof") || []).filter(
    (b) => String(b.body || "").trim().length >= 24
  );
  if (loyaltyProof.length !== 6) gaps.push(`loyalty.proof count ${loyaltyProof.length} (require 6)`);
  if (![...bySlot.keys()].some((k) => k.startsWith("commercial."))) {
    gaps.push("commercial engine missing");
  }
  const feeSlots = [...bySlot.keys()].filter((k) => /^economics\.fee/.test(k));
  if (feeSlots.length < 2) gaps.push("economics fee buckets incomplete");
  gaps.push("fee_amounts_unverified_pending_disclosure_exhibit");
  if (!standards?.ok) {
    gaps.push(...(standards.loyalty?.reasons || []));
    if (standards.region?.badHeadings?.length) gaps.push("region_acronym_casing");
  }
  const realIncomplete = (incompleteDrafts || []).filter(
    (d) => Array.isArray(d.candidateBlocks) && d.candidateBlocks.length > 0
  );
  if (realIncomplete.length) gaps.push(`incomplete_drafts:${realIncomplete.length}`);
  const gallery = [1, 2, 3, 4, 5, 6].filter((n) => {
    const b = (bySlot.get(`materials.gallery.${n}`) || [])[0];
    return b && (b.imageUrl || /^https?:\/\//i.test(String(b.body || "")));
  });
  if (gallery.length < 6) gaps.push(`gallery ${gallery.length}/6`);
  const galleryWithVerifiedImage = [1, 2, 3, 4, 5, 6].filter((n) => {
    const b = (bySlot.get(`materials.gallery.${n}`) || [])[0];
    return b && b.imageUrl && /^https?:\/\//i.test(String(b.imageUrl));
  });
  if (galleryWithVerifiedImage.length < 6) {
    gaps.push(`gallery_images_pending_verified_cdn:${galleryWithVerifiedImage.length}/6`);
  }
  return gaps;
}

function tenTabScore(blocks, gaps, standardsOk) {
  let score = Math.min(100, Math.round((blocks || []).length / 1.5));
  score -= gaps.length * 6;
  if (!standardsOk) score -= 20;
  return Math.max(0, Math.min(100, score));
}

function subsectionCoverage(blocks) {
  const tabs = {
    overview: 0,
    commercial: 0,
    loyalty: 0,
    economics: 0,
    operations: 0,
    footprint: 0,
    materials: 0,
    insight: 0,
    standards: 0,
    valueOwners: 0,
  };
  for (const b of blocks || []) {
    const k = String(b.slotKey || "");
    const pref = k.split(".")[0];
    if (pref in tabs) tabs[pref] += 1;
  }
  const covered = Object.values(tabs).filter((n) => n > 0).length;
  return { tabs, coveredTabs: covered, tenTabCoveragePct: Math.round((covered / 10) * 100) };
}

export async function processDepthBrand(repoRoot, pack, opts = {}) {
  const previewDir = join(repoRoot, "public/brand-explorer-phase2e-preview");
  mkdirSync(previewDir, { recursive: true });
  const batchTag = opts.batchTag || pack.batchTag || "depth-batch";
  const candidateVersion = opts.candidateVersion || `factory-2-0-${batchTag}`;

  const liveName = pack.liveBrandName || pack.brandName;
  const live = await fetchPresentationBlocksForBrand(pack.brandId, liveName);
  const built = buildDepthCandidate(pack, live.blocks);

  const candidateFile = `${pack.slug}-factory2-depth-candidate.json`;
  writeFileSync(join(previewDir, candidateFile), JSON.stringify(built.candidate, null, 2) + "\n");

  const frozen = {
    contract: "BE_FACTORY_2_0_FROZEN_APPROVAL_PAYLOAD_V1",
    brandId: pack.brandId,
    brandName: pack.brandName,
    slug: pack.slug,
    generatedAt: new Date().toISOString(),
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    approvalContentHash: built.assembledHash,
    overlayHash: built.overlayHash,
    brandExplorer: { version: 1, blocks: built.proposedBlocks },
    researchSpendUsd: 0,
    sources: pack.sources,
    batchTag,
  };
  const frozenFile = `${pack.slug}-factory2-frozen-approval-payload-v1.json`;
  writeFileSync(join(previewDir, frozenFile), JSON.stringify(frozen, null, 2) + "\n");

  const writePlan = buildCardinalityAwareWritePlan({
    brandId: pack.brandId,
    brandName: pack.brandName,
    currentBlocks: live.blocks,
    proposedBlocks: built.proposedBlocks,
  });

  const gaps = assessGaps(
    built.proposedBlocks,
    built.standards,
    built.assembly.incompleteDrafts
  );
  const coverage = subsectionCoverage(built.proposedBlocks);
  const score = tenTabScore(built.proposedBlocks, gaps, built.standards.ok);
  const factoryStatus = FACTORY_2_STATUSES.NEEDS_REMEDIATION;

  const stage = {
    contract: BE_FACTORY_2_0_BATCH_V1,
    brandId: pack.brandId,
    brandName: pack.brandName,
    parentCompany: pack.parentCompany,
    factoryStatus,
    approvalStatus: "NOT_APPROVED",
    writebackAuthorized: false,
    processedAt: new Date().toISOString(),
    dataFreshness: new Date().toISOString().slice(0, 10),
    batchTag,
    researchCompletion: {
      liveBlocks: live.blocks.length,
      overlayBlocks: built.overlayBlocks.length,
      proposedBlocks: built.proposedBlocks.length,
      overlayHash: built.overlayHash,
      assembledHash: built.assembledHash,
      tenTabCoveragePct: coverage.tenTabCoveragePct,
      coveredTabs: coverage.coveredTabs,
    },
    candidateHash: built.overlayHash,
    assembledHash: built.assembledHash,
    candidate: built.candidate,
    certification: {
      permanentStandards: built.standards,
      gaps,
      tenTabQualityScore: score,
      subsectionCoverage: coverage,
      incompleteDrafts: (built.assembly.incompleteDrafts || [])
        .filter((d) => Array.isArray(d.candidateBlocks) && d.candidateBlocks.length > 0)
        .map((d) => ({
          slotKey: d.slotKey,
          provenance: d.provenance,
          reason: d.reason,
        })),
      browserCert: null,
      localProductParity: null,
      imagesVerified: coverage.tabs.materials >= 6,
      note: "Depth candidate staged; fee amounts await verified disclosure exhibits before READY_FOR_APPROVAL",
    },
    writePlan: {
      counts: writePlan.counts,
      mutatingOps:
        writePlan.counts.update + writePlan.counts.create + writePlan.counts.deactivate,
      applied: false,
      airtableWrites: 0,
      dryRunOpsSample: (writePlan.operations || []).slice(0, 12),
    },
    researchSpend: { totalUsd: 0, paidCalls: 0, budgetUsd: 0 },
    sources: Object.entries(pack.sources || {}).map(([k, v]) => ({ key: k, value: v })),
    preview: {
      overlayUrl: `/brand-explorer-phase2e-preview/${candidateFile}`,
      frozenUrl: `/brand-explorer-phase2e-preview/${frozenFile}`,
      compareStudioUrl: `/app#/brand-explorer-compare?id=${pack.brandId}&beInternalPreview=1`,
      proposedProductUrl: `/app#/brand-explorer-combined?id=${pack.brandId}&beInternalPreview=1&beProfileMode=proposed`,
    },
  };

  saveBrandStage(repoRoot, pack.brandId, stage);
  writeJson(join(stagingRoot(repoRoot), "brands", pack.brandId, "write-plan-full-dry-run.json"), {
    counts: writePlan.counts,
    operations: writePlan.operations,
  });

  return {
    ok: true,
    brandId: pack.brandId,
    brandName: pack.brandName,
    slug: pack.slug,
    factoryStatus,
    candidateHash: built.overlayHash,
    assembledHash: built.assembledHash,
    score,
    gaps,
    coverage,
    writePlanCounts: writePlan.counts,
    mutatingOps: stage.writePlan.mutatingOps,
    researchSpendUsd: 0,
    preview: stage.preview,
    standardsOk: built.standards.ok,
    candidateVersion,
  };
}

export function appendRegistryEntries(repoRoot, results, packsById) {
  const registryPath = join(repoRoot, "public/js/brand-explorer-staged-candidate-registry.js");
  let src = readFileSync(registryPath, "utf8");
  // Prefer the CANDIDATES array terminator immediately before helper functions.
  const markers = ["\n  ];\n\n  function nz(s)", "\n  ];\r\n\r\n  function nz(s)"];
  const marker = markers.find((m) => src.includes(m));
  if (!marker) {
    return { ok: false, error: "registry_marker_missing", added: [] };
  }
  const added = [];
  for (const r of results) {
    if (!r.ok) continue;
    if (src.includes(`id: '${r.brandId}'`)) continue;
    const pack = packsById.get(r.brandId);
    if (!pack) continue;
    const entry = `,
    {
      id: '${pack.brandId}',
      slug: '${pack.slug}',
      name: '${pack.brandName}',
      overlayUrl: '/brand-explorer-phase2e-preview/${pack.slug}-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: '${r.candidateVersion || "factory-2-0-depth"}',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '${r.assembledHash}'
    }`;
    src = src.replace(marker, entry + marker);
    added.push(pack.slug);
  }
  writeFileSync(registryPath, src);
  return { ok: true, added };
}

export function appendRegistryNameResolvers(repoRoot, resolvers) {
  const registryPath = join(repoRoot, "public/js/brand-explorer-staged-candidate-registry.js");
  let src = readFileSync(registryPath, "utf8");
  for (const { testSnippet, insertBefore } of resolvers) {
    if (src.includes(testSnippet)) continue;
    if (!src.includes(insertBefore)) continue;
    src = src.replace(insertBefore, `${testSnippet}\n    ${insertBefore}`);
  }
  writeFileSync(registryPath, src);
}

export async function runDepthBatch(repoRoot, packs, opts = {}) {
  const reportDir = join(repoRoot, "reports/brand-explorer/factory-2-0");
  mkdirSync(reportDir, { recursive: true });
  const batchTag = opts.batchTag || "depth-batch";
  const reportPrefix = opts.reportPrefix || "batch";
  const candidateVersion = opts.candidateVersion || `factory-2-0-${batchTag}`;

  const auto = loadBrandStage(repoRoot, AUTOGRAPH_BRAND_ID);
  const preservation = {
    autographPreserved: Boolean(auto),
    autographStatus: auto?.factoryStatus || null,
    autographHash: auto?.candidateHash || null,
    protectedIds: [...PROTECTED_BRAND_IDS],
    priorBatchStagesPreserved: true,
    note: `Curio/Ascend protected; Autograph + prior batch stages untouched by ${batchTag}`,
  };
  writeJson(join(reportDir, `${reportPrefix}-preservation.json`), preservation);

  const results = [];
  for (const pack of packs) {
    try {
      console.error(`Processing ${pack.brandName}…`);
      const r = await processDepthBrand(repoRoot, pack, { batchTag, candidateVersion });
      results.push(r);
      console.error(`  → ${r.factoryStatus} score=${r.score} gaps=${r.gaps.length}`);
    } catch (err) {
      results.push({
        ok: false,
        brandId: pack.brandId,
        brandName: pack.brandName,
        factoryStatus: FACTORY_2_STATUSES.BLOCKED,
        error: String(err?.message || err),
        researchSpendUsd: 0,
      });
      console.error(`  → BLOCKED ${err?.message || err}`);
      saveBrandStage(repoRoot, pack.brandId, {
        contract: BE_FACTORY_2_0_BATCH_V1,
        brandId: pack.brandId,
        brandName: pack.brandName,
        factoryStatus: FACTORY_2_STATUSES.BLOCKED,
        approvalStatus: "NOT_APPROVED",
        error: String(err?.message || err),
        processedAt: new Date().toISOString(),
        researchSpend: { totalUsd: 0, paidCalls: 0 },
        batchTag,
      });
    }
  }

  const packsById = new Map(packs.map((p) => [p.brandId, p]));
  const registry = appendRegistryEntries(repoRoot, results, packsById);

  const queue = loadQueue(repoRoot);
  if (queue?.brands) {
    for (const r of results) {
      const row = queue.brands.find((b) => b.brandId === r.brandId);
      if (row) row.status = r.factoryStatus;
    }
    queue.updatedAt = new Date().toISOString();
    saveQueue(repoRoot, queue);
  }

  const dashboard = buildPortfolioDashboard(repoRoot);
  const consolidated = {
    contract: opts.contract || "BE_FACTORY_2_0_DEPTH_BATCH_REVIEW_V1",
    batchTag,
    generatedAt: new Date().toISOString(),
    authorization: {
      researchStagingDryRun: true,
      productionPresentationWriteback: false,
      customerPublication: false,
      paidResearchBudgetUsd: 0,
    },
    preservation,
    registry,
    batchResults: results,
    summary: {
      processed: results.length,
      ok: results.filter((r) => r.ok).length,
      readyForReview: results.filter((r) => r.factoryStatus === "READY_FOR_REVIEW").length,
      needsRemediation: results.filter((r) => r.factoryStatus === "NEEDS_REMEDIATION").length,
      blocked: results.filter((r) => r.factoryStatus === "BLOCKED").length,
      researchSpendUsdTotal: results.reduce((n, r) => n + (r.researchSpendUsd || 0), 0),
      expectedPublicationWritesTotal: results.reduce((n, r) => n + (r.mutatingOps || 0), 0),
    },
    portfolioSummary: dashboard.summary,
    founderReviewRows: results.map((r) => ({
      brand: r.brandName,
      brandId: r.brandId,
      researchCompletion: r.ok
        ? `${r.coverage?.coveredTabs || "?"}/10 tabs · score ${r.score}`
        : "BLOCKED",
      tenTabCoverage: r.coverage || null,
      qualityStatus: r.factoryStatus,
      sourcesVerified: r.ok,
      imagesVerified: r.ok ? Boolean(r.coverage?.tabs?.materials >= 6) : false,
      structuredStaging: r.ok ? "DURABLE_STAGED" : "FAILED",
      candidateHash: r.candidateHash || null,
      assembledHash: r.assembledHash || null,
      compareStudioUrl: r.preview?.compareStudioUrl || null,
      proposedProductUrl: r.preview?.proposedProductUrl || null,
      expectedAirtableWrites: r.writePlanCounts || null,
      researchCostsUsd: r.researchSpendUsd || 0,
      remainingGaps: r.gaps || [r.error],
      approvalDisposition:
        r.factoryStatus === "READY_FOR_REVIEW" ? "READY_FOR_APPROVAL" : r.factoryStatus,
    })),
  };

  writeJson(join(reportDir, `${reportPrefix}-consolidated-review.json`), consolidated);
  writeJson(join(reportDir, "04-portfolio-dashboard.json"), dashboard);

  const md = [
    `# Factory 2.0 — ${opts.title || batchTag} Consolidated Review`,
    ``,
    `**Generated:** ${consolidated.generatedAt}`,
    `**Production Presentation writes:** OFF`,
    `**Paid research:** $0`,
    ``,
    `## Preservation`,
    `- Autograph status: \`${preservation.autographStatus}\``,
    `- Curio/Ascend: SKIPPED_PROTECTED`,
    `- Prior batch stages: preserved`,
    ``,
    `## Batch summary`,
    `| Metric | Value |`,
    `|---|---:|`,
    `| Processed | ${consolidated.summary.processed} |`,
    `| NEEDS_REMEDIATION | ${consolidated.summary.needsRemediation} |`,
    `| READY_FOR_REVIEW | ${consolidated.summary.readyForReview} |`,
    `| BLOCKED | ${consolidated.summary.blocked} |`,
    `| Research spend USD | ${consolidated.summary.researchSpendUsdTotal} |`,
    `| Expected writes (if later approved) | ${consolidated.summary.expectedPublicationWritesTotal} |`,
    ``,
    `## Brands`,
    ``,
  ];
  for (const row of consolidated.founderReviewRows) {
    md.push(`### ${row.brand} (\`${row.brandId}\`)`);
    md.push(`- Disposition: **${row.approvalDisposition}**`);
    md.push(`- Research: ${row.researchCompletion}`);
    md.push(`- Candidate hash: \`${row.candidateHash || "—"}\``);
    md.push(`- Assembled hash: \`${row.assembledHash || "—"}\``);
    md.push(`- Compare: ${row.compareStudioUrl || "—"}`);
    md.push(`- Proposed: ${row.proposedProductUrl || "—"}`);
    md.push(`- Expected writes: ${JSON.stringify(row.expectedAirtableWrites)}`);
    md.push(`- Gaps: ${(row.remainingGaps || []).join("; ") || "—"}`);
    md.push(``);
  }
  md.push(`## Next`);
  md.push(`1. Attach verified fee exhibits → re-run depth builder → promote to READY_FOR_APPROVAL.`);
  md.push(`2. Founder portfolio approval binds per-brand candidate hashes (no generic token).`);
  md.push(`3. Production writeback only after exact approval + activation path.`);
  writeFileSync(join(reportDir, `${reportPrefix}-consolidated-review.md`), md.join("\n") + "\n");

  return consolidated;
}
