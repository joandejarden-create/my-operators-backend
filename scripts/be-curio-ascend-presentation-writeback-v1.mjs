#!/usr/bin/env node
/**
 * Founder-authorized Curio → Ascend Presentation writeback.
 *
 * Dry-run (default):
 *   node scripts/be-curio-ascend-presentation-writeback-v1.mjs --brand curio
 *
 * Apply (requires founder token + BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES=1):
 *   node scripts/be-curio-ascend-presentation-writeback-v1.mjs --brand curio --apply \
 *     --founder-token FOUNDER_AUTHORIZE_BE_CURIO_ASCEND_PRESENTATION_WRITEBACK_20261120
 *
 * Then:
 *   node scripts/be-curio-ascend-presentation-writeback-v1.mjs --brand ascend --apply --founder-token …
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import {
  requireFounderWriteToken,
  expectedFounderToken,
  fetchPresentationBlocksForBrand,
  loadPhase2hCandidate,
  assembleProposedBlocks,
  buildCardinalityAwareWritePlan,
  applyPresentationWritePlan,
  persistWritebackReport,
  hashPresentationText,
} from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";
import {
  loadActivationConfig,
  activationConfigPath,
} from "../lib/brand-explorer/be-founder-approval-publication-v1.js";

const ROOT = process.cwd();

const BRANDS = {
  curio: {
    id: "receQkxgjlezsc1xg",
    slug: "curio",
    name: "Curio Collection by Hilton",
    candidatePath: "brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json",
    manifestPath: "brand-explorer-phase2e-preview/curio-collection-staged-candidate-manifest-v1.json",
  },
  ascend: {
    id: "reclkgOzvAcBheUSo",
    slug: "ascend",
    name: "Ascend Hotel Collection",
    candidatePath: "brand-explorer-phase2e-preview/ascend-phase2h-candidate.json",
    manifestPath: "brand-explorer-phase2e-preview/ascend-staged-candidate-manifest-v1.json",
  },
};

function parseArgs(argv) {
  const out = {
    brand: "curio",
    apply: false,
    founderToken: "",
    authorizeActivation: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--authorize-activation") out.authorizeActivation = true;
    else if (a === "--brand" && argv[i + 1]) out.brand = String(argv[++i]).toLowerCase();
    else if (a === "--founder-token" && argv[i + 1]) out.founderToken = String(argv[++i]);
  }
  return out;
}

function authorizeActivationFile() {
  const path = activationConfigPath(ROOT);
  const cfg = loadActivationConfig(ROOT);
  const next = {
    ...cfg,
    productionWritesAuthorized: true,
    automaticPublicationEnabled: false,
    authorizedBy: "founder-chat-2026-11-20",
    authorizedAt: new Date().toISOString(),
    authorizationNote:
      "Founder explicitly authorized Curio then Ascend Presentation writeback so Current Saved matches cumulative Proposed Update.",
    hardStops: {
      ...(cfg.hardStops || {}),
      curioAscendNotApproved: false,
      presentationTableOnly: true,
      noUnrelatedWorkstreamWrites: true,
    },
  };
  writeFileSync(path, JSON.stringify(next, null, 2) + "\n");
  return next;
}

function sampleOps(operations, n = 12) {
  return operations.slice(0, n).map((o) => ({
    op: o.op,
    slotKey: o.slotKey,
    recordId: o.recordId || null,
    titleAfter: o.after?.title || null,
    bodyAfterPreview: o.after?.body ? String(o.after.body).slice(0, 100) : null,
  }));
}

async function runOne(brandKey, opts) {
  const brand = BRANDS[brandKey];
  if (!brand) throw new Error(`Unknown brand ${brandKey}`);

  const live = await fetchPresentationBlocksForBrand(brand.id, brand.name);
  const candidate = loadPhase2hCandidate(ROOT, brand.candidatePath);
  const proposed = assembleProposedBlocks(live.blocks, candidate);
  const writePlan = buildCardinalityAwareWritePlan({
    brandId: brand.id,
    brandName: brand.name,
    currentBlocks: live.blocks,
    proposedBlocks: proposed.blocks,
  });

  const mutating = writePlan.counts.update + writePlan.counts.create + writePlan.counts.deactivate;
  const report = {
    contract: "BE_CURIO_ASCEND_PRESENTATION_WRITEBACK_V1",
    generatedAt: new Date().toISOString(),
    mode: opts.apply ? "APPLY" : "DRY_RUN",
    brandId: brand.id,
    brandSlug: brand.slug,
    brandName: brand.name,
    candidateVersion: candidate.candidateVersion || "phase2h-component-parity",
    liveBlockCount: live.blocks.length,
    liveContentHash: live.contentHash,
    proposedBlockCount: proposed.blocks.length,
    proposedContentHash: proposed.contentHash,
    assemblySummary: proposed.assembly.summary,
    writePlanCounts: writePlan.counts,
    mutatingOps: mutating,
    sampleOps: sampleOps(writePlan.operations),
    airtableWrites: 0,
    applied: false,
    founderAuthorized: requireFounderWriteToken(opts.founderToken),
    envProductionWrites: String(process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES || "") === "1",
  };

  if (!opts.apply) {
    const path = persistWritebackReport(ROOT, report);
    report.reportPath = path;
    return report;
  }

  if (!requireFounderWriteToken(opts.founderToken)) {
    report.status = "BLOCKED_FOUNDER_TOKEN";
    report.error = `Pass --founder-token ${expectedFounderToken()}`;
    persistWritebackReport(ROOT, report);
    return report;
  }
  if (String(process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES || "") !== "1") {
    report.status = "BLOCKED_ENV";
    report.error = "Set BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES=1 for apply";
    persistWritebackReport(ROOT, report);
    return report;
  }

  const activation = loadActivationConfig(ROOT);
  if (!activation.productionWritesAuthorized) {
    report.status = "BLOCKED_ACTIVATION";
    report.error =
      "configs/brand-explorer-founder-publication-activation-v1.json productionWritesAuthorized=false — re-run with --authorize-activation";
    persistWritebackReport(ROOT, report);
    return report;
  }

  const applyResult = await applyPresentationWritePlan({
    writePlan,
    dryRun: false,
    brandName: brand.name,
  });
  report.applied = true;
  report.airtableWrites = applyResult.airtableWrites;
  report.linkField = applyResult.linkField;
  report.applyResultsCount = applyResult.results.length;

  // Readback
  const after = await fetchPresentationBlocksForBrand(brand.id, brand.name);
  const afterActive = after.blocks.filter((b) => b.active !== false);
  const afterTextHash = hashPresentationText(afterActive);
  report.readback = {
    blockCount: after.blocks.length,
    activeBlockCount: afterActive.length,
    contentHash: after.contentHash,
    textHash: afterTextHash,
    matchesProposedTextHash: afterTextHash === proposed.textHash,
  };

  // Spot-check fee join amounts survived
  const feeJoin = after.blocks.find((b) => b.slotKey === "economics.fee.join");
  report.spotChecks = {
    feeJoinHas85000: feeJoin ? /\$85,000/.test(feeJoin.body || "") : null,
    feeJoinBodyPreview: feeJoin ? String(feeJoin.body || "").slice(0, 160) : null,
    scenario2: after.blocks.find((b) => b.slotKey === "overview.scenario.2")?.title || null,
  };

  // Update staged manifest approval pointer (local file only)
  const manifestAbs = join(ROOT, "public", brand.manifestPath);
  if (existsSync(manifestAbs)) {
    const man = JSON.parse(readFileSync(manifestAbs, "utf8"));
    man.founderApprovalStatus = "FOUNDER_APPROVED_WRITEBACK";
    man.presentationWritebackAt = report.generatedAt;
    man.presentationWritebackHash = after.contentHash;
    man.writes = (Number(man.writes) || 0) + applyResult.airtableWrites;
    man.note = "Founder-authorized cumulative Presentation writeback applied — Current Saved should match Proposed";
    writeFileSync(manifestAbs, JSON.stringify(man, null, 2) + "\n");
    report.manifestUpdated = brand.manifestPath;
  }

  report.status = report.readback.matchesProposedTextHash ? "APPLIED_VERIFIED" : "APPLIED_HASH_DRIFT";
  const path = persistWritebackReport(ROOT, report);
  report.reportPath = path;
  return report;
}

async function main() {
  const opts = parseArgs(process.argv);
  if (opts.authorizeActivation) {
    const cfg = authorizeActivationFile();
    console.log(
      JSON.stringify(
        {
          activationAuthorized: true,
          productionWritesAuthorized: cfg.productionWritesAuthorized,
          authorizedBy: cfg.authorizedBy,
        },
        null,
        2
      )
    );
  }

  if (!BRANDS[opts.brand]) {
    console.error(`Unknown --brand ${opts.brand}. Use curio|ascend`);
    process.exit(2);
  }

  const report = await runOne(opts.brand, opts);
  console.log(JSON.stringify(report, null, 2));
  if (report.status && String(report.status).startsWith("BLOCKED")) process.exit(2);
  if (opts.apply && report.status === "APPLIED_HASH_DRIFT") process.exit(3);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
