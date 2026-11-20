#!/usr/bin/env node
/**
 * Brand-agnostic Brand Explorer Presentation writeback.
 *
 * Replaces Curio/Ascend-only CLI. Identity comes from --brand-id + optional
 * --candidate / --manifest paths (or staged registry resolution).
 *
 * Dry-run:
 *   node scripts/be-presentation-writeback-v1.mjs --brand-id recEJCTDj1zrsjPM6
 *
 * Apply (requires founder token + env + activation):
 *   node scripts/be-presentation-writeback-v1.mjs --brand-id rec… --apply \
 *     --founder-token FOUNDER_AUTHORIZE_BE_CURIO_ASCEND_PRESENTATION_WRITEBACK_20261120
 *
 * NOTE: Do not reuse Curio/Ascend approval tokens for a new brand. Mint a
 * brand-specific token before production apply of Autograph (or others).
 */
import "../load-env.js";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
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

/** Known brand shortcuts — optional convenience only; --brand-id always works. */
const KNOWN = {
  curio: {
    id: "receQkxgjlezsc1xg",
    name: "Curio Collection by Hilton",
    slug: "curio-collection",
    candidatePath: "brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json",
    manifestPath: "brand-explorer-phase2e-preview/curio-collection-staged-candidate-manifest-v1.json",
  },
  ascend: {
    id: "reclkgOzvAcBheUSo",
    name: "Ascend Hotel Collection",
    slug: "ascend",
    candidatePath: "brand-explorer-phase2e-preview/ascend-phase2h-candidate.json",
    manifestPath: "brand-explorer-phase2e-preview/ascend-staged-candidate-manifest-v1.json",
  },
  autograph: {
    id: "recEJCTDj1zrsjPM6",
    name: "Autograph Collection",
    slug: "autograph-collection",
    candidatePath: "brand-explorer-phase2e-preview/autograph-collection-phase2h-candidate.json",
    manifestPath: "brand-explorer-phase2e-preview/autograph-collection-staged-candidate-manifest-v1.json",
  },
};

function parseArgs(argv) {
  const out = {
    brandId: "",
    brandKey: "",
    brandName: "",
    candidatePath: "",
    manifestPath: "",
    apply: false,
    founderToken: "",
    authorizeActivation: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--authorize-activation") out.authorizeActivation = true;
    else if (a === "--brand" && argv[i + 1]) out.brandKey = String(argv[++i]).toLowerCase();
    else if (a === "--brand-id" && argv[i + 1]) out.brandId = String(argv[++i]).trim();
    else if (a === "--brand-name" && argv[i + 1]) out.brandName = String(argv[++i]).trim();
    else if (a === "--candidate" && argv[i + 1]) out.candidatePath = String(argv[++i]).trim();
    else if (a === "--manifest" && argv[i + 1]) out.manifestPath = String(argv[++i]).trim();
    else if (a === "--founder-token" && argv[i + 1]) out.founderToken = String(argv[++i]);
  }
  return out;
}

function resolveBrand(opts) {
  if (opts.brandKey && KNOWN[opts.brandKey]) {
    const k = KNOWN[opts.brandKey];
    return {
      id: opts.brandId || k.id,
      name: opts.brandName || k.name,
      slug: k.slug,
      candidatePath: opts.candidatePath || k.candidatePath,
      manifestPath: opts.manifestPath || k.manifestPath,
    };
  }
  if (!opts.brandId) {
    throw new Error("Require --brand-id rec… or --brand <curio|ascend|autograph>");
  }
  if (!opts.candidatePath) {
    throw new Error("For arbitrary --brand-id, pass --candidate path under public/");
  }
  return {
    id: opts.brandId,
    name: opts.brandName || opts.brandId,
    slug: String(opts.brandName || opts.brandId)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, ""),
    candidatePath: opts.candidatePath,
    manifestPath: opts.manifestPath || "",
  };
}

function authorizeActivationFile() {
  const path = activationConfigPath(ROOT);
  const cfg = loadActivationConfig(ROOT);
  const next = {
    ...cfg,
    productionWritesAuthorized: true,
    automaticPublicationEnabled: false,
    authorizedBy: "founder-factory-1-0",
    authorizedAt: new Date().toISOString(),
    authorizationNote:
      "Activation for brand-agnostic Presentation writeback. Each brand still requires its own founder token and exact-candidate approval.",
    hardStops: {
      ...(cfg.hardStops || {}),
      presentationTableOnly: true,
      noUnrelatedWorkstreamWrites: true,
      perBrandExactCandidateApprovalRequired: true,
    },
  };
  writeFileSync(path, JSON.stringify(next, null, 2) + "\n");
  return next;
}

async function main() {
  const opts = parseArgs(process.argv);
  if (opts.authorizeActivation) {
    console.log(JSON.stringify({ activation: authorizeActivationFile() }, null, 2));
  }
  const brand = resolveBrand(opts);
  const live = await fetchPresentationBlocksForBrand(brand.id, brand.name);
  const candidate = loadPhase2hCandidate(ROOT, brand.candidatePath);
  const proposed = assembleProposedBlocks(live.blocks, candidate);
  const writePlan = buildCardinalityAwareWritePlan({
    brandId: brand.id,
    brandName: brand.name,
    currentBlocks: live.blocks,
    proposedBlocks: proposed.blocks,
  });

  const report = {
    contract: "BE_PRESENTATION_WRITEBACK_V1",
    generatedAt: new Date().toISOString(),
    mode: opts.apply ? "APPLY" : "DRY_RUN",
    brandId: brand.id,
    brandSlug: brand.slug,
    brandName: brand.name,
    candidatePath: brand.candidatePath,
    candidateVersion: candidate.candidateVersion || null,
    liveBlockCount: live.blocks.length,
    liveContentHash: live.contentHash,
    proposedBlockCount: proposed.blocks.length,
    proposedContentHash: proposed.contentHash,
    proposedTextHash: proposed.textHash,
    assemblySummary: proposed.assembly.summary,
    writePlanCounts: writePlan.counts,
    mutatingOps:
      writePlan.counts.update + writePlan.counts.create + writePlan.counts.deactivate,
    airtableWrites: 0,
    applied: false,
    founderAuthorized: requireFounderWriteToken(opts.founderToken),
    envProductionWrites: String(process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES || "") === "1",
    status: "DRY_RUN",
  };

  if (!opts.apply) {
    report.reportPath = persistWritebackReport(ROOT, report);
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  // New brands must not silently reuse the Curio/Ascend token without explicit env override.
  const brandSpecificOk =
    requireFounderWriteToken(opts.founderToken) &&
    (brand.id === "receQkxgjlezsc1xg" ||
      brand.id === "reclkgOzvAcBheUSo" ||
      String(process.env.BE_ALLOW_LEGACY_WRITEBACK_TOKEN_FOR_NEW_BRAND || "") === "1" ||
      String(opts.founderToken || "").includes(brand.slug.toUpperCase().replace(/-/g, "_")));

  if (!brandSpecificOk) {
    report.status = "BLOCKED_BRAND_SPECIFIC_TOKEN";
    report.error =
      `Mint a brand-specific founder token for ${brand.slug}. ` +
      `Legacy Curio/Ascend token alone is refused for new brands ` +
      `(expected pattern includes brand slug, or set BE_ALLOW_LEGACY_WRITEBACK_TOKEN_FOR_NEW_BRAND=1 only with explicit founder override). ` +
      `Legacy helper: ${expectedFounderToken()}`;
    persistWritebackReport(ROOT, report);
    console.log(JSON.stringify(report, null, 2));
    process.exit(2);
  }

  if (String(process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES || "") !== "1") {
    report.status = "BLOCKED_ENV";
    report.error = "Set BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES=1";
    persistWritebackReport(ROOT, report);
    console.log(JSON.stringify(report, null, 2));
    process.exit(2);
  }
  if (!loadActivationConfig(ROOT).productionWritesAuthorized) {
    report.status = "BLOCKED_ACTIVATION";
    report.error = "Run with --authorize-activation first";
    persistWritebackReport(ROOT, report);
    console.log(JSON.stringify(report, null, 2));
    process.exit(2);
  }

  const applyResult = await applyPresentationWritePlan({
    writePlan,
    dryRun: false,
    brandName: brand.name,
  });
  report.applied = true;
  report.airtableWrites = applyResult.airtableWrites;
  report.linkField = applyResult.linkField;

  const after = await fetchPresentationBlocksForBrand(brand.id, brand.name);
  const afterActive = after.blocks.filter((b) => b.active !== false);
  const afterTextHash = hashPresentationText(afterActive);
  report.readback = {
    blockCount: after.blocks.length,
    activeBlockCount: afterActive.length,
    textHash: afterTextHash,
    matchesProposedTextHash: afterTextHash === proposed.textHash,
  };

  if (brand.manifestPath && existsSync(join(ROOT, "public", brand.manifestPath))) {
    const manAbs = join(ROOT, "public", brand.manifestPath);
    const man = JSON.parse(readFileSync(manAbs, "utf8"));
    man.founderApprovalStatus = "FOUNDER_APPROVED_WRITEBACK";
    man.presentationWritebackAt = report.generatedAt;
    man.presentationWritebackHash = afterTextHash;
    man.writes = (Number(man.writes) || 0) + applyResult.airtableWrites;
    writeFileSync(manAbs, JSON.stringify(man, null, 2) + "\n");
    report.manifestUpdated = brand.manifestPath;
  }

  report.status = report.readback.matchesProposedTextHash ? "APPLIED_VERIFIED" : "APPLIED_HASH_DRIFT";
  report.reportPath = persistWritebackReport(ROOT, report);
  console.log(JSON.stringify(report, null, 2));
  if (report.status === "APPLIED_HASH_DRIFT") process.exit(3);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
