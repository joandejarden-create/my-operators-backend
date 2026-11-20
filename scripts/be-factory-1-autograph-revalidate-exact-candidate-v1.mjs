#!/usr/bin/env node
/**
 * Re-validate Autograph exact overlay candidate hash (no writeback).
 * Expected overlay: d7510b9b6844e46c66dbf40f06cb14bd85b164983388024a956673d698e90979
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import {
  fetchPresentationBlocksForBrand,
  buildCardinalityAwareWritePlan,
  assembleProposedBlocks,
  loadPhase2hCandidate,
  persistWritebackReport,
} from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";
import { hashPresentationBlocks } from "../lib/brand-explorer/be-founder-approval-publication-v1.js";
import { evaluateFactoryPermanentStandardsGate } from "../lib/brand-explorer/be-factory-permanent-loyalty-region-standards-v1.js";

const ROOT = process.cwd();
const BRAND_ID = "recEJCTDj1zrsjPM6";
const BRAND_NAME = "Autograph Collection";
const EXPECTED_OVERLAY =
  "d7510b9b6844e46c66dbf40f06cb14bd85b164983388024a956673d698e90979";
const CANDIDATE_REL =
  "brand-explorer-phase2e-preview/autograph-collection-phase2h-candidate.json";
const OUT = join(ROOT, "reports/brand-explorer/factory-1-0/autograph-exact-candidate-revalidate");

async function main() {
  mkdirSync(OUT, { recursive: true });
  const candidate = loadPhase2hCandidate(ROOT, CANDIDATE_REL);
  const overlayBlocks =
    (candidate.brandExplorer && candidate.brandExplorer.blocks) || candidate.blocks || [];
  const overlayHash = hashPresentationBlocks(overlayBlocks);
  if (overlayHash !== EXPECTED_OVERLAY) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          status: "HASH_MISMATCH",
          expected: EXPECTED_OVERLAY,
          actual: overlayHash,
        },
        null,
        2
      )
    );
    process.exit(3);
  }

  const live = await fetchPresentationBlocksForBrand(BRAND_ID, BRAND_NAME);
  // Force assemble from overlay candidate (not frozen payload) for this exact hash identity.
  const proposed = assembleProposedBlocks(live.blocks, {
    ...candidate,
    contract: "BE_PHASE2H_DEPTH_COMPOSITION_V1",
  });
  const standards = evaluateFactoryPermanentStandardsGate({
    blocks: proposed.blocks,
    loyaltyParticipates: true,
    requireLoyaltyProof: true,
  });
  const writePlan = buildCardinalityAwareWritePlan({
    brandId: BRAND_ID,
    brandName: BRAND_NAME,
    currentBlocks: live.blocks,
    proposedBlocks: proposed.blocks,
  });

  const report = {
    contract: "BE_FACTORY_1_0_AUTOGRAPH_EXACT_CANDIDATE_REVALIDATE_V1",
    generatedAt: new Date().toISOString(),
    brandId: BRAND_ID,
    brandName: BRAND_NAME,
    exactCandidateOverlayHash: EXPECTED_OVERLAY,
    overlayHashVerified: overlayHash === EXPECTED_OVERLAY,
    assembledContentHash: proposed.contentHash,
    assembledTextHash: proposed.textHash,
    liveBlockCount: live.blocks.length,
    overlayBlockCount: overlayBlocks.length,
    assembledBlockCount: proposed.blocks.length,
    assemblySummary: proposed.assembly?.summary || null,
    permanentStandards: standards,
    writePlanCounts: writePlan.counts,
    mutatingOps:
      writePlan.counts.update + writePlan.counts.create + writePlan.counts.deactivate,
    airtableWrites: 0,
    applied: false,
    curioAscendUntouched: true,
    protectedBrandIds: ["receQkxgjlezsc1xg", "reclkgOzvAcBheUSo"],
    writebackAuthorized: false,
    status:
      overlayHash === EXPECTED_OVERLAY && standards.ok
        ? "VALIDATED_AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL"
        : "BLOCKED",
    note: "Do not reuse prior frozen approval hashes. Founder must approve this exact overlay + resulting assembled hash for writeback.",
  };

  const path = join(OUT, "REVALIDATE.json");
  writeFileSync(path, JSON.stringify(report, null, 2));
  persistWritebackReport(ROOT, {
    contract: "BE_PRESENTATION_WRITEBACK_V1",
    mode: "DRY_RUN",
    brandId: BRAND_ID,
    brandSlug: "autograph-collection",
    brandName: BRAND_NAME,
    candidatePath: CANDIDATE_REL,
    candidateVersion: candidate.candidateVersion || null,
    exactCandidateOverlayHash: EXPECTED_OVERLAY,
    proposedContentHash: proposed.contentHash,
    writePlanCounts: writePlan.counts,
    airtableWrites: 0,
    applied: false,
    status: "DRY_RUN_EXACT_OVERLAY",
  });
  console.log(JSON.stringify(report, null, 2));
  process.exit(report.status.startsWith("VALIDATED") ? 0 : 2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
