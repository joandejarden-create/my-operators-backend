#!/usr/bin/env node
/**
 * Evidence pack: cumulative proposal assembler vs raw Phase 2H candidate.
 * No Airtable writes. No content regeneration.
 */
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { buildCumulativeProposedBrand } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const preview = path.join(root, "public", "brand-explorer-phase2e-preview");
const outDir = path.join(
  root,
  "reports",
  "brand-explorer",
  "phase2h-a-renderer-parity",
  "cumulative-proposal-v1",
);

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function hash(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

function blocksOf(brand) {
  return (brand?.brandExplorer?.blocks || brand?.blocks || []).slice();
}

function slotBlocks(blocks, slotKey) {
  return blocks.filter((b) => String(b.slotKey || "") === slotKey);
}

function bulletCount(text) {
  return String(text || "")
    .split(/\n+/)
    .map((l) => l.replace(/^[\s•\-*]+/, "").trim())
    .filter(Boolean).length;
}

fs.mkdirSync(outDir, { recursive: true });

const current = readJson(path.join(preview, "curio-gate1-renderer-parity-payload.json"));
const candidate = readJson(path.join(preview, "curio-collection-phase2h-candidate.json"));
const { brand: proposed, assembly } = buildCumulativeProposedBrand(current, candidate, {
  candidateVersion: candidate.candidateVersion,
});

const currentBlocks = blocksOf(current);
const candidateBlocks = blocksOf(candidate);
const proposedBlocks = blocksOf(proposed);

const checks = {
  keyDifferentiatorsIdentity: {
    slotKey: "overview.differentiators.identity",
    currentBullets: bulletCount(slotBlocks(currentBlocks, "overview.differentiators.identity")[0]?.body),
    candidateBullets: bulletCount(
      slotBlocks(candidateBlocks, "overview.differentiators.identity")[0]?.body,
    ),
    proposedBullets: bulletCount(slotBlocks(proposedBlocks, "overview.differentiators.identity")[0]?.body),
    required: 4,
  },
  keyDifferentiatorsCommercial: {
    slotKey: "overview.differentiators.commercial",
    currentBullets: bulletCount(
      slotBlocks(currentBlocks, "overview.differentiators.commercial")[0]?.body,
    ),
    candidateBullets: bulletCount(
      slotBlocks(candidateBlocks, "overview.differentiators.commercial")[0]?.body,
    ),
    proposedBullets: bulletCount(
      slotBlocks(proposedBlocks, "overview.differentiators.commercial")[0]?.body,
    ),
    required: 4,
  },
  loyaltyProofCards: {
    slotKey: "loyalty.proof",
    currentCards: slotBlocks(currentBlocks, "loyalty.proof").filter((b) => String(b.body || "").trim())
      .length,
    candidateCards: slotBlocks(candidateBlocks, "loyalty.proof").filter((b) =>
      String(b.body || "").trim(),
    ).length,
    proposedCards: slotBlocks(proposedBlocks, "loyalty.proof").filter((b) => String(b.body || "").trim())
      .length,
    proposedTitles: slotBlocks(proposedBlocks, "loyalty.proof").map((b) => b.title),
    required: 6,
  },
};

const pass =
  checks.keyDifferentiatorsIdentity.proposedBullets >= 4 &&
  checks.keyDifferentiatorsCommercial.proposedBullets >= 4 &&
  checks.loyaltyProofCards.proposedCards >= 6 &&
  !slotBlocks(proposedBlocks, "loyalty.proof").some(
    (b) => !String(b.body || "").trim() || /loyalty proof points/i.test(b.title || ""),
  );

const report = {
  contract: "BE_CUMULATIVE_PROPOSAL_EVIDENCE_V1",
  generatedAt: new Date().toISOString(),
  permanentCompareUrl: "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1",
  sources: {
    left: {
      label: "CURRENT · SAVED PROFILE",
      file: "curio-gate1-renderer-parity-payload.json (frozen saved Presentation mirror)",
      blockCount: currentBlocks.length,
      hash: hash(currentBlocks),
    },
    stagedCandidate: {
      label: "RAW PHASE 2H CANDIDATE (not used wholesale)",
      file: "curio-collection-phase2h-candidate.json",
      candidateVersion: candidate.candidateVersion,
      blockCount: candidateBlocks.length,
      hash: hash(candidateBlocks),
      olderFallbackUsed: false,
    },
    right: {
      label: "PROPOSED · CUMULATIVE PUBLICATION PROFILE",
      assembly: assembly.contract,
      blockCount: proposedBlocks.length,
      hash: hash(proposedBlocks),
      provenanceCounts: assembly.provenanceCounts,
      incompleteDraftSlotCount: assembly.incompleteDrafts.length,
      approvalBlockedByRegression: assembly.approvalBlockedByRegression,
    },
  },
  screenshotRegressions: checks,
  incompleteDraftsSample: assembly.incompleteDrafts.slice(0, 30).map((d) => ({
    slotKey: d.slotKey,
    provenance: d.provenance,
    reason: d.reason,
  })),
  outstandingNotReadyToReplace: assembly.incompleteDrafts.map((d) => d.slotKey),
  pass,
  governance: {
    airtableWrites: 0,
    contentRegeneration: false,
    curioAscendApproved: false,
    gate2: "BLOCKED",
  },
};

fs.writeFileSync(path.join(outDir, "00-CUMULATIVE_PROPOSAL_STATUS.md"), `# Cumulative Proposal Assembly — Gate 1 correction

**Status:** ${pass ? "PASS — screenshot regressions repaired in cumulative PROPOSED" : "FAIL"}

## Contract
- LEFT = CURRENT saved profile
- RIGHT = CURRENT baseline + only complete non-regressive staged changes
- Incomplete factory drafts are held out (not publication)

## Key Differentiators
- Identity bullets proposed: **${checks.keyDifferentiatorsIdentity.proposedBullets}** (required 4; raw candidate ${checks.keyDifferentiatorsIdentity.candidateBullets})
- Commercial bullets proposed: **${checks.keyDifferentiatorsCommercial.proposedBullets}** (required 4; raw candidate ${checks.keyDifferentiatorsCommercial.candidateBullets})

## Loyalty Benefits / Program Strengths
- Proposed populated cards: **${checks.loyaltyProofCards.proposedCards}** (required 6; raw candidate nonempty ${checks.loyaltyProofCards.candidateCards})
- Titles: ${checks.loyaltyProofCards.proposedTitles.join(" · ")}

## Provenance counts
\`\`\`json
${JSON.stringify(assembly.provenanceCounts, null, 2)}
\`\`\`

## Hashes
- CURRENT blocks: \`${report.sources.left.hash}\`
- Raw candidate: \`${report.sources.stagedCandidate.hash}\`
- Cumulative PROPOSED: \`${report.sources.right.hash}\`

## Permanent URL
http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1

No Airtable writes. No Gate 2 regeneration. Curio/Ascend not approved.
`);

fs.writeFileSync(path.join(outDir, "cumulative-proposal-report.json"), JSON.stringify(report, null, 2));
fs.writeFileSync(
  path.join(outDir, "incomplete-drafts.json"),
  JSON.stringify(assembly.incompleteDrafts, null, 2),
);

console.log(JSON.stringify({ pass, outDir, checks, provenanceCounts: assembly.provenanceCounts }, null, 2));
process.exit(pass ? 0 : 1);
