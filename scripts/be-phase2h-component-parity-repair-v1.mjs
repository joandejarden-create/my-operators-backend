#!/usr/bin/env node
/**
 * Phase 2H — deepen Curio/Ascend candidates + write substantive scorecards (no Airtable writes).
 * Starts from Phase 2G sanitized candidates, applies evidence composition, never copies baseline bodies.
 */
import { mkdirSync, writeFileSync, readFileSync, existsSync, copyFileSync } from "fs";
import { join } from "path";
import {
  enrichCandidateWithDepth,
  loadEvidencePack,
  inventoryClaims,
} from "../lib/brand-explorer/be-phase2h-depth-composition-v1.js";
import {
  evaluateSubstantiveParity,
  evaluateFounderRegressionCases,
} from "../lib/brand-explorer/be-phase2h-substantive-parity-gates-v1.js";

const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/phase2h-parity");
const preview = join(root, "public/brand-explorer-phase2e-preview");
mkdirSync(outDir, { recursive: true });

function loadJson(rel) {
  return JSON.parse(readFileSync(join(root, rel), "utf8"));
}

function baselineBlocks(snap) {
  const rows = snap?.presentation?.blocks || snap?.presentation?.rows || [];
  return rows
    .map((r, i) => ({
      slotKey: String(r.slotKey || r.fields?.["Slot Key"] || "").trim(),
      title: String(r.title || r.fields?.Title || "").trim(),
      body: String(r.body || r.fields?.Body || "").trim(),
      imageUrl: r.imageUrl || null,
      sort: typeof r.sort === "number" ? r.sort : i + 1,
    }))
    .filter((b) => b.slotKey);
}

const curioBase = loadJson("public/brand-explorer-phase2e-preview/curio-collection-phase2g-candidate.json");
const ascendBase = existsSync(join(preview, "ascend-phase2g-candidate.json"))
  ? loadJson("public/brand-explorer-phase2e-preview/ascend-phase2g-candidate.json")
  : loadJson("public/brand-explorer-phase2e-preview/ascend-phase2f-candidate.json");

const curioPack = loadEvidencePack("reports/brand-explorer/phase2e-quality-curio/01-evidence-pack.json");
const ascendPack = loadEvidencePack("reports/brand-explorer/phase2e-quality-ascend/01-evidence-pack.json");
const baselineSnap = loadJson("public/brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json");
const baseline = baselineBlocks(baselineSnap);

const regionalFromHpc = (pack) => {
  const byRegion = pack?.hpc?.byRegion || pack?.hpc?.metrics?.byRegion || null;
  return byRegion || {};
};

const curioEnriched = enrichCandidateWithDepth(curioBase, curioPack, {
  brandName: "Curio Collection by Hilton",
  parentCompany: "Hilton Worldwide",
  regionalHpc: regionalFromHpc(curioPack),
});
const ascendEnriched = enrichCandidateWithDepth(ascendBase, ascendPack || { evidence: [] }, {
  brandName: "Ascend Hotel Collection",
  parentCompany: "Choice Hotels",
  regionalHpc: regionalFromHpc(ascendPack),
});

const curioParity = evaluateSubstantiveParity({
  baselineBlocks: baseline,
  candidateBlocks: curioEnriched.enriched.brandExplorer.blocks,
});
const ascendParity = evaluateSubstantiveParity({
  baselineBlocks: baseline, // structure benchmark only; Ascend content judged on nonempty + gates
  candidateBlocks: ascendEnriched.enriched.brandExplorer.blocks,
});
const regressions = evaluateFounderRegressionCases(baseline, curioEnriched.enriched.brandExplorer.blocks);

function scorecard(parity, researchGaps, brand) {
  return {
    phase: "2H",
    brand,
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    priorityBlocked: parity.metrics.priorityBlocked,
    priorityPass: parity.priorityPass,
    researchGapCount: researchGaps.length,
    dispositions: parity.metrics.dispositions,
    blockedPriority: parity.blockedPriority,
    note: "Phase 2H automated substantive gates ≠ founder approval. No Airtable writes.",
  };
}

writeFileSync(join(preview, "curio-collection-phase2h-candidate.json"), JSON.stringify(curioEnriched.enriched, null, 2));
writeFileSync(join(preview, "ascend-phase2h-candidate.json"), JSON.stringify(ascendEnriched.enriched, null, 2));
writeFileSync(
  join(preview, "curio-collection-phase2h-scorecard.json"),
  JSON.stringify(scorecard(curioParity, curioEnriched.researchGaps, "curio-collection"), null, 2),
);
writeFileSync(
  join(preview, "ascend-phase2h-scorecard.json"),
  JSON.stringify(scorecard(ascendParity, ascendEnriched.researchGaps, "ascend"), null, 2),
);

// Curio research / parity register for priority subsections
const priorityRegister = curioParity.priorityResults.map((r) => ({
  subsectionId: r.subsectionId,
  label: r.label,
  disposition: r.disposition,
  failures: r.failures,
  existing: {
    claims: r.existing.claimCount,
    words: r.existing.wordCount,
    items: r.existing.nonemptyItemCount,
  },
  candidate: {
    claims: r.candidate.claimCount,
    words: r.candidate.wordCount,
    items: r.candidate.nonemptyItemCount,
  },
  missingInsights: r.missingInsights,
  baselineClaimSamples: (r.existing.claims || []).slice(0, 6).map((c) => c.text?.slice?.(0, 120) || c),
}));

writeFileSync(join(outDir, "01-curio-subsection-parity-register.json"), JSON.stringify(priorityRegister, null, 2));
writeFileSync(join(outDir, "02-curio-research-gaps.json"), JSON.stringify(curioEnriched.researchGaps, null, 2));
writeFileSync(join(outDir, "02-ascend-research-gaps.json"), JSON.stringify(ascendEnriched.researchGaps, null, 2));
writeFileSync(join(outDir, "03-founder-regression-cases.json"), JSON.stringify(regressions, null, 2));
writeFileSync(join(outDir, "04-curio-substantive-parity.json"), JSON.stringify(curioParity, null, 2));
writeFileSync(join(outDir, "04-ascend-substantive-parity.json"), JSON.stringify(ascendParity, null, 2));

const whyBase = baseline.find((b) => b.slotKey === "overview.why_value");
const whyCand = curioEnriched.enriched.brandExplorer.blocks.find((b) => b.slotKey === "overview.why_value");
writeFileSync(
  join(outDir, "05-why-value-claim-inventory.json"),
  JSON.stringify(
    {
      baselineClaims: inventoryClaims(whyBase?.body || ""),
      candidateClaims: inventoryClaims(whyCand?.body || ""),
      note: "Baseline is coverage checklist only — candidate must not copy baseline prose.",
    },
    null,
    2,
  ),
);

const summary = {
  phase: "2H",
  writes: 0,
  paidResearch: 0,
  founderApproved: false,
  curio: scorecard(curioParity, curioEnriched.researchGaps, "curio-collection"),
  ascend: scorecard(ascendParity, ascendEnriched.researchGaps, "ascend"),
  founderRegressionsFailed: regressions.filter((r) => r.failed).map((r) => r.id),
};

writeFileSync(join(outDir, "00-phase2h-summary.json"), JSON.stringify(summary, null, 2));
writeFileSync(
  join(outDir, "00-phase2h-summary.md"),
  `# Phase 2H — component parity + depth recovery

**Founder approval:** NOT claimed (STOP for founder review)

| Brand | Priority blocked | Research gaps | Status |
|---|---:|---:|---|
| Curio | ${curioParity.metrics.priorityBlocked} | ${curioEnriched.researchGaps.length} | NOT_FOUNDER_APPROVED |
| Ascend | ${ascendParity.metrics.priorityBlocked} | ${ascendEnriched.researchGaps.length} | NOT_FOUNDER_APPROVED |

Founder regression failures: ${summary.founderRegressionsFailed.join(", ") || "none"}

Airtable writes: 0 · Paid research: 0
`,
);

// Keep contract copy in preview folder in sync
copyFileSync(
  join(root, "configs/brand-explorer-subsection-parity-contract-v1.json"),
  join(preview, "subsection-parity-contract-v1.json"),
);

console.log(JSON.stringify(summary, null, 2));
