#!/usr/bin/env node
/**
 * Dry-run Approve & Publish for Curio + Ascend.
 * Confirms gates block publication and airtableWrites=0.
 * Does NOT enable production writes.
 */
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  runFounderApprovalPublication,
  PUBLICATION_MODES,
  loadActivationConfig,
  hashPresentationBlocks,
} from "../lib/brand-explorer/be-founder-approval-publication-v1.js";

const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/comparison-studio/publication-dry-run-v1");
mkdirSync(outDir, { recursive: true });

function loadCandidate(rel) {
  return JSON.parse(readFileSync(join(root, "public", rel), "utf8"));
}

function blocksFrom(overlay) {
  return (overlay.brandExplorer && overlay.brandExplorer.blocks) || [];
}

const brands = [
  {
    id: "receQkxgjlezsc1xg",
    slug: "curio-collection",
    name: "Curio Collection by Hilton",
    candidatePath: "brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json",
    baselinePath: "brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json",
  },
  {
    id: "reclkgOzvAcBheUSo",
    slug: "ascend",
    name: "Ascend Hotel Collection",
    candidatePath: "brand-explorer-phase2e-preview/ascend-phase2h-candidate.json",
    baselinePath: null,
  },
];

const activation = loadActivationConfig(root);
const report = {
  version: "BE_FOUNDER_APPROVAL_PUBLICATION_DRY_RUN_V1",
  generatedAt: new Date().toISOString(),
  productionWritesAuthorized: Boolean(activation.productionWritesAuthorized),
  curioAscend: "NOT_APPROVED",
  gate1: "AWAITING_FOUNDER_APPROVAL",
  gate2: "BLOCKED",
  brands: {},
};

for (const b of brands) {
  const candidate = loadCandidate(b.candidatePath);
  const proposedBlocks = blocksFrom(candidate);
  let currentBlocks = [];
  if (b.baselinePath) {
    const snap = loadCandidate(b.baselinePath);
    const rows =
      (snap.presentation && (snap.presentation.blocks || snap.presentation.rows)) || [];
    currentBlocks = rows.map((r, i) => ({
      recordId: r.recordId || r.id || null,
      slotKey: String(r.slotKey || (r.fields && r.fields["Slot Key"]) || "").trim(),
      title: String(r.title || (r.fields && r.fields.Title) || "").trim(),
      body: String(r.body || (r.fields && r.fields.Body) || "").trim(),
      sort: typeof r.sort === "number" ? r.sort : i + 1,
      imageUrl: r.imageUrl || null,
    }));
  }

  const result = runFounderApprovalPublication({
    root,
    mode: PUBLICATION_MODES.DRY_RUN,
    brandId: b.id,
    brandSlug: b.slug,
    brandName: b.name,
    candidateVersion: candidate.candidateVersion || "phase2h-component-parity",
    currentBlocks,
    proposedBlocks,
    approveAllChanged: true,
    brandDecision: { decision: "approve_and_publish" },
    gate1: { founderApproved: false },
    gate2: { status: "BLOCKED", founderApproved: false },
    scorecard: {
      founderApprovalStatus: candidate.founderApprovalStatus || "NOT_FOUNDER_APPROVED",
      priorityBlocked: 0,
      researchGapCount: Array.isArray(candidate.researchGaps)
        ? candidate.researchGaps.length
        : candidate.researchGaps || 0,
    },
    approver: "dry-run-script",
    persistSnapshot: true,
    persistAudit: true,
  });

  report.brands[b.slug] = {
    brandId: b.id,
    status: result.status,
    contentHash: result.contentHash,
    airtableWrites: result.airtableWrites,
    publicationApplied: result.publicationApplied,
    writePlanSummary: result.writePlanSummary,
    gateFailures: (result.steps.find((s) => s.step === "VALIDATE_GATES") || {}).failures || [],
    auditRelativePath: result.auditRelativePath,
    baselineHash: hashPresentationBlocks(currentBlocks),
    proposedBlockCount: proposedBlocks.length,
  };
}

const allBlocked = Object.values(report.brands).every(
  (b) => b.status === "BLOCKED_GATES" && b.airtableWrites === 0 && b.publicationApplied === false
);
report.ok = allBlocked && report.productionWritesAuthorized === false;

writeFileSync(join(outDir, "01-dry-run-report.json"), JSON.stringify(report, null, 2));
writeFileSync(
  join(outDir, "00-FOUNDER_PUBLICATION_STATUS.md"),
  `# Brand Explorer — Approve & Publish (dry-run)

**Production writes authorized:** ${report.productionWritesAuthorized}  
**Curio / Ascend:** NOT APPROVED  
**Gate 1:** AWAITING_FOUNDER_APPROVAL  
**Gate 2:** BLOCKED  
**Airtable writes in this run:** 0

## Workflow

The Comparison Studio **Approve & Publish** action runs:

1. Lock candidate + immutable content hash  
2. Validate Gate 1 / Gate 2 / founder approval / content gates  
3. Build exact Presentation write plan (before/after)  
4. Verify Brand Basics identity + concurrent modification  
5. Create recoverable snapshot  
6. Apply writes (**gated off**)  
7. Staged publication boundary (no public exposure)  
8. Readback verify plan  
9. Promote to customer (**gated off**)  
10. Post-publish verify plan  
11. Record audit  
12. Rollback path from snapshot  

Activation file: \`configs/brand-explorer-founder-publication-activation-v1.json\`  
(\`productionWritesAuthorized: false\`)

## Dry-run results

${Object.entries(report.brands)
  .map(
    ([slug, b]) =>
      `### ${slug}\n- status: **${b.status}**\n- hash: \`${b.contentHash}\`\n- writes: ${b.airtableWrites}\n- plan: ${JSON.stringify(b.writePlanSummary)}\n`
  )
  .join("\n")}

Ordinary approved publications will use this path once gates pass and the founder separately authorizes production publishing. No separate manual Airtable editing is required for ordinary approved publications after activation.
`
);

console.log(JSON.stringify({ ok: report.ok, outDir, brands: report.brands }, null, 2));
if (!report.ok) process.exit(2);
