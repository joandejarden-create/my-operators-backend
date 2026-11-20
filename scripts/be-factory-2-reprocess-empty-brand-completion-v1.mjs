#!/usr/bin/env node
/**
 * Re-run Factory 2.0 depth for thin / empty-CURRENT brands after Autograph
 * slot-parity completion lands in the depth candidate builder.
 *
 * $0 paid. No production Presentation writeback.
 * Reprocesses batches 5, 6, and 7 (where empty Presentation brands concentrated),
 * then refreshes master queue + HE compare sync files when present.
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, copyFileSync, existsSync, readFileSync, readdirSync } from "fs";
import { join } from "path";
import {
  listBatch5,
  listBatch6,
  listBatch7,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import { runDepthBatch } from "../lib/brand-explorer/factory-2/be-factory-2-depth-batch-runner-v1.js";
import { reconcileMasterQueue } from "../lib/brand-explorer/factory-2/be-factory-2-master-queue-reconcile-v1.js";
import { measureEmptyBrandCompletion } from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";

const ROOT = process.cwd();
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";

async function runOne(label, packs, batchTag, reportPrefix, candidateVersion) {
  console.error(`\n=== ${label}: ${packs.length} brands ===`);
  return runDepthBatch(ROOT, packs, {
    batchTag,
    reportPrefix,
    candidateVersion,
    contract: `BE_FACTORY_2_0_${batchTag.toUpperCase().replace(/-/g, "_")}_COMPLETION_V1`,
    title: `${label} — empty-brand slot-parity completion`,
  });
}

async function main() {
  const b5 = await runOne(
    "Batch 5",
    listBatch5(),
    "batch-5",
    "41-batch5",
    "factory-2-0-batch-5-v1"
  );
  const b6 = await runOne(
    "Batch 6",
    listBatch6(),
    "batch-6",
    "51-batch6",
    "factory-2-0-batch-6-v1"
  );
  const b7 = await runOne(
    "Batch 7",
    listBatch7(),
    "batch-7",
    "61-batch7",
    "factory-2-0-batch-7-v1"
  );

  try {
    reconcileMasterQueue({ repoRoot: ROOT, writeReports: true });
  } catch (err) {
    console.error("master queue reconcile warning:", err.message || err);
  }

  // Sync registry + compare studio + sample frozen payloads to HE when present
  if (existsSync(HE)) {
    const syncFiles = [
      "public/js/brand-explorer-staged-candidate-registry.js",
      "public/js/brand-explorer-compare-studio.js",
      "public/js/brand-explorer-cumulative-proposal-assembler.js",
    ];
    for (const rel of syncFiles) {
      const src = join(ROOT, rel);
      const dest = join(HE, rel);
      if (existsSync(src) && existsSync(join(HE, "public/js"))) {
        copyFileSync(src, dest);
      }
    }
    const previewSrc = join(ROOT, "public/brand-explorer-phase2e-preview");
    const previewDest = join(HE, "public/brand-explorer-phase2e-preview");
    if (existsSync(previewDest)) {
      for (const f of readdirSync(previewSrc).filter((x) =>
        x.endsWith("-factory2-frozen-approval-payload-v1.json")
      )) {
        copyFileSync(join(previewSrc, f), join(previewDest, f));
      }
      for (const f of readdirSync(previewSrc).filter((x) =>
        x.endsWith("-factory2-depth-candidate.json")
      )) {
        copyFileSync(join(previewSrc, f), join(previewDest, f));
      }
    }
    console.error("Synced frozen payloads + registry to HE workspace");
  }

  const rows = [];
  const preview = join(ROOT, "public/brand-explorer-phase2e-preview");
  for (const f of readdirSync(preview).filter((x) =>
    x.endsWith("-factory2-frozen-approval-payload-v1.json")
  )) {
    const j = JSON.parse(readFileSync(join(preview, f), "utf8"));
    const blocks = j.brandExplorer?.blocks || [];
    const m = measureEmptyBrandCompletion(blocks);
    rows.push({
      slug: j.slug,
      brandName: j.brandName,
      blocks: blocks.length,
      completionPct: m.completionPct,
      galleryWithImage: m.galleryWithImage,
      missingCount: m.missing.length,
      missingSample: m.missing.slice(0, 8),
    });
  }
  rows.sort((a, b) => a.completionPct - b.completionPct);

  const reportDir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(reportDir, { recursive: true });
  const report = {
    contract: "BE_FACTORY_2_EMPTY_BRAND_COMPLETION_REPROCESS_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    productionPresentationWriteback: false,
    batches: {
      batch5: b5?.summary || null,
      batch6: b6?.summary || null,
      batch7: b7?.summary || null,
    },
    frozenCompletionAudit: rows,
    thinStill: rows.filter((r) => r.completionPct < 85),
    note: "Empty-brand Autograph slot-parity completion. Fee amounts still disclosure-gated. Gallery CDN remains READY gap when no fixture pool.",
  };
  writeFileSync(
    join(reportDir, "73-empty-brand-completion-reprocess.json"),
    JSON.stringify(report, null, 2) + "\n"
  );
  writeFileSync(
    join(reportDir, "73-empty-brand-completion-reprocess.md"),
    [
      `# Factory 2.0 — Empty-Brand Slot-Parity Completion`,
      ``,
      `Generated: ${report.generatedAt}`,
      `Paid research: $0 · Production Presentation writes: OFF`,
      ``,
      `## Batch summaries`,
      ``,
      `| Batch | Processed | NEEDS_REMEDIATION | READY | BLOCKED |`,
      `|---|---:|---:|---:|---:|`,
      `| 5 | ${b5?.summary?.processed ?? "—"} | ${b5?.summary?.needsRemediation ?? "—"} | ${b5?.summary?.readyForReview ?? "—"} | ${b5?.summary?.blocked ?? "—"} |`,
      `| 6 | ${b6?.summary?.processed ?? "—"} | ${b6?.summary?.needsRemediation ?? "—"} | ${b6?.summary?.readyForReview ?? "—"} | ${b6?.summary?.blocked ?? "—"} |`,
      `| 7 | ${b7?.summary?.processed ?? "—"} | ${b7?.summary?.needsRemediation ?? "—"} | ${b7?.summary?.readyForReview ?? "—"} | ${b7?.summary?.blocked ?? "—"} |`,
      ``,
      `## Frozen payload completion (lowest first)`,
      ``,
      `| Brand | Blocks | Completion | Gallery CDN | Missing |`,
      `|---|---:|---:|---:|---:|`,
      ...rows.map(
        (r) =>
          `| ${r.brandName} | ${r.blocks} | ${r.completionPct}% | ${r.galleryWithImage}/6 | ${r.missingCount} |`
      ),
      ``,
      `Brands below 85% completion: **${report.thinStill.length}**`,
      ``,
    ].join("\n") + "\n"
  );

  console.error(`\nCompletion audit written. Below 85%: ${report.thinStill.length}`);
  console.log(JSON.stringify({ ok: true, thinStill: report.thinStill.length, rows: rows.length }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
