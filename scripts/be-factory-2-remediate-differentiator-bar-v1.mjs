#!/usr/bin/env node
/**
 * Remediate Factory 2.0 Key Differentiators to Curio/Ascend bar
 * (exactly four newline bullets on Experience & Identity and Commercial & Distribution).
 *
 * Reprocesses all authorized depth packs (batches 5–9 + named five / 3 / 4).
 * $0 paid. No production Presentation writeback.
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, copyFileSync, existsSync, readdirSync, readFileSync } from "fs";
import { join } from "path";
import {
  listNamedBatchFive,
  listBatch3,
  listBatch4,
  listBatch5,
  listBatch6,
  listBatch7,
  listBatch8,
  listBatch9,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import { runDepthBatch } from "../lib/brand-explorer/factory-2/be-factory-2-depth-batch-runner-v1.js";
import { reconcileMasterQueue } from "../lib/brand-explorer/factory-2/be-factory-2-master-queue-reconcile-v1.js";
import {
  measureEmptyBrandCompletion,
  DIFFERENTIATOR_MIN_BULLETS,
  countPresentationBullets,
} from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import { buildDepthGapFillBlocks } from "../lib/brand-explorer/factory-2/be-factory-2-depth-candidate-builder-v1.js";

const ROOT = process.cwd();
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";

async function runOne(label, packs, batchTag, reportPrefix, candidateVersion) {
  if (!packs.length) return null;
  console.error(`\n=== ${label}: ${packs.length} brands ===`);
  return runDepthBatch(ROOT, packs, {
    batchTag,
    reportPrefix,
    candidateVersion,
    contract: `BE_FACTORY_2_0_${String(batchTag).toUpperCase().replace(/-/g, "_")}_DIFF_BAR_V1`,
    title: `${label} — Key Differentiator four-bullet bar remediation`,
  });
}

async function main() {
  // Quick preflight: builder must emit 4/4 before expensive Airtable reprocess
  for (const pack of [...listBatch6(), ...listBatch8(), ...listBatch9()].slice(0, 3)) {
    const blocks = buildDepthGapFillBlocks(pack);
    const m = measureEmptyBrandCompletion(blocks);
    if (!m.differentiatorBarPass) {
      console.error("Builder preflight FAILED", pack.slug, m);
      process.exit(2);
    }
  }

  await runOne("Named batch five", listNamedBatchFive(), "named-batch-five", "13-named-batch-five", "factory-2-0-named-batch-five-v1");
  await runOne("Batch 3", listBatch3(), "batch-3", "21-batch3", "factory-2-0-batch-3-v1");
  await runOne("Batch 4", listBatch4(), "batch-4", "31-batch4", "factory-2-0-batch-4-v1");
  await runOne("Batch 5", listBatch5(), "batch-5", "41-batch5", "factory-2-0-batch-5-v1");
  await runOne("Batch 6", listBatch6(), "batch-6", "51-batch6", "factory-2-0-batch-6-v1");
  await runOne("Batch 7", listBatch7(), "batch-7", "61-batch7", "factory-2-0-batch-7-v1");
  await runOne("Batch 8", listBatch8(), "batch-8", "81-batch8", "factory-2-0-batch-8-v1");
  await runOne("Batch 9", listBatch9(), "batch-9", "91-batch9", "factory-2-0-batch-9-v1");

  try {
    reconcileMasterQueue({ repoRoot: ROOT, writeReports: true });
  } catch (err) {
    console.error("master queue reconcile warning:", err.message || err);
  }

  if (existsSync(HE)) {
    for (const rel of [
      "public/js/brand-explorer-staged-candidate-registry.js",
      "public/js/brand-explorer-compare-studio.js",
      "public/js/brand-explorer-cumulative-proposal-assembler.js",
    ]) {
      const src = join(ROOT, rel);
      const dest = join(HE, rel);
      if (existsSync(src) && existsSync(join(HE, "public/js"))) copyFileSync(src, dest);
    }
    const previewSrc = join(ROOT, "public/brand-explorer-phase2e-preview");
    const previewDest = join(HE, "public/brand-explorer-phase2e-preview");
    if (existsSync(previewDest)) {
      for (const f of readdirSync(previewSrc).filter(
        (x) =>
          x.endsWith("-factory2-frozen-approval-payload-v1.json") ||
          x.endsWith("-factory2-depth-candidate.json")
      )) {
        copyFileSync(join(previewSrc, f), join(previewDest, f));
      }
    }
    console.error("Synced remediated frozen payloads to HE 8080 workspace");
  }

  const preview = join(ROOT, "public/brand-explorer-phase2e-preview");
  const rows = [];
  for (const f of readdirSync(preview).filter((x) =>
    x.endsWith("-factory2-frozen-approval-payload-v1.json")
  )) {
    const j = JSON.parse(readFileSync(join(preview, f), "utf8"));
    const blocks = j.brandExplorer?.blocks || [];
    const id = blocks.find((b) => b.slotKey === "overview.differentiators.identity");
    const com = blocks.find((b) => b.slotKey === "overview.differentiators.commercial");
    const ib = countPresentationBullets(id?.body);
    const cb = countPresentationBullets(com?.body);
    rows.push({
      slug: j.slug || f,
      brand: j.brandName,
      batchTag: j.batchTag || null,
      identityBullets: ib,
      commercialBullets: cb,
      pass: ib >= DIFFERENTIATOR_MIN_BULLETS && cb >= DIFFERENTIATOR_MIN_BULLETS,
    });
  }
  const fail = rows.filter((r) => !r.pass);
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const report = {
    generatedAt: new Date().toISOString(),
    contract: "BE_FACTORY_2_0_DIFFERENTIATOR_BAR_REMEDIATION_V1",
    minBullets: DIFFERENTIATOR_MIN_BULLETS,
    summary: {
      audited: rows.length,
      pass: rows.filter((r) => r.pass).length,
      fail: fail.length,
    },
    fail,
    rows,
  };
  writeFileSync(join(dir, "93-differentiator-bar-remediation.json"), JSON.stringify(report, null, 2) + "\n");
  writeFileSync(
    join(dir, "93-differentiator-bar-remediation.md"),
    [
      `# Factory 2.0 — Key Differentiator Four-Bullet Bar Remediation`,
      ``,
      `Generated: ${report.generatedAt}`,
      `Standard: Curio / Ascend / WoodSpring — **${DIFFERENTIATOR_MIN_BULLETS} bullets** on Experience & Identity and Commercial & Distribution.`,
      ``,
      `| Metric | Value |`,
      `|---|---:|`,
      `| Audited frozen payloads | ${report.summary.audited} |`,
      `| PASS | ${report.summary.pass} |`,
      `| FAIL | ${report.summary.fail} |`,
      ``,
      fail.length
        ? `## Remaining failures\n\n` +
          fail.map((r) => `- ${r.brand} (\`${r.slug}\`) id=${r.identityBullets} com=${r.commercialBullets}`).join("\n")
        : `All audited Factory frozen payloads meet the four-bullet differentiator bar.`,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(report.summary, null, 2));
  if (fail.length) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
