#!/usr/bin/env node
/**
 * Brand Explorer Factory — semi-automated Choice P1 enrichment toward L2 (Blu parity path).
 *
 *   node scripts/brand-explorer-factory.mjs --queue p1
 *   node scripts/brand-explorer-factory.mjs --brand "Park Plaza by Choice"
 *   node scripts/brand-explorer-factory.mjs --queue p1 --apply --max-iterations 3
 *
 * Default: dry-run (no Airtable writes). QA loop fills missing slots when --apply.
 * Docs: docs/brand-explorer-factory.md
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import "../load-env.js";
import { resolveFactoryTarget, resolveChoiceFactoryStrategy } from "../lib/brand-explorer/brand-explorer-parent-registry.mjs";
import { ensureChoiceFullFixture } from "../lib/brand-explorer/ensure-choice-full-fixture.mjs";
import { runBrandExplorerQaGate } from "../lib/brand-explorer/brand-explorer-qa-gate.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

function parseArgs(argv) {
  const args = argv.slice(2);
  const brandIdx = args.indexOf("--brand");
  const queueIdx = args.indexOf("--queue");
  const maxIdx = args.indexOf("--max-iterations");
  return {
    brand: brandIdx >= 0 ? String(args[brandIdx + 1] || "").trim() : "",
    queue: queueIdx >= 0 ? String(args[queueIdx + 1] || "p1").trim() : "",
    list: args.includes("--list"),
    apply: args.includes("--apply"),
    withImages: args.includes("--with-images"),
    qaOnly: args.includes("--qa-only"),
    maxIterations: maxIdx >= 0 ? Math.max(1, parseInt(args[maxIdx + 1], 10) || 3) : 3,
  };
}

function runNode(scriptRel, extraArgs = [], { optional = false } = {}) {
  const script = path.join(ROOT, scriptRel);
  console.log("\n>>", path.basename(scriptRel), extraArgs.join(" ") || "");
  const r = spawnSync(process.execPath, [script, ...extraArgs], {
    cwd: ROOT,
    stdio: "inherit",
    env: process.env,
  });
  if (r.status !== 0 && !optional) {
    throw new Error(`Command failed: ${scriptRel} (exit ${r.status})`);
  }
  return r.status === 0;
}

/**
 * @param {ReturnType<import('./lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest>[number]} brand
 */
function phaseGenerateTier1(brand) {
  if (brand.isTier1) {
    runNode("scripts/generate-choice-tier1-explorer-full.mjs", ["--brand", brand.profileName]);
  }
  const ensured = ensureChoiceFullFixture(brand);
  console.log(
    ensured.created
      ? `Created full fixture (${ensured.rowCount} rows): ${path.relative(ROOT, ensured.path)}`
      : `Full fixture exists (${ensured.rowCount} rows)`
  );
}

/**
 * @param {ReturnType<import('./lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest>[number]} brand
 * @param {boolean} apply
 * @param {boolean} withImages
 */
function phaseRestoreL2(brand, apply, withImages) {
  if (brand.premium?.applyScript) {
    if (apply) runNode(brand.premium.applyScript);
    else console.log("Would run premium apply:", brand.premium.applyScript);
    return;
  }

  const args = ["--brand", brand.profileName];
  if (!apply) args.push("--dry-run");
  if (withImages) args.push("--with-images");
  if (apply) args.push("--sync-full");
  runNode("scripts/restore-choice-tier1-brand-explorer.mjs", args);
}

/**
 * @param {ReturnType<import('./lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest>[number]} brand
 * @param {boolean} apply
 * @param {{ missingSlotKeys?: string[], shortCounts?: { slotKey: string }[] } | null | undefined} gapAudit
 */
function autoFixGaps(brand, apply, gapAudit) {
  if (!brand.fullFixture) {
    const args = ["--brand", brand.airtableName];
    if (!apply) args.push("--dry-run");
    runNode("scripts/apply-choice-explorer-presentation-gaps-batch.mjs", args);
    return;
  }

  const fixturePath = path.join(ROOT, brand.fullFixture);
  const baseArgs = ["--brand-record-id", brand.recordId, "--fixture", fixturePath];
  if (!apply) baseArgs.push("--dry-run");

  const momentumIssue =
    gapAudit?.missingSlotKeys?.some((k) => k.startsWith("footprint.momentum")) ||
    gapAudit?.shortCounts?.some((s) => s.slotKey.startsWith("footprint.momentum"));

  if (momentumIssue) {
    for (const prefix of ["footprint.momentum", "footprint.momentum_label"]) {
      runNode("scripts/apply-brand-explorer-presentation-fixture.mjs", [
        ...baseArgs,
        "--replace-slot-prefix",
        prefix,
      ]);
    }
  }

  runNode("scripts/apply-brand-explorer-presentation-fixture.mjs", [...baseArgs, "--only-missing"]);
}

/**
 * @param {ReturnType<import('./lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest>[number]} brand
 * @param {{ apply: boolean, withImages: boolean, maxIterations: number, qaOnly: boolean }} opts
 */
async function processChoiceBrand(brand, opts) {
  const strategy = resolveChoiceFactoryStrategy(brand);
  console.log("\n" + "=".repeat(72));
  console.log(`Factory: ${brand.profileName}`);
  console.log(`  Airtable: ${brand.airtableName} (${brand.recordId})`);
  console.log(`  Strategy: ${strategy} | Parity: ${brand.parity}`);
  console.log(`  Mode: ${opts.apply ? "APPLY" : "dry-run"}`);

  /** @type {object[]} */
  const iterations = [];

  if (!opts.qaOnly) {
    if (brand.isTier1) phaseGenerateTier1(brand);
    else ensureChoiceFullFixture(brand);
    // Refresh manifest row after fixture creation
    brand.fullFixture = brand.fullFixture || `fixtures/brand-explorer-presentation-${brand.slug}-full.json`;

    if (opts.apply && !brand.premium?.applyScript) {
      phaseRestoreL2(brand, opts.apply, opts.withImages);
    } else if (!opts.apply) {
      console.log("\n[dry-run] Would run restore L2 path with --sync-full when --apply is set.");
    }
  }

  for (let i = 1; i <= opts.maxIterations; i++) {
    console.log(`\n--- QA iteration ${i}/${opts.maxIterations} ---`);
    const qa = await runBrandExplorerQaGate(brand, { skipFixtureCheck: opts.qaOnly });
    iterations.push({ iteration: i, ...qa });

    console.log(qa.pass ? "QA PASS (L1 gate)" : "QA FAIL");
    for (const b of qa.blockers) console.log("  blocker:", b);
    for (const w of qa.warnings || []) console.log("  warning:", w);

    if (qa.pass) break;

    if (!opts.apply) {
      console.log("  (dry-run — skipping auto-fix; re-run with --apply)");
      break;
    }

    if (qa.gapAudit && !qa.gapAudit.l1Complete) {
      autoFixGaps(brand, true, qa.gapAudit);
    } else {
      break;
    }
  }

  const finalQa = iterations[iterations.length - 1] || (await runBrandExplorerQaGate(brand, { skipFixtureCheck: opts.qaOnly }));
  const report = {
    generatedAt: new Date().toISOString(),
    brand: brand.profileName,
    airtableName: brand.airtableName,
    recordId: brand.recordId,
    strategy,
    apply: opts.apply,
    pass: finalQa.pass,
    l1Complete: finalQa.l1Complete,
    blockers: finalQa.blockers,
    warnings: finalQa.warnings,
    uiUrl: finalQa.uiUrl,
    iterations,
    nextSteps: finalQa.pass
      ? ["Human L2 review in Brand Explorer UI", "Mark Needs Review in FPP when satisfied"]
      : ["Fix blockers manually", "Re-run factory with --apply"],
  };

  fs.mkdirSync(REPORTS, { recursive: true });
  const reportPath = path.join(REPORTS, `brand-explorer-factory-${brand.slug}.json`);
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  console.log(`\nReport: ${reportPath}`);

  return report;
}

async function main() {
  const { brand, queue, list, apply, withImages, maxIterations, qaOnly } = parseArgs(process.argv);

  if (list) {
    runNode("scripts/choice-brand-explorer-pipeline.mjs", ["--list"]);
    console.log("\nP1 queue (Airtable names):");
    for (const name of (await import("./lib/choice-brand-explorer-manifest.mjs")).CHOICE_P1_ENRICHMENT_QUEUE) {
      console.log(" ", name);
    }
    return;
  }

  const targetRef = brand || queue || "p1";
  const target = resolveFactoryTarget(targetRef);
  if (!target.brands.length) {
    console.error("No brands resolved for:", targetRef);
    console.error('Use --brand "Park Plaza by Choice" or --queue p1');
    process.exit(1);
  }

  if (target.nonChi) {
    console.error("Non-CHI brands (Kimpton/Curio) are reference templates only — use Choice --queue p1 for factory runs.");
    process.exit(1);
  }

  console.log("Brand Explorer Factory");
  console.log(`  Queue: ${target.queue || "(single brand)"}`);
  console.log(`  Brands: ${target.brands.length}`);
  console.log(`  Max QA iterations: ${maxIterations}`);

  /** @type {object[]} */
  const summary = [];
  let failed = 0;

  for (const brandRow of target.brands) {
    try {
      const report = await processChoiceBrand(brandRow, { apply, withImages, maxIterations, qaOnly });
      summary.push({ brand: brandRow.profileName, pass: report.pass, report: report.recordId });
      if (!report.pass) failed += 1;
    } catch (err) {
      console.error(`Factory failed for ${brandRow.profileName}:`, err.message || err);
      failed += 1;
      summary.push({ brand: brandRow.profileName, pass: false, error: String(err.message || err) });
    }
  }

  const summaryPath = path.join(REPORTS, "brand-explorer-factory-summary.json");
  fs.writeFileSync(
    summaryPath,
    `${JSON.stringify({ generatedAt: new Date().toISOString(), apply, summary }, null, 2)}\n`,
    "utf8"
  );

  console.log("\n" + "=".repeat(72));
  console.log(`Factory complete: ${summary.length - failed}/${summary.length} passed L1 QA gate`);
  console.log(`Summary: ${summaryPath}`);

  if (failed) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
