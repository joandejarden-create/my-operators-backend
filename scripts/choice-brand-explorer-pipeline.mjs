/**
 * Repeatable Choice Brand Explorer pipeline — one brand at a time toward Radisson Blu parity.
 *
 *   node scripts/choice-brand-explorer-pipeline.mjs --brand "Comfort Inn & Suites"
 *   node scripts/choice-brand-explorer-pipeline.mjs --brand "Radisson Blu (Choice)" --apply
 *   node scripts/choice-brand-explorer-pipeline.mjs --list
 *
 * Phases (default: all except apply):
 *   sources → generate → apply-basics → apply-presentation → cala → audit
 *
 *   --phase sources|generate|apply-basics|apply-presentation|cala|audit|qa-loop
 *   --apply          run Airtable writes (default is dry-run for apply phases)
 *   --skip-images    pass through to CALA footprint batch
 */
import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  listChoiceBrandManifest,
  printManifestSummary,
  resolveChoiceBrandManifest,
} from "./lib/choice-brand-explorer-manifest.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseArgs(argv) {
  const args = argv.slice(2);
  const brandIdx = args.indexOf("--brand");
  const phaseIdx = args.indexOf("--phase");
  return {
    brand: brandIdx >= 0 ? String(args[brandIdx + 1] || "").trim() : "",
    phase: phaseIdx >= 0 ? String(args[phaseIdx + 1] || "").trim() : "",
    list: args.includes("--list"),
    apply: args.includes("--apply"),
    skipImages: args.includes("--skip-images"),
    maxIterations: args.includes("--max-iterations")
      ? Math.max(1, parseInt(args[args.indexOf("--max-iterations") + 1], 10) || 3)
      : 3,
  };
}

function runNode(scriptRel, extraArgs = [], { optional = false } = {}) {
  const script = path.join(ROOT, scriptRel);
  const args = [script, ...extraArgs];
  console.log("\n>>", path.basename(scriptRel), extraArgs.join(" ") || "");
  const r = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit", env: process.env });
  if (r.status !== 0 && !optional) {
    console.error(`Failed: ${scriptRel}`);
    process.exit(r.status ?? 1);
  }
  return r.status === 0;
}

function printSourcesChecklist(brand) {
  const root = process.env.PARTNER_REFERENCE_ROOT || "G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material\\";
  console.log("\n=== Phase: sources (manual + CLI) ===");
  console.log("Reference root:", root);
  console.log("Brand folder:", `${root}${brand.referenceFolder}`);
  console.log("");
  console.log("Checklist:");
  console.log("  [ ] development/ — brand brochure, one-pager, pitch deck");
  console.log("  [ ] fdd/ — U.S. FDD PDF + extracted text (see docs/choice-fdd-inventory.md)");
  console.log("  [ ] regional/ — CALA / Mexico materials if available");
  console.log("  [ ] press/ — media center press kit");
  console.log("");
  console.log("Repo text extractions (if missing, run crawls):");
  console.log(`  fixtures/choice-dev-site-text/our-brands__*__${brand.slug.replace(/-choice$/, "")}.txt`);
  console.log(`  fixtures/choice-media-center-text/*${brand.airtableName.split(" ")[0]}*.txt`);
  console.log("");
  console.log("CLI helpers:");
  console.log('  npm run partner-reference:init-folder -- --company "Choice Hotels International" --brand "' + brand.airtableName + '"');
  console.log('  npm run partner-reference:download -- --url "<pdf-url>" --company "Choice Hotels International" --brand "' + brand.airtableName + '" --type development-brochure --apply --register');
  if (brand.premium?.referenceDoc) {
    console.log("  Reference doc:", brand.premium.referenceDoc);
  }
}

function phaseGenerate(brand) {
  console.log("\n=== Phase: generate fixtures ===");
  if (brand.premium?.buildScript) {
    runNode(brand.premium.buildScript);
    return;
  }
  if (brand.isTier1) {
    runNode("scripts/generate-choice-tier1-explorer-full.mjs", ["--brand", brand.profileName]);
    return;
  }
  console.log(
    "No Tier 1 profile or premium build script for",
    brand.profileName,
    "— create split fixtures (see docs/choice-brand-explorer-completion-runbook.md § Premium path)."
  );
}

function phaseApplyBasics(brand, apply) {
  console.log("\n=== Phase: apply-basics ===");
  const fixture = path.join(ROOT, "fixtures", "brand-basics-from-choice-materials", `${brand.slug}.json`);
  const alt = path.join(ROOT, "fixtures", `brand-basics-${brand.slug}.json`);
  const hasDedicated = [fixture, alt].some((p) => fs.existsSync(p));
  if (!hasDedicated) {
    console.log("No dedicated Brand Basics fixture — run full batch when ready:");
    console.log("  npm run apply-choice-brand-basics-batch" + (apply ? "" : " -- --dry-run"));
    return;
  }
  console.log("Dedicated basics fixture found — apply via brand-specific script or batch.");
}

function phaseApplyPresentation(brand, apply) {
  console.log("\n=== Phase: apply-presentation ===");
  if (!apply) {
    console.log("Dry run — re-run with --apply to push to Airtable.");
  }

  if (brand.premium?.applyScript) {
    if (apply) runNode(brand.premium.applyScript);
    else console.log("Would run:", brand.premium.applyScript);
    return;
  }

  if (!brand.fullFixture) {
    console.error("No full fixture at fixtures/brand-explorer-presentation-" + brand.slug + "-full.json");
    console.error("Run generate phase first.");
    process.exit(1);
  }

  const applyArgs = [
    "--brand-record-id",
    brand.recordId,
    "--fixture",
    brand.fullFixture,
    apply ? "--only-missing" : "--dry-run",
  ];
  if (apply) runNode("scripts/apply-brand-explorer-presentation-fixture.mjs", applyArgs);
  else console.log("Would run apply-brand-explorer-presentation-fixture.mjs", applyArgs.join(" "));
}

function phaseCala(brand, apply, skipImages) {
  console.log("\n=== Phase: cala footprint + momentum ===");
  const dry = apply ? [] : ["--dry-run"];
  const brandArg = ["--brand", brand.profileName];
  const img = skipImages ? ["--skip-images"] : [];
  if (apply) {
    runNode("scripts/apply-choice-cala-footprint-openings-batch.mjs", [...brandArg, ...img]);
    runNode("scripts/apply-choice-footprint-momentum-from-openings-batch.mjs", brandArg);
    runNode("scripts/patch-choice-case-study-from-openings.mjs", brandArg, { optional: true });
  } else {
    runNode("scripts/apply-choice-cala-footprint-openings-batch.mjs", [...dry, ...brandArg, ...img]);
  }
}

function phaseAudit(brand) {
  console.log("\n=== Phase: audit ===");
  runNode("scripts/audit-choice-explorer-presentation-gaps.mjs", ["--brand", brand.profileName]);
  console.log("\nQA: open Brand Explorer UI:");
  console.log("  /brand-explorer-combined.html?id=" + encodeURIComponent(brand.airtableName));
}

const PHASES = ["sources", "generate", "apply-basics", "apply-presentation", "cala", "audit", "qa-loop"];

async function phaseQaLoop(brand, apply, maxIterations = 3) {
  console.log("\n=== Phase: qa-loop ===");
  const { runBrandExplorerQaGate } = await import("../lib/brand-explorer/brand-explorer-qa-gate.mjs");

  for (let i = 1; i <= maxIterations; i++) {
    const qa = await runBrandExplorerQaGate(brand);
    console.log(`Iteration ${i}: ${qa.pass ? "PASS" : "FAIL"}`);
    for (const b of qa.blockers) console.log("  blocker:", b);

    if (qa.pass) return;

    if (!apply) {
      console.log("Dry-run — use --apply to auto-fix gaps via apply-choice-explorer-presentation-gaps-batch");
      return;
    }

    if (qa.gapAudit && !qa.gapAudit.l1Complete) {
      runNode("scripts/apply-choice-explorer-presentation-gaps-batch.mjs", ["--brand", brand.airtableName]);
    } else {
      return;
    }
  }
}

async function main() {
  const { brand: brandRef, phase, list, apply, skipImages, maxIterations } = parseArgs(process.argv);

  if (list) {
    printManifestSummary();
    return;
  }

  if (!brandRef) {
    console.error("Usage: node scripts/choice-brand-explorer-pipeline.mjs --brand \"Comfort Inn & Suites\" [--apply] [--phase cala]");
    console.error("       node scripts/choice-brand-explorer-pipeline.mjs --list");
    process.exit(1);
  }

  const brand = resolveChoiceBrandManifest(brandRef);
  if (!brand) {
    console.error("Unknown Choice brand:", brandRef);
    console.error("Run with --list to see manifest.");
    process.exit(1);
  }

  console.log("Choice Brand Explorer pipeline");
  console.log("  Profile:", brand.profileName);
  console.log("  Airtable:", brand.airtableName, brand.recordId);
  console.log("  Strategy:", brand.applyStrategy, "| Parity:", brand.parity);
  console.log("  Apply mode:", apply ? "LIVE" : "dry-run (add --apply for Airtable writes)");

  const run = phase ? [phase] : PHASES.filter((p) => p !== "qa-loop");
  for (const p of run) {
    if (!PHASES.includes(p)) {
      console.error("Unknown phase:", p);
      process.exit(1);
    }
    if (p === "sources") printSourcesChecklist(brand);
    else if (p === "generate") phaseGenerate(brand);
    else if (p === "apply-basics") phaseApplyBasics(brand, apply);
    else if (p === "apply-presentation") phaseApplyPresentation(brand, apply);
    else if (p === "cala") phaseCala(brand, apply, skipImages);
    else if (p === "audit") phaseAudit(brand);
    else if (p === "qa-loop") await phaseQaLoop(brand, apply, maxIterations);
  }

  console.log("\nPipeline finished for", brand.profileName);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
