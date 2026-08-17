/**
 * Curio Collection brand pipeline: sync Hilton reference folder → extract facts → fixtures → optional Airtable apply.
 *
 *   node scripts/curio-brand-source-pipeline.mjs
 *   node scripts/curio-brand-source-pipeline.mjs --apply
 *   node scripts/curio-brand-source-pipeline.mjs --apply --skip-airtable
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import "../load-env.js";
import { PILOT_BRANDS } from "../api/lib/partner-intelligence-explorer-field-registry.js";
import { runPartnerBrandExtraction } from "../lib/partner-intelligence/run-extraction.js";
import { writeKimptonExtractionArtifacts } from "../lib/partner-intelligence/apply-brand-extraction-to-airtable.js";
import { resolveReferenceRoot } from "../lib/partner-intelligence/airtable-source.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const SKIP_AIRTABLE = process.argv.includes("--skip-airtable");
const PILOT = PILOT_BRANDS.curioCollection;
const TEMPLATE_FIXTURE = path.join(
  ROOT,
  "fixtures",
  "brand-explorer-presentation-curio-full.json"
);

async function main() {
  const referenceRoot = resolveReferenceRoot();
  console.log("Reference root:", referenceRoot);
  console.log("Brand:", PILOT.brandName, PILOT.recordId);

  const result = await runPartnerBrandExtraction(PILOT.recordId, {
    force: true,
    syncFolder: true,
    autoApprove: true,
  });

  console.log("\nExtraction summary:");
  console.log("  Sources considered:", result.sourcesConsidered);
  console.log("  Facts created:", result.factsCreated);
  console.log("  With values:", result.factsWithValues);
  console.log("  Gaps:", result.gapFacts);
  console.log("  Folder files:", result.folderFileCount);

  for (const run of result.sourceRuns) {
    if (run.error) console.log("  ERROR", run.sourceTitle, "—", run.error);
    else if (run.skipped) console.log("  SKIP", run.sourceTitle, run.reason);
    else console.log("  OK", run.sourceTitle, `(${run.role}, ${run.candidateCount} candidates)`);
  }

  const artifacts = writeKimptonExtractionArtifacts(
    result.merged,
    PILOT,
    TEMPLATE_FIXTURE,
    "curio"
  );
  console.log("\nWrote fixtures:");
  console.log(" ", artifacts.setupPath);
  console.log(" ", artifacts.presentationPath);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    referenceRoot,
    brand: PILOT,
    extraction: {
      runId: result.runId,
      sourcesConsidered: result.sourcesConsidered,
      factsWithValues: result.factsWithValues,
      gapFacts: result.gapFacts,
      sourceRuns: result.sourceRuns,
    },
    mergedSample: result.merged
      .filter((f) => f.dataGap !== "Yes")
      .map((f) => ({
        fieldKey: f.fieldKey,
        value: String(f.extractedValue).slice(0, 200),
        evidence: String(f.evidenceText).slice(0, 160),
        source: f.pageSectionAnchor || f._sourceTitle,
      })),
    artifacts,
  };

  const reportPath = path.join(ROOT, "reports", "curio-brand-source-pipeline.json");
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log("Report:", reportPath);

  if (!APPLY || SKIP_AIRTABLE) {
    console.log(APPLY ? "\n--skip-airtable: fixtures only." : "\nDry run. Re-run with --apply to push to Airtable.");
    return;
  }

  console.log("\nApplying Brand Setup from sources…");
  const seed = spawnSync(
    "node",
    [
      path.join(ROOT, "scripts", "seed-kimpton-brand-setup.mjs"),
      "--apply",
      "--overwrite",
      "--fixture",
      artifacts.setupPath,
    ],
    { stdio: "inherit", cwd: ROOT, env: process.env }
  );
  if (seed.status !== 0) {
    console.error("Brand Setup seed failed — review fixture and Airtable field mapping.");
    process.exit(seed.status || 1);
  }

  console.log("\nApplying Brand Explorer presentation from sources…");
  const pres = spawnSync(
    "node",
    [
      path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs"),
      "--brand-name",
      PILOT.brandName,
      "--fixture",
      artifacts.presentationPath,
      "--replace",
    ],
    { stdio: "inherit", cwd: ROOT, env: process.env }
  );
  if (pres.status !== 0) process.exit(pres.status || 1);

  console.log("\nDone. Review:", PILOT.explorerUrl);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
