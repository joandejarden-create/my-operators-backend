#!/usr/bin/env node
/**
 * Gap-fill Hilton descriptions via city-level location pages.
 *
 *   node scripts/plan-hilton-census-descriptions-gap.mjs
 *   node scripts/plan-hilton-census-descriptions-gap.mjs --open-only
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadBlankHiltonCensusRows,
  planHiltonDescriptionsViaCityPages,
} from "../lib/hotel-census/plan-hilton-city-gap-descriptions.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

async function main() {
  const openOnly = process.argv.includes("--open-only");
  const anyTargetBlank = !process.argv.includes("--description-only");
  const rows = await loadBlankHiltonCensusRows({ openOnly, anyTargetBlank });
  console.log(`=== Hilton city-gap description plan (${openOnly ? "open only" : "all blank"}) ===`);
  console.log("Rows to process:", rows.length);

  const plan = await planHiltonDescriptionsViaCityPages(rows, {
    minNameSim: 0.5,
    fetchDelayMs: 250,
    pageDelayMs: 200,
    onProgress: (msg) => console.log(" ", msg),
  });

  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "hilton-census-descriptions-plan-city-gap.json");
  writeFileSync(
    jsonPath,
    JSON.stringify({ generatedAt: new Date().toISOString(), openOnly, ...plan }, null, 2)
  );

  console.log("\n=== Summary ===");
  console.log("  Ready:", plan.planRows.filter((r) => r.status === "ready").length);
  console.log("  Skipped:", plan.skipped.length);
  console.log("  Fetch errors:", plan.fetchErrors.length);
  console.log("Report:", jsonPath);
  if (process.argv.includes("--apply") && plan.planRows?.filter((r) => r.status === "ready").length) {
    const { spawnSync } = await import("node:child_process");
    spawnSync(process.execPath, ["scripts/apply-hilton-census-descriptions.mjs", "--input", jsonPath], {
      cwd: join(__dirname, ".."),
      stdio: "inherit",
    });
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
