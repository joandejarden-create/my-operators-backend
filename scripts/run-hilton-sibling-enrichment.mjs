#!/usr/bin/env node
/**
 * Sibling Hilton: plan+apply directory Website/Property ID for non-Active brands
 * with the largest CALA blank-Website gaps (Garden Inn, Hampton, Tru, …).
 *
 *   node scripts/run-hilton-sibling-enrichment.mjs
 *   node scripts/run-hilton-sibling-enrichment.mjs --apply
 *   node scripts/run-hilton-sibling-enrichment.mjs --brands="Hilton Garden Inn|Hampton by Hilton|Tru by Hilton"
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { join } from "node:path";

const APPLY = process.argv.includes("--apply");
const brandsArg = process.argv.find((a) => a.startsWith("--brands="))?.split("=")[1];
const DEFAULT_BRANDS = [
  "Hilton Garden Inn",
  "Hampton by Hilton",
  "Tru by Hilton",
  "DoubleTree by Hilton",
  "Home2 Suites by Hilton",
  "Homewood Suites by Hilton",
  "Embassy Suites by Hilton",
  "Canopy by Hilton",
  "Spark by Hilton",
  "Waldorf Astoria",
];

const BRANDS = brandsArg
  ? brandsArg.split("|").map((s) => s.trim()).filter(Boolean)
  : DEFAULT_BRANDS;

function run(cmd, args) {
  console.log(`\n$ node ${args.join(" ")}`);
  const r = spawnSync(process.execPath, args, { stdio: "inherit" });
  if (r.status) throw new Error(`Command failed (${r.status}): ${args.join(" ")}`);
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const summary = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    brands: [],
  };

  for (const brand of BRANDS) {
    console.log(`\n======== ${brand} ========`);
    try {
      run("node", [
        "scripts/plan-hilton-census-enrichment.mjs",
        "--brand",
        brand,
        "--min-confidence",
        "medium",
      ]);
      const slug = brand
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "");
      const planPath = join("reports", `hilton-census-enrichment-plan-${slug}.json`);
      let ready = 0;
      if (existsSync(planPath)) {
        const plan = JSON.parse(readFileSync(planPath, "utf8"));
        ready = plan.readyToApply ?? plan.planRows?.filter((r) => Object.keys(r.applyFields || {}).length).length ?? 0;
      }
      const entry = { brand, planPath, readyToApply: ready, applied: false };
      if (APPLY && ready > 0) {
        run("node", ["scripts/apply-hilton-census-enrichment.mjs", "--input", planPath]);
        entry.applied = true;
      }
      summary.brands.push(entry);
    } catch (err) {
      summary.brands.push({ brand, error: String(err?.message || err) });
      console.error(err?.message || err);
    }
  }

  // Descriptions + amenities for siblings with PID blanks
  if (APPLY) {
    try {
      run("node", [
        "scripts/sync-hilton-census-amenities.mjs",
        "--apply",
        "--fill-blank-only",
        "--brand-codes",
        "GI,HP,RU,DT,HT,HW,ES,PY,PE,WA,CH,HI",
      ]);
    } catch (err) {
      summary.amenitiesError = String(err?.message || err);
    }
  }

  writeFileSync("reports/hilton-sibling-enrichment-summary.json", JSON.stringify(summary, null, 2));
  console.log("\n=== Hilton sibling summary ===");
  console.log(JSON.stringify(summary.brands, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
