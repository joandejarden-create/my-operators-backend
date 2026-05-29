/**
 * One presentation row per CHI brand: overview.portfolio_context
 *   Title = ladder tier 0–3
 *   Body  = relative positioning copy
 * Removes legacy overview.portfolio_ladder_tier + overview.relative_positioning rows.
 *
 *   node scripts/apply-choice-portfolio-context-batch.mjs --dry-run
 *   node scripts/apply-choice-portfolio-context-batch.mjs
 */
import fs from "fs";
import os from "os";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import {
  CHI_LADDER_TIER_LABELS,
  portfolioContextForAirtableBrand,
  portfolioPresentationRowsForBrand,
} from "./lib/choice-chi-portfolio-context.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");
const BASICS = "Brand Setup - Brand Basics";
const TABLE = "Brand Setup - Brand Explorer Presentation";
const LEGACY_SLOTS = ["overview.portfolio_ladder_tier", "overview.relative_positioning"];

function parseArgs(argv) {
  const i = argv.indexOf("--brand");
  return {
    dryRun: argv.includes("--dry-run"),
    brandFilter: i >= 0 ? String(argv[i + 1] || "").trim() : "",
  };
}

async function listChiBrands(base) {
  const rows = await base(BASICS).select({ maxRecords: 500 }).all();
  return rows
    .filter((r) => String(r.get("Parent Company") || "").includes("Choice Hotels International"))
    .map((r) => String(r.get("Brand Name") || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

async function deleteLegacyRows(base, brandName, dryRun) {
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  for (const formula of [
    `{Brand Name} = "${esc}"`,
    `{Brand} = "${esc}"`,
  ]) {
    try {
      const rows = await base(TABLE).select({ filterByFormula: formula, maxRecords: 500 }).all();
      for (const r of rows) {
        if (!seen.has(r.id)) {
          seen.add(r.id);
          merged.push(r);
        }
      }
    } catch {
      /* optional */
    }
  }
  const toDrop = merged.filter((r) => LEGACY_SLOTS.includes(String(r.get("Slot Key") || "").trim()));
  if (!toDrop.length) return 0;
  if (!dryRun) {
    for (let i = 0; i < toDrop.length; i += 10) {
      await base(TABLE).destroy(toDrop.slice(i, i + 10).map((r) => r.id));
    }
  }
  return toDrop.length;
}

function runApply({ dryRun, brandName, fixturePath }) {
  const args = [
    APPLY,
    "--brand-name",
    brandName,
    "--fixture",
    fixturePath,
    "--replace-slot-prefix",
    "overview.portfolio_context",
  ];
  if (dryRun) args.push("--dry-run");
  const r = spawnSync(process.execPath, args, { cwd: ROOT, encoding: "utf8" });
  if (r.stdout) process.stdout.write(r.stdout);
  if (r.stderr) process.stderr.write(r.stderr);
  if (r.status !== 0) {
    throw new Error(`apply failed for ${brandName}: exit ${r.status}`);
  }
}

async function main() {
  const { dryRun, brandFilter } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  const base = new Airtable({ apiKey: key }).base(baseId);

  let brands = await listChiBrands(base);
  if (brandFilter) brands = brands.filter((b) => b === brandFilter);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dc-chi-portfolio-"));
  let applied = 0;
  let skipped = 0;
  let legacyDeleted = 0;

  try {
    for (const brandName of brands) {
      const ctx = portfolioContextForAirtableBrand(brandName);
      if (!ctx) {
        console.log(`- ${brandName}: skip (no portfolio map)`);
        skipped++;
        continue;
      }

      const rows = portfolioPresentationRowsForBrand(brandName);
      const fixturePath = path.join(tmpDir, `${brandName.replace(/[^\w.-]+/g, "_")}.json`);
      fs.writeFileSync(
        fixturePath,
        JSON.stringify(
          {
            targetBrandBasicsName: brandName,
            instructions: "CHI portfolio context (one row) — apply-choice-portfolio-context-batch.mjs",
            rows,
          },
          null,
          2
        ),
        "utf8"
      );

      console.log(
        `\n=== ${brandName} (tier ${ctx.ladderTier} · ${CHI_LADDER_TIER_LABELS[ctx.ladderTier]}) ===`
      );
      const dropped = await deleteLegacyRows(base, brandName, dryRun);
      if (dropped) {
        console.log(`  Removed ${dropped} legacy row(s) (portfolio_ladder_tier / relative_positioning)`);
        legacyDeleted += dropped;
      }
      runApply({ dryRun, brandName, fixturePath });
      applied++;
    }
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      /* ignore */
    }
  }

  console.log(
    `\nDone. Brands: ${applied}, skipped: ${skipped}, legacy rows removed: ${legacyDeleted}${dryRun ? " (dry-run)" : ""}.`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
