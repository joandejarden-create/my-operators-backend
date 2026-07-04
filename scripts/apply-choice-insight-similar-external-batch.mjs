/**
 * Replace incomplete insight.similar rows with external competitor peers.
 * Skips Radisson by Choice and Radisson Blu by Choice (already curated).
 *
 *   node scripts/apply-choice-insight-similar-external-batch.mjs --dry-run
 *   node scripts/apply-choice-insight-similar-external-batch.mjs
 */
import fs from "fs";
import os from "os";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import {
  externalSimilarPeersForBrand,
  insightSimilarRowsNeedFix,
} from "./lib/choice-chi-insight-similar-external.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");
const BASICS = "Brand Setup - Brand Basics";
const TABLE = "Brand Setup - Brand Explorer Presentation";

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

async function similarRowsForBrand(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  for (const formula of [
    `AND({Slot Key} = "insight.similar", {Brand Name} = "${esc}")`,
    `AND({Slot Key} = "insight.similar", {Brand} = "${esc}")`,
  ]) {
    try {
      const rows = await base(TABLE).select({ filterByFormula: formula, maxRecords: 20 }).all();
      for (const r of rows) {
        if (!seen.has(r.id)) {
          seen.add(r.id);
          merged.push(r);
        }
      }
    } catch {
      /* optional fields */
    }
  }
  return merged;
}

function buildFixtureRows(brandName, peers) {
  return peers.map((p) => ({
    slotKey: "insight.similar",
    title: p.title,
    body: p.body,
    sort: p.sort,
  }));
}

async function main() {
  const { dryRun, brandFilter } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  let brands = await listChiBrands(base);
  if (brandFilter) brands = brands.filter((b) => b === brandFilter);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dc-insight-similar-"));
  let updated = 0;
  let skipped = 0;

  try {
    for (const brandName of brands) {
      const peers = externalSimilarPeersForBrand(brandName);
      if (!peers) {
        console.log(`- ${brandName}: skip (reference brand or no peer map)`);
        skipped++;
        continue;
      }

      const existing = await similarRowsForBrand(base, brandName);
      const snapshot = existing.map((r) => ({
        title: r.get("Title"),
        body: r.get("Body"),
      }));
      if (!insightSimilarRowsNeedFix(snapshot)) {
        console.log(`- ${brandName}: ok (${existing.length} external peer row(s))`);
        skipped++;
        continue;
      }

      const fixturePath = path.join(tmpDir, `${brandName.replace(/[^\w.-]+/g, "_")}.json`);
      fs.writeFileSync(
        fixturePath,
        JSON.stringify(
          {
            targetBrandBasicsName: brandName,
            instructions: "External insight.similar peers — apply-choice-insight-similar-external-batch.mjs",
            rows: buildFixtureRows(brandName, peers),
          },
          null,
          2
        ),
        "utf8"
      );

      console.log(`\n=== ${brandName} (${peers.length} peer row(s)) ===`);
      const args = [APPLY, "--brand-name", brandName, "--fixture", fixturePath, "--replace-slot-prefix", "insight.similar"];
      if (dryRun) args.push("--dry-run");

      const res = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit", env: process.env });
      if (res.status === 0) updated++;
      else console.error(`Failed for ${brandName}`);
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }

  console.log(`\n${dryRun ? "Would update" : "Updated"} ${updated} brand(s); skipped ${skipped}.`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
