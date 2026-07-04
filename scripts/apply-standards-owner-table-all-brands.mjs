/**
 * Seed owner-table standards (standards.requirement rows) for every Brand Basics row
 * that has no standards.requirement presentation rows yet.
 *
 *   node scripts/apply-standards-owner-table-all-brands.mjs --dry-run
 *   node scripts/apply-standards-owner-table-all-brands.mjs
 *
 * Uses fixtures/brand-explorer-presentation-standards-owner-table-template.json
 * Env: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE = path.join(
  ROOT,
  "fixtures/brand-explorer-presentation-standards-owner-table-template.json"
);
const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const LINK_FIELDS = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

function getBase() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID.");
  return new Airtable({ apiKey: key }).base(baseId);
}

function linkFieldName(sample) {
  for (const f of LINK_FIELDS) {
    if (sample.fields && sample.fields[f]) return f;
  }
  return LINK_FIELDS[0];
}

async function basicsIdsWithStandardsRequirement(base, linkField) {
  const ids = new Set();
  await base(TABLE)
    .select({ fields: ["Slot Key", linkField], pageSize: 100 })
    .eachPage((records, next) => {
      for (const rec of records) {
        if (String(rec.get("Slot Key") || "").trim() !== "standards.requirement") continue;
        const links = rec.get(linkField);
        if (!Array.isArray(links)) continue;
        links.forEach((id) => ids.add(id));
      }
      next();
    });
  return ids;
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const base = getBase();
  const basicsRows = await base(BASICS).select({ maxRecords: 500 }).all();
  const sample = await base(TABLE).select({ maxRecords: 1 }).firstPage();
  const linkField = sample.length ? linkFieldName(sample[0]) : LINK_FIELDS[0];
  const hasReq = await basicsIdsWithStandardsRequirement(base, linkField);

  const toSeed = [];
  for (const b of basicsRows) {
    const name = String(b.get("Brand Name") || "").trim();
    if (!name) continue;
    if (!hasReq.has(b.id)) toSeed.push({ id: b.id, name });
  }

  console.log(
    `Brands with standards.requirement: ${hasReq.size} · to seed (generic 8-area table): ${toSeed.length}`
  );
  if (!toSeed.length) return;

  for (const brand of toSeed) {
    console.log(`${dryRun ? "[dry-run] " : ""}Seed standards table → ${brand.name} (${brand.id})`);
    if (dryRun) continue;
    const res = spawnSync(
      process.execPath,
      [
        path.join(__dirname, "apply-brand-explorer-presentation-fixture.mjs"),
        "--brand-record-id",
        brand.id,
        "--fixture",
        FIXTURE,
        "--only-missing",
      ],
      { cwd: ROOT, stdio: "inherit", env: process.env }
    );
    if (res.status !== 0) {
      console.error(`Failed for ${brand.name}`);
      process.exit(res.status || 1);
    }
  }
  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
