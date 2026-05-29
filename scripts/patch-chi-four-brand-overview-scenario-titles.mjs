/**
 * Patch overview.scenario.{1,2,3} Title only for four CHI brands.
 * Leaves Body, Images, and all other fields untouched.
 *
 * Usage:
 *   node scripts/patch-chi-four-brand-overview-scenario-titles.mjs --dry-run
 *   node scripts/patch-chi-four-brand-overview-scenario-titles.mjs
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BRANDS = [
  "Park Plaza by Choice",
  "Radisson Collection by Choice",
  "Radisson Inn & Suites",
  "WoodSpring Suites",
];

const TITLE_MAP = {
  "Park Plaza by Choice": [
    "Upscale Full-Service Conversion",
    "Gateway Capital Growth",
    "Radisson-Family Tier Alignment",
  ],
  "Radisson Collection by Choice": [
    "Collection-Grade Repositioning",
    "CALA Luxury Gateway Growth",
    "Curated Portfolio Standardization",
  ],
  "Radisson Inn & Suites": [
    "Upper-Midscale Corridor Conversion",
    "Andean / Central America Growth",
    "Regional Tier Standardization",
  ],
  "WoodSpring Suites": [
    "Extended-Stay Corridor Conversion",
    "U.S. / Canada Weekly-Demand Growth",
    "Extended-Portfolio Fee Discipline",
  ],
};

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

async function selectScenarioRows(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(TABLE)
    .select({
      filterByFormula: `AND(OR({Brand Name} = "${esc}", {Brand} = "${esc}"), OR({Slot Key} = "overview.scenario.1", {Slot Key} = "overview.scenario.2", {Slot Key} = "overview.scenario.3"))`,
      maxRecords: 20,
    })
    .all();
  return rows;
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  const base = new Airtable({ apiKey: key }).base(baseId);

  let updated = 0;
  for (const brand of BRANDS) {
    const wanted = TITLE_MAP[brand];
    const rows = await selectScenarioRows(base, brand);
    const bySlot = new Map(rows.map((r) => [String(r.get("Slot Key") || "").trim(), r]));
    console.log(`\n${brand}`);
    for (let i = 1; i <= 3; i++) {
      const slot = `overview.scenario.${i}`;
      const rec = bySlot.get(slot);
      if (!rec) {
        console.log(`  ${slot}: missing`);
        continue;
      }
      const before = String(rec.get("Title") || "").trim();
      const after = wanted[i - 1];
      if (before === after) {
        console.log(`  ${slot}: unchanged`);
        continue;
      }
      console.log(`  ${slot}: "${before}" -> "${after}"`);
      if (!dryRun) await base(TABLE).update(rec.id, { Title: after });
      updated++;
    }
  }
  console.log(`\n${dryRun ? "Would update" : "Updated"} ${updated} title row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
