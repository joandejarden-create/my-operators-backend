/**
 * Update Body + Case Summary fields on existing footprint.openings rows (by Title match).
 *
 *   node scripts/patch-footprint-openings-from-fixture.mjs --dry-run --brand-name Radisson
 *   node scripts/patch-footprint-openings-from-fixture.mjs --brand-name Radisson
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const SLOT = "footprint.openings";
const FIXTURE = path.join(ROOT, "fixtures/brand-explorer-presentation-radisson-footprint-openings.json");

function parseArgs(argv) {
  const dryRun = argv.includes("--dry-run");
  let brandName = "";
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--brand-name" && argv[i + 1]) brandName = argv[++i];
  }
  return { dryRun, brandName: brandName.trim() || "Radisson" };
}

function buildPatchFields(r) {
  const fields = {
    Body: r.body ?? "",
    "Case Summary Overview": r.caseSummaryOverview ?? "",
    "Case Summary Owner Objective": r.caseSummaryOwnerObjective ?? "",
    "Case Summary Brand Relevance": r.caseSummaryBrandRelevance ?? "",
    "Case Summary Interpretation": r.caseSummaryInterpretation ?? "",
    "Case Summary Tags": r.caseSummaryTags ?? "",
  };
  return fields;
}

async function main() {
  const { dryRun, brandName } = parseArgs(process.argv);
  const data = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  const byTitle = new Map(
    data.rows.filter((r) => r.slotKey === SLOT).map((r) => [String(r.title).trim(), r])
  );
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const pushAll = (list) => {
    for (const r of list) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };
  try {
    pushAll(
      await base(TABLE)
        .select({
          filterByFormula: `AND({Slot Key} = "${SLOT}", {Brand Name} = "${esc}")`,
          maxRecords: 50,
        })
        .all()
    );
  } catch {
    /* optional Brand Name column */
  }
  pushAll(
    await base(TABLE)
      .select({ filterByFormula: `{Slot Key} = "${SLOT}"`, maxRecords: 50 })
      .all()
  );
  const records = merged.filter((rec) => {
    if (String(rec.get("Slot Key") || "").trim() !== SLOT) return false;
    const title = String(rec.get("Title") || "").trim();
    return byTitle.has(title);
  });
  if (!records.length) {
    console.log(
      `No matching ${SLOT} rows (${merged.length} in base with slot; fixture titles: ${[...byTitle.keys()].join(", ")}).`
    );
    return;
  }
  let updated = 0;
  for (const rec of records) {
    const title = String(rec.get("Title") || "").trim();
    const row = byTitle.get(title);
    if (!row) {
      console.log(`Skip (no fixture): ${title} (${rec.id})`);
      continue;
    }
    const fields = buildPatchFields(row);
    console.log(`${dryRun ? "Would update" : "Updating"} ${title} (${rec.id})`);
    if (!dryRun) await base(TABLE).update(rec.id, fields);
    updated++;
  }
  console.log(`${dryRun ? "Would update" : "Updated"} ${updated} row(s).`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
