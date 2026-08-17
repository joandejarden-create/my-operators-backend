/**
 * Apply curated Design Hotels CALA Hotel Census creates (sitemap gap properties).
 *
 *   node scripts/apply-design-hotels-census-manual-additions.mjs --dry-run
 *   node scripts/apply-design-hotels-census-manual-additions.mjs
 *   node scripts/apply-design-hotels-census-manual-additions.mjs --portfolio-key wake-biohotel
 */
import "../load-env.js";
import { writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  DESIGN_HOTELS_CENSUS_MANUAL_PLAN,
  HOTEL_CENSUS_TABLE,
  findDuplicateCandidates,
  rowToAirtableFields,
  validateDesignHotelsCensusManualRow,
} from "../lib/hotel-census/design-hotels-census-manual-additions.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT = join(
  __dirname,
  "..",
  "reports",
  "design-hotels-census-manual-additions-log.json"
);

function parseArgs() {
  const dryRun = process.argv.includes("--dry-run");
  const keyArg = process.argv.find((a) => a.startsWith("--portfolio-key"));
  const portfolioKey = keyArg ? process.argv[process.argv.indexOf(keyArg) + 1] : null;
  return { dryRun, portfolioKey };
}

async function main() {
  const { dryRun, portfolioKey } = parseArgs();
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  let plan = DESIGN_HOTELS_CENSUS_MANUAL_PLAN;
  if (portfolioKey) {
    plan = plan.filter((r) => r.portfolioKey === portfolioKey);
    if (!plan.length) throw new Error(`No plan row for portfolio-key: ${portfolioKey}`);
  }

  console.log(`=== Design Hotels manual census (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);

  for (const row of plan) {
    const v = validateDesignHotelsCensusManualRow(row);
    if (!v.pass) {
      throw new Error(`Validation failed for ${row.portfolioKey}: ${v.errors.join("; ")}`);
    }
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const selectFields = ["name", "country", "Affiliation", "Website"];
  const censusRecords = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, pageSize: 100 })
    .all();

  const log = { dryRun, appliedAt: new Date().toISOString(), rows: [] };
  let created = 0;
  let skipped = 0;

  for (const row of plan) {
    const fields = rowToAirtableFields(row);
    const dupes = findDuplicateCandidates(censusRecords, row);

    if (dupes.length) {
      skipped++;
      const entry = {
        portfolioKey: row.portfolioKey,
        action: "skip_duplicate",
        recordIds: dupes.map((d) => d.id),
        duplicateNames: dupes.map((d) => d.fields.name),
        fields,
      };
      log.rows.push(entry);
      console.log(
        `SKIP duplicate ${row.name} → ${dupes.map((d) => `${d.id} (${d.fields.name})`).join(", ")}`
      );
      continue;
    }

    if (dryRun) {
      log.rows.push({ portfolioKey: row.portfolioKey, action: "create_dry_run", fields });
      console.log(`[dry-run] CREATE ${row.name}`, fields);
      continue;
    }

    const [rec] = await base(HOTEL_CENSUS_TABLE).create([{ fields }], { typecast: true });
    created++;
    censusRecords.push(rec);
    log.rows.push({
      portfolioKey: row.portfolioKey,
      action: "create",
      recordId: rec.id,
      fields,
    });
    console.log(`CREATED ${row.name} → ${rec.id}`);
  }

  writeFileSync(REPORT, JSON.stringify(log, null, 2), "utf8");
  console.log(`\nDone: ${created} created, ${skipped} skipped`);
  console.log("Report:", REPORT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
