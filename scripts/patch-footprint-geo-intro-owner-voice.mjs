import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const SLOT = "footprint.geo_intro";

const BODY =
  "This brand sits within the Choice Hotels International portfolio and should be evaluated like an owner decision, not a brochure summary. Confirm local demand drivers, required investment level, prototype/PIP scope, and expected net return against your real comp set before you commit.";

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await base(TABLE)
    .select({ filterByFormula: `{Slot Key} = "${SLOT}"`, maxRecords: 2000 })
    .all();

  console.log(`${dryRun ? "[dry-run] " : ""}Found ${rows.length} ${SLOT} row(s).`);
  let updated = 0;
  for (const r of rows) {
    const current = String(r.get("Body") || "").trim();
    if (current === BODY) continue;
    const brand = String(r.get("Brand Name") || "").trim();
    console.log(`- ${brand}`);
    if (!dryRun) {
      await base(TABLE).update(r.id, { Body: BODY });
    }
    updated += 1;
  }
  console.log(`${dryRun ? "Would update" : "Updated"} ${updated} row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

