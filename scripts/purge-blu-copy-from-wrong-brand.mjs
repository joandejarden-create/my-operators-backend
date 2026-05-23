/**
 * Remove presentation rows on core "Radisson" that contain Blu-specific copy
 * (from mistaken npm apply that resolved brand name to Radisson only).
 *
 *   node scripts/purge-blu-copy-from-wrong-brand.mjs --dry-run
 *   node scripts/purge-blu-copy-from-wrong-brand.mjs
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const WRONG_BRAND_NAME = process.env.WRONG_RADISSON_BRAND_NAME || "Radisson";
const BLU_MARKERS = [
  /radisson blu/i,
  /think in black/i,
  /nordic nouveau/i,
  /enticing moments/i,
  /curatorial warmth/i,
  /inspired professional/i,
  /bariloche/i,
  /belo horizonte, savassi/i,
  /plaza el bosque santiago/i,
];

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const esc = WRONG_BRAND_NAME.replace(/"/g, '\\"');
  const rows = await base(TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
    .all();
  const toDelete = rows.filter((r) => {
    const blob = `${r.get("Title") || ""} ${r.get("Body") || ""}`;
    return BLU_MARKERS.some((re) => re.test(blob));
  });
  console.log(`"${WRONG_BRAND_NAME}": ${rows.length} total presentation rows`);
  console.log(`Blu-marker matches to delete: ${toDelete.length}`);
  if (toDelete.length) {
    const keys = [...new Set(toDelete.map((r) => r.get("Slot Key")))].slice(0, 15);
    console.log("Sample slot keys:", keys.join(", "));
  }
  if (dryRun || !toDelete.length) return;
  for (let i = 0; i < toDelete.length; i += 10) {
    await base(TABLE).destroy(toDelete.slice(i, i + 10).map((r) => r.id));
  }
  console.log("Deleted Blu copy from core Radisson.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
