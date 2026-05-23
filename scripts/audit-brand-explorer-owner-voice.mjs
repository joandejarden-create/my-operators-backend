/**
 * Scan Brand Explorer presentation rows for internal/editorial phrasing.
 *
 *   node scripts/audit-brand-explorer-owner-voice.mjs
 *   node scripts/audit-brand-explorer-owner-voice.mjs --brand "Radisson"
 */
import "../load-env.js";
import Airtable from "airtable";
import { scanTextForInternalVoice } from "./lib/owner-voice-phrases.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const LINK_FIELDS = ["Brand", "Brand Setup - Brand Basics", "Brand Basics"];
const SELECT_FIELDS = [
  "Slot Key",
  "Title",
  "Body",
  "Brand",
  "Case Summary Overview",
  "Case Summary Owner Objective",
  "Case Summary Brand Relevance",
  "Case Summary Interpretation",
];

const CASE_SUMMARY_FIELDS = [
  ["Case Summary Overview", "Case Summary Overview"],
  ["Case Summary Owner Objective", "Case Summary Owner Objective"],
  ["Case Summary Brand Relevance", "Case Summary Brand Relevance"],
  ["Case Summary Interpretation", "Case Summary Interpretation"],
];

function parseArgs() {
  const i = process.argv.indexOf("--brand");
  return { brandFilter: i >= 0 ? String(process.argv[i + 1] || "").trim() : "" };
}

function getBase() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  return new Airtable({ apiKey: key }).base(baseId);
}

async function loadBasicsMap(base) {
  const map = new Map();
  await base(BASICS)
    .select({ fields: ["Brand Name"], pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) map.set(r.id, String(r.get("Brand Name") || r.id));
      next();
    });
  return map;
}

function linkIds(rec) {
  for (const f of LINK_FIELDS) {
    const v = rec.get(f);
    if (Array.isArray(v) && v.length) return v;
  }
  return [];
}

async function main() {
  const { brandFilter } = parseArgs();
  const base = getBase();
  const basicsMap = await loadBasicsMap(base);
  const byBrand = new Map();

  await base(TABLE)
    .select({
      fields: SELECT_FIELDS,
      pageSize: 100,
    })
    .eachPage((records, next) => {
      for (const rec of records) {
        const links = linkIds(rec);
        const brandId = links[0] || "unknown";
        const brandName = basicsMap.get(brandId) || brandId;
        if (brandFilter && !brandName.toLowerCase().includes(brandFilter.toLowerCase())) continue;

        const texts = [["Body", rec.get("Body")], ...CASE_SUMMARY_FIELDS.map(([label, field]) => [label, rec.get(field)])];
        for (const [field, text] of texts) {
          const hits = scanTextForInternalVoice(String(text || ""));
          if (!hits.length) continue;
          if (!byBrand.has(brandName)) byBrand.set(brandName, []);
          byBrand.get(brandName).push({
            slot: rec.get("Slot Key"),
            title: rec.get("Title"),
            field,
            hits: hits.map((h) => h.id),
            sample: String(text).slice(0, 120).replace(/\s+/g, " "),
          });
        }
      }
      next();
    });

  const brands = [...byBrand.keys()].sort();
  if (!brands.length) {
    console.log("No internal-voice hits found" + (brandFilter ? ` for filter "${brandFilter}"` : "") + ".");
    return;
  }
  console.log(`\n=== Brand Explorer owner-voice audit${brandFilter ? ` (filter: ${brandFilter})` : ""} ===\n`);
  let total = 0;
  for (const name of brands) {
    const issues = byBrand.get(name);
    total += issues.length;
    console.log(`${name} — ${issues.length} field(s) flagged`);
    for (const issue of issues.slice(0, 8)) {
      console.log(`  [${issue.slot}] ${issue.title || "(no title)"} · ${issue.field} · ${issue.hits.join(", ")}`);
      console.log(`    …${issue.sample}…`);
    }
    if (issues.length > 8) console.log(`  …and ${issues.length - 8} more`);
    console.log("");
  }
  console.log(`Total flagged fields: ${total} across ${brands.length} brand(s)`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
