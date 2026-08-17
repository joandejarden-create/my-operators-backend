/**
 * Upsert footprint / geo / growth presentation rows for one brand from a fixture.
 *   node scripts/patch-footprint-presentation-by-slot.mjs --brand-name Radisson
 *   node scripts/patch-footprint-presentation-by-slot.mjs --dry-run --brand-name Radisson
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const LINK_FIELD_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];
const FIXTURE = path.join(ROOT, "fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json");

function parseArgs(argv) {
  const dryRun = argv.includes("--dry-run");
  let brandName = "Radisson";
  let brandRecordId = "";
  let fixturePath = FIXTURE;
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--brand-name" && argv[i + 1]) brandName = argv[++i];
    else if (argv[i] === "--brand-record-id" && argv[i + 1]) brandRecordId = argv[++i];
    else if (argv[i] === "--fixture" && argv[i + 1]) fixturePath = path.resolve(ROOT, argv[++i]);
  }
  return { dryRun, brandName, brandRecordId: brandRecordId.trim(), fixturePath };
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const records = await base(BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  if (records.length !== 1) throw new Error(`Expected one Brand Basics row for "${brandName}", got ${records.length}`);
  return records[0].id;
}

async function selectPresentationForBrand(base, brandRecordId, brandName) {
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
    pushAll(await base(TABLE).select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 }).all());
  } catch {
    /* optional */
  }
  for (const linkField of LINK_FIELD_CANDIDATES) {
    try {
      const formula = `FIND("${brandRecordId.replace(/"/g, '\\"')}", ARRAYJOIN({${linkField}})) > 0`;
      pushAll(await base(TABLE).select({ filterByFormula: formula, maxRecords: 500 }).all());
    } catch {
      /* */
    }
  }
  return merged;
}

function buildFields(brandRecordId, linkField, r, brandName) {
  return {
    [linkField]: [brandRecordId],
    "Slot Key": r.slotKey,
    Title: r.title ?? "",
    Body: r.body ?? "",
    "Sort Order": typeof r.sort === "number" ? r.sort : 0,
    Active: true,
    "Brand Name": brandName,
  };
}

async function main() {
  const { dryRun, brandName, brandRecordId, fixturePath } = parseArgs(process.argv);
  const data = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  let brandId = brandRecordId;
  let brandNameForRow = brandName;
  if (!brandId) {
    brandId = await findBasicsByName(base, brandName);
  } else {
    try {
      const basics = await base(BASICS).find(brandId);
      brandNameForRow = String(basics.get("Brand Name") || brandName).trim();
    } catch {
      /* keep CLI brand name */
    }
  }
  const existing = await selectPresentationForBrand(base, brandId, brandNameForRow);
  const bySlot = new Map();
  for (const rec of existing) {
    const sk = String(rec.get("Slot Key") || "").trim();
    if (sk && !bySlot.has(sk)) bySlot.set(sk, rec);
  }
  let created = 0;
  let updated = 0;
  for (const row of data.rows) {
    const sk = String(row.slotKey || "").trim();
    const rec = bySlot.get(sk);
    let lastErr;
    if (rec) {
      for (const linkField of LINK_FIELD_CANDIDATES) {
        try {
          const fields = buildFields(brandId, linkField, row, brandNameForRow);
          console.log(`${dryRun ? "Would update" : "Updating"} ${sk} (${rec.id})`);
          if (!dryRun) await base(TABLE).update(rec.id, fields);
          updated++;
          lastErr = null;
          break;
        } catch (e) {
          lastErr = e;
        }
      }
      if (lastErr) throw lastErr;
    } else {
      for (const linkField of LINK_FIELD_CANDIDATES) {
        try {
          const fields = buildFields(brandId, linkField, row, brandNameForRow);
          console.log(`${dryRun ? "Would create" : "Creating"} ${sk}`);
          if (!dryRun) await base(TABLE).create([{ fields }]);
          created++;
          lastErr = null;
          break;
        } catch (e) {
          lastErr = e;
        }
      }
      if (lastErr) throw lastErr;
    }
  }
  console.log(`Done. ${dryRun ? "Would" : ""} create ${created}, update ${updated}.`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
