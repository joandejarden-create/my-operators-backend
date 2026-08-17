import "../load-env.js";
import Airtable from "airtable";
import { AIRTABLE_BRAND_TO_FDD_STEM } from "./lib/choice-fdd-materials-config.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const LINK_FIELD_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

async function findBasics(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(BASICS).select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 }).all();
  return rows[0] || null;
}

async function selectPresentation(base, brandRecordId, brandName) {
  const escapedName = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const push = (records) => {
    for (const r of records) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };
  try {
    push(await base(TABLE).select({ filterByFormula: `{Brand Name} = "${escapedName}"`, maxRecords: 500 }).all());
  } catch {}
  try {
    push(await base(TABLE).select({ filterByFormula: `{Brand} = "${escapedName}"`, maxRecords: 500 }).all());
  } catch {}
  if (merged.length) return merged;
  for (const linkField of LINK_FIELD_CANDIDATES) {
    try {
      const formula = `FIND("${brandRecordId}", ARRAYJOIN({${linkField}})) > 0`;
      const rows = await base(TABLE).select({ filterByFormula: formula, maxRecords: 500 }).all();
      if (rows.length) return rows;
    } catch {}
  }
  return [];
}

function isFddRow(r) {
  return (
    String(r.get("Slot Key") || "").trim() === "materials.file" &&
    /franchise disclosure document/i.test(String(r.get("Title") || ""))
  );
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const brands = Object.keys(AIRTABLE_BRAND_TO_FDD_STEM);
let ok = 0;
let missing = 0;

for (const brand of brands) {
  const basics = await findBasics(base, brand);
  if (!basics) {
    console.log("NO BASICS:", brand);
    missing++;
    continue;
  }
  const rows = await selectPresentation(base, basics.id, brand);
  const fdd = rows.find(isFddRow);
  const img = fdd?.get("Image");
  const has = Array.isArray(img) && img.length && String(img[0]?.url || "").includes("airtableusercontent.com");
  if (has) ok++;
  else {
    missing++;
    console.log("MISSING:", brand, fdd?.id || "no FDD row", fdd?.get("Image")?.[0]?.filename || "");
  }
}
console.log(`FDD attachments: ${ok}/${brands.length} ok, ${missing} missing`);
