/**
 * Upload Radisson (Choice) materials.file PDFs from Choice shared-drive materials.
 * View/Download in Brand Explorer use block.imageUrl (Airtable CDN attachment).
 *
 *   node scripts/attach-radisson-choice-materials-pdfs.mjs --dry-run
 *   node scripts/attach-radisson-choice-materials-pdfs.mjs --apply
 */
import fs from "fs";
import path from "path";
import "../load-env.js";
import Airtable from "airtable";
import { CHOICE_MATERIALS_ROOT } from "./lib/choice-brand-materials-config.mjs";
import { uploadImageBytesToAirtable, MAX_AIRTABLE_ATTACHMENT_BYTES } from "./lib/choice-airtable-upload-attachment.mjs";
import { clearBrandDetailCache } from "../api/brand-library.js";
import { RADISSON_CHOICE_BRAND_AIRTABLE_NAME } from "../lib/radisson-choice-materials-restore.js";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BRAND_NAME = RADISSON_CHOICE_BRAND_AIRTABLE_NAME;
const BRAND_ID = process.env.RADISSON_CHOICE_BASICS_ID || "recywbx1YQSTCPqW1";
const APPLY = process.argv.includes("--apply");

const MATERIALS_DIR = path.join(CHOICE_MATERIALS_ROOT, "Radisson");

/** Title substring → local PDF filename */
const PDF_BY_TITLE = {
  "CALA One-Pager": "Radisson One Pager 2025.pdf",
  "Brand Brochure": "1. Brand Book - RD.pdf",
};

const DEV_URL = "https://www.choicehotelsdevelopment.com/brands/#radisson";

function formatBytes(n) {
  if (n >= 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  if (n >= 1024) return `${Math.round(n / 1024)} KB`;
  return `${n} B`;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const key = process.env.AIRTABLE_API_KEY;
  if (!baseId || !key) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  if (!fs.existsSync(MATERIALS_DIR)) {
    console.error(`Materials folder not found: ${MATERIALS_DIR}`);
    process.exit(1);
  }

  const base = new Airtable({ apiKey: key }).base(baseId);
  const esc = BRAND_NAME.replace(/"/g, '\\"');
  const rows = await base(TABLE)
    .select({
      filterByFormula: `AND({Brand Name} = "${esc}", {Slot Key} = "materials.file")`,
      maxRecords: 20,
    })
    .all();

  let updated = 0;
  for (const row of rows) {
    const title = String(row.get("Title") || "").trim();
    console.log(`\n${title || row.id}`);

    if (/development/i.test(title)) {
      const body = `Web · Brand architecture and development resources\n${DEV_URL}`;
      if (!APPLY) {
        console.log(`  would set body + link ${DEV_URL}`);
        continue;
      }
      await base(TABLE).update(row.id, { Body: body });
      console.log("  updated body (web link)");
      updated++;
      continue;
    }

    const pdfName = Object.entries(PDF_BY_TITLE).find(([k]) => title.includes(k))?.[1];
    if (!pdfName) {
      console.log("  skip: no PDF mapping");
      continue;
    }
    const pdfPath = path.join(MATERIALS_DIR, pdfName);
    if (!fs.existsSync(pdfPath)) {
      console.log(`  skip: missing ${pdfPath}`);
      continue;
    }
    const stat = fs.statSync(pdfPath);
    if (stat.size > MAX_AIRTABLE_ATTACHMENT_BYTES) {
      console.log(`  skip: ${pdfName} is ${formatBytes(stat.size)} (> 5 MB upload limit)`);
      continue;
    }

    const body = `PDF · ${formatBytes(stat.size)} · ${pdfName}\nBadge: Unverified by Brand`;
    if (!APPLY) {
      console.log(`  would upload ${pdfName} (${formatBytes(stat.size)})`);
      continue;
    }

    const buffer = fs.readFileSync(pdfPath);
    await uploadImageBytesToAirtable({
      baseId,
      recordId: row.id,
      buffer,
      contentType: "application/pdf",
      filename: pdfName,
    });
    await base(TABLE).update(row.id, { Body: body });
    console.log(`  uploaded ${pdfName}`);
    updated++;
  }

  if (APPLY && updated) {
    clearBrandDetailCache(BRAND_ID);
    clearBrandDetailCache(BRAND_NAME);
    console.log("\nCleared brand detail cache for fresh gallery/materials URLs.");
  }

  console.log(`\n${APPLY ? "Updated" : "Would update"} ${updated} materials.file row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
