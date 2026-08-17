/**
 * Attach latest Choice FDD PDF to Brand Explorer materials.file (Franchise Disclosure Document).
 * Creates the row when missing; uploads PDF to Image (direct bytes ≤5 MB, else public URL mirror).
 *
 *   node scripts/attach-choice-fdd-materials-pdfs.mjs --dry-run
 *   node scripts/attach-choice-fdd-materials-pdfs.mjs --apply
 *   node scripts/attach-choice-fdd-materials-pdfs.mjs --apply --brand "Comfort Inn"
 */
import fs from "fs";
import "../load-env.js";
import Airtable from "airtable";
import { clearBrandDetailCache } from "../api/brand-library.js";
import {
  uploadImageBytesToAirtable,
  MAX_AIRTABLE_ATTACHMENT_BYTES,
} from "./lib/choice-airtable-upload-attachment.mjs";
import {
  DEFAULT_FDD_DIR,
  FDD_SLOT_KEY,
  FDD_TITLE,
  loadFddManifest,
  indexManifestByStem,
  resolveFddPdfForBrand,
  listBrandsWithFddOnFile,
} from "./lib/choice-fdd-materials-config.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const LINK_FIELD_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

function parseArgs(argv) {
  const apply = argv.includes("--apply");
  const dryRun = argv.includes("--dry-run") || !apply;
  let brand = "";
  const bi = argv.indexOf("--brand");
  if (bi >= 0 && argv[bi + 1]) brand = String(argv[bi + 1]).trim();
  let fddDir = DEFAULT_FDD_DIR;
  const di = argv.indexOf("--dir");
  if (di >= 0 && argv[di + 1]) fddDir = argv[di + 1];
  return { dryRun, apply, brandFilter: brand, fddDir };
}

function formatBytes(n) {
  if (n >= 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  if (n >= 1024) return `${Math.round(n / 1024)} KB`;
  return `${n} B`;
}

function isFddMaterialsRow(rec) {
  const sk = String(rec.get("Slot Key") || "").trim();
  const title = String(rec.get("Title") || "").trim();
  return sk === FDD_SLOT_KEY && /franchise disclosure document/i.test(title);
}

function hasStoredAttachment(imgs) {
  if (!Array.isArray(imgs) || !imgs.length) return false;
  const u = String(imgs[0]?.url || "");
  return u.includes("airtableusercontent.com");
}

function attachmentMatchesPdf(imgs, pdfName) {
  if (!Array.isArray(imgs) || !imgs.length) return false;
  const fn = String(imgs[0]?.filename || "").trim();
  return fn === pdfName || fn.includes(pdfName.replace(/\.pdf$/i, ""));
}

async function mirrorToPublicUrl(buffer, filename) {
  const form = new FormData();
  form.append("reqtype", "fileupload");
  form.append("fileToUpload", new Blob([buffer], { type: "application/pdf" }), filename);
  const res = await fetch("https://catbox.moe/user/api.php", { method: "POST", body: form });
  if (!res.ok) throw new Error(`mirror upload ${res.status}`);
  const publicUrl = (await res.text()).trim();
  if (!/^https?:\/\//i.test(publicUrl)) throw new Error(`bad mirror url: ${publicUrl.slice(0, 80)}`);
  return publicUrl;
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const records = await base(BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  return records[0] || null;
}

async function selectPresentationForBrand(base, brandRecordId, brandName) {
  const escapedName = String(brandName || "").replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const pushAll = (records) => {
    for (const r of records) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };
  try {
    pushAll(await base(TABLE).select({ filterByFormula: `{Brand Name} = "${escapedName}"`, maxRecords: 500 }).all());
  } catch {
    /* optional column */
  }
  try {
    pushAll(await base(TABLE).select({ filterByFormula: `{Brand} = "${escapedName}"`, maxRecords: 500 }).all());
  } catch {
    /* schema differs */
  }
  if (merged.length) return merged;
  const escapedId = String(brandRecordId).replace(/"/g, '\\"');
  for (const linkField of LINK_FIELD_CANDIDATES) {
    try {
      const formula = `FIND("${escapedId}", ARRAYJOIN({${linkField}})) > 0`;
      const records = await base(TABLE).select({ filterByFormula: formula, maxRecords: 500 }).all();
      if (records.length) return records;
    } catch {
      /* wrong link field */
    }
  }
  return [];
}

function buildCreateFields(brandRecordId, linkField, brandName, sortOrder) {
  return {
    [linkField]: [brandRecordId],
    "Slot Key": FDD_SLOT_KEY,
    Title: FDD_TITLE,
    Body: "PDF · Franchise disclosure document on file.\nBadge: Unverified by Brand",
    "Sort Order": sortOrder,
    Active: true,
    "Brand Name": brandName,
  };
}

async function createFddRow(base, brandRecordId, brandName, existingRows, dryRun) {
  const fileRows = existingRows.filter((r) => String(r.get("Slot Key") || "").trim() === FDD_SLOT_KEY);
  const sorts = fileRows.map((r) => Number(r.get("Sort Order"))).filter((n) => Number.isFinite(n));
  const sortOrder = sorts.length ? Math.min(...sorts) : 0;

  if (dryRun) {
    console.log(`  would create materials.file "${FDD_TITLE}" (Sort Order ${sortOrder})`);
    return { id: "dry-run-rec", created: true };
  }

  let lastErr;
  for (const linkField of LINK_FIELD_CANDIDATES) {
    try {
      const fields = buildCreateFields(brandRecordId, linkField, brandName, sortOrder);
      const [created] = await base(TABLE).create([{ fields }]);
      console.log(`  created row ${created.id} via link field "${linkField}"`);
      return { id: created.id, created: true };
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error("Could not create FDD materials.file row");
}

async function attachPdfToRow({ base, baseId, recordId, pdfPath, pdfName, dryRun }) {
  const stat = fs.statSync(pdfPath);
  const body = `PDF · ${formatBytes(stat.size)} · ${pdfName}\nBadge: Unverified by Brand`;

  if (dryRun) {
    const via = stat.size <= MAX_AIRTABLE_ATTACHMENT_BYTES ? "bytes" : "mirror+url";
    console.log(`  would attach ${pdfName} (${formatBytes(stat.size)}) via ${via}`);
    console.log(`  would set Body: ${body.split("\n")[0]}…`);
    return { ok: true, dryRun: true };
  }

  const buffer = fs.readFileSync(pdfPath);
  if (buffer.length <= MAX_AIRTABLE_ATTACHMENT_BYTES) {
    await uploadImageBytesToAirtable({
      baseId,
      recordId,
      buffer,
      contentType: "application/pdf",
      filename: pdfName,
    });
    console.log(`  uploaded ${pdfName} (${formatBytes(buffer.length)}) via bytes`);
  } else {
    const publicUrl = await mirrorToPublicUrl(buffer, pdfName);
    console.log(`  mirrored ${pdfName} → ${publicUrl.slice(0, 60)}…`);
    await base(TABLE).update(recordId, { Image: [{ url: publicUrl, filename: pdfName }] });
    let ok = false;
    for (let attempt = 0; attempt < 5; attempt++) {
      await new Promise((r) => setTimeout(r, 2500));
      const check = await base(TABLE).find(recordId);
      ok = hasStoredAttachment(check.get("Image"));
      if (ok) break;
    }
    if (!ok) throw new Error("Airtable did not ingest mirrored PDF attachment");
    console.log(`  attached via URL mirror (${formatBytes(buffer.length)})`);
  }

  await base(TABLE).update(recordId, { Body: body });
  return { ok: true };
}

async function main() {
  const { dryRun, apply, brandFilter, fddDir } = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const key = process.env.AIRTABLE_API_KEY;
  if (!baseId || !key) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const { files } = loadFddManifest(fddDir);
  const stemIndex = indexManifestByStem(fddDir, files);
  const brands = listBrandsWithFddOnFile(stemIndex, brandFilter);
  if (!brands.length) {
    console.error(brandFilter ? `No brands match --brand "${brandFilter}"` : "No brands with FDD PDFs on file");
    process.exit(1);
  }

  const base = new Airtable({ apiKey: key }).base(baseId);
  console.log(`${dryRun ? "DRY RUN" : "APPLY"} — ${brands.length} brand(s), FDD dir: ${fddDir}\n`);

  let created = 0;
  let updated = 0;
  let skipped = 0;
  let failed = 0;
  const touchedBrandIds = new Set();

  for (const brandName of brands) {
    console.log(`\n${brandName}`);
    const pdfEntry = resolveFddPdfForBrand(brandName, stemIndex);
    if (!pdfEntry) {
      console.log("  skip: no PDF on disk for configured stem");
      skipped++;
      continue;
    }

    const basics = await findBasicsByName(base, brandName);
    if (!basics) {
      console.log("  skip: no Brand Basics row");
      skipped++;
      continue;
    }

    const presentation = await selectPresentationForBrand(base, basics.id, brandName);
    let fddRow = presentation.find(isFddMaterialsRow);
    if (!fddRow) {
      const out = await createFddRow(base, basics.id, brandName, presentation, dryRun);
      if (dryRun) {
        created++;
        await attachPdfToRow({
          base,
          baseId,
          recordId: out.id,
          pdfPath: pdfEntry.fullPath,
          pdfName: pdfEntry.pdf,
          dryRun: true,
        });
        continue;
      }
      fddRow = await base(TABLE).find(out.id);
      created++;
    } else {
      const imgs = fddRow.get("Image");
      if (attachmentMatchesPdf(imgs, pdfEntry.pdf) && hasStoredAttachment(imgs)) {
        console.log(`  skip: already has ${pdfEntry.pdf}`);
        skipped++;
        continue;
      }
    }

    try {
      await attachPdfToRow({
        base,
        baseId,
        recordId: fddRow.id,
        pdfPath: pdfEntry.fullPath,
        pdfName: pdfEntry.pdf,
        dryRun,
      });
      if (!dryRun) {
        updated++;
        touchedBrandIds.add(basics.id);
        touchedBrandIds.add(brandName);
      } else {
        updated++;
      }
    } catch (err) {
      console.log(`  error: ${err.message}`);
      failed++;
    }

    await new Promise((r) => setTimeout(r, dryRun ? 0 : 400));
  }

  if (apply && touchedBrandIds.size) {
    for (const id of touchedBrandIds) clearBrandDetailCache(id);
    console.log("\nCleared brand detail cache for updated brands.");
  }

  console.log(
    `\n${dryRun ? "Would create" : "Created"} ${created}; ${dryRun ? "Would update" : "Updated"} ${updated}; skipped ${skipped}; failed ${failed}.`
  );
  if (failed) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
