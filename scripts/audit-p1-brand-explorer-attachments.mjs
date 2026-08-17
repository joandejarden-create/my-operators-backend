/**
 * Read-only audit: P1 Brand Explorer openings, images, materials attachments in Airtable.
 *   node scripts/audit-p1-brand-explorer-attachments.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import { buildCalaFootprintOpeningRows } from "./lib/choice-cala-footprint-opening-rows.mjs";
import { resolveChiBrandBasicsName } from "./lib/choice-chi-brand-resolve.mjs";
import { extractPropertyUrlFromBody } from "./lib/choice-hotel-page-image.mjs";
import { resolveFootprintOpeningImageUrl } from "./lib/choice-footprint-opening-image-map.mjs";
import { CHOICE_P1_ENRICHMENT_QUEUE } from "./lib/choice-brand-explorer-manifest.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";

function hasAirtableStoredAttachment(fieldVal) {
  return (
    Array.isArray(fieldVal) &&
    fieldVal.length > 0 &&
    String(fieldVal[0]?.url || "").includes("airtableusercontent.com")
  );
}

function hasAnyAttachment(fieldVal) {
  return Array.isArray(fieldVal) && fieldVal.length > 0 && Boolean(fieldVal[0]?.url);
}

async function listChiBrandNames(base) {
  const rows = await base(BASICS).select({ maxRecords: 500 }).all();
  return rows
    .filter((r) => String(r.get("Parent Company") || "").includes("Choice Hotels International"))
    .map((r) => String(r.get("Brand Name") || "").trim())
    .filter(Boolean);
}

async function fetchBrandPresentationRows(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  for (const formula of [`{Brand Name} = "${esc}"`, `{Brand} = "${esc}"`]) {
    try {
      for (const r of await base(TABLE).select({ filterByFormula: formula, maxRecords: 500 }).all()) {
        if (!seen.has(r.id)) {
          seen.add(r.id);
          merged.push(r);
        }
      }
    } catch {
      /* schema */
    }
  }
  return merged;
}

function normalizeTitle(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const chiNames = await listChiBrandNames(base);
  /** @type {object[]} */
  const report = [];

  for (const requested of CHOICE_P1_ENRICHMENT_QUEUE) {
    const live = resolveChiBrandBasicsName(requested, chiNames) || requested;
    const rows = await fetchBrandPresentationRows(base, live);
    const openings = rows.filter((r) => String(r.get("Slot Key") || "").trim() === "footprint.openings");
    const expectedOpenings = buildCalaFootprintOpeningRows(live);
    const expectedTitles = new Set(expectedOpenings.map((o) => normalizeTitle(o.title)));

    const openingDetails = openings.map((r) => {
      const body = String(r.get("Body") || "");
      const pageUrl = extractPropertyUrlFromBody(body);
      const mapUrl = pageUrl ? resolveFootprintOpeningImageUrl(pageUrl) : null;
      return {
        title: String(r.get("Title") || "").trim(),
        hasUploadedImage: hasAirtableStoredAttachment(r.get("Image")),
        pageUrl: pageUrl || null,
        imageMappable: Boolean(mapUrl),
      };
    });

    const airtableTitles = new Set(openingDetails.map((o) => normalizeTitle(o.title)));
    const missingFromAirtable = [...expectedTitles].filter((t) => !airtableTitles.has(t));
    const extraInAirtable = [...airtableTitles].filter((t) => expectedTitles.size && !expectedTitles.has(t));

    const materialsFile = rows.filter((r) => String(r.get("Slot Key") || "").trim() === "materials.file");
    const gallery = rows.filter((r) => String(r.get("Slot Key") || "").startsWith("materials.gallery"));
    const caseStudies = rows.filter((r) => String(r.get("Slot Key") || "").trim() === "materials.caseStudy");
    const momentum = rows.filter((r) => String(r.get("Slot Key") || "").startsWith("footprint.momentum"));
    const fddRows = materialsFile.filter((r) =>
      /franchise disclosure document/i.test(String(r.get("Title") || ""))
    );
    const fileDetails = materialsFile.map((r) => {
      const imgs = r.get("Image");
      const att = Array.isArray(imgs) && imgs[0] ? imgs[0] : null;
      return {
        title: String(r.get("Title") || "").trim(),
        hasAttachment: Boolean(att?.url),
        storedInAirtable: hasAirtableStoredAttachment(imgs),
        filename: att?.filename || null,
      };
    });

    report.push({
      requested,
      liveBrandName: live,
      totalPresentationRows: rows.length,
      openings: {
        expectedCalaCount: expectedOpenings.length,
        airtableCount: openings.length,
        countMatch: expectedOpenings.length === openings.length,
        withUploadedImage: openingDetails.filter((o) => o.hasUploadedImage).length,
        mappableButNoUpload: openingDetails.filter((o) => !o.hasUploadedImage && o.imageMappable).length,
        noImageNoMap: openingDetails.filter((o) => !o.hasUploadedImage && !o.imageMappable).length,
        missingExpectedTitles: missingFromAirtable,
        unexpectedTitles: extraInAirtable,
        records: openingDetails,
      },
      materials: {
        fileRowCount: materialsFile.length,
        fileRecords: fileDetails,
        fddRowCount: fddRows.length,
        fddWithAnyAttachment: fddRows.filter((r) => hasAnyAttachment(r.get("Image"))).length,
        fddWithStoredAttachment: fddRows.filter((r) => hasAirtableStoredAttachment(r.get("Image"))).length,
        galleryRowCount: gallery.length,
        galleryWithStoredImage: gallery.filter((r) => hasAirtableStoredAttachment(r.get("Image"))).length,
        caseStudyRowCount: caseStudies.length,
        caseStudyWithStoredImage: caseStudies.filter((r) => hasAirtableStoredAttachment(r.get("Image"))).length,
      },
      momentumRowCount: momentum.length,
    });
  }

  const outPath = path.join(ROOT, "reports", "p1-brand-explorer-attachment-audit.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify({ generatedAt: new Date().toISOString(), brands: report }, null, 2)}\n`);

  console.log("P1 Brand Explorer attachment audit\n");
  for (const b of report) {
    const o = b.openings;
    console.log(`=== ${b.liveBrandName} ===`);
    console.log(`  Presentation rows: ${b.totalPresentationRows}`);
    console.log(
      `  Openings: ${o.airtableCount}/${o.expectedCalaCount} CALA expected` +
        (o.countMatch ? " ✓" : " ✗ MISMATCH")
    );
    console.log(
      `  Opening images uploaded: ${o.withUploadedImage}/${o.airtableCount}` +
        (o.mappableButNoUpload ? ` (${o.mappableButNoUpload} mappable, not uploaded)` : "")
    );
    if (o.missingExpectedTitles.length) console.log(`  Missing openings: ${o.missingExpectedTitles.join("; ")}`);
    if (o.unexpectedTitles.length) console.log(`  Unexpected openings: ${o.unexpectedTitles.join("; ")}`);
    console.log(
      `  FDD PDF: ${b.materials.fddWithStoredAttachment}/${b.materials.fddRowCount} stored` +
        ` | materials.file rows: ${b.materials.fileRowCount}`
    );
    for (const f of b.materials.fileRecords || []) {
      const mark = f.storedInAirtable ? "✓" : f.hasAttachment ? "~" : "✗";
      console.log(`    ${mark} ${f.title}${f.filename ? ` (${f.filename})` : ""}`);
    }
    console.log(
      `  Gallery images: ${b.materials.galleryWithStoredImage}/${b.materials.galleryRowCount}` +
        ` | Case study images: ${b.materials.caseStudyWithStoredImage}/${b.materials.caseStudyRowCount}`
    );
    console.log(`  Momentum rows: ${b.momentumRowCount}`);
    console.log("");
  }
  console.log(`Full report: ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
