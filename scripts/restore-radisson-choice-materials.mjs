/**
 * Restore Radisson (Choice) materials.file rows + materials.gallery images.
 *
 *   node scripts/restore-radisson-choice-materials.mjs --dry-run
 *   node scripts/restore-radisson-choice-materials.mjs --apply
 *   node scripts/restore-radisson-choice-materials.mjs --images-only
 */
import "../load-env.js";
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { resolveRadissonChoiceImageDownloadUrl } from "./lib/radisson-choice-image-resolve.mjs";
import { clearBrandDetailCache } from "../api/brand-library.js";
import { fetchHoteldamImageBytes } from "./lib/choice-hoteldam-download.mjs";
import { extractPropertyUrlFromBody } from "./lib/choice-hotel-page-image.mjs";
import {
  extFromImageContentType,
  MAX_AIRTABLE_ATTACHMENT_BYTES,
  uploadImageBytesToAirtable,
} from "./lib/choice-airtable-upload-attachment.mjs";
import {
  RADISSON_CHOICE_BRAND_AIRTABLE_NAME,
  RADISSON_CHOICE_GALLERY_PROPERTY_URL,
  RADISSON_CHOICE_GALLERY_TO_OPENING_KEY,
  RADISSON_CHOICE_SCENARIO_PROPERTY_URL,
} from "../lib/radisson-choice-materials-restore.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const IMAGE_FIELD = "Image";
const BRAND_NAME = RADISSON_CHOICE_BRAND_AIRTABLE_NAME;
const MAX_BYTES = MAX_AIRTABLE_ATTACHMENT_BYTES;

const APPLY_FIXTURES = process.argv.includes("--apply");
const ATTACH = APPLY_FIXTURES || process.argv.includes("--images-only");
const FORCE = process.argv.includes("--force");

function firstImageUrl(record) {
  const imgs = record.get(IMAGE_FIELD);
  if (!Array.isArray(imgs) || !imgs.length) return "";
  const att = imgs[0];
  return String(att?.url || att?.thumbnails?.large?.url || att?.thumbnails?.small?.url || "").trim();
}

function hasStoredImage(record) {
  if (FORCE) return false;
  const imgs = record.get(IMAGE_FIELD);
  if (!Array.isArray(imgs) || !imgs.length) return false;
  const att = imgs[0];
  if (att?.thumbnails) return true;
  const url = String(att?.url || "");
  return /airtableusercontent\.com/i.test(url);
}

async function listBrandRows(base) {
  const esc = BRAND_NAME.replace(/"/g, '\\"');
  return base(TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
    .all();
}

async function attachImage(base, baseId, record, pageUrl, imageUrl) {
  const downloaded = await fetchHoteldamImageBytes(imageUrl, pageUrl, { usePuppeteer: true });
  if (downloaded) {
    const { buffer, contentType } = downloaded;
    if (buffer.length > MAX_BYTES) {
      console.log(`  skip: ${buffer.length} bytes > 5MB`);
      return false;
    }
    try {
      const ext = extFromImageContentType(contentType);
      await uploadImageBytesToAirtable({
        baseId,
        recordId: record.id,
        buffer,
        contentType,
        filename: `${record.id}.${ext}`,
      });
      return true;
    } catch (err) {
      console.log(`  upload bytes failed: ${err.message}`);
    }
  }
  console.log(`  skip: could not download image`);
  return false;
}

async function restoreGallery(base, baseId, rows) {
  const openings = rows.filter((r) => r.get("Slot Key") === "footprint.openings");
  const galleries = rows
    .filter((r) => String(r.get("Slot Key") || "").startsWith("materials.gallery."))
    .sort((a, b) => String(a.get("Slot Key")).localeCompare(String(b.get("Slot Key"))));

  let updated = 0;
  let skipped = 0;

  for (const g of galleries) {
    const slot = String(g.get("Slot Key") || "").trim();
    if (hasStoredImage(g)) {
      console.log(`  gallery ${slot}: already has image — skip`);
      skipped++;
      continue;
    }

    let pageUrl = RADISSON_CHOICE_GALLERY_PROPERTY_URL[slot] || "";
    const key = RADISSON_CHOICE_GALLERY_TO_OPENING_KEY[slot];
    if (key) {
      const match = openings.find((o) => String(o.get("Title") || "").includes(key));
      if (match) {
        const openingImg = firstImageUrl(match);
        if (openingImg) {
          console.log(`  gallery ${slot}: reuse opening image`);
          if (ATTACH) {
            await base(TABLE).update(g.id, { [IMAGE_FIELD]: [{ url: openingImg }] });
          }
          updated++;
          continue;
        }
      }
    }

    if (!pageUrl && slot === "materials.gallery.1") {
      pageUrl = RADISSON_CHOICE_GALLERY_PROPERTY_URL["materials.gallery.1"];
    }

    const imageUrl = pageUrl ? resolveRadissonChoiceImageDownloadUrl(pageUrl) : "";
    if (!imageUrl) {
      console.log(`  gallery ${slot}: no hoteldam mapping — skip`);
      skipped++;
      continue;
    }

    console.log(`  gallery ${slot}: ${imageUrl.slice(0, 90)}…`);
    if (ATTACH) {
      const ok = await attachImage(base, baseId, g, pageUrl, imageUrl);
      if (ok) updated++;
      else skipped++;
    } else {
      updated++;
    }
    await new Promise((r) => setTimeout(r, 300));
  }

  return { updated, skipped };
}

async function restoreCaseStudies(base, baseId, rows) {
  const caseStudies = rows.filter((r) => r.get("Slot Key") === "materials.caseStudy");
  let updated = 0;
  let skipped = 0;

  for (const row of caseStudies) {
    const title = String(row.get("Title") || "").trim();
    if (hasStoredImage(row)) {
      console.log(`  caseStudy "${title}": already has image — skip`);
      skipped++;
      continue;
    }
    const pageUrl = extractPropertyUrlFromBody(String(row.get("Body") || ""));
    const imageUrl = pageUrl ? resolveRadissonChoiceImageDownloadUrl(pageUrl) : "";
    if (!imageUrl) {
      console.log(`  caseStudy "${title}": no hoteldam mapping — skip`);
      skipped++;
      continue;
    }
    console.log(`  caseStudy "${title}": ${imageUrl.slice(0, 90)}…`);
    if (ATTACH) {
      const ok = await attachImage(base, baseId, row, pageUrl, imageUrl);
      if (ok) updated++;
      else skipped++;
    } else {
      updated++;
    }
    await new Promise((r) => setTimeout(r, 300));
  }

  return { updated, skipped };
}

async function restoreScenarioImages(base, baseId, rows) {
  const scenarios = rows.filter((r) => String(r.get("Slot Key") || "").startsWith("overview.scenario."));
  let updated = 0;
  let skipped = 0;

  for (const row of scenarios) {
    const slot = String(row.get("Slot Key") || "").trim();
    const title = String(row.get("Title") || "").trim();
    if (hasStoredImage(row)) {
      console.log(`  ${slot}: already has image — skip`);
      skipped++;
      continue;
    }
    const pageUrl = RADISSON_CHOICE_SCENARIO_PROPERTY_URL[slot] || "";
    const imageUrl = pageUrl ? resolveRadissonChoiceImageDownloadUrl(pageUrl) : "";
    if (!imageUrl) {
      console.log(`  ${slot}: no mapping — skip`);
      skipped++;
      continue;
    }
    console.log(`  ${slot} "${title}": ${imageUrl.slice(0, 90)}…`);
    if (ATTACH) {
      const ok = await attachImage(base, baseId, row, pageUrl, imageUrl);
      if (ok) updated++;
      else skipped++;
    } else {
      updated++;
    }
    await new Promise((r) => setTimeout(r, 300));
  }

  return { updated, skipped };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const key = process.env.AIRTABLE_API_KEY;
  if (!baseId || !key) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  console.log(`${!ATTACH ? "[dry-run] " : ""}Restoring ${BRAND_NAME} materials.file + materials.gallery…`);

  if (APPLY_FIXTURES) {
    for (const [fixture, prefix] of [
      ["fixtures/brand-explorer-presentation-radisson-choice-materials.json", "materials.file"],
      ["fixtures/brand-explorer-presentation-radisson-choice-gallery.json", "materials.gallery."],
    ]) {
      const res = spawnSync(
        process.execPath,
        [
          path.join(ROOT, "scripts/apply-brand-explorer-presentation-fixture.mjs"),
          "--brand-name",
          BRAND_NAME,
          "--fixture",
          path.join(ROOT, fixture),
          "--replace-slot-prefix",
          prefix,
        ],
        { stdio: "inherit", cwd: ROOT, env: process.env }
      );
      if (res.status !== 0) process.exit(res.status || 1);
    }
  }

  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await listBrandRows(base);
  console.log(`\n${!ATTACH ? "[dry-run] " : ""}Attaching overview.scenario images…`);
  const scenarios = await restoreScenarioImages(base, baseId, rows);
  console.log(
    `Scenarios: ${!ATTACH ? "would update" : "updated"} ${scenarios.updated}, skipped ${scenarios.skipped}.`
  );

  console.log(`\n${!ATTACH ? "[dry-run] " : ""}Attaching materials.gallery images…`);
  const gallery = await restoreGallery(base, baseId, rows);
  console.log(
    `\nGallery: ${!ATTACH ? "would update" : "updated"} ${gallery.updated}, skipped ${gallery.skipped}.`
  );

  console.log(`\n${!ATTACH ? "[dry-run] " : ""}Attaching materials.caseStudy images…`);
  const caseStudies = await restoreCaseStudies(base, baseId, rows);
  console.log(
    `Case studies: ${!ATTACH ? "would update" : "updated"} ${caseStudies.updated}, skipped ${caseStudies.skipped}.`
  );

  if (ATTACH) {
    clearBrandDetailCache(process.env.RADISSON_CHOICE_BASICS_ID || "recywbx1YQSTCPqW1");
    clearBrandDetailCache(BRAND_NAME);
    console.log("Cleared brand detail API cache — hard-refresh the Brand Explorer page.");
  }
  if (!ATTACH) {
    console.log("\nRe-run with --apply to write to Airtable.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
