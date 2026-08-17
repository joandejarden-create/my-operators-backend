/**
 * Attach materials.gallery images for Ascend Hotel Collection (press kit + hoteldam).
 *
 *   node scripts/restore-ascend-choice-gallery-images.mjs --dry-run
 *   node scripts/restore-ascend-choice-gallery-images.mjs
 *   node scripts/restore-ascend-choice-gallery-images.mjs --force
 */
import "../load-env.js";
import Airtable from "airtable";
import { fileURLToPath } from "url";
import path from "path";
import { clearBrandDetailCache } from "../api/brand-library.js";
import { fetchHoteldamImageBytes } from "./lib/choice-hoteldam-download.mjs";
import {
  extFromImageContentType,
  MAX_AIRTABLE_ATTACHMENT_BYTES,
  uploadImageBytesToAirtable,
} from "./lib/choice-airtable-upload-attachment.mjs";
import {
  ASCEND_CHOICE_BRAND_AIRTABLE_NAME,
  ASCEND_CHOICE_GALLERY_IMAGE_URL,
  ASCEND_CHOICE_GALLERY_TO_OPENING_KEY,
} from "../lib/ascend-choice-materials-restore.js";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const IMAGE_FIELD = "Image";
const BRAND_NAME = ASCEND_CHOICE_BRAND_AIRTABLE_NAME;
const DRY = process.argv.includes("--dry-run");
const FORCE = process.argv.includes("--force");

function hasStoredImage(record) {
  if (FORCE) return false;
  const imgs = record.get(IMAGE_FIELD);
  if (!Array.isArray(imgs) || !imgs.length) return false;
  const att = imgs[0];
  if (att?.thumbnails) return true;
  const url = String(att?.url || "");
  return /airtableusercontent\.com/i.test(url);
}

function firstImageUrl(record) {
  const imgs = record.get(IMAGE_FIELD);
  if (!Array.isArray(imgs) || !imgs.length) return "";
  const att = imgs[0];
  return String(att?.url || att?.thumbnails?.large?.url || "").trim();
}

async function downloadImageBytes(imageUrl) {
  const res = await fetch(imageUrl, {
    headers: { "User-Agent": "deal-capture-proxy/1.0", Accept: "image/*,*/*" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const contentType = res.headers.get("content-type") || "image/jpeg";
  const buffer = Buffer.from(await res.arrayBuffer());
  return { buffer, contentType };
}

async function attachFromUrl(base, baseId, record, imageUrl, referer) {
  let downloaded = null;
  if (/hoteldam/i.test(imageUrl)) {
    downloaded = await fetchHoteldamImageBytes(imageUrl, referer || imageUrl, { usePuppeteer: true });
  } else {
    try {
      downloaded = await downloadImageBytes(imageUrl);
    } catch (err) {
      console.log(`  download failed: ${err.message}`);
      return false;
    }
  }
  if (!downloaded) return false;
  const { buffer, contentType } = downloaded;
  if (buffer.length > MAX_AIRTABLE_ATTACHMENT_BYTES) {
    console.log(`  skip: ${buffer.length} bytes > 5MB`);
    return false;
  }
  if (DRY) return true;
  const ext = extFromImageContentType(contentType);
  await uploadImageBytesToAirtable({
    baseId,
    recordId: record.id,
    buffer,
    contentType,
    filename: `${record.id}.${ext}`,
  });
  return true;
}

async function main() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const esc = BRAND_NAME.replace(/"/g, '\\"');
  const rows = await base(TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
    .all();

  const openings = rows.filter((r) => r.get("Slot Key") === "footprint.openings");
  const galleries = rows
    .filter((r) => String(r.get("Slot Key") || "").startsWith("materials.gallery."))
    .sort((a, b) => String(a.get("Slot Key")).localeCompare(String(b.get("Slot Key"))));

  console.log(`${DRY ? "[dry-run] " : ""}Ascend gallery images: ${galleries.length} slot(s)`);

  let updated = 0;
  let skipped = 0;

  for (const g of galleries) {
    const slot = String(g.get("Slot Key") || "").trim();
    const title = String(g.get("Title") || "").trim();
    if (hasStoredImage(g)) {
      console.log(`  ${slot} "${title}": already has image — skip`);
      skipped++;
      continue;
    }

    const key = ASCEND_CHOICE_GALLERY_TO_OPENING_KEY[slot];
    if (key) {
      const match = openings.find((o) => String(o.get("Title") || "").includes(key));
      const openingImg = match ? firstImageUrl(match) : "";
      if (openingImg) {
        console.log(`  ${slot} "${title}": reuse footprint opening image`);
        if (!DRY) {
          await base(TABLE).update(g.id, { [IMAGE_FIELD]: [{ url: openingImg }] });
        }
        updated++;
        continue;
      }
    }

    const imageUrl = ASCEND_CHOICE_GALLERY_IMAGE_URL[slot] || "";
    if (!imageUrl) {
      console.log(`  ${slot} "${title}": no image source — skip`);
      skipped++;
      continue;
    }

    console.log(`  ${slot} "${title}": ${imageUrl.slice(0, 88)}…`);
    const ok = await attachFromUrl(base, baseId, g, imageUrl, imageUrl);
    if (ok) updated++;
    else skipped++;
    await new Promise((r) => setTimeout(r, 400));
  }

  if (!DRY && updated > 0) {
    try {
      clearBrandDetailCache();
    } catch {
      /* optional */
    }
  }

  console.log(`Done. Updated ${updated}, skipped ${skipped}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
