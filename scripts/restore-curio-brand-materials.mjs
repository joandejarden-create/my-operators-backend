/**
 * Restore Curio materials.file PDFs + materials.gallery images after accidental row replace.
 *
 * Root cause: apply-curio-cala-materials.mjs used --replace-slot-prefix "materials."
 * which deletes ALL materials.* rows including Image attachments on gallery + file rows.
 *
 *   node scripts/restore-curio-brand-materials.mjs --dry-run
 *   node scripts/restore-curio-brand-materials.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import Airtable from "airtable";
import {
  CURIO_GALLERY_PROPERTY_URL,
  CURIO_GALLERY_TO_OPENING_KEY,
  CURIO_MATERIALS_FILE_ROWS,
} from "../lib/curio-brand-explorer-materials-restore.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BRAND_NAME = "Curio Collection by Hilton";
const TABLE = "Brand Setup - Brand Explorer Presentation";
const IMAGE_FIELD = "Image";

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

const FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
};

function pickMetaImage(html) {
  const tags = html.match(/<meta[^>]*>/gi) || [];
  for (const tag of tags) {
    const key = (
      tag.match(/\bproperty=["']([^"']+)["']/i)?.[1] ||
      tag.match(/\bname=["']([^"']+)["']/i)?.[1] ||
      ""
    )
      .trim()
      .toLowerCase();
    if (!["og:image", "twitter:image", "twitter:image:src"].includes(key)) continue;
    const content = tag.match(/\bcontent=["']([^"']+)["']/i)?.[1];
    if (content) return content.trim();
  }
  return "";
}

function absolutize(baseUrl, maybeRelative) {
  try {
    return new URL(maybeRelative, baseUrl).toString();
  } catch {
    return "";
  }
}

async function fetchText(url) {
  const res = await fetch(url, { headers: FETCH_HEADERS, redirect: "follow" });
  if (!res.ok) return "";
  const ct = res.headers.get("content-type") || "";
  if (!/html|xml|text/i.test(ct)) return "";
  return res.text();
}

async function resolveHeroImageUrl(pageUrl) {
  const html = await fetchText(pageUrl);
  if (!html) return "";
  const meta = pickMetaImage(html);
  return absolutize(pageUrl, meta);
}

function firstImageUrl(record) {
  const imgs = record.get(IMAGE_FIELD);
  if (!Array.isArray(imgs) || !imgs.length) return "";
  const att = imgs[0];
  return String(att?.url || att?.thumbnails?.large?.url || att?.thumbnails?.small?.url || "").trim();
}

function hasStoredImage(record) {
  const url = firstImageUrl(record);
  return /^https?:\/\//i.test(url);
}

async function listBrandRows(base) {
  const esc = BRAND_NAME.replace(/"/g, '\\"');
  return base(TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
    .all();
}

async function restoreGallery(base, rows) {
  const openings = rows.filter((r) => r.get("Slot Key") === "footprint.openings");
  const galleries = rows.filter((r) => String(r.get("Slot Key") || "").startsWith("materials.gallery."));
  let updated = 0;
  let skipped = 0;

  for (const g of galleries) {
    const slot = String(g.get("Slot Key") || "").trim();
    if (hasStoredImage(g)) {
      console.log(`  gallery ${slot}: already has image — skip`);
      skipped++;
      continue;
    }

    let imageUrl = "";
    const key = CURIO_GALLERY_TO_OPENING_KEY[slot];
    if (key) {
      const match = openings.find((o) => String(o.get("Title") || "").includes(key));
      if (match) imageUrl = firstImageUrl(match);
    }
    if (!imageUrl && CURIO_GALLERY_PROPERTY_URL[slot]) {
      console.log(`  gallery ${slot}: fetching og:image from ${CURIO_GALLERY_PROPERTY_URL[slot]}`);
      imageUrl = await resolveHeroImageUrl(CURIO_GALLERY_PROPERTY_URL[slot]);
    }
    // Lifestyle detail tile — reuse first property hero if brand site has no og:image
    if (!imageUrl && slot === "materials.gallery.6") {
      const fallback = galleries.find((x) => x.get("Slot Key") === "materials.gallery.1");
      if (fallback) imageUrl = firstImageUrl(fallback);
      if (!imageUrl) {
        const nacar = openings.find((o) => String(o.get("Title") || "").includes("Nacar"));
        if (nacar) imageUrl = firstImageUrl(nacar);
      }
      if (imageUrl) console.log(`  gallery ${slot}: using lifestyle fallback from Nacar`);
    }
    if (!imageUrl) {
      console.log(`  gallery ${slot}: no image source — skip`);
      skipped++;
      continue;
    }

    console.log(`  gallery ${slot}: attach ${imageUrl.slice(0, 90)}…`);
    if (!DRY) {
      await base(TABLE).update(g.id, { [IMAGE_FIELD]: [{ url: imageUrl }] });
    }
    updated++;
    await new Promise((r) => setTimeout(r, 250));
  }

  return { updated, skipped };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const key = process.env.AIRTABLE_API_KEY;
  if (!baseId || !key) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const fixturePath = path.join(ROOT, "fixtures", "brand-explorer-presentation-curio-materials-restore.json");
  fs.writeFileSync(
    fixturePath,
    JSON.stringify(
      {
        targetBrandBasicsName: BRAND_NAME,
        instructions: "Restore materials.file PDF attachments — run scripts/restore-curio-brand-materials.mjs --apply",
        rows: CURIO_MATERIALS_FILE_ROWS,
      },
      null,
      2
    )
  );
  console.log(`${DRY ? "[dry-run] " : ""}Wrote ${fixturePath}`);

  console.log(`\n${DRY ? "[dry-run] " : ""}Restoring materials.file rows (replace prefix — text + PDF URLs)…`);
  if (!DRY) {
    const res = spawnSync(
      "node",
      [
        path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs"),
        "--brand-name",
        BRAND_NAME,
        "--fixture",
        fixturePath,
        "--replace-slot-prefix",
        "materials.file",
      ],
      { stdio: "inherit", cwd: ROOT, env: process.env }
    );
    if (res.status !== 0) process.exit(res.status || 1);
  } else {
    for (const row of CURIO_MATERIALS_FILE_ROWS) {
      console.log(`  would create ${row.title} → ${row.imageUrl.slice(0, 80)}…`);
    }
  }

  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await listBrandRows(base);
  console.log(`\n${DRY ? "[dry-run] " : ""}Restoring materials.gallery images…`);
  const gallery = await restoreGallery(base, rows);
  console.log(
    `\nGallery: ${DRY ? "would update" : "updated"} ${gallery.updated}, skipped ${gallery.skipped}.`
  );
  console.log(
    DRY
      ? "\nDry run. Re-run with --apply to push to Airtable."
      : "\nDone. Hard-refresh Brand Explorer Materials tab for Curio Collection by Hilton."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
