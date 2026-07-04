/**
 * Attach hoteldam hero images to footprint.openings (upload bytes — Choice blocks Airtable URL fetch).
 *
 *   node scripts/attach-choice-footprint-opening-images.mjs --dry-run
 *   node scripts/attach-choice-footprint-opening-images.mjs
 *   node scripts/attach-choice-footprint-opening-images.mjs --brand "Quality Inn"
 */
import "../load-env.js";
import Airtable from "airtable";
import { extractPropertyUrlFromBody } from "./lib/choice-hotel-page-image.mjs";
import { resolveFootprintOpeningImageUrl } from "./lib/choice-footprint-opening-image-map.mjs";
import { downloadHoteldamImage } from "./lib/choice-hoteldam-download.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const FIELD = "Image";
const SLOT = "footprint.openings";
const MAX_BYTES = 5 * 1024 * 1024;

const BROWSER_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  Referer: "https://www.choicehotels.com/",
};

function parseArgs(argv) {
  const i = argv.indexOf("--brand");
  return {
    dryRun: argv.includes("--dry-run"),
    brandFilter: i >= 0 ? String(argv[i + 1] || "").trim() : "",
    force: argv.includes("--force"),
  };
}

function extFromContentType(ct) {
  if (/png/i.test(ct)) return "png";
  if (/webp/i.test(ct)) return "webp";
  if (/gif/i.test(ct)) return "gif";
  return "jpg";
}

async function uploadBytesToAirtable(baseId, recordId, buffer, contentType, filename) {
  const endpoint = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}/${recordId}/${encodeURIComponent(
    FIELD
  )}/uploadAttachment`;
  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contentType,
      file: buffer.toString("base64"),
      filename,
    }),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`uploadAttachment ${res.status}: ${text.slice(0, 300)}`);
  const json = text ? JSON.parse(text) : {};
  return json?.fields?.[FIELD] || json?.records?.[0]?.fields?.[FIELD];
}

async function listOpeningRows(base, brandFilter) {
  let formula = `{Slot Key} = "${SLOT}"`;
  if (brandFilter) {
    const esc = brandFilter.replace(/"/g, '\\"');
    formula = `AND({Slot Key} = "${SLOT}", {Brand Name} = "${esc}")`;
  }
  return base(TABLE).select({ filterByFormula: formula, maxRecords: 1000 }).all();
}

async function main() {
  const { dryRun, brandFilter, force } = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const key = process.env.AIRTABLE_API_KEY;
  if (!baseId || !key) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await listOpeningRows(base, brandFilter);
  const targets = rows.filter((r) => {
    const imgs = r.get(FIELD);
    return force || !Array.isArray(imgs) || !imgs.length || !imgs[0]?.thumbnails;
  });

  console.log(`${dryRun ? "[dry-run] " : ""}${targets.length} footprint.openings row(s) to process.`);

  let updated = 0;
  let skippedNoUrl = 0;
  let skippedNoMap = 0;
  let skippedDownload = 0;

  for (const row of targets) {
    const brand = String(row.get("Brand Name") || "").trim();
    const title = String(row.get("Title") || "").trim();
    const pageUrl = extractPropertyUrlFromBody(String(row.get("Body") || ""));
    if (!pageUrl) {
      skippedNoUrl += 1;
      continue;
    }
    const imageUrl = resolveFootprintOpeningImageUrl(pageUrl);
    if (!imageUrl) {
      console.log(`- ${brand}: skip "${title}" (no hoteldam mapping for ${pageUrl})`);
      skippedNoMap += 1;
      continue;
    }

    console.log(`- ${brand}: ${title}`);
    console.log(`  ${imageUrl}`);

    if (dryRun) {
      updated += 1;
      continue;
    }

    try {
      const downloaded = await downloadHoteldamImage(imageUrl, pageUrl);
      if (downloaded) {
        const { buffer, contentType } = downloaded;
        if (buffer.length > MAX_BYTES) {
          console.log(`  skip: ${buffer.length} bytes > 5MB`);
          skippedDownload += 1;
          continue;
        }
        const ext = extFromContentType(contentType);
        await uploadBytesToAirtable(baseId, row.id, buffer, contentType, `${row.id}.${ext}`);
        updated += 1;
        continue;
      }

      console.log(`  fallback: Airtable fetch from URL`);
      await base(TABLE).update(row.id, { [FIELD]: [{ url: imageUrl }] });
      updated += 1;
    } catch (err) {
      console.log(`  error: ${err.message}`);
      skippedDownload += 1;
    }
    await new Promise((r) => setTimeout(r, 200));
  }

  console.log(
    `${dryRun ? "Would update" : "Updated"} ${updated}; ` +
      `skipped: ${skippedNoUrl} no URL, ${skippedNoMap} no map, ${skippedDownload} download/upload fail.`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
