/**
 * Attach case study images by downloading bytes locally, then Airtable uploadAttachment API.
 * Fixes empty UI when { url } attach fails (Choice blocks Airtable fetch).
 *
 *   node scripts/attach-case-study-images-upload-bytes.mjs --dry-run
 *   node scripts/attach-case-study-images-upload-bytes.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import { IMAGE_BY_SOURCE_URL } from "./lib/case-study-image-url-map.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const FIELD = "Image";
const MAX_BYTES = 5 * 1024 * 1024;

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

function firstChoiceUrl(body) {
  const m = String(body || "").match(/https?:\/\/[^\s)]+/i);
  return m ? m[0] : "";
}

function extFromContentType(ct) {
  if (/png/i.test(ct)) return "png";
  if (/webp/i.test(ct)) return "webp";
  if (/gif/i.test(ct)) return "gif";
  return "jpg";
}

async function uploadBytesToAirtable(baseId, recordId, buffer, contentType, filename) {
  const endpoint = `https://content.airtable.com/v0/${baseId}/${recordId}/${FIELD}/uploadAttachment`;
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
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  if (!res.ok) {
    throw new Error(`uploadAttachment ${res.status}: ${text.slice(0, 300)}`);
  }
  const imgs = json?.fields?.[FIELD] || json?.records?.[0]?.fields?.[FIELD];
  return imgs;
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const key = process.env.AIRTABLE_API_KEY;
  if (!baseId || !key) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await base(TABLE)
    .select({ filterByFormula: `{Slot Key} = "materials.caseStudy"`, maxRecords: 1000 })
    .all();

  let updated = 0;
  let skipped = 0;
  for (const r of rows) {
    const imgs = r.get(FIELD);
    if (Array.isArray(imgs) && imgs.length > 0 && imgs[0]?.thumbnails) {
      continue;
    }

    const body = String(r.get("Body") || "");
    const srcUrl = firstChoiceUrl(body);
    const imageUrl = srcUrl ? IMAGE_BY_SOURCE_URL[srcUrl] : "";
    if (!imageUrl) {
      skipped++;
      continue;
    }

    const brand = String(r.get("Brand Name") || "").trim();
    const title = String(r.get("Title") || "").trim();
    console.log(`- ${brand}: ${title}`);

    if (dryRun) {
      updated++;
      continue;
    }

    const imgRes = await fetch(imageUrl, { redirect: "follow" });
    if (!imgRes.ok) {
      console.log(`  skip: download ${imgRes.status}`);
      skipped++;
      continue;
    }
    const buffer = Buffer.from(await imgRes.arrayBuffer());
    if (buffer.length > MAX_BYTES) {
      console.log(`  skip: ${buffer.length} bytes > 5MB`);
      skipped++;
      continue;
    }
    const contentType = imgRes.headers.get("content-type") || "image/jpeg";
    const ext = extFromContentType(contentType);
    const filename = `${r.id}.${ext}`;

    try {
      const out = await uploadBytesToAirtable(baseId, r.id, buffer, contentType, filename);
      const ok = Array.isArray(out) && out[0]?.thumbnails;
      console.log(`  ${ok ? "ok (cdn)" : "warn (no thumbnails yet)"}: ${out?.[0]?.url?.slice(0, 70) || ""}`);
      updated++;
    } catch (err) {
      console.log(`  error: ${err.message}`);
      skipped++;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }

  console.log(`${dryRun ? "Would update" : "Updated"} ${updated}; skipped ${skipped}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
