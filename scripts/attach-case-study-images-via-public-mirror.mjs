/**
 * Mirror hotel images to a temporary public URL, then attach to Airtable Image field.
 * Choice hoteldam URLs often fail when Airtable fetches them directly.
 *
 *   node scripts/attach-case-study-images-via-public-mirror.mjs --dry-run
 *   node scripts/attach-case-study-images-via-public-mirror.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import { IMAGE_BY_SOURCE_URL } from "./lib/case-study-image-url-map.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

function firstChoiceUrl(body) {
  const m = String(body || "").match(/https?:\/\/[^\s)]+/i);
  return m ? m[0] : "";
}

function hasStoredImage(imgs) {
  if (!Array.isArray(imgs) || !imgs.length) return false;
  const u = String(imgs[0]?.url || "");
  return u.includes("airtableusercontent.com") && !!imgs[0]?.thumbnails;
}

async function mirrorToPublicUrl(buffer, filename) {
  const form = new FormData();
  form.append("reqtype", "fileupload");
  form.append("fileToUpload", new Blob([buffer]), filename);
  const res = await fetch("https://catbox.moe/user/api.php", { method: "POST", body: form });
  if (!res.ok) throw new Error(`catbox ${res.status}`);
  const publicUrl = (await res.text()).trim();
  if (!/^https?:\/\//i.test(publicUrl)) throw new Error(`bad mirror url: ${publicUrl}`);
  return publicUrl;
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const rows = await base(TABLE)
    .select({ filterByFormula: `{Slot Key} = "materials.caseStudy"`, maxRecords: 1000 })
    .all();

  let updated = 0;
  let skipped = 0;

  for (const r of rows) {
    if (hasStoredImage(r.get("Image"))) continue;

    const srcUrl = firstChoiceUrl(String(r.get("Body") || ""));
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

    try {
      const imgRes = await fetch(imageUrl, { redirect: "follow" });
      if (!imgRes.ok) throw new Error(`download ${imgRes.status}`);
      const buffer = Buffer.from(await imgRes.arrayBuffer());
      const ext = /\.jpe?g/i.test(imageUrl) ? "jpg" : /\.png/i.test(imageUrl) ? "png" : "jpg";
      const publicUrl = await mirrorToPublicUrl(buffer, `${r.id}.${ext}`);
      console.log(`  mirror: ${publicUrl}`);

      const updatedRec = await base(TABLE).update(r.id, { Image: [{ url: publicUrl }] });
      let ok = false;
      let check = null;
      for (let attempt = 0; attempt < 4; attempt++) {
        await new Promise((resolve) => setTimeout(resolve, 2500));
        check = await base(TABLE).find(r.id);
        ok = hasStoredImage(check.get("Image"));
        if (ok) break;
      }
      console.log(`  ${ok ? "ok (Airtable CDN)" : "pending/failed"}: ${check?.get("Image")?.[0]?.url?.slice(0, 60) || "none"}`);
      if (ok) updated++;
      else skipped++;
    } catch (err) {
      console.log(`  error: ${err.message}`);
      skipped++;
    }
    await new Promise((resolve) => setTimeout(resolve, 400));
  }

  console.log(`${dryRun ? "Would update" : "Updated"} ${updated}; skipped ${skipped}.`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
