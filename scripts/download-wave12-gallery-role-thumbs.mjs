#!/usr/bin/env node
/**
 * Download one thumbnail per distinct Wave 12 gallery stem for visual role curation.
 * Writes to data/wave12-gallery-role-thumbs/{slug}/
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";
import { buildImageIdentity } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures");
const OUT = path.join(ROOT, "data", "wave12-gallery-role-thumbs");

function thumbUrl(url) {
  const u = String(url || "");
  if (/digital\.ihg\.com\/is\/image/i.test(u)) {
    // Scene7 resize
    const base = u.split("?")[0];
    return `${base}?wid=480&hei=320&fit=constrain`;
  }
  if (/cache\.marriott\.com/i.test(u)) {
    return u.includes("?") ? u : `${u}?output-quality=60&downsize=480px:*`;
  }
  return u;
}

async function main() {
  const brandIdx = process.argv.indexOf("--brands");
  const brands =
    brandIdx >= 0 && process.argv[brandIdx + 1]
      ? process.argv[brandIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : [...WAVE12_SLUGS];
  const maxPer = Number(process.argv.find((a, i, arr) => arr[i - 1] === "--max") || 18);

  fs.mkdirSync(OUT, { recursive: true });
  const manifest = {};

  for (const slug of brands) {
    const fixturePath = path.join(FIXTURES, `wave12-${slug}-gallery-pool.json`);
    if (!fs.existsSync(fixturePath)) continue;
    const rows = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
    const dir = path.join(OUT, slug);
    fs.mkdirSync(dir, { recursive: true });
    const seen = new Set();
    const entries = [];
    for (const row of rows) {
      const id = buildImageIdentity(row.imageUrl);
      if (!id.duplicateGroupId || seen.has(id.duplicateGroupId)) continue;
      seen.add(id.duplicateGroupId);
      if (entries.length >= maxPer) break;
      const stem = (id.sourceImageId || id.duplicateGroupId).replace(/[^a-z0-9._-]+/gi, "_").slice(0, 80);
      const ext = /\.png/i.test(row.imageUrl) ? "png" : "jpg";
      const file = `${String(entries.length + 1).padStart(2, "0")}_${stem}.${ext}`;
      const dest = path.join(dir, file);
      const url = thumbUrl(row.imageUrl);
      try {
        const res = await fetch(url, {
          headers: { "User-Agent": "Mozilla/5.0 DealalityRoleCurate/1.0" },
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const buf = Buffer.from(await res.arrayBuffer());
        fs.writeFileSync(dest, buf);
        entries.push({
          file,
          stem: id.sourceImageId || id.duplicateGroupId,
          imageUrl: row.imageUrl,
          propertyName: row.propertyName || "",
        });
        process.stdout.write(".");
      } catch (err) {
        entries.push({
          file: null,
          stem: id.sourceImageId || id.duplicateGroupId,
          imageUrl: row.imageUrl,
          error: err.message,
        });
        process.stdout.write("x");
      }
    }
    manifest[slug] = entries;
    console.log(`\n${slug}: ${entries.filter((e) => e.file).length} thumbs`);
  }

  fs.writeFileSync(path.join(OUT, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  console.log(`Wrote ${path.join(OUT, "manifest.json")}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
