import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { extractPropertyUrlFromBody } from "./lib/choice-hotel-page-image.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const SLOT = "footprint.openings";
const FIELD = "Image";
const MAX_BYTES = 5 * 1024 * 1024;
const REVIEW_CSV = path.resolve(
  ROOT,
  "reports/independent-census-promotion-review-choice-brand-directory-2026-05-20.csv"
);

const FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
};

function parseArgs(argv) {
  const i = argv.indexOf("--brand");
  return {
    dryRun: argv.includes("--dry-run"),
    force: argv.includes("--force"),
    brandFilter: i >= 0 ? String(argv[i + 1] || "").trim() : "",
  };
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (c === '"') inQ = false;
      else cur += c;
    } else if (c === '"') inQ = true;
    else if (c === ",") {
      out.push(cur);
      cur = "";
    } else cur += c;
  }
  out.push(cur);
  return out;
}

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}

function propertyIdFromUrl(url) {
  const parts = String(url || "").replace(/\/$/, "").split("/");
  return String(parts[parts.length - 1] || "").toLowerCase();
}

function loadOfficialWebsiteMap() {
  const csv = fs.readFileSync(REVIEW_CSV, "utf8");
  const rows = parseCsv(csv);
  const map = new Map();
  for (const row of rows) {
    const pid = String(row.choicePropertyId || "").trim().toLowerCase();
    const site = String(row.candidateWebsite || "").trim();
    if (!pid || !site || !/^https?:\/\//i.test(site)) continue;
    if (!map.has(pid)) map.set(pid, site);
  }
  return map;
}

function pickMetaImage(html) {
  const tags = html.match(/<meta[^>]*>/gi) || [];
  for (const tag of tags) {
    const key =
      (tag.match(/\bproperty=["']([^"']+)["']/i)?.[1] ||
        tag.match(/\bname=["']([^"']+)["']/i)?.[1] ||
        "")
        .trim()
        .toLowerCase();
    if (!["og:image", "twitter:image", "twitter:image:src"].includes(key)) continue;
    const content = tag.match(/\bcontent=["']([^"']+)["']/i)?.[1];
    if (content) return content.trim();
  }
  const link = html.match(/<link[^>]+rel=["']image_src["'][^>]+href=["']([^"']+)["'][^>]*>/i);
  if (link?.[1]) return link[1].trim();
  const inlineHoteldam = html.match(
    /https:\/\/www\.choicehotels\.com\/hoteldam\/[^"'\s]+?\.(?:jpg|jpeg|png|webp)/i
  );
  if (inlineHoteldam?.[0]) return inlineHoteldam[0];
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
  const attempts = [
    () => fetch(url, { redirect: "follow" }),
    () => fetch(url, { headers: FETCH_HEADERS, redirect: "follow" }),
  ];
  for (const get of attempts) {
    try {
      const res = await get();
      if (!res.ok) continue;
      const ct = res.headers.get("content-type") || "";
      if (!/html|xml|text/i.test(ct)) continue;
      return await res.text();
    } catch {
      // ignore and try next strategy
    }
  }
  return "";
}

async function fetchImageBytes(url, referer = "") {
  const headersWithRef = referer ? { ...FETCH_HEADERS, Referer: referer } : FETCH_HEADERS;
  const attempts = [
    () => fetch(url, { redirect: "follow" }),
    () => fetch(url, { headers: headersWithRef, redirect: "follow" }),
  ];
  for (const get of attempts) {
    try {
      const res = await get();
      if (!res.ok) continue;
      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("image")) continue;
      const buf = Buffer.from(await res.arrayBuffer());
      if (!buf.length || buf.length > MAX_BYTES) continue;
      return { buffer: buf, contentType: ct };
    } catch {
      // ignore and try next strategy
    }
  }
  return null;
}

async function listRows(base, brandFilter) {
  let formula = `{Slot Key} = "${SLOT}"`;
  if (brandFilter) {
    const esc = brandFilter.replace(/"/g, '\\"');
    formula = `AND({Slot Key} = "${SLOT}", {Brand Name} = "${esc}")`;
  }
  return base(TABLE).select({ filterByFormula: formula, maxRecords: 1000 }).all();
}

async function main() {
  const { dryRun, force, brandFilter } = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const key = process.env.AIRTABLE_API_KEY;
  if (!baseId || !key) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const websiteByPid = loadOfficialWebsiteMap();
  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await listRows(base, brandFilter);
  const targets = rows.filter((r) => {
    if (force) return true;
    const imgs = r.get(FIELD);
    return !Array.isArray(imgs) || !imgs.length || !imgs[0]?.thumbnails;
  });

  console.log(`${dryRun ? "[dry-run] " : ""}${targets.length} row(s) to process.`);

  let updated = 0;
  let skippedNoPid = 0;
  let skippedNoSite = 0;
  let skippedNoImage = 0;
  let failed = 0;

  for (const row of targets) {
    const brand = String(row.get("Brand Name") || "").trim();
    const title = String(row.get("Title") || "").trim();
    const pageUrl = extractPropertyUrlFromBody(String(row.get("Body") || ""));
    const pid = propertyIdFromUrl(pageUrl);
    if (!pid || pid.length < 4) {
      skippedNoPid += 1;
      continue;
    }
    const site = websiteByPid.get(pid) || pageUrl;
    if (!site) {
      console.log(`- ${brand}: skip "${title}" (no official website for ${pid})`);
      skippedNoSite += 1;
      continue;
    }

    try {
      const html = await fetchText(site);
      if (!html) {
        console.log(`- ${brand}: skip "${title}" (site blocked: ${site})`);
        skippedNoImage += 1;
        continue;
      }
      const metaImage = pickMetaImage(html);
      const imageUrl = absolutize(site, metaImage);
      if (!imageUrl) {
        console.log(`- ${brand}: skip "${title}" (no official meta image)`);
        skippedNoImage += 1;
        continue;
      }
      const img = await fetchImageBytes(imageUrl, site);
      if (!img) {
        console.log(`- ${brand}: skip "${title}" (image blocked/unavailable)`);
        skippedNoImage += 1;
        continue;
      }

      console.log(`- ${brand}: ${title}`);
      console.log(`  site: ${site}`);
      console.log(`  img:  ${imageUrl}`);

      if (!dryRun) {
        await base(TABLE).update(row.id, { [FIELD]: [{ url: imageUrl }] });
      }
      updated += 1;
    } catch (err) {
      console.log(`- ${brand}: "${title}" error: ${err.message}`);
      failed += 1;
    }

    await new Promise((r) => setTimeout(r, 200));
  }

  console.log(
    `${dryRun ? "Would update" : "Updated"} ${updated}; skipped: ${skippedNoPid} no pid, ` +
      `${skippedNoSite} no site, ${skippedNoImage} no image, ${failed} errors.`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

