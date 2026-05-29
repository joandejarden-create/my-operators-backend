import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { extractPropertyUrlFromBody } from "./lib/choice-hotel-page-image.mjs";
import { resolveFootprintOpeningImageUrl } from "./lib/choice-footprint-opening-image-map.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const SLOT = "footprint.openings";
const REVIEW_CSV = path.resolve(
  ROOT,
  "reports/independent-census-promotion-review-choice-brand-directory-2026-05-20.csv"
);
const OUT_CSV = path.resolve(ROOT, "reports/footprint-openings-image-upload-manifest.csv");
const OUT_JSON = path.resolve(ROOT, "reports/footprint-openings-image-upload-manifest.json");

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

function csvEscape(v) {
  const s = String(v ?? "");
  if (!/[",\n]/.test(s)) return s;
  return `"${s.replace(/"/g, '""')}"`;
}

function propertyIdFromUrl(url) {
  const parts = String(url || "").replace(/\/$/, "").split("/");
  return (parts[parts.length - 1] || "").toLowerCase();
}

function loadOfficialSitesByPropertyId() {
  const map = new Map();
  const csv = fs.readFileSync(REVIEW_CSV, "utf8");
  const rows = parseCsv(csv);
  for (const row of rows) {
    const pid = String(row.choicePropertyId || "").trim().toLowerCase();
    const website = String(row.candidateWebsite || "").trim();
    if (!pid || !website || !/^https?:\/\//i.test(website)) continue;
    if (!map.has(pid)) map.set(pid, website);
  }
  return map;
}

async function main() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const officialSiteByPid = loadOfficialSitesByPropertyId();
  const base = new Airtable({ apiKey: key }).base(baseId);
  const rows = await base(TABLE)
    .select({ filterByFormula: `{Slot Key} = "${SLOT}"`, maxRecords: 1000 })
    .all();

  const manifest = rows.map((r) => {
    const body = String(r.get("Body") || "");
    const propertyUrl = extractPropertyUrlFromBody(body);
    const propertyId = propertyIdFromUrl(propertyUrl);
    const mappedHoteldam = propertyUrl ? resolveFootprintOpeningImageUrl(propertyUrl) : "";
    const officialSite = propertyId ? officialSiteByPid.get(propertyId) || "" : "";
    const existingImages = r.get("Image");
    const hasThumbnail =
      Array.isArray(existingImages) && existingImages.length > 0 && !!existingImages[0]?.thumbnails;
    return {
      recordId: r.id,
      brandName: String(r.get("Brand Name") || ""),
      title: String(r.get("Title") || ""),
      propertyUrl,
      propertyId,
      officialWebsite: officialSite,
      recommendedImageUrl: mappedHoteldam,
      hasThumbnail,
      currentImageUrl:
        Array.isArray(existingImages) && existingImages.length ? String(existingImages[0]?.url || "") : "",
      notes: mappedHoteldam
        ? "Use mapped hotel photo URL for manual attach if thumbnail missing."
        : officialSite
        ? "Open officialWebsite and upload hero image manually."
        : "No mapped image URL or official website in review CSV.",
    };
  });

  const headers = [
    "recordId",
    "brandName",
    "title",
    "propertyUrl",
    "propertyId",
    "officialWebsite",
    "recommendedImageUrl",
    "hasThumbnail",
    "currentImageUrl",
    "notes",
  ];

  const csvLines = [headers.join(",")];
  for (const row of manifest) {
    csvLines.push(headers.map((h) => csvEscape(row[h])).join(","));
  }

  fs.mkdirSync(path.dirname(OUT_CSV), { recursive: true });
  fs.writeFileSync(OUT_CSV, csvLines.join("\n") + "\n");
  fs.writeFileSync(OUT_JSON, JSON.stringify(manifest, null, 2) + "\n");

  const withThumb = manifest.filter((r) => r.hasThumbnail).length;
  const withMapped = manifest.filter((r) => r.recommendedImageUrl).length;
  const withOfficialSite = manifest.filter((r) => r.officialWebsite).length;
  console.log(
    `Wrote manifest for ${manifest.length} rows. ` +
      `thumbnails=${withThumb}, mappedImageUrl=${withMapped}, officialWebsite=${withOfficialSite}`
  );
  console.log(OUT_CSV);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

