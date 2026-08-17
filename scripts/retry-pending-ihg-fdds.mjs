/**
 * Retry pending MN FDD downloads from reports/ihg-state-fdd-harvest.json
 * Uses long delays + fresh browser per file to avoid 429/403.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import puppeteer from "puppeteer";
import {
  buildReferenceMaterialPaths,
  ensureReferenceDirectory,
  writeCaptureReadme,
  appendCaptureLog,
  resolveReferenceRoot,
  sanitizeFileName,
} from "../lib/partner-intelligence/reference-material-paths.js";
import { sleep } from "../lib/partner-intelligence/harvest-browser-capture.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const COMPANY = "IHG Hotels & Resorts";
const APPLY = process.argv.includes("--apply");

const PENDING = [
  { brand: "Ruby", year: "2025", downloadUrl: "https://cards.web.commerce.state.mn.us/documents/%7B60DF279B-0000-C819-8606-43C9FCF12D24%7D/download?documentClass=FRANCHISE_REGISTRATIONS&contentSequence=0" },
  { brand: "Vignette Collection", year: "2024", downloadUrl: "https://cards.web.commerce.state.mn.us/documents/%7BA0D01391-0000-CC1E-917F-4B3F338C5E79%7D/download?documentClass=FRANCHISE_REGISTRATIONS&contentSequence=0" },
  { brand: "Voco Hotels", year: "2024", downloadUrl: "https://cards.web.commerce.state.mn.us/documents/%7B5097EF90-0000-C91A-BC6E-05987E6B6F64%7D/download?documentClass=FRANCHISE_REGISTRATIONS&contentSequence=0" },
];

async function downloadOne(asset) {
  const browser = await puppeteer.launch({ headless: "new" });
  const page = await browser.newPage();
  await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36");
  await page.goto("https://cards.web.commerce.state.mn.us/franchise-registrations", { waitUntil: "networkidle2", timeout: 120000 });
  await sleep(5000);
  const bytes = await page.evaluate(async (url) => {
    const res = await fetch(url, { credentials: "include" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return Array.from(new Uint8Array(await res.arrayBuffer()));
  }, asset.downloadUrl);
  await browser.close();
  return Buffer.from(bytes);
}

function saveFdd(asset, buf) {
  const paths = buildReferenceMaterialPaths({
    companyFolder: COMPANY,
    brandName: asset.brand,
    typeKey: "fdd",
    title: sanitizeFileName(`${asset.brand} FDD ${asset.year} (MN state filing)`),
    ext: ".pdf",
  });
  ensureReferenceDirectory(paths.absoluteDir);
  writeCaptureReadme(COMPANY, path.join(resolveReferenceRoot(), COMPANY));
  fs.writeFileSync(paths.absoluteFile, buf);
  return paths.relativePath;
}

for (const asset of PENDING) {
  const dest = buildReferenceMaterialPaths({
    companyFolder: COMPANY,
    brandName: asset.brand,
    typeKey: "fdd",
    title: sanitizeFileName(`${asset.brand} FDD ${asset.year} (MN state filing)`),
    ext: ".pdf",
  });
  if (fs.existsSync(dest.absoluteFile)) {
    console.log("SKIP exists", asset.brand);
    continue;
  }
  if (!APPLY) {
    console.log("WOULD", asset.brand);
    continue;
  }
  try {
    console.log("Downloading", asset.brand, "…");
    const buf = await downloadOne(asset);
    if (!buf.slice(0, 5).toString().startsWith("%PDF")) throw new Error("Not PDF");
    console.log("OK", saveFdd(asset, buf), buf.length);
  } catch (e) {
    console.warn("FAIL", asset.brand, e.message);
  }
  await sleep(15000);
}
