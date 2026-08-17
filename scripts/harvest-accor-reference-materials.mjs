#!/usr/bin/env node
/**
 * Harvest Accor hotel-development / management pages into Brand Reference Material/Accor.
 *
 *   node scripts/harvest-accor-reference-materials.mjs --dry-run
 *   node scripts/harvest-accor-reference-materials.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import puppeteer from "puppeteer";
import { captureHtmlWithBrowser } from "../lib/partner-intelligence/harvest-browser-capture.js";
import {
  buildReferenceMaterialPaths,
  ensureReferenceDirectory,
  resolveReferenceRoot,
  writeCaptureReadme,
  REFERENCE_SUBFOLDERS,
} from "../lib/partner-intelligence/reference-material-paths.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const COMPANY = "Accor";
const APPLY = process.argv.includes("--apply");

const HTML_PAGES = [
  {
    url: "https://group.accor.com/en/hotel-development",
    title: "Develop with Accor",
    typeKey: "development-brochure",
    category: "Hotel development",
  },
  {
    url: "https://group.accor.com/en/hotel-development/solutions-for-every-project",
    title: "Accor solutions for every project — partnership models",
    typeKey: "development-brochure",
    category: "Operating model",
  },
  {
    url: "https://group.accor.com/en/hotel-development/maximize-your-revenue",
    title: "Accor maximize your revenue",
    typeKey: "development-brochure",
    category: "Commercial strategy",
  },
  {
    url: "https://group.accor.com/en/group",
    title: "Accor at a glance",
    typeKey: "development-brochure",
    category: "General company overview",
  },
];

const PDFS = [
  {
    url: "https://assets.group.accor.com/yrj0orc8tx24/1gfnIc7BnVhT7xnnVrKMYj/5cea7a817eaa174b0193af9b8b592a58/Accor_Overview_2026.pdf",
    title: "Accor Overview 2026",
    typeKey: "development-brochure",
    category: "Portfolio overview",
  },
  {
    url: "https://assets.group.accor.com/yrj0orc8tx24/4bpCURWZXqOBx8dEYvSANx/1b76f77a967e522e3d496aff9d4e1f57/Why_Join_Accor_2026.pdf",
    title: "Why Join Accor 2026",
    typeKey: "development-brochure",
    category: "Owner development presentation",
  },
];

async function downloadPdf(asset, refRoot) {
  const paths = buildReferenceMaterialPaths({
    companyFolder: COMPANY,
    typeKey: asset.typeKey,
    title: asset.title,
    ext: ".pdf",
    referenceRoot: refRoot,
  });
  ensureReferenceDirectory(paths.absoluteDir);
  if (!APPLY) {
    return { wouldWrite: paths.relativePath, url: asset.url };
  }
  const res = await fetch(asset.url, {
    headers: { "User-Agent": "DealalityReferenceCapture/1.0 (+https://dealality.com)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`PDF fetch ${res.status}: ${asset.url}`);
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(paths.absoluteFile, buf);
  return { relativePath: paths.relativePath, bytes: buf.length, url: asset.url };
}

async function main() {
  const refRoot = resolveReferenceRoot();
  const companyDir = path.join(refRoot, COMPANY);
  fs.mkdirSync(companyDir, { recursive: true });
  for (const sub of Object.values(REFERENCE_SUBFOLDERS)) {
    fs.mkdirSync(path.join(companyDir, sub), { recursive: true });
  }
  writeCaptureReadme(COMPANY, companyDir);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    company: COMPANY,
    referenceRoot: refRoot,
    html: [],
    pdfs: [],
    errors: [],
  };

  console.log(`[accor-harvest] dryRun=${!APPLY} root=${refRoot}`);

  for (const pdf of PDFS) {
    try {
      const r = await downloadPdf(pdf, refRoot);
      report.pdfs.push({ title: pdf.title, ...r });
      console.log("PDF", APPLY ? "OK" : "WOULD", r.relativePath || r.wouldWrite);
    } catch (err) {
      report.errors.push({ title: pdf.title, error: err.message });
      console.warn("PDF FAIL", err.message);
    }
  }

  if (APPLY) {
    const browser = await puppeteer.launch({ headless: "new" });
    try {
      for (const page of HTML_PAGES) {
        console.log("HTML", page.title);
        try {
          const result = await captureHtmlWithBrowser(browser, {
            url: page.url,
            title: page.title,
            companyFolder: COMPANY,
            typeKey: page.typeKey,
            category: page.category,
            referenceRoot: refRoot,
            alsoMhtml: true,
            alsoPdf: false,
            gotoTimeout: 120000,
            warmOrigin: "https://group.accor.com/",
          });
          report.html.push({ title: page.title, url: page.url, ...result });
          console.log("  OK", result.relativePath);
          await new Promise((r) => setTimeout(r, 3000));
        } catch (err) {
          report.errors.push({ title: page.title, url: page.url, error: err.message });
          console.warn("  FAIL", err.message);
        }
      }
    } finally {
      await browser.close();
    }
  } else {
    for (const page of HTML_PAGES) {
      report.html.push({ title: page.title, url: page.url, wouldCapture: true });
      console.log("HTML WOULD", page.url);
    }
  }

  const outJson = path.join(ROOT, "reports", "harvest-accor-reference-materials.json");
  fs.mkdirSync(path.dirname(outJson), { recursive: true });
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2));
  console.log("Wrote", outJson);
  console.log(
    `summary html=${report.html.length} pdfs=${report.pdfs.length} errors=${report.errors.length}`
  );
  if (report.errors.length && APPLY) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
