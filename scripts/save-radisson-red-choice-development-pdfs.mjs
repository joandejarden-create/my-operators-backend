/**
 * Save Radisson RED by Choice development PDFs to Brand Reference Material.
 *
 * Sources:
 *  - Choice CALA shared drive: PIP Template-Radisson RED_2022.pdf
 *  - Radisson Hotel Group (global RED positioning / owner matrix): Enjoy It brochure
 *  - Choice Upscale by Choice overview (Showpad PDF; includes upscale portfolio incl. RED)
 *
 *   node scripts/save-radisson-red-choice-development-pdfs.mjs
 *   node scripts/save-radisson-red-choice-development-pdfs.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildReferenceMaterialPaths,
  ensureReferenceDirectory,
  writeCaptureReadme,
  appendCaptureLog,
  resolveReferenceRoot,
} from "../lib/partner-intelligence/reference-material-paths.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const APPLY = process.argv.includes("--apply");
const COMPANY = "Choice Hotels International";
const BRAND = "Radisson RED by Choice";

const CALA_PIP = path.join(
  "G:",
  "Shared drives",
  "Choice Hotels",
  "Choice Hotels (CALA)",
  "Choice Hotels - CALA Regionalization - 2024",
  "Data Requests",
  "Sample Choice PIP Reports",
  "PIP Template-Radisson RED_2022.pdf"
);

const REMOTE_PDFS = [
  {
    url: "https://media.radissonhotels.net/image/corporate--business-development/miscellaneous/16256-140839-m24144694.pdf",
    title: "Radisson RED - Enjoy It development brochure (RHG 2022)",
    typeKey: "development-brochure",
    note: "Global RHG owner brochure; RED positioning, brand matrix, design approach. Americas RED owned by Choice.",
  },
  {
    url: "https://choicehotels.showpad.com/catalog/share/SfMhnGxzH6UyqBkszxhTD/5bb80fc7a83be9333222334ddd7095a3/14238909b96192f916dc2208539a3cee2b1e549a04ba54fe8b2d0de0e16eccce/processed",
    title: "Upscale by Choice brand overview guide",
    typeKey: "development-brochure",
    note: "Choice Hotels development — upscale portfolio overview (Showpad).",
  },
];

async function downloadPdf(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "DealalityReferenceCapture/1.0",
      Accept: "application/pdf,*/*",
    },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.slice(0, 4).toString() !== "%PDF") {
    throw new Error(`Not a PDF (${buf.length} bytes): ${url}`);
  }
  return buf;
}

function planLocalCopy(sourcePath, title) {
  const paths = buildReferenceMaterialPaths({
    companyFolder: COMPANY,
    brandName: BRAND,
    typeKey: "prototype",
    title,
    ext: path.extname(sourcePath) || ".pdf",
  });
  return { sourcePath, title, paths, kind: "local-copy" };
}

function planRemote(remote) {
  const paths = buildReferenceMaterialPaths({
    companyFolder: COMPANY,
    brandName: BRAND,
    typeKey: remote.typeKey,
    title: remote.title,
    ext: ".pdf",
  });
  return { ...remote, paths, kind: "download" };
}

async function main() {
  const root = resolveReferenceRoot();
  const jobs = [
    planLocalCopy(CALA_PIP, "PIP Template - Radisson RED (2022)"),
    ...REMOTE_PDFS.map(planRemote),
  ];

  console.log("Reference root:", root);
  console.log("Mode:", APPLY ? "apply" : "dry-run");
  console.log("");

  const report = { saved: [], skipped: [], errors: [] };

  for (const job of jobs) {
    console.log(`→ ${job.title}`);
    console.log(`  ${job.paths.relativePath}`);
    if (job.note) console.log(`  Note: ${job.note}`);

    if (job.kind === "local-copy" && !fs.existsSync(job.sourcePath)) {
      const msg = `Source missing: ${job.sourcePath}`;
      console.log(`  SKIP: ${msg}`);
      report.skipped.push({ title: job.title, reason: msg });
      continue;
    }

    if (!APPLY) {
      report.skipped.push({ title: job.title, reason: "dry-run" });
      console.log("  (dry-run — use --apply to write)");
      continue;
    }

    try {
      ensureReferenceDirectory(job.paths.absoluteDir);
      writeCaptureReadme(COMPANY, path.join(root, job.paths.companyFolder));

      let buf;
      if (job.kind === "local-copy") {
        buf = fs.readFileSync(job.sourcePath);
      } else {
        buf = await downloadPdf(job.url);
      }

      fs.writeFileSync(job.paths.absoluteFile, buf);
      appendCaptureLog(COMPANY, {
        url: job.url || job.sourcePath,
        relativePath: job.paths.relativePath,
        typeKey: job.typeKey || "prototype",
        brand: BRAND,
        title: job.title,
      });
      console.log(`  Saved ${buf.length} bytes`);
      report.saved.push({
        title: job.title,
        absoluteFile: job.paths.absoluteFile,
        bytes: buf.length,
      });
    } catch (err) {
      console.log(`  ERROR: ${err.message}`);
      report.errors.push({ title: job.title, error: err.message });
    }
  }

  const manifestPath = path.join(root, COMPANY, "brands", BRAND, "development", "capture-manifest.json");
  if (APPLY) {
    fs.mkdirSync(path.dirname(manifestPath), { recursive: true });
    fs.writeFileSync(
      manifestPath,
      JSON.stringify({ capturedAt: new Date().toISOString(), ...report }, null, 2),
      "utf8"
    );
    console.log("\nManifest:", manifestPath);
  }

  console.log(`\nDone: ${report.saved.length} saved, ${report.skipped.length} skipped, ${report.errors.length} errors`);
  if (report.errors.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
