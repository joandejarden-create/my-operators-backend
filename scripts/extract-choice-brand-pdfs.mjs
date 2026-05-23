/**
 * Extract text from Choice brand materials PDFs into fixtures/choice-pdf-text/
 * node scripts/extract-choice-brand-pdfs.mjs
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import {
  CHOICE_MATERIALS_ROOT,
  FOLDER_TO_AIRTABLE_NAME,
  ROOT_PDF_BY_BRAND,
  PDF_PRIORITY,
} from "./lib/choice-brand-materials-config.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "fixtures", "choice-pdf-text");
const PY = path.join(__dirname, "lib", "extract-pdf-text.py");

function pickPdf(files) {
  const pdfs = files.filter((f) => /\.pdf$/i.test(f));
  for (const pat of PDF_PRIORITY) {
    const hit = pdfs.find((f) => pat.test(f));
    if (hit) return hit;
  }
  return pdfs[0] || null;
}

function extract(pdfPath) {
  const r = spawnSync("python", [PY, pdfPath], { encoding: "utf8", maxBuffer: 20 * 1024 * 1024 });
  if (r.error) throw r.error;
  if (r.status !== 0) throw new Error(r.stderr || `python failed: ${pdfPath}`);
  return r.stdout;
}

fs.mkdirSync(OUT, { recursive: true });

const manifest = [];

for (const [folder, brandName] of Object.entries(FOLDER_TO_AIRTABLE_NAME)) {
  const dir = path.join(CHOICE_MATERIALS_ROOT, folder);
  if (!fs.existsSync(dir)) {
    console.warn(`Skip missing folder: ${dir}`);
    continue;
  }
  const files = fs.readdirSync(dir);
  const pdf = pickPdf(files);
  if (!pdf) {
    console.warn(`No PDF in ${folder}`);
    continue;
  }
  const full = path.join(dir, pdf);
  const slug = brandName.replace(/[^\w]+/g, "-").toLowerCase();
  const text = extract(full);
  const outFile = path.join(OUT, `${slug}.txt`);
  fs.writeFileSync(outFile, text, "utf8");
  manifest.push({ brandName, folder, pdf, outFile: path.relative(ROOT, outFile) });
  console.log(`OK ${brandName} <- ${pdf}`);
}

for (const [brandName, pdfs] of Object.entries(ROOT_PDF_BY_BRAND)) {
  for (const rel of pdfs) {
    const full = path.join(CHOICE_MATERIALS_ROOT, rel);
    if (!fs.existsSync(full)) continue;
    const slug = brandName.replace(/[^\w]+/g, "-").toLowerCase();
    const text = extract(full);
    const outFile = path.join(OUT, `${slug}.txt`);
    fs.writeFileSync(outFile, text, "utf8");
    manifest.push({ brandName, folder: "(root)", pdf: rel, outFile: path.relative(ROOT, outFile) });
    console.log(`OK ${brandName} <- ${rel}`);
  }
}

fs.writeFileSync(path.join(OUT, "manifest.json"), JSON.stringify(manifest, null, 2));
