/**
 * Extract text from all numbered FDD PDFs in Dealality CHI folder.
 * node scripts/extract-choice-fdd-item19.mjs
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PY = path.join(__dirname, "lib", "extract-pdf-text.py");
const OUT = path.join(ROOT, "fixtures", "choice-fdd-text");
const FDD_RE = /^\d{5}-\d{6}-\d{2}\.pdf$/i;
const FDD_DIR_NAME = "FDDs";

export function resolveChiRoot() {
  if (process.env.CHOICE_BRAND_REFERENCE_ROOT) return process.env.CHOICE_BRAND_REFERENCE_ROOT;
  const gDrive = "G:\\My Drive";
  if (!fs.existsSync(gDrive)) return null;
  const deal = fs
    .readdirSync(gDrive, { withFileTypes: true })
    .find((d) => d.isDirectory() && /^Dealality/i.test(d.name));
  if (!deal) return null;
  return path.join(
    gDrive,
    deal.name,
    "Platform Design & Build",
    "Brand Reference Material",
    "Choice Hotels International"
  );
}

function loadFddManifest(fddDir) {
  const manifestPath = path.join(fddDir, "fdd-filename-manifest.json");
  if (!fs.existsSync(manifestPath)) return null;
  try {
    const data = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
    return Array.isArray(data.files) ? data.files : null;
  } catch {
    return null;
  }
}

function listFddPdfs(chiRoot) {
  /** @type {Map<string, { full: string, slug: string }>} */
  const files = new Map();
  const fddDir = path.join(chiRoot, FDD_DIR_NAME);
  if (fs.existsSync(fddDir)) {
    const manifest = loadFddManifest(fddDir);
    if (manifest?.length) {
      for (const row of manifest) {
        const full = path.join(fddDir, row.pdf);
        if (fs.existsSync(full)) files.set(row.stem, { full, slug: row.stem });
      }
      return [...files.values()].sort((a, b) => a.slug.localeCompare(b.slug));
    }
    for (const ent of fs.readdirSync(fddDir, { withFileTypes: true })) {
      if (!ent.isFile() || !ent.name.toLowerCase().endsWith(".pdf")) continue;
      const full = path.join(fddDir, ent.name);
      if (FDD_RE.test(ent.name)) {
        const slug = ent.name.replace(/\.pdf$/i, "");
        if (!files.has(slug)) files.set(slug, { full, slug });
      }
    }
    if (files.size) return [...files.values()].sort((a, b) => a.slug.localeCompare(b.slug));
  }

  function walk(dir) {
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) walk(full);
      else if (FDD_RE.test(ent.name)) {
        const slug = ent.name.replace(/\.pdf$/i, "");
        if (!files.has(slug)) files.set(slug, { full, slug });
      }
    }
  }
  walk(chiRoot);
  return [...files.values()].sort((a, b) => a.slug.localeCompare(b.slug));
}

function extract(pdfPath) {
  const r = spawnSync("python", [PY, pdfPath], {
    encoding: "utf8",
    maxBuffer: 40 * 1024 * 1024,
    env: { ...process.env, PYTHONIOENCODING: "utf-8" },
  });
  if (r.error) throw r.error;
  if (r.status !== 0) throw new Error(r.stderr || `extract failed: ${pdfPath}`);
  return r.stdout;
}

const chiRoot = resolveChiRoot();
if (!chiRoot) {
  console.error("Set CHOICE_BRAND_REFERENCE_ROOT or mount G:\\My Drive");
  process.exit(1);
}

const pdfs = listFddPdfs(chiRoot);
fs.mkdirSync(OUT, { recursive: true });
console.log(`Found ${pdfs.length} FDD PDFs under ${chiRoot}`);

for (const { slug, full } of pdfs) {
  const outFile = path.join(OUT, `${slug}.txt`);
  const name = path.basename(full);
  process.stdout.write(`Extract ${name} → ${slug}.txt... `);
  const text = extract(full);
  fs.writeFileSync(outFile, text, "utf8");
  console.log(`${(text.length / 1024).toFixed(0)} KB`);
}

console.log(`Done → ${path.relative(ROOT, OUT)}`);
