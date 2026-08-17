/**
 * Rename Choice CHI FDD PDFs: "{Brand Name} FDD {Year}.pdf"
 * Uses fixtures/choice-fdd-text/*.txt headers when available; falls back to registration id in filename.
 *
 *   node scripts/rename-choice-fdd-pdfs.mjs --dry-run
 *   node scripts/rename-choice-fdd-pdfs.mjs
 *   node scripts/rename-choice-fdd-pdfs.mjs --dir "G:\\My Drive\\..."
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_DIR =
  "G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material\\Choice Hotels International\\FDDs";
const TEXT_DIR = path.join(ROOT, "fixtures", "choice-fdd-text");
const FDD_RE = /^(\d{5})-(\d{6})-(\d{2})\.pdf$/i;

/** @param {string} headerLine */
function brandFromHeader(headerLine) {
  const h = String(headerLine || "").trim();
  if (!h) return null;
  const before = h.split(/Franchise Disclosure Document/i)[0].replace(/[–—-]\s*$/, "").trim();
  const rules = [
    [/^Radisson Individuals/i, "Radisson Individuals"],
    [/^Radisson Blu/i, "Radisson Blu"],
    [/^Country Inn/i, "Country Inn & Suites by Radisson"],
    [/^Park Inn/i, "Park Inn by Radisson"],
    [/^Radisson/i, "Radisson"],
    [/^Everhome/i, "Everhome Suites"],
    [/^Ascend/i, "Ascend Hotel Collection"],
    [/^Cambria/i, "Cambria Hotels"],
    [/^Clarion/i, "Clarion"],
    [/^WoodSpring/i, "WoodSpring Suites"],
    [/^Comfort/i, "Comfort Inn & Suites"],
    [/^Econo\s*Lodge|^EconoLodge/i, "Econo Lodge"],
    [/^MainStay/i, "MainStay Suites"],
    [/^Quality/i, "Quality Inn"],
    [/^Rodeway/i, "Rodeway Inn"],
    [/^Sleep Inn/i, "Sleep Inn"],
    [/^Suburban/i, "Suburban Studios"],
  ];
  for (const [re, name] of rules) {
    if (re.test(before)) return name;
  }
  return before || null;
}

/** @param {string} stem e.g. 35771-202604-09 */
function metaFromText(stem) {
  const txtPath = path.join(TEXT_DIR, `${stem}.txt`);
  if (!fs.existsSync(txtPath)) return { brand: null, year: null };
  const raw = fs.readFileSync(txtPath, "utf8").slice(0, 4000);
  const header =
    raw.match(/^[^\n]*Franchise Disclosure Document[^\n]*/im)?.[0] ||
    raw.match(/^[^\n]{10,120}/m)?.[0] ||
    "";
  const year = raw.match(/April 1,\s*(\d{4})/i)?.[1] || stem.match(/-(\d{4})\d{2}-/)?.[1] || null;
  return { brand: brandFromHeader(header), year };
}

function sanitizeFileName(name) {
  return String(name)
    .replace(/[<>:"/\\|?*]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function parseArgs(argv) {
  const dryRun = argv.includes("--dry-run");
  let dir = DEFAULT_DIR;
  const i = argv.indexOf("--dir");
  if (i >= 0 && argv[i + 1]) dir = argv[i + 1];
  return { dryRun, dir };
}

function main() {
  const { dryRun, dir } = parseArgs(process.argv);
  if (!fs.existsSync(dir)) throw new Error(`FDD folder not found: ${dir}`);

  const pdfs = fs.readdirSync(dir).filter((f) => FDD_RE.test(f));
  if (!pdfs.length) throw new Error(`No numbered FDD PDFs in ${dir}`);

  /** @type {Map<string, number>} */
  const targetCounts = new Map();
  const plan = [];

  for (const file of pdfs.sort()) {
    const m = file.match(FDD_RE);
    if (!m) continue;
    const stem = `${m[1]}-${m[2]}-${m[3]}`;
    const regSuffix = m[3];
    const { brand, year } = metaFromText(stem);
    if (!brand || !year) {
      console.warn(`Skip (no brand/year): ${file}`);
      continue;
    }
    let target = sanitizeFileName(`${brand} FDD ${year}.pdf`);
    const baseTarget = target;
    const count = targetCounts.get(baseTarget) || 0;
    if (count > 0) {
      target = sanitizeFileName(`${brand} FDD ${year} (${stem}).pdf`);
    }
    targetCounts.set(baseTarget, count + 1);

    const src = path.join(dir, file);
    const dest = path.join(dir, target);
    plan.push({ file, target, src, dest, brand, year, regSuffix });
  }

  console.log(dryRun ? "DRY RUN\n" : "APPLY\n");
  for (const p of plan) {
    if (fs.existsSync(p.dest) && path.resolve(p.src) !== path.resolve(p.dest)) {
      console.warn(`SKIP collision: ${p.file} → ${p.target} (target exists)`);
      continue;
    }
    console.log(`${p.file}\n  → ${p.target}`);
    if (!dryRun) {
      if (path.resolve(p.src) === path.resolve(p.dest)) continue;
      fs.renameSync(p.src, p.dest);
    }
  }
  console.log(`\n${dryRun ? "Would rename" : "Renamed"} ${plan.length} file(s).`);

  if (!dryRun && plan.length) {
    const manifest = {
      description: "Maps human-readable FDD PDF filenames to FTC registration stems (fixtures/choice-fdd-text/*.txt).",
      generated: new Date().toISOString().slice(0, 10),
      files: plan.map((p) => ({
        pdf: p.target,
        stem: `${p.file.replace(/\.pdf$/i, "")}`,
        brand: p.brand,
        year: p.year,
      })),
    };
    const manifestPath = path.join(dir, "fdd-filename-manifest.json");
    fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
    console.log(`Wrote ${manifestPath}`);
  }
}

main();
