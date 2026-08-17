/**
 * READ-ONLY FDD document inventory for PoC audit.
 * Writes only to reports/fdd-intelligence/. Does not move/delete FDD files.
 */
import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REF =
  process.env.BRAND_REFERENCE_MATERIAL_ROOT ||
  "G:/My Drive/Dealality™/Platform Design & Build/Brand Reference Material";
const OUT = path.join(ROOT, "reports/fdd-intelligence");
fs.mkdirSync(OUT, { recursive: true });

function sha256File(p) {
  try {
    const h = createHash("sha256");
    const fd = fs.openSync(p, "r");
    const buf = Buffer.alloc(1024 * 1024);
    let n;
    while ((n = fs.readSync(fd, buf, 0, buf.length, null)) > 0) {
      h.update(buf.subarray(0, n));
    }
    fs.closeSync(fd);
    return h.digest("hex");
  } catch (e) {
    return `ERROR:${e.message}`;
  }
}

function walk(dir, acc = []) {
  if (!fs.existsSync(dir)) return acc;
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, acc);
    else acc.push(p);
  }
  return acc;
}

const brandHints = [
  [/curio/i, "Curio Collection by Hilton"],
  [/kimpton/i, "Kimpton"],
  [/indigo/i, "Hotel Indigo"],
  [/tribute/i, "Tribute Portfolio"],
  [/radisson blu/i, "Radisson Blu"],
  [/radisson individuals/i, "Radisson Individuals"],
  [/park inn/i, "Park Inn by Radisson"],
  [/country inn/i, "Country Inn & Suites by Radisson"],
  [/radisson/i, "Radisson"],
  [/ascend/i, "Ascend Hotel Collection"],
  [/cambria/i, "Cambria Hotels"],
  [/clarion/i, "Clarion"],
  [/comfort/i, "Comfort Inn & Suites"],
  [/quality/i, "Quality Inn"],
  [/sleep/i, "Sleep Inn"],
  [/econo/i, "Econo Lodge"],
  [/rodeway/i, "Rodeway Inn"],
  [/mainstay/i, "MainStay Suites"],
  [/suburban/i, "Suburban Studios"],
  [/everhome/i, "Everhome Suites"],
  [/woodspring/i, "WoodSpring Suites"],
  [/crowne/i, "Crowne Plaza"],
  [/intercontinental/i, "InterContinental"],
  [/voco/i, "Voco"],
  [/vignette/i, "Vignette Collection"],
  [/even hotels/i, "Even Hotels"],
  [/ruby/i, "Ruby"],
  [/ac.?hotels|ac-hotels/i, "AC Hotels"],
  [/aloft/i, "Aloft"],
  [/canopy/i, "Canopy"],
  [/autograph/i, "Autograph Collection"],
  [/sheraton/i, "Sheraton"],
  [/westin/i, "Westin"],
  [/courtyard/i, "Courtyard"],
  [/fairfield/i, "Fairfield"],
  [/moxy/i, "Moxy"],
  [/element/i, "Element"],
  [/renaissance/i, "Renaissance"],
  [/delta/i, "Delta Hotels"],
  [/le.?meridien/i, "Le Meridien"],
  [/luxury collection/i, "The Luxury Collection"],
  [/springhill/i, "SpringHill Suites"],
  [/residence/i, "Residence Inn"],
  [/towneplace/i, "TownePlace Suites"],
  [/four points/i, "Four Points"],
  [/outdoor/i, "Outdoor Collection"],
  [/citizenm/i, "citizenM"],
  [/studiores/i, "StudioRes"],
  [/city express/i, "City Express"],
  [/series by marriott/i, "Series by Marriott"],
  [/apartments by marriott/i, "Apartments by Marriott Bonvoy"],
  [/marriott.?and.?jw|jw marriott/i, "Marriott / JW Marriott"],
  [/home2/i, "Home2 Suites"],
  [/spark/i, "Spark by Hilton"],
  [/motto/i, "Motto by Hilton"],
  [/hgi/i, "Hilton Garden Inn"],
];

const economicsBrands = new Set(["Curio Collection by Hilton", "Kimpton"]);
const item19Brands = new Set([
  "Ascend Hotel Collection",
  "Cambria Hotels",
  "Clarion",
  "Comfort Inn & Suites",
  "Country Inn & Suites by Radisson",
  "Econo Lodge",
  "MainStay Suites",
  "Park Inn by Radisson",
  "Quality Inn",
  "Radisson",
  "Radisson Blu",
  "Rodeway Inn",
  "Sleep Inn",
  "Suburban Studios",
  "Curio Collection by Hilton",
  "WoodSpring Suites",
  "Everhome Suites",
  "Radisson Individuals",
]);

function inferMeta(filePath) {
  const base = path.basename(filePath);
  const lower = filePath.toLowerCase().replace(/\\/g, "/");
  let parent = "";
  if (lower.includes("/choice hotels")) parent = "Choice Hotels";
  else if (lower.includes("/hilton")) parent = "Hilton";
  else if (lower.includes("/ihg hotels")) parent = "IHG";
  else if (lower.includes("/marriott")) parent = "Marriott";
  else if (lower.includes("uploads/fdd-intelligence")) parent = "Unknown (uploads)";

  const ym = base.match(/(20\d{2})/);
  const year = ym ? Number(ym[1]) : null;
  let jurisdiction = "";
  if (/mexico/i.test(base)) jurisdiction = "Mexico";
  else if (/canada/i.test(base)) jurisdiction = "Canada";
  else if (/MN state|minnesota/i.test(base) || /mn state/i.test(filePath))
    jurisdiction = "Minnesota (US)";
  else if (/US|United States|3-31-202/i.test(base)) jurisdiction = "United States";

  let brand = "";
  for (const [re, name] of brandHints) {
    if (re.test(base) || re.test(filePath)) {
      brand = name;
      break;
    }
  }

  const parserExists =
    parent === "Choice Hotels" ||
    brand === "Curio Collection by Hilton" ||
    brand === "Kimpton";

  return {
    parent_company: parent,
    brand,
    franchisor_legal_entity: "",
    fdd_year: year,
    effective_date: "",
    document_type: "FDD PDF",
    original_pdf_present: "yes",
    extracted_text_present: "unknown",
    source_url_known: /MN state filing/i.test(base) ? "yes (MN CARDS harvest)" : "partial/unknown",
    source_domain: /MN state filing/i.test(base) ? "cards.web.commerce.state.mn.us" : "",
    state_jurisdiction: jurisdiction,
    file_path: filePath,
    file_size: fs.statSync(filePath).size,
    sha256: "",
    parser_exists: parserExists ? "yes" : "no",
    economics_extracted: economicsBrands.has(brand)
      ? "yes"
      : parent === "Choice Hotels" && item19Brands.has(brand)
        ? "partial (Item6/17/19)"
        : "no",
    item19_extracted: item19Brands.has(brand) ? "yes" : "no",
    historical_versions_present: "see inventory",
    confidence: brand && parent ? "medium" : "low",
    notes: "",
  };
}

const pdfRoots = [
  path.join(REF, "IHG Hotels & Resorts/fdd"),
  path.join(REF, "Hilton/fdd"),
  path.join(REF, "Hilton/operator-materials"),
  path.join(REF, "Choice Hotels International/FDDs"),
  path.join(REF, "Marriott International/fdd"),
  path.join(ROOT, "uploads/fdd-intelligence"),
];

const pdfs = [];
for (const r of pdfRoots) {
  for (const f of walk(r)) {
    if (!/\.pdf$/i.test(f)) continue;
    const base = path.basename(f);
    if (
      /brochure|pitch|one pager|brand book|messaging|PIP Template|LATAM/i.test(base) &&
      !/FDD/i.test(base)
    ) {
      continue;
    }
    if (!/fdd|FDD|franchise/i.test(f) && !/uploads[\\/]fdd-intelligence/i.test(f)) continue;
    pdfs.push(f);
  }
}
for (const f of walk(path.join(REF, "Choice Hotels International"))) {
  if (/\.pdf$/i.test(f) && /FDD/i.test(path.basename(f))) pdfs.push(f);
}

const seen = new Set();
const uniquePdfs = [];
for (const p of pdfs) {
  const k = p.toLowerCase();
  if (seen.has(k)) continue;
  seen.add(k);
  uniquePdfs.push(p);
}

const rows = [];
for (const p of uniquePdfs) {
  const m = inferMeta(p);
  m.sha256 = m.file_size <= 15 * 1024 * 1024 ? sha256File(p) : "deferred_large_file";
  rows.push(m);
}

const textRows = [];
const choiceText = path.join(ROOT, "fixtures/choice-fdd-text");
if (fs.existsSync(choiceText)) {
  for (const f of fs.readdirSync(choiceText)) {
    if (!f.endsWith(".txt")) continue;
    const fp = path.join(choiceText, f);
    let year = null;
    if (f.includes("202604") || f.includes("2026")) year = 2026;
    else if (f.includes("202504") || f.includes("202506") || f.includes("2025")) year = 2025;
    else if (f.includes("202404") || f.includes("2024")) year = 2024;
    textRows.push({
      parent_company: "Choice Hotels",
      brand: "",
      franchisor_legal_entity: "",
      fdd_year: year,
      effective_date: "",
      document_type: "Extracted FDD text",
      original_pdf_present: "yes (G: Drive FDDs / numbered IDs)",
      extracted_text_present: "yes",
      source_url_known: "no",
      source_domain: "",
      state_jurisdiction: "United States (Choice filing ID in filename)",
      file_path: fp,
      file_size: fs.statSync(fp).size,
      sha256: sha256File(fp),
      parser_exists: "yes (Choice Item6/17/19)",
      economics_extracted: "partial",
      item19_extracted: "yes",
      historical_versions_present: "yes",
      confidence: "high",
      notes: "Mapped via docs/choice-fdd-inventory.md",
    });
  }
}

for (const rel of [
  "reports/curio-fdd-plain.txt",
  "reports/kimpton-fdd-plain.txt",
  "reports/kimpton-fdd-extract.txt",
]) {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) continue;
  const isCurio = rel.includes("curio");
  textRows.push({
    parent_company: isCurio ? "Hilton" : "IHG",
    brand: isCurio ? "Curio Collection by Hilton" : "Kimpton",
    franchisor_legal_entity: "",
    fdd_year: isCurio ? 2026 : 2024,
    effective_date: "",
    document_type: "Extracted FDD text",
    original_pdf_present: "yes (reference library)",
    extracted_text_present: "yes",
    source_url_known: isCurio ? "unknown" : "yes (MN CARDS)",
    source_domain: isCurio ? "" : "cards.web.commerce.state.mn.us",
    state_jurisdiction: isCurio ? "United States" : "Minnesota (US)",
    file_path: fp,
    file_size: fs.statSync(fp).size,
    sha256: sha256File(fp),
    parser_exists: "yes",
    economics_extracted: "yes",
    item19_extracted: isCurio ? "yes" : "no",
    historical_versions_present: isCurio ? "yes" : "unknown",
    confidence: "high",
    notes: "",
  });
}

const all = [...rows, ...textRows];
const headers = [
  "Parent Company",
  "Brand",
  "Franchisor Legal Entity",
  "FDD Year",
  "Effective Date",
  "Document Type",
  "Original PDF Present?",
  "Extracted Text Present?",
  "Source URL Known?",
  "Source Domain",
  "State/Jurisdiction",
  "File Path",
  "File Size",
  "SHA256",
  "Parser Exists?",
  "Economics Extracted?",
  "Item 19 Extracted?",
  "Historical Versions Present?",
  "Confidence",
  "Notes",
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

const csvLines = [headers.join(",")];
for (const r of all) {
  csvLines.push(
    [
      r.parent_company,
      r.brand,
      r.franchisor_legal_entity,
      r.fdd_year,
      r.effective_date,
      r.document_type,
      r.original_pdf_present,
      r.extracted_text_present,
      r.source_url_known,
      r.source_domain,
      r.state_jurisdiction,
      r.file_path,
      r.file_size,
      r.sha256,
      r.parser_exists,
      r.economics_extracted,
      r.item19_extracted,
      r.historical_versions_present,
      r.confidence,
      r.notes,
    ]
      .map(csvEscape)
      .join(",")
  );
}
fs.writeFileSync(path.join(OUT, "fdd-document-inventory.csv"), csvLines.join("\n"), "utf8");

const brands = new Set(rows.map((r) => r.brand).filter(Boolean));
const parents = new Set(rows.map((r) => r.parent_company).filter(Boolean));
const years = [...new Set(rows.map((r) => r.fdd_year).filter(Boolean))].sort();
const byBrand = {};
for (const r of rows) {
  if (!r.brand) continue;
  byBrand[r.brand] = byBrand[r.brand] || [];
  byBrand[r.brand].push(r.fdd_year);
}
const multiYear = Object.entries(byBrand)
  .filter(([, ys]) => new Set(ys.filter(Boolean)).size > 1)
  .map(([b]) => b);
const econ = [...new Set(rows.filter((r) => r.economics_extracted === "yes").map((r) => r.brand))];
const i19 = [...new Set(rows.filter((r) => r.item19_extracted === "yes").map((r) => r.brand))];
const missingProv = rows.filter((r) => !String(r.source_url_known).startsWith("yes")).length;

const md = [
  "# FDD Document Inventory",
  "",
  `Generated: ${new Date().toISOString()}`,
  "",
  "## Summary",
  "",
  "| Metric | Count |",
  "|---|---|",
  `| Total FDD PDFs (deduped paths) | ${rows.length} |`,
  `| Total extracted FDD text documents | ${textRows.length} |`,
  `| Unique brands (from PDF filenames) | ${brands.size} |`,
  `| Unique parent companies | ${parents.size} |`,
  `| FDD years represented | ${years.join(", ")} |`,
  `| Brands with multiple historical years | ${multiYear.length} |`,
  `| Brands with economics extraction | ${econ.join(", ") || "—"} |`,
  `| Brands with Item 19 extraction (PDF-side) | ${i19.length} |`,
  `| PDF rows with source provenance missing/partial | ${missingProv} |`,
  "",
  "### Multi-year brands",
  multiYear
    .map((b) => `- ${b}: ${[...new Set(byBrand[b].filter(Boolean))].sort().join(", ")}`)
    .join("\n") || "_none detected_",
  "",
  "### Notes",
  "- Choice numbered PDFs live primarily under Google Drive `Choice Hotels International/FDDs`; repo holds extracted text in `fixtures/choice-fdd-text`.",
  "- `uploads/fdd-intelligence` contains duplicate AC Hotels uploads plus Mexico Hilton brand FDDs.",
  "- Kimpton/Indigo/other IHG MN filings live under reference library `IHG Hotels & Resorts/fdd`.",
  "- SHA256 deferred for files >15MB.",
  "",
  "Full machine-readable inventory: `fdd-document-inventory.csv`",
];
fs.writeFileSync(path.join(OUT, "fdd-document-inventory.md"), md.join("\n"), "utf8");
fs.writeFileSync(
  path.join(OUT, "fdd-document-inventory-summary.json"),
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      totalFddPdfs: rows.length,
      totalExtractedText: textRows.length,
      uniqueBrands: [...brands].sort(),
      uniqueParents: [...parents].sort(),
      years,
      multiYearBrands: multiYear,
      economicsBrands: econ,
      item19BrandCount: i19.length,
      missingProvenancePdfRows: missingProv,
    },
    null,
    2
  ),
  "utf8"
);

console.log(
  JSON.stringify(
    { pdfs: rows.length, texts: textRows.length, brands: brands.size, parents: [...parents] },
    null,
    2
  )
);
