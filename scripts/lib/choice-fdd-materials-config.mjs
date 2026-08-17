/**
 * Choice CHI FDD → Brand Explorer materials.file (Franchise Disclosure Document).
 * PDFs: Dealality FDDs folder + fdd-filename-manifest.json
 * Airtable Brand Name values match Brand Setup - Brand Basics (May 2026 export).
 */

import fs from "fs";
import path from "path";

export const DEFAULT_FDD_DIR =
  process.env.CHOICE_FDD_DIR ||
  "G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material\\Choice Hotels International\\FDDs";

export const FDD_SLOT_KEY = "materials.file";
export const FDD_TITLE = "Franchise Disclosure Document";

/**
 * Airtable Brand Name → FTC stem for the latest FDD on file (see docs/choice-fdd-inventory.md).
 * @type {Record<string, string>}
 */
export const AIRTABLE_BRAND_TO_FDD_STEM = {
  "Ascend Hotel Collection": "35768-202604-08",
  "Cambria Hotels": "35798-202604-03",
  Clarion: "35770-202604-09",
  "Clarion Pointe": "35770-202604-09",
  "Comfort Inn & Suites": "35771-202604-09",
  "Country Inn & Suites by Radisson": "35772-202604-09",
  "Econo Lodge": "35773-202604-09",
  "Everhome Suites": "35774-202604-09",
  "MainStay Suites": "35775-202604-09",
  "Park Inn by Choice": "35776-202604-09",
  "Quality Inn": "35778-202604-09",
  "Radisson by Choice": "35779-202604-10",
  "Radisson Blu by Choice": "35781-202604-09",
  "Radisson Individuals by Choice": "35782-202604-05",
  "Radisson Inn & Suites": "35779-202604-10",
  "Radisson RED by Choice": "35779-202604-10",
  "Rodeway Inn": "35784-202604-09",
  "Sleep Inn": "35785-202604-09",
  "Suburban Studios": "35786-202604-09",
  "WoodSpring Suites": "33395-202504-08",
};

export function loadFddManifest(fddDir = DEFAULT_FDD_DIR) {
  const manifestPath = path.join(fddDir, "fdd-filename-manifest.json");
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Missing manifest: ${manifestPath}. Run scripts/rename-choice-fdd-pdfs.mjs first.`);
  }
  const data = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  if (!Array.isArray(data.files) || !data.files.length) {
    throw new Error(`Empty manifest: ${manifestPath}`);
  }
  return { manifestPath, files: data.files, fddDir };
}

/**
 * @returns {Map<string, { stem: string, pdf: string, brand: string, year: string, fullPath: string }>}
 */
export function indexManifestByStem(fddDir, files) {
  /** @type {Map<string, { stem: string, pdf: string, brand: string, year: string, fullPath: string }>} */
  const map = new Map();
  for (const row of files) {
    const fullPath = path.join(fddDir, row.pdf);
    if (fs.existsSync(fullPath)) map.set(row.stem, { ...row, fullPath });
  }
  return map;
}

/**
 * @param {string} airtableBrandName
 * @param {Map<string, object>} stemIndex
 */
export function resolveFddPdfForBrand(airtableBrandName, stemIndex) {
  const stem = AIRTABLE_BRAND_TO_FDD_STEM[airtableBrandName];
  if (!stem) return null;
  const entry = stemIndex.get(stem);
  if (!entry) return null;
  return entry;
}

export function listBrandsWithFddOnFile(stemIndex, brandFilter) {
  const brands = Object.keys(AIRTABLE_BRAND_TO_FDD_STEM).filter((name) => {
    const stem = AIRTABLE_BRAND_TO_FDD_STEM[name];
    return stemIndex.has(stem);
  });
  if (brandFilter) {
    const f = brandFilter.toLowerCase();
    return brands.filter((b) => b.toLowerCase().includes(f));
  }
  return brands.sort((a, b) => a.localeCompare(b));
}
