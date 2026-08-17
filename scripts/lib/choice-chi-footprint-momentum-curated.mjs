/**
 * Curated footprint.momentum — Choice media-center announcements only (media.choicehotels.com).
 * Sources: brand fixtures + fixtures/choice-cala-footprint-momentum-press.json (CALA dev site PRs).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const PRESS_CATALOG = path.join(ROOT, "fixtures/choice-cala-footprint-momentum-press.json");

/** Airtable Brand Basics name → hand-maintained fixture (full momentum block) */
const FIXTURE_BY_AIRTABLE_BRAND = {
  "Ascend Hotel Collection": "fixtures/brand-explorer-presentation-ascend-hotel-collection-footprint-momentum.json",
  "Radisson by Choice": "fixtures/brand-explorer-presentation-radisson-footprint-momentum.json",
  "Radisson Blu by Choice": "fixtures/brand-explorer-presentation-radisson-blu-footprint-momentum.json",
  "Radisson RED  (Choice)": "fixtures/brand-explorer-presentation-radisson-red-choice-footprint-momentum.json",
  "Radisson RED by Choice": "fixtures/brand-explorer-presentation-radisson-red-choice-footprint-momentum.json",
  "Country Inn & Suites by Radisson": "fixtures/brand-explorer-presentation-country-inn-footprint-momentum.json",
};

/** Press / media-center announcement URLs (not consumer property pages). */
export function isChoicePressAnnouncementUrl(url) {
  return /^https:\/\/media\.choicehotels\.com\//i.test(String(url || "").trim());
}

/**
 * @param {{ title: string; date: string; description: string; url: string }} item
 */
function itemToMomentumRow(item, sort) {
  const url = String(item.url || "").trim();
  if (!isChoicePressAnnouncementUrl(url)) {
    throw new Error(`Momentum item "${item.title}" must use media.choicehotels.com URL, got: ${url}`);
  }
  return {
    slotKey: "footprint.momentum",
    title: String(item.title || "").trim(),
    body: `${String(item.date || "").trim()}\n\n${String(item.description || "").trim()}\n\n${url}`,
    sort,
  };
}

/**
 * @param {{ label?: string; items: { title: string; date: string; description: string; url: string }[] }} brandConfig
 * @returns {{ slotKey: string; title: string; body: string; sort: number }[]}
 */
function rowsFromPressCatalogBrand(brandConfig) {
  const items = Array.isArray(brandConfig?.items) ? brandConfig.items : [];
  if (!items.length) return [];

  const rows = [
    {
      slotKey: "footprint.momentum_label",
      title: "",
      body:
        String(brandConfig.label || "").trim() ||
        "Choice Hotels CALA openings · linked announcements",
      sort: 0,
    },
  ];

  items.forEach((item, i) => {
    rows.push(itemToMomentumRow(item, i + 1));
  });
  return rows;
}

function loadPressCatalogRows(airtableBrandName) {
  if (!fs.existsSync(PRESS_CATALOG)) return [];
  const data = JSON.parse(fs.readFileSync(PRESS_CATALOG, "utf8"));
  const brandConfig = data?.brands?.[airtableBrandName];
  if (!brandConfig) return [];
  return rowsFromPressCatalogBrand(brandConfig);
}

function loadFixtureRows(airtableBrandName) {
  const rel = FIXTURE_BY_AIRTABLE_BRAND[String(airtableBrandName || "").trim()];
  if (!rel) return [];

  const fixturePath = path.join(ROOT, rel);
  if (!fs.existsSync(fixturePath)) return [];

  const data = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const rows = Array.isArray(data.rows) ? data.rows : [];

  return rows
    .filter((r) => {
      const sk = String(r.slotKey || "").trim();
      return sk === "footprint.momentum_label" || sk === "footprint.momentum";
    })
    .map((r, i) => ({
      slotKey: String(r.slotKey).trim(),
      title: String(r.title || "").trim(),
      body: String(r.body || "").trim(),
      sort: r.sort ?? i,
    }))
    .sort((a, b) => a.sort - b.sort);
}

/**
 * @param {string} airtableBrandName
 * @returns {{ slotKey: string; title: string; body: string; sort: number }[]}
 */
export function buildCuratedMomentumPresentationRows(airtableBrandName) {
  const name = String(airtableBrandName || "").trim();
  const fromFixture = loadFixtureRows(name);
  if (fromFixture.length) return fromFixture;
  return loadPressCatalogRows(name);
}

export function hasCuratedMomentum(airtableBrandName) {
  const rows = buildCuratedMomentumPresentationRows(airtableBrandName);
  return rows.some((r) => r.slotKey === "footprint.momentum");
}

export function listBrandsWithCuratedMomentum() {
  const names = new Set(Object.keys(FIXTURE_BY_AIRTABLE_BRAND));
  if (fs.existsSync(PRESS_CATALOG)) {
    const data = JSON.parse(fs.readFileSync(PRESS_CATALOG, "utf8"));
    for (const brand of Object.keys(data?.brands || {})) {
      if (data.brands[brand]?.items?.length) names.add(brand);
    }
  }
  return [...names];
}
