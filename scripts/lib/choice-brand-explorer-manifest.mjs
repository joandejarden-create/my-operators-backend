/**
 * Choice Hotels International — Brand Explorer completion manifest.
 * Gold standard: Radisson Blu (Choice) — split fixtures + CALA case studies + economics.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { TIER1_BRANDS } from "./choice-tier1-explorer-profiles.mjs";
import { AIRTABLE_TO_PROFILE_NAME, resolveProfileForAirtableName } from "./choice-chi-brand-resolve.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..");
const BASICS_EXPORT = path.join(ROOT, "fixtures", "choice-basics-audit-export.json");

/** @typedef {'complete' | 'generated' | 'needs-enrichment'} BluParityStatus */
/** @typedef {'premium-split' | 'full-json'} ApplyStrategy */

/**
 * Brands with hand-maintained split fixtures (Radisson Blu pattern).
 * @type {Record<string, { slug: string, buildScript: string, applyScript: string, referenceDoc: string, parity: BluParityStatus }>}
 */
export const PREMIUM_SPLIT_BRANDS = {
  "Radisson Blu (Choice)": {
    slug: "radisson-blu",
    buildScript: "scripts/build-radisson-blu-tab-fixtures.mjs",
    applyScript: "scripts/apply-radisson-blu-choice-all-fixtures.mjs",
    referenceDoc: "docs/radisson-blu-choice-reference.md",
    parity: "complete",
  },
};

/** Tier 1 brands with L2 split-fixture enrichment applied (see restore-choice-tier1-brand-explorer.mjs). */
export const TIER1_ENRICHED_BRANDS = {
  "Radisson (Choice)": {
    slug: "radisson-choice",
    restoreScript: "scripts/restore-radisson-choice-presentation.mjs",
    parity: "complete",
  },
  "Ascend Hotel Collection": {
    slug: "ascend-hotel-collection",
    restoreScript: "scripts/restore-choice-tier1-brand-explorer.mjs",
    parity: "complete",
  },
  "Radisson RED  (Choice)": {
    slug: "radisson-red-choice",
    restoreScript: "scripts/restore-choice-tier1-brand-explorer.mjs",
    parity: "complete",
  },
};

/** Radisson-family + upscale brands to upgrade next (stub → Blu-style split fixtures). */
export const BLU_PARITY_CANDIDATES = [
  "Radisson (Choice)",
  "Radisson RED (Choice)",
  "Radisson RED  (Choice)",
  "Radisson Collection (Choice)",
  "Radisson Collection  (Choice)",
  "Radisson Individual (Choice)",
  "Park Plaza (Choice)",
  "Park Inn by Radisson (Choice)",
  "Country Inn & Suites by Radisson (Choice)",
];

/** P1 L2 enrichment queue — Airtable Brand Basics names (factory default batch). */
export const CHOICE_P1_ENRICHMENT_QUEUE = [
  "Country Inn & Suites by Radisson",
  "Park Inn by Choice",
  "Park Plaza by Choice",
  "Radisson Collection by Choice",
  "Radisson Individuals by Choice",
];

const BLU_PARITY_CANDIDATE_KEYS = new Set(BLU_PARITY_CANDIDATES.map((n) => normalizeBrandKey(n)));

function normalizeBrandKey(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function loadBasicsExport() {
  if (!fs.existsSync(BASICS_EXPORT)) return [];
  return JSON.parse(fs.readFileSync(BASICS_EXPORT, "utf8"));
}

function tier1Slug(profileName) {
  const hit = TIER1_BRANDS.find((b) => b.name === profileName);
  if (hit) return hit.slug;
  return resolveProfileForAirtableName(profileName).slug;
}

function fullFixturePath(slug) {
  return path.join(ROOT, "fixtures", `brand-explorer-presentation-${slug}-full.json`);
}

function hasFullFixture(slug) {
  return fs.existsSync(fullFixturePath(slug));
}

/**
 * @returns {Array<{
 *   airtableName: string,
 *   profileName: string,
 *   recordId: string,
 *   slug: string,
 *   isTier1: boolean,
 *   applyStrategy: ApplyStrategy,
 *   parity: BluParityStatus,
 *   fullFixture: string|null,
 *   premium: typeof PREMIUM_SPLIT_BRANDS[string]|null,
 *   referenceFolder: string,
 * }>}
 */
export function listChoiceBrandManifest() {
  const rows = loadBasicsExport();
  return rows.map((row) => {
    const airtableName = String(row["Brand Name"] || "").trim();
    const profileName = AIRTABLE_TO_PROFILE_NAME[airtableName] || airtableName;
    const slug = tier1Slug(profileName);
    const premium = PREMIUM_SPLIT_BRANDS[profileName] || null;
    const enriched = TIER1_ENRICHED_BRANDS[profileName] || null;
    const isTier1 = TIER1_BRANDS.some((b) => b.name === profileName);
    const applyStrategy = premium ? "premium-split" : "full-json";
    let parity = "generated";
    if (premium?.parity === "complete" || enriched?.parity === "complete") parity = "complete";
    else if (BLU_PARITY_CANDIDATE_KEYS.has(normalizeBrandKey(profileName))) parity = "needs-enrichment";

    return {
      airtableName,
      profileName,
      recordId: row.id,
      slug,
      isTier1,
      applyStrategy,
      parity,
      fullFixture: hasFullFixture(slug)
        ? `fixtures/brand-explorer-presentation-${slug}-full.json`
        : null,
      premium,
      enriched,
      referenceFolder: `Choice Hotels International/brands/${airtableName}/`,
    };
  });
}

/**
 * @param {string} brandRef — Airtable name or profile name
 */
export function resolveChoiceBrandManifest(brandRef) {
  const q = String(brandRef || "").trim().toLowerCase();
  const all = listChoiceBrandManifest();
  return (
    all.find(
      (b) =>
        b.airtableName.toLowerCase() === q ||
        b.profileName.toLowerCase() === q ||
        b.slug === q.replace(/\s+/g, "-")
    ) || null
  );
}

export function printManifestSummary() {
  const all = listChoiceBrandManifest();
  const complete = all.filter((b) => b.parity === "complete").length;
  const needs = all.filter((b) => b.parity === "needs-enrichment").length;
  console.log(`Choice CHI brands: ${all.length}`);
  console.log(`  Blu parity complete: ${complete}`);
  console.log(`  Generated (slot-complete baseline): ${all.length - complete - needs}`);
  console.log(`  Needs Blu-style enrichment: ${needs}`);
  console.log("");
  for (const b of all) {
    const fixture = b.premium ? "premium-split" : b.fullFixture || "(no full fixture file)";
    console.log(
      `  ${b.profileName.padEnd(42)} ${b.parity.padEnd(18)} ${b.recordId}  ${fixture}`
    );
  }
}
