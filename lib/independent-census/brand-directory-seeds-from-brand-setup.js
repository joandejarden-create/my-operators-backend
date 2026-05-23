/**
 * Phase 4G — Build brand-directory search-list seeds from Brand Setup (read-only).
 */

import {
  loadBrandSetupBasics,
  isBrandStatusActive,
  isCalaRelevant,
  normalizeBrandKey,
  exactKey,
} from "./brand-setup-cala-inventory.js";
import { normalizeParentCompanyKey } from "../hotel-census/brand-alias-resolve.js";

export function parentCompanyMatches(brand, opts) {
  const rawParent = exactKey(brand.parentCompany);
  const norm = brand.normalizedParentCompany || normalizeParentCompanyKey(rawParent);

  if (opts.parentCompany) {
    const want = exactKey(opts.parentCompany);
    if (rawParent === want) return true;
    if (normalizeBrandKey(rawParent) === normalizeBrandKey(want)) return true;
  }

  if (opts.normalizedParentCompany) {
    const wantNorm = normalizeParentCompanyKey(opts.normalizedParentCompany);
    if (norm === wantNorm) return true;
  }

  return !opts.parentCompany && !opts.normalizedParentCompany;
}

export function buildSeedRecord(brand) {
  const active = isBrandStatusActive(brand.brandStatus);
  const sourceUrl = brand.brandDirectoryUrl || brand.officialWebsite || "";
  const missingSourceUrl = !exactKey(sourceUrl);

  let sourceNotes = `Brand Setup seed (record ${brand.airtableRecordId}). Status: ${brand.brandStatus || "unknown"}.`;
  if (missingSourceUrl) {
    sourceNotes =
      "Missing official URL in Brand Setup; requires manual source URL before discovery";
  }

  return {
    brand: brand.brandName,
    parentCompany: brand.parentCompany,
    normalizedParentCompany: brand.normalizedParentCompany,
    brandStatus: brand.brandStatus,
    brandFamily: brand.brandFamily,
    chainScaleOrPositioning: brand.chainScale || brand.positioningTier || "",
    serviceModel: brand.serviceModel,
    regionOffered: brand.regionOffered,
    sourceUrl,
    sourceMethod: "brand_setup_directory_seed",
    sourceNotes,
    includeInBrandExplorer: brand.includeInBrandExplorer,
    requiresManualReview: true,
    priorityRank: active ? 1 : 2,
    missingSourceUrl,
    brandSetupRecordId: brand.airtableRecordId,
    sourceName: `${brand.brandName} Directory`,
    country: "",
    hotelUrls: [],
  };
}

/**
 * @param {object} opts
 * @param {string} [opts.parentCompany]
 * @param {string} [opts.normalizedParentCompany]
 * @param {boolean} [opts.activeOnly]
 * @param {boolean} [opts.calaRegionOnly]
 * @param {string} [opts.batchId]
 */
export async function generateBrandDirectorySeedsFromBrandSetup(opts = {}) {
  const brandData = await loadBrandSetupBasics({ activeOnly: opts.activeOnly === true });
  let brands = brandData.brands;

  brands = brands.filter((b) => parentCompanyMatches(b, opts));

  if (opts.calaRegionOnly !== false) {
    brands = brands.filter((b) => isCalaRelevant(b.regionOffered));
  }

  const seeds = brands.map(buildSeedRecord);
  const activeCount = seeds.filter((s) => s.priorityRank === 1).length;
  const missingUrl = seeds.filter((s) => s.missingSourceUrl).length;

  return {
    generatedAt: new Date().toISOString(),
    phase: "4G-brand-directory-seeds-from-brand-setup",
    batchId: opts.batchId || "brand-directory-seeds-from-brand-setup",
    parentCompanyFilter: opts.parentCompany || null,
    normalizedParentCompanyFilter: opts.normalizedParentCompany || null,
    activeOnly: opts.activeOnly === true,
    calaRegionOnly: opts.calaRegionOnly !== false,
    brandSetupRecordsRead: brandData.totalLoaded,
    brandsMatched: brands.length,
    activeLiveCount: activeCount,
    otherStatusCount: seeds.length - activeCount,
    missingSourceUrlCount: missingUrl,
    seeds,
  };
}
