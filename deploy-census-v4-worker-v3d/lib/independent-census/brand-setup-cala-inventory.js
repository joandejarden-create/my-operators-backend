/**
 * Read-only Brand Setup CALA parent-company inventory for independent census prioritization.
 * Source of truth: Brand Setup - Brand Basics (+ Brand Alias Mapping read-only compare).
 */

import Airtable from "airtable";
import { getPlatformBase } from "../hotel-census/platform-base.js";
import {
  ALIAS_FIELDS,
  BRAND_ALIAS_TABLE,
} from "../hotel-census/fields.js";
import { normalizeParentCompanyKey } from "../hotel-census/brand-alias-resolve.js";

export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";

export const BRAND_BASICS_FIELDS = {
  brandName: "Brand Name",
  parentCompany: "Parent Company",
  chainScale: "Hotel Chain Scale",
  brandModel: "Brand Model",
  serviceModel: "Hotel Service Model",
  positioning: "Brand Positioning",
  architecture: "Brand Architecture",
  regionOffered: "Region Offered",
  status: "Brand Status",
  website: "Brand Website",
  websiteAlt: "Website",
};

const CALA_REGION_TOKENS = [
  "cala",
  "caribbean",
  "latin america",
  "latin-america",
  "latam",
  "central america",
  "south america",
  "mexico",
  "dominican",
  "puerto rico",
  "costa rica",
  "panama",
  "colombia",
  "brazil",
  "argentina",
  "chile",
  "peru",
  "ecuador",
  "guatemala",
  "honduras",
  "jamaica",
  "cuba",
  "bahamas",
  "aruba",
  "curacao",
];

/** Target priority order (report mapping only — actual labels from Brand Setup win). */
export const PARENT_PRIORITY_TARGETS = [
  { rank: 1, label: "Choice Hotels", patterns: ["choice hotels", "choice hotels international"] },
  { rank: 2, label: "IHG", patterns: ["ihg", "intercontinental hotels group"] },
  { rank: 3, label: "Marriott International", patterns: ["marriott"] },
  { rank: 4, label: "Hilton", patterns: ["hilton"] },
  { rank: 5, label: "Hyatt", patterns: ["hyatt"] },
  { rank: 6, label: "Accor", patterns: ["accor"] },
  { rank: 7, label: "Wyndham", patterns: ["wyndham"] },
  { rank: 8, label: "Radisson / legacy Radisson", patterns: ["radisson", "rhg", "rezidor"] },
  { rank: 9, label: "Meliá", patterns: ["melia", "meliá"] },
  { rank: 10, label: "Barceló", patterns: ["barcelo", "barceló"] },
  { rank: 11, label: "Palladium", patterns: ["palladium"] },
  { rank: 12, label: "Karisma", patterns: ["karisma"] },
  { rank: 13, label: "Playa", patterns: ["playa hotels", "playa resort"] },
  { rank: 14, label: "RCD", patterns: ["rcd", "rcd hotels"] },
  { rank: 15, label: "Other regional / independent operators", patterns: [] },
];

export function exactKey(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function normalizeBrandKey(v) {
  return exactKey(v)
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
}

function valueToStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v)) {
    return v
      .map((item) => {
        if (typeof item === "string") return item.trim();
        if (item && typeof item.name === "string") return item.name.trim();
        return "";
      })
      .filter(Boolean)
      .join("; ");
  }
  if (typeof v === "object" && v.name) return String(v.name).trim();
  return String(v).trim();
}

function valuesToList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) {
    return v
      .map((item) => {
        if (typeof item === "string") return item.trim();
        if (item && typeof item.name === "string") return item.name.trim();
        return "";
      })
      .filter(Boolean);
  }
  const one = valueToStr(v);
  return one ? [one] : [];
}

export function isBrandStatusActive(statusRaw) {
  const s = String(statusRaw ?? "").toLowerCase();
  return /active|live/i.test(s);
}

export function isCalaRelevant(regions) {
  const list = Array.isArray(regions) ? regions : [regions];
  for (const r of list) {
    const k = normalizeBrandKey(r);
    if (!k) continue;
    if (k === "cala") return true;
    if (CALA_REGION_TOKENS.some((t) => k.includes(t))) return true;
    if (k === "am" || k === "americas") return true;
  }
  return false;
}

export function resolveNormalizedParentCompany(rawParent) {
  const raw = exactKey(rawParent);
  const norm = normalizeParentCompanyKey(raw);
  let recommendedPriority = PARENT_PRIORITY_TARGETS[PARENT_PRIORITY_TARGETS.length - 1];

  for (const target of PARENT_PRIORITY_TARGETS) {
    if (!target.patterns.length) continue;
    if (target.patterns.some((p) => norm.includes(p) || normalizeBrandKey(raw).includes(p))) {
      recommendedPriority = target;
      break;
    }
  }

  return {
    raw,
    normalized: norm || "(missing)",
    recommendedPriorityRank: recommendedPriority.rank,
    recommendedPriorityLabel: recommendedPriority.label,
  };
}

function pickField(fields, keys) {
  for (const k of keys) {
    if (fields[k] !== undefined && fields[k] !== null && fields[k] !== "") {
      return fields[k];
    }
  }
  return null;
}

function discoverIncludeInExplorer(fields, allKeys) {
  const key = [...allKeys].find((k) => /include in brand explorer/i.test(k));
  if (!key) return { fieldPresent: false, value: "" };
  const v = fields[key];
  if (v === true) return { fieldPresent: true, value: "yes" };
  if (v === false) return { fieldPresent: true, value: "no" };
  return { fieldPresent: true, value: valueToStr(v) || "blank" };
}

function discoverDirectoryUrl(fields, allKeys) {
  const key = [...allKeys].find(
    (k) =>
      /brand directory/i.test(k) &&
      (/url|website|link/i.test(k) || k.toLowerCase() === "brand directory url")
  );
  if (key) return valueToStr(fields[key]);
  return "";
}

export function mapBrandBasicsRecord(record) {
  const f = record.fields || {};
  const allKeys = new Set(Object.keys(f));
  const regions = valuesToList(
    pickField(f, [BRAND_BASICS_FIELDS.regionOffered, "Region Offered"])
  );
  const explorerFlag = discoverIncludeInExplorer(f, allKeys);
  const website = valueToStr(
    pickField(f, [BRAND_BASICS_FIELDS.website, BRAND_BASICS_FIELDS.websiteAlt, "Website"])
  );
  const directoryUrl = discoverDirectoryUrl(f, allKeys) || website;
  const parentRaw = valueToStr(f[BRAND_BASICS_FIELDS.parentCompany]);
  const parentInfo = resolveNormalizedParentCompany(parentRaw);
  const status = valueToStr(f[BRAND_BASICS_FIELDS.status]);
  const includeExplorer =
    explorerFlag.fieldPresent
      ? explorerFlag.value
      : isBrandStatusActive(status)
        ? "yes (derived from Brand Status)"
        : "no (derived from Brand Status)";

  const missing = [];
  if (!parentRaw) missing.push("parent_company");
  if (!valueToStr(f[BRAND_BASICS_FIELDS.architecture])) missing.push("brand_family");
  if (!website && !directoryUrl) missing.push("website_or_directory_url");
  if (!regions.length) missing.push("region_offered");

  return {
    airtableRecordId: record.id,
    brandName: valueToStr(f[BRAND_BASICS_FIELDS.brandName]),
    normalizedBrandName: normalizeBrandKey(f[BRAND_BASICS_FIELDS.brandName]),
    parentCompany: parentRaw,
    normalizedParentCompany: parentInfo.normalized,
    recommendedParentPriorityRank: parentInfo.recommendedPriorityRank,
    recommendedParentPriorityLabel: parentInfo.recommendedPriorityLabel,
    brandFamily: valueToStr(f[BRAND_BASICS_FIELDS.architecture]),
    chainScale: valueToStr(f[BRAND_BASICS_FIELDS.chainScale]),
    positioningTier: valueToStr(f[BRAND_BASICS_FIELDS.positioning]),
    serviceModel: valueToStr(f[BRAND_BASICS_FIELDS.serviceModel]),
    brandModel: valueToStr(f[BRAND_BASICS_FIELDS.brandModel]),
    regionOffered: regions,
    calaRelevant: isCalaRelevant(regions),
    includeInBrandExplorer: includeExplorer,
    brandStatus: status,
    brandDirectoryUrl: directoryUrl,
    officialWebsite: website,
    missingKeyFields: missing,
    fieldKeysPresent: [...allKeys].sort(),
  };
}

/**
 * @param {{ activeOnly?: boolean }} [opts]
 */
export async function loadBrandSetupBasics(opts = {}) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID for Brand Setup read");
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const selectFields = [
    BRAND_BASICS_FIELDS.brandName,
    BRAND_BASICS_FIELDS.parentCompany,
    BRAND_BASICS_FIELDS.chainScale,
    BRAND_BASICS_FIELDS.brandModel,
    BRAND_BASICS_FIELDS.serviceModel,
    BRAND_BASICS_FIELDS.positioning,
    BRAND_BASICS_FIELDS.architecture,
    BRAND_BASICS_FIELDS.regionOffered,
    BRAND_BASICS_FIELDS.status,
    BRAND_BASICS_FIELDS.website,
  ];

  const records = await base(BRAND_BASICS_TABLE)
    .select({ fields: selectFields, pageSize: 100 })
    .all();

  let brands = records.map(mapBrandBasicsRecord).filter((b) => b.brandName);

  if (opts.activeOnly === true) {
    brands = brands.filter((b) => isBrandStatusActive(b.brandStatus));
  }

  return {
    table: BRAND_BASICS_TABLE,
    airtableBaseId: baseId,
    totalLoaded: records.length,
    brandsInScope: brands.length,
    activeOnly: opts.activeOnly === true,
    fieldsRequested: selectFields,
    brands,
  };
}

export async function loadBrandAliasMappingReadOnly() {
  const base = getPlatformBase();
  if (!base) {
    return { totalLoaded: 0, rows: [], aliasesByCanonical: new Map(), canonicalInAlias: new Set() };
  }

  const records = await base(BRAND_ALIAS_TABLE)
    .select({ fields: Object.values(ALIAS_FIELDS), pageSize: 100 })
    .all();

  const aliasesByCanonical = new Map();
  const canonicalInAlias = new Set();
  const rows = [];

  for (const rec of records) {
    const f = rec.fields || {};
    const canonical = exactKey(f[ALIAS_FIELDS.canonicalBrandName]);
    const alias = exactKey(f[ALIAS_FIELDS.aliasSourceBrandName]);
    const active =
      f[ALIAS_FIELDS.active] === true ||
      ["yes", "true", "1", "active"].includes(
        String(f[ALIAS_FIELDS.active] ?? "").trim().toLowerCase()
      );

    if (canonical) canonicalInAlias.add(canonical);
    rows.push({
      recordId: rec.id,
      canonicalBrandName: canonical,
      aliasSourceBrandName: alias,
      parentCompany: exactKey(f[ALIAS_FIELDS.parentCompany]),
      active,
    });

    if (!canonical) continue;
    if (!aliasesByCanonical.has(canonical)) {
      aliasesByCanonical.set(canonical, { active: [], inactive: [] });
    }
    const bucket = active ? "active" : "inactive";
    if (alias) aliasesByCanonical.get(canonical)[bucket].push(alias);
  }

  return { totalLoaded: records.length, rows, aliasesByCanonical, canonicalInAlias };
}

function attachAliasInfo(brand, aliasData) {
  const bucket = aliasData.aliasesByCanonical.get(brand.brandName) || {
    active: [],
    inactive: [],
  };
  const activeCount = bucket.active.length;
  return {
    ...brand,
    aliasMappingPresent: activeCount > 0 || bucket.inactive.length > 0,
    aliasCount: activeCount + bucket.inactive.length,
    activeAliasCount: activeCount,
  };
}

export function buildBrandSetupCalaInventory(brandData, aliasData) {
  const brands = brandData.brands.map((b) => attachAliasInfo(b, aliasData));
  const brandNamesInSetup = new Set(brands.map((b) => b.brandName));

  const parentCounts = new Map();
  const parentRawLabels = new Map();

  for (const b of brands) {
    const pk = b.normalizedParentCompany;
    parentCounts.set(pk, (parentCounts.get(pk) || 0) + 1);
    if (!parentRawLabels.has(pk)) parentRawLabels.set(pk, new Set());
    if (b.parentCompany) parentRawLabels.get(pk).add(b.parentCompany);
  }

  const parentCompanies = [...parentCounts.entries()]
    .map(([normalized, brandCount]) => {
      const rawLabels = [...(parentRawLabels.get(normalized) || [])].sort();
      const sample = brands.find((b) => b.normalizedParentCompany === normalized);
      return {
        normalizedParentCompany: normalized,
        rawParentCompanyLabels: rawLabels,
        inconsistentSpelling: rawLabels.length > 1,
        brandCount,
        calaBrandCount: brands.filter(
          (b) => b.normalizedParentCompany === normalized && b.calaRelevant
        ).length,
        recommendedPriorityRank: sample?.recommendedParentPriorityRank ?? 15,
        recommendedPriorityLabel: sample?.recommendedParentPriorityLabel ?? "Other",
      };
    })
    .sort((a, b) => a.recommendedPriorityRank - b.recommendedPriorityRank || b.brandCount - a.brandCount);

  const nameCounts = new Map();
  for (const b of brands) {
    const nk = b.normalizedBrandName;
    if (!nk) continue;
    nameCounts.set(nk, (nameCounts.get(nk) || 0) + 1);
  }
  const duplicateBrandNames = [...nameCounts.entries()]
    .filter(([, c]) => c > 1)
    .map(([name, count]) => ({ normalizedBrandName: name, count }));

  const aliasOnlyNotInSetup = [...aliasData.canonicalInAlias].filter(
    (c) => !brandNamesInSetup.has(c)
  );
  const setupWithoutAlias = brands
    .filter((b) => !b.aliasMappingPresent)
    .map((b) => b.brandName);

  const calaBrands = brands.filter((b) => b.calaRelevant);
  const activeBrands = brands.filter((b) => isBrandStatusActive(b.brandStatus));
  const missingParent = brands.filter((b) => !b.parentCompany).length;
  const missingFamily = brands.filter((b) => !b.brandFamily).length;
  const missingUrl = brands.filter((b) => !b.officialWebsite && !b.brandDirectoryUrl).length;

  const brandsByPriority = {};
  for (const b of brands) {
    const label = b.recommendedParentPriorityLabel;
    if (!brandsByPriority[label]) brandsByPriority[label] = [];
    brandsByPriority[label].push(b.brandName);
  }

  const normalizationNotes = parentCompanies
    .filter((p) => p.inconsistentSpelling)
    .map((p) => ({
      normalizedParentCompany: p.normalizedParentCompany,
      rawLabels: p.rawParentCompanyLabels,
    }));

  return {
    generatedAt: new Date().toISOString(),
    phase: "brand-setup-cala-inventory",
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    independentCensusWrites: false,
    strFieldsUsed: false,
    sourceOfTruth: {
      brandSetupTable: BRAND_BASICS_TABLE,
      brandSetupBase: "AIRTABLE_BASE_ID",
      aliasTable: BRAND_ALIAS_TABLE,
      aliasBase: "AIRTABLE_BASE_ID_ALT",
    },
    scope: {
      activeOnlyFilter: brandData.activeOnly,
      allStatusesIncluded: !brandData.activeOnly,
    },
    totals: {
      brandSetupRecordsRead: brandData.totalLoaded,
      brandsInScope: brands.length,
      activeBrandCount: activeBrands.length,
      inactiveOrDraftBrandCount: brands.length - activeBrands.length,
      parentCompanyCount: parentCompanies.length,
      calaRelevantBrandCount: calaBrands.length,
      nonCalaFlaggedBrandCount: brands.length - calaBrands.length,
      aliasRowsRead: aliasData.totalLoaded,
      aliasOnlyNotInSetupCount: aliasOnlyNotInSetup.length,
      setupWithoutAliasCount: setupWithoutAlias.length,
      duplicateBrandNameGroups: duplicateBrandNames.length,
      inconsistentParentCompanyGroups: normalizationNotes.length,
    },
    dataQuality: {
      missingParentCompany: missingParent,
      missingBrandFamily: missingFamily,
      missingWebsiteOrDirectoryUrl: missingUrl,
      missingRegionOffered: brands.filter((b) => !b.regionOffered.length).length,
    },
    parentCompanies,
    brandsByRecommendedPriority: brandsByPriority,
    duplicateBrandNames,
    inconsistentParentCompanies: normalizationNotes,
    aliasOnlyNotInSetup: aliasOnlyNotInSetup.slice(0, 100),
    setupWithoutAlias: setupWithoutAlias.slice(0, 200),
    brands,
    recommendedPipelineActions: [
      "Use Brand Setup parent companies as independent census validation order (not Hotel Census Parent Company).",
      "Process brand-directory discovery per parent priority after official chain URLs are present in Brand Setup.",
      "Backfill Region Offered (CALA) and Brand Website before large-scale brand-directory imports.",
      "Align Brand Alias Mapping canonical names to Brand Setup Brand Name for census-backed matchers.",
    ],
  };
}

export function inventoryToCsvRow(b) {
  return {
    "Parent Company": b.parentCompany,
    "Normalized Parent Company": b.normalizedParentCompany,
    "Brand Name": b.brandName,
    "Normalized Brand Name": b.normalizedBrandName,
    "Brand Family / Collection": b.brandFamily,
    "Chain Scale / Positioning Tier": b.chainScale || b.positioningTier,
    "Service Model": b.serviceModel,
    "Region / CALA Flag": b.calaRelevant
      ? `CALA-relevant (${b.regionOffered.join("; ")})`
      : b.regionOffered.join("; ") || "",
    "Include in Brand Explorer": b.includeInBrandExplorer,
    "Brand Status": b.brandStatus,
    "Brand Directory URL": b.brandDirectoryUrl,
    "Official Website": b.officialWebsite,
    "Alias Mapping Present?": b.aliasMappingPresent ? "yes" : "no",
    "Alias Count": b.aliasCount,
    "Missing Key Fields": b.missingKeyFields.join("; "),
    "Recommended Parent Priority": `${b.recommendedParentPriorityRank}. ${b.recommendedParentPriorityLabel}`,
    Notes: "",
  };
}

export function gapsToCsvRow(b) {
  const issues = [...b.missingKeyFields];
  if (!b.aliasMappingPresent) issues.push("no_alias_mapping");
  if (!b.calaRelevant && !b.regionOffered.length) issues.push("cala_region_unknown");
  return {
    ...inventoryToCsvRow(b),
    "Gap Flags": issues.join("; "),
    Notes: issues.length ? "Needs Brand Setup or alias backfill before directory discovery." : "",
  };
}

export const INVENTORY_CSV_COLUMNS = [
  "Parent Company",
  "Normalized Parent Company",
  "Brand Name",
  "Normalized Brand Name",
  "Brand Family / Collection",
  "Chain Scale / Positioning Tier",
  "Service Model",
  "Region / CALA Flag",
  "Include in Brand Explorer",
  "Brand Status",
  "Brand Directory URL",
  "Official Website",
  "Alias Mapping Present?",
  "Alias Count",
  "Missing Key Fields",
  "Recommended Parent Priority",
  "Notes",
];

export const GAPS_CSV_COLUMNS = [...INVENTORY_CSV_COLUMNS.slice(0, -1), "Gap Flags", "Notes"];
