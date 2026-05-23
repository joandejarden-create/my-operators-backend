/**
 * Phase Choice-A — Read-only Choice target list from legacy Hotel Census (safe fields only).
 */

import {
  loadHotelCensusReadOnly,
  normalizeCountry,
  normalizeKey,
  normalizeText,
} from "./match-current-census.js";
import { computeCandidateDedupeKey } from "./normalize-candidate.js";
import { loadBrandSetupBasics } from "./brand-setup-cala-inventory.js";

export const DEFAULT_CHOICE_PARENT_PATTERNS = [
  "choice hotels international",
  "choice hotels",
  "choice",
];

/** Fallback brand keywords when Brand Setup read is unavailable. */
export const DEFAULT_CHOICE_BRAND_KEYWORDS = [
  "ascend hotel collection",
  "ascend",
  "cambria",
  "cambria hotels",
  "comfort inn",
  "comfort suites",
  "comfort",
  "quality inn",
  "clarion pointe",
  "clarion",
  "sleep inn",
  "mainstay suites",
  "suburban studios",
  "econo lodge",
  "rodeway inn",
  "woodspring",
  "woodspring suites",
  "everhome suites",
  "radisson collection",
  "radisson blu",
  "radisson red",
  "radisson individuals",
  "radisson individual",
  "radisson",
  "country inn",
  "country inn & suites",
  "park inn",
  "park plaza",
  "park inn by radisson",
];

function parseList(str) {
  return String(str || "")
    .split(",")
    .map((s) => normalizeKey(s.trim()))
    .filter(Boolean);
}

function textMatchesAny(text, patterns) {
  const k = normalizeKey(text);
  if (!k) return false;
  return patterns.some((p) => k.includes(p) || p.includes(k));
}

function buildBrandIndex(brandSetup) {
  const byKey = new Map();
  const choiceBrands = [];
  for (const b of brandSetup?.brands || []) {
    const parentK = normalizeKey(b.parentCompany);
    const nameK = normalizeKey(b.brandName);
    if (!nameK) continue;
    const isChoice =
      textMatchesAny(parentK, DEFAULT_CHOICE_PARENT_PATTERNS) ||
      parentK.includes("choice");
    if (isChoice) {
      choiceBrands.push(b.brandName);
      byKey.set(nameK, b.brandName);
    }
  }
  return { byKey, choiceBrands };
}

/**
 * Resolve display brand from affiliation / parent / Brand Setup index.
 */
export function resolveTargetBrand(row, brandIndex, brandKeywords) {
  const aff = normalizeKey(row.affiliation);
  const parent = normalizeKey(row.parentCompany);

  if (aff && brandIndex.byKey.has(aff)) {
    return brandIndex.byKey.get(aff);
  }

  for (const [bk, display] of brandIndex.byKey) {
    if (aff && (aff.includes(bk) || bk.includes(aff))) return display;
  }

  for (const kw of brandKeywords) {
    if (aff && aff.includes(kw)) {
      return row.affiliation || kw;
    }
  }

  if (textMatchesAny(parent, DEFAULT_CHOICE_PARENT_PATTERNS)) {
    return row.affiliation || row.parentCompany || "Choice Hotels";
  }

  return row.affiliation || "";
}

export function isChoiceTargetRow(row, opts) {
  const parentPatterns = opts.parentPatterns || DEFAULT_CHOICE_PARENT_PATTERNS;
  const brandKeywords = opts.brandKeywords || DEFAULT_CHOICE_BRAND_KEYWORDS;
  const brandFilter = opts.brandFilter ? normalizeKey(opts.brandFilter) : "";

  const parent = normalizeKey(row.parentCompany);
  const aff = normalizeKey(row.affiliation);

  const parentMatch = textMatchesAny(parent, parentPatterns);
  const brandMatch =
    textMatchesAny(aff, brandKeywords) ||
    brandKeywords.some((kw) => aff.includes(kw));

  if (!parentMatch && !brandMatch) return false;

  if (brandFilter) {
    const targetBrand = normalizeKey(
      resolveTargetBrand(row, opts.brandIndex, brandKeywords)
    );
    if (
      !aff.includes(brandFilter) &&
      !targetBrand.includes(brandFilter) &&
      !parent.includes(brandFilter)
    ) {
      return false;
    }
  }

  if (opts.countries?.size) {
    const co = normalizeCountry(row.country);
    if (co && !opts.countries.has(co)) return false;
  }

  return true;
}

export function buildTargetMatchKey(row) {
  return computeCandidateDedupeKey(
    row.name,
    row.city,
    row.country,
    row.lat,
    row.lng
  );
}

export function targetRowToReport(target) {
  return {
    legacyRecordId: target.recordId,
    legacyHotelName: target.name,
    legacyCity: target.city,
    legacyCountry: target.country,
    legacyLatitude: target.lat,
    legacyLongitude: target.lng,
    legacyWebsite: target.website,
    legacyTelephone: target.telephone,
    legacyAffiliation: target.affiliation,
    legacyParentCompany: target.parentCompany,
    legacyStatus: target.status,
    legacyRooms: target.rooms ?? "",
    targetBrand: target.targetBrand,
    targetCountry: target.targetCountry,
    targetMatchKey: target.targetMatchKey,
    notes: target.notes,
  };
}

export const CHOICE_TARGET_CSV_COLUMNS = [
  "legacyRecordId",
  "legacyHotelName",
  "legacyCity",
  "legacyCountry",
  "legacyLatitude",
  "legacyLongitude",
  "legacyWebsite",
  "legacyTelephone",
  "legacyAffiliation",
  "legacyParentCompany",
  "legacyStatus",
  "legacyRooms",
  "targetBrand",
  "targetCountry",
  "targetMatchKey",
  "notes",
];

/**
 * @param {object} opts
 */
export async function buildChoiceTargetList(opts) {
  const parentPatterns = opts.parentCompany
    ? parseList(opts.parentCompany).length
      ? parseList(opts.parentCompany)
      : [normalizeKey(opts.parentCompany)]
    : DEFAULT_CHOICE_PARENT_PATTERNS;

  const brandKeywords = opts.brandFilter
    ? [...DEFAULT_CHOICE_BRAND_KEYWORDS, normalizeKey(opts.brandFilter)]
    : DEFAULT_CHOICE_BRAND_KEYWORDS;

  const countries = opts.countriesStr
    ? new Set(
        opts.countriesStr
          .split(",")
          .map((c) => normalizeCountry(c.trim()))
          .filter(Boolean)
      )
    : null;

  let brandSetup = { brands: [], brandsInScope: 0 };
  try {
    brandSetup = await loadBrandSetupBasics({ activeOnly: true });
  } catch {
    brandSetup = { brands: [], brandsInScope: 0 };
  }

  const brandIndex = buildBrandIndex(brandSetup);
  const census = await loadHotelCensusReadOnly({});

  const targets = [];
  let scanned = 0;

  for (const row of census.rows) {
    scanned++;
    const candidate = {
      recordId: row.recordId,
      name: row.name,
      city: row.city,
      country: row.country,
      countryNorm: row.countryNorm,
      lat: row.lat ?? row.coords?.lat ?? null,
      lng: row.lng ?? row.coords?.lng ?? null,
      website: row.website || "",
      telephone: row.telephone || "",
      affiliation: row.affiliation,
      parentCompany: row.parentCompany,
      status: row.status,
      rooms: row.rooms,
    };

    if (
      !isChoiceTargetRow(candidate, {
        parentPatterns,
        brandKeywords,
        brandFilter: opts.brandFilter,
        brandIndex,
        countries,
      })
    ) {
      continue;
    }

    const targetBrand = resolveTargetBrand(candidate, brandIndex, brandKeywords);
    const notes = [
      "Read-only legacy Hotel Census benchmark target (not source of truth).",
      parentPatterns.some((p) => normalizeKey(candidate.parentCompany).includes(p))
        ? "Parent Company matches Choice filter."
        : "Affiliation/brand matches Choice filter.",
    ].join(" ");

    targets.push({
      ...candidate,
      targetBrand,
      targetCountry: normalizeCountry(candidate.country) || candidate.country,
      targetMatchKey: buildTargetMatchKey(candidate),
      notes,
    });
  }

  const byCountry = {};
  const byBrand = {};
  for (const t of targets) {
    const co = t.targetCountry || "(unknown)";
    byCountry[co] = (byCountry[co] || 0) + 1;
    const br = t.targetBrand || "(unknown)";
    byBrand[br] = (byBrand[br] || 0) + 1;
  }

  return {
    batchId: opts.batchId,
    parentCompanyFilter: opts.parentCompany || "Choice Hotels International",
    brandFilter: opts.brandFilter || null,
    countriesFilter: countries ? [...countries] : null,
    legacyCensusRecordsLoaded: census.totalLoaded,
    legacyCensusRecordsScanned: scanned,
    choiceTargetCount: targets.length,
    brandSetupBrandsLoaded: brandSetup.brandsInScope,
    choiceBrandsFromSetup: brandIndex.choiceBrands,
    byCountry,
    byBrand,
    targets,
    hotelCensusReads: true,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}
