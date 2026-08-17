/**
 * Normalize Hotel Property Census intake fields to match existing census conventions.
 *
 * Family / Source Family + Brand Family = parent family only (Marriott, Hilton, Choice, …).
 * Regional chains use their own family name (never collapse to "Other").
 * State / Region: omit placeholder "Unknown" (existing DR rows leave blank).
 * Hostels / hostals are out of census intake scope.
 */

import {
  getCensusOfficialEntry,
  resolveCensusOfficialBrand,
} from "../research-engine-v2/census-official-brand-registry.js";
import { resolveDominicanRepublicStateRegion } from "./dominican-republic-state-region.js";
import {
  extractCityFromAutographStyleName,
  extractCityFromOfficialUrl,
} from "./census-city-an-casino-cleanup.js";

/**
 * Global parent families already on Family / Source Family select.
 * Regional families below must also exist as select options (see ensure script).
 */
export const CENSUS_GLOBAL_SOURCE_FAMILIES = Object.freeze([
  "Marriott",
  "IHG",
  "Hilton",
  "Choice",
  "Wyndham",
  "Accor",
  "Preferred",
  "Hyatt",
]);

/**
 * Regional / independent hotel groups — Family / Source Family = group name.
 * Never map these to "Other". Occidental → Barceló; Starfish → Karisma Hotels;
 * Hyatt Inclusive Collection brands → Hyatt.
 *
 * Keys are Current Brand labels (or aliases); values are select-safe family names.
 */
export const CENSUS_REGIONAL_SOURCE_FAMILY_BY_BRAND = Object.freeze({
  RIU: "RIU",
  Barceló: "Barceló",
  Occidental: "Barceló",
  Meliá: "Meliá",
  "Bahía Príncipe": "Bahía Príncipe",
  Catalonia: "Catalonia",
  "Club Med": "Club Med",
  ClubMed: "Club Med",
  "Be Live": "Be Live",
  Hodelpa: "Hodelpa",
  "Amhsa Marina Hotels": "Amhsa Marina Hotels",
  "Breezes (SuperClubs)": "SuperClubs",
  Breezes: "SuperClubs",
  SuperClubs: "SuperClubs",
  SuperClub: "SuperClubs",
  "Super Clubs": "SuperClubs",
  "Excellence Resorts": "Excellence Resorts",
  Excellence: "Excellence Resorts",
  "Hard Rock Hotels": "Hard Rock Hotels",
  "Starfish Resorts": "Karisma Hotels",
  "Majestic Resorts": "Majestic Resorts",
  "Blau Hotels": "Blau Hotels",
  Lopesan: "Lopesan",
  "Karisma Hotels": "Karisma Hotels",
  "Sirenis Hotels & Resorts": "Sirenis Hotels & Resorts",
  Sirenis: "Sirenis Hotels & Resorts",
  "Dreams (Hyatt Inclusive Collection)": "Hyatt",
  "Secrets (Hyatt Inclusive Collection)": "Hyatt",
  "Breathless (Hyatt Inclusive Collection)": "Hyatt",
  "Hyatt Zilara": "Hyatt",
  Hyatt: "Hyatt",
});

/** Distinct Family / Source Family options that must exist on HPC (global + regional). */
export const CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS = Object.freeze([
  ...new Set([
    ...CENSUS_GLOBAL_SOURCE_FAMILIES,
    ...Object.values(CENSUS_REGIONAL_SOURCE_FAMILY_BY_BRAND),
  ]),
]);

/**
 * Brand Family (single-line text) — match dominant production census labels.
 * Family / Source Family stays the short select (IHG, Marriott, …).
 * Brand Family uses the fuller corporate form already common in HPC.
 */
export const CENSUS_BRAND_FAMILY_DISPLAY_BY_SOURCE_FAMILY = Object.freeze({
  Marriott: "Marriott International",
  IHG: "IHG Hotels & Resorts",
  Hilton: "Hilton",
  Choice: "Choice Hotels International, Inc.",
  Wyndham: "Wyndham",
  Accor: "Accor",
  Preferred: "Preferred Hotels & Resorts",
  Hyatt: "Hyatt",
  // Regional: Brand Family = same group name (no longer corporate form in census yet)
  RIU: "RIU",
  Barceló: "Barceló",
  Meliá: "Meliá",
  "Bahía Príncipe": "Bahía Príncipe",
  Catalonia: "Catalonia",
  "Club Med": "Club Med",
  "Be Live": "Be Live",
  Hodelpa: "Hodelpa",
  "Amhsa Marina Hotels": "Amhsa Marina Hotels",
  SuperClubs: "SuperClubs",
  "Excellence Resorts": "Excellence Resorts",
  "Hard Rock Hotels": "Hard Rock Hotels",
  "Karisma Hotels": "Karisma Hotels",
  "Majestic Resorts": "Majestic Resorts",
  "Blau Hotels": "Blau Hotels",
  Lopesan: "Lopesan",
  "Sirenis Hotels & Resorts": "Sirenis Hotels & Resorts",
});

/** Aliases that should normalize to a Brand Family display label. */
const BRAND_FAMILY_ALIASES_TO_DISPLAY = Object.freeze({
  marriott: "Marriott International",
  "marriott international": "Marriott International",
  ihg: "IHG Hotels & Resorts",
  "ihg hotels & resorts": "IHG Hotels & Resorts",
  "intercontinental hotels group": "IHG Hotels & Resorts",
  "intercontinental hotel group": "IHG Hotels & Resorts",
  hilton: "Hilton",
  "hilton worldwide": "Hilton",
  choice: "Choice Hotels International, Inc.",
  "choice hotels": "Choice Hotels International, Inc.",
  "choice hotels international": "Choice Hotels International, Inc.",
  "choice hotels international, inc.": "Choice Hotels International, Inc.",
  "choice hotels international, inc": "Choice Hotels International, Inc.",
  wyndham: "Wyndham",
  "wyndham hotels": "Wyndham",
  "wyndham hotels & resorts": "Wyndham",
  accor: "Accor",
  accorhotels: "Accor",
  preferred: "Preferred Hotels & Resorts",
  "preferred hotels": "Preferred Hotels & Resorts",
  "preferred hotels & resorts": "Preferred Hotels & Resorts",
  hyatt: "Hyatt",
  "hyatt hotels": "Hyatt",
  "hyatt hotels corporation": "Hyatt",
});

/**
 * Resolve Brand Family text from a Family / Source Family select value.
 * @param {string} sourceFamily
 */
export function brandFamilyDisplayForSourceFamily(sourceFamily) {
  const key = String(sourceFamily || "").trim();
  if (!key) return "";
  if (CENSUS_BRAND_FAMILY_DISPLAY_BY_SOURCE_FAMILY[key]) {
    return CENSUS_BRAND_FAMILY_DISPLAY_BY_SOURCE_FAMILY[key];
  }
  const alias = BRAND_FAMILY_ALIASES_TO_DISPLAY[key.toLowerCase()];
  return alias || key;
}

/**
 * Normalize an existing Brand Family cell to census-canonical display text.
 * @param {string} brandFamilyRaw
 * @param {string} [sourceFamily]
 */
export function canonicalizeBrandFamilyDisplay(brandFamilyRaw, sourceFamily = "") {
  const raw = String(brandFamilyRaw || "").trim();
  if (raw) {
    const alias = BRAND_FAMILY_ALIASES_TO_DISPLAY[raw.toLowerCase()];
    if (alias) return alias;
    if (CENSUS_BRAND_FAMILY_DISPLAY_BY_SOURCE_FAMILY[raw]) {
      return CENSUS_BRAND_FAMILY_DISPLAY_BY_SOURCE_FAMILY[raw];
    }
  }
  return brandFamilyDisplayForSourceFamily(sourceFamily) || raw;
}

/** @deprecated use CENSUS_GLOBAL_SOURCE_FAMILIES + regional map */
export const CENSUS_CANONICAL_SOURCE_FAMILIES = Object.freeze([
  ...CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS,
  "Other",
]);

const INVALID_FAMILY_PLACEHOLDERS = new Set([
  "independent_open_sources",
  "known_chain_osm",
  "independent",
  "unknown",
  "other",
  "",
]);

/**
 * True when property name indicates hostel / hostal (not hotel census).
 * @param {string} name
 */
export function isHostelOrHostalProperty(name) {
  const n = String(name || "").trim().toLowerCase();
  if (!n) return false;
  // Word-boundary-ish: hostal / hostel as token (Spanish + English)
  // Also catch common OSM typo "hotal"
  return /(?:^|[^a-z])(?:hostal|hostel|hostels|hotal)(?:[^a-z]|$)/i.test(n);
}

/**
 * Resolve regional chain family from Current Brand label.
 * @param {string} brand
 * @returns {string}
 */
export function resolveRegionalSourceFamily(brand) {
  const raw = String(brand || "").trim();
  if (!raw) return "";
  if (CENSUS_REGIONAL_SOURCE_FAMILY_BY_BRAND[raw]) {
    return CENSUS_REGIONAL_SOURCE_FAMILY_BY_BRAND[raw];
  }
  const lower = raw.toLowerCase();
  for (const [key, family] of Object.entries(CENSUS_REGIONAL_SOURCE_FAMILY_BY_BRAND)) {
    if (key.toLowerCase() === lower) return family;
  }
  // Soft match: brand contains key (e.g. long Hyatt Inclusive labels)
  for (const [key, family] of Object.entries(CENSUS_REGIONAL_SOURCE_FAMILY_BY_BRAND)) {
    if (key.length >= 4 && lower.includes(key.toLowerCase())) return family;
  }
  return "";
}

/**
 * Resolve parent family for Family / Source Family + Brand Family.
 * @param {string} brandRaw
 * @param {object} [opts]
 * @returns {{ sourceFamily: string, brandFamily: string }}
 */
export function resolveIntakeParentFamily(brandRaw, opts = {}) {
  const brand = String(brandRaw || "").trim();
  if (!brand || /^independent$/i.test(brand) || /^brand-unconfirmed$/i.test(brand)) {
    return { sourceFamily: "", brandFamily: "" };
  }

  // Regional / own-family chains first (never "Other")
  const regional = resolveRegionalSourceFamily(brand);
  if (regional) {
    return {
      sourceFamily: regional,
      brandFamily: brandFamilyDisplayForSourceFamily(regional),
    };
  }

  const resolved = resolveCensusOfficialBrand(brand, {
    propertyName: opts.propertyName || "",
    sourceUrl: opts.sourceUrl || opts.officialPropertyUrl || "",
    sourceFamily: opts.sourceFamily || "",
  });
  if (resolved.ok && resolved.parent) {
    const parent = String(resolved.parent).trim();
    if (CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS.includes(parent)) {
      return {
        sourceFamily: parent,
        brandFamily: brandFamilyDisplayForSourceFamily(parent),
      };
    }
    // Unknown registry parent — do not invent; leave blank for steward
    return { sourceFamily: "", brandFamily: "" };
  }

  const entry = getCensusOfficialEntry(brand);
  if (entry?.parent) {
    const parent = String(entry.parent).trim();
    if (CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS.includes(parent)) {
      return {
        sourceFamily: parent,
        brandFamily: brandFamilyDisplayForSourceFamily(parent),
      };
    }
    return { sourceFamily: "", brandFamily: "" };
  }

  // Already a required family label
  if (
    CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS.some(
      (f) => f.toLowerCase() === brand.toLowerCase()
    )
  ) {
    const canon = CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS.find(
      (f) => f.toLowerCase() === brand.toLowerCase()
    );
    return {
      sourceFamily: canon,
      brandFamily: brandFamilyDisplayForSourceFamily(canon),
    };
  }

  return { sourceFamily: "", brandFamily: "" };
}

/**
 * Normalize Family / Source Family + Brand Family + State / Region on a payload.
 * Mutates a shallow copy; does not invent new select options.
 * @param {Record<string, unknown>} fields
 * @param {object} [opts]
 */
export function normalizeIntakeCensusFamilyFields(fields, opts = {}) {
  const out = { ...fields };
  const brand = String(out["Current Brand"] || opts.brand || "").trim();
  const affiliation = String(out["Affiliation Status"] || "").trim();
  const isIndependent =
    opts.forceIndependent === true ||
    /^independent$/i.test(affiliation) ||
    /^independent$/i.test(brand);

  if (isIndependent) {
    // Do not invent independent_open_sources — leave blank like non-chain census rows
    delete out["Family / Source Family"];
    delete out["Brand Family"];
  } else {
    const resolved = resolveIntakeParentFamily(brand, {
      propertyName: out["Property Name"],
      officialPropertyUrl: out["Official Property URL"],
      sourceUrl: out["Source URL"],
      sourceFamily: out["Family / Source Family"],
    });
    const parent = resolved.sourceFamily;

    const existingFamily = String(out["Family / Source Family"] || "").trim();
    const existingBrandFamily = String(out["Brand Family"] || "").trim();

    if (parent) {
      out["Family / Source Family"] = parent;
      out["Brand Family"] = brandFamilyDisplayForSourceFamily(parent);
    } else {
      // Drop invented / non-canonical values that would typecast new select options
      if (
        INVALID_FAMILY_PLACEHOLDERS.has(existingFamily.toLowerCase()) ||
        (existingFamily &&
          !CENSUS_REQUIRED_SOURCE_FAMILY_OPTIONS.includes(existingFamily))
      ) {
        delete out["Family / Source Family"];
      }
      if (
        !existingBrandFamily ||
        INVALID_FAMILY_PLACEHOLDERS.has(existingBrandFamily.toLowerCase())
      ) {
        delete out["Brand Family"];
      } else {
        out["Brand Family"] = canonicalizeBrandFamilyDisplay(
          existingBrandFamily,
          existingFamily
        );
      }
    }
  }

  // City: never write placeholder Unknown (prefer blank / omit)
  let city = String(out.City || "").trim();
  if (!city || /^unknown$/i.test(city)) {
    const fromName = extractCityFromAutographStyleName(out["Property Name"]);
    const fromUrl = extractCityFromOfficialUrl(
      out["Official Property URL"] || out["Source URL"] || ""
    );
    city = fromName || fromUrl || "";
    if (city) out.City = city;
    else delete out.City;
  }

  // State / Region: never write placeholder Unknown (prefer blank / omit)
  const state = String(out["State / Region"] || "").trim();
  if (!state || /^unknown$/i.test(state)) {
    delete out["State / Region"];
  }

  // High-confidence DR province from City when State still blank
  const country = String(out.Country || "").trim();
  if (
    !out["State / Region"] &&
    /^dominican republic$/i.test(country)
  ) {
    const resolved = resolveDominicanRepublicStateRegion(out.City);
    if (resolved.ok && resolved.confidence === "High" && resolved.province) {
      out["State / Region"] = resolved.province;
      if (resolved.city_canonical || resolved.suggest_city_cleanup) {
        out.City =
          resolved.city_canonical ||
          resolved.suggest_city_cleanup ||
          out.City;
      }
    }
  }

  return out;
}

/**
 * Build remediation patch for an already-inserted intake row.
 * @param {Record<string, unknown>} fields — current / intended fields
 * @param {object} [opts]
 */
export function buildIntakeCensusFormatRemediationPatch(fields, opts = {}) {
  const normalized = normalizeIntakeCensusFamilyFields(fields, opts);
  /** @type {Record<string, unknown|null>} */
  const patch = {};

  const fam = normalized["Family / Source Family"];
  const bf = normalized["Brand Family"];
  const city = normalized.City;
  const state = normalized["State / Region"];
  const prevFam = String(fields["Family / Source Family"] || "").trim();
  const prevBf = String(fields["Brand Family"] || "").trim();
  const prevCity = String(fields.City || "").trim();
  const prevState = String(fields["State / Region"] || "").trim();

  if (fam) {
    if (fam !== prevFam) patch["Family / Source Family"] = fam;
  } else if (prevFam) {
    // Clear invented option
    patch["Family / Source Family"] = null;
  }

  if (bf) {
    if (bf !== prevBf) patch["Brand Family"] = bf;
  } else if (prevBf) {
    patch["Brand Family"] = null;
  }

  if (city) {
    if (city !== prevCity) patch.City = city;
  } else if (prevCity && /^unknown$/i.test(prevCity)) {
    patch.City = null;
  }

  if (state) {
    if (state !== prevState) patch["State / Region"] = state;
  } else if (prevState && /^unknown$/i.test(prevState)) {
    patch["State / Region"] = null;
  }

  return {
    patch,
    normalized,
    delete_record: isHostelOrHostalProperty(
      fields["Property Name"] || opts.propertyName || ""
    ),
    reasons: {
      family_fixed: Boolean(patch["Family / Source Family"] !== undefined),
      brand_family_fixed: Boolean(patch["Brand Family"] !== undefined),
      city_fixed: Boolean(patch.City !== undefined),
      state_fixed: Boolean(patch["State / Region"] !== undefined),
      hostel_delete: isHostelOrHostalProperty(
        fields["Property Name"] || opts.propertyName || ""
      ),
    },
  };
}
