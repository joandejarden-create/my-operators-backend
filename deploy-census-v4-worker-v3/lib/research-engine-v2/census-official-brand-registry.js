/**
 * Official Census Brand Registry — source-confirmed brand names for Hotel Property Census.
 *
 * Separate from Brand Setup Active/Live (Brand Explorer universe).
 * Census may hold official parent brands that are not yet Active/Live.
 * Never writes Brand Setup / Brand Explorer.
 */

import { HIGH_BRAND_ALIAS_TO_CANONICAL } from "./census-brand-canonical-dictionary.js";
import { ACCOR_BRAND_CODE_TO_NAME } from "./census-autopilot-accor-cala-discovery-adapter.js";
import {
  WYNDHAM_BRAND_SLUG_TO_NAME,
  wyndhamBrandSlugFromUrl,
} from "./census-autopilot-wyndham-cala-discovery-adapter.js";
import { IHG_BRAND_SLUG_TO_NAME } from "./census-autopilot-coverage-steward-resolution.js";
import { mapMarriottMexicoBrand } from "./clean-census/marriott-mexico-discovery.js";

export const CENSUS_OFFICIAL_BRAND_REGISTRY_VERSION = "census-official-brand-registry-v1";

/**
 * Official census brand display names + parent family.
 * Includes Active + non-Active official inventory brands.
 */
export const CENSUS_OFFICIAL_BRANDS = Object.freeze({
  // Marriott
  Sheraton: { parent: "Marriott", soft: false },
  "Four Points by Sheraton": { parent: "Marriott", soft: false },
  "JW Marriott": { parent: "Marriott", soft: false },
  "W Hotels": { parent: "Marriott", soft: false },
  "St. Regis": { parent: "Marriott", soft: false },
  "The Luxury Collection": { parent: "Marriott", soft: true },
  "Autograph Collection": { parent: "Marriott", soft: true },
  "Tribute Portfolio": { parent: "Marriott", soft: true },
  "Design Hotels": { parent: "Accor", soft: true, listing_families: ["Marriott", "Accor"] },
  "Fairfield by Marriott": { parent: "Marriott", soft: false },
  "Delta Hotels": { parent: "Marriott", soft: false },
  "Renaissance Hotels": { parent: "Marriott", soft: false },
  "Le Méridien": { parent: "Marriott", soft: false },
  "City Express Plus by Marriott": { parent: "Marriott", soft: false },
  "City Express Junior by Marriott": { parent: "Marriott", soft: false },
  "City Express Suites by Marriott": { parent: "Marriott", soft: false },
  "City Centro by Marriott": { parent: "Marriott", soft: false },
  "City Express by Marriott": { parent: "Marriott", soft: false },
  "Courtyard by Marriott": { parent: "Marriott", soft: false },
  "AC Hotels by Marriott": { parent: "Marriott", soft: false },
  "Residence Inn by Marriott": { parent: "Marriott", soft: false },
  "Aloft Hotels": { parent: "Marriott", soft: false },
  "Moxy Hotels": { parent: "Marriott", soft: false },
  EDITION: { parent: "Marriott", soft: false },
  "Apartments by Marriott Bonvoy": { parent: "Marriott", soft: false },
  "Marriott Hotels": { parent: "Marriott", soft: false },
  Westin: { parent: "Marriott", soft: false },
  // IHG
  "Holiday Inn": { parent: "IHG", soft: false },
  "Holiday Inn Express": { parent: "IHG", soft: false },
  "Holiday Inn Resort": { parent: "IHG", soft: false },
  "Holiday Inn Club Vacations": { parent: "IHG", soft: false },
  "Crowne Plaza": { parent: "IHG", soft: false },
  InterContinental: { parent: "IHG", soft: false },
  "Staybridge Suites": { parent: "IHG", soft: false },
  "Candlewood Suites": { parent: "IHG", soft: false },
  "Hotel Indigo": { parent: "IHG", soft: false },
  "Voco Hotels": { parent: "IHG", soft: false },
  Iberostar: { parent: "IHG", soft: false },
  "Iberostar Waves": { parent: "IHG", soft: false },
  "Iberostar Selection": { parent: "IHG", soft: false },
  "avid hotels": { parent: "IHG", soft: false },
  "JOIA Iberostar": { parent: "IHG", soft: false },
  Garner: { parent: "IHG", soft: false },
  // Hilton
  "Hilton Hotels & Resorts": { parent: "Hilton", soft: false },
  "Hilton Garden Inn": { parent: "Hilton", soft: false },
  "Hampton by Hilton": { parent: "Hilton", soft: false },
  "Waldorf Astoria": { parent: "Hilton", soft: false },
  "Hilton Grand Vacations": { parent: "Hilton", soft: false },
  "Tapestry Collection by Hilton": { parent: "Hilton", soft: true },
  "Small Luxury Hotels of the World": {
    parent: "SLH",
    soft: true,
    listing_families: ["Hilton", "SLH"],
  },
  // Choice
  "Comfort Inn & Suites": { parent: "Choice", soft: false },
  "Sleep Inn": { parent: "Choice", soft: false },
  "Radisson by Choice": { parent: "Choice", soft: false },
  "Radisson Blu by Choice": { parent: "Choice", soft: false },
  "Radisson Individuals by Choice": { parent: "Choice", soft: true },
  "Ascend Hotel Collection": { parent: "Choice", soft: true },
  // Accor
  Sofitel: { parent: "Accor", soft: false },
  Novotel: { parent: "Accor", soft: false },
  Mercure: { parent: "Accor", soft: false },
  ibis: { parent: "Accor", soft: false },
  "MGallery Collection": { parent: "Accor", soft: true },
  "SO/": { parent: "Accor", soft: false },
  Pullman: { parent: "Accor", soft: false },
  "Banyan Tree": { parent: "Accor", soft: false },
  Angsana: { parent: "Accor", soft: false },
  Hyde: { parent: "Accor", soft: false },
  Mondrian: { parent: "Accor", soft: false },
  SLS: { parent: "Accor", soft: false },
  "Handwritten Collection": { parent: "Accor", soft: true },
  TRIBE: { parent: "Accor", soft: false },
  "Mama Shelter": { parent: "Accor", soft: false },
  // Wyndham
  "Wyndham Hotels & Resorts": { parent: "Wyndham", soft: false },
  "Wyndham Garden": { parent: "Wyndham", soft: false },
  "Wyndham Grand": { parent: "Wyndham", soft: false },
  "Wyndham Alltra": { parent: "Wyndham", soft: false },
  Ramada: { parent: "Wyndham", soft: false },
  "La Quinta by Wyndham": { parent: "Wyndham", soft: false },
  "Trademark Collection by Wyndham": { parent: "Wyndham", soft: true },
  "Registry Collection": { parent: "Wyndham", soft: true },
  "Esplendor by Wyndham": { parent: "Wyndham", soft: false },
  "Dazzler by Wyndham": { parent: "Wyndham", soft: false },
  "Microtel by Wyndham": { parent: "Wyndham", soft: false },
  "Tryp by Wyndham": { parent: "Wyndham", soft: false },
  "Days Inn": { parent: "Wyndham", soft: false },
  "DoubleTree by Hilton": { parent: "Hilton", soft: false },
  "Curio Collection by Hilton": { parent: "Hilton", soft: true },
  "Tru by Hilton": { parent: "Hilton", soft: false },
  "Homewood Suites by Hilton": { parent: "Hilton", soft: false },
  "Embassy Suites by Hilton": { parent: "Hilton", soft: false },
  "Motto by Hilton": { parent: "Hilton", soft: false },
  "Canopy by Hilton": { parent: "Hilton", soft: false },
  "Conrad Hotels & Resorts": { parent: "Hilton", soft: false },
  "Quality Inn": { parent: "Choice", soft: false },
  "Comfort Suites": { parent: "Choice", soft: false },
  "Fairmont Hotels & Resorts": { parent: "Accor", soft: false },
  "Kimpton Hotels": { parent: "IHG", soft: false },
  // Preferred
  "Preferred Hotels & Resorts": { parent: "Preferred", soft: true },
});

/** Opaque / dirty tokens that must never be guessed from name alone. */
export const OPAQUE_BRAND_CODE_PATTERNS = Object.freeze([
  /^es\s*xi$/i,
  /^pt\s*br$/i,
  /^sam$/i,
  /^mod$/i,
  /^hyd$/i,
  /^sou$/i,
  /^ban$/i,
  /^[a-z]{2,3}$/i, // 2–3 letter Accor-style codes without map
]);

/** Extra High aliases beyond Brand Setup Active map. */
export const CENSUS_HIGH_ALIAS_TO_CANONICAL = Object.freeze({
  ...HIGH_BRAND_ALIAS_TO_CANONICAL,
  holidayinn: "Holiday Inn",
  "holiday inn": "Holiday Inn",
  jw: "JW Marriott",
  autograph: "Autograph Collection",
  "iberostar-waves": "Iberostar Waves",
  "iberostar waves": "Iberostar Waves",
  "iberostar-selection": "Iberostar Selection",
  "iberostar selection": "Iberostar Selection",
  iberostar: "Iberostar",
  intercontinental: "InterContinental",
  staybridge: "Staybridge Suites",
  "staybridge suites": "Staybridge Suites",
  candlewood: "Candlewood Suites",
  "candlewood suites": "Candlewood Suites",
  "sleep inn": "Sleep Inn",
  sleepinn: "Sleep Inn",
  "wyndham garden": "Wyndham Garden",
  "wyndham hotels": "Wyndham Hotels & Resorts",
  "wyndham hotels & resorts": "Wyndham Hotels & Resorts",
  "wyndham alltra": "Wyndham Alltra",
  "registry collection": "Registry Collection",
  "es xl": "Esplendor by Wyndham", // requires /es-xl/ URL evidence in resolver
  esxl: "Esplendor by Wyndham",
  "es-xl": "Esplendor by Wyndham",
  esplendor: "Esplendor by Wyndham",
  "esplendor by wyndham": "Esplendor by Wyndham",
  microtel: "Microtel by Wyndham",
  "microtel by wyndham": "Microtel by Wyndham",
  "microtel inn": "Microtel by Wyndham",
  tryp: "Tryp by Wyndham",
  "tryp by wyndham": "Tryp by Wyndham",
  "doubletree": "DoubleTree by Hilton",
  "doubletree by hilton": "DoubleTree by Hilton",
  "double tree": "DoubleTree by Hilton",
  "curio collection": "Curio Collection by Hilton",
  "curio collection by hilton": "Curio Collection by Hilton",
  curio: "Curio Collection by Hilton",
  "tru by hilton": "Tru by Hilton",
  tru: "Tru by Hilton",
  "homewood suites": "Homewood Suites by Hilton",
  "homewood suites by hilton": "Homewood Suites by Hilton",
  "embassy suites": "Embassy Suites by Hilton",
  "embassy suites by hilton": "Embassy Suites by Hilton",
  "motto by hilton": "Motto by Hilton",
  motto: "Motto by Hilton",
  "canopy by hilton": "Canopy by Hilton",
  canopy: "Canopy by Hilton",
  conrad: "Conrad Hotels & Resorts",
  "conrad hotels": "Conrad Hotels & Resorts",
  "conrad hotels & resorts": "Conrad Hotels & Resorts",
  "quality inn": "Quality Inn",
  qualityinn: "Quality Inn",
  "comfort suites": "Comfort Suites",
  fairmont: "Fairmont Hotels & Resorts",
  "fairmont hotels & resorts": "Fairmont Hotels & Resorts",
  "kimpton hotels": "Kimpton Hotels",
  kimpton: "Kimpton Hotels",
  "fairfield by marriott": "Fairfield by Marriott",
  fairfield: "Fairfield by Marriott",
  "delta hotels": "Delta Hotels",
  "renaissance hotels": "Renaissance Hotels",
  renaissance: "Renaissance Hotels",
  "le meridien": "Le Méridien",
  "le méridien": "Le Méridien",
  "waldorf astoria": "Waldorf Astoria",
  "hilton grand vacations": "Hilton Grand Vacations",
  sofitel: "Sofitel",
  "holiday inn resort": "Holiday Inn Resort",
  "holiday inn club vacations": "Holiday Inn Club Vacations",
  "city express plus by marriott": "City Express Plus by Marriott",
  "city express junior by marriott": "City Express Junior by Marriott",
  "city express suites by marriott": "City Express Suites by Marriott",
  "city centro by marriott": "City Centro by Marriott",
  "city express by marriott": "City Express by Marriott",
  "city express": "City Express by Marriott",
  "courtyard by marriott": "Courtyard by Marriott",
  courtyard: "Courtyard by Marriott",
  "ac hotels by marriott": "AC Hotels by Marriott",
  "ac hotels": "AC Hotels by Marriott",
  "residence inn by marriott": "Residence Inn by Marriott",
  "residence inn": "Residence Inn by Marriott",
  "aloft hotels": "Aloft Hotels",
  aloft: "Aloft Hotels",
  "moxy hotels": "Moxy Hotels",
  moxy: "Moxy Hotels",
  edition: "EDITION",
  "apartments by marriott bonvoy": "Apartments by Marriott Bonvoy",
  "marriott executive apartments": "Apartments by Marriott Bonvoy",
  "avid hotels": "avid hotels",
  avid: "avid hotels",
  "joia iberostar": "JOIA Iberostar",
  "joia-iberostar": "JOIA Iberostar",
  joia: "JOIA Iberostar",
  garner: "Garner",
  "garner hotels": "Garner",
  "banyan tree": "Banyan Tree",
  angsana: "Angsana",
  hyde: "Hyde",
  mondrian: "Mondrian",
  sls: "SLS",
  "handwritten collection": "Handwritten Collection",
  handwritten: "Handwritten Collection",
  tribe: "TRIBE",
  "mama shelter": "Mama Shelter",
  "days inn": "Days Inn",
  daysinn: "Days Inn",
  "days-inn": "Days Inn",
});

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normCompact(s) {
  return norm(s).replace(/[^a-z0-9]/g, "");
}

/**
 * @param {string} brand
 */
export function isOpaqueBrandCode(brand) {
  const raw = String(brand || "").trim();
  if (!raw) return false;
  // Known mapped Accor codes are not opaque
  if (ACCOR_BRAND_CODE_TO_NAME[raw.toUpperCase()]) return false;
  // Es Xl with Wyndham slug is handled as alias — not opaque when URL proves it
  if (/^es\s*xl$/i.test(raw)) return false;
  for (const re of OPAQUE_BRAND_CODE_PATTERNS) {
    if (re.test(raw)) return true;
  }
  // Title-cased Accor leftovers like "Sam" when 3 letters
  if (/^[A-Za-z]{2,3}$/.test(raw) && !CENSUS_OFFICIAL_BRANDS[raw]) return true;
  return false;
}

/**
 * @param {string} brand
 */
export function isCensusOfficialBrand(brand) {
  const b = String(brand || "").trim();
  if (!b) return false;
  if (CENSUS_OFFICIAL_BRANDS[b]) return true;
  const n = norm(b);
  for (const name of Object.keys(CENSUS_OFFICIAL_BRANDS)) {
    if (norm(name) === n) return true;
  }
  return false;
}

/**
 * @param {string} brand
 */
export function getCensusOfficialEntry(brand) {
  const b = String(brand || "").trim();
  if (CENSUS_OFFICIAL_BRANDS[b]) return { canonical: b, ...CENSUS_OFFICIAL_BRANDS[b] };
  const n = norm(b);
  for (const [name, meta] of Object.entries(CENSUS_OFFICIAL_BRANDS)) {
    if (norm(name) === n) return { canonical: name, ...meta };
  }
  return null;
}

/**
 * Decode brand from official property URL (Marriott / IHG / Hilton / Wyndham / Accor / Choice).
 * @param {string} url
 * @param {string} [propertyName]
 * @param {string} [sourceFamily]
 */
export function decodeBrandFromOfficialUrl(url, propertyName = "", sourceFamily = "") {
  const u = String(url || "").trim();
  if (!u || !/^https?:\/\//i.test(u)) {
    return { ok: false, reason: "no_url" };
  }
  if (/booking\.com|expedia\.|tripadvisor\.|google\.|mapbox\./i.test(u)) {
    return { ok: false, reason: "forbidden_host" };
  }

  const lower = u.toLowerCase();

  // IHG path brand
  const ihg = lower.match(/ihg\.com\/([a-z0-9-]+)\//i);
  if (ihg) {
    const slug = ihg[1];
    const name =
      IHG_BRAND_SLUG_TO_NAME[slug] ||
      CENSUS_HIGH_ALIAS_TO_CANONICAL[slug] ||
      CENSUS_HIGH_ALIAS_TO_CANONICAL[norm(slug.replace(/-/g, " "))];
    // Prefer Express when path is holidayinnexpress even if alias says Holiday Inn
    if (slug === "voco") {
      return {
        ok: true,
        canonical: "Voco Hotels",
        parent: "IHG",
        method: "ihg_url_slug",
        confidence: "High",
      };
    }
    if (name) {
      const entry = getCensusOfficialEntry(name);
      return {
        ok: true,
        canonical: entry?.canonical || name,
        parent: entry?.parent || "IHG",
        method: "ihg_url_slug",
        confidence: "High",
      };
    }
  }

  // Wyndham path brand (skip locale segments like pt-br)
  if (/wyndhamhotels\.com/i.test(lower)) {
    const slug = wyndhamBrandSlugFromUrl(u);
    if (slug === "es-xl" || slug === "esplendor") {
      return {
        ok: true,
        canonical: "Esplendor by Wyndham",
        parent: "Wyndham",
        method: "wyndham_url_slug",
        confidence: "High",
      };
    }
    const mapped =
      (slug &&
        Object.prototype.hasOwnProperty.call(WYNDHAM_BRAND_SLUG_TO_NAME, slug) &&
        WYNDHAM_BRAND_SLUG_TO_NAME[slug]) ||
      (slug && CENSUS_HIGH_ALIAS_TO_CANONICAL[slug]) ||
      (slug && CENSUS_HIGH_ALIAS_TO_CANONICAL[norm(slug.replace(/-/g, " "))]);
    if (mapped) {
      const entry = getCensusOfficialEntry(mapped);
      if (entry || Object.prototype.hasOwnProperty.call(WYNDHAM_BRAND_SLUG_TO_NAME, slug)) {
        return {
          ok: true,
          canonical: entry?.canonical || mapped,
          parent: entry?.parent || "Wyndham",
          method: "wyndham_url_slug",
          confidence: "High",
        };
      }
    }
  }

  // Marriott — use existing URL/title mapper
  if (/marriott\.com/i.test(lower)) {
    const mapped = mapMarriottMexicoBrand(propertyName, u);
    if (mapped && mapped !== "Marriott Bonvoy — Brand Unconfirmed") {
      const entry = getCensusOfficialEntry(mapped);
      return {
        ok: true,
        canonical: entry?.canonical || mapped,
        parent: entry?.parent || "Marriott",
        method: "marriott_url_brand",
        confidence: "High",
      };
    }
    // Design Hotels member pages
    if (/design-hotels|member-of-design/i.test(lower) || /member of design hotels/i.test(propertyName)) {
      return {
        ok: true,
        canonical: "Design Hotels",
        parent: "Accor",
        method: "marriott_design_hotels_member",
        confidence: "High",
        listing_family: "Marriott",
      };
    }
  }

  // Hilton SLH
  if (/hilton\.com/i.test(lower) && (/\/[a-z0-9]+lx-/i.test(lower) || /slh/i.test(propertyName))) {
    return {
      ok: true,
      canonical: "Small Luxury Hotels of the World",
      parent: "SLH",
      method: "hilton_slh_listing",
      confidence: "High",
      listing_family: "Hilton",
    };
  }

  // Accor — brand code in catalog not always in URL; Sofitel path etc.
  if (/accor\.com/i.test(lower)) {
    if (/sofitel/i.test(lower) || /sofitel/i.test(propertyName)) {
      return { ok: true, canonical: "Sofitel", parent: "Accor", method: "accor_url", confidence: "High" };
    }
    if (/novotel/i.test(lower) || /novotel/i.test(propertyName)) {
      return { ok: true, canonical: "Novotel", parent: "Accor", method: "accor_url", confidence: "High" };
    }
    if (/mgallery|m-gallery/i.test(lower)) {
      return {
        ok: true,
        canonical: "MGallery Collection",
        parent: "Accor",
        method: "accor_url",
        confidence: "High",
      };
    }
  }

  // Choice sleep inn etc.
  if (/choicehotels\.com/i.test(lower)) {
    if (/sleep-inn/i.test(lower)) {
      return { ok: true, canonical: "Sleep Inn", parent: "Choice", method: "choice_url", confidence: "High" };
    }
    if (/comfort-inn/i.test(lower)) {
      return {
        ok: true,
        canonical: "Comfort Inn & Suites",
        parent: "Choice",
        method: "choice_url",
        confidence: "High",
      };
    }
  }

  void sourceFamily;
  return { ok: false, reason: "url_brand_not_decoded" };
}

/**
 * High-confidence alias / slug / casing normalize using census registry.
 * @param {string} brandRaw
 * @param {{ propertyName?: string, sourceUrl?: string, sourceFamily?: string }} [opts]
 */
export function resolveCensusOfficialBrand(brandRaw, opts = {}) {
  const brand = String(brandRaw || "").trim();
  const propertyName = String(opts.propertyName || "");
  const sourceUrl = String(opts.sourceUrl || "");
  const sourceFamily = String(opts.sourceFamily || "");

  if (!brand) {
    return { ok: false, reason: "brand_blank", steward_code: "brand_blank" };
  }

  // Accor catalog brand codes (even when isOpaqueBrandCode is false because mapped)
  {
    const code = brand.toUpperCase().replace(/\s+/g, "");
    if (ACCOR_BRAND_CODE_TO_NAME[code]) {
      const name = ACCOR_BRAND_CODE_TO_NAME[code];
      const entry = getCensusOfficialEntry(name);
      const canonical = entry?.canonical || name;
      const already =
        norm(canonical) === norm(brand) || normCompact(canonical) === normCompact(brand);
      return {
        ok: true,
        canonical,
        parent: entry?.parent || "Accor",
        method: "accor_brand_code_map",
        confidence: "High",
        was_opaque_code: !already,
        already_canonical: already,
      };
    }
  }

  // Alias / slug map BEFORE opaque-code gate (jw, holidayinn, crowneplaza, es-xl)
  const earlyAlias =
    CENSUS_HIGH_ALIAS_TO_CANONICAL[norm(brand)] ||
    CENSUS_HIGH_ALIAS_TO_CANONICAL[normCompact(brand)];
  if (earlyAlias && getCensusOfficialEntry(earlyAlias)) {
    // continue through full alias logic below — skip opaque for known aliases
  } else if (isOpaqueBrandCode(brand) && !earlyAlias) {
    const fromUrl = decodeBrandFromOfficialUrl(sourceUrl, propertyName, sourceFamily);
    if (fromUrl.ok) {
      return { ...fromUrl, was_opaque_code: true };
    }
    return {
      ok: false,
      reason: "brand_code_unresolved",
      steward_code: "brand_code_unresolved",
      brand,
    };
  }

  // Exact official
  const exact = getCensusOfficialEntry(brand);
  if (exact) {
    // Repair false Esplendor assignments (dirty Es Xl code was over-applied)
    if (
      /^esplendor by wyndham$/i.test(exact.canonical) &&
      !/wyndhamhotels\.com\/es-xl\//i.test(sourceUrl) &&
      !/esplendor/i.test(sourceUrl) &&
      !/esplendor/i.test(propertyName)
    ) {
      const fromUrl = decodeBrandFromOfficialUrl(sourceUrl, propertyName, sourceFamily);
      if (fromUrl.ok && fromUrl.canonical !== exact.canonical) {
        return { ...fromUrl, repaired_false_esplendor: true };
      }
    }
    // Casing normalize
    if (exact.canonical !== brand) {
      return {
        ok: true,
        canonical: exact.canonical,
        parent: exact.parent,
        method: "official_casing",
        confidence: "High",
        soft: exact.soft,
        listing_families: exact.listing_families,
      };
    }
    return {
      ok: true,
      canonical: exact.canonical,
      parent: exact.parent,
      method: "exact_official",
      confidence: "High",
      soft: exact.soft,
      listing_families: exact.listing_families,
      already_canonical: true,
    };
  }

  // Alias map
  const alias =
    CENSUS_HIGH_ALIAS_TO_CANONICAL[norm(brand)] ||
    CENSUS_HIGH_ALIAS_TO_CANONICAL[normCompact(brand)];
  if (alias) {
    // Es Xl / es-xl → Esplendor ONLY when official Wyndham path proves it
    if (/^es\s*xl$/i.test(brand) || normCompact(brand) === "esxl") {
      if (/wyndhamhotels\.com\/es-xl\//i.test(sourceUrl) || /esplendor/i.test(propertyName)) {
        return {
          ok: true,
          canonical: "Esplendor by Wyndham",
          parent: "Wyndham",
          method: "wyndham_es_xl_url_evidence",
          confidence: "High",
        };
      }
      const fromUrl = decodeBrandFromOfficialUrl(sourceUrl, propertyName, sourceFamily);
      if (fromUrl.ok && fromUrl.canonical !== "Esplendor by Wyndham") {
        return { ...fromUrl, replaced_dirty_brand: brand };
      }
      if (fromUrl.ok) return { ...fromUrl, replaced_dirty_brand: brand };
      return {
        ok: false,
        reason: "brand_code_unresolved",
        steward_code: "brand_code_unresolved",
        brand,
      };
    }

    // holidayinn → Holiday Inn only when URL/name is not Express
    if (norm(brand) === "holiday inn" || normCompact(brand) === "holidayinn") {
      if (/express/i.test(propertyName) || /holidayinnexpress/i.test(sourceUrl)) {
        return {
          ok: true,
          canonical: "Holiday Inn Express",
          parent: "IHG",
          method: "alias_express_evidence",
          confidence: "High",
        };
      }
      if (/holidayinn\//i.test(sourceUrl) || /ihg\.com\/holidayinn\//i.test(sourceUrl)) {
        return {
          ok: true,
          canonical: "Holiday Inn",
          parent: "IHG",
          method: "alias_full_service_url",
          confidence: "High",
        };
      }
      // slug holidayinn without express evidence — High to Holiday Inn when family IHG
      if (/ihg/i.test(sourceFamily)) {
        return {
          ok: true,
          canonical: "Holiday Inn",
          parent: "IHG",
          method: "alias_ihg_family",
          confidence: "High",
        };
      }
    }

    const entry = getCensusOfficialEntry(alias);
    if (entry) {
      return {
        ok: true,
        canonical: entry.canonical,
        parent: entry.parent,
        method: "census_high_alias",
        confidence: "High",
        soft: entry.soft,
      };
    }
    // Alias target known but not in registry yet — still High if founder-listed
    return {
      ok: true,
      canonical: alias,
      parent: null,
      method: "census_high_alias_unregistered",
      confidence: "High",
      brand_setup_promotion_candidate: true,
    };
  }

  // URL decode override when brand dirty / unconfirmed
  if (/brand unconfirmed/i.test(brand) || !exact) {
    const fromUrl = decodeBrandFromOfficialUrl(sourceUrl, propertyName, sourceFamily);
    if (fromUrl.ok) {
      return { ...fromUrl, replaced_dirty_brand: brand };
    }
  }

  // Promotion candidate: looks like a real brand name, official family, not opaque
  if (brand.length >= 4 && !/unconfirmed/i.test(brand) && sourceFamily) {
    return {
      ok: false,
      reason: "brand_setup_promotion_candidate",
      steward_code: "brand_setup_promotion_candidate",
      brand,
      parent_hint: sourceFamily,
    };
  }

  return {
    ok: false,
    reason: "brand_unknown_not_in_registry",
    steward_code: "brand_unknown_not_in_registry",
    brand,
  };
}
