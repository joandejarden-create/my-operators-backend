/**
 * Canonical Brand Dictionary for Hotel Property Census Autopilot.
 * Built read-only from Active/Live Brand Setup + approved alias maps.
 * Never writes Brand Setup / Brand Explorer.
 */

import { buildActiveBrandSetupControlList } from "./census-autopilot-active-brand-scope.js";
import { APPROVED_SOFT_BRAND_MAPPINGS } from "./census-autopilot-brand-census-matcher.js";
import { resolveExtractorFamily } from "./census-family-extractor-registry.js";

export const BRAND_DICTIONARY_VERSION = "census-brand-canonical-dictionary-v1";

/** Soft brands / collections — affiliation must be source-confirmed. */
export const SOFT_BRAND_COLLECTION_SLUGS = Object.freeze([
  "autograph-collection",
  "tribute-portfolio",
  "the-luxury-collection",
  "design-hotels",
  "radisson-individuals",
  "ascend",
  "preferred-hotels-and-resorts",
  "mgallery",
  "trademark-collection",
  "tapestry-collection-by-hilton",
  "curio-collection",
  "lxr-hotels-and-resorts",
]);

/** Official domain → source family / parent family. */
export const OFFICIAL_DOMAIN_FAMILY = Object.freeze({
  "marriott.com": "Marriott",
  "sheraton.marriott.com": "Marriott",
  "hilton.com": "Hilton",
  "ihg.com": "IHG",
  "holidayinn.com": "IHG",
  "crowneplaza.com": "IHG",
  "choicehotels.com": "Choice",
  "radissonhotels.com": "Choice",
  "radissonhotelsamericas.com": "Choice",
  "accor.com": "Accor",
  "all.accor.com": "Accor",
  "wyndhamhotels.com": "Wyndham",
  "preferredhotels.com": "Preferred",
  "designhotels.com": "Accor",
  "slh.com": "SLH",
});

/**
 * Explicit High alias → canonical brand name (not slug).
 * Only listed mappings may autofix without Brand Setup exact match.
 */
export const HIGH_BRAND_ALIAS_TO_CANONICAL = Object.freeze({
  "four points": "Four Points by Sheraton",
  fourpoints: "Four Points by Sheraton",
  "four points sheraton": "Four Points by Sheraton",
  "hilton garden": "Hilton Garden Inn",
  "hilton garden inn": "Hilton Garden Inn",
  hilton: "Hilton Hotels & Resorts",
  "hilton hotels": "Hilton Hotels & Resorts",
  "hilton hotels & resorts": "Hilton Hotels & Resorts",
  "hilton hotels and resorts": "Hilton Hotels & Resorts",
  "hampton inn": "Hampton by Hilton",
  "hampton by hilton": "Hampton by Hilton",
  hampton: "Hampton by Hilton",
  holidayinnexpress: "Holiday Inn Express",
  "holiday inn express": "Holiday Inn Express",
  "holiday inn express & suites": "Holiday Inn Express",
  "holiday inn express and suites": "Holiday Inn Express",
  crowneplaza: "Crowne Plaza",
  "crowne plaza": "Crowne Plaza",
  hotelindigo: "Hotel Indigo",
  "hotel indigo": "Hotel Indigo",
  "hotel indigo by ihg": "Hotel Indigo",
  "hotel indigo ihg": "Hotel Indigo",
  "hampton inn by hilton": "Hampton by Hilton",
  "hampton inn & suites": "Hampton by Hilton",
  "hampton inn and suites": "Hampton by Hilton",
  "four points by sheraton": "Four Points by Sheraton",
  "m gallery": "MGallery Collection",
  mgallery: "MGallery Collection",
  "mgallery collection": "MGallery Collection",
  "radisson individual": "Radisson Individuals by Choice",
  "radisson individuals": "Radisson Individuals by Choice",
  "radisson individuals by choice": "Radisson Individuals by Choice",
  "trademark collection": "Trademark Collection by Wyndham",
  "trademark collection by wyndham": "Trademark Collection by Wyndham",
  "w hotel": "W Hotels",
  "w hotels": "W Hotels",
  "jw marriott": "JW Marriott",
  "st regis": "St. Regis",
  "st. regis": "St. Regis",
  "luxury collection": "The Luxury Collection",
  "the luxury collection": "The Luxury Collection",
  "autograph collection": "Autograph Collection",
  "tribute portfolio": "Tribute Portfolio",
  "design hotels": "Design Hotels",
  "ascend hotel collection": "Ascend Hotel Collection",
  ascend: "Ascend Hotel Collection",
  "preferred hotels": "Preferred Hotels & Resorts",
  "preferred hotels & resorts": "Preferred Hotels & Resorts",
  "preferred hotels and resorts": "Preferred Hotels & Resorts",
  voco: "Voco Hotels",
  "voco hotels": "Voco Hotels",
  "so/ hotels and resorts": "SO/",
  "so hotels and resorts": "SO/",
  avid: "avid hotels",
  "avid hotels": "avid hotels",
  evenhotels: "Even Hotels",
  "even hotels": "Even Hotels",
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
 * Extract registrable host from URL.
 */
export function hostFromUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    return u.hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Resolve source family from official URL host.
 */
export function familyFromOfficialUrl(url) {
  const host = hostFromUrl(url);
  if (!host) return null;
  for (const [domain, family] of Object.entries(OFFICIAL_DOMAIN_FAMILY)) {
    if (host === domain || host.endsWith(`.${domain}`)) return family;
  }
  return null;
}

/**
 * Build canonical brand dictionary (read-only Brand Setup Active/Live).
 * @param {{ controlList?: object, region?: string, parentCompany?: string|null }} [opts]
 */
export function buildCanonicalBrandDictionary(opts = {}) {
  const controlList =
    opts.controlList ||
    buildActiveBrandSetupControlList({
      region: opts.region || "CALA",
      parentCompany: opts.parentCompany || null,
    });

  /** @type {Map<string, object>} */
  const byCanonicalNorm = new Map();
  /** @type {Map<string, string>} aliasNorm → canonical name */
  const aliasToCanonical = new Map();
  /** @type {Map<string, object>} */
  const bySlug = new Map();

  const brands = [];

  for (const b of controlList.brands || []) {
    const canonical = String(b.brand_name || "").trim();
    if (!canonical) continue;
    const slug = String(b.brand_slug || "").trim();
    const parent = b.parent_company || null;
    const family =
      b.brand_family ||
      b.extractor_family ||
      resolveExtractorFamily(parent || slug).family ||
      "generic";
    const soft = SOFT_BRAND_COLLECTION_SLUGS.includes(slug);

    const entry = {
      canonical_brand_name: canonical,
      brand_slug: slug,
      parent_company: parent,
      brand_family: family,
      soft_brand_collection: soft,
      accepted_aliases: [...(b.census_matching_aliases || [])],
      source_families_allowed: [family].filter(Boolean),
      brand_setup_record_id: b.brand_setup_record_id || null,
    };

    brands.push(entry);
    byCanonicalNorm.set(norm(canonical), entry);
    byCanonicalNorm.set(normCompact(canonical), entry);
    if (slug) {
      bySlug.set(slug, entry);
      aliasToCanonical.set(norm(slug.replace(/-/g, " ")), canonical);
      aliasToCanonical.set(normCompact(slug), canonical);
    }
    aliasToCanonical.set(norm(canonical), canonical);
    for (const a of entry.accepted_aliases) {
      aliasToCanonical.set(norm(a), canonical);
      aliasToCanonical.set(normCompact(a), canonical);
    }
  }

  // Approved soft mappings (slug → canonical via bySlug)
  for (const [alias, slug] of Object.entries(APPROVED_SOFT_BRAND_MAPPINGS)) {
    const entry = bySlug.get(slug);
    if (entry) aliasToCanonical.set(norm(alias), entry.canonical_brand_name);
  }

  // Explicit High aliases (only if target exists in Active dictionary OR always for known strings)
  for (const [alias, canonical] of Object.entries(HIGH_BRAND_ALIAS_TO_CANONICAL)) {
    const hit =
      byCanonicalNorm.get(norm(canonical)) ||
      byCanonicalNorm.get(normCompact(canonical));
    if (hit) {
      aliasToCanonical.set(norm(alias), hit.canonical_brand_name);
      aliasToCanonical.set(normCompact(alias), hit.canonical_brand_name);
    } else {
      // Keep alias for steward/hint even if not in current Active list
      aliasToCanonical.set(norm(alias), canonical);
    }
  }

  return {
    version: BRAND_DICTIONARY_VERSION,
    generated_at: new Date().toISOString(),
    brand_setup_read_only: true,
    active_brand_count: brands.length,
    brands,
    by_canonical_norm: byCanonicalNorm,
    by_slug: bySlug,
    alias_to_canonical: aliasToCanonical,
    soft_brand_slugs: [...SOFT_BRAND_COLLECTION_SLUGS],
    official_domain_family: { ...OFFICIAL_DOMAIN_FAMILY },
  };
}
/**
 * Lookup dictionary entry for a brand string.
 * @param {string} brandRaw
 * @param {object} dictionary
 * @param {{ propertyName?: string, sourceUrl?: string }} [opts]
 */
export function lookupCanonicalBrand(brandRaw, dictionary, opts = {}) {
  const n = norm(brandRaw);
  const c = normCompact(brandRaw);
  if (!n) return { ok: false, reason: "blank" };

  const byName = dictionary.by_canonical_norm;
  const aliasMap = dictionary.alias_to_canonical;
  const propertyName = String(opts.propertyName || "");
  const sourceUrl = String(opts.sourceUrl || "");

  // holidayinn / Holiday Inn → Express only when name/url supports Express
  if (n === "holidayinn" || n === "holiday inn" || c === "holidayinn") {
    if (/express/i.test(propertyName) || /express/i.test(sourceUrl)) {
      const entry = byName.get(norm("Holiday Inn Express"));
      if (entry) {
        return {
          ok: true,
          entry,
          match: "alias",
          canonical: entry.canonical_brand_name,
          in_active_dictionary: true,
        };
      }
    }
    return {
      ok: false,
      reason: "holiday_inn_full_service_not_in_active_dictionary",
      brand: brandRaw,
      suggested_steward: true,
    };
  }

  if (byName.has(n)) {
    return {
      ok: true,
      entry: byName.get(n),
      match: "exact_canonical",
      canonical: byName.get(n).canonical_brand_name,
    };
  }
  if (byName.has(c)) {
    return {
      ok: true,
      entry: byName.get(c),
      match: "exact_canonical_compact",
      canonical: byName.get(c).canonical_brand_name,
    };
  }

  const aliasCanonical = aliasMap.get(n) || aliasMap.get(c);
  if (aliasCanonical) {
    let entry =
      byName.get(norm(aliasCanonical)) || byName.get(normCompact(aliasCanonical)) || null;
    if (!entry) {
      for (const e of dictionary.brands || []) {
        const en = norm(e.canonical_brand_name);
        const hint = norm(aliasCanonical);
        if (en === hint || en.includes(hint) || hint.includes(en)) {
          entry = e;
          break;
        }
      }
    }
    return {
      ok: Boolean(entry),
      entry,
      match: "alias",
      canonical: entry?.canonical_brand_name || aliasCanonical,
      in_active_dictionary: Boolean(entry),
    };
  }

  // Misspelling: edit distance ≤ 2 vs canonical names (length ≥ 6)
  if (n.length >= 6) {
    let best = null;
    let bestDist = 99;
    for (const entry of dictionary.brands || []) {
      const cand = norm(entry.canonical_brand_name);
      if (Math.abs(cand.length - n.length) > 2) continue;
      const d = levenshtein(n, cand);
      if (d > 0 && d <= 2 && d < bestDist) {
        bestDist = d;
        best = entry;
      }
    }
    if (best) {
      return {
        ok: true,
        entry: best,
        match: "misspelling",
        canonical: best.canonical_brand_name,
        edit_distance: bestDist,
      };
    }
  }

  return { ok: false, reason: "unknown_not_in_dictionary", brand: brandRaw };
}

function levenshtein(a, b) {
  const m = a.length;
  const n = b.length;
  const dp = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost);
    }
  }
  return dp[m][n];
}
