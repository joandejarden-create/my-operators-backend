/**
 * Census Autopilot family directory adapters.
 *
 * Prefer fetchable official directory / HQV / VIC / deep-page signals BEFORE
 * falling back to bot-blocked property URLs (Hilton/Choice/Marriott 403).
 *
 * No Airtable writes. No Brand Explorer / Brand Setup writes. No Webhound runs.
 */

import {
  fetchHiltonLocationsPage,
  extractHotelsFromPageData,
  normalizeHiltonDirectoryHotel,
} from "../hilton-brand-directory-extract.js";
import { loadHiltonBrandDirectoryConfigs } from "../hilton-brand-registry.js";
import { formatAmenitiesText } from "../hilton-amenity-map.js";
import {
  buildChoiceRegionalPageForCountry,
  CHOICE_FETCH_HEADERS,
  parseChoiceRegionalHotelsFromHtml,
  canonicalChoicePropertyUrl,
} from "../choice-regional-directory-extract.js";
import { extractChoiceRegionalHotelCards } from "./clean-census/choice-mexico-discovery.js";
import { extractDeepOfficialPageSignals } from "./clean-census/field-research.js";
import {
  fetchMarriottHqvCoordinates,
  MARRIOTT_HQV_LEARNING,
} from "./marriott-hqv-coordinate-client.js";
import { loadVicClaimIndex } from "./production-census-first-pass-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";

export const FAMILY_ADAPTER_VERSION = "census-autopilot-family-directory-adapters-v2-wave-2";

/** @type {{ hilton: Map<string, object>|null, choice: Map<string, object>|null, marriott: Map<string, object>|null, accor: Map<string, object>|null, loadedAt: string|null, errors: object[], vic: { byId: Map<string, object>, loaded: object[] }|null }} */
const CACHE = {
  hilton: null,
  choice: null,
  marriott: null,
  accor: null,
  loadedAt: null,
  errors: [],
  unresolved: /** @type {Map<string, object>} */ (new Map()),
  /** Cached VIC claim index — never re-parse JSON per record (was hanging Autopilot address phase). */
  vic: null,
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Extract Hilton ctyhocn from identity key / URL / brand property code.
 * @param {object} fields
 * @param {string} [identityKey]
 */
export function extractHiltonCtyhocn(fields = {}, identityKey = "") {
  const fromField = String(
    fields["Brand Property Code"] || fields.ctyhocn || fields.Ctyhocn || ""
  )
    .trim()
    .toUpperCase();
  if (/^[A-Z0-9]{5,8}$/.test(fromField)) return fromField;

  const id = String(identityKey || fields["Property Identity Key"] || "");
  const idm = id.match(/ind_hilton_[a-z]{2}_([a-z0-9]+)/i);
  if (idm) return idm[1].toUpperCase();

  const url = String(
    fields["Official Property URL"] ||
      fields["Official URL"] ||
      fields["Source URL"] ||
      ""
  );
  const um = url.match(/hilton\.com\/en\/hotels\/([a-z0-9]+)-/i);
  if (um) return um[1].toUpperCase();
  return null;
}

/**
 * Extract Accor property id from identity / official URL.
 */
export function extractAccorPropertyId(fields = {}, identityKey = "") {
  const id = String(identityKey || fields["Property Identity Key"] || "");
  const idm = id.match(/ind_accor_[a-z]{2}_([a-z0-9]+)/i);
  if (idm) return idm[1].toUpperCase();
  const url = String(
    fields["Official Property URL"] ||
      fields["Official URL"] ||
      fields["Source URL"] ||
      ""
  );
  const um =
    url.match(/all\.accor\.com\/hotel\/([a-z0-9]+)/i) ||
    url.match(/\/hotel\/([A-Z0-9]{4,8})\//i);
  if (um) return um[1].toUpperCase();
  const code = String(fields["Brand Property Code"] || "").trim().toUpperCase();
  if (/^[A-Z0-9]{4,8}$/.test(code)) return code;
  return null;
}

/**
 * Extract Choice property id (e.g. MX165, CR013, CB008) from identity / URL.
 */
export function extractChoicePropertyId(fields = {}, identityKey = "") {
  const id = String(identityKey || fields["Property Identity Key"] || "");
  // ind_choice_mx_mx086 | ind_choice_cr_cr013 | ind_choice_co_cb008
  const idm = id.match(/ind_choice_[a-z]{2}_([a-z]{2}\d{2,4})/i);
  if (idm) return idm[1].toUpperCase();

  const url = String(
    fields["Official Property URL"] ||
      fields["Official URL"] ||
      fields["Source URL"] ||
      ""
  );
  const um =
    url.match(/\/([a-z]{2}\d{2,4})(?:\/|$|\?)/i) ||
    url.match(/choicehotels\.com\/[^/]+\/[^/]+\/[^/]+\/([a-z]{2}\d{2,4})/i);
  if (um) return um[1].toUpperCase();

  const code = String(fields["Brand Property Code"] || "").trim().toUpperCase();
  if (/^[A-Z]{2}\d{2,4}$/.test(code)) return code;
  return null;
}

/**
 * Extract Marriott MARSHA from URL / identity.
 */
export function extractMarshaCode(fields = {}, identityKey = "") {
  const url = String(
    fields["Official Property URL"] ||
      fields["Official URL"] ||
      fields["Source URL"] ||
      ""
  );
  const um = url.match(/\/hotels\/([A-Z0-9]{5})-/i) || url.match(/\/([A-Z0-9]{5})-hotel/i);
  if (um) return um[1].toUpperCase();
  const id = String(identityKey || fields["Property Identity Key"] || "");
  const idm =
    id.match(/ind_marriott_[a-z]{2}_([a-z0-9]{5})/i) ||
    id.match(/ind_marriott_mx_([a-z0-9]{5})/i);
  if (idm) return idm[1].toUpperCase();
  const code = String(fields["Brand Property Code"] || "").trim().toUpperCase();
  if (/^[A-Z0-9]{5}$/.test(code)) return code;
  return null;
}

export function familyFromIdentity(fields = {}, identityKey = "") {
  const brandFam = String(
    fields["Brand Family"] || fields["Family / Source Family"] || fields.Family || ""
  ).trim();
  if (["Marriott", "IHG", "Hilton", "Choice", "Accor", "Wyndham", "Preferred"].includes(brandFam)) {
    return brandFam;
  }
  if (/marriott/i.test(brandFam)) return "Marriott";
  if (/hilton/i.test(brandFam)) return "Hilton";
  if (/choice|radisson|ascend|comfort|quality|sleep|econo|cambria/i.test(brandFam)) {
    return "Choice";
  }
  if (/ihg|intercontinental|holiday inn|kimpton|voco|avid/i.test(brandFam)) return "IHG";
  if (/accor|ibis|novotel|mercure|sofitel|pullman|mgallery|fairmont/i.test(brandFam)) {
    return "Accor";
  }
  if (/wyndham|ramada|days inn|super 8|la quinta|tryp/i.test(brandFam)) return "Wyndham";
  if (/preferred/i.test(brandFam)) return "Preferred";

  const f = String(fields["Parent Company"] || "").trim();
  if (["Marriott", "IHG", "Hilton", "Choice", "Accor", "Wyndham", "Preferred"].includes(f)) {
    return f;
  }
  const id = String(identityKey || fields["Property Identity Key"] || "");
  if (id.includes("_marriott_")) return "Marriott";
  if (id.includes("_ihg_")) return "IHG";
  if (id.includes("_hilton_")) return "Hilton";
  if (id.includes("_choice_")) return "Choice";
  if (id.includes("_accor_")) return "Accor";
  if (id.includes("_wyndham_")) return "Wyndham";
  if (id.includes("_preferred_")) return "Preferred";
  return f || brandFam || "Other";
}

/**
 * Record a repeated unresolved source pattern for Webhound learning (never run here).
 */
export function noteUnresolvedSourcePattern(pattern) {
  const id = String(pattern?.id || pattern?.pattern || "").trim();
  if (!id) return;
  const prev = CACHE.unresolved.get(id) || {
    id,
    family: pattern.family || null,
    pattern: pattern.pattern || id,
    count: 0,
    sample_urls: [],
    what_code_needs_to_learn: pattern.what_code_needs_to_learn || null,
    never_write_from_webhound: true,
  };
  prev.count += 1;
  const url = pattern.sample_url || null;
  if (url && prev.sample_urls.length < 5 && !prev.sample_urls.includes(url)) {
    prev.sample_urls.push(url);
  }
  CACHE.unresolved.set(id, prev);
}

export function getUnresolvedSourcePatterns({ minCount = 2 } = {}) {
  return [...CACHE.unresolved.values()]
    .filter((p) => p.count >= minCount)
    .sort((a, b) => b.count - a.count);
}

export function resetFamilyDirectoryCaches() {
  CACHE.hilton = null;
  CACHE.choice = null;
  CACHE.loadedAt = null;
  CACHE.errors = [];
  CACHE.unresolved = new Map();
}

/**
 * Load Hilton Mexico brand location directories into memory (ctyhocn → hotel).
 */
export async function ensureHiltonMexicoDirectoryCache(opts = {}) {
  if (CACHE.hilton && !opts.force) return CACHE.hilton;
  const byCty = new Map();
  const delayMs = opts.delayMs ?? 150;
  try {
    const configs = await loadHiltonBrandDirectoryConfigs();
    for (const cfg of configs) {
      const url = `https://www.hilton.com/en/locations/mexico/${cfg.locationsSlug}/`;
      try {
        if (delayMs) await sleep(delayMs);
        const page = await fetchHiltonLocationsPage(url, {
          timeoutMs: opts.timeoutMs ?? opts.fetchTimeoutMs,
          signal: opts.signal,
        });
        const hotels = extractHotelsFromPageData(page.pageData);
        for (const hotel of hotels) {
          const normalized = normalizeHiltonDirectoryHotel(hotel, {
            sourceUrl: url,
            countryPage: "Mexico",
          });
          if (!normalized.ctyhocn) continue;
          const isMx =
            /Mexico/i.test(normalized.country || "") ||
            String(normalized.countryCode || "").toUpperCase() === "MX";
          if (!isMx) continue;
          const prev = byCty.get(normalized.ctyhocn);
          if (!prev || (normalized.amenityIds?.length || 0) > (prev.amenityIds?.length || 0)) {
            byCty.set(normalized.ctyhocn, {
              ...normalized,
              affiliation: cfg.canonicalBrandName,
              parent: "Hilton",
            });
          }
        }
      } catch (err) {
        CACHE.errors.push({
          family: "Hilton",
          url,
          error: err?.message || String(err),
        });
      }
    }
  } catch (err) {
    CACHE.errors.push({ family: "Hilton", error: err?.message || String(err) });
  }
  CACHE.hilton = byCty;
  CACHE.loadedAt = new Date().toISOString();
  return byCty;
}

/**
 * Load Choice Mexico regional JSON-LD + hotel cards (propertyId → card).
 */
export async function ensureChoiceMexicoRegionalCache(opts = {}) {
  if (CACHE.choice && !opts.force) return CACHE.choice;
  const byId = new Map();
  const page = buildChoiceRegionalPageForCountry("Mexico");
  try {
    const res = await fetch(page.url, {
      headers: CHOICE_FETCH_HEADERS,
      redirect: "follow",
      signal: AbortSignal.timeout(opts.timeoutMs || 60000),
    });
    const html = await res.text();
    if (!res.ok || /access denied|robot check/i.test(html)) {
      CACHE.errors.push({
        family: "Choice",
        url: page.url,
        error: !res.ok ? `http_${res.status}` : "blocked",
      });
      CACHE.choice = byId;
      return byId;
    }
    const ld = parseChoiceRegionalHotelsFromHtml(html);
    const cards = extractChoiceRegionalHotelCards(html);
    const cardById = new Map(cards.map((c) => [c.propertyId, c]));
    for (const h of ld) {
      const id = String(h.propertyId || "").toUpperCase();
      if (!id.startsWith("MX")) continue;
      const card = cardById.get(id) || {};
      byId.set(id, {
        propertyId: id,
        name: h.name,
        propertyUrl: canonicalChoicePropertyUrl(h.propertyUrl),
        city: card.city || null,
        state: card.state || null,
        country: "Mexico",
        addressLine1: card.addressLine1 || null,
        addressLine2: card.addressLine2 || null,
        postalCode: card.postalCode || null,
        latitude: card.latitude ?? null,
        longitude: card.longitude ?? null,
        amenityGroupLabels: card.amenityGroupLabels || [],
        source_url: page.url,
        source: "choice_regional_jsonld_card",
        // Regional cards expose amenity "description" labels, not hotel narrative copy.
        narrative_description: null,
        narrative_description_supported: false,
      });
    }
    // Cards without JSON-LD name still useful for address/amenities
    for (const card of cards) {
      const id = card.propertyId;
      if (byId.has(id)) continue;
      byId.set(id, {
        propertyId: id,
        name: null,
        propertyUrl: null,
        city: card.city || null,
        state: card.state || null,
        country: "Mexico",
        addressLine1: card.addressLine1 || null,
        addressLine2: card.addressLine2 || null,
        postalCode: card.postalCode || null,
        latitude: card.latitude ?? null,
        longitude: card.longitude ?? null,
        amenityGroupLabels: card.amenityGroupLabels || [],
        source_url: page.url,
        source: "choice_regional_hotel_card",
        narrative_description: null,
        narrative_description_supported: false,
      });
    }
  } catch (err) {
    CACHE.errors.push({
      family: "Choice",
      url: page.url,
      error: err?.message || String(err),
    });
  }
  CACHE.choice = byId;
  CACHE.loadedAt = new Date().toISOString();
  return byId;
}

/**
 * Prefetch Hilton + Choice CALA directories (idempotent).
 * Prefer full CALA country coverage over Mexico-only caches.
 */
export async function warmFamilyDirectoryCaches(opts = {}) {
  const delayMs = opts.delayMs ?? 100;
  const { ensureHiltonCalaDirectoryCache } = await import(
    "./census-autopilot-hilton-cala-discovery-adapter.js"
  );
  const { ensureChoiceCalaRegionalCache } = await import(
    "./census-autopilot-choice-cala-discovery-adapter.js"
  );
  const hilton = await ensureHiltonCalaDirectoryCache({
    delayMs,
    force: opts.force,
    countries: opts.countries || null,
  });
  const choice = await ensureChoiceCalaRegionalCache({
    timeoutMs: opts.timeoutMs || 60000,
    force: opts.force,
    countries: opts.countries || null,
  });
  let marriott = new Map();
  try {
    const { ensureMarriottCalaCountrySitemapCache } = await import(
      "./census-autopilot-marriott-discovery-adapter.js"
    );
    marriott = await ensureMarriottCalaCountrySitemapCache({
      delayMs: delayMs,
      force: opts.force,
      countries: opts.countries || null,
    });
  } catch (err) {
    CACHE.errors.push({
      family: "Marriott",
      error: err?.message || String(err),
    });
  }
  CACHE.hilton = hilton;
  CACHE.choice = choice;
  CACHE.marriott = marriott;
  CACHE.loadedAt = new Date().toISOString();
  return {
    version: FAMILY_ADAPTER_VERSION,
    loaded_at: CACHE.loadedAt,
    hilton_count: hilton.size,
    choice_count: choice.size,
    marriott_count: marriott.size,
    errors: [
      ...(hilton._meta?.errors || []),
      ...(choice._meta?.errors || []),
      ...(marriott._meta?.errors || []),
      ...CACHE.errors,
    ],
    hilton_meta: hilton._meta || null,
    choice_meta: choice._meta || null,
    marriott_meta: marriott._meta || null,
  };
}

/**
 * Lookup Hilton directory row for a census record (CALA-wide when warmed).
 */
export async function lookupHiltonDirectoryRow(fields, identityKey, opts = {}) {
  const ctyhocn = extractHiltonCtyhocn(fields, identityKey);
  if (!ctyhocn) return { ok: false, reason: "missing_ctyhocn" };
  if (!CACHE.hilton || opts.force) {
    const { ensureHiltonCalaDirectoryCache } = await import(
      "./census-autopilot-hilton-cala-discovery-adapter.js"
    );
    CACHE.hilton = await ensureHiltonCalaDirectoryCache(opts);
  }
  const row = CACHE.hilton.get(ctyhocn);
  if (!row) return { ok: false, reason: "not_in_hilton_cala_directory", ctyhocn };
  return { ok: true, ctyhocn, row };
}

/**
 * Match Choice directory row by canonical name + brand + city + country
 * when property ID is missing or not on the regional page.
 */
export function matchChoiceDirectoryRowByNameCity(fields = {}, cache = null) {
  const map = cache || CACHE.choice;
  if (!map || typeof map.values !== "function") return null;
  const name = norm(fields["Property Name"] || fields["Canonical Property Name"]);
  const city = norm(fields.City);
  const country = norm(fields.Country);
  const brand = norm(fields["Current Brand"] || "");
  if (!name || name.length < 4) return null;

  let best = null;
  let bestScore = 0;
  for (const row of map.values()) {
    if (!row || typeof row !== "object" || !row.propertyId) continue;
    const rName = norm(row.name);
    if (!rName) continue;
    const rCity = norm(row.city);
    const rCountry = norm(row.country);
    let score = 0;
    if (rName === name) score += 50;
    else if (rName.includes(name) || name.includes(rName)) score += 30;
    else continue;
    if (city && rCity && (rCity === city || rCity.includes(city) || city.includes(rCity))) {
      score += 25;
    } else if (city && rCity) {
      continue; // city conflict — do not soft-match
    }
    if (country && rCountry && (rCountry === country || rCountry.includes(country) || country.includes(rCountry))) {
      score += 15;
    }
    if (brand && rName.includes(brand.split(" ")[0])) score += 5;
    if (score > bestScore) {
      bestScore = score;
      best = row;
    }
  }
  if (bestScore >= 75 && best) return best;
  return null;
}

/**
 * Lookup Choice regional card for a census record (CALA-wide when warmed).
 * Prefers property ID; falls back to name + brand + city + country.
 */
export async function lookupChoiceRegionalRow(fields, identityKey, opts = {}) {
  if (!CACHE.choice || opts.force) {
    const { ensureChoiceCalaRegionalCache } = await import(
      "./census-autopilot-choice-cala-discovery-adapter.js"
    );
    CACHE.choice = await ensureChoiceCalaRegionalCache(opts);
  }
  const propertyId = extractChoicePropertyId(fields, identityKey);
  if (propertyId) {
    const row = CACHE.choice.get(propertyId);
    if (row) return { ok: true, propertyId, row, match: "property_id" };
  }

  // Exact property-level URL match against directory propertyUrl
  const censusUrl = String(
    fields["Official Property URL"] || fields["Official URL"] || fields["Source URL"] || ""
  )
    .trim()
    .replace(/\/+$/, "")
    .toLowerCase();
  if (censusUrl.includes("choicehotels.com")) {
    for (const row of CACHE.choice.values()) {
      const u = String(row?.propertyUrl || "")
        .trim()
        .replace(/\/+$/, "")
        .toLowerCase();
      if (u && u === censusUrl) {
        return {
          ok: true,
          propertyId: row.propertyId,
          row,
          match: "exact_property_url",
        };
      }
    }
  }

  const byName = matchChoiceDirectoryRowByNameCity(fields, CACHE.choice);
  if (byName) {
    return {
      ok: true,
      propertyId: byName.propertyId,
      row: byName,
      match: "name_brand_city_country",
    };
  }

  return {
    ok: false,
    reason: propertyId ? "not_in_choice_cala_regional" : "missing_choice_property_id",
    propertyId: propertyId || null,
  };
}

/**
 * Lookup Marriott country-sitemap row (MARSHA → URL/title metadata only).
 */
export async function lookupMarriottSitemapRow(fields, identityKey, opts = {}) {
  const marsha = extractMarshaCode(fields, identityKey);
  if (!marsha) return { ok: false, reason: "missing_marsha" };
  if (!CACHE.marriott || opts.force) {
    const { ensureMarriottCalaCountrySitemapCache } = await import(
      "./census-autopilot-marriott-discovery-adapter.js"
    );
    CACHE.marriott = await ensureMarriottCalaCountrySitemapCache(opts);
  }
  const row = CACHE.marriott.get(marsha);
  if (!row) return { ok: false, reason: "not_in_marriott_cala_sitemap", marsha };
  return { ok: true, marsha, row };
}

/**
 * Lookup Accor catalog property (address + phone from official API).
 */
export async function lookupAccorCatalogRow(fields, identityKey, opts = {}) {
  const propertyId = extractAccorPropertyId(fields, identityKey);
  if (!propertyId) return { ok: false, reason: "missing_accor_property_id" };
  if (!CACHE.accor) CACHE.accor = new Map();
  if (CACHE.accor.has(propertyId) && !opts.force) {
    const cached = CACHE.accor.get(propertyId);
    if (cached?.ok === false) return cached;
    if (cached?.row) return { ok: true, propertyId, row: cached.row };
  }
  try {
    const { fetchAccorCatalogByIds } = await import("../accor-catalog-api.js");
    const result = await Promise.race([
      fetchAccorCatalogByIds([propertyId], opts),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("accor_catalog_timeout")), opts.timeoutMs || 12000)
      ),
    ]);
    const hotel = (result.hotels || [])[0] || null;
    if (!hotel) {
      const miss = { ok: false, reason: "not_in_accor_catalog", propertyId };
      CACHE.accor.set(propertyId, miss);
      return miss;
    }
    CACHE.accor.set(propertyId, { ok: true, row: hotel });
    return { ok: true, propertyId, row: hotel };
  } catch (err) {
    return {
      ok: false,
      reason: "accor_catalog_error",
      propertyId,
      error: err?.message || String(err),
    };
  }
}

/**
 * Build address candidate from family directory (before property-page fetch).
 */
export async function resolveDirectoryAddressCandidate({
  fields = {},
  identityKey = "",
  family = null,
  skipVic = false,
} = {}) {
  const fam = family || familyFromIdentity(fields, identityKey);

  if (fam === "Hilton") {
    const hit = await lookupHiltonDirectoryRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const address =
      String(hit.row.addressLine1 || "").trim() ||
      String(hit.row.addressFormatted || "")
        .split(",")[0]
        ?.trim() ||
      "";
    if (!isStreetLevelAddress(address)) {
      return { ok: false, family: fam, reason: "directory_address_not_street_level", ctyhocn: hit.ctyhocn };
    }
    return {
      ok: true,
      family: fam,
      address,
      source_url: hit.row.propertyUrl || hit.row.website || hit.row.sourceUrl,
      confidence: "High",
      method: "hilton_locations_directory",
      source_type: hit.row.propertyUrl || hit.row.website
        ? "official_property_page"
        : "official_brand_directory",
      ctyhocn: hit.ctyhocn,
      property_name: hit.row.name,
      city: hit.row.city,
      country: hit.row.country || "Mexico",
    };
  }

  if (fam === "Choice") {
    const hit = await lookupChoiceRegionalRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const line1 = String(hit.row.addressLine1 || "").trim();
    const line2 = String(hit.row.addressLine2 || "").trim();
    // Choice cards often put the house number on line 2 (e.g. "Carretera Mexico" + "Toluca No. 5454 …").
    const combined = [line1, line2].filter(Boolean).join(", ");
    const intersectionOk = (a) => {
      const s = String(a || "").trim();
      if (s.length < 15) return false;
      if (/^[A-Z0-9]{4,}\+[A-Z0-9]{2,}\b/i.test(s)) return false;
      return /\b(avenue|ave\.?|street|st\.?|calle|carrera|avenida|carretera|blvd|boulevard|via|vía|road|camino|autopista|periferico|periférico|avenida)\b/i.test(
        s
      );
    };
    const address =
      (isStreetLevelAddress(combined) && combined) ||
      (isStreetLevelAddress(line1) && line1) ||
      (intersectionOk(combined) && combined) ||
      (intersectionOk(line1) && line1) ||
      "";
    if (!address) {
      return {
        ok: false,
        family: fam,
        reason: "directory_address_not_street_level",
        propertyId: hit.propertyId,
        address_line1: line1 || null,
        address_line2: line2 || null,
      };
    }
    // Prefer census Official Property URL, then directory property URL; never regional as Address Source URL
    const censusOfficial = String(
      fields["Official Property URL"] || fields["Official URL"] || ""
    ).trim();
    const sourceUrl =
      (censusOfficial.includes("choicehotels.com") ? censusOfficial : null) ||
      hit.row.propertyUrl ||
      null;
    if (!sourceUrl || /regional-hotels/i.test(sourceUrl)) {
      return {
        ok: false,
        family: fam,
        reason: "choice_missing_property_level_source_url",
        propertyId: hit.propertyId,
      };
    }
    return {
      ok: true,
      family: fam,
      address,
      source_url: sourceUrl,
      evidence_directory_url: hit.row.source_url,
      confidence: "High",
      method:
        hit.match === "name_brand_city_country"
          ? "choice_regional_name_city_match"
          : hit.match === "exact_property_url"
            ? "choice_exact_property_url"
            : "choice_regional_hotel_card",
      source_type: "official_property_page",
      propertyId: hit.propertyId,
      property_name: hit.row.name,
      city: hit.row.city,
      state: hit.row.state || null,
      country: hit.row.country || fields.Country || null,
      match: hit.match || "property_id",
    };
  }

  if (fam === "Marriott") {
    const hit = await lookupMarriottSitemapRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    // Country hotel-sitemaps expose MARSHA + title + URL only — no invent address
    const addr =
      String(hit.row.addressLine1 || hit.row.address || hit.row.streetAddress || "").trim();
    if (!isStreetLevelAddress(addr)) {
      return {
        ok: false,
        family: fam,
        reason: "marriott_sitemap_metadata_lacks_address",
        classification: "source_blocked_level_2",
        marsha: hit.marsha,
        property_url: hit.row.propertyUrl || hit.row.website || null,
      };
    }
    return {
      ok: true,
      family: fam,
      address: addr,
      source_url: hit.row.propertyUrl || hit.row.website || hit.row.sourceUrl,
      confidence: "High",
      method: "marriott_official_sitemap_metadata",
      source_type: "official_brand_directory",
      marsha: hit.marsha,
      city: hit.row.city,
      country: hit.row.country,
    };
  }

  if (fam === "Accor") {
    const hit = await lookupAccorCatalogRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const address = String(hit.row.address1 || "").trim();
    if (!isStreetLevelAddress(address)) {
      return { ok: false, family: fam, reason: "accor_catalog_address_not_street_level" };
    }
    return {
      ok: true,
      family: fam,
      address,
      source_url: hit.row.propertyUrl,
      confidence: "High",
      method: "accor_catalog_api",
      source_type: "official_brand_directory",
      propertyId: hit.propertyId,
      city: hit.row.city,
      country: hit.row.country,
    };
  }

  // VIC claim index for any family (no fetch) — module-cached; never re-read per record.
  // Level 2 Wave 2 skips VIC (skipVic) — too heavy / not official parent adapter.
  if (skipVic) {
    return { ok: false, family: fam, reason: "no_directory_address" };
  }
  if (!CACHE.vic) CACHE.vic = loadVicClaimIndex();
  const vic = CACHE.vic;
  const key = identityKey || fields["Property Identity Key"];
  const rec = key ? vic.byId.get(key) : null;
  const claim = (rec?.field_claims || []).find((c) =>
    ["Address", "Address 1", "Street Address"].includes(c.field)
  );
  if (claim && isStreetLevelAddress(claim.value) && claim.confidence === "High") {
    return {
      ok: true,
      family: fam,
      address: String(claim.value).trim(),
      source_url: claim.evidence_url || rec.official_property_url || null,
      confidence: "High",
      method: "vic_claim",
      source_type: "vic_high_claim",
    };
  }

  return { ok: false, family: fam, reason: "no_directory_address" };
}

/**
 * Build High phone candidate from Hilton directory contactInfo (official).
 * Choice regional cards typically lack phone — return needs_property_page.
 */
export async function resolveDirectoryPhoneCandidate({
  fields = {},
  identityKey = "",
  family = null,
} = {}) {
  const fam = family || familyFromIdentity(fields, identityKey);

  if (fam === "Hilton") {
    const hit = await lookupHiltonDirectoryRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const phone = String(hit.row.phone || "").trim();
    if (!phone || phone.length < 8) {
      return { ok: false, family: fam, reason: "hilton_directory_phone_missing", ctyhocn: hit.ctyhocn };
    }
    return {
      ok: true,
      family: fam,
      phone,
      source_url:
        hit.row.propertyUrl ||
        hit.row.website ||
        hit.row.sourceUrl ||
        null,
      confidence: "High",
      method: "hilton_locations_directory_phone",
      source_type: "official_brand_directory",
      ctyhocn: hit.ctyhocn,
    };
  }

  if (fam === "Choice") {
    const hit = await lookupChoiceRegionalRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const phone = String(hit.row.phone || hit.row.telephone || "").trim();
    if (phone && phone.length >= 8) {
      const sourceUrl =
        String(fields["Official Property URL"] || "").trim() ||
        hit.row.propertyUrl ||
        null;
      if (sourceUrl && !/regional-hotels/i.test(sourceUrl)) {
        return {
          ok: true,
          family: fam,
          phone,
          source_url: sourceUrl,
          confidence: "High",
          method: "choice_directory_phone",
          source_type: "official_brand_directory",
          propertyId: hit.propertyId,
        };
      }
    }
    // Regional cards typically lack phone — require property page / JSON-LD.
    return {
      ok: false,
      family: fam,
      reason: "choice_regional_phone_needs_property_page",
      propertyId: hit.propertyId,
      property_url: hit.row.propertyUrl || null,
    };
  }

  if (fam === "Marriott") {
    const hit = await lookupMarriottSitemapRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const phone = String(hit.row.phone || hit.row.telephone || "").trim();
    if (!phone || phone.length < 8) {
      return {
        ok: false,
        family: fam,
        reason: "marriott_sitemap_metadata_lacks_phone",
        classification: "source_blocked_level_2",
        marsha: hit.marsha,
      };
    }
    return {
      ok: true,
      family: fam,
      phone,
      source_url: hit.row.propertyUrl || hit.row.website,
      confidence: "High",
      method: "marriott_official_sitemap_metadata_phone",
      source_type: "official_brand_directory",
      marsha: hit.marsha,
    };
  }

  if (fam === "Accor") {
    const hit = await lookupAccorCatalogRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const phone = String(hit.row.telephone || "").trim();
    if (!phone || phone.length < 8) {
      return { ok: false, family: fam, reason: "accor_catalog_phone_missing" };
    }
    return {
      ok: true,
      family: fam,
      phone,
      source_url: hit.row.propertyUrl,
      confidence: "High",
      method: "accor_catalog_api_phone",
      source_type: "official_brand_directory",
      propertyId: hit.propertyId,
    };
  }

  return { ok: false, family: fam, reason: "no_directory_phone_adapter" };
}

/**
 * Build amenities candidate from Hilton amenityIds / Choice amenity groups.
 */
export async function resolveDirectoryAmenitiesCandidate({
  fields = {},
  identityKey = "",
  family = null,
} = {}) {
  const fam = family || familyFromIdentity(fields, identityKey);

  if (fam === "Hilton") {
    const hit = await lookupHiltonDirectoryRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const ids = hit.row.amenityIds || [];
    const text = formatAmenitiesText(ids);
    const tags = text
      ? text.split(";").map((s) => s.trim()).filter(Boolean)
      : [];
    if (tags.length < 2) {
      return { ok: false, family: fam, reason: "insufficient_hilton_amenity_ids", ctyhocn: hit.ctyhocn };
    }
    return {
      ok: true,
      family: fam,
      source_text: tags.join("\n"),
      tags,
      confidence: "High",
      method: "hilton_directory_amenityIds",
      source_url: hit.row.sourceUrl,
      patterns_matched: ["hilton_amenityIds"],
    };
  }

  if (fam === "Choice") {
    const hit = await lookupChoiceRegionalRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const tags = [...new Set(hit.row.amenityGroupLabels || [])].filter(Boolean);
    if (tags.length < 2) {
      return {
        ok: false,
        family: fam,
        reason: "insufficient_choice_amenity_groups",
        propertyId: hit.propertyId,
      };
    }
    return {
      ok: true,
      family: fam,
      source_text: tags.join("\n"),
      tags,
      confidence: "High",
      method: "choice_regional_amenity_groups",
      source_url: hit.row.propertyUrl || hit.row.source_url,
      patterns_matched: ["choice_amenityGroups"],
    };
  }

  return { ok: false, family: fam, reason: "family_has_no_directory_amenities_adapter" };
}

/**
 * Choice narrative descriptions are NOT on regional cards (amenity labels only).
 * Returns unsupported so Autopilot does not invent copy.
 */
export async function resolveDirectoryDescriptionCandidate({
  fields = {},
  identityKey = "",
  family = null,
} = {}) {
  const fam = family || familyFromIdentity(fields, identityKey);
  if (fam === "Choice") {
    const hit = await lookupChoiceRegionalRow(fields, identityKey);
    if (hit.ok && hit.row.narrative_description_supported && hit.row.narrative_description) {
      return {
        ok: true,
        family: fam,
        text: hit.row.narrative_description,
        confidence: "High",
        method: "choice_regional_narrative",
        source_url: hit.row.propertyUrl || hit.row.source_url,
      };
    }
    return {
      ok: false,
      family: fam,
      reason: "choice_regional_cards_lack_hotel_narrative_description",
      amenities_available: Boolean(hit.ok && (hit.row?.amenityGroupLabels || []).length >= 2),
    };
  }
  if (fam === "Hilton") {
    return {
      ok: false,
      family: fam,
      reason: "hilton_directory_has_amenities_not_narrative_description",
    };
  }
  return { ok: false, family: fam, reason: "no_directory_description_adapter" };
}

/**
 * Coordinate candidate: Marriott HQV → Hilton directory → Choice regional geo.
 */
export async function resolveDirectoryCoordinateCandidate({
  fields = {},
  identityKey = {},
  family = null,
} = {}) {
  const fam = family || familyFromIdentity(fields, identityKey);

  if (fam === "Marriott") {
    const marsha = extractMarshaCode(fields, identityKey);
    if (!marsha) {
      noteUnresolvedSourcePattern({
        id: "marriott_missing_marsha",
        family: "Marriott",
        pattern: "Marriott record missing MARSHA for HQV",
        what_code_needs_to_learn: "Seed MARSHA from sitemap / Official URL before HQV.",
        sample_url: fields["Official URL"] || fields["Source URL"] || null,
      });
      return { ok: false, family: fam, reason: "missing_marsha", hqv_learning: MARRIOTT_HQV_LEARNING };
    }
    const hqv = await fetchMarriottHqvCoordinates(marsha);
    if (hqv.ok) {
      return {
        ok: true,
        family: fam,
        lat: hqv.lat,
        lng: hqv.lng,
        confidence: hqv.confidence || "High",
        method: hqv.method,
        source_url: hqv.source_url,
        marsha,
      };
    }
    noteUnresolvedSourcePattern({
      id: `marriott_hqv_${hqv.reason || "failed"}`,
      family: "Marriott",
      pattern: `Marriott HQV failed: ${hqv.reason || "unknown"}`,
      what_code_needs_to_learn:
        "Harvest MARRIOTT_GRAPHQL_OPERATION_SIGNATURE / Akamai-safe HQV path.",
      sample_url: fields["Official URL"] || null,
    });
    return {
      ok: false,
      family: fam,
      reason: hqv.reason || "hqv_failed",
      marsha,
      hqv_learning: MARRIOTT_HQV_LEARNING,
    };
  }

  if (fam === "Hilton") {
    const hit = await lookupHiltonDirectoryRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const lat = Number(hit.row.latitude);
    const lng = Number(hit.row.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180) {
      return { ok: false, family: fam, reason: "hilton_directory_coords_invalid", ctyhocn: hit.ctyhocn };
    }
    if (lat === 0 && lng === 0) {
      return { ok: false, family: fam, reason: "hilton_directory_zero_zero", ctyhocn: hit.ctyhocn };
    }
    return {
      ok: true,
      family: fam,
      lat,
      lng,
      confidence: "High",
      method: "hilton_locations_directory_coordinate",
      source_url: hit.row.sourceUrl,
      ctyhocn: hit.ctyhocn,
    };
  }

  if (fam === "Choice") {
    const hit = await lookupChoiceRegionalRow(fields, identityKey);
    if (!hit.ok) return { ok: false, family: fam, reason: hit.reason };
    const lat = Number(hit.row.latitude);
    const lng = Number(hit.row.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return { ok: false, family: fam, reason: "choice_regional_coords_missing", propertyId: hit.propertyId };
    }
    return {
      ok: true,
      family: fam,
      lat,
      lng,
      confidence: "High",
      method: "choice_regional_geoLocation",
      source_url: hit.row.propertyUrl || hit.row.source_url,
      propertyId: hit.propertyId,
    };
  }

  return { ok: false, family: fam, reason: "no_directory_coordinate_adapter" };
}

/**
 * Apply deep page signals when an official page fetch already succeeded.
 */
export function applyDeepOfficialPageSignals(html, url = "") {
  const deep = extractDeepOfficialPageSignals(html, url);
  return {
    ok: true,
    method: "extractDeepOfficialPageSignals",
    source_url: url || null,
    rooms: deep.rooms,
    latitude: deep.latitude,
    longitude: deep.longitude,
    amenitiesMentioned: deep.amenitiesMentioned || [],
    phone: deep.phone,
    openDateHint: deep.openDateHint,
    // Owner/operator hints exist but Autopilot must NOT write them without founder approval
    managementHint: deep.managementHint,
    ownerHint: deep.ownerHint,
    write_owner_operator_allowed: false,
  };
}

/**
 * Unified resolver used by Autopilot queues — directory first, then caller may fetch property URL.
 *
 * @param {{
 *   fields: object,
 *   identityKey?: string,
 *   family?: string,
 *   lanes?: string[],
 *   pageHtml?: string|null,
 *   pageUrl?: string|null,
 * }} opts
 */
export async function resolveFamilyDirectorySignals(opts = {}) {
  const fields = opts.fields || {};
  const identityKey = opts.identityKey || fields["Property Identity Key"] || "";
  const family = opts.family || familyFromIdentity(fields, identityKey);
  const lanes = new Set(opts.lanes || ["address", "amenities", "description", "coordinates"]);

  /** @type {object} */
  const out = {
    version: FAMILY_ADAPTER_VERSION,
    family,
    identity_key: identityKey,
    address: null,
    phone: null,
    amenities: null,
    description: null,
    coordinates: null,
    deep_page: null,
    skipped_property_url_fetch_recommended: false,
  };

  if (lanes.has("address")) {
    out.address = await resolveDirectoryAddressCandidate({ fields, identityKey, family });
  }
  if (lanes.has("phone")) {
    out.phone = await resolveDirectoryPhoneCandidate({ fields, identityKey, family });
  }
  if (lanes.has("amenities")) {
    out.amenities = await resolveDirectoryAmenitiesCandidate({ fields, identityKey, family });
  }
  if (lanes.has("description")) {
    out.description = await resolveDirectoryDescriptionCandidate({ fields, identityKey, family });
  }
  if (lanes.has("coordinates")) {
    out.coordinates = await resolveDirectoryCoordinateCandidate({ fields, identityKey, family });
  }

  if (opts.pageHtml) {
    out.deep_page = applyDeepOfficialPageSignals(opts.pageHtml, opts.pageUrl || "");
    // Merge deep amenities if directory missed
    if (
      (!out.amenities || !out.amenities.ok) &&
      (out.deep_page.amenitiesMentioned || []).length >= 2
    ) {
      out.amenities = {
        ok: true,
        family,
        source_text: out.deep_page.amenitiesMentioned.join("\n"),
        tags: out.deep_page.amenitiesMentioned,
        confidence: "Medium",
        method: "deep_official_page_signals",
        source_url: opts.pageUrl || null,
        patterns_matched: ["extractDeepOfficialPageSignals"],
      };
    }
    if (
      (!out.coordinates || !out.coordinates.ok) &&
      out.deep_page.latitude != null &&
      out.deep_page.longitude != null
    ) {
      out.coordinates = {
        ok: true,
        family,
        lat: out.deep_page.latitude,
        lng: out.deep_page.longitude,
        confidence: "High",
        method: "deep_official_page_signals",
        source_url: opts.pageUrl || null,
      };
    }
  }

  out.skipped_property_url_fetch_recommended = Boolean(
    (out.address?.ok && lanes.has("address")) ||
      (out.amenities?.ok && lanes.has("amenities")) ||
      (out.coordinates?.ok && lanes.has("coordinates"))
  );

  return out;
}

export function getFamilyAdapterCacheStats() {
  return {
    version: FAMILY_ADAPTER_VERSION,
    loaded_at: CACHE.loadedAt,
    hilton_count: CACHE.hilton?.size || 0,
    choice_count: CACHE.choice?.size || 0,
    errors: [...CACHE.errors],
    unresolved_patterns: getUnresolvedSourcePatterns({ minCount: 1 }),
  };
}
