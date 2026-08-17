/**
 * Preferred Hotels & Resorts directory discovery for Autopilot source_discovery.
 *
 * Source: official preferredhotels.com/directory (__NEXT_DATA__ properties).
 * Collection names (L.V.X., Lifestyle, etc.) are metadata only — never create
 * duplicate Brand Setup brands. Brand = Preferred Hotels & Resorts.
 */

import {
  CALA_DISCOVERY_PRIORITY_COUNTRIES,
  resolveDiscoveryCountries,
} from "./census-autopilot-cala-discovery-shared.js";

export const PREFERRED_DISCOVERY_ADAPTER_VERSION =
  "census-autopilot-preferred-directory-discovery-adapter-v1";

export const PREFERRED_ORIGIN = "https://preferredhotels.com";
export const PREFERRED_DIRECTORY_URL = `${PREFERRED_ORIGIN}/directory?numberOfRooms=1`;

export const PREFERRED_DISCOVERY_SOURCE = Object.freeze({
  type: "official_preferred_directory",
  directory_url: PREFERRED_DIRECTORY_URL,
  property_url_pattern: `${PREFERRED_ORIGIN}/hotels/{country-slug}/{property-slug}`,
  brand_display: "Preferred Hotels & Resorts",
  parent_company: "Preferred Hotels & Resorts",
  collection_note:
    "fieldPreferredCollections are collection labels only — do not invent Brand Setup rows",
});

export const PREFERRED_CALA_PRIORITY_COUNTRIES = Object.freeze([
  ...CALA_DISCOVERY_PRIORITY_COUNTRIES,
]);

export const PREFERRED_FETCH_HEADERS = Object.freeze({
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
});

/**
 * @param {string} countryLabel
 */
export function classifyPreferredCountryDiscoveryReadiness(countryLabel) {
  const label = String(countryLabel || "").trim();
  if (!label) {
    return {
      country: label,
      readiness: "needs_adapter",
      ready: false,
      adapter: null,
      note: "Empty Preferred country label",
      directory_url: null,
    };
  }
  const priority = PREFERRED_CALA_PRIORITY_COUNTRIES.includes(label);
  return {
    country: label,
    readiness: "supported",
    ready: true,
    adapter: "ensurePreferredCalaDirectoryCache",
    note: priority
      ? `Official Preferred /directory (priority): ${PREFERRED_DIRECTORY_URL}`
      : `Official Preferred /directory: ${PREFERRED_DIRECTORY_URL}`,
    directory_url: PREFERRED_DIRECTORY_URL,
  };
}

/**
 * Flatten Preferred __NEXT_DATA__ properties buckets.
 * @param {unknown} properties
 */
export function flattenPreferredDirectoryProperties(properties) {
  const out = [];
  if (!Array.isArray(properties)) return out;
  for (const bucket of properties) {
    if (Array.isArray(bucket)) out.push(...bucket);
    else if (bucket && typeof bucket === "object") out.push(...Object.values(bucket));
  }
  return out.filter((p) => p && typeof p === "object");
}

/**
 * Parse Preferred directory HTML → property rows.
 * @param {string} html
 */
export function parsePreferredDirectoryHtml(html) {
  const m = String(html || "").match(
    /<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/
  );
  if (!m) return { ok: false, error: "missing__NEXT_DATA__", propertyRows: [] };

  let data;
  try {
    data = JSON.parse(m[1]);
  } catch (err) {
    return { ok: false, error: `json_parse: ${err?.message || err}`, propertyRows: [] };
  }

  const rawProps = data?.props?.pageProps?.properties;
  const flat = flattenPreferredDirectoryProperties(rawProps);
  /** @type {object[]} */
  const propertyRows = [];
  const seen = new Set();

  for (const p of flat) {
    const nid = p.nid != null ? String(p.nid) : "";
    const path = String(p.entityUrl?.path || "").trim();
    const name = String(p.fieldDisplayTitle || "").trim();
    const country = String(p.fieldCountryName || "").trim();
    if (!nid || !name || !country) continue;
    if (seen.has(nid)) continue;
    seen.add(nid);

    const collections = (p.fieldPreferredCollections || [])
      .map((c) => String(c?.entity?.name || "").trim())
      .filter(Boolean);

    propertyRows.push({
      propertyId: nid,
      nid,
      name,
      brand: PREFERRED_DISCOVERY_SOURCE.brand_display,
      parent: PREFERRED_DISCOVERY_SOURCE.parent_company,
      city: String(p.fieldAddress?.locality || "").trim() || null,
      state: String(p.fieldStateName || "").trim() || null,
      country,
      addressLine1: null,
      propertyUrl: path ? `${PREFERRED_ORIGIN}${path}` : null,
      path,
      collections,
      region: String(p.fieldRegion?.entity?.name || "").trim() || null,
      rooms: p.fieldNumRooms ?? null,
      source: "preferred_directory",
      sourceUrl: PREFERRED_DIRECTORY_URL,
      discovery_adapter: "preferred_directory",
    });
  }

  return {
    ok: true,
    propertyRows,
    total_in_directory: flat.length,
  };
}

/**
 * @param {object} [opts]
 */
export async function ensurePreferredCalaDirectoryCache(opts = {}) {
  if (opts.cache) return opts.cache;

  const countriesWanted = resolveDiscoveryCountries(
    opts.country,
    opts.countries || PREFERRED_CALA_PRIORITY_COUNTRIES
  ).filter((c) => classifyPreferredCountryDiscoveryReadiness(c).ready);
  const wantNorm = new Set(countriesWanted.map((c) => c.toLowerCase()));

  /** @type {Map<string, object>} */
  const byKey = new Map();
  const errors = [];
  const countryStats = [];

  const fetchFn = opts.fetchFn || globalThis.fetch;
  let parsed = { ok: false, propertyRows: [], error: "not_fetched" };

  try {
    const res = await fetchFn(PREFERRED_DIRECTORY_URL, {
      headers: PREFERRED_FETCH_HEADERS,
      redirect: "follow",
    });
    if (!res.ok) {
      errors.push({ stage: "directory_fetch", error: `http_${res.status}` });
      parsed = { ok: false, propertyRows: [], error: `http_${res.status}` };
    } else {
      parsed = parsePreferredDirectoryHtml(await res.text());
      if (!parsed.ok) errors.push({ stage: "directory_parse", error: parsed.error });
    }
  } catch (err) {
    errors.push({ stage: "directory_fetch", error: err?.message || String(err) });
  }

  const countsByCountry = Object.fromEntries(countriesWanted.map((c) => [c, 0]));

  for (const row of parsed.propertyRows || []) {
    const country = String(row.country || "").trim();
    if (!country) continue;
    if (wantNorm.size && !wantNorm.has(country.toLowerCase())) continue;

    const normalized = {
      ...row,
      brand: PREFERRED_DISCOVERY_SOURCE.brand_display,
      parent: PREFERRED_DISCOVERY_SOURCE.parent_company,
    };
    byKey.set(`${country}|${row.propertyId}`, normalized);
    byKey.set(String(row.propertyId), normalized);
    if (countsByCountry[country] != null) countsByCountry[country] += 1;
    else countsByCountry[country] = 1;
  }

  for (const country of countriesWanted) {
    countryStats.push({
      country,
      hotel_count: countsByCountry[country] || 0,
      ok: true,
      mode: "preferred_directory_next_data",
    });
  }

  byKey._meta = {
    version: PREFERRED_DISCOVERY_ADAPTER_VERSION,
    source: PREFERRED_DISCOVERY_SOURCE,
    countries: countriesWanted,
    country_stats: countryStats,
    directory_total: parsed.total_in_directory ?? null,
    cala_filtered: iteratePreferredDirectoryRows(byKey).length,
    errors,
    loaded_at: new Date().toISOString(),
  };
  return byKey;
}

/**
 * @param {Map<string, object>} cache
 */
export function iteratePreferredDirectoryRows(cache) {
  const seen = new Set();
  const rows = [];
  for (const [, row] of cache.entries()) {
    if (!row?.propertyId) continue;
    const key = `${row.country || ""}|${row.propertyId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push(row);
  }
  return rows;
}
