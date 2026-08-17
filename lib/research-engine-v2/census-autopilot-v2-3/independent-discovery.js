/**
 * Independent universe discovery — NO Cvent imports.
 * Lanes A (official brand dirs) + C (bounded SerpApi) for pilot countries.
 */

import { createHash } from "node:crypto";
import { ensureIhgCalaDestinationCache } from "../census-autopilot-ihg-cala-discovery-adapter.js";
import { ensureHiltonCalaDirectoryCache } from "../census-autopilot-hilton-cala-discovery-adapter.js";
import { ensureMarriottCalaCountrySitemapCache } from "../census-autopilot-marriott-discovery-adapter.js";
import { ensureChoiceCalaRegionalCache } from "../census-autopilot-choice-cala-discovery-adapter.js";
import { loadVicRecords } from "../census-autopilot-v2/master-candidate.js";
import { inferBrandFamily, normName } from "../census-autopilot-v2/identity-dedupe.js";
import {
  searchGoogleHotels,
  SerpApiCreditTracker,
  getAccount,
  safeErrorMessage,
} from "../providers/serpapi-google-hotels/index.js";
import { createSerpApiResearchCache } from "../census-autopilot-v2-1/serpapi-cache.js";
import { DISCOVERY_LANES, PILOT_COUNTRIES } from "./constants.js";
import { assertCventAccess } from "./cvent-firewall.js";

const SOFT_RE =
  /curio|tapestry|autograph|tribute|design hotels|luxury collection|preferred hotels|leading hotels|ascend |slh\b|radisson individuals/i;

function pid(country, name, officialId) {
  const base = officialId
    ? `${country}|id:${officialId}`
    : `${country}|${normName(name)}`;
  return `pid_${createHash("sha1").update(base).digest("hex").slice(0, 16)}`;
}

function classifyAsset(name) {
  const n = String(name || "");
  return {
    resort_like: /resort|spa|all.?inclusive|beach|villa/i.test(n),
    urban_like: /inn|suites|express|garden|downtown|city|centro|hotel /i.test(n) && !/resort/i.test(n),
    soft_collection: SOFT_RE.test(n),
  };
}

function toDiscoveryRecord(row) {
  const name = row.name || row.hotelName || row.official_name;
  const country = row.country;
  const family = row.family || row.parent || inferBrandFamily(name);
  const officialId = row.propertyId || row.marshaCode || row.ctyhocn || row.property_ids?.[0] || null;
  const asset = classifyAsset(name);
  const branded = family !== "Independent" && family !== "Unknown";
  return {
    property_identity_id: pid(country, name, officialId),
    discovery_evidence: {
      source_type: row.source_type || "official_brand_directory",
      source_url: row.sourceUrl || row.propertyUrl || row.website || null,
      lane: row.lane || DISCOVERY_LANES.A_OFFICIAL_BRAND,
      retrieved_at: new Date().toISOString(),
      confidence: row.discovery_confidence || "HIGH",
      evidence_class: "DISCOVERY_EVIDENCE",
    },
    // Physical hotel — brand is temporal affiliation, not immutable identity
    physical: {
      current_name: name,
      country,
      city: row.city || null,
      official_url: row.propertyUrl || row.website || null,
      official_property_id: officialId,
      lat: row.latitude ?? row.lat ?? null,
      lng: row.longitude ?? row.lng ?? null,
    },
    affiliation: {
      // Property-level brand only — never default to parent/source family.
      current_brand: row.brand || row.affiliation || null,
      brand_family: family,
      historical_brands: [],
      current_operator: null,
      historical_operators: [],
    },
    strata: {
      branded,
      independent: !branded,
      soft_collection: asset.soft_collection || row.lane === DISCOVERY_LANES.B_SOFT_COLLECTION,
      resort_like: asset.resort_like,
      urban_like: asset.urban_like,
      discovery_lane: row.lane || DISCOVERY_LANES.A_OFFICIAL_BRAND,
      serpapi_call_type: row.serpapi_call_type || null,
    },
    cvent_used_as_production_evidence: false,
    field_evidence: {}, // enrichment comes later — discovery ≠ Golden completeness
  };
}

function dedupePhysical(records) {
  const byId = new Map();
  const byNameCountry = new Map();
  for (const r of records) {
    const id = r.property_identity_id;
    if (byId.has(id)) {
      const prev = byId.get(id);
      // Prefer official ID / richer record
      if (!prev.physical.official_property_id && r.physical.official_property_id) {
        byId.set(id, r);
      }
      continue;
    }
    const nk = `${r.physical.country}|${normName(r.physical.current_name)}`;
    if (byNameCountry.has(nk)) {
      const prevId = byNameCountry.get(nk);
      const prev = byId.get(prevId);
      if (prev && !prev.physical.official_property_id && r.physical.official_property_id) {
        byId.delete(prevId);
        byId.set(id, r);
        byNameCountry.set(nk, id);
      }
      continue;
    }
    byId.set(id, r);
    byNameCountry.set(nk, id);
  }
  return [...byId.values()];
}

/**
 * Lane A — official brand/parent directories for pilot countries.
 * Intentionally does NOT call assertCventAccess('discovery') with Cvent — we assert
 * that discovery code path never requests Cvent.
 */
export async function discoverOfficialBrandDirectories(countries, opts = {}) {
  // Prove discovery cannot open Cvent
  try {
    assertCventAccess("discovery");
  } catch (err) {
    if (err.code !== "CVENT_FIREWALL_DISCOVERY") throw err;
  }

  const log = opts.log || (() => {});
  const rows = [];
  const coverage = [];

  // IHG
  log("[v2.3] Lane A — IHG CALA destination…");
  try {
    const ihg = await ensureIhgCalaDestinationCache({ countries, delayMs: opts.delayMs ?? 150 });
    let n = 0;
    for (const [k, row] of ihg) {
      if (!String(k).includes("|")) continue; // skip id-only duplicate keys
      if (!countries.includes(row.country)) continue;
      rows.push(
        toDiscoveryRecord({
          ...row,
          name: row.name || row.hotelName,
          family: "IHG",
          parent: "IHG",
          lane: DISCOVERY_LANES.A_OFFICIAL_BRAND,
          source_type: "official_brand_directory",
          discovery_confidence: "HIGH",
        })
      );
      n += 1;
    }
    coverage.push({ family: "IHG", hotels: n, ok: true });
  } catch (err) {
    coverage.push({ family: "IHG", hotels: 0, ok: false, error: String(err?.message || err) });
  }

  // Hilton
  log("[v2.3] Lane A — Hilton CALA locations…");
  try {
    const hilton = await ensureHiltonCalaDirectoryCache({ countries, delayMs: opts.delayMs ?? 120 });
    let n = 0;
    for (const [k, row] of hilton) {
      if (!String(k).includes("|")) continue;
      if (!countries.includes(row.country)) continue;
      rows.push(
        toDiscoveryRecord({
          ...row,
          name: row.name,
          family: "Hilton",
          parent: "Hilton",
          propertyId: row.ctyhocn,
          lane: SOFT_RE.test(row.name || "")
            ? DISCOVERY_LANES.B_SOFT_COLLECTION
            : DISCOVERY_LANES.A_OFFICIAL_BRAND,
          source_type: "official_brand_directory",
        })
      );
      n += 1;
    }
    coverage.push({ family: "Hilton", hotels: n, ok: true });
  } catch (err) {
    coverage.push({ family: "Hilton", hotels: 0, ok: false, error: String(err?.message || err) });
  }

  // Marriott
  log("[v2.3] Lane A — Marriott country sitemaps…");
  try {
    const marriott = await ensureMarriottCalaCountrySitemapCache({
      countries,
      delayMs: opts.delayMs ?? 200,
    });
    let n = 0;
    for (const [k, row] of marriott) {
      if (k === "_meta" || row._meta) continue;
      if (!String(k).includes("|")) continue; // prefer country|marsha keys
      if (!countries.includes(row.country)) continue;
      rows.push(
        toDiscoveryRecord({
          ...row,
          name: row.name,
          family: "Marriott",
          parent: "Marriott",
          propertyId: row.marshaCode,
          lane: SOFT_RE.test(row.name || row.brand || "")
            ? DISCOVERY_LANES.B_SOFT_COLLECTION
            : DISCOVERY_LANES.A_OFFICIAL_BRAND,
          source_type: "official_brand_directory",
        })
      );
      n += 1;
    }
    coverage.push({ family: "Marriott", hotels: n, ok: true });
  } catch (err) {
    coverage.push({ family: "Marriott", hotels: 0, ok: false, error: String(err?.message || err) });
  }

  // Choice
  log("[v2.3] Lane A — Choice regional…");
  try {
    const choice = await ensureChoiceCalaRegionalCache({ countries, delayMs: opts.delayMs ?? 150 });
    let n = 0;
    for (const [k, row] of choice) {
      if (k === "_meta" || row._meta) continue;
      if (!countries.includes(row.country)) continue;
      rows.push(
        toDiscoveryRecord({
          ...row,
          name: row.name || row.hotelName,
          family: "Choice",
          parent: "Choice",
          propertyId: row.propertyId || row.hotelId,
          lane: SOFT_RE.test(row.name || "")
            ? DISCOVERY_LANES.B_SOFT_COLLECTION
            : DISCOVERY_LANES.A_OFFICIAL_BRAND,
          source_type: "official_brand_directory",
        })
      );
      n += 1;
    }
    coverage.push({ family: "Choice", hotels: n, ok: true });
  } catch (err) {
    coverage.push({ family: "Choice", hotels: 0, ok: false, error: String(err?.message || err) });
  }

  return { rows, coverage };
}

/**
 * Seed VIC Mexico records as independent discovery (already non-Cvent).
 */
export function discoverVicMexicoSeeds(root, countries) {
  if (!countries.includes("Mexico")) return [];
  const vic = loadVicRecords(root);
  return vic.map((r) =>
    toDiscoveryRecord({
      name: r.name,
      country: r.country || "Mexico",
      city: r.city,
      brand: r.brand,
      family: r.family,
      website: r.website,
      propertyUrl: r.website,
      propertyId: r.property_ids?.[0],
      property_ids: r.property_ids,
      lane: DISCOVERY_LANES.A_OFFICIAL_BRAND,
      source_type: "verified_independent_census_seed",
      sourceUrl: r.website,
      discovery_confidence: "HIGH",
    })
  );
}

/** Representative cities for SerpApi independent discovery (not from Cvent). */
const DISCOVERY_CITIES = {
  Mexico: ["Mexico City", "Cancun", "Guadalajara", "Monterrey", "Puerto Vallarta"],
  "Dominican Republic": ["Punta Cana", "Santo Domingo", "Puerto Plata"],
  "Costa Rica": ["San Jose", "Liberia", "Jaco"],
  Colombia: ["Bogota", "Cartagena", "Medellin"],
  Brazil: ["Sao Paulo", "Rio de Janeiro", "Salvador"],
  Argentina: ["Buenos Aires", "Mendoza", "Cordoba"],
  Jamaica: ["Montego Bay", "Kingston", "Ocho Rios"],
  Barbados: ["Bridgetown", "Holetown"],
};

/**
 * Lane C — SerpApi Google Hotels discovery for independent / long-tail hotels.
 * Tracks SERPAPI_DISCOVERY_CALL separately from enrichment.
 */
export async function discoverViaSerpApi(repoRoot, countries, opts = {}) {
  const ceiling = opts.ceiling ?? Number(process.env.CAV23_SERPAPI_DISCOVERY_CEILING || 80);
  const log = opts.log || (() => {});
  const cache = createSerpApiResearchCache(repoRoot);
  const account = await getAccount();
  const starting = account.ok ? account.total_searches_left ?? account.plan_searches_left : null;
  const tracker = new SerpApiCreditTracker({ ceiling, startingSearchesLeft: starting });

  const rows = [];
  const queries = [];
  for (const country of countries) {
    for (const city of DISCOVERY_CITIES[country] || []) {
      queries.push({ country, city, q: `hotels in ${city}, ${country}` });
    }
  }

  let discoveryCalls = 0;
  for (const { country, city, q } of queries) {
    if (!tracker.canSpend(1)) break;
    try {
      const search = await searchGoogleHotels(
        { q, gl: "us" },
        { tracker, hotelId: `disc_${country}_${city}` }
      );
      discoveryCalls += search.from_dealality_cache ? 0 : 1;
      // Mark call type on tracker purpose via hotelId prefix
      for (const cand of search.candidates || []) {
        const name = cand.name;
        if (!name) continue;
        const family = inferBrandFamily(name);
        rows.push(
          toDiscoveryRecord({
            name,
            country,
            city: city,
            brand: family !== "Independent" ? family : null,
            family,
            website: cand.website,
            propertyUrl: cand.website || cand.google_property_url,
            latitude: cand.latitude,
            longitude: cand.longitude,
            lane:
              family === "Independent"
                ? DISCOVERY_LANES.C_INDEPENDENT
                : DISCOVERY_LANES.A_OFFICIAL_BRAND,
            source_type: "serpapi_google_hotels_discovery",
            sourceUrl: cand.google_property_url || null,
            discovery_confidence: "MEDIUM",
            serpapi_call_type: "SERPAPI_DISCOVERY_CALL",
            propertyId: null, // Google token is external — not Dealality primary
          })
        );
      }
      // Persist lightly in Dealality cache via search path already in provider
      if ((discoveryCalls + rows.length) % 20 === 0) {
        log(`[v2.3] SerpApi discovery calls≈${tracker.charged} hotels≈${rows.length}`);
      }
    } catch (err) {
      log(`[v2.3] SerpApi discovery error: ${safeErrorMessage(err)}`);
    }
    await new Promise((r) => setTimeout(r, 250));
  }

  const accountEnd = await getAccount();
  return {
    rows,
    discovery_calls: tracker.charged,
    actual_delta:
      starting != null && accountEnd.ok
        ? starting - (accountEnd.total_searches_left ?? accountEnd.plan_searches_left)
        : tracker.charged,
    queries_planned: queries.length,
    cache_stats: cache.stats(),
  };
}

/**
 * Full independent discovery for pilot countries.
 */
export async function runIndependentDiscovery(opts) {
  const { root, countries = PILOT_COUNTRIES, log = console.log } = opts;

  // Firewall: discovery must not access Cvent
  try {
    assertCventAccess("discovery");
    throw new Error("Firewall failed open");
  } catch (err) {
    if (err.code !== "CVENT_FIREWALL_DISCOVERY") throw err;
  }

  const official = await discoverOfficialBrandDirectories(countries, {
    log,
    delayMs: opts.delayMs,
  });
  const vicSeeds = discoverVicMexicoSeeds(root, countries);
  const serp = await discoverViaSerpApi(root, countries, {
    ceiling: opts.serpapiDiscoveryCeiling,
    log,
  });

  const merged = dedupePhysical([...official.rows, ...vicSeeds, ...serp.rows]);

  const byCountry = {};
  const byLane = {};
  let branded = 0;
  let independent = 0;
  let resorts = 0;
  let urban = 0;
  let soft = 0;
  let officialDir = 0;
  let serpapiDisc = 0;
  let other = 0;

  for (const r of merged) {
    byCountry[r.physical.country] = (byCountry[r.physical.country] || 0) + 1;
    const lane = r.strata.discovery_lane;
    byLane[lane] = (byLane[lane] || 0) + 1;
    if (r.strata.branded) branded += 1;
    else independent += 1;
    if (r.strata.resort_like) resorts += 1;
    if (r.strata.urban_like) urban += 1;
    if (r.strata.soft_collection) soft += 1;
    if (r.discovery_evidence.source_type === "official_brand_directory") officialDir += 1;
    else if (r.discovery_evidence.source_type === "serpapi_google_hotels_discovery") serpapiDisc += 1;
    else if (r.discovery_evidence.source_type === "verified_independent_census_seed") other += 1;
    else other += 1;
  }

  return {
    version: "independent-discovery-v2.3",
    countries,
    brand_directory_coverage: official.coverage,
    raw_before_dedupe: official.rows.length + vicSeeds.length + serp.rows.length,
    unique_physical: merged.length,
    stats: {
      branded,
      independent,
      resorts,
      urban,
      soft_collection: soft,
      official_directory: officialDir,
      serpapi_discovery: serpapiDisc,
      other_approved: other,
      by_country: byCountry,
      by_lane: byLane,
    },
    serpapi_discovery: {
      calls: serp.discovery_calls,
      actual_delta: serp.actual_delta,
      queries_planned: serp.queries_planned,
    },
    records: merged,
  };
}
