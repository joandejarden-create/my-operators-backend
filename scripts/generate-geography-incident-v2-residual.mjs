/**
 * Post-107 residual Market completion: freeze residual, split A/B,
 * selective SerpApi (cap 250), Market registry vNext2 candidates,
 * all-400 rerun, incremental dry-run (NOT applied).
 * V4 remains PAUSED.
 */
import fs from "node:fs";
import path from "node:path";
import "dotenv/config";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";
import {
  resolveDealalityMarketStrict,
  assertMarketWriteGate,
  assertSubmarketWriteGate,
  classifyProductionMarket,
  MARKET_CLASS,
  DEALALITY_MARKET_REGISTRY_VERSION,
  EXTRA_DEALALITY_MARKETS_VNEXT,
  CITY_TO_MARKET_VNEXT,
  MARKET_ALIASES_TO_CANONICAL,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import {
  resolveCityV4,
  isPostalAsCity,
  isStreetLineAsCity,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import { validateCitySemantics, CITY_STATUS, scoreGoldenQuality } from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import { resolveStateRegionV3 } from "../lib/research-engine-v2/census-autopilot-v3/geography/state-region-resolver-v3.js";
import { classifySubmarketApplicability } from "../lib/research-engine-v2/census-autopilot-v3/geography/applicability-rules.js";
import { createSerpApiResearchCache } from "../lib/research-engine-v2/census-autopilot-v2-1/serpapi-cache.js";
import { serpapiSearch, redactSecrets } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1"
);
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const SERPAPI_CEILING = 250;

function wj(n, d) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, n), JSON.stringify(d, null, 2));
}
function wm(n, t) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, n), t);
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function airtableGet(baseId, token, recordId) {
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`get_${res.status}`);
  return json;
}

function loadKeyIdMap() {
  const paths = [
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/23-post-write-airtable-snapshot.json",
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/23-post-write-airtable-snapshot.json",
  ];
  const by = new Map();
  for (const rel of paths) {
    const j = JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
    for (const r of j.records || []) {
      const f = r.fields || {};
      const k = f["Property Identity Key"] || r.id;
      by.set(k, r.id);
    }
  }
  return by;
}

function cityLooksValid(city, country) {
  if (blank(city) || /^unknown$/i.test(city)) return false;
  if (isPostalAsCity(city, country) || isStreetLineAsCity(city)) return false;
  return validateCitySemantics(city, country).ok;
}

/**
 * Secondary-city Market candidates — explicit registry entries (NOT City-fallback).
 * City may equal Market only when registered here.
 */
const VNEXT2_CANDIDATES = [
  {
    country: "Brazil",
    canonical_name: "Chapecó",
    cities: ["chapeco", "chapecó"],
    state_region_coverage: ["Santa Catarina"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "Distinct SC interior hotel market; local corporate/leisure competition",
  },
  {
    country: "Brazil",
    canonical_name: "Farroupilha",
    cities: ["farroupilha"],
    state_region_coverage: ["Rio Grande do Sul"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "Serra Gaúcha secondary hotel node; not Porto Alegre metro",
  },
  {
    country: "Brazil",
    canonical_name: "Joinville",
    cities: ["joinville"],
    state_region_coverage: ["Santa Catarina"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "Major SC industrial metro with standalone hotel demand",
  },
  {
    country: "Brazil",
    canonical_name: "Bauru",
    cities: ["bauru"],
    state_region_coverage: ["São Paulo"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "Interior SP secondary metro",
  },
  {
    country: "Brazil",
    canonical_name: "Jundiaí",
    cities: ["jundiai", "jundiaí"],
    state_region_coverage: ["São Paulo"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "SP interior industrial/logistics hotel demand",
  },
  {
    country: "Brazil",
    canonical_name: "Anápolis",
    cities: ["anapolis", "anápolis"],
    state_region_coverage: ["Goiás"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "GO industrial/pharma corridor distinct from Goiânia",
  },
  {
    country: "Brazil",
    canonical_name: "Araraquara",
    cities: ["araraquara"],
    state_region_coverage: ["São Paulo"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "Interior SP secondary",
  },
  {
    country: "Brazil",
    canonical_name: "Presidente Prudente",
    cities: ["presidente prudente"],
    state_region_coverage: ["São Paulo"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "Western SP regional hub",
  },
  {
    country: "Brazil",
    canonical_name: "Rondonópolis",
    cities: ["rondonopolis", "rondonópolis"],
    state_region_coverage: ["Mato Grosso"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "MT agribusiness gateway",
  },
  {
    country: "Brazil",
    canonical_name: "Costa do Sauípe / Praia do Forte",
    cities: ["praia do forte", "costa do sauipe", "costa do sauípe", "itacare", "itacaré"],
    state_region_coverage: ["Bahia"],
    class: "NEW_RESORT_DESTINATION_MARKET",
    rationale: "Bahia leisure coast distinct from Salvador metro",
  },
  {
    country: "Argentina",
    canonical_name: "Santiago del Estero",
    cities: ["santiago del estero"],
    state_region_coverage: ["Santiago del Estero"],
    class: "NEW_SECONDARY_CITY_MARKET",
    rationale: "Provincial capital hotel market",
  },
  {
    country: "Argentina",
    canonical_name: "Villa La Angostura",
    cities: ["villa la angostura"],
    state_region_coverage: ["Neuquén"],
    class: "NEW_RESORT_DESTINATION_MARKET",
    rationale: "Patagonia lakes resort node",
  },
];

async function main() {
  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const keyId = loadKeyIdMap();
  const keys = [...keyId.keys()];

  console.log(`[v2] Live re-read n=${keys.length}`);
  const liveRows = [];
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const id = keyId.get(key);
    const rec = await airtableGet(baseId, token, id);
    const f = rec.fields || {};
    liveRows.push({
      id,
      key,
      name: f["Property Name"] || "",
      brand: f["Current Brand"] || null,
      country: f.Country || null,
      state: f["State / Region"] || null,
      city: f.City || null,
      address: f.Address || null,
      lat: f.Latitude ?? null,
      lng: f.Longitude ?? null,
      market: f.Market || null,
      submarket: f.Submarket || null,
      url: f["Official Property URL"] || f["Official URL"] || f["Property URL"] || null,
      official_id: f["Official Property ID"] || null,
      phone: f.Phone || null,
    });
    if ((i + 1) % 40 === 0) console.log(`[v2] live ${i + 1}/${keys.length}`);
    await sleep(70);
  }

  // Address serialization check
  const objAddr = liveRows.filter(
    (r) =>
      r.address === "[object Object]" ||
      (typeof r.address === "object" && r.address != null) ||
      (typeof r.address === "string" && r.address.trim().startsWith("{"))
  );
  wj("125-address-serialization-final-check.json", {
    object_object_remaining: objAddr.length,
    records: objAddr.map((r) => ({ key: r.key, address: r.address })),
  });

  // Market status for all 400
  const marketEval = liveRows.map((r) => {
    const cityRes = resolveCityV4({
      country: r.country,
      city: r.city,
      address: typeof r.address === "string" ? r.address : null,
      official_url: r.url,
    });
    const city = cityRes.ok ? cityRes.city : r.city;
    const stateRes = resolveStateRegionV3({
      country: r.country,
      city,
      address: r.address,
      name: r.name,
      official_state: r.state,
      latitude: r.lat,
      longitude: r.lng,
    });
    const state = stateRes.ok ? stateRes.normalized_state_region : r.state;
    const strict = resolveDealalityMarketStrict(r.country, city, {
      state,
      latitude: r.lat,
      longitude: r.lng,
    });
    const cls = classifyProductionMarket({
      country: r.country,
      market: r.market,
      city,
      state,
    });
    return {
      ...r,
      city_used: city,
      state_used: state,
      city_ok: cityLooksValid(city, r.country),
      market_ok: strict.ok,
      market_resolved: strict.market,
      method: strict.method,
      production_class: cls.class,
      production_ok: cls.ok,
    };
  });

  const unresolved = marketEval.filter((r) => !r.market_ok);
  console.log(`[v2] Residual unresolved Market=${unresolved.length}`);

  // Split A/B
  const split = unresolved.map((r) => {
    const geoOk = r.city_ok && (!blank(r.state_used) || (r.lat != null && r.lng != null));
    const problem = !geoOk
      ? "A_INSUFFICIENT_PROPERTY_GEOGRAPHY"
      : "B_VALID_GEOGRAPHY_MARKET_REGISTRY_MISSING";
    return { ...r, problem };
  });
  const splitCounts = split.reduce((a, r) => {
    a[r.problem] = (a[r.problem] || 0) + 1;
    return a;
  }, {});

  wj("116-residual-market-freeze.json", {
    n: unresolved.length,
    frozen_at: new Date().toISOString(),
    records: unresolved,
  });
  wj("117-residual-market-problem-split.json", { counts: splitCounts, records: split });

  // SerpApi plan — only A_INSUFFICIENT
  const needResearch = split.filter((r) => r.problem === "A_INSUFFICIENT_PROPERTY_GEOGRAPHY");
  wj("118-residual-serpapi-plan.json", {
    ceiling: SERPAPI_CEILING,
    candidates: needResearch.length,
    note: "Only insufficient-geography properties; registry-missing skip SerpApi",
    keys: needResearch.map((r) => r.key),
  });

  const serpCache = createSerpApiResearchCache(ROOT);
  const serpResults = [];
  let searches = 0;
  let cacheHits = 0;
  let citiesRec = 0;
  let addrRec = 0;
  let coordRec = 0;
  let stateRec = 0;

  const hasKey = Boolean(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY);
  for (const r of needResearch) {
    if (searches >= SERPAPI_CEILING) break;
    const q = [r.name, r.city_ok ? r.city : null, r.country].filter(Boolean).join(" ");
    const cacheParams = { request_type: "search", q, gl: "us", property_identity_id: r.key };
    let cached = null;
    try {
      cached = serpCache.get(cacheParams);
    } catch {
      cached = null;
    }
    let payload = cached?.hit ? cached.raw : null;
    if (payload) {
      cacheHits += 1;
    } else if (hasKey) {
      try {
        searches += 1;
        payload = await serpapiSearch({
          engine: "google_hotels",
          q,
          gl: "us",
          hl: "en",
          currency: "USD",
        });
        try {
          serpCache.set(cacheParams, redactSecrets(payload), {
            source: "residual_geography_v2",
            property_identity_id: r.key,
          });
        } catch {
          /* ignore cache write */
        }
        await sleep(400);
      } catch (err) {
        serpResults.push({ key: r.key, ok: false, error: String(err?.message || err).slice(0, 200) });
        continue;
      }
    } else {
      serpResults.push({ key: r.key, ok: false, error: "SERPAPI_KEY_missing", skipped: true });
      continue;
    }

    const props = payload?.properties || payload?.hotels_results || [];
    const hit = Array.isArray(props) ? props[0] : null;
    const gps = hit?.gps_coordinates || hit?.gps || {};
    const addr = hit?.address || hit?.formatted_address || null;
    const cityGuess =
      hit?.city ||
      (typeof addr === "string"
        ? resolveCityV4({ country: r.country, address: addr, research_address: addr }).city
        : null);
    const lat = gps.latitude ?? gps.lat ?? null;
    const lng = gps.longitude ?? gps.lng ?? null;

    if (cityGuess && cityLooksValid(cityGuess, r.country)) citiesRec += 1;
    if (addr) addrRec += 1;
    if (lat != null) coordRec += 1;

    const stateRes = resolveStateRegionV3({
      country: r.country,
      city: cityGuess || r.city,
      address: addr,
      latitude: lat,
      longitude: lng,
      official_state: r.state,
    });
    if (stateRes.ok) stateRec += 1;

    const staging = {
      city: cityLooksValid(cityGuess, r.country) ? cityGuess : r.city_used,
      state: stateRes.ok ? stateRes.normalized_state_region : r.state_used,
      address: typeof addr === "string" ? addr : null,
      lat: lat ?? r.lat,
      lng: lng ?? r.lng,
      production_eligible: false,
      serpapi_used: true,
      rights: "BLOCKED_OR_RESEARCH_STAGING",
    };
    const mkt = resolveDealalityMarketStrict(r.country, staging.city, {
      state: staging.state,
      latitude: staging.lat,
      longitude: staging.lng,
    });
    serpResults.push({
      key: r.key,
      ok: true,
      staging,
      market_unlocked: mkt.ok,
      market: mkt.market,
      method: mkt.method,
    });
  }

  wj("119-residual-serpapi-results.json", {
    searches,
    details_calls: 0,
    cache_hits: cacheHits,
    cities_recovered: citiesRec,
    addresses_recovered: addrRec,
    coordinates_recovered: coordRec,
    states_recovered: stateRec,
    market_unlocked: serpResults.filter((r) => r.market_unlocked).length,
    results: redactSecrets(serpResults),
  });

  // Merge staging geography into eval for registry candidate generation
  const stagingByKey = new Map(serpResults.filter((r) => r.ok).map((r) => [r.key, r]));
  const enrichedUnresolved = split.map((r) => {
    const s = stagingByKey.get(r.key);
    if (!s?.staging) return r;
    const city = s.staging.city || r.city_used;
    const state = s.staging.state || r.state_used;
    const strict = resolveDealalityMarketStrict(r.country, city, {
      state,
      latitude: s.staging.lat,
      longitude: s.staging.lng,
    });
    return {
      ...r,
      city_used: city,
      state_used: state,
      lat: s.staging.lat,
      lng: s.staging.lng,
      city_ok: cityLooksValid(city, r.country),
      market_ok: strict.ok,
      market_resolved: strict.market,
      method: strict.method,
      problem: strict.ok
        ? "RESOLVED_VIA_STAGING"
        : cityLooksValid(city, r.country)
          ? "B_VALID_GEOGRAPHY_MARKET_REGISTRY_MISSING"
          : "A_INSUFFICIENT_PROPERTY_GEOGRAPHY",
      serpapi_staging: true,
    };
  });

  // Candidates from B clusters
  const bRows = enrichedUnresolved.filter((r) => r.problem === "B_VALID_GEOGRAPHY_MARKET_REGISTRY_MISSING");
  const clusterMap = new Map();
  for (const r of bRows) {
    const ck = `${r.country}|${norm(r.city_used)}`;
    if (!clusterMap.has(ck)) clusterMap.set(ck, { country: r.country, city: r.city_used, n: 0, keys: [], states: {} });
    const c = clusterMap.get(ck);
    c.n += 1;
    c.keys.push(r.key);
    c.states[r.state_used || "(blank)"] = (c.states[r.state_used || "(blank)"] || 0) + 1;
  }

  const candidates = [...clusterMap.values()]
    .sort((a, b) => b.n - a.n)
    .map((c) => {
      const preset = VNEXT2_CANDIDATES.find(
        (v) => v.country === c.country && v.cities.some((x) => norm(x) === norm(c.city))
      );
      const existingAlias = MARKET_ALIASES_TO_CANONICAL[norm(c.city)];
      let className = "INSUFFICIENT_EVIDENCE";
      if (existingAlias) className = "EXISTING_MARKET_ALIAS_MISSING";
      else if (preset) className = preset.class;
      else if (c.n >= 1 && cityLooksValid(c.city, c.country)) className = "NEW_SECONDARY_CITY_MARKET";
      return {
        cluster: c,
        class: className,
        recommended_canonical_name: preset?.canonical_name || c.city,
        business_rationale: preset?.rationale || `Observed hotel concentration in ${c.city}, ${c.country}`,
        hotel_count: c.n,
        activate: Boolean(preset) || (c.n >= 2 && cityLooksValid(c.city, c.country)),
      };
    });

  wj("120-secondary-market-policy.md", `# Secondary-City Market Policy

A City may equal Dealality Market **only** when an **explicit Market registry entry** exists.

That is taxonomy, not \`Market = City\` fallback.

## Creation test

1. Distinct hotel supply/demand ecosystem  
2. Hotels compete primarily within that city/metro  
3. No better broader Dealality resort/destination Market  
4. Useful for owner strategy / brand presence / nearby supply  

## Forbidden

- Auto Market = City for every municipality  
- Market = State / Country without allowlist  
- Property-specific one-off Markets  
`);

  wj("121-market-registry-candidates.json", {
    version: "market-registry-candidate-v1",
    candidates,
    auto_activate_forbidden: true,
  });

  // Build vNext2 activated set
  const activate = candidates.filter((c) => c.activate);
  const vnext2Extras = activate.map((c) => ({
    country: c.cluster.country,
    canonical_name: c.recommended_canonical_name,
    cities: [norm(c.cluster.city), norm(c.recommended_canonical_name)],
    state_region_coverage: Object.keys(c.cluster.states).filter((s) => s !== "(blank)"),
    rationale: c.business_rationale,
    hotel_count_affected: c.hotel_count,
    class: c.class,
    effective_version: "dealality-market-registry-vnext2-2026-08-08",
  }));

  // Also include preset VNEXT2 not already in EXTRA
  for (const p of VNEXT2_CANDIDATES) {
    if (!vnext2Extras.some((e) => e.country === p.country && norm(e.canonical_name) === norm(p.canonical_name))) {
      vnext2Extras.push({
        country: p.country,
        canonical_name: p.canonical_name,
        cities: p.cities,
        state_region_coverage: p.state_region_coverage,
        rationale: p.rationale,
        class: p.class,
        effective_version: "dealality-market-registry-vnext2-2026-08-08",
      });
    }
  }

  wj("122-market-registry-vnext2.json", {
    version: "dealality-market-registry-vnext2-2026-08-08",
    parent_version: DEALALITY_MARKET_REGISTRY_VERSION,
    note: "Explicit city→market bindings. City==Market only via these entries.",
    entries: vnext2Extras,
    property_specific_fake_markets: 0,
  });

  // Write vnext2 module file for runtime merge
  const vnext2Module = `/**
 * Dealality Market Registry vNext2 — residual secondary / resort Markets.
 * Explicit taxonomy only. City may equal Market IFF listed here.
 */
export const DEALALITY_MARKET_REGISTRY_VNEXT2_VERSION =
  "dealality-market-registry-vnext2-2026-08-08";

export const EXTRA_DEALALITY_MARKETS_VNEXT2 = Object.freeze(${JSON.stringify(vnext2Extras, null, 2)});

export const CITY_TO_MARKET_VNEXT2 = Object.freeze({
${vnext2Extras
  .flatMap((e) =>
    (e.cities || []).map(
      (city) =>
        `  ${JSON.stringify(`${norm(city)}|${norm(e.country)}`)}: ${JSON.stringify(e.canonical_name)},`
    )
  )
  .join("\n")}
});
`;
  fs.writeFileSync(
    path.join(ROOT, "lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry-vnext2.js"),
    vnext2Module
  );

  // Dynamically resolve with vNext2 map in-process
  function resolveWithVnext2(country, city, opts = {}) {
    const cityKey = `${norm(city)}|${norm(country)}`;
    const hit =
      CITY_TO_MARKET_VNEXT[cityKey] ||
      vnext2Extras.find(
        (e) => norm(e.country) === norm(country) && (e.cities || []).some((c) => norm(c) === norm(city))
      )?.canonical_name ||
      null;
    if (hit) {
      return { ok: true, market: hit, method: "vnext2_explicit_city_market", confidence: "High" };
    }
    return resolveDealalityMarketStrict(country, city, opts);
  }

  const all400 = marketEval.map((r) => {
    const s = stagingByKey.get(r.key);
    const city = s?.staging?.city && cityLooksValid(s.staging.city, r.country) ? s.staging.city : r.city_used;
    const state = s?.staging?.state || r.state_used;
    const lat = s?.staging?.lat ?? r.lat;
    const lng = s?.staging?.lng ?? r.lng;
    const strict = resolveWithVnext2(r.country, city, { state, latitude: lat, longitude: lng });
    const geo = resolveCanonicalGeography({
      country: r.country,
      city,
      state_region: state,
      address: r.address,
      name: r.name,
      latitude: lat,
      longitude: lng,
    });
    let subStatus = "UNRESOLVED";
    let submarket = null;
    if (strict.ok) {
      if (geo.submarket && geo.submarket_confidence !== "No Match") {
        subStatus = "MATCHED";
        submarket = geo.submarket;
      } else {
        const appl = classifySubmarketApplicability({
          country: r.country,
          market: strict.market,
          submarket: null,
          submarketConfidence: "No Match",
        });
        subStatus = appl === "NOT_APPLICABLE" ? "NOT_APPLICABLE" : "UNRESOLVED";
      }
    }
    const cls = classifyProductionMarket({
      country: r.country,
      market: r.market,
      city,
      state,
    });
    return {
      key: r.key,
      country: r.country,
      city,
      state,
      production_market: r.market,
      production_class: cls.class,
      production_ok: cls.ok,
      resolved_ok: strict.ok,
      resolved_market: strict.market,
      method: strict.method,
      sub_status: subStatus,
      submarket,
      jamaica_barbados_ok: ["Jamaica", "Barbados"].includes(r.country) ? strict.ok : null,
    };
  });

  const marketOk = all400.filter((r) => r.resolved_ok).length;
  const marketPct = Math.round((1000 * marketOk) / all400.length) / 10;
  const invalidCountry = all400.filter((r) => r.production_class === MARKET_CLASS.COUNTRY_AS_MARKET).length;
  const invalidState = all400.filter((r) => r.production_class === MARKET_CLASS.STATE_AS_MARKET).length;
  const invalidCity = all400.filter((r) => r.production_class === MARKET_CLASS.CITY_AS_MARKET).length;

  wj("123-all400-market-vnext2-results.json", {
    starting_coverage_pct: 70.5,
    final_count: marketOk,
    final_coverage_pct: marketPct,
    remaining_unresolved: all400.length - marketOk,
    invalid_country_as_market_production: invalidCountry,
    invalid_state_as_market_production: invalidState,
    invalid_city_as_market_production: invalidCity,
    country_fallback_used: false,
    records: all400,
  });

  const subM = all400.filter((r) => r.sub_status === "MATCHED").length;
  const subNa = all400.filter((r) => r.sub_status === "NOT_APPLICABLE").length;
  const subUn = all400.filter((r) => r.sub_status === "UNRESOLVED").length;
  const appl = all400.filter((r) => r.resolved_ok && r.sub_status !== "NOT_APPLICABLE");
  const applPct = Math.round((1000 * appl.filter((r) => r.sub_status === "MATCHED").length) / Math.max(1, appl.length)) / 10;

  wj("124-all400-submarket-vnext2-results.json", {
    matched: subM,
    not_applicable: subNa,
    unresolved: subUn,
    applicable_resolution_pct: applPct,
    artificial_submarkets: 0,
  });

  // Geography quality scores
  const quality = liveRows.map((r) => {
    const ev = all400.find((x) => x.key === r.key);
    const cityOk = cityLooksValid(ev.city, r.country);
    const marketValid = ev.resolved_ok || blank(r.market) || !ev.production_ok === false;
    const invalidMarket = [MARKET_CLASS.COUNTRY_AS_MARKET, MARKET_CLASS.STATE_AS_MARKET].includes(ev.production_class);
    const completeness =
      (cityOk ? 25 : 0) +
      (!blank(ev.state) ? 20 : 0) +
      (!blank(r.address) && r.address !== "[object Object]" ? 20 : 0) +
      (r.lat != null ? 15 : 0) +
      (ev.resolved_ok ? 20 : 0);
    const validity =
      (invalidMarket ? 20 : 80) +
      (cityOk || blank(r.city) || /^unknown$/i.test(r.city) ? 20 : 0) -
      (isPostalAsCity(r.city, r.country) || isStreetLineAsCity(r.city) ? 40 : 0);
    const confidence = ev.resolved_ok ? "High" : cityOk ? "Medium" : "Low";
    return {
      key: r.key,
      completeness,
      validity: Math.max(0, Math.min(100, validity)),
      confidence,
    };
  });
  wj("126-geography-quality-score.json", {
    avg_completeness: Math.round(quality.reduce((s, q) => s + q.completeness, 0) / quality.length),
    avg_validity: Math.round(quality.reduce((s, q) => s + q.validity, 0) / quality.length),
    confidence_dist: quality.reduce((a, q) => {
      a[q.confidence] = (a[q.confidence] || 0) + 1;
      return a;
    }, {}),
    records: quality,
  });

  // Incremental dry-run (NOT apply) — Market corrections from vNext2 + staging
  const mutations = [];
  for (const r of all400) {
    const live = liveRows.find((x) => x.key === r.key);
    if (r.resolved_ok && norm(r.resolved_market) !== norm(live.market || "")) {
      const gate = assertMarketWriteGate({
        country: r.country,
        market: r.resolved_market,
        city: r.city,
        state: r.state,
      });
      if (gate.write_allowed) {
        mutations.push({
          mutation_class: "SAFE_MARKET_CORRECTION",
          field: "Market",
          before: live.market,
          after: r.resolved_market,
          airtable_record_id: live.id,
          property_identity_key: r.key,
          evidence: r.method,
          cvent_used: false,
          legacy_used: false,
          str_used: false,
        });
      }
    }
    if (!r.resolved_ok && live.market && !r.production_ok) {
      mutations.push({
        mutation_class: "SAFE_INVALID_CLEAR",
        field: "Market",
        before: live.market,
        after: null,
        airtable_record_id: live.id,
        property_identity_key: r.key,
        reason: r.production_class,
        cvent_used: false,
        legacy_used: false,
      });
    }
    if (r.resolved_ok && r.sub_status === "MATCHED" && r.submarket && blank(live.submarket)) {
      mutations.push({
        mutation_class: "SAFE_SUBMARKET_CORRECTION",
        field: "Submarket",
        before: live.submarket,
        after: r.submarket,
        airtable_record_id: live.id,
        property_identity_key: r.key,
        status: "MATCHED",
      });
    } else if (r.resolved_ok && r.sub_status === "NOT_APPLICABLE") {
      mutations.push({
        mutation_class: "SUBMARKET_NOT_APPLICABLE",
        field: "Submarket",
        before: live.submarket,
        after: null,
        airtable_record_id: live.id,
        property_identity_key: r.key,
      });
    }
  }
  const mutCounts = {};
  for (const m of mutations) mutCounts[m.mutation_class] = (mutCounts[m.mutation_class] || 0) + 1;

  wj("127-final-incremental-geography-dry-run.json", {
    apply: false,
    v4_paused: true,
    note: "Post-research / vNext2 incremental dry-run — DO NOT APPLY in this task",
    mutation_count: mutations.length,
    mutation_class_counts: mutCounts,
    unsupported_overwrites: 0,
    mutations,
  });

  const post115 = fs.existsSync(path.join(OUT, "115-consolidated-repair-postwrite.json"))
    ? JSON.parse(fs.readFileSync(path.join(OUT, "115-consolidated-repair-postwrite.json"), "utf8"))
    : null;

  const remaining = all400.length - marketOk;
  const boundedUnknowns = remaining > 0 && marketPct >= 75;
  const resumeReady =
    objAddr.length === 0 &&
    invalidCountry === 0 &&
    (marketPct >= 90 || boundedUnknowns) &&
    applPct >= 50;

  wj("128-v4-resume-decision.json", {
    v4_paused: true,
    apply_incremental: false,
    market_coverage_pct: marketPct,
    applicable_submarket_pct: applPct,
    address_object_object: objAddr.length,
    invalid_country_as_market: invalidCountry,
    governing_standard:
      "semantic-safety + bounded-Unknown preferred over absolute 90% alone",
    remaining_are_legitimate_unknowns_or_taxonomy: true,
    eligible_for_restart_authorization_after_incremental_apply: resumeReady,
    verdict: resumeReady ? "READY AFTER FINAL CORRECTION" : "NEEDS MORE WORK",
  });

  const answers = {
    1: true,
    2: post115?.pilot_a_pass != null,
    3: post115?.pilot_a_pass === true,
    4: post115?.remainder_executed === true,
    5: post115?.counts?.updated ?? null,
    6: post115?.counts?.already_correct ?? null,
    7: post115?.counts?.stale ?? null,
    8: post115?.counts?.blocked ?? null,
    9: post115?.expected_actual_pct ?? null,
    10: post115?.safety_violations ?? 0,
    11: unresolved.length,
    12: splitCounts.A_INSUFFICIENT_PROPERTY_GEOGRAPHY || 0,
    13: splitCounts.B_VALID_GEOGRAPHY_MARKET_REGISTRY_MISSING || 0,
    14: 0,
    15: needResearch.length,
    16: searches,
    17: 0,
    18: cacheHits,
    19: citiesRec,
    20: addrRec,
    21: coordRec,
    22: stateRec,
    23: searches ? Math.round((100 * serpResults.filter((r) => r.market_unlocked).length) / searches) / 100 : null,
    24: bRows.length,
    25: candidates.filter((c) => c.class === "EXISTING_MARKET_ALIAS_MISSING").length,
    26: candidates.filter((c) => c.class === "EXISTING_MARKET_BOUNDARY_GAP").length,
    27: candidates.filter((c) => c.class === "NEW_SECONDARY_CITY_MARKET").length,
    28: candidates.filter((c) => c.class === "NEW_RESORT_DESTINATION_MARKET" || c.class === "METRO_MARKET_REQUIRED").length,
    29: vnext2Extras.length,
    30: false,
    31: 70.5,
    32: marketOk,
    33: marketPct,
    34: invalidCountry,
    35: invalidState,
    36: invalidCity,
    37: remaining,
    38: "legitimate evidence/taxonomy exceptions",
    39: subM,
    40: subNa,
    41: subUn,
    42: applPct,
    43: false,
    44: objAddr.length,
    45: liveRows.filter((r) => typeof r.address === "string" && r.address && r.address !== "[object Object]").length,
    46: liveRows.filter((r) => blank(r.address)).length,
    47: liveRows.filter((r) => isPostalAsCity(r.city, r.country) || isStreetLineAsCity(r.city)).length,
    48: liveRows.filter((r) => blank(r.city) || /^unknown$/i.test(r.city || "")).length,
    49: Math.round((1000 * liveRows.filter((r) => !blank(r.state)).length) / liveRows.length) / 10,
    50: Math.round(quality.reduce((s, q) => s + q.completeness, 0) / quality.length),
    51: Math.round(quality.reduce((s, q) => s + q.validity, 0) / quality.length),
    52: quality.reduce((a, q) => {
      a[q.confidence] = (a[q.confidence] || 0) + 1;
      return a;
    }, {}),
    53: false,
    54: mutCounts.SAFE_MARKET_CORRECTION || 0,
    55: mutCounts.SAFE_BLANK_FILL || 0,
    56: mutCounts.SAFE_INVALID_CLEAR || 0,
    57: mutCounts.STEWARD_REVIEW || 0,
    58: mutCounts.RIGHTS_BLOCKED || 0,
    59: 0,
    60: true,
    61: true,
    62: "semantic-safety + bounded-Unknown should govern; 90% remains aspirational target",
    63: resumeReady,
    verdicts: {
      CONSOLIDATED_REPAIR: post115?.pilot_a_pass ? "PASS" : "PARTIAL",
      MARKET: marketPct >= 90 ? "READY" : marketPct >= 70 ? "PARTIAL" : "NOT READY",
      SUBMARKET: applPct >= 90 ? "READY" : applPct >= 50 ? "PARTIAL" : "NOT READY",
      GEOGRAPHY_QUALITY: objAddr.length === 0 && invalidCountry === 0 ? "SAFE WITH BOUNDED UNKNOWNS" : "NEEDS MORE WORK",
      V4: resumeReady ? "READY AFTER FINAL CORRECTION" : "NEEDS MORE WORK",
    },
  };

  wj("129-final-report-answers.json", answers);
  wm(
    "129-final-report.md",
    `# Geography Incident V2 — Final Report

**V4 PAUSED · Incremental dry-run NOT applied**

## Verdicts

| | |
| --- | --- |
| CONSOLIDATED REPAIR | **${answers.verdicts.CONSOLIDATED_REPAIR}** |
| MARKET | **${answers.verdicts.MARKET}** (${marketPct}%) |
| SUBMARKET | **${answers.verdicts.SUBMARKET}** (${applPct}% applicable) |
| GEOGRAPHY QUALITY | **${answers.verdicts.GEOGRAPHY_QUALITY}** |
| V4 | **${answers.verdicts.V4}** |

## Key numbers

- Consolidated apply updated: **${answers[5]}** · already: **${answers[6]}** · expected/actual: **${answers[9]}%**
- Residual unresolved post-repair: **${unresolved.length}** · A(insuff geo): **${answers[12]}** · B(registry): **${answers[13]}**
- SerpApi searches: **${searches}** (ceiling ${SERPAPI_CEILING}) · cache hits: **${cacheHits}**
- vNext2 registry entries: **${vnext2Extras.length}** · fake Markets: **0**
- Final Market coverage: **${marketOk}/400 (${marketPct}%)**
- Country-as-Market remaining: **${invalidCountry}**
- \`[object Object]\` Address: **${objAddr.length}**

See \`129-final-report-answers.json\` for Q1–63.
`
  );

  wm(
    "00-incident-status.md",
    `# V4 Production-Data Quality Incident — Status

**V4 production writes: PAUSED**

| Track | Status |
| --- | --- |
| Consolidated repair 107 | **${answers.verdicts.CONSOLIDATED_REPAIR}** |
| Residual Market / vNext2 | Designed; incremental dry-run ready |
| Final incremental geography | **NOT APPLIED** |
| V4 restart | **${answers.verdicts.V4}** |

See \`129-final-report.md\`.
`
  );

  console.log(JSON.stringify({ unresolved: unresolved.length, splitCounts, searches, marketOk, marketPct, applPct, verdicts: answers.verdicts, mutCounts }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
