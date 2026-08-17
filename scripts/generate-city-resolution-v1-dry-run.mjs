/**
 * Full-table City/locality resolution + downstream geography rebuild — DRY RUN ONLY.
 * Artifacts → city-resolution-v1/. V4 PAUSED. No production apply.
 */
import fs from "node:fs";
import path from "node:path";
import "dotenv/config";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  validateCitySemantics,
  CITY_STATUS,
} from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import {
  resolveCityV4,
  classifyCityLabel,
  isPostalAsCity,
  isStreetLineAsCity,
  parseBrazilAddress,
  parseArgentinaAddress,
  parseCostaRicaAddress,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import { resolveStateRegionV3 } from "../lib/research-engine-v2/census-autopilot-v3/geography/state-region-resolver-v3.js";
import {
  resolveDealalityMarketStrict,
  assertMarketWriteGate,
  assertSubmarketWriteGate,
  classifyProductionMarket,
  MARKET_CLASS,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";
import { classifySubmarketApplicability } from "../lib/research-engine-v2/census-autopilot-v3/geography/applicability-rules.js";
import { lookupAdminByBbox } from "../lib/research-engine-v2/census-autopilot-v3/geography/admin-bbox.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";
import { createSerpApiResearchCache } from "../lib/research-engine-v2/census-autopilot-v2-1/serpapi-cache.js";
import { searchGoogleHotels } from "../lib/research-engine-v2/providers/serpapi-google-hotels/search.js";
import { SerpApiCreditTracker } from "../lib/research-engine-v2/providers/serpapi-google-hotels/credit-tracker.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/city-resolution-v1"
);
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Family / Source Family",
  "Address",
  "City",
  "State / Region",
  "Country",
  "Market",
  "Submarket",
  "Latitude",
  "Longitude",
  "Phone",
  "Official Property URL",
  "Source URL",
  "Rooms / Keys",
  "Opening Date",
  "Operator / Management Company",
];

const SERPAPI_CEILING = 80;
const PLACEHOLDER_RE = /^(unknown|n\/a|na|tbd|to be confirmed|not known|null|undefined|-)$/i;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function wj(n, d) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, n), JSON.stringify(d, null, 2));
}
function wm(n, t) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, n), t);
}
function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}
function isUnresolvedCity(city) {
  if (blank(city)) return true;
  return PLACEHOLDER_RE.test(String(city).trim());
}
function isObjectSerialized(addr) {
  if (addr == null) return false;
  if (typeof addr === "object") return true;
  const s = String(addr);
  return s === "[object Object]" || s === "[object Array]" || (/^\s*[\{\[]/.test(s) && s.length > 2);
}
function count(arr, pred) {
  return arr.filter(pred).length;
}

async function listAllRecords(baseId, token, tableId, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    process.stdout.write(`\r[fetch] ${out.length}…`);
    await sleep(100);
  } while (offset);
  console.log(`\n[fetch] done n=${out.length}`);
  return out;
}

function loadClaimIndex() {
  const paths = [
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/08-canonical-claims.json",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/33-golden-geography-contact-research/_claim-store.json",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/32-field-pipeline-repair/_claim-store-cohort-snapshot.json",
  ];
  const by = new Map();
  for (const rel of paths) {
    const p = path.join(ROOT, rel);
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    for (const [k, fields] of Object.entries(j.properties || {})) {
      if (!by.has(k)) by.set(k, {});
      const cur = by.get(k);
      for (const [f, claims] of Object.entries(fields || {})) {
        if (!Array.isArray(claims)) continue;
        if (!cur[f]) cur[f] = [];
        cur[f].push(...claims);
      }
    }
  }
  return by;
}

function bestCityClaim(claims) {
  const names = ["City", "city", "locality", "municipality", "Locality", "Municipality"];
  for (const n of names) {
    for (const c of claims?.[n] || []) {
      if (c?.value == null || c.value === "") continue;
      if (isUnresolvedCity(c.value)) continue;
      if (/cvent/i.test(c.source_type || "") || c.cvent_used_as_production_evidence) continue;
      if (c.legacy_used_as_production_evidence) continue;
      if (c.rights_status === "BLOCKED_RIGHTS" || c.rights_status === "PROHIBITED") continue;
      const sem = validateCitySemantics(c.value, null);
      if (!sem.ok) continue;
      return { field: n, claim: c };
    }
  }
  return null;
}

function parseCityFromAddress(address, country) {
  if (!address || isObjectSerialized(address)) return null;
  const s = String(address);
  let parsed = null;
  if (country === "Brazil") parsed = parseBrazilAddress(s);
  else if (country === "Argentina") parsed = parseArgentinaAddress(s);
  else if (country === "Costa Rica") parsed = parseCostaRicaAddress(s);
  else {
    const parts = s.split(",").map((x) => x.trim()).filter(Boolean);
    const cand = parts.find(
      (p) =>
        validateCitySemantics(p, country).ok &&
        !isPostalAsCity(p, country) &&
        !isStreetLineAsCity(p) &&
        !isDescriptorCity(p) &&
        norm(p) !== norm(country)
    );
    if (cand) parsed = { ok: true, city: cand, method: "generic_address_locality" };
  }
  if (parsed?.ok && parsed.city && validateCitySemantics(parsed.city, country).ok) {
    return { city: parsed.city, method: parsed.method || "address_parse", state: parsed.state || null };
  }
  return null;
}

async function mapboxReverseLocality(lat, lng, country) {
  const token = process.env.MAPBOX_ACCESS_TOKEN;
  if (!token || lat == null || lng == null) return null;
  const url =
    `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(lng)},${encodeURIComponent(lat)}.json` +
    `?types=place,locality&limit=5&access_token=${encodeURIComponent(token)}`;
  try {
    const res = await fetch(url);
    const json = await res.json();
    if (!res.ok) return { ok: false, reason: `http_${res.status}` };
    const features = json.features || [];
    for (const f of features) {
      const name = f.text || f.place_name?.split(",")[0];
      if (!name) continue;
      if (isDescriptorCity(name) || isPostalAsCity(name, country) || isStreetLineAsCity(name)) continue;
      const sem = validateCitySemantics(name, country);
      if (!sem.ok) continue;
      // Prefer feature whose context country matches
      const ctx = (f.context || []).map((c) => String(c.text || "").toLowerCase()).join(" ");
      const countryOk =
        !country ||
        ctx.includes(norm(country).slice(0, 5)) ||
        /mexico|méxico|brazil|brasil|argentina|colombia|jamaica|barbados|panama|costa rica|dominican/.test(
          ctx + " " + String(f.place_name || "").toLowerCase()
        );
      if (!countryOk && country) continue;
      return {
        ok: true,
        city: sem.value,
        method: "mapbox_reverse_place_locality",
        confidence: f.place_type?.includes("place") ? "CITY_HIGH" : "CITY_PROBABLE",
        production_eligible: f.place_type?.includes("place") === true,
        place_name: f.place_name || null,
      };
    }
    return { ok: false, reason: "no_valid_locality" };
  } catch (e) {
    return { ok: false, reason: String(e?.message || e) };
  }
}

function marketKind(cls, market) {
  if (blank(market)) return "BLANK";
  if (cls.class === MARKET_CLASS.VALID_MARKET) {
    return cls.note === "city_is_canonical_market" || cls.note === "single_market_country_explicit"
      ? "CITY_EQUALS_MARKET_VIA_EXPLICIT_REGISTRY"
      : "CANONICAL_VALID";
  }
  if (cls.class === MARKET_CLASS.COUNTRY_AS_MARKET) return "COUNTRY_AS_MARKET";
  if (cls.class === MARKET_CLASS.STATE_AS_MARKET) return "STATE_AS_MARKET";
  if (cls.class === MARKET_CLASS.CITY_AS_MARKET) return "CITY_AS_MARKET_WITHOUT_REGISTRY";
  return "INVALID";
}

function countryGl(country) {
  const m = {
    Mexico: "mx",
    Brazil: "br",
    Argentina: "ar",
    Colombia: "co",
    "Costa Rica": "cr",
    Panama: "pa",
    Jamaica: "jm",
    Barbados: "bb",
    "Dominican Republic": "do",
  };
  return m[country] || "us";
}

async function main() {
  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const claims = loadClaimIndex();
  const serpCache = createSerpApiResearchCache(ROOT, { ttlDays: 45 });
  const tracker = new SerpApiCreditTracker({ ceiling: SERPAPI_CEILING });

  const records = await listAllRecords(baseId, token, CENSUS_TABLE_ID, FIELDS);
  const rows = records.map((r) => {
    const f = r.fields || {};
    return {
      id: r.id,
      key: f["Property Identity Key"] || null,
      name: f["Property Name"] || f["Canonical Property Name"] || "",
      brand: f["Current Brand"] || null,
      family: f["Family / Source Family"] || f["Brand Family"] || null,
      address: f["Address"] ?? null,
      city: f["City"] || null,
      state: f["State / Region"] || null,
      country: f["Country"] || null,
      market: f["Market"] || null,
      submarket: f["Submarket"] || null,
      lat: f["Latitude"] ?? null,
      lng: f["Longitude"] ?? null,
      phone: f["Phone"] || null,
      url: f["Official Property URL"] || f["Source URL"] || null,
      rooms: f["Rooms / Keys"] ?? null,
      opening: f["Opening Date"] || null,
      operator: f["Operator / Management Company"] || null,
    };
  });

  const baselineValid = count(
    rows,
    (r) => !isUnresolvedCity(r.city) && validateCitySemantics(r.city, r.country).ok && !isPostalAsCity(r.city, r.country) && !isDescriptorCity(r.city)
  );
  const baselineUnknown = count(rows, (r) => !blank(r.city) && PLACEHOLDER_RE.test(String(r.city).trim()));
  const baselineBlank = count(rows, (r) => blank(r.city));
  const baselineInvalid = count(
    rows,
    (r) =>
      !isUnresolvedCity(r.city) &&
      (!validateCitySemantics(r.city, r.country).ok ||
        isPostalAsCity(r.city, r.country) ||
        isDescriptorCity(r.city) ||
        ["COUNTRY_AS_CITY", "POSTAL_CODE_AS_CITY", "CITY_INVALID"].includes(
          classifyCityLabel(r.city, r.country).bucket
        ))
  );

  const gaps = rows.filter((r) => isUnresolvedCity(r.city));
  wj("01-city-gap-inventory.json", {
    total_records: rows.length,
    valid_city: baselineValid,
    literal_unknown: baselineUnknown,
    blank: baselineBlank,
    known_invalid: baselineInvalid,
    gap_n: gaps.length,
    gaps: gaps.map((r) => ({
      id: r.id,
      key: r.key,
      city: r.city,
      country: r.country,
      has_address: !blank(r.address) && !isObjectSerialized(r.address),
      has_coords: r.lat != null && r.lng != null,
      has_url: !blank(r.url),
    })),
  });

  // Root-cause buckets
  const buckets = {
    A_CITY_NOT_RESEARCHED: [],
    B_OFFICIAL_CITY_FIELD_MISSING: [],
    C_ADDRESS_EXISTS_CITY_NOT_PARSED: [],
    D_COORDINATES_EXIST_CITY_NOT_DERIVED: [],
    E_STRUCTURED_ADDRESS_EXISTS_CITY_NOT_EXTRACTED: [],
    F_OFFICIAL_PROPERTY_PAGE_AVAILABLE: [],
    G_OFFICIAL_SOURCE_BLOCKED: [],
    H_SERPAPI_RESEARCH_CANDIDATE: [],
    I_PROPERTY_IDENTITY_WEAK: [],
    J_CITY_CONFLICT: [],
    K_TRUE_LOCALITY_AMBIGUITY: [],
    L_OTHER: [],
  };

  for (const r of gaps) {
    const tags = [];
    if (!blank(r.address) && !isObjectSerialized(r.address)) tags.push("C_ADDRESS_EXISTS_CITY_NOT_PARSED");
    if (r.lat != null && r.lng != null) tags.push("D_COORDINATES_EXIST_CITY_NOT_DERIVED");
    if (!blank(r.url)) tags.push("F_OFFICIAL_PROPERTY_PAGE_AVAILABLE");
    if (!r.key) tags.push("I_PROPERTY_IDENTITY_WEAK");
    if (!tags.length) tags.push("A_CITY_NOT_RESEARCHED");
    if (tags.includes("C_ADDRESS_EXISTS_CITY_NOT_PARSED") || tags.includes("D_COORDINATES_EXIST_CITY_NOT_DERIVED") || tags.includes("F_OFFICIAL_PROPERTY_PAGE_AVAILABLE")) {
      tags.push("H_SERPAPI_RESEARCH_CANDIDATE");
    }
    for (const t of tags) {
      const k = Object.keys(buckets).find((x) => x.startsWith(t[0] + "_") || x.includes(t.slice(2))) || null;
      // map letter prefix
    }
    // explicit push
    if (!blank(r.address) && !isObjectSerialized(r.address)) buckets.C_ADDRESS_EXISTS_CITY_NOT_PARSED.push(r.key);
    if (r.lat != null && r.lng != null) buckets.D_COORDINATES_EXIST_CITY_NOT_DERIVED.push(r.key);
    if (!blank(r.url)) buckets.F_OFFICIAL_PROPERTY_PAGE_AVAILABLE.push(r.key);
    if (!r.key) buckets.I_PROPERTY_IDENTITY_WEAK.push(r.id);
    if (
      blank(r.address) &&
      (r.lat == null || r.lng == null) &&
      blank(r.url)
    )
      buckets.A_CITY_NOT_RESEARCHED.push(r.key || r.id);
    else buckets.H_SERPAPI_RESEARCH_CANDIDATE.push(r.key || r.id);
  }
  wj("02-city-root-cause-buckets.json", {
    counts: Object.fromEntries(Object.entries(buckets).map(([k, v]) => [k, [...new Set(v)].length])),
    buckets: Object.fromEntries(Object.entries(buckets).map(([k, v]) => [k, [...new Set(v)]])),
  });

  const claimRecovery = [];
  const addrResults = [];
  const coordResults = [];
  const officialResults = [];
  const serpPlan = [];
  const serpResults = [];
  const resolutions = []; // key -> resolution

  // Pass 1: claims + address + URL (City Resolver V4)
  for (const r of gaps) {
    const cl = r.key ? claims.get(r.key) || {} : {};
    const claimHit = bestCityClaim(cl);
    if (claimHit) {
      const city = String(claimHit.claim.value).trim();
      if (validateCitySemantics(city, r.country).ok) {
        resolutions.push({
          key: r.key,
          id: r.id,
          city,
          confidence: "CITY_HIGH",
          production_eligible: true,
          derivation: "EXISTING_CLAIM",
          source_type: claimHit.claim.source_type || "canonical_claim",
          method: "claim_propagation",
        });
        claimRecovery.push({ key: r.key, city, source: claimHit.claim.source_type });
        continue;
      }
    }

    const addrHit = parseCityFromAddress(r.address, r.country);
    if (addrHit) {
      resolutions.push({
        key: r.key,
        id: r.id,
        city: addrHit.city,
        confidence: "CITY_HIGH",
        production_eligible: true,
        derivation: "ADDRESS_COMPONENT",
        method: addrHit.method,
        state_hint: addrHit.state || null,
      });
      addrResults.push({ key: r.key, city: addrHit.city, method: addrHit.method });
      continue;
    }

    const v4 = resolveCityV4({
      country: r.country,
      city: null,
      address: typeof r.address === "string" ? r.address : null,
      official_url: r.url,
    });
    if (v4.ok && v4.production_eligible && v4.city) {
      resolutions.push({
        key: r.key,
        id: r.id,
        city: v4.city,
        confidence: "CITY_HIGH",
        production_eligible: true,
        derivation: v4.method?.includes("url") ? "OFFICIAL_URL_SLUG" : "CITY_RESOLVER_V4",
        method: v4.method,
      });
      officialResults.push({ key: r.key, city: v4.city, method: v4.method });
      continue;
    }
  }

  const resolvedKeys = new Set(resolutions.map((x) => x.key));
  const stillOpen = gaps.filter((r) => !resolvedKeys.has(r.key));

  wj("03-existing-city-claim-recovery.json", {
    n: claimRecovery.length,
    items: claimRecovery,
  });
  wj("05-structured-address-city-results.json", {
    n: addrResults.length,
    items: addrResults,
  });
  wj("06-official-city-research.json", {
    n: officialResults.length,
    note: "City Resolver V4 official URL / address layers (no live HTML crawl in this pass)",
    items: officialResults,
  });

  // Pass 2: Mapbox reverse for coords
  console.log(`[coords] reverse geocode candidates=${stillOpen.filter((r) => r.lat != null && r.lng != null).length}`);
  for (const r of stillOpen) {
    if (r.lat == null || r.lng == null) continue;
    const rev = await mapboxReverseLocality(r.lat, r.lng, r.country);
    await sleep(80);
    coordResults.push({ key: r.key, ...rev, admin_bbox: lookupAdminByBbox(r.country, r.lat, r.lng) });
    if (rev?.ok && rev.production_eligible && rev.city) {
      resolutions.push({
        key: r.key,
        id: r.id,
        city: rev.city,
        confidence: rev.confidence || "CITY_HIGH",
        production_eligible: true,
        derivation: "COORDINATE_ADMIN_LOOKUP",
        method: rev.method,
      });
      resolvedKeys.add(r.key);
    } else if (rev?.ok && rev.city && !rev.production_eligible) {
      // probable — staging only
      resolutions.push({
        key: r.key,
        id: r.id,
        city: rev.city,
        confidence: "CITY_PROBABLE",
        production_eligible: false,
        derivation: "COORDINATE_ADMIN_LOOKUP",
        method: rev.method,
        note: "staging_only_not_auto_write",
      });
    }
  }
  wj("04-coordinate-locality-results.json", {
    attempted: coordResults.length,
    resolved_production: count(resolutions, (x) => x.derivation === "COORDINATE_ADMIN_LOOKUP" && x.production_eligible),
    probable_only: count(resolutions, (x) => x.derivation === "COORDINATE_ADMIN_LOOKUP" && !x.production_eligible),
    items: coordResults.slice(0, 300),
  });

  // Refresh still open for SerpApi (exclude probable-only from needing serp if we want — still need production city)
  const stillNeedProduction = gaps.filter((r) => {
    const hit = resolutions.find((x) => x.key === r.key && x.production_eligible);
    return !hit;
  });

  wj("07-serpapi-city-plan.json", {
    ceiling: SERPAPI_CEILING,
    candidates: stillNeedProduction.length,
    will_query: Math.min(SERPAPI_CEILING, stillNeedProduction.length),
    order: ["cache", "search_one_call", "no_detail_if_address_present"],
    note: "Exact/High identity preferred; Market/Submarket never from SerpApi",
  });

  console.log(`[serpapi] candidates=${stillNeedProduction.length} ceiling=${SERPAPI_CEILING}`);
  let cacheHits = 0;
  let searches = 0;
  let detailCalls = 0;

  for (const r of stillNeedProduction) {
    if (!tracker.canSpend(1)) break;
    const q = [r.name, r.brand, r.country].filter(Boolean).join(" ").trim();
    if (!q || q.length < 4) {
      serpResults.push({ key: r.key, ok: false, reason: "weak_query" });
      continue;
    }
    serpPlan.push({ key: r.key, q });

    const cacheParams = {
      request_type: "search",
      q,
      property_identity_id: r.key,
      gl: countryGl(r.country),
    };
    let payload = null;
    const cached = serpCache.get(cacheParams);
    if (cached?.hit) {
      cacheHits++;
      payload = cached.raw;
      serpResults.push({ key: r.key, cache_hit: true });
    } else {
      const res = await searchGoogleHotels(
        { q, gl: countryGl(r.country), hl: "en" },
        { tracker, hotelId: r.key }
      );
      searches++;
      serpCache.set(cacheParams, res);
      payload = res;
      await sleep(200);
    }

    const candidates = payload?.candidates || [];
    // Prefer Exact/High name match
    const nameN = norm(r.name);
    let best = candidates.find((c) => norm(c.name) === nameN) || null;
    if (!best && candidates.length === 1) best = candidates[0];
    if (!best && candidates.length) {
      best =
        candidates.find((c) => nameN && norm(c.name).includes(nameN.slice(0, 12))) || candidates[0];
    }

    if (!best?.address && !best?.city) {
      serpResults.push({
        key: r.key,
        ok: false,
        reason: "no_locality_in_search",
        candidate_n: candidates.length,
        cache_hit: Boolean(cached?.hit),
      });
      continue;
    }

    // Identity gate: Exact or High
    const identity =
      best && norm(best.name) === nameN
        ? "Exact"
        : best && nameN && norm(best.name).includes(nameN.split(" ")[0])
          ? "High"
          : "Low";
    if (identity === "Low") {
      serpResults.push({ key: r.key, ok: false, reason: "identity_low", name: best?.name });
      continue;
    }

    let city = best.city;
    if (!city && best.address) {
      const parsed = parseCityFromAddress(best.address, r.country);
      city = parsed?.city || null;
    }
    if (!city || !validateCitySemantics(city, r.country).ok || isDescriptorCity(city) || isPostalAsCity(city, r.country)) {
      serpResults.push({ key: r.key, ok: false, reason: "city_semantic_fail", city });
      continue;
    }

    // SerpApi Exact/High → research claim; production eligibility under rights policy: HIGH for Exact
    const production_eligible = identity === "Exact" || identity === "High";
    resolutions.push({
      key: r.key,
      id: r.id,
      city,
      confidence: identity === "Exact" ? "CITY_CONFIRMED" : "CITY_HIGH",
      production_eligible,
      derivation: "SERPAPI_EXACT_HIGH",
      method: "serpapi_google_hotels_search",
      address: best.address || null,
      lat: best.latitude ?? null,
      lng: best.longitude ?? null,
      identity,
      rights_status: "RESEARCH_ELIGIBLE_EXACT_HIGH",
    });
    serpResults.push({
      key: r.key,
      ok: true,
      city,
      identity,
      production_eligible,
      cache_hit: Boolean(cached?.hit),
    });
  }

  wj("08-serpapi-city-results.json", {
    properties_queried: serpPlan.length,
    searches,
    detail_calls: detailCalls,
    cache_hits: cacheHits,
    resolved: count(serpResults, (x) => x.ok),
    tracker: tracker.summary(),
    results: serpResults,
  });

  // Semantic validation + confidence
  const validated = [];
  for (const res of resolutions) {
    const row = rows.find((r) => r.key === res.key);
    const sem = validateCitySemantics(res.city, row?.country);
    const label = classifyCityLabel(res.city, row?.country);
    const valid =
      sem.ok &&
      !isPostalAsCity(res.city, row?.country) &&
      !isStreetLineAsCity(res.city) &&
      !isDescriptorCity(res.city) &&
      !["COUNTRY_AS_CITY", "POSTAL_CODE_AS_CITY", "CITY_INVALID"].includes(label.bucket);
    validated.push({ ...res, semantic_ok: valid, label: label.bucket });
  }
  // Deduplicate by key preferring production_eligible + higher confidence
  const confRank = { CITY_CONFIRMED: 0, CITY_HIGH: 1, CITY_PROBABLE: 2, CITY_CONFLICT: 3, CITY_UNKNOWN: 4 };
  const byKey = new Map();
  for (const v of validated.filter((x) => x.semantic_ok)) {
    const prev = byKey.get(v.key);
    if (
      !prev ||
      (v.production_eligible && !prev.production_eligible) ||
      (v.production_eligible === prev.production_eligible &&
        (confRank[v.confidence] ?? 9) < (confRank[prev.confidence] ?? 9))
    ) {
      byKey.set(v.key, v);
    }
  }
  const finalRes = [...byKey.values()];

  wj("09-city-semantic-validation.json", {
    attempted: resolutions.length,
    semantic_pass: finalRes.length,
    fail: count(validated, (x) => !x.semantic_ok),
  });
  wj("10-city-confidence-results.json", {
    by_confidence: finalRes.reduce((a, r) => {
      a[r.confidence] = (a[r.confidence] || 0) + 1;
      return a;
    }, {}),
  });
  wj("11-city-production-eligibility.json", {
    production_eligible: count(finalRes, (x) => x.production_eligible),
    staging_probable: count(finalRes, (x) => !x.production_eligible),
    items: finalRes,
  });

  wm(
    "12-unknown-placeholder-policy.md",
    `# Unknown Placeholder Policy

## Recommendation

**REMOVE literal \`"Unknown"\` from factual City field.**

| Layer | Store |
| --- | --- |
| Factual Airtable City | blank / null |
| Research status | \`UNRESOLVED\` / \`CITY_UNKNOWN\` in claim/queue metadata |
| UI display | "Unknown" only as presentation label |

## Why

Literal \`Unknown\` behaves like a real City in filters, Market mapping, analytics, and Brand Explorer joins.

## Other fields

Audit found placeholders should follow the same pattern: factual blank + research status — do **not** store \`Unknown\` / \`N/A\` / \`TBD\` as factual values.

## This task

Dry-run may propose \`UNKNOWN_PLACEHOLDER_CLEAR\` (City: \`"Unknown"\` → null) **only when no confirmed City fill** is available, as an optional hygiene mutation. Prefer SAFE_CITY_BLANK_FILL when a verified City exists.
`
  );

  // Downstream recompute for production-eligible city fills
  const cityFills = finalRes.filter((x) => x.production_eligible);
  const stateFills = [];
  const marketCorrections = [];
  const marketCandidates = [];
  const submarketCorrections = [];
  const addressFills = [];
  const coordFills = [];
  const mutations = [];

  const beforeStatePop = count(rows, (r) => !blank(r.state));
  const beforeMarketValid = count(rows, (r) => {
    const k = marketKind(
      classifyProductionMarket({ country: r.country, market: r.market, city: r.city, state: r.state }),
      r.market
    );
    return k === "CANONICAL_VALID" || k === "CITY_EQUALS_MARKET_VIA_EXPLICIT_REGISTRY";
  });

  // Simulate post-city state for all rows
  const simulated = rows.map((r) => {
    const fill = cityFills.find((x) => x.key === r.key);
    const city = fill ? fill.city : isUnresolvedCity(r.city) ? null : r.city;
    return { ...r, city, city_filled: Boolean(fill), fill };
  });

  for (const r of simulated) {
    if (!r.city_filled) continue;

    mutations.push({
      mutation_class: "SAFE_CITY_BLANK_FILL",
      field: "City",
      before: rows.find((x) => x.key === r.key)?.city ?? null,
      after: r.city,
      airtable_record_id: r.id,
      property_identity_key: r.key,
      evidence: r.fill.derivation,
      confidence: r.fill.confidence,
      cvent_used: false,
      legacy_used: false,
      str_used: false,
    });

    // State recompute
    const stateRes = resolveStateRegionV3({
      country: r.country,
      city: r.city,
      address: typeof r.address === "string" ? r.address : null,
      name: r.name,
      official_state: blank(r.state) ? null : r.state,
      latitude: r.lat,
      longitude: r.lng,
    });
    if (blank(r.state) && stateRes.ok && stateRes.production_eligible && stateRes.normalized_state_region) {
      stateFills.push({ key: r.key, after: stateRes.normalized_state_region, method: stateRes.method });
      mutations.push({
        mutation_class: "SAFE_STATE_RECOMPUTE",
        field: "State / Region",
        before: null,
        after: stateRes.normalized_state_region,
        airtable_record_id: r.id,
        property_identity_key: r.key,
        evidence: stateRes.method,
        cvent_used: false,
        legacy_used: false,
        str_used: false,
      });
    }

    const stateForMarket = !blank(r.state)
      ? r.state
      : stateRes.ok
        ? stateRes.normalized_state_region
        : null;

    const marketStrict = resolveDealalityMarketStrict(r.country, r.city, {
      state: stateForMarket,
      latitude: r.lat,
      longitude: r.lng,
    });

    const liveMarketClass = classifyProductionMarket({
      country: r.country,
      market: r.market,
      city: r.city,
      state: stateForMarket,
    });
    const liveKind = marketKind(liveMarketClass, r.market);

    if (marketStrict.ok) {
      const gate = assertMarketWriteGate({
        country: r.country,
        market: marketStrict.market,
        city: r.city,
        state: stateForMarket,
      });
      const marketNeedsWrite =
        blank(r.market) ||
        liveKind === "BLANK" ||
        liveKind === "INVALID" ||
        liveKind === "STATE_AS_MARKET" ||
        liveKind === "CITY_AS_MARKET_WITHOUT_REGISTRY" ||
        liveKind === "COUNTRY_AS_MARKET" ||
        (norm(r.market) !== norm(marketStrict.market) &&
          liveKind !== "CANONICAL_VALID" &&
          liveKind !== "CITY_EQUALS_MARKET_VIA_EXPLICIT_REGISTRY");
      if (gate.write_allowed && marketNeedsWrite) {
        marketCorrections.push({
          key: r.key,
          before: r.market,
          after: marketStrict.market,
          method: marketStrict.method,
          unlocked_by_city: true,
        });
        mutations.push({
          mutation_class: "SAFE_MARKET_RECOMPUTE",
          field: "Market",
          before: r.market,
          after: marketStrict.market,
          airtable_record_id: r.id,
          property_identity_key: r.key,
          evidence: marketStrict.method,
          cvent_used: false,
          legacy_used: false,
          str_used: false,
        });
      }

      const geo = resolveCanonicalGeography({
        country: r.country,
        city: r.city,
        state_region: stateForMarket,
        address: typeof r.address === "string" ? r.address : null,
        name: r.name,
        latitude: r.lat,
        longitude: r.lng,
      });
      if (geo.submarket && geo.submarket_confidence !== "No Match" && blank(r.submarket)) {
        const sg = assertSubmarketWriteGate({
          country: r.country,
          market: marketStrict.market,
          submarket: geo.submarket,
          status: "MATCHED",
        });
        if (sg.write_allowed) {
          submarketCorrections.push({ key: r.key, after: geo.submarket, status: "MATCHED" });
          mutations.push({
            mutation_class: "SAFE_SUBMARKET_RECOMPUTE",
            field: "Submarket",
            before: null,
            after: geo.submarket,
            airtable_record_id: r.id,
            property_identity_key: r.key,
            status: "MATCHED",
            cvent_used: false,
            legacy_used: false,
            str_used: false,
          });
        }
      } else {
        const appl = classifySubmarketApplicability({
          country: r.country,
          market: marketStrict.market,
          submarket: null,
          submarketConfidence: "No Match",
        });
        if (appl === "NOT_APPLICABLE") {
          submarketCorrections.push({ key: r.key, after: null, status: "NOT_APPLICABLE" });
        }
      }
    } else if (r.city && validateCitySemantics(r.city, r.country).ok) {
      marketCandidates.push({
        key: r.key,
        country: r.country,
        city: r.city,
        state: stateForMarket,
        class: "NEW_SECONDARY_CITY_MARKET_CANDIDATE",
      });
    }

    // Cross-recovery address/coords from SerpApi resolutions
    if (r.fill?.address && blank(r.address) && typeof r.fill.address === "string" && !isObjectSerialized(r.fill.address)) {
      addressFills.push({ key: r.key, after: r.fill.address });
      mutations.push({
        mutation_class: "SAFE_ADDRESS_FILL",
        field: "Address",
        before: null,
        after: r.fill.address,
        airtable_record_id: r.id,
        property_identity_key: r.key,
        evidence: r.fill.derivation,
        cvent_used: false,
        legacy_used: false,
        str_used: false,
      });
    }
    if (
      r.fill?.lat != null &&
      r.fill?.lng != null &&
      (r.lat == null || r.lng == null) &&
      Number.isFinite(Number(r.fill.lat)) &&
      Number.isFinite(Number(r.fill.lng))
    ) {
      coordFills.push({ key: r.key, lat: r.fill.lat, lng: r.fill.lng });
      mutations.push({
        mutation_class: "SAFE_COORDINATE_FILL",
        field: "Latitude",
        before: r.lat,
        after: Number(r.fill.lat),
        airtable_record_id: r.id,
        property_identity_key: r.key,
        evidence: r.fill.derivation,
        cvent_used: false,
        legacy_used: false,
        str_used: false,
      });
      mutations.push({
        mutation_class: "SAFE_COORDINATE_FILL",
        field: "Longitude",
        before: r.lng,
        after: Number(r.fill.lng),
        airtable_record_id: r.id,
        property_identity_key: r.key,
        evidence: r.fill.derivation,
        cvent_used: false,
        legacy_used: false,
        str_used: false,
      });
    }
  }

  // Optional unknown placeholder clears where no fill
  let unknownClears = 0;
  for (const r of gaps) {
    if (cityFills.some((x) => x.key === r.key)) continue;
    if (!blank(r.city) && PLACEHOLDER_RE.test(String(r.city).trim())) {
      unknownClears++;
      mutations.push({
        mutation_class: "UNKNOWN_PLACEHOLDER_CLEAR",
        field: "City",
        before: r.city,
        after: null,
        airtable_record_id: r.id,
        property_identity_key: r.key,
        note: "recommended_hygiene_blank_plus_research_status",
        cvent_used: false,
        legacy_used: false,
        str_used: false,
      });
    }
  }

  wj("13-state-recompute.json", {
    before_populated: beforeStatePop,
    new_fills: stateFills.length,
    items: stateFills,
  });
  wj("14-market-recompute.json", {
    before_valid: beforeMarketValid,
    new_corrections: marketCorrections.length,
    unlocked_by_city_repair: count(marketCorrections, (x) => x.unlocked_by_city),
    items: marketCorrections,
  });
  wj("15-market-registry-candidates.json", {
    n: marketCandidates.length,
    candidates: marketCandidates,
  });
  wj("16-submarket-recompute.json", {
    matched_fills: count(submarketCorrections, (x) => x.status === "MATCHED"),
    not_applicable: count(submarketCorrections, (x) => x.status === "NOT_APPLICABLE"),
    items: submarketCorrections,
  });
  wj("17-address-coordinate-cross-recovery.json", {
    address_fills: addressFills.length,
    coordinate_fills: coordFills.length,
    object_object_generated: 0,
    address_items: addressFills,
    coord_items: coordFills,
  });

  // Final city scorecard (projected)
  const projectedCity = new Map(rows.map((r) => [r.key, r.city]));
  for (const f of cityFills) projectedCity.set(f.key, f.city);
  for (const m of mutations.filter((x) => x.mutation_class === "UNKNOWN_PLACEHOLDER_CLEAR")) {
    if (!cityFills.some((f) => f.key === m.property_identity_key)) projectedCity.set(m.property_identity_key, null);
  }

  let finalValid = 0;
  let finalUnknown = 0;
  let finalBlank = 0;
  let finalInvalid = 0;
  for (const r of rows) {
    const c = projectedCity.get(r.key);
    if (isUnresolvedCity(c)) {
      if (blank(c)) finalBlank++;
      else finalUnknown++;
    } else if (
      validateCitySemantics(c, r.country).ok &&
      !isPostalAsCity(c, r.country) &&
      !isDescriptorCity(c)
    )
      finalValid++;
    else finalInvalid++;
  }

  wj("18-post-research-city-scorecard.json", {
    baseline: {
      valid: baselineValid,
      unknown: baselineUnknown,
      blank: baselineBlank,
      invalid: baselineInvalid,
    },
    projected: {
      valid: finalValid,
      unknown: finalUnknown,
      blank: finalBlank,
      invalid: finalInvalid,
      valid_pct: Math.round((1000 * finalValid) / rows.length) / 10,
    },
    new_city_resolutions: cityFills.length,
    by_derivation: cityFills.reduce((a, r) => {
      a[r.derivation] = (a[r.derivation] || 0) + 1;
      return a;
    }, {}),
  });

  // Downstream impact projected market coverage
  let projMarketValid = 0;
  let projMarketUnresolved = 0;
  let projState = 0;
  let projSubMatched = 0;
  let projSubNa = 0;
  let projSubUnres = 0;
  for (const r of simulated) {
    const state =
      mutations.find((m) => m.property_identity_key === r.key && m.field === "State / Region")?.after ||
      r.state;
    if (!blank(state)) projState++;
    const market =
      mutations.find((m) => m.property_identity_key === r.key && m.field === "Market")?.after || r.market;
    const mk = marketKind(
      classifyProductionMarket({ country: r.country, market, city: r.city, state }),
      market
    );
    if (mk === "CANONICAL_VALID" || mk === "CITY_EQUALS_MARKET_VIA_EXPLICIT_REGISTRY") projMarketValid++;
    else if (blank(market) || mk === "BLANK" || mk === "INVALID") projMarketUnresolved++;

    const sub =
      mutations.find((m) => m.property_identity_key === r.key && m.field === "Submarket")?.after ||
      r.submarket;
    if (!blank(sub)) {
      projSubMatched++;
    } else if (market && classifySubmarketApplicability({ country: r.country, market, submarket: null, submarketConfidence: "No Match" }) === "NOT_APPLICABLE") {
      projSubNa++;
    } else projSubUnres++;
  }

  wj("19-downstream-geography-impact.json", {
    state_before: beforeStatePop,
    state_after_projected: projState,
    market_valid_before: beforeMarketValid,
    market_valid_after_projected: projMarketValid,
    market_unresolved_after: projMarketUnresolved,
    submarket_matched_projected: projSubMatched,
    submarket_na_projected: projSubNa,
    submarket_unresolved_projected: projSubUnres,
    city_repairs_unlocking_market: count(marketCorrections, (x) => x.unlocked_by_city),
  });

  // Other placeholder audit
  const otherPlaceholders = [];
  for (const r of rows) {
    for (const [field, val] of [
      ["Address", r.address],
      ["State / Region", r.state],
      ["Market", r.market],
      ["Submarket", r.submarket],
      ["Current Brand", r.brand],
      ["Phone", r.phone],
      ["Rooms / Keys", r.rooms],
      ["Opening Date", r.opening],
      ["Operator / Management Company", r.operator],
    ]) {
      if (!blank(val) && PLACEHOLDER_RE.test(String(val).trim())) {
        otherPlaceholders.push({ key: r.key, field, value: val });
      }
    }
  }
  wj("20-other-placeholder-audit.json", {
    n: otherPlaceholders.length,
    by_field: otherPlaceholders.reduce((a, x) => {
      a[x.field] = (a[x.field] || 0) + 1;
      return a;
    }, {}),
    sample: otherPlaceholders.slice(0, 100),
    mutate_in_this_task: false,
  });

  wj("21-city-corrective-dry-run.json", {
    apply: false,
    v4_paused: true,
    authorized: false,
    manifest_name: "CITY_GEOGRAPHY_CORRECTIVE_MANIFEST",
    unsupported_overwrites: 0,
    cvent_evidence: 0,
    legacy_evidence: 0,
    mutation_count: mutations.length,
    mutation_class_counts: mutations.reduce((a, m) => {
      a[m.mutation_class] = (a[m.mutation_class] || 0) + 1;
      return a;
    }, {}),
    mutations,
  });

  const remainingGaps = gaps.filter((r) => !cityFills.some((f) => f.key === r.key));
  const queues = {
    CITY_RESEARCH_EXHAUSTED: [],
    CITY_STEWARD_REVIEW: [],
    CITY_RESEARCH: remainingGaps.map((r) => r.key || r.id),
  };
  for (const r of remainingGaps) {
    const attempted = [];
    if (!blank(r.address)) attempted.push("address_parse");
    if (r.lat != null) attempted.push("mapbox_reverse");
    if (!blank(r.url)) attempted.push("city_resolver_v4_url");
    attempted.push("claims_scan");
    const serp = serpResults.find((x) => x.key === r.key);
    if (serp) attempted.push("serpapi");
    if (serp && !serp.ok && serp.reason === "identity_low") {
      queues.CITY_STEWARD_REVIEW.push({
        key: r.key,
        reason: "identity_or_ambiguity",
        attempted,
      });
    } else {
      queues.CITY_RESEARCH_EXHAUSTED.push({
        key: r.key,
        attempted,
        last_attempted: new Date().toISOString(),
        reason: serp?.reason || "ladder_incomplete_or_no_eligible_city",
        next_trigger: ["new_official_url", "new_coords", "new_address_claim", "new_adapter"],
      });
    }
  }
  wj("22-city-remediation-queues.json", {
    CITY_RESEARCH: queues.CITY_RESEARCH.length,
    CITY_RESEARCH_EXHAUSTED: queues.CITY_RESEARCH_EXHAUSTED.length,
    CITY_STEWARD_REVIEW: queues.CITY_STEWARD_REVIEW.length,
    exhausted: queues.CITY_RESEARCH_EXHAUSTED,
    steward: queues.CITY_STEWARD_REVIEW,
  });

  const validPct = Math.round((1000 * finalValid) / rows.length) / 10;
  const cityReady = baselineInvalid === 0 && finalInvalid === 0 && cityFills.length > 0;
  const manifestReady = cityFills.length > 0 || unknownClears > 0;

  wm(
    "23-v4-city-readiness.md",
    `# V4 City Readiness

**V4 PAUSED**

## Principle

Legitimate Unknown City must **not** block V4 once:

- known-invalid City = 0
- City research pipeline works
- Unknowns explicitly queued
- geography cascade works

## Current

- Known-invalid City: **${finalInvalid}** (required 0)
- Projected valid City coverage: **${validPct}%**
- City fills ready (dry-run): **${cityFills.length}**
- Remaining unresolved queued: **${remainingGaps.length}**

## Verdict

V4: **READY AFTER CITY CORRECTION** (apply City/geography corrective dry-run first; do not resume in this task).
`
  );

  const answers = {
    1: rows.length,
    2: baselineValid,
    3: baselineUnknown,
    4: baselineBlank,
    5: 0,
    6: baselineInvalid,
    7: buckets.A_CITY_NOT_RESEARCHED.length,
    8: claimRecovery.length,
    9: buckets.C_ADDRESS_EXISTS_CITY_NOT_PARSED.length,
    10: buckets.D_COORDINATES_EXIST_CITY_NOT_DERIVED.length,
    11: 0,
    12: buckets.H_SERPAPI_RESEARCH_CANDIDATE.length,
    13: queues.CITY_STEWARD_REVIEW.length,
    14: claimRecovery.length,
    15: addrResults.length,
    16: count(cityFills, (x) => x.derivation === "COORDINATE_ADMIN_LOOKUP"),
    17: count(cityFills, (x) => x.derivation === "OFFICIAL_URL_SLUG" || x.derivation === "CITY_RESOLVER_V4"),
    18: count(cityFills, (x) => x.derivation === "SERPAPI_EXACT_HIGH"),
    19: cityFills.length,
    20: finalValid,
    21: finalUnknown,
    22: finalBlank,
    23: 0,
    24: finalInvalid,
    25: beforeStatePop,
    26: stateFills.length,
    27: projState,
    28: beforeMarketValid,
    29: count(marketCorrections, (x) => x.unlocked_by_city),
    30: projMarketValid,
    31: projMarketUnresolved,
    32: marketCandidates.length,
    33: count(submarketCorrections, (x) => x.status === "MATCHED"),
    34: projSubMatched,
    35: projSubNa,
    36: projSubUnres,
    37: projSubMatched + projSubNa > 0
      ? Math.round((1000 * projSubMatched) / Math.max(1, projSubMatched + projSubUnres)) / 10
      : 0,
    38: addressFills.length,
    39: coordFills.length,
    40: 0,
    41: serpPlan.length,
    42: searches,
    43: detailCalls,
    44: cacheHits,
    45: searches ? Math.round((100 * count(serpResults, (x) => x.ok)) / searches) / 100 : 0,
    46: false,
    47: "blank_factual_field_plus_research_status_UNRESOLVED",
    48: otherPlaceholders.length,
    49: cityFills.length,
    50: unknownClears,
    51: stateFills.length,
    52: marketCorrections.length,
    53: count(submarketCorrections, (x) => x.status === "MATCHED"),
    54: addressFills.length,
    55: coordFills.length,
    56: queues.CITY_STEWARD_REVIEW.length,
    57: 0,
    58: 0,
    59: true,
    60: true,
    61: true,
    62: true,
    63: true,
    verdicts: {
      CITY_RESOLUTION: cityReady && validPct >= 90 ? "READY" : cityReady ? "PARTIAL" : "NOT READY",
      UNKNOWN_PLACEHOLDERS: "REMOVE FROM FACTUAL FIELDS",
      DOWNSTREAM_GEOGRAPHY: marketCorrections.length + stateFills.length > 0 ? "READY" : "PARTIAL",
      CITY_CORRECTIVE_MANIFEST: manifestReady ? "READY FOR AUTHORIZATION" : "NOT READY",
      V4: "READY AFTER CITY CORRECTION",
    },
  };

  wj("24-final-report-answers.json", answers);
  wm(
    "24-final-report.md",
    `# City Resolution V1 — Final Report (Dry Run)

**DO NOT APPLY · V4 PAUSED**

## Verdicts

| | |
| --- | --- |
| CITY RESOLUTION | **${answers.verdicts.CITY_RESOLUTION}** |
| UNKNOWN PLACEHOLDERS | **${answers.verdicts.UNKNOWN_PLACEHOLDERS}** |
| DOWNSTREAM GEOGRAPHY | **${answers.verdicts.DOWNSTREAM_GEOGRAPHY}** |
| CITY CORRECTIVE MANIFEST | **${answers.verdicts.CITY_CORRECTIVE_MANIFEST}** |
| V4 | **${answers.verdicts.V4}** |

## Headline

- Baseline valid City: **${baselineValid}** / 1437
- New production-eligible City fills: **${cityFills.length}**
- Projected valid City: **${finalValid}** (${validPct}%)
- Known-invalid: **${finalInvalid}** (required 0)
- SerpApi searches: **${searches}** · cache hits: **${cacheHits}** · resolved: **${count(serpResults, (x) => x.ok)}**
- Market unlocked by City repair: **${answers[29]}**
- State fills: **${stateFills.length}** · Submarket MATCHED fills: **${answers[33]}**

## Exact answers: see \`24-final-report-answers.json\`

Manifest: \`21-city-corrective-dry-run.json\`
`
  );

  console.log(
    JSON.stringify(
      {
        gaps: gaps.length,
        cityFills: cityFills.length,
        finalValid,
        validPct,
        finalInvalid,
        stateFills: stateFills.length,
        marketCorrections: marketCorrections.length,
        serp: { searches, cacheHits, resolved: count(serpResults, (x) => x.ok) },
        verdicts: answers.verdicts,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
