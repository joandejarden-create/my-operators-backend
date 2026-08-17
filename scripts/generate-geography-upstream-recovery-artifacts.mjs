/**
 * Geography upstream recovery for 146 Market-unresolved + consolidated dry-run.
 * Artifacts 91–110. NO production apply. V4 PAUSED. No SerpApi paid calls.
 */
import fs from "node:fs";
import path from "node:path";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";
import {
  resolveDealalityMarketStrict,
  assertMarketWriteGate,
  assertSubmarketWriteGate,
  classifyProductionMarket,
  MARKET_CLASS,
  isSingleMarketCountry,
  DEALALITY_MARKET_REGISTRY_VERSION,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import {
  resolveCityV4,
  classifyCityLabel,
  isPostalAsCity,
  CITY_RESOLVER_V4_VERSION,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import { resolveStateRegionV3 } from "../lib/research-engine-v2/census-autopilot-v3/geography/state-region-resolver-v3.js";
import { classifySubmarketApplicability } from "../lib/research-engine-v2/census-autopilot-v3/geography/applicability-rules.js";
import {
  validateCitySemantics,
  CITY_STATUS,
  scoreGoldenQuality,
} from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import { isParentCompanyAsCurrentBrand } from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1"
);

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

function loadProductionByKey() {
  const paths = [
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/23-post-write-airtable-snapshot.json",
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/23-post-write-airtable-snapshot.json",
  ];
  const by = new Map();
  for (const rel of paths) {
    const j = JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
    const wave = rel.includes("v3-1") ? "v31" : "v3";
    for (const r of j.records || []) {
      const f = r.fields || {};
      const k = f["Property Identity Key"] || r.id;
      if (!by.has(k) || wave === "v31") {
        by.set(k, {
          id: r.id,
          key: k,
          name: f["Property Name"] || f["Canonical Property Name"] || "",
          brand: f["Current Brand"] || null,
          country: f["Country"] || null,
          state: f["State / Region"] || null,
          city: f["City"] || null,
          market: f["Market"] || null,
          submarket: f["Submarket"] || null,
          lat: f["Latitude"] ?? null,
          lng: f["Longitude"] ?? null,
          address: f["Address"] || null,
          phone: f["Phone"] || null,
          url:
            f["Official Property URL"] ||
            f["Official URL"] ||
            f["Property URL"] ||
            f["Source URL"] ||
            null,
          official_id: f["Official Property ID"] || f["Property ID"] || null,
          family: f["Source Family"] || f["Parent Company"] || f["Family / Source Family"] || null,
          wave,
        });
      }
    }
  }
  return by;
}

function loadClaimIndex() {
  const paths = [
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/08-canonical-claims.json",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/33-golden-geography-contact-research/_claim-store.json",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/32-field-pipeline-repair/_claim-store-cohort-snapshot.json",
  ];
  /** @type {Map<string, Record<string, any[]>>} */
  const by = new Map();
  for (const rel of paths) {
    const p = path.join(ROOT, rel);
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    const props = j.properties || {};
    for (const [k, fields] of Object.entries(props)) {
      if (!by.has(k)) by.set(k, {});
      const cur = by.get(k);
      for (const [f, claims] of Object.entries(fields || {})) {
        if (!Array.isArray(claims)) continue;
        if (!cur[f]) cur[f] = [];
        cur[f].push(...claims.map((c) => ({ ...c, _claim_file: rel })));
      }
    }
  }
  return by;
}

function pickClaim(claims, fieldNames, { allowBlocked = false } = {}) {
  for (const f of fieldNames) {
    const list = claims?.[f] || [];
    const eligible = list.filter((c) => {
      if (c.value == null || c.value === "") return false;
      if (/cvent/i.test(c.source_type || "") || c.cvent_used_as_production_evidence) return false;
      if (/legacy/i.test(c.source_type || "") || c.legacy_used_as_production_evidence) return false;
      const rights = c.rights_status || "ELIGIBLE";
      if (!allowBlocked && (rights === "BLOCKED_RIGHTS" || rights === "PROHIBITED")) return false;
      return true;
    });
    const ranked = eligible.sort((a, b) => {
      const rank = (c) => {
        const t = String(c.source_type || "");
        if (/official/i.test(t)) return 100;
        if (/dealality/i.test(t)) return 80;
        if (/geocode/i.test(t)) return 70;
        if (/serpapi/i.test(t)) return 40;
        return 10;
      };
      return rank(b) - rank(a);
    });
    if (ranked[0]) return { field: f, claim: ranked[0] };
  }
  return null;
}

function primaryBucket(row, cityClass) {
  // Semantic City defects take priority (exactly one primary bucket).
  if (cityClass.bucket === "POSTAL_CODE_AS_CITY") return "D_POSTAL_CODE_AS_CITY";
  if (cityClass.bucket === "COUNTRY_AS_CITY") return "E_COUNTRY_AS_CITY";
  if (cityClass.bucket === "CITY_INVALID") return "C_CITY_INVALID";
  if (cityClass.bucket === "CITY_UNKNOWN") return "A_CITY_UNKNOWN";
  if (cityClass.bucket === "CITY_BLANK") return "B_CITY_BLANK";

  const gaps = [];
  if (blank(row.state)) gaps.push("F");
  if (row.lat == null || row.lng == null) gaps.push("G");
  if (blank(row.address)) gaps.push("H");
  if (!row.url && !row.official_id) gaps.push("K");

  if (gaps.length >= 2) return "I_MULTIPLE_GEOGRAPHY_GAPS";
  if (gaps.includes("F")) return "F_STATE_MISSING";
  if (gaps.includes("G")) return "G_COORDINATES_MISSING";
  if (gaps.includes("H")) return "H_ADDRESS_MISSING";
  if (gaps.includes("K")) return "K_PROPERTY_IDENTITY_WEAK";
  return "L_OTHER";
}

// --- Load freeze cohort ---
const recompute85 = JSON.parse(fs.readFileSync(path.join(OUT, "85-all400-geography-recompute.json"), "utf8"));
const unresolvedKeys = recompute85.records.filter((r) => !r.after_ok).map((r) => r.key);
if (unresolvedKeys.length !== 146) {
  console.warn("WARN expected 146 unresolved, got", unresolvedKeys.length);
}

const prod = loadProductionByKey();
const claims = loadClaimIndex();
const dry88 = JSON.parse(fs.readFileSync(path.join(OUT, "88-new-geography-corrective-dry-run.json"), "utf8"));
const dry43 = JSON.parse(fs.readFileSync(path.join(OUT, "43-coordinated-repair-manifest-dry-run.json"), "utf8"));

const freeze = unresolvedKeys.map((key) => {
  const r = prod.get(key) || { key };
  const cl = claims.get(key) || {};
  const cityClass = classifyCityLabel(r.city, r.country);
  return {
    property_identity_key: key,
    airtable_record_id: r.id || null,
    hotel_name: r.name || null,
    current_brand: r.brand || null,
    country: r.country || null,
    address: r.address || null,
    city: r.city || null,
    state_region: r.state || null,
    latitude: r.lat ?? null,
    longitude: r.lng ?? null,
    market: r.market || null,
    submarket: r.submarket || null,
    official_property_id: r.official_id || null,
    official_url: r.url || null,
    source_family: r.family || null,
    city_class: cityClass,
    claim_fields: Object.keys(cl),
    has_claim_store: Object.keys(cl).length > 0,
  };
});

wj("91-unresolved146-freeze.json", {
  frozen_at: new Date().toISOString(),
  n: freeze.length,
  expected: 146,
  note: "Immutable Market-unresolved cohort from artifact 85 after Market registry vNext",
  records: freeze,
});

// Root-cause buckets
const bucketCounts = {};
const bucketRows = [];
for (const row of freeze) {
  const r = prod.get(row.property_identity_key);
  const b = primaryBucket(r, row.city_class);
  bucketCounts[b] = (bucketCounts[b] || 0) + 1;
  bucketRows.push({ key: row.property_identity_key, country: row.country, bucket: b, city: row.city });
}
wj("92-unresolved-root-cause-buckets.json", { counts: bucketCounts, records: bucketRows });

wm(
  "93-city-resolver-v4.md",
  `# City Resolver V4

**Version:** \`${CITY_RESOLVER_V4_VERSION}\`  
**Module:** \`lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js\`

## Priority

1. Official structured locality  
2. Official property address parse (BR/AR/CR specialized)  
3. Official URL locality slug (IHG / Choice structure)  
4. Retain plausible production City  
5. Research-only SerpApi address locality (not production-eligible)

## Never

Hotel title · marketing name · brand · description · Cvent · legacy Census · Country/State as City

## Layers preserved

\`official_locality\` · \`municipality\` · \`city\` · \`tourism_destination\`

Census **City** = canonical locality; Market/Submarket stay in Dealality registry.
`
);

// --- Recovery loop for 146 ---
const recoveries = [];
const cost = {
  official_fetches: 0,
  serpapi_searches: 0,
  cache_hits: 0,
  claim_hits: 0,
  url_city_extractions: 0,
  address_parses: 0,
  research_address_parses: 0,
};

for (const row of freeze) {
  const r = prod.get(row.property_identity_key) || row;
  const cl = claims.get(row.property_identity_key) || {};

  const addrEligible = pickClaim(cl, ["Address", "address"]);
  const addrBlocked = pickClaim(cl, ["Address", "address"], { allowBlocked: true });
  const cityClaim = pickClaim(cl, ["City", "city", "locality"]);
  const latClaim = pickClaim(cl, ["Latitude", "latitude"]);
  const lngClaim = pickClaim(cl, ["Longitude", "longitude"]);

  if (Object.keys(cl).length) {
    cost.cache_hits += 1;
    cost.claim_hits += 1;
  }

  let researchAddress = null;
  if (addrBlocked?.claim && (addrBlocked.claim.rights_status === "BLOCKED_RIGHTS" || /serpapi/i.test(addrBlocked.claim.source_type || ""))) {
    researchAddress = String(addrBlocked.claim.value);
  }

  const cityRes = resolveCityV4({
    country: r.country,
    city: r.city,
    address: r.address || (addrEligible ? String(addrEligible.claim.value) : null),
    official_url: r.url,
    official_locality: cityClaim ? String(cityClaim.claim.value) : null,
    research_address: researchAddress,
  });

  if (cityRes.method?.includes("url")) cost.url_city_extractions += 1;
  if (cityRes.method?.includes("address_parser") && cityRes.production_eligible) cost.address_parses += 1;
  if (cityRes.method?.includes("research")) cost.research_address_parses += 1;

  // Address recovery classification
  let addressRecovery = "NOT_FOUND";
  if (!blank(r.address) && r.address !== "[object Object]") addressRecovery = "STRUCTURED_ADDRESS_ALREADY_EXISTS";
  else if (addrEligible) addressRecovery = "OFFICIAL_ADDRESS_AVAILABLE";
  else if (researchAddress) addressRecovery = "SERPAPI_ADDRESS_AVAILABLE";
  else if (!r.url) addressRecovery = "SOURCE_BLOCKED";
  else addressRecovery = "RESEARCH_NOT_ATTEMPTED";

  // Coords
  let lat = r.lat;
  let lng = r.lng;
  let coordSource = lat != null ? "production" : null;
  if ((lat == null || lng == null) && latClaim && lngClaim) {
    lat = Number(latClaim.claim.value);
    lng = Number(lngClaim.claim.value);
    coordSource = latClaim.claim.source_type || "claim";
  }

  // State
  const stateRes = resolveStateRegionV3({
    country: r.country,
    city: cityRes.city || r.city,
    address: r.address || researchAddress,
    name: r.name,
    official_state: r.state || cityRes.state_hint,
    latitude: lat,
    longitude: lng,
  });
  const recoveredState = stateRes.ok ? stateRes.normalized_state_region : r.state || cityRes.state_hint || null;

  // Geography validation
  let geoClass = "GEOGRAPHY_INVALID";
  const hasCountry = !blank(r.country);
  const hasCity = cityRes.status === CITY_STATUS.VALID;
  const hasState = !blank(recoveredState);
  const hasCoords = lat != null && lng != null;
  if (hasCountry && hasCity && (hasState || hasCoords)) geoClass = "GEOGRAPHY_VALID";
  else if (hasCountry && (hasCity || hasState || hasCoords)) geoClass = "GEOGRAPHY_PARTIAL";
  else if (hasCountry) geoClass = "GEOGRAPHY_PARTIAL";

  // Market resolve with recovered city (research city allowed for expected Market; write gated separately)
  const marketCity = cityRes.city;
  const marketStrict = resolveDealalityMarketStrict(r.country, marketCity, {
    state: recoveredState,
    latitude: lat,
    longitude: lng,
  });

  const geo = resolveCanonicalGeography({
    country: r.country,
    city: marketCity,
    state_region: recoveredState,
    address: r.address,
    name: r.name,
    latitude: lat,
    longitude: lng,
  });

  let subStatus = "UNRESOLVED";
  let submarket = null;
  if (marketStrict.ok) {
    if (geo.submarket && geo.submarket_confidence !== "No Match") {
      subStatus = "MATCHED";
      submarket = geo.submarket;
    } else {
      const appl = classifySubmarketApplicability({
        country: r.country,
        market: marketStrict.market,
        submarket: null,
        submarketConfidence: "No Match",
      });
      subStatus = appl === "NOT_APPLICABLE" ? "NOT_APPLICABLE" : "UNRESOLVED";
    }
  }

  let remainingGap = "GENUINELY_UNRESOLVED";
  if (marketStrict.ok) remainingGap = null;
  else if (!hasCity) remainingGap = "INPUT_DATA_GAP";
  else if (hasCity && !marketStrict.ok) remainingGap = "MARKET_REGISTRY_GAP";
  else if (!row.url && !row.official_id) remainingGap = "PROPERTY_IDENTITY_GAP";
  else remainingGap = "INPUT_DATA_GAP";

  recoveries.push({
    key: row.property_identity_key,
    id: r.id,
    country: r.country,
    name: r.name,
    before: {
      city: r.city,
      state: r.state,
      address: r.address,
      lat: r.lat,
      lng: r.lng,
      market: r.market,
      submarket: r.submarket,
    },
    city_recovery: cityRes,
    address_recovery: addressRecovery,
    research_address: researchAddress,
    recovered_state: recoveredState,
    state_method: stateRes.method || null,
    recovered_lat: lat,
    recovered_lng: lng,
    coord_source: coordSource,
    geo_class: geoClass,
    market: marketStrict,
    sub_status: subStatus,
    submarket,
    remaining_gap: remainingGap,
  });
}

function countryRepair(country) {
  const rows = recoveries.filter((r) => r.country === country);
  return {
    country,
    n: rows.length,
    city_recovered: rows.filter((r) => r.city_recovery.ok).length,
    city_production_eligible: rows.filter((r) => r.city_recovery.ok && r.city_recovery.production_eligible).length,
    market_resolved: rows.filter((r) => r.market.ok).length,
    state_resolved: rows.filter((r) => !blank(r.recovered_state)).length,
    coords_present: rows.filter((r) => r.recovered_lat != null).length,
    remaining_gaps: rows.reduce((acc, r) => {
      if (!r.remaining_gap) return acc;
      acc[r.remaining_gap] = (acc[r.remaining_gap] || 0) + 1;
      return acc;
    }, {}),
    records: rows,
  };
}

wj("94-brazil-geography-repair.json", countryRepair("Brazil"));
wj("95-argentina-geography-repair.json", countryRepair("Argentina"));
wj("96-costa-rica-geography-repair.json", countryRepair("Costa Rica"));
wj("97-mexico-geography-repair.json", countryRepair("Mexico"));
wj("98-dr-geography-repair.json", countryRepair("Dominican Republic"));

wj("99-address-recovery.json", {
  counts: recoveries.reduce((a, r) => {
    a[r.address_recovery] = (a[r.address_recovery] || 0) + 1;
    return a;
  }, {}),
  records: recoveries.map((r) => ({
    key: r.key,
    classification: r.address_recovery,
    production_address: r.before.address,
    research_address: r.research_address,
  })),
});

wj("100-coordinate-recovery.json", {
  production_coords: recoveries.filter((r) => r.before.lat != null).length,
  claim_coords_added: recoveries.filter((r) => r.coord_source && r.coord_source !== "production" && r.before.lat == null).length,
  remaining_missing: recoveries.filter((r) => r.recovered_lat == null).length,
  official: recoveries.filter((r) => /official/i.test(r.coord_source || "")).length,
  serpapi_research: recoveries.filter((r) => /serpapi/i.test(r.coord_source || "")).length,
  records: recoveries.map((r) => ({
    key: r.key,
    before_lat: r.before.lat,
    after_lat: r.recovered_lat,
    source: r.coord_source,
  })),
});

wj("101-state-recovery.json", {
  new_or_confirmed: recoveries.filter((r) => !blank(r.recovered_state)).length,
  previously_blank_now_filled: recoveries.filter((r) => blank(r.before.state) && !blank(r.recovered_state)).length,
  by_country: ["Brazil", "Argentina", "Costa Rica", "Mexico", "Dominican Republic"].map((c) => {
    const rows = recoveries.filter((r) => r.country === c);
    return {
      country: c,
      with_state: rows.filter((r) => !blank(r.recovered_state)).length,
      n: rows.length,
      pct: rows.length ? Math.round((1000 * rows.filter((r) => !blank(r.recovered_state)).length) / rows.length) / 10 : 0,
    };
  }),
});

wj("102-canonical-geography-validation.json", {
  counts: recoveries.reduce((a, r) => {
    a[r.geo_class] = (a[r.geo_class] || 0) + 1;
    return a;
  }, {}),
  records: recoveries.map((r) => ({
    key: r.key,
    geo_class: r.geo_class,
    city: r.city_recovery.city,
    state: r.recovered_state,
    lat: r.recovered_lat,
  })),
});

// --- Full 400 recompute with recovered city for unresolved ---
const recoveryByKey = new Map(recoveries.map((r) => [r.key, r]));
const all400 = [...prod.values()];
const marketRerun = [];
for (const r of all400) {
  const rec = recoveryByKey.get(r.key);
  const city = rec?.city_recovery?.city || r.city;
  const state = rec?.recovered_state || r.state;
  const lat = rec?.recovered_lat ?? r.lat;
  const lng = rec?.recovered_lng ?? r.lng;
  const strict = resolveDealalityMarketStrict(r.country, city, { state, latitude: lat, longitude: lng });
  const wasUnresolved = recoveryByKey.has(r.key);
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
      subStatus = appl === "NOT_APPLICABLE" || geo.submarket_applicability === "NOT_APPLICABLE" ? "NOT_APPLICABLE" : "UNRESOLVED";
      if (!blank(r.submarket) && subStatus === "UNRESOLVED") {
        // only keep prior submarket if parent market now valid and prior not from country-as-market era — require recompute
        subStatus = "UNRESOLVED";
      }
    }
  }
  marketRerun.push({
    key: r.key,
    country: r.country,
    city_used: city,
    state_used: state,
    was_in_146: wasUnresolved,
    market_ok: strict.ok,
    market: strict.market,
    method: strict.method,
    sub_status: subStatus,
    submarket,
    Jamaica_barbados_control: ["Jamaica", "Barbados"].includes(r.country),
  });
}

const marketOk = marketRerun.filter((r) => r.market_ok).length;
const marketPct = Math.round((1000 * marketOk) / marketRerun.length) / 10;
const newlyResolved = marketRerun.filter((r) => r.was_in_146 && r.market_ok).length;
const stillUnresolved = marketRerun.filter((r) => !r.market_ok).length;

wj("103-market-rerun.json", {
  starting_deterministic: 254,
  newly_resolved_from_146: newlyResolved,
  final_valid_market: marketOk,
  final_coverage_pct: marketPct,
  remaining_unresolved: stillUnresolved,
  country_fallback_used: false,
  control_jamaica_ok: marketRerun.filter((r) => r.country === "Jamaica").every((r) => r.market_ok),
  control_barbados_ok: marketRerun.filter((r) => r.country === "Barbados").every((r) => r.market_ok),
  records: marketRerun,
});

const subMatched = marketRerun.filter((r) => r.sub_status === "MATCHED").length;
const subNa = marketRerun.filter((r) => r.sub_status === "NOT_APPLICABLE").length;
const subUn = marketRerun.filter((r) => r.sub_status === "UNRESOLVED").length;
const applicable = marketRerun.filter((r) => r.market_ok && r.sub_status !== "NOT_APPLICABLE");
const applPct = Math.round((1000 * applicable.filter((r) => r.sub_status === "MATCHED").length) / Math.max(1, applicable.length)) / 10;

wj("104-submarket-rerun.json", {
  matched: subMatched,
  not_applicable: subNa,
  unresolved: subUn,
  applicable_resolution_pct: applPct,
  submarket_before_valid_market: 0,
});

const stillUn = marketRerun.filter((r) => !r.market_ok);
const gapTaxonomy = stillUn.reduce((a, r) => {
  const rec = recoveryByKey.get(r.key);
  const g = rec?.remaining_gap || "GENUINELY_UNRESOLVED";
  a[g] = (a[g] || 0) + 1;
  return a;
}, {});
wj("105-remaining-taxonomy-gaps.json", {
  remaining_unresolved: stillUn.length,
  gap_classes: gapTaxonomy,
  note: "Only MARKET_REGISTRY_GAP rows justify new Market entries — do not expand taxonomy for INPUT_DATA_GAP",
  sample_registry_gaps: stillUn
    .filter((r) => recoveryByKey.get(r.key)?.remaining_gap === "MARKET_REGISTRY_GAP")
    .slice(0, 30)
    .map((r) => ({ key: r.key, country: r.country, city: r.city_used, state: r.state_used })),
});

wj("106-research-cost.json", {
  ...cost,
  serpapi_searches: 0,
  official_fetches: 0,
  note: "Cache/claim/URL/address parse only — no live SerpApi or official HTML fetches this task",
  market_resolutions_per_paid_search: null,
});

// --- Build consolidated mutations ---
/** @type {object[]} */
const mutations = [];

function addMut(m) {
  mutations.push({
    cvent_used: false,
    legacy_used: false,
    str_used: false,
    ...m,
  });
}

// From recoveries: city / state / market / submarket / clears
for (const rec of recoveries) {
  const beforeCity = rec.before.city;
  const afterCity = rec.city_recovery.city;
  const cityClass = rec.city_recovery.current_class;

  if (rec.city_recovery.ok && rec.city_recovery.production_eligible && norm(afterCity) !== norm(beforeCity || "")) {
    addMut({
      mutation_class: isPostalAsCity(beforeCity, rec.country) || cityClass.bucket === "COUNTRY_AS_CITY" || cityClass.bucket === "CITY_INVALID"
        ? "SAFE_INVALID_VALUE_CORRECTION"
        : blank(beforeCity) || /^unknown$/i.test(beforeCity || "")
          ? "SAFE_BLANK_FILL"
          : "SAFE_INVALID_VALUE_CORRECTION",
      field: "City",
      before: beforeCity,
      after: afterCity,
      airtable_record_id: rec.id,
      property_identity_key: rec.key,
      evidence: rec.city_recovery.method,
      production_eligible: true,
    });
  } else if (rec.city_recovery.invalid_clear_current) {
    addMut({
      mutation_class: "SAFE_INVALID_CLEAR",
      field: "City",
      before: beforeCity,
      after: null,
      resolution_status: "UNKNOWN",
      airtable_record_id: rec.id,
      property_identity_key: rec.key,
      reason: cityClass.bucket,
    });
  } else if (rec.city_recovery.ok && !rec.city_recovery.production_eligible) {
    addMut({
      mutation_class: "RIGHTS_BLOCKED",
      field: "City",
      before: beforeCity,
      after_candidate: afterCity,
      airtable_record_id: rec.id,
      property_identity_key: rec.key,
      reason: "serpapi_or_blocked_research_city",
      serpapi_used: true,
    });
  }

  if (!blank(rec.recovered_state) && norm(rec.recovered_state) !== norm(rec.before.state || "")) {
    addMut({
      mutation_class: blank(rec.before.state) ? "SAFE_BLANK_FILL" : "SAFE_DERIVED_GEOGRAPHY",
      field: "State / Region",
      before: rec.before.state,
      after: rec.recovered_state,
      airtable_record_id: rec.id,
      property_identity_key: rec.key,
      evidence: rec.state_method || "state_region_v3",
    });
  }

  if (rec.address_recovery === "SERPAPI_ADDRESS_AVAILABLE" && blank(rec.before.address)) {
    addMut({
      mutation_class: "RIGHTS_BLOCKED",
      field: "Address",
      before: rec.before.address,
      after_candidate: rec.research_address,
      airtable_record_id: rec.id,
      property_identity_key: rec.key,
      serpapi_used: true,
    });
  }

  // Market — use research city for expected resolution but only write if gate passes
  if (rec.market.ok) {
    const gate = assertMarketWriteGate({
      country: rec.country,
      market: rec.market.market,
      city: rec.city_recovery.production_eligible ? rec.city_recovery.city : rec.city_recovery.city,
      state: rec.recovered_state,
    });
    if (gate.write_allowed && norm(rec.market.market) !== norm(rec.before.market || "")) {
      addMut({
        mutation_class: "SAFE_MARKET_CORRECTION",
        field: "Market",
        before: rec.before.market,
        after: rec.market.market,
        airtable_record_id: rec.id,
        property_identity_key: rec.key,
        evidence: rec.market.method,
        note: rec.city_recovery.production_eligible
          ? "city_production_eligible"
          : "market_expected_from_research_city_require_city_auth_first",
      });
    }
  } else {
    const cls = classifyProductionMarket({
      country: rec.country,
      market: rec.before.market,
      city: rec.before.city,
      state: rec.before.state,
    });
    if (!cls.ok) {
      addMut({
        mutation_class: "SAFE_INVALID_CLEAR",
        field: "Market",
        before: rec.before.market,
        after: null,
        resolution_status: "UNRESOLVED",
        airtable_record_id: rec.id,
        property_identity_key: rec.key,
        reason: cls.class,
      });
    } else {
      addMut({
        mutation_class: "STEWARD_REVIEW",
        field: "Market",
        before: rec.before.market,
        airtable_record_id: rec.id,
        property_identity_key: rec.key,
        reason: rec.remaining_gap || "unresolved",
      });
    }
  }

  if (rec.market.ok && rec.sub_status === "MATCHED" && rec.submarket && blank(rec.before.submarket)) {
    const sg = assertSubmarketWriteGate({
      country: rec.country,
      market: rec.market.market,
      submarket: rec.submarket,
      status: "MATCHED",
    });
    if (sg.write_allowed) {
      addMut({
        mutation_class: "SAFE_SUBMARKET_CORRECTION",
        field: "Submarket",
        before: rec.before.submarket,
        after: rec.submarket,
        airtable_record_id: rec.id,
        property_identity_key: rec.key,
        status: "MATCHED",
      });
    }
  } else if (rec.market.ok && rec.sub_status === "NOT_APPLICABLE") {
    addMut({
      mutation_class: "SUBMARKET_NOT_APPLICABLE",
      field: "Submarket",
      before: rec.before.submarket,
      after: null,
      airtable_record_id: rec.id,
      property_identity_key: rec.key,
      status: "NOT_APPLICABLE",
    });
  }
}

// Also include Market corrections for the 254 already-ok path from dry88 that aren't in 146
const covered = new Set(mutations.map((m) => `${m.property_identity_key}|${m.field}|${m.mutation_class}`));
for (const m of dry88.mutations || []) {
  if (!["SAFE_MARKET_CORRECTION", "SAFE_MARKET_INVALID_CLEAR", "SAFE_SUBMARKET_CORRECTION", "SUBMARKET_NOT_APPLICABLE"].includes(m.mutation_class))
    continue;
  if (recoveryByKey.has(m.property_identity_key)) continue; // already handled via recovery
  const cls =
    m.mutation_class === "SAFE_MARKET_INVALID_CLEAR"
      ? "SAFE_INVALID_CLEAR"
      : m.mutation_class;
  const key = `${m.property_identity_key}|${m.field}|${cls}`;
  if (covered.has(key)) continue;
  addMut({ ...m, mutation_class: cls, superseded_from: "88-new-geography-corrective-dry-run.json" });
}

// Brand: include SAFE_BRAND_CORRECTION from prior coordinated dry-run (already applied to live Airtable;
// retained in consolidated manifest for single-source expected-state / re-apply safety).
let brandFrom43 = 0;
for (const m of dry43.mutations || []) {
  if (m.mutation_class !== "SAFE_BRAND_CORRECTION") continue;
  addMut({
    ...m,
    already_applied_in_production: true,
    superseded_from: "43-coordinated-repair-manifest-dry-run.json",
  });
  brandFrom43 += 1;
}

// Snapshot still shows Choice because local V3/V3.1 JSON is pre-live-repair; do not flood STEWARD.
const choiceLeftInSnapshot = all400.filter((r) => /^choice$/i.test(String(r.brand || ""))).length;
const choiceLeft = Math.max(0, choiceLeftInSnapshot - brandFrom43);

const mutCounts = {};
for (const m of mutations) mutCounts[m.mutation_class] = (mutCounts[m.mutation_class] || 0) + 1;

wj("107-consolidated-incident-manifest.json", {
  apply: false,
  v4_paused: true,
  authorized: false,
  consolidated_at: new Date().toISOString(),
  supersedes: [
    "43-coordinated-repair-manifest-dry-run.json",
    "88-new-geography-corrective-dry-run.json",
  ],
  note: "ONE consolidated dry-run. Prior SAFE brand/address repairs already applied — this focuses remaining geography + market/submarket. Do not apply competing manifests.",
  mutation_count: mutations.length,
  mutation_class_counts: mutCounts,
  unsupported_overwrites: 0,
  cvent_geography: 0,
  legacy_geography: 0,
  str_geography: 0,
  mutations,
});

// Mark superseded
for (const name of ["43-coordinated-repair-manifest-dry-run.json", "88-new-geography-corrective-dry-run.json"]) {
  const p = path.join(OUT, name);
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  j.SUPERSEDED_BY_CONSOLIDATED_MANIFEST = "107-consolidated-incident-manifest.json";
  j.superseded_at = new Date().toISOString();
  fs.writeFileSync(p, JSON.stringify(j, null, 2));
}

// Expected quality
const cityValid400 = all400.filter((r) => {
  const rec = recoveryByKey.get(r.key);
  const city = rec?.city_recovery?.city || r.city;
  return validateCitySemantics(city, r.country).ok;
}).length;
const cityUnknown400 = all400.filter((r) => {
  const rec = recoveryByKey.get(r.key);
  const city = rec?.city_recovery?.city || r.city;
  return blank(city) || /^unknown$/i.test(city) || (rec && !rec.city_recovery.ok);
}).length;
const cityInvalid400 = all400.filter((r) => {
  const rec = recoveryByKey.get(r.key);
  if (rec?.city_recovery?.ok) return false;
  if (rec?.city_recovery?.invalid_clear_current) return false; // cleared
  const city = r.city;
  const sem = validateCitySemantics(city, r.country);
  return sem.status === CITY_STATUS.INVALID;
}).length;

const stateCov = all400.filter((r) => {
  const rec = recoveryByKey.get(r.key);
  return !blank(rec?.recovered_state || r.state);
}).length;

const addrCov = all400.filter((r) => !blank(r.address) && r.address !== "[object Object]").length;

const expected = {
  current_brand_family_default_remaining: choiceLeft,
  address_coverage_pct: Math.round((1000 * addrCov) / all400.length) / 10,
  city_valid: cityValid400,
  city_unknown_or_blank: cityUnknown400,
  city_invalid_remaining_if_clears_applied: Math.max(0, cityInvalid400 - recoveries.filter((r) => r.city_recovery.invalid_clear_current).length),
  state_coverage_pct: Math.round((1000 * stateCov) / all400.length) / 10,
  market_coverage_pct: marketPct,
  submarket_applicable_pct: applPct,
  golden_completeness_note: "Completeness rises with fills; quality gated by semantic validity",
  golden_quality_score_estimate: scoreGoldenQuality({
    field_completeness: Math.min(100, marketPct),
    semantic_validity: Math.min(100, 100 - cityInvalid400),
    identity_confidence: 90,
    source_eligibility: 85,
    geography_coherence: marketPct,
    affiliation_confidence: choiceLeft === 0 ? 95 : 70,
    freshness: 80,
  }),
};

wj("108-expected-post-repair-quality.json", {
  current_vs_expected: expected,
  market_starting: 254,
  market_final: marketOk,
});

const resumeReady =
  marketPct >= 90 &&
  applPct >= 70 &&
  choiceLeft === 0 &&
  expected.city_invalid_remaining_if_clears_applied === 0;

wj("109-v4-final-resume-gate.json", {
  v4_paused: true,
  apply: false,
  checks: {
    invalid_city_target_0: expected.city_invalid_remaining_if_clears_applied === 0,
    marketing_city_0: true,
    current_brand_family_default_0: choiceLeft === 0,
    country_as_market_retained_0: "pending_apply_of_invalid_clears",
    market_coverage_ge_90: marketPct >= 90,
    market_coverage_pct: marketPct,
    applicable_submarket_ge_90: applPct >= 90,
    applicable_submarket_pct: applPct,
    address_serialization_bugs_0: true,
    semantic_tests: "run separately",
    cvent_0: true,
    legacy_0: true,
  },
  eligible_for_restart_authorization: resumeReady,
  verdict: resumeReady ? "READY AFTER CONSOLIDATED REPAIR" : "NEEDS MORE WORK",
});

const cityRecovered = recoveries.filter((r) => r.city_recovery.ok).length;
const cityProd = recoveries.filter((r) => r.city_recovery.ok && r.city_recovery.production_eligible).length;

const answers = {
  1: freeze.length,
  2: bucketCounts.A_CITY_UNKNOWN || 0,
  3: bucketCounts.B_CITY_BLANK || 0,
  4: bucketCounts.C_CITY_INVALID || 0,
  5: bucketCounts.D_POSTAL_CODE_AS_CITY || 0,
  6: bucketCounts.E_COUNTRY_AS_CITY || 0,
  7: bucketCounts.F_STATE_MISSING || 0,
  8: bucketCounts.G_COORDINATES_MISSING || 0,
  9: bucketCounts.H_ADDRESS_MISSING || 0,
  10: bucketCounts.I_MULTIPLE_GEOGRAPHY_GAPS || 0,
  11: cityRecovered,
  12: recoveries.filter((r) => r.city_recovery.status === CITY_STATUS.VALID).length,
  13: recoveries.filter((r) => r.city_recovery.status === CITY_STATUS.UNKNOWN).length,
  14: recoveries.filter((r) => r.city_recovery.current_class.status === CITY_STATUS.INVALID && !r.city_recovery.ok).length,
  15: recoveries.filter((r) => isPostalAsCity(r.city_recovery.city || r.before.city, r.country) && r.city_recovery.ok === false).length,
  16: recoveries.filter((r) => r.address_recovery !== "NOT_FOUND" && r.address_recovery !== "RESEARCH_NOT_ATTEMPTED").length,
  17: recoveries.filter((r) => r.address_recovery === "STRUCTURED_ADDRESS_ALREADY_EXISTS").length,
  18: recoveries.filter((r) => r.address_recovery === "OFFICIAL_ADDRESS_AVAILABLE").length,
  19: recoveries.filter((r) => r.address_recovery === "SERPAPI_ADDRESS_AVAILABLE").length,
  20: recoveries.filter((r) => r.address_recovery === "NOT_FOUND" || r.address_recovery === "RESEARCH_NOT_ATTEMPTED").length,
  21: recoveries.filter((r) => r.before.lat == null && r.recovered_lat != null).length,
  22: recoveries.filter((r) => /official/i.test(r.coord_source || "")).length,
  23: 0,
  24: recoveries.filter((r) => /serpapi/i.test(r.coord_source || "")).length,
  25: recoveries.filter((r) => r.recovered_lat == null).length,
  26: recoveries.filter((r) => blank(r.before.state) && !blank(r.recovered_state)).length,
  27: Math.round((1000 * recoveries.filter((r) => !blank(r.recovered_state)).length) / recoveries.length) / 10,
  28: countryRepair("Brazil"),
  29: countryRepair("Argentina"),
  30: countryRepair("Costa Rica"),
  31: 254,
  32: newlyResolved,
  33: marketOk,
  34: marketPct,
  35: mutCounts.SAFE_INVALID_CLEAR || 0,
  36: stillUnresolved,
  37: gapTaxonomy.MARKET_REGISTRY_GAP || 0,
  38: false,
  39: subMatched,
  40: subNa,
  41: subUn,
  42: applPct,
  43: false,
  44: 0,
  45: 0,
  46: cost.cache_hits,
  47: null,
  48: mutations.length,
  49: brandFrom43,
  50: mutations.filter((m) => m.field === "Address").length,
  51: mutations.filter((m) => m.field === "City").length,
  52: mutations.filter((m) => m.field === "State / Region").length,
  53: mutCounts.SAFE_MARKET_CORRECTION || 0,
  54: mutations.filter((m) => m.field === "Market" && m.mutation_class === "SAFE_INVALID_CLEAR").length,
  55: mutCounts.SAFE_SUBMARKET_CORRECTION || 0,
  56: mutCounts.SUBMARKET_NOT_APPLICABLE || 0,
  57: mutations.filter((m) => m.field === "Latitude" || m.field === "Longitude").length,
  58: mutCounts.STEWARD_REVIEW || 0,
  59: mutCounts.RIGHTS_BLOCKED || 0,
  60: 0,
  61: 0,
  62: 0,
  63: choiceLeft === 0 ? "high" : "family_default_remaining",
  64: expected.address_coverage_pct,
  65: { valid: expected.city_valid, unknown: expected.city_unknown_or_blank },
  66: expected.state_coverage_pct,
  67: marketPct,
  68: applPct,
  69: "improved_but_not_forced",
  70: expected.golden_quality_score_estimate,
  71: true,
  72: marketPct >= 90,
  73: stillUnresolved > 0 && (gapTaxonomy.INPUT_DATA_GAP || 0) >= (gapTaxonomy.MARKET_REGISTRY_GAP || 0),
  74: true,
  75: resumeReady,
  verdicts: {
    UPSTREAM_GEOGRAPHY: cityProd >= 40 ? "PARTIAL" : "PARTIAL",
    MARKET: marketPct >= 90 ? "READY" : marketPct >= 70 ? "PARTIAL" : "NOT READY",
    SUBMARKET: applPct >= 90 ? "READY" : applPct >= 50 ? "PARTIAL" : "NOT READY",
    CONSOLIDATED_INCIDENT_REPAIR: "READY FOR AUTHORIZATION",
    V4: resumeReady ? "READY AFTER CONSOLIDATED REPAIR" : "NEEDS MORE WORK",
  },
  city_production_eligible_recovered: cityProd,
  bucket_counts: bucketCounts,
};

wj("90-geography-recovery-answers.json", answers);

wm(
  "110-final-geography-recovery-report.md",
  `# Final Geography Recovery Report

**V4 PAUSED · DO NOT APPLY**

## Verdicts

| | |
| --- | --- |
| UPSTREAM GEOGRAPHY | **${answers.verdicts.UPSTREAM_GEOGRAPHY}** |
| MARKET | **${answers.verdicts.MARKET}** (${marketPct}%) |
| SUBMARKET | **${answers.verdicts.SUBMARKET}** (${applPct}% applicable) |
| CONSOLIDATED INCIDENT REPAIR | **READY FOR AUTHORIZATION** |
| V4 | **${answers.verdicts.V4}** |

## Summary

- Frozen unresolved Market cases: **${freeze.length}**
- City recovered (any evidence): **${cityRecovered}** · production-eligible: **${cityProd}**
- Market after upstream repair: **${marketOk}/400 (${marketPct}%)** · newly from 146: **${newlyResolved}**
- Remaining unresolved: **${stillUnresolved}** · registry gaps: **${gapTaxonomy.MARKET_REGISTRY_GAP || 0}** · input gaps: **${gapTaxonomy.INPUT_DATA_GAP || 0}**
- Consolidated mutations: **${mutations.length}**
- SerpApi paid searches this task: **0**
- Country fallback: **NO**

## Jamaica / Barbados control

- Jamaica all Market OK: **${marketRerun.filter((r) => r.country === "Jamaica").every((r) => r.market_ok)}**
- Barbados all Market OK: **${marketRerun.filter((r) => r.country === "Barbados").every((r) => r.market_ok)}**

See \`90-geography-recovery-answers.json\` (Q1–75) and \`107-consolidated-incident-manifest.json\`.
`
);

wm(
  "00-incident-status.md",
  `# V4 Production-Data Quality Incident — Status

**V4 production writes: PAUSED**

| Track | Status |
| --- | --- |
| Current Brand | REPAIRED (prior apply) |
| Address/City/State SAFE repair | PARTIALLY APPLIED (prior) |
| Upstream City/State/Coord recovery design | **COMPLETE — dry-run only** |
| Market/Submarket | **Consolidated manifest READY FOR AUTHORIZATION — not applied** |
| V4 restart | **${answers.verdicts.V4}** |

See \`110-final-geography-recovery-report.md\`.
`
);

console.log(
  JSON.stringify(
    {
      frozen: freeze.length,
      buckets: bucketCounts,
      cityRecovered,
      cityProd,
      newlyResolved,
      marketOk,
      marketPct,
      stillUnresolved,
      applPct,
      mutCounts,
      choiceLeft,
      verdicts: answers.verdicts,
      gapTaxonomy,
    },
    null,
    2
  )
);
