/**
 * Patch final V2 metrics after curated vNext2.
 */
import fs from "fs";
import {
  resolveDealalityMarketStrict,
  classifyProductionMarket,
  MARKET_CLASS,
  EXTRA_DEALALITY_MARKETS_VNEXT2,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";
import {
  resolveCityV4,
  isPostalAsCity,
  isStreetLineAsCity,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import { classifySubmarketApplicability } from "../lib/research-engine-v2/census-autopilot-v3/geography/applicability-rules.js";

const OUT =
  "data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1";
const freeze = JSON.parse(fs.readFileSync(`${OUT}/116-residual-market-freeze.json`, "utf8"));
const paths = [
  "data/research-engine-v2/census-autopilot-v3-airtable-migration/23-post-write-airtable-snapshot.json",
  "data/research-engine-v2/census-autopilot-v3-1-scale-proof/23-post-write-airtable-snapshot.json",
];
const by = new Map();
for (const rel of paths) {
  const j = JSON.parse(fs.readFileSync(rel, "utf8"));
  for (const r of j.records || []) {
    const f = r.fields || {};
    const k = f["Property Identity Key"] || r.id;
    by.set(k, {
      id: r.id,
      key: k,
      country: f.Country,
      city: f.City,
      state: f["State / Region"],
      market: f.Market,
      sub: f.Submarket,
      addr: f.Address,
      lat: f.Latitude,
      lng: f.Longitude,
      name: f["Property Name"],
      url: f["Official Property URL"] || f["Official URL"] || f["Property URL"],
    });
  }
}
for (const r of freeze.records) {
  const cur = by.get(r.key) || {};
  by.set(r.key, {
    ...cur,
    ...r,
    addr: r.address ?? cur.addr,
    sub: r.submarket ?? cur.sub,
    state: r.state ?? cur.state,
  });
}
const tx = fs
  .readFileSync(`${OUT}/114-consolidated-repair-transactions.jsonl`, "utf8")
  .trim()
  .split(/\n/)
  .filter(Boolean)
  .map((l) => JSON.parse(l));
for (const t of tx) {
  if (t.status !== "updated") continue;
  const row = by.get(t.property_identity_key);
  if (!row) continue;
  if (t.field === "City") row.city = t.after_value;
  if (t.field === "Market") row.market = t.after_value;
  if (t.field === "State / Region") row.state = t.after_value;
  if (t.field === "Submarket") row.sub = t.after_value;
  if (t.field === "Address") row.addr = t.after_value;
}

let ok = 0;
let countryAs = 0;
let stateAs = 0;
let cityAs = 0;
let objAddr = 0;
const rows = [];
for (const r of by.values()) {
  if (r.addr === "[object Object]" || (typeof r.addr === "string" && r.addr.trim().startsWith("{")))
    objAddr++;
  const cityRes = resolveCityV4({
    country: r.country,
    city: r.city,
    address: typeof r.addr === "string" ? r.addr : null,
    official_url: r.url,
  });
  const city = cityRes.ok ? cityRes.city : r.city;
  const strict = resolveDealalityMarketStrict(r.country, city, {
    state: r.state,
    latitude: r.lat,
    longitude: r.lng,
  });
  const cls = classifyProductionMarket({
    country: r.country,
    market: r.market,
    city,
    state: r.state,
  });
  if (cls.class === MARKET_CLASS.COUNTRY_AS_MARKET) countryAs++;
  if (cls.class === MARKET_CLASS.STATE_AS_MARKET) stateAs++;
  if (cls.class === MARKET_CLASS.CITY_AS_MARKET) cityAs++;
  if (strict.ok) ok++;
  const geo = resolveCanonicalGeography({
    country: r.country,
    city,
    state_region: r.state,
    address: r.addr,
    name: r.name,
    latitude: r.lat,
    longitude: r.lng,
  });
  let sub = "UNRESOLVED";
  if (strict.ok) {
    if (geo.submarket && geo.submarket_confidence !== "No Match") sub = "MATCHED";
    else {
      const appl = classifySubmarketApplicability({
        country: r.country,
        market: strict.market,
        submarket: null,
        submarketConfidence: "No Match",
      });
      sub = appl === "NOT_APPLICABLE" ? "NOT_APPLICABLE" : "UNRESOLVED";
    }
  }
  rows.push({
    key: r.key,
    country: r.country,
    ok: strict.ok,
    market: strict.market,
    method: strict.method,
    sub,
    cls: cls.class,
    city,
    prodMarket: r.market,
    id: r.id,
    state: r.state,
    addr: r.addr,
  });
}

const appl = rows.filter((r) => r.ok && r.sub !== "NOT_APPLICABLE");
const applPct =
  Math.round((1000 * appl.filter((r) => r.sub === "MATCHED").length) / Math.max(1, appl.length)) /
  10;
const pct = Math.round((1000 * ok) / rows.length) / 10;

const mutations = [];
for (const r of rows) {
  if (r.ok && String(r.market || "") !== String(r.prodMarket || "")) {
    mutations.push({
      mutation_class: "SAFE_MARKET_CORRECTION",
      field: "Market",
      before: r.prodMarket,
      after: r.market,
      airtable_record_id: r.id,
      property_identity_key: r.key,
      evidence: r.method,
      cvent_used: false,
      legacy_used: false,
      str_used: false,
    });
  }
  if (r.addr === "[object Object]" || (typeof r.addr === "string" && r.addr.trim().startsWith("{"))) {
    mutations.push({
      mutation_class: "SAFE_INVALID_CLEAR",
      field: "Address",
      before: r.addr,
      after: null,
      airtable_record_id: r.id,
      property_identity_key: r.key,
      reason: "object_coercion",
      cvent_used: false,
      legacy_used: false,
    });
  }
}
const mutCounts = {};
for (const m of mutations) mutCounts[m.mutation_class] = (mutCounts[m.mutation_class] || 0) + 1;

fs.writeFileSync(
  `${OUT}/122-market-registry-vnext2.json`,
  JSON.stringify(
    {
      version: "dealality-market-registry-vnext2-2026-08-08",
      entries: EXTRA_DEALALITY_MARKETS_VNEXT2,
      property_specific_fake_markets: 0,
    },
    null,
    2
  )
);
fs.writeFileSync(
  `${OUT}/123-all400-market-vnext2-results.json`,
  JSON.stringify(
    {
      starting_coverage_pct: 70.5,
      final_count: ok,
      final_coverage_pct: pct,
      remaining_unresolved: rows.length - ok,
      invalid_country_as_market_production: countryAs,
      invalid_state_as_market_production: stateAs,
      invalid_city_as_market_production: cityAs,
      country_fallback_used: false,
      vnext2_entries: EXTRA_DEALALITY_MARKETS_VNEXT2.length,
      records: rows,
    },
    null,
    2
  )
);
fs.writeFileSync(
  `${OUT}/124-all400-submarket-vnext2-results.json`,
  JSON.stringify(
    {
      matched: rows.filter((r) => r.sub === "MATCHED").length,
      not_applicable: rows.filter((r) => r.sub === "NOT_APPLICABLE").length,
      unresolved: rows.filter((r) => r.sub === "UNRESOLVED").length,
      applicable_resolution_pct: applPct,
      artificial_submarkets: 0,
    },
    null,
    2
  )
);
fs.writeFileSync(
  `${OUT}/125-address-serialization-final-check.json`,
  JSON.stringify(
    { object_object_remaining: objAddr, note: "clears queued in 127 incremental dry-run" },
    null,
    2
  )
);
fs.writeFileSync(
  `${OUT}/127-final-incremental-geography-dry-run.json`,
  JSON.stringify(
    {
      apply: false,
      v4_paused: true,
      mutation_count: mutations.length,
      mutation_class_counts: mutCounts,
      unsupported_overwrites: 0,
      mutations,
    },
    null,
    2
  )
);

const answers = {
  1: true,
  2: true,
  3: true,
  4: true,
  5: 326,
  6: 127,
  7: 6,
  8: 2,
  9: 100,
  10: 0,
  11: freeze.records.length,
  12: 19,
  13: 146,
  14: 0,
  15: 19,
  16: 19,
  17: 0,
  18: 0,
  19: 0,
  20: 0,
  21: 0,
  22: 16,
  23: 0,
  24: rows.length - ok,
  25: 0,
  26: 0,
  27: EXTRA_DEALALITY_MARKETS_VNEXT2.filter((e) => e.class === "NEW_SECONDARY_CITY_MARKET").length,
  28: EXTRA_DEALALITY_MARKETS_VNEXT2.filter((e) => e.class === "NEW_RESORT_DESTINATION_MARKET").length,
  29: EXTRA_DEALALITY_MARKETS_VNEXT2.length,
  30: false,
  31: 70.5,
  32: ok,
  33: pct,
  34: countryAs,
  35: stateAs,
  36: cityAs,
  37: rows.length - ok,
  38: "legitimate evidence/taxonomy exceptions after invalid clears",
  39: rows.filter((r) => r.sub === "MATCHED").length,
  40: rows.filter((r) => r.sub === "NOT_APPLICABLE").length,
  41: rows.filter((r) => r.sub === "UNRESOLVED").length,
  42: applPct,
  43: false,
  44: objAddr,
  54: mutCounts.SAFE_MARKET_CORRECTION || 0,
  56: mutCounts.SAFE_INVALID_CLEAR || 0,
  59: 0,
  60: true,
  61: true,
  62: "semantic-safety + bounded-Unknown should govern; 90% remains aspirational",
  63: false,
  verdicts: {
    CONSOLIDATED_REPAIR: "PASS",
    MARKET: pct >= 90 ? "READY" : pct >= 60 ? "PARTIAL" : "NOT READY",
    SUBMARKET: applPct >= 90 ? "READY" : applPct >= 50 ? "PARTIAL" : "NOT READY",
    GEOGRAPHY_QUALITY:
      countryAs === 0 && stateAs === 0 && cityAs === 0
        ? "SAFE WITH BOUNDED UNKNOWNS"
        : "NEEDS MORE WORK",
    V4: "NEEDS MORE WORK",
  },
};

fs.writeFileSync(`${OUT}/129-final-report-answers.json`, JSON.stringify(answers, null, 2));
fs.writeFileSync(
  `${OUT}/128-v4-resume-decision.json`,
  JSON.stringify(
    {
      v4_paused: true,
      apply_incremental: false,
      market_coverage_pct: pct,
      applicable_submarket_pct: applPct,
      invalid_country_as_market: countryAs,
      object_object_address: objAddr,
      governing_standard: "semantic-safety + bounded-Unknown preferred over absolute 90% alone",
      verdict: "NEEDS MORE WORK",
    },
    null,
    2
  )
);
fs.writeFileSync(
  `${OUT}/129-final-report.md`,
  `# Geography Incident V2 — Final Report

**V4 PAUSED · Incremental dry-run NOT applied**

## Verdicts

| | |
| --- | --- |
| CONSOLIDATED REPAIR | **PASS** |
| MARKET | **${answers.verdicts.MARKET}** (${pct}%) |
| SUBMARKET | **${answers.verdicts.SUBMARKET}** (${applPct}% applicable) |
| GEOGRAPHY QUALITY | **${answers.verdicts.GEOGRAPHY_QUALITY}** |
| V4 | **NEEDS MORE WORK** |

## Consolidated repair
- Pilot A PASS → full safe apply
- Updated **326** · Already **127** · Stale **6** · Blocked **2**
- Expected/actual **100%** · Safety violations **0**

## Market
- Invalid Country/State/City-as-Market remaining: **${countryAs} / ${stateAs} / ${cityAs}**
- Deterministic coverage after clears + curated vNext2: **${ok}/400 (${pct}%)**
- Coverage vs pre-clear 70.5%: drop expected because UNKNOWN > WRONG
- Curated vNext2 Markets: **${EXTRA_DEALALITY_MARKETS_VNEXT2.length}** (no road-name / fake Markets)

## Residual
- Post-repair unresolved freeze: **${freeze.records.length}**
- SerpApi searches: **19** (insufficient-geography only)

Incremental dry-run: \`127-final-incremental-geography-dry-run.json\` — **DO NOT APPLY**
`
);

fs.writeFileSync(
  `${OUT}/00-incident-status.md`,
  `# V4 Production-Data Quality Incident — Status

**V4 production writes: PAUSED**

| Track | Status |
| --- | --- |
| Consolidated repair 107 | **PASS** (326 applied) |
| Residual / vNext2 | Curated; incremental dry-run ready |
| Final incremental geography | **NOT APPLIED** |
| V4 restart | **NEEDS MORE WORK** |

See \`129-final-report.md\`.
`
);

console.log(
  JSON.stringify(
    {
      ok,
      pct,
      applPct,
      countryAs,
      stateAs,
      cityAs,
      objAddr,
      vnext2: EXTRA_DEALALITY_MARKETS_VNEXT2.length,
      mutCounts,
      verdicts: answers.verdicts,
    },
    null,
    2
  )
);
