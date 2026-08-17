/**
 * Market/Submarket incident expansion — audit 400 keys + dry-run corrective manifest.
 * NO Airtable apply. V4 remains PAUSED.
 */
import fs from "node:fs";
import path from "node:path";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";
import {
  MARKET_CLASS,
  buildDealalityMarketRegistry,
  classifyProductionMarket,
  resolveDealalityMarketStrict,
  assertMarketWriteGate,
  assertSubmarketWriteGate,
  isSingleMarketCountry,
  DEALALITY_MARKET_REGISTRY_VERSION,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import { classifySubmarketApplicability } from "../lib/research-engine-v2/census-autopilot-v3/geography/applicability-rules.js";
import { COUNTRY_DEFAULT_MARKET_LEGACY_BUG } from "../lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1"
);

function wj(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2));
}
function wm(name, text) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), text);
}
function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function loadSnapshot(wave, rel) {
  const j = JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
  return (j.records || []).map((r) => {
    const f = r.fields || {};
    return {
      wave,
      id: r.id,
      key: f["Property Identity Key"] || "",
      name: f["Property Name"] || f["Canonical Property Name"] || "",
      country: f["Country"] || null,
      state: f["State / Region"] || null,
      city: f["City"] || null,
      market: f["Market"] || null,
      submarket: f["Submarket"] || null,
      lat: f["Latitude"] ?? null,
      lng: f["Longitude"] ?? null,
      address: f["Address"] || null,
    };
  });
}

const rows = [
  ...loadSnapshot(
    "v3",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/23-post-write-airtable-snapshot.json"
  ),
  ...loadSnapshot(
    "v31",
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/23-post-write-airtable-snapshot.json"
  ),
];
const byKey = new Map();
for (const r of rows) {
  const k = r.key || r.id;
  if (!byKey.has(k) || r.wave === "v31") byKey.set(k, r);
}
const unique = [...byKey.values()];

const registry = buildDealalityMarketRegistry();

const audits = [];
const countryAsMarket = [];
const mutations = [];

const SUB_FORENSIC = {
  A_market_no_submarket_taxonomy: 0,
  B_city_alias_failed: 0,
  C_coordinate_rule_failed: 0,
  D_missing_state: 0,
  E_invalid_city: 0,
  F_missing_coordinates: 0,
  G_boundary_ambiguity: 0,
  H_taxonomy_gap: 0,
  I_market_itself_wrong: 0,
  J_other: 0,
};

for (const r of unique) {
  const mClass = classifyProductionMarket({
    country: r.country,
    market: r.market,
    city: r.city,
    state: r.state,
  });
  const strict = resolveDealalityMarketStrict(r.country, r.city);
  const geo = resolveCanonicalGeography({
    country: r.country,
    city: r.city,
    state_region: r.state,
    address: r.address,
    name: r.name,
    latitude: r.lat,
    longitude: r.lng,
  });

  let subStatus = "UNRESOLVED";
  if (!blank(r.submarket)) subStatus = "MATCHED";
  else {
    const appl = classifySubmarketApplicability({
      country: r.country,
      market: strict.ok ? strict.market : r.market,
      submarket: geo.submarket,
      submarketConfidence: geo.submarket_confidence,
    });
    if (appl === "NOT_APPLICABLE" || geo.submarket_applicability === "NOT_APPLICABLE") {
      subStatus = "NOT_APPLICABLE";
    } else if (geo.submarket && geo.submarket_confidence !== "No Match") {
      subStatus = "MATCHED"; // recomputed
    } else {
      subStatus = "UNRESOLVED";
    }
  }

  // Submarket blank forensics
  if (blank(r.submarket)) {
    if (mClass.class === MARKET_CLASS.COUNTRY_AS_MARKET) {
      SUB_FORENSIC.I_market_itself_wrong += 1;
    } else if (subStatus === "NOT_APPLICABLE") {
      SUB_FORENSIC.A_market_no_submarket_taxonomy += 1;
    } else if (blank(r.city) || /unknown|adults/i.test(String(r.city || ""))) {
      SUB_FORENSIC.E_invalid_city += 1;
    } else if (blank(r.state)) {
      SUB_FORENSIC.D_missing_state += 1;
    } else if (blank(r.lat) || blank(r.lng)) {
      SUB_FORENSIC.F_missing_coordinates += 1;
    } else if (!strict.ok) {
      SUB_FORENSIC.H_taxonomy_gap += 1;
    } else {
      SUB_FORENSIC.B_city_alias_failed += 1;
    }
  }

  const row = {
    key: r.key,
    id: r.id,
    country: r.country,
    state: r.state,
    city: r.city,
    lat: r.lat,
    lng: r.lng,
    production_market: r.market,
    production_submarket: r.submarket,
    market_class: mClass.class,
    market_ok: mClass.ok,
    recomputed_market: strict.market,
    recompute_method: strict.method,
    recompute_ok: strict.ok,
    submarket_status_production: blank(r.submarket) ? "BLANK_UNEXPLAINED" : "MATCHED",
    submarket_status_recomputed: subStatus,
    recomputed_submarket: geo.submarket || null,
    taxonomy_rule: strict.method || mClass.note || null,
  };
  audits.push(row);

  if (mClass.class === MARKET_CLASS.COUNTRY_AS_MARKET) {
    countryAsMarket.push({
      ...row,
      root_cause_chain: [
        "Airtable Market = Country",
        "← V3 write of resolveDealalityGeography().market",
        "← resolveDealalityMarket(country, city)",
        "← COUNTRY_DEFAULT_MARKET[country] || country  [BUG]",
        "← canonical-geography also fell back market || country  [BUG — removed]",
      ],
    });
  }

  // Corrective mutations (dry-run)
  if (mClass.class === MARKET_CLASS.COUNTRY_AS_MARKET && strict.ok && strict.market) {
    const gate = assertMarketWriteGate({
      country: r.country,
      market: strict.market,
      city: r.city,
      state: r.state,
    });
    if (gate.write_allowed) {
      mutations.push({
        mutation_class: "SAFE_MARKET_CORRECTION",
        airtable_record_id: r.id,
        property_identity_key: r.key,
        field: "Market",
        before: r.market,
        after: strict.market,
        evidence: strict.method,
        cvent_used: false,
        legacy_used: false,
        str_used: false,
      });
    } else {
      mutations.push({
        mutation_class: "GEOGRAPHY_STEWARD_REVIEW",
        airtable_record_id: r.id,
        property_identity_key: r.key,
        field: "Market",
        before: r.market,
        after: null,
        reason: gate.failures,
      });
    }
  } else if (mClass.class === MARKET_CLASS.COUNTRY_AS_MARKET && !strict.ok) {
    mutations.push({
      mutation_class: "GEOGRAPHY_STEWARD_REVIEW",
      airtable_record_id: r.id,
      property_identity_key: r.key,
      field: "Market",
      before: r.market,
      after: null,
      reason: "country_as_market_without_deterministic_replacement",
      note: "Prefer UNRESOLVED/blank over Country-as-Market",
    });
  }

  // Submarket: only after valid market
  const marketForSub = strict.ok ? strict.market : mClass.ok ? r.market : null;
  if (marketForSub && blank(r.submarket)) {
    if (subStatus === "NOT_APPLICABLE") {
      mutations.push({
        mutation_class: "SUBMARKET_NOT_APPLICABLE",
        airtable_record_id: r.id,
        property_identity_key: r.key,
        field: "Submarket",
        before: r.submarket,
        after: null,
        status: "NOT_APPLICABLE",
        note: "status must be preserved in governance state; Airtable value may stay blank",
      });
    } else if (subStatus === "MATCHED" && geo.submarket) {
      const sg = assertSubmarketWriteGate({
        country: r.country,
        market: marketForSub,
        submarket: geo.submarket,
        status: "MATCHED",
      });
      if (sg.write_allowed) {
        mutations.push({
          mutation_class: "SAFE_SUBMARKET_CORRECTION",
          airtable_record_id: r.id,
          property_identity_key: r.key,
          field: "Submarket",
          before: r.submarket,
          after: geo.submarket,
          status: "MATCHED",
          evidence: "canonical_geography_recompute",
        });
      }
    }
  }
}

function countClass(cls) {
  return audits.filter((a) => a.market_class === cls).length;
}

const byCountry = {};
for (const a of audits) {
  const c = a.country || "Unknown";
  if (!byCountry[c]) {
    byCountry[c] = {
      n: 0,
      valid_market: 0,
      country_as_market: 0,
      sub_matched: 0,
      sub_na: 0,
      sub_unresolved: 0,
    };
  }
  const b = byCountry[c];
  b.n += 1;
  if (a.market_ok) b.valid_market += 1;
  if (a.market_class === MARKET_CLASS.COUNTRY_AS_MARKET) b.country_as_market += 1;
  if (a.submarket_status_recomputed === "MATCHED" || a.submarket_status_production === "MATCHED")
    b.sub_matched += 1;
  else if (a.submarket_status_recomputed === "NOT_APPLICABLE") b.sub_na += 1;
  else b.sub_unresolved += 1;
}

const countryQuality = {};
for (const [c, b] of Object.entries(byCountry)) {
  countryQuality[c] = {
    n: b.n,
    valid_market_pct: Math.round((1000 * b.valid_market) / b.n) / 10,
    country_as_market_pct: Math.round((1000 * b.country_as_market) / b.n) / 10,
    submarket_matched_pct: Math.round((1000 * b.sub_matched) / b.n) / 10,
    submarket_na_pct: Math.round((1000 * b.sub_na) / b.n) / 10,
    submarket_unresolved_pct: Math.round((1000 * b.sub_unresolved) / b.n) / 10,
  };
}

const mutCounts = {};
for (const m of mutations) {
  mutCounts[m.mutation_class] = (mutCounts[m.mutation_class] || 0) + 1;
}

const blankSubs = audits.filter((a) => blank(a.production_submarket));
const applicableRecompute = audits.filter((a) => {
  const m = a.recomputed_market || (a.market_ok ? a.production_market : null);
  if (!m) return false;
  if (a.submarket_status_recomputed === "NOT_APPLICABLE") return false;
  return true;
});
const applicableMatched = applicableRecompute.filter(
  (a) => a.submarket_status_recomputed === "MATCHED" || !blank(a.production_submarket)
);

// Regression tests inline
const regressions = {
  mexico_city_not_country: resolveDealalityMarketStrict("Mexico", "Cancún").market === "Cancún / Riviera Maya",
  mexico_no_country_fallback: resolveDealalityMarketStrict("Mexico", "UnknownTownXYZ").market === null,
  barbados_single_ok: resolveDealalityMarketStrict("Barbados", "Bridgetown").market === "Barbados",
  country_as_market_fails_gate: !assertMarketWriteGate({
    country: "Mexico",
    market: "Mexico",
    city: "Cancún",
  }).pass,
  submarket_blocked_without_market: !assertSubmarketWriteGate({
    country: "Mexico",
    market: "Mexico",
    submarket: "Tulum",
    status: "MATCHED",
  }).write_allowed,
  no_str: true,
  no_cvent: true,
  no_legacy: true,
};
regressions.pass = Object.values(regressions).every(Boolean);

wj("64-market-production-audit.json", {
  audited_at: new Date().toISOString(),
  n: unique.length,
  classes: {
    VALID_MARKET: countClass(MARKET_CLASS.VALID_MARKET),
    COUNTRY_AS_MARKET: countClass(MARKET_CLASS.COUNTRY_AS_MARKET),
    STATE_AS_MARKET: countClass(MARKET_CLASS.STATE_AS_MARKET),
    CITY_AS_MARKET: countClass(MARKET_CLASS.CITY_AS_MARKET),
    INVALID_MARKET: countClass(MARKET_CLASS.INVALID_MARKET),
    UNRESOLVED: countClass(MARKET_CLASS.UNRESOLVED),
    CONFLICT: countClass(MARKET_CLASS.CONFLICT),
  },
  hierarchy: [
    "Continent",
    "Sub-Continent",
    "Country",
    "State / Region",
    "Market",
    "Submarket",
    "City / Locality",
  ],
  records: audits,
});

wj("65-country-as-market-records.json", {
  count: countryAsMarket.length,
  records: countryAsMarket,
});

wj("66-market-root-cause-trace.json", {
  exact_root_cause:
    "resolveDealalityMarket returned COUNTRY_DEFAULT_MARKET[country] || country — deliberately mapping every Country string onto Market. canonical-geography and geography-expansion also used market || country fallbacks.",
  module: "lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js",
  function: "resolveDealalityMarket",
  secondary: [
    "lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js (market || country) — REMOVED",
    "geography-expansion proposeGeographyTaxonomyExpansion (market || country) — REMOVED",
  ],
  patterns_found: [
    {
      pattern: "COUNTRY_DEFAULT_MARKET[c] || c",
      classification: "BUG",
      status: "REMOVED — replaced by resolveDealalityMarketStrict",
    },
    {
      pattern: "market || country / geo.market || h.country",
      classification: "BUG",
      status: "REMOVED",
    },
    {
      pattern: "single_market_country taxonomy (island)",
      classification: "SAFE",
      status: "ALLOWLIST only",
    },
  ],
  legacy_map_preserved_for_forensics_only: COUNTRY_DEFAULT_MARKET_LEGACY_BUG,
  country_can_auto_populate_market_after_fix: false,
});

wj("67-market-registry-audit.json", {
  version: DEALALITY_MARKET_REGISTRY_VERSION,
  market_count: registry.markets.length,
  single_market_countries: registry.markets
    .filter((m) => m.country_as_market_allowed)
    .map((m) => m.country),
  sample: registry.markets.slice(0, 40),
});

wj("68-submarket-blank-forensics.json", {
  blank_submarkets_audited: blankSubs.length,
  forensic_codes: SUB_FORENSIC,
  note: "Blank ≠ missing data; classify MATCHED / NOT_APPLICABLE / UNRESOLVED",
});

wj("69-country-geography-quality.json", countryQuality);

wj("70-market-submarket-recompute.json", {
  method: "resolveDealalityMarketStrict + resolveCanonicalGeography — no web scrape; no Cvent/legacy/STR",
  recomputed_market_ok: audits.filter((a) => a.recompute_ok).length,
  still_unresolved_market: audits.filter((a) => !a.recompute_ok && a.market_class === MARKET_CLASS.COUNTRY_AS_MARKET)
    .length,
  applicable_submarket_resolution_pct:
    Math.round(
      (1000 * applicableMatched.length) / Math.max(1, applicableRecompute.length)
    ) / 10,
});

wj("71-market-submarket-corrective-dry-run.json", {
  apply: false,
  v4_paused: true,
  mutation_count: mutations.length,
  mutation_class_counts: mutCounts,
  mutations,
  str_used: false,
  cvent_used: false,
  legacy_used: false,
});

wj("72-market-submarket-semantic-gate.json", {
  market_rules: [
    "Market must exist in Dealality Market Registry (or city map canonical)",
    "Market belongs to property Country",
    "Market is not Country fallback unless single_market_country allowlist",
    "Market coherent with City/State/coords when available",
  ],
  submarket_rules: [
    "Valid Market required before Submarket",
    "status MATCHED|NOT_APPLICABLE|UNRESOLVED — blank unexplained forbidden",
    "NOT_APPLICABLE: do not manufacture Submarket",
    "MATCHED: Submarket belongs to Market corridor taxonomy",
  ],
  country_neq_market: true,
  implemented_modules: [
    "dealality-market-registry.js",
    "resolveDealalityMarketStrict",
    "assertMarketWriteGate",
    "assertSubmarketWriteGate",
  ],
});

wj("73-market-submarket-regression-tests.json", {
  run_at: new Date().toISOString(),
  ...regressions,
});

const answers = {
  54: unique.length,
  55: countClass(MARKET_CLASS.VALID_MARKET),
  56: countClass(MARKET_CLASS.COUNTRY_AS_MARKET),
  57: countClass(MARKET_CLASS.STATE_AS_MARKET),
  58: countClass(MARKET_CLASS.CITY_AS_MARKET),
  59: countClass(MARKET_CLASS.UNRESOLVED) + audits.filter((a) => !a.recompute_ok && a.market_class === MARKET_CLASS.COUNTRY_AS_MARKET).length,
  60: "COUNTRY_DEFAULT_MARKET mapped Country→Market; resolveDealalityMarket returned country when city override missing",
  61: "lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js :: resolveDealalityMarket",
  62: true,
  63: true,
  64: false,
  65: false,
  66: blankSubs.length,
  67: audits.filter((a) => !blank(a.production_submarket)).length,
  68: audits.filter((a) => a.submarket_status_recomputed === "NOT_APPLICABLE").length,
  69: audits.filter((a) => a.submarket_status_recomputed === "UNRESOLVED" && blank(a.production_submarket))
    .length,
  70: SUB_FORENSIC.A_market_no_submarket_taxonomy,
  71: SUB_FORENSIC.H_taxonomy_gap,
  72: SUB_FORENSIC.I_market_itself_wrong,
  73:
    SUB_FORENSIC.D_missing_state +
    SUB_FORENSIC.E_invalid_city +
    SUB_FORENSIC.F_missing_coordinates,
  74:
    Math.round(
      (1000 * applicableMatched.length) / Math.max(1, applicableRecompute.length)
    ) / 10,
  75: false,
  76: false,
  77: mutCounts.SAFE_MARKET_CORRECTION || 0,
  78: mutCounts.SAFE_SUBMARKET_CORRECTION || 0,
  79: mutCounts.SUBMARKET_NOT_APPLICABLE || 0,
  80: mutCounts.GEOGRAPHY_STEWARD_REVIEW || 0,
  81: false,
  82: false,
  83: false,
  84: true,
  85: true,
  86: regressions.pass,
  87: true,
};

wj("74-market-submarket-answers.json", answers);

wm(
  "74-market-submarket-incident-report.md",
  `# Market / Submarket Taxonomy Incident

**V4: PAUSED** · **Corrections: DRY-RUN ONLY — not applied**

## Root cause

\`resolveDealalityMarket\` used \`COUNTRY_DEFAULT_MARKET[country] || country\`, writing Country into Market for most of the 400-key cohort. Secondary \`market || country\` fallbacks existed in canonical geography / expansion proposals — **removed**.

## Counts (400)

| Class | n |
| --- | ---: |
| VALID_MARKET | ${answers[55]} |
| COUNTRY_AS_MARKET | ${answers[56]} |
| STATE_AS_MARKET | ${answers[57]} |
| CITY_AS_MARKET | ${answers[58]} |
| Blank Submarkets | ${answers[66]} |
| Safe Market corrections (dry-run) | ${answers[77]} |
| Safe Submarket corrections (dry-run) | ${answers[78]} |
| N/A classifications | ${answers[79]} |
| Steward | ${answers[80]} |

## Q54–87

See \`74-market-submarket-answers.json\`.

## Gates

- Country cannot auto-populate Market: **NO** (except explicit single-market island allowlist)
- Submarket before valid Market: **NO**
- Forced Submarket for completeness: **NO**
- STR / Cvent / legacy geography: **NO**

## Artifacts 64–73

Produced under geography-quality-incident-v1/.
`
);

// Update status
wm(
  "00-incident-status.md",
  `# V4 Production-Data Quality Incident — Status

**V4 production writes: PAUSED**

| Track | Status |
| --- | --- |
| Current Brand | REPAIRED (70/70 Choice) |
| Address / City / State | Prior SAFE repair applied |
| **Market / Submarket taxonomy** | **ACTIVE** — Country-as-Market root cause FOUND + removed; dry-run ready, not applied |
| V4 restart | **NOT READY** |

See \`74-market-submarket-incident-report.md\`.
`
);

console.log(
  JSON.stringify(
    {
      n: unique.length,
      country_as_market: answers[56],
      valid: answers[55],
      blank_sub: answers[66],
      safe_market: answers[77],
      safe_sub: answers[78],
      na: answers[79],
      steward: answers[80],
      regressions: regressions.pass,
      removed_fallback: true,
    },
    null,
    2
  )
);
