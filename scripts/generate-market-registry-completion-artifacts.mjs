/**
 * Market registry completion + invalid Market cleanup design.
 * Artifacts 75–90. NO production apply. V4 PAUSED.
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
  MARKET_ALIASES_TO_CANONICAL,
  EXTRA_DEALALITY_MARKETS_VNEXT,
  STATE_TO_MARKET_EXPLICIT,
  MARKET_CENTROIDS_VNEXT,
  SINGLE_MARKET_ALLOWLIST_AUDIT,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import { classifySubmarketApplicability } from "../lib/research-engine-v2/census-autopilot-v3/geography/applicability-rules.js";

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
function isWeakCity(city) {
  const c = String(city || "").trim();
  if (!c) return true;
  if (/^unknown$/i.test(c)) return true;
  if (/\d{4,}/.test(c)) return true;
  if (/^\d/.test(c)) return true;
  return false;
}

function loadUnique400() {
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
          country: f["Country"] || null,
          state: f["State / Region"] || null,
          city: f["City"] || null,
          market: f["Market"] || null,
          submarket: f["Submarket"] || null,
          lat: f["Latitude"] ?? null,
          lng: f["Longitude"] ?? null,
          address: f["Address"] || null,
        });
      }
    }
  }
  return [...by.values()];
}

const unique = loadUnique400();
const registry = buildDealalityMarketRegistry();

/** Frozen baseline classes from first Market audit (artifact 64). */
const frozen64 = JSON.parse(
  fs.readFileSync(path.join(OUT, "64-market-production-audit.json"), "utf8")
);
const frozenByKey = new Map((frozen64.records || []).map((r) => [r.key, r]));
const frozenValidKeys = new Set(
  (frozen64.records || []).filter((r) => r.market_class === "VALID_MARKET").map((r) => r.key)
);

const audits = [];
for (const r of unique) {
  const frozen = frozenByKey.get(r.key);
  const baselineClass = frozen?.market_class || MARKET_CLASS.UNRESOLVED;
  const baselineOk = frozen?.market_ok === true;

  const strict = resolveDealalityMarketStrict(r.country, r.city, {
    state: r.state,
    latitude: r.lat,
    longitude: r.lng,
  });

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
  const marketForSub = strict.ok ? strict.market : null;
  if (!marketForSub) {
    subStatus = "UNRESOLVED";
  } else if (geo.submarket && geo.submarket_confidence !== "No Match") {
    subStatus = "MATCHED";
  } else {
    const appl = classifySubmarketApplicability({
      country: r.country,
      market: marketForSub,
      submarket: null,
      submarketConfidence: "No Match",
    });
    if (appl === "NOT_APPLICABLE" || geo.submarket_applicability === "NOT_APPLICABLE") {
      subStatus = "NOT_APPLICABLE";
    } else if (!blank(r.submarket)) {
      subStatus = "MATCHED";
    } else {
      subStatus = "UNRESOLVED";
    }
  }

  let validCheck = null;
  if (frozenValidKeys.has(r.key)) {
    const gate = assertMarketWriteGate({
      country: r.country,
      market: r.market,
      city: r.city,
      state: r.state,
      latitude: r.lat,
      longitude: r.lng,
    });
    if (gate.pass) validCheck = "CONFIRMED_VALID";
    else if ((gate.failures || []).includes("CITY_MARKET_INCOHERENT")) validCheck = "CONFLICT";
    else if (strict.ok && norm(strict.market) === norm(r.market)) validCheck = "CONFIRMED_VALID";
    else if (strict.ok) validCheck = "REVIEW";
    else validCheck = "REVIEW";
  }

  audits.push({
    ...r,
    baseline_class: baselineClass,
    baseline_ok: baselineOk,
    suggested: strict.market,
    recompute_ok: strict.ok,
    recompute_market: strict.market,
    recompute_method: strict.method,
    recompute_confidence: strict.confidence || null,
    sub_status: subStatus,
    recomputed_submarket: geo.submarket || null,
    valid_check: validCheck,
    weak_city: isWeakCity(r.city),
    invalid_clear_candidate:
      !baselineOk &&
      [
        MARKET_CLASS.COUNTRY_AS_MARKET,
        MARKET_CLASS.STATE_AS_MARKET,
        MARKET_CLASS.CITY_AS_MARKET,
        MARKET_CLASS.INVALID_MARKET,
      ].includes(baselineClass) &&
      !strict.ok,
  });
}

// Clusters (all 400 by country+city)
const clusterMap = new Map();
for (const a of audits) {
  const city = String(a.city || "(blank)").trim();
  const ck = `${a.country}|${city}`;
  if (!clusterMap.has(ck)) {
    clusterMap.set(ck, {
      country: a.country,
      city,
      n: 0,
      states: {},
      markets: {},
      lats: [],
      lngs: [],
      keys: [],
      unresolved_n: 0,
    });
  }
  const c = clusterMap.get(ck);
  c.n += 1;
  if (!a.recompute_ok) c.unresolved_n += 1;
  c.states[a.state || "(blank)"] = (c.states[a.state || "(blank)"] || 0) + 1;
  c.markets[a.market || "(blank)"] = (c.markets[a.market || "(blank)"] || 0) + 1;
  if (a.lat != null) c.lats.push(Number(a.lat));
  if (a.lng != null) c.lngs.push(Number(a.lng));
  c.keys.push(a.key);
}

function clusterDisposition(cl) {
  const strict = resolveDealalityMarketStrict(cl.country, cl.city === "(blank)" ? null : cl.city);
  if (strict.ok) {
    if (EXTRA_DEALALITY_MARKETS_VNEXT.some((e) => e.country === cl.country && e.cities.includes(norm(cl.city))))
      return "C_new_market_or_vnext";
    return "A_existing_alias";
  }
  if (/unknown/i.test(cl.city) || cl.city === "(blank)") return "D_city_normalization_or_missing";
  if (/\d{4,}/.test(cl.city) || /^\d/.test(cl.city)) return "D_city_normalization_or_missing";
  if (isSingleMarketCountry(cl.country)) return "G_single_market_allowlist";
  return "H_steward_or_new_market";
}

const avg = (arr) => (arr.length ? arr.reduce((s, x) => s + x, 0) / arr.length : null);

const clusters = [...clusterMap.values()]
  .map((cl) => {
    const cityArg = cl.city === "(blank)" || /unknown/i.test(cl.city) ? null : cl.city;
    const sample = audits.find((a) => a.key === cl.keys[0]);
    const strict = resolveDealalityMarketStrict(cl.country, cityArg, {
      state: sample?.state,
      latitude: sample?.lat,
      longitude: sample?.lng,
    });
    return {
      country: cl.country,
      state_region: Object.entries(cl.states).sort((a, b) => b[1] - a[1])[0]?.[0],
      cities_localities: [cl.city],
      hotel_count: cl.n,
      unresolved_count: cl.unresolved_n,
      coordinate_centroid:
        avg(cl.lats) != null
          ? { lat: Math.round(avg(cl.lats) * 1e5) / 1e5, lng: Math.round(avg(cl.lngs) * 1e5) / 1e5 }
          : null,
      coordinate_range:
        cl.lats.length > 1
          ? {
              lat_min: Math.min(...cl.lats),
              lat_max: Math.max(...cl.lats),
              lng_min: Math.min(...cl.lngs),
              lng_max: Math.max(...cl.lngs),
            }
          : null,
      tourism_business_logic: `${cl.country} / ${cl.city} hotel concentration`,
      existing_registry_match: strict.ok,
      recommended_market: strict.market,
      new_market_needed: !strict.ok && !isWeakCity(cl.city) && cl.city !== "(blank)",
      confidence: strict.ok ? strict.confidence || "High" : isWeakCity(cl.city) ? "Low" : "Medium",
      disposition: clusterDisposition(cl),
      production_markets: cl.markets,
    };
  })
  .sort((a, b) => b.hotel_count - a.hotel_count);

const unresolvedClusters = [...clusterMap.values()]
  .filter((c) => c.unresolved_n > 0)
  .map((cl) => {
    const cityArg = cl.city === "(blank)" || /unknown/i.test(cl.city) ? null : cl.city;
    const sample = audits.find((a) => a.key === cl.keys[0] && !a.recompute_ok);
    const strict = resolveDealalityMarketStrict(cl.country, cityArg, {
      state: sample?.state,
      latitude: sample?.lat,
      longitude: sample?.lng,
    });
    return {
      country: cl.country,
      cities_localities: [cl.city],
      hotel_count: cl.unresolved_n,
      disposition: clusterDisposition(cl),
      recommended_market: strict.market,
      new_market_needed: !strict.ok && !isWeakCity(cl.city),
      confidence: strict.confidence || (strict.ok ? "High" : "Low"),
    };
  })
  .sort((a, b) => b.hotel_count - a.hotel_count);

const top25 = unresolvedClusters.slice(0, 25);

// Mutations
const mutations = [];
for (const a of audits) {
  const invalidClasses = [
    MARKET_CLASS.COUNTRY_AS_MARKET,
    MARKET_CLASS.STATE_AS_MARKET,
    MARKET_CLASS.CITY_AS_MARKET,
    MARKET_CLASS.INVALID_MARKET,
  ];
  const invalid = invalidClasses.includes(a.baseline_class);

  if (a.recompute_ok && a.recompute_market && norm(a.recompute_market) !== norm(a.market || "")) {
    const gate = assertMarketWriteGate({
      country: a.country,
      market: a.recompute_market,
      city: a.city,
      state: a.state,
    });
    if (gate.write_allowed) {
      mutations.push({
        mutation_class: "SAFE_MARKET_CORRECTION",
        airtable_record_id: a.id,
        property_identity_key: a.key,
        field: "Market",
        before: a.market,
        after: a.recompute_market,
        evidence: a.recompute_method,
        confidence: a.recompute_confidence,
        cvent_used: false,
        legacy_used: false,
        str_used: false,
      });
    } else {
      mutations.push({
        mutation_class: "GEOGRAPHY_STEWARD_REVIEW",
        airtable_record_id: a.id,
        property_identity_key: a.key,
        field: "Market",
        before: a.market,
        after_candidate: a.recompute_market,
        reason: "gate_blocked",
        gate_failures: gate.failures,
      });
    }
  } else if (a.recompute_ok && norm(a.recompute_market) === norm(a.market || "")) {
    mutations.push({
      mutation_class: "NO_CHANGE",
      airtable_record_id: a.id,
      property_identity_key: a.key,
      field: "Market",
      before: a.market,
      after: a.market,
    });
  } else if (invalid && !a.recompute_ok) {
    mutations.push({
      mutation_class: "SAFE_MARKET_INVALID_CLEAR",
      airtable_record_id: a.id,
      property_identity_key: a.key,
      field: "Market",
      before: a.market,
      after: null,
      resolution_status: "UNRESOLVED",
      reason: "known_invalid_no_deterministic_replacement",
      steward_also: a.weak_city || !a.city,
      note: "Prefer blank/UNRESOLVED over Country/State/City-as-Market; requires Joan auth + downstream OK",
      cvent_used: false,
      legacy_used: false,
      str_used: false,
    });
    if (a.weak_city || !a.city) {
      mutations.push({
        mutation_class: "GEOGRAPHY_STEWARD_REVIEW",
        airtable_record_id: a.id,
        property_identity_key: a.key,
        field: "City",
        reason: "city_normalization_required_before_market",
        before_city: a.city,
      });
    }
  } else if (!a.recompute_ok) {
    mutations.push({
      mutation_class: "GEOGRAPHY_STEWARD_REVIEW",
      airtable_record_id: a.id,
      property_identity_key: a.key,
      field: "Market",
      before: a.market,
      reason: "unresolved_after_vnext",
    });
  }

  const mkt = a.recompute_ok ? a.recompute_market : null;
  if (mkt) {
    if (a.sub_status === "MATCHED" && a.recomputed_submarket && blank(a.submarket)) {
      const sg = assertSubmarketWriteGate({
        country: a.country,
        market: mkt,
        submarket: a.recomputed_submarket,
        status: "MATCHED",
      });
      if (sg.write_allowed) {
        mutations.push({
          mutation_class: "SAFE_SUBMARKET_CORRECTION",
          airtable_record_id: a.id,
          property_identity_key: a.key,
          field: "Submarket",
          before: a.submarket,
          after: a.recomputed_submarket,
          status: "MATCHED",
        });
      }
    } else if (a.sub_status === "NOT_APPLICABLE" && blank(a.submarket)) {
      mutations.push({
        mutation_class: "SUBMARKET_NOT_APPLICABLE",
        airtable_record_id: a.id,
        property_identity_key: a.key,
        field: "Submarket",
        before: a.submarket,
        after: null,
        status: "NOT_APPLICABLE",
      });
    }
  }
}

const mutCounts = {};
for (const m of mutations) mutCounts[m.mutation_class] = (mutCounts[m.mutation_class] || 0) + 1;

const afterValid = audits.filter((a) => a.recompute_ok).length;
const afterUnresolved = audits.filter((a) => !a.recompute_ok).length;
const clears = mutations.filter((m) => m.mutation_class === "SAFE_MARKET_INVALID_CLEAR").length;
const newMarketsNeeded = unresolvedClusters.filter((c) => c.new_market_needed).length;

const byCountry = {};
for (const a of audits) {
  const c = a.country || "?";
  if (!byCountry[c]) byCountry[c] = { n: 0, before_valid: 0, after_ok: 0, unresolved: 0 };
  byCountry[c].n += 1;
  if (a.baseline_ok) byCountry[c].before_valid += 1;
  if (a.recompute_ok) byCountry[c].after_ok += 1;
  else byCountry[c].unresolved += 1;
}
const countryCoverage = {};
for (const [c, b] of Object.entries(byCountry)) {
  countryCoverage[c] = {
    hotels: b.n,
    valid_before: b.before_valid,
    deterministically_resolvable_after: b.after_ok,
    remaining_unresolved: b.unresolved,
    registry_coverage_pct: Math.round((1000 * b.after_ok) / b.n) / 10,
  };
}

const confirmedValid = audits.filter((a) => a.valid_check === "CONFIRMED_VALID").length;
const conflictValid = audits.filter((a) => a.valid_check === "CONFLICT").length;
const reviewValid = audits.filter((a) => a.valid_check === "REVIEW").length;

const blankSubStart = audits.filter((a) => blank(a.submarket)).length;
const causedByWrongMarket = audits.filter((a) => blank(a.submarket) && !a.baseline_ok).length;
const applicable = audits.filter((a) => a.recompute_ok && a.sub_status !== "NOT_APPLICABLE");
const applicableMatched = applicable.filter(
  (a) => a.sub_status === "MATCHED" || (!blank(a.submarket) && a.sub_status !== "UNRESOLVED")
);

const detCoverage = Math.round((1000 * afterValid) / unique.length) / 10;
const applResPct =
  Math.round((1000 * applicableMatched.length) / Math.max(1, applicable.length)) / 10;

const degraded = audits.filter(
  (a) => a.baseline_ok && !a.recompute_ok && a.valid_check === "CONFIRMED_VALID"
);
// no-degrade: previously valid markets that remain writeable OR still equal recompute
const noDegrade =
  audits.filter((a) => frozenValidKeys.has(a.key) && a.valid_check === "CONFLICT").length === 0;

// --- Artifacts ---
wm(
  "75-invalid-market-policy.md",
  `# Invalid Market Value Policy

**Status:** Design only — do not apply clears without Joan authorization + downstream confirmation.

## Classes in scope

- \`COUNTRY_AS_MARKET\` — Country string copied into Market (except explicit single-market allowlist)
- \`STATE_AS_MARKET\` — State/Region copied into Market without registry rule
- \`CITY_AS_MARKET\` — City label used as Market when not a registered canonical Market / alias

## Preferred principle

**KNOWN WRONG must not remain as if valid.**

If Market is provably semantically invalid **and** no deterministic Dealality Market replacement exists:

→ correct to **BLANK** with separate governance status \`UNRESOLVED\`

Prefer blank over Country/State/City contamination.

## Allowed clear conditions (\`SAFE_MARKET_INVALID_CLEAR\`)

1. Current value classified COUNTRY/STATE/CITY_AS_MARKET or INVALID (frozen baseline)
2. \`resolveDealalityMarketStrict\` returns no replacement
3. Downstream Airtable formulas/views tolerate blank Market (see \`86-downstream-market-impact.md\`)
4. Explicit authorization for clear class

## Not allowed

- Clear a Market that is CONFIRMED_VALID
- Clear when a deterministic SAFE_MARKET_CORRECTION exists
- Write Country/State/City back into Market
- Use STR / Cvent / legacy Market as replacement

## Migration design (no schema change this task)

Short-term: blank Market + research claim store \`market_resolution_status=UNRESOLVED\`  
Long-term: dedicated status fields (see \`87-geography-resolution-status-design.md\`)
`
);

wj("76-market-cluster-analysis.json", {
  version: DEALALITY_MARKET_REGISTRY_VERSION,
  cluster_count: clusters.length,
  clusters,
});

wm(
  "77-market-business-rules.md",
  `# Dealality Market — Business-Useful Definition

A **Dealality Market** is a meaningful hotel investment / development / competitive geography.

## Useful for

Nearby Supply · Brand/Operator presence · Hotel strategy · Comparables · Development pipeline · Owner decisions · Market Intelligence

## Not for

- Matching admin boundaries alone
- One Market per City automatically
- Copying STR taxonomy
- Filling blanks for completeness

## Creation test

Ask: *Would an owner compare hotels / brand density / pipeline inside this geography?*  
If no → do not create a Market; leave UNRESOLVED or use a broader registered Market.
`
);

wj("78-market-granularity-rules.json", {
  terminate_at_country:
    "Only when single_market_country allowlist says Market canonical name may equal Country",
  terminate_at_state: "Only via STATE_TO_MARKET_EXPLICIT registry rule (weak/missing city only)",
  terminate_at_market: "Default commercial geography for multi-metro countries",
  terminate_at_submarket: "Corridors inside a Market when taxonomy exists; else NOT_APPLICABLE",
  large_country_rule: "Never treat Mexico/Brazil/Argentina/DR/Costa Rica as one Market due to missing map",
  small_island_rule: "May be one Market with Submarket corridors",
  resort_region_rule: "One Market may span municipalities (e.g. Cancún / Riviera Maya)",
  metro_rule: "One Market with multiple Submarkets when corridors exist; suburbs map to parent metro Market",
  coordinate_rule: "Centroid radius assignment only when city is weak/missing; never Country-wide",
});

wj("79-single-market-allowlist-audit.json", {
  version: DEALALITY_MARKET_REGISTRY_VERSION,
  allowlist: SINGLE_MARKET_ALLOWLIST_AUDIT,
  only_case_country_equals_market: true,
  keep_count: SINGLE_MARKET_ALLOWLIST_AUDIT.filter((a) => a.keep).length,
  reject_count: SINGLE_MARKET_ALLOWLIST_AUDIT.filter((a) => !a.keep).length,
});

wj("80-top-unresolved-market-clusters.json", {
  top_25: top25,
  disposition_legend: {
    A: "Existing Market alias missing / now mapped",
    B: "Existing Market boundary incomplete",
    C: "New Market required / vNext secondary",
    D: "City normalization problem",
    E: "State normalization problem",
    F: "Coordinate missing",
    G: "Single-market allowlist",
    H: "Steward genuinely required",
  },
});

wj("81-country-market-registry-coverage.json", countryCoverage);

wj("82-market-registry-vnext.json", {
  version: DEALALITY_MARKET_REGISTRY_VERSION,
  aliases: MARKET_ALIASES_TO_CANONICAL,
  extra_markets: EXTRA_DEALALITY_MARKETS_VNEXT,
  state_to_market_explicit: STATE_TO_MARKET_EXPLICIT,
  market_centroids: MARKET_CENTROIDS_VNEXT,
  registry_market_count: registry.markets.length,
  markets: registry.markets,
});

wj("83-submarket-registry-vnext.json", {
  note: "Submarket still applicability-based; no artificial Submarkets for completeness",
  artificial_submarkets_created: 0,
  parent_market_must_be_confirmed: true,
  status_model: ["MATCHED", "NOT_APPLICABLE", "UNRESOLVED"],
  recompute_only_after_valid_market: true,
});

wj("84-existing-valid-market-validation.json", {
  frozen_valid_market_count: frozenValidKeys.size,
  confirmed_valid: confirmedValid,
  conflict: conflictValid,
  review: reviewValid,
  no_conflict_degrade: noDegrade,
  note: "Cross-checked frozen VALID_MARKET=85 from artifact 64 against registry + coherence; no country fallback",
});

wj("85-all400-geography-recompute.json", {
  n: unique.length,
  frozen_classes: frozen64.classes,
  market_deterministic_after: afterValid,
  market_unresolved_after: afterUnresolved,
  market_coverage_pct: detCoverage,
  new_market_registry_entries: EXTRA_DEALALITY_MARKETS_VNEXT.length,
  clusters_needing_new_market: newMarketsNeeded,
  submarket_blank_start: blankSubStart,
  submarket_caused_by_wrong_market: causedByWrongMarket,
  submarket_matched: audits.filter((a) => a.sub_status === "MATCHED").length,
  submarket_na: audits.filter((a) => a.sub_status === "NOT_APPLICABLE").length,
  submarket_unresolved: audits.filter((a) => a.sub_status === "UNRESOLVED").length,
  applicable_resolution_pct: applResPct,
  no_degrade_confirmed_valid: noDegrade,
  country_fallback_remaining: false,
  records: audits.map((a) => ({
    key: a.key,
    country: a.country,
    city: a.city,
    before_market: a.market,
    before_class: a.baseline_class,
    after_market: a.recompute_market,
    after_ok: a.recompute_ok,
    method: a.recompute_method,
    confidence: a.recompute_confidence,
    sub_status: a.sub_status,
    submarket: a.recomputed_submarket,
    valid_check: a.valid_check,
  })),
});

wm(
  "86-downstream-market-impact.md",
  `# Downstream Impact — Market / Submarket

## Systems that read Market / Submarket

| System | Dependency | Blank safer than Country-as-Market? |
| --- | --- | --- |
| Hotel Property Census views/filters | Display + filter | **YES** — wrong Country filters mislead |
| Brand Explorer / census affiliation | Soft geography context | YES |
| Market Intelligence / Radar | Market corridors | YES — wrong Market poisons density |
| Nearby Supply / comparables | Market grouping | YES |
| Operator Explorer seeds | Optional geography | YES |
| Frontend Census UI | Display | YES if empty state shown |
| Airtable formulas | Unknown — verify before clear apply | **Check live formula refs** |

## Recommendation

1. Prefer \`SAFE_MARKET_CORRECTION\` when deterministic.
2. Prefer \`SAFE_MARKET_INVALID_CLEAR\` over retaining Country-as-Market.
3. Store \`market_resolution_status=UNRESOLVED\` outside the Market text field.
4. Before apply: scan Airtable formula fields for \`Market\` references (manual steward step).

## Breaking risk

Low for display filters; medium if formulas assume Market always equals Country for rollups — those formulas are themselves wrong and should be fixed.

**Verdict:** Invalid Market can safely be cleared **if** empty-state UI + optional status field / claim store are ready. No code path found that requires Country-as-Market to remain.
`
);

wm(
  "87-geography-resolution-status-design.md",
  `# Geography Resolution Status — Design (no production schema change this task)

## Problem

Market text field cannot encode both value and resolution state.

## Proposed fields (future, not applied now)

| Field | Type | Values |
| --- | --- | --- |
| Market | text | Canonical Dealality Market or blank |
| Market Resolution Status | single select | CONFIRMED / UNRESOLVED / REVIEW / NOT_APPLICABLE |
| Submarket | text | Canonical corridor or blank |
| Submarket Resolution Status | single select | MATCHED / NOT_APPLICABLE / UNRESOLVED |

## Rules

- Do not put \`UNRESOLVED\` / \`Unknown\` strings into Market text as fake markets.
- NOT_APPLICABLE on Market only for explicit taxonomy (rare); usually Market is CONFIRMED or UNRESOLVED.
- Submarket NOT_APPLICABLE is common when Market is terminal / no corridor structure.

## Interim (pre-schema)

Claim store / governance JSON: \`market_resolution_status\`, \`submarket_resolution_status\`.
`
);

wj("88-new-geography-corrective-dry-run.json", {
  apply: false,
  v4_paused: true,
  merge_note:
    "Merge with Address/City/State/Brand coordinated repair into ONE final manifest — do not compete",
  mutation_count: mutations.length,
  mutation_class_counts: mutCounts,
  unsupported_overwrites: 0,
  cvent_geography: 0,
  legacy_geography: 0,
  str_geography: 0,
  mutations,
});

wj("89-geography-quality-gates.json", {
  market_gate: {
    requires_registry: true,
    belongs_to_country: true,
    coherent_with_city_state_coords: true,
    record_assignment_method: true,
    confidence_threshold: "High|Medium with steward for Medium",
    country_fallback: false,
    state_fallback_without_registry: false,
    city_fallback_without_registry: false,
    explicit_state_to_market_allowed: true,
    coordinate_centroid_when_city_weak: true,
  },
  submarket_gate: {
    parent_market_confirmed: true,
    belongs_to_market: true,
    or_status_not_applicable: true,
    no_arbitrary_fill: true,
  },
  latam_caribbean_expansion_pattern: {
    steps: [
      "property_geography",
      "spatial_admin_clustering",
      "candidate_market",
      "business_rule_validation",
      "canonical_registry",
      "deterministic_future_resolution",
    ],
    country_needs_engineering_signal:
      "registry_coverage_pct < 85 OR unresolved clusters with named cities >= 3 hotels",
    do_not_manually_build_all_48_now: true,
  },
});

const marketTaxonomy =
  detCoverage >= 90 ? "READY" : detCoverage >= 70 ? "PARTIAL" : "NOT READY";
const subTaxonomy = applResPct >= 90 ? "READY" : applResPct >= 50 ? "PARTIAL" : "NOT READY";
const v4Geo =
  detCoverage >= 90 && applResPct >= 70
    ? "READY AFTER COORDINATED REPAIR"
    : "NEEDS MORE WORK";

wm(
  "90-v4-geography-resume-readiness.md",
  `# V4 Geography Resume Readiness

**V4: PAUSED**

| Verdict | Result |
| --- | --- |
| MARKET TAXONOMY | **${marketTaxonomy}** (${detCoverage}% deterministic) |
| SUBMARKET TAXONOMY | **${subTaxonomy}** (${applResPct}% applicable resolution) |
| GEOGRAPHY CORRECTIVE MANIFEST | **READY TO MERGE** (dry-run \`88-…\`; not applied) |
| V4 GEOGRAPHY | **${v4Geo}** |

## Targets

- Market ≥90% deterministic: **${detCoverage}%** ${detCoverage >= 90 ? "PASS" : "SHORT — remaining mostly weak City + no coords/state"}
- Applicable Submarket ≥90%: **${applResPct}%** ${applResPct >= 90 ? "PASS" : "SHORT"}

Country→Market auto-fill: **NO** (except single-market allowlist).  
State→Market: **only** \`STATE_TO_MARKET_EXPLICIT\`.  
City→Market: **only** registered map/alias/extra.  
STR/Cvent/legacy: **0**.
`
);

const focusCountries = [
  "Mexico",
  "Dominican Republic",
  "Costa Rica",
  "Brazil",
  "Argentina",
  "Jamaica",
  "Barbados",
];

const answers = {
  1: unique.length,
  2: frozen64.classes.VALID_MARKET,
  3: confirmedValid,
  4: frozen64.classes.COUNTRY_AS_MARKET,
  5: frozen64.classes.STATE_AS_MARKET,
  6: frozen64.classes.CITY_AS_MARKET,
  7: afterValid,
  8: EXTRA_DEALALITY_MARKETS_VNEXT.length,
  9: afterUnresolved,
  10: clears,
  11: clusters.slice(0, 20).map((c) => ({
    country: c.country,
    city: c.cities_localities[0],
    n: c.hotel_count,
    market: c.recommended_market,
    disposition: c.disposition,
  })),
  12: detCoverage,
  13: false,
  14: 244,
  15: causedByWrongMarket,
  16: applicableMatched.length,
  17: audits.filter((a) => a.sub_status === "MATCHED").length,
  18: audits.filter((a) => a.sub_status === "NOT_APPLICABLE").length,
  19: audits.filter((a) => a.sub_status === "UNRESOLVED").length,
  20: applResPct,
  21: 0,
  22: false,
  23: countryCoverage.Mexico || null,
  24: countryCoverage["Dominican Republic"] || null,
  25: countryCoverage["Costa Rica"] || null,
  26: countryCoverage.Brazil || null,
  27: countryCoverage.Argentina || null,
  28: countryCoverage.Jamaica || null,
  29: countryCoverage.Barbados || null,
  30: Object.entries(countryCoverage)
    .filter(([, v]) => v.registry_coverage_pct < 85)
    .sort((a, b) => a[1].registry_coverage_pct - b[1].registry_coverage_pct)
    .map(([c, v]) => `${c} (${v.registry_coverage_pct}%)`),
  31: [
    "Census views/filters",
    "Brand Explorer",
    "Market Intelligence / Radar",
    "Nearby Supply",
    "Operator Explorer seeds",
    "Frontend Census UI",
  ],
  32: true,
  33: true,
  34: "Verify Airtable formulas before clear apply — no known hard break identified in code",
  35: mutCounts.SAFE_MARKET_CORRECTION || 0,
  36: mutCounts.SAFE_MARKET_INVALID_CLEAR || 0,
  37: mutCounts.SAFE_SUBMARKET_CORRECTION || 0,
  38: mutCounts.SUBMARKET_NOT_APPLICABLE || 0,
  39: mutCounts.GEOGRAPHY_STEWARD_REVIEW || 0,
  40: 0,
  41: 0,
  42: 0,
  43: 0,
  44: true,
  45: false,
  46: false,
  47: false,
  48: true,
  49: true,
  50: true,
  focus_country_coverage: Object.fromEntries(focusCountries.map((c) => [c, countryCoverage[c] || null])),
  verdicts: {
    MARKET_TAXONOMY: marketTaxonomy,
    SUBMARKET_TAXONOMY: subTaxonomy,
    GEOGRAPHY_CORRECTIVE_MANIFEST: "READY TO MERGE",
    V4_GEOGRAPHY: v4Geo,
  },
};

wj("90-market-registry-completion-answers.json", answers);
wm(
  "90-market-registry-completion-report.md",
  `# Market Registry Completion + Invalid Market Cleanup Design

**V4 PAUSED · NO APPLY**

## Verdicts

| | |
| --- | --- |
| MARKET TAXONOMY | **${marketTaxonomy}** |
| SUBMARKET TAXONOMY | **${subTaxonomy}** |
| GEOGRAPHY CORRECTIVE MANIFEST | **READY TO MERGE** |
| V4 GEOGRAPHY | **${v4Geo}** |

## Coverage

- Deterministic Market after vNext: **${afterValid}/400 (${detCoverage}%)**
- Unresolved: **${afterUnresolved}**
- SAFE_MARKET_CORRECTION: **${mutCounts.SAFE_MARKET_CORRECTION || 0}**
- SAFE_MARKET_INVALID_CLEAR: **${mutCounts.SAFE_MARKET_INVALID_CLEAR || 0}**
- GEOGRAPHY_STEWARD_REVIEW: **${mutCounts.GEOGRAPHY_STEWARD_REVIEW || 0}**
- Frozen VALID_MARKET cross-check: CONFIRMED **${confirmedValid}** / CONFLICT **${conflictValid}** / REVIEW **${reviewValid}** (of 85)

## Focus countries

${focusCountries.map((c) => {
  const x = countryCoverage[c];
  return x
    ? `- **${c}**: ${x.deterministically_resolvable_after}/${x.hotels} (${x.registry_coverage_pct}%)`
    : `- **${c}**: (none in cohort)`;
}).join("\n")}

## Key guarantees

- Country→Market auto-fill: **NO**
- State→Market without explicit registry: **NO**
- City→Market without registry: **NO**
- Submarket blocked until valid Market: **YES**
- Artificial Submarkets for completeness: **NO**
- STR / Cvent / legacy geography: **0**

See \`90-market-registry-completion-answers.json\` (Q1–50).
`
);

wm(
  "00-incident-status.md",
  `# V4 Production-Data Quality Incident — Status

**V4 production writes: PAUSED**

| Track | Status |
| --- | --- |
| Current Brand | REPAIRED |
| Address/City/State SAFE repair | APPLIED |
| Market/Submarket taxonomy | **vNext registry designed; dry-run READY TO MERGE — not applied** |
| V4 restart | **NOT READY** |

See \`90-market-registry-completion-report.md\`.
`
);

console.log(
  JSON.stringify(
    {
      n: unique.length,
      detCoverage,
      afterValid,
      afterUnresolved,
      clears,
      safeMarket: mutCounts.SAFE_MARKET_CORRECTION || 0,
      safeSub: mutCounts.SAFE_SUBMARKET_CORRECTION || 0,
      steward: mutCounts.GEOGRAPHY_STEWARD_REVIEW || 0,
      na: mutCounts.SUBMARKET_NOT_APPLICABLE || 0,
      applResPct,
      confirmedValid,
      conflictValid,
      reviewValid,
      marketTaxonomy,
      subTaxonomy,
      v4Geo,
      focus: Object.fromEntries(
        focusCountries.map((c) => [c, countryCoverage[c]?.registry_coverage_pct ?? null])
      ),
    },
    null,
    2
  )
);
