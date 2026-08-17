/**
 * Core Identity Foundation Closure v1
 *
 * 1) Reconcile HOLD ledger (active vs resolved)
 * 2) State / Region backfill (applicable only)
 * 3) City / Locality completion
 * 4) Brazil sanity check
 * 5) Targeted discovery toward ~15k
 * 6) 52-geography matrix refresh
 *
 * Writes only Hotel Property Census tbl9aY5ijiuIzzWam (State/Region, City, shells).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolveStateRegionFromCity,
  isDirtyStateRegionValue,
} from "./census-city-to-state-map.js";
import { resolveStateRegionV3 } from "./census-autopilot-v3/geography/state-region-resolver-v3.js";
import { isDescriptorCity, normalizePlaceKey } from "./census-city-state-normalizer.js";
import { normName } from "./census-autopilot-v2/identity-dedupe.js";
import {
  listDealalityCalaGeographies,
  normalizeGeographyLabel,
  resolveDealalityCalaGeography,
} from "./dealality-cala-geography-registry-v1.js";
import {
  CENSUS_TABLE_ID,
  MATCH,
  loadMasterUniverseCandidates,
  loadHbxCandidates,
  mergeCandidateUniverses,
  classifyAgainstCensus,
  classifyShellPreflightQuality,
  SHELL_PREFLIGHT_CLASS,
} from "./full-cala-15k-census-shell-insert-v1.js";
import { runFullCala15kShellOrchestratorV1 } from "./full-cala-15k-shell-orchestrator-v1.js";
import { runFullCalaGeographyCoverageRegistryAuditV1 } from "./full-cala-geography-coverage-registry-audit-v1.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { createLiveHotelPropertyCensusAdapter } from "./census-autopilot-batch-engine.js";
import { resolveHbxConfig, hbxFetchJson, contentUrl } from "./hbx-content-api-client.js";
import {
  extractHbxHotel,
  pullCountryHotels,
} from "./hbx-content-api-cala-wave1-dry-run-v1.js";
import { createHbxRequestRateLimiter } from "./hbx-request-rate-limiter-v1.js";
import { searchGoogleHotels } from "./providers/serpapi-google-hotels/search.js";
import { SerpApiCreditTracker } from "./providers/serpapi-google-hotels/credit-tracker.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const FOUNDATION_OBJECTIVE =
  "full-cala-core-identity-foundation-closure-v1";
export const FOUNDATION_VERSION =
  "full-cala-core-identity-foundation-closure-v1";

export const HOLD_RESOLUTION = Object.freeze({
  ACTIVE_UNRESOLVED_HOLD: "ACTIVE_UNRESOLVED_HOLD",
  RESOLVED_TO_CENSUS: "RESOLVED_TO_CENSUS",
  RESOLVED_EXISTING_MATCH: "RESOLVED_EXISTING_MATCH",
  RESOLVED_DUPLICATE: "RESOLVED_DUPLICATE",
  INVALID: "INVALID",
  MANUAL_REVIEW: "MANUAL_REVIEW",
  HISTORICAL_HOLD_SUPERSEDED: "HISTORICAL_HOLD_SUPERSEDED",
});

/** Small territories where State/Region is not a useful Dealality level. */
export const STATE_REGION_NOT_APPLICABLE = new Set([
  "Aruba",
  "Bonaire",
  "Sint Eustatius",
  "Saba",
  "Curaçao",
  "Sint Maarten",
  "Saint Martin",
  "Saint Barthélemy",
  "Anguilla",
  "Montserrat",
  "Bermuda",
  "Cayman Islands",
  "Turks and Caicos Islands",
  "British Virgin Islands",
  "U.S. Virgin Islands",
  "Guadeloupe",
  "Martinique",
  "French Guiana",
]);

const STATE_DIR = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-core-identity-foundation"
);
const HOLDS_FILE = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
);
const APPLIED_FILE = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator/applied-index.json"
);
const CANDIDATES_DIR = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-core-identity-census/hbx-candidates"
);
const MERGED_PACK = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-cala-full-geography-candidate-pack.json"
);

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}
function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || String(v).trim() === "";
}

async function listCensusFoundation(baseId, token, tableId) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of [
      "Property Name",
      "Canonical Property Name",
      "Country",
      "City",
      "State / Region",
      "Address",
      "Official Property URL",
      "Phone",
      "HBX Hotel Code",
      "Property Identity Key",
    ]) {
      params.append("fields[]", f);
    }
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`census_list_failed:${res.status}:${json?.error?.message || ""}`);
    }
    records.push(...(json.records || []));
    offset = json.offset;
    await sleep(110);
  } while (offset);
  return records;
}

function buildCensusLookup(records) {
  const byNameCountry = new Map();
  const byHbx = new Map();
  for (const r of records) {
    const name = normName(
      r.fields?.["Canonical Property Name"] || r.fields?.["Property Name"] || ""
    );
    const country = normalizeGeographyLabel(r.fields?.Country || "");
    if (name && country) {
      const key = `${name}|${country}`;
      if (!byNameCountry.has(key)) byNameCountry.set(key, []);
      byNameCountry.get(key).push(r);
    }
    const hbx = r.fields?.["HBX Hotel Code"];
    if (hbx != null && hbx !== "") byHbx.set(Number(hbx), r);
  }
  return { byNameCountry, byHbx, count: records.length };
}

/**
 * Explain prior HBX_HOLDS_UPGRADED metric + rebuild active HOLD set.
 */
export function reconcileHoldLedger({ censusLookup, applied, universe, holds }) {
  const byId = new Map((universe || []).map((c) => [c.candidate_id, c]));
  const appliedHbx = new Set((applied?.hbx_codes || []).map(Number));
  const appliedCand = new Set(applied?.candidate_ids || []);

  const tallies = {
    ACTIVE_UNRESOLVED_HOLD: 0,
    RESOLVED_TO_CENSUS: 0,
    RESOLVED_EXISTING_MATCH: 0,
    RESOLVED_DUPLICATE: 0,
    INVALID: 0,
    MANUAL_REVIEW: 0,
    HISTORICAL_HOLD_SUPERSEDED: 0,
  };

  const active = {};
  const resolved = {};
  const hbxUpgradeExplain = {
    definition:
      "Prior HBX_HOLDS_UPGRADED counted in-memory name+country overlaps between HBX pack and Cvent candidates during the Core Identity run. It did NOT remove those candidates from holds-ledger.json.",
    prior_reported: 3879,
    of_those_now_resolved_to_census: 0,
    of_those_existing_match: 0,
    of_those_still_active_hold: 0,
    of_those_invalid: 0,
  };

  for (const [id, h] of Object.entries(holds.by_candidate_id || {})) {
    const cand = byId.get(id);
    let status = HOLD_RESOLUTION.ACTIVE_UNRESOLVED_HOLD;
    let reason = h.reason || h.class;

    if (h.class === "non_hotel_reject" || /non_hotel/i.test(h.class || "")) {
      status = HOLD_RESOLUTION.INVALID;
      reason = "non_hotel_reject";
    } else if (appliedCand.has(id)) {
      status = HOLD_RESOLUTION.RESOLVED_TO_CENSUS;
      reason = "applied_candidate_id";
    } else if (cand) {
      const hbxCode =
        cand.external_ids?.hbx_code != null
          ? Number(cand.external_ids.hbx_code)
          : null;
      if (hbxCode != null && appliedHbx.has(hbxCode)) {
        status = HOLD_RESOLUTION.RESOLVED_TO_CENSUS;
        reason = "applied_hbx_code";
      } else if (hbxCode != null && censusLookup.byHbx.has(hbxCode)) {
        status = HOLD_RESOLUTION.RESOLVED_EXISTING_MATCH;
        reason = "census_hbx_match";
      } else {
        const key = `${normName(cand.property_name || cand.normalized_property_name)}|${normalizeGeographyLabel(cand.country)}`;
        const hits = censusLookup.byNameCountry.get(key) || [];
        if (hits.length === 1) {
          status = HOLD_RESOLUTION.RESOLVED_EXISTING_MATCH;
          reason = "census_name_country_match";
        } else if (hits.length > 1) {
          status = HOLD_RESOLUTION.RESOLVED_DUPLICATE;
          reason = "multiple_census_name_country_matches";
        } else if (h.class === "shell_insert_with_review") {
          status = HOLD_RESOLUTION.MANUAL_REVIEW;
        }
      }
    } else {
      // orphan hold row without candidate — treat as historical
      status = HOLD_RESOLUTION.HISTORICAL_HOLD_SUPERSEDED;
      reason = "candidate_missing_from_universe";
    }

    tallies[status] = (tallies[status] || 0) + 1;
    const row = {
      ...h,
      resolution_status: status,
      resolution_reason: reason,
      resolved_at: new Date().toISOString(),
    };
    if (status === HOLD_RESOLUTION.ACTIVE_UNRESOLVED_HOLD) {
      active[id] = {
        country: h.country,
        class: h.class,
        reason: h.reason,
        evidence_fingerprint: h.evidence_fingerprint,
        held_at: h.held_at,
        resolution_status: status,
      };
    } else {
      resolved[id] = row;
    }
  }

  // Cross-check prior "upgraded" concept: Cvent holds whose name+country now in Census
  let upgradedResolvedToCensus = 0;
  let upgradedExistingMatch = 0;
  let upgradedStillActive = 0;
  let upgradedInvalid = 0;
  let upgradedOther = 0;
  for (const [id, h] of Object.entries(holds.by_candidate_id || {})) {
    const cand = byId.get(id);
    if (!cand) continue;
    const key = `${normName(cand.property_name || "")}|${normalizeGeographyLabel(cand.country)}`;
    const inCensus = (censusLookup.byNameCountry.get(key) || []).length > 0;
    if (!inCensus) continue;
    const status = active[id]
      ? HOLD_RESOLUTION.ACTIVE_UNRESOLVED_HOLD
      : resolved[id]?.resolution_status;
    if (status === HOLD_RESOLUTION.ACTIVE_UNRESOLVED_HOLD) upgradedStillActive += 1;
    else if (status === HOLD_RESOLUTION.RESOLVED_TO_CENSUS) upgradedResolvedToCensus += 1;
    else if (status === HOLD_RESOLUTION.RESOLVED_EXISTING_MATCH) upgradedExistingMatch += 1;
    else if (status === HOLD_RESOLUTION.INVALID) upgradedInvalid += 1;
    else upgradedOther += 1;
  }
  hbxUpgradeExplain.of_those_now_resolved_to_census = upgradedResolvedToCensus;
  hbxUpgradeExplain.of_those_existing_match = upgradedExistingMatch;
  hbxUpgradeExplain.of_those_still_active_hold = upgradedStillActive;
  hbxUpgradeExplain.of_those_invalid = upgradedInvalid;
  hbxUpgradeExplain.of_those_other_resolved = upgradedOther;
  hbxUpgradeExplain.name_country_overlap_now_in_census =
    upgradedResolvedToCensus +
    upgradedExistingMatch +
    upgradedStillActive +
    upgradedInvalid +
    upgradedOther;
  // Approximate: name-country overlaps that were the upgrade signal
  hbxUpgradeExplain.note =
    "Prior HBX_HOLDS_UPGRADED=3879 was an in-memory overlap count only; holds-ledger.json was not rewritten. Reconciliation now removes resolved rows from the active HOLD queue. TOTAL_REMAINING_HOLDS previously counted all historical ledger rows (including ones already in Census).";
  hbxUpgradeExplain.total_remaining_holds_was =
    "historical_ledger_rows_including_resolved";
  hbxUpgradeExplain.active_unresolved_holds_now = Object.keys(active).length;

  return {
    tallies,
    active_count: Object.keys(active).length,
    resolved_count: Object.keys(resolved).length,
    active,
    resolved,
    hbxUpgradeExplain,
    prior_total_remaining_holds: Object.keys(holds.by_candidate_id || {}).length,
  };
}

function isStateApplicable(country) {
  const c = String(country || "").trim();
  if (!c) return false;
  if (STATE_REGION_NOT_APPLICABLE.has(c)) return false;
  const g = resolveDealalityCalaGeography(c);
  if (g && STATE_REGION_NOT_APPLICABLE.has(g.name)) return false;
  return true;
}

/** Bootstrap unambiguous city→state from records that already have both. */
function buildIntraCensusCityStateIndex(records) {
  const buckets = new Map();
  for (const r of records) {
    const f = r.fields || {};
    const country = normalizeGeographyLabel(f.Country || "");
    const city = normalizePlaceKey(f.City || "");
    const state = String(f["State / Region"] || "").trim();
    if (!country || !city || !state || isDirtyStateRegionValue(state)) continue;
    if (!isStateApplicable(f.Country)) continue;
    const key = `${country}|${city}`;
    if (!buckets.has(key)) buckets.set(key, new Set());
    buckets.get(key).add(state);
  }
  const out = new Map();
  for (const [key, set] of buckets) {
    if (set.size === 1) out.set(key, [...set][0]);
  }
  return out;
}

function proposeStateRegion(rec, cityStateIndex = null) {
  const f = rec.fields || {};
  const country = String(f.Country || "").trim();
  const city = String(f.City || "").trim();
  const address = String(f.Address || "").trim();
  const existing = String(f["State / Region"] || "").trim();
  const name = String(f["Canonical Property Name"] || f["Property Name"] || "").trim();

  if (!isStateApplicable(country)) {
    return { applicable: false, patch: null, status: "STATE_REGION_NOT_APPLICABLE" };
  }
  if (existing && !isDirtyStateRegionValue(existing)) {
    return { applicable: true, patch: null, status: "ALREADY_SET" };
  }

  const fromCity = resolveStateRegionFromCity({
    city,
    country,
    state: existing,
  });
  if (fromCity.ok && fromCity.state) {
    return {
      applicable: true,
      patch: { "State / Region": fromCity.state },
      status: "CITY_MAP",
      method: fromCity.method,
    };
  }

  if (cityStateIndex && city) {
    const key = `${normalizeGeographyLabel(country)}|${normalizePlaceKey(city)}`;
    const boot = cityStateIndex.get(key);
    if (boot) {
      return {
        applicable: true,
        patch: { "State / Region": boot },
        status: "INTRA_CENSUS_CITY_BOOTSTRAP",
        method: "intra_census_unambiguous_city_state",
      };
    }
  }

  const v3 = resolveStateRegionV3({
    country,
    city,
    address,
    name,
    official_state: null,
    address_production_eligible: true,
    coords_production_eligible: false,
  });
  if (v3.ok && v3.production_eligible && v3.normalized_state_region) {
    return {
      applicable: true,
      patch: { "State / Region": v3.normalized_state_region },
      status: "V3_RESOLVER",
      method: v3.method,
    };
  }

  return { applicable: true, patch: null, status: "UNRESOLVED" };
}

function inferCityFromAddress(address, country) {
  const a = String(address || "").trim();
  if (!a) return null;
  // "Street, City, State" / "Street, City - UF"
  const parts = a.split(",").map((p) => p.trim()).filter(Boolean);
  if (parts.length >= 2) {
    let cand = parts[parts.length - 2] || parts[parts.length - 1];
    // Brazil "São Paulo - SP"
    cand = cand.replace(/\s*-\s*[A-Z]{2}\s*$/, "").trim();
    cand = cand.replace(/\b\d{4,}\b/g, "").trim();
    if (cand && cand.length >= 2 && !isDescriptorCity(cand) && cand.length < 60) {
      return cand;
    }
  }
  return null;
}

function completeness(records) {
  const n = records.length || 1;
  let name = 0,
    country = 0,
    city = 0,
    state = 0,
    address = 0,
    phone = 0,
    web = 0;
  let applicable = 0,
    applicableWithState = 0;
  for (const r of records) {
    const f = r.fields || {};
    if (!isBlank(f["Property Name"] || f["Canonical Property Name"])) name += 1;
    if (!isBlank(f.Country)) country += 1;
    if (!isBlank(f.City)) city += 1;
    if (!isBlank(f.Address)) address += 1;
    if (!isBlank(f.Phone)) phone += 1;
    if (!isBlank(f.Website || f["Official Property URL"])) web += 1;
    if (isStateApplicable(f.Country)) {
      applicable += 1;
      if (!isBlank(f["State / Region"]) && !isDirtyStateRegionValue(f["State / Region"])) {
        applicableWithState += 1;
        state += 1;
      }
    }
  }
  const pct = (x) => Math.round((100 * x) / n);
  return {
    n: records.length,
    NAME_COMPLETENESS: pct(name),
    COUNTRY_COMPLETENESS: pct(country),
    CITY_COMPLETENESS: pct(city),
    ADDRESS_COMPLETENESS: pct(address),
    PHONE_COMPLETENESS: pct(phone),
    WEBSITE_COMPLETENESS: pct(web),
    STATE_REGION_APPLICABLE_RECORDS: applicable,
    STATE_REGION_COMPLETENESS_OF_APPLICABLE: applicable
      ? Math.round((100 * applicableWithState) / applicable)
      : 100,
    STATE_REGION_GLOBAL_PCT: pct(state),
  };
}

function brazilSanityCheck(records) {
  const br = records.filter((r) => /brazil|brasil/i.test(String(r.fields?.Country || "")));
  const names = new Map();
  let missingCity = 0;
  let hotelLike = 0;
  let pousada = 0;
  let hostel = 0;
  let nonHotelCue = 0;
  for (const r of br) {
    const name = String(r.fields?.["Property Name"] || "");
    const key = `${normName(name)}|${normalizeGeographyLabel(r.fields?.City || "")}`;
    names.set(key, (names.get(key) || 0) + 1);
    if (isBlank(r.fields?.City)) missingCity += 1;
    if (/hotel|resort|hyatt|marriott|hilton|ibis|mercure|transamerica/i.test(name))
      hotelLike += 1;
    if (/pousada/i.test(name)) pousada += 1;
    if (/hostel/i.test(name)) hostel += 1;
    if (/\b(apartment|apto|flat|condo|office|spa only)\b/i.test(name) && !/hotel/i.test(name))
      nonHotelCue += 1;
  }
  const dupKeys = [...names.values()].filter((n) => n > 1).length;
  const dupRecords = [...names.values()].filter((n) => n > 1).reduce((a, b) => a + b, 0);
  return {
    status: "COMPLETE",
    brazil_count: br.length,
    exact_name_city_duplicate_groups: dupKeys,
    records_in_duplicate_groups: dupRecords,
    missing_city: missingCity,
    hotel_token_names: hotelLike,
    pousada_names: pousada,
    hostel_names: hostel,
    non_hotel_cue_names: nonHotelCue,
    recommendation:
      dupKeys > 50
        ? "CAUTION_DUPLICATE_RISK — prefer Caribbean/SA gap discovery over Brazil volume"
        : "OK_TO_ADD_BRAZIL_REMAINDER_CAUTIOUSLY — prioritize undercovered CALA geos first",
  };
}

function rebuildMergedFromCandidates() {
  const byCode = new Map();
  const packs = [
    path.join(ROOT, "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"),
    MERGED_PACK,
  ];
  for (const fp of packs) {
    if (!fs.existsSync(fp)) continue;
    const j = readJson(fp, { candidates: [] });
    for (const c of j.candidates || []) {
      if (c.hbx_hotel_code != null) byCode.set(Number(c.hbx_hotel_code), c);
    }
  }
  for (const dir of [
    CANDIDATES_DIR,
    path.join(ROOT, "data/research-engine-v2/full-cala-hbx-geography-discovery/candidates"),
  ]) {
    if (!fs.existsSync(dir)) continue;
    for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
      const j = readJson(path.join(dir, f), { candidates: [] });
      for (const c of j.candidates || []) {
        if (c.hbx_hotel_code == null) continue;
        const n = Number(c.hbx_hotel_code);
        if (!byCode.has(n)) byCode.set(n, c);
      }
    }
  }
  const candidates = [...byCode.values()];
  writeJson(MERGED_PACK, {
    objective: FOUNDATION_OBJECTIVE,
    generated_at: new Date().toISOString(),
    count: candidates.length,
    candidates,
  });
  return candidates.length;
}

/** Map HBX hotel code → city from already-acquired candidate packs. */
function buildHbxCityByCode() {
  const byCode = new Map();
  const ingest = (c) => {
    const code = c.hbx_hotel_code != null ? Number(c.hbx_hotel_code) : null;
    const city = String(c.city || "").trim();
    if (code == null || !city || isDescriptorCity(city)) return;
    if (!byCode.has(code)) byCode.set(code, city);
  };
  for (const fp of [
    MERGED_PACK,
    path.join(ROOT, "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"),
  ]) {
    if (!fs.existsSync(fp)) continue;
    for (const c of readJson(fp, { candidates: [] }).candidates || []) ingest(c);
  }
  for (const dir of [
    CANDIDATES_DIR,
    path.join(ROOT, "data/research-engine-v2/full-cala-hbx-geography-discovery/candidates"),
  ]) {
    if (!fs.existsSync(dir)) continue;
    for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
      for (const c of readJson(path.join(dir, f), { candidates: [] }).candidates || []) {
        ingest(c);
      }
    }
  }
  return byCode;
}

/** name|country → city when unique across Cvent + HBX candidate universes. */
function buildCandidateCityByNameCountry(universe = []) {
  const buckets = new Map();
  const add = (name, country, city) => {
    const n = normName(name);
    const c = normalizeGeographyLabel(country);
    const cityT = String(city || "").trim();
    if (!n || !c || !cityT || isDescriptorCity(cityT)) return;
    const key = `${n}|${c}`;
    if (!buckets.has(key)) buckets.set(key, new Set());
    buckets.get(key).add(cityT);
  };
  for (const cand of universe) {
    add(
      cand.property_name || cand.normalized_property_name,
      cand.country,
      cand.city
    );
  }
  for (const fp of [
    MERGED_PACK,
    path.join(ROOT, "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"),
  ]) {
    if (!fs.existsSync(fp)) continue;
    for (const c of readJson(fp, { candidates: [] }).candidates || []) {
      add(c.name || c.property_name, c.country, c.city);
    }
  }
  const out = new Map();
  for (const [key, set] of buckets) {
    if (set.size === 1) out.set(key, [...set][0]);
  }
  return out;
}

const GAP_PREFER_COUNTRIES = [
  "Chile",
  "Peru",
  "Jamaica",
  "Ecuador",
  "Barbados",
  "Uruguay",
  "Puerto Rico",
  "Trinidad and Tobago",
  "Belize",
  "Honduras",
  "Guatemala",
  "British Virgin Islands",
  "Saint Kitts and Nevis",
  "Curaçao",
  "Bonaire",
  "Saint Martin",
  "Sint Maarten",
  "Montserrat",
  "Cuba",
];

export async function runCoreIdentityFoundationClosureV1(opts = {}) {
  const log = opts.log || console.log;
  const mode = String(opts.mode || "dry-run").toLowerCase();
  const enableWrites = Boolean(
    opts.enableProductionWrites && (mode === "run" || mode === "resume")
  );
  const generated_at = new Date().toISOString();
  const hbxBudget = Math.max(0, Number(opts.hbxBudget ?? 8));
  const serpMax = Math.max(0, Number(opts.serpMax ?? 40));

  try {
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
    if (CENSUS_TABLE_ID !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
      throw new Error("wrong_production_table");
    }
  } catch (err) {
    return {
      ok: false,
      CORE_CENSUS_STATUS: "production_census_core_identity_stop_for_founder_review",
      FOUNDER_DECISION_REQUIRED: "YES",
      FOUNDER_DECISION: String(err?.message || err),
      generated_at,
    };
  }

  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base?.target_base_id || base?.baseId;

  log(`[foundation] listing Census…`);
  const records = await listCensusFoundation(baseId, token, CENSUS_TABLE_ID);
  const censusBefore = records.length;
  const lookup = buildCensusLookup(records);
  const holdsRaw = readJson(HOLDS_FILE, { by_candidate_id: {} });
  const applied = readJson(APPLIED_FILE, { hbx_codes: [], candidate_ids: [] });
  const universe = loadMasterUniverseCandidates();

  log(`[foundation] reconciling HOLD ledger…`);
  const recon = reconcileHoldLedger({
    censusLookup: lookup,
    applied,
    universe,
    holds: holdsRaw,
  });

  if (enableWrites) {
    writeJson(HOLDS_FILE, {
      version: 2,
      updated_at: generated_at,
      by_candidate_id: recon.active,
      note: "Active unresolved holds only; resolved archive separate",
    });
    writeJson(path.join(STATE_DIR, "holds-resolved-archive.json"), {
      generated_at,
      count: recon.resolved_count,
      by_candidate_id: recon.resolved,
    });
    writeJson(path.join(STATE_DIR, "hold-reconciliation-report.json"), {
      generated_at,
      tallies: recon.tallies,
      active_count: recon.active_count,
      hbx_upgrade_explain: recon.hbxUpgradeExplain,
      prior_total: recon.prior_total_remaining_holds,
    });
  } else {
    writeJson(path.join(STATE_DIR, "hold-reconciliation-dry-run.json"), {
      generated_at,
      tallies: recon.tallies,
      active_count: recon.active_count,
      hbx_upgrade_explain: recon.hbxUpgradeExplain,
    });
  }

  const brazil = brazilSanityCheck(records);
  writeJson(path.join(STATE_DIR, "brazil-sanity-check.json"), brazil);

  // State / Region proposals
  log(`[foundation] State/Region proposals…`);
  const cityStateIndex = buildIntraCensusCityStateIndex(records);
  const statePatches = [];
  let applicable = 0;
  let already = 0;
  let na = 0;
  let unresolvedState = 0;
  for (const rec of records) {
    const p = proposeStateRegion(rec, cityStateIndex);
    if (!p.applicable) {
      na += 1;
      continue;
    }
    applicable += 1;
    if (p.status === "ALREADY_SET") {
      already += 1;
      continue;
    }
    if (p.patch) statePatches.push({ id: rec.id, fields: p.patch, method: p.method });
    else unresolvedState += 1;
  }

  // City proposals from address + already-acquired HBX / Cvent evidence
  log(`[foundation] City proposals…`);
  const cityPatches = [];
  const hbxCityByCode = buildHbxCityByCode();
  const candCityByKey = buildCandidateCityByNameCountry(universe);
  let cityUnresolved = 0;
  let cityFromHbx = 0;
  let cityFromAddress = 0;
  let cityFromCandidate = 0;
  for (const rec of records) {
    const f = rec.fields || {};
    if (!isBlank(f.City)) continue;
    const hbxCode =
      f["HBX Hotel Code"] != null && f["HBX Hotel Code"] !== ""
        ? Number(f["HBX Hotel Code"])
        : null;
    if (hbxCode != null && hbxCityByCode.has(hbxCode)) {
      cityPatches.push({
        id: rec.id,
        fields: { City: hbxCityByCode.get(hbxCode) },
        method: "hbx_acquired_evidence",
        status: "CITY_CONFIRMED",
      });
      cityFromHbx += 1;
      continue;
    }
    const nameKey = `${normName(f["Canonical Property Name"] || f["Property Name"] || "")}|${normalizeGeographyLabel(f.Country || "")}`;
    if (candCityByKey.has(nameKey)) {
      cityPatches.push({
        id: rec.id,
        fields: { City: candCityByKey.get(nameKey) },
        method: "candidate_name_country_city",
        status: "CITY_INFERRED_HIGH",
      });
      cityFromCandidate += 1;
      continue;
    }
    const inferred = inferCityFromAddress(f.Address, f.Country);
    if (inferred) {
      cityPatches.push({
        id: rec.id,
        fields: { City: inferred },
        method: "address_parse",
        status: "CITY_INFERRED_HIGH",
      });
      cityFromAddress += 1;
    } else {
      cityUnresolved += 1;
    }
  }

  if (mode === "dry-run") {
    const comp0 = completeness(records);
    const report = {
      ok: true,
      CORE_CENSUS_STATUS: "production_census_core_identity_foundation_dry_run_ready",
      mode: "dry-run",
      production_writes: false,
      CENSUS_BEFORE: censusBefore,
      ACTIVE_HOLDS_BEFORE: recon.prior_total_remaining_holds,
      HOLDS_STILL_ACTIVE: recon.active_count,
      HOLD_TALLIES: recon.tallies,
      HBX_HOLDS_UPGRADED_RECONCILED: recon.hbxUpgradeExplain,
      STATE_PATCHES_PROPOSED: statePatches.length,
      CITY_PATCHES_PROPOSED: cityPatches.length,
      CITY_FROM_HBX: cityFromHbx,
      CITY_FROM_CANDIDATE: cityFromCandidate,
      CITY_FROM_ADDRESS: cityFromAddress,
      CITY_UNRESOLVED_AFTER_ADDRESS: cityUnresolved,
      STATE_REGION_APPLICABLE_RECORDS: applicable,
      STATE_ALREADY_SET: already,
      STATE_NOT_APPLICABLE: na,
      STATE_UNRESOLVED: unresolvedState,
      BRAZIL_SANITY: brazil,
      ...comp0,
      FOUNDER_DECISION_REQUIRED: "NO",
      NEXT_STEP:
        "Launch: npm run census:core-identity-foundation -- --mode run --enable-production-writes",
      generated_at,
    };
    writeJson(
      path.join(ROOT, "reports/research-engine-v2/full-cala-core-identity-foundation-dry-run.json"),
      report
    );
    log(
      `[foundation] DRY-RUN active_holds=${recon.active_count} state_patches=${statePatches.length} city_patches=${cityPatches.length}`
    );
    return report;
  }

  // Optional SerpAPI for remaining missing cities (bounded)
  let serpCityResolved = 0;
  if (enableWrites && serpMax > 0 && cityUnresolved > 0) {
    const key = String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim();
    if (key) {
      const tracker = new SerpApiCreditTracker({ ceiling: Math.min(serpMax, 40) });
      const missing = records
        .filter((r) => isBlank(r.fields?.City))
        .filter((r) => !cityPatches.some((p) => p.id === r.id))
        .slice(0, Math.min(serpMax, 40));
      for (const rec of missing) {
        if (!tracker.canSpend(1)) break;
        const name = rec.fields?.["Property Name"];
        const country = rec.fields?.Country;
        log(`[foundation] SerpAPI city ${name} (${country})`);
        const res = await searchGoogleHotels(
          { q: `${name} hotel ${country}`, gl: "us", hl: "en" },
          { tracker }
        );
        await sleep(350);
        if (!res.ok || !res.candidates?.length) continue;
        const hit = res.candidates[0];
        const city =
          hit.city ||
          (typeof hit.address === "string"
            ? hit.address.split(",").slice(-2, -1)[0]?.trim()
            : null);
        if (!city || isDescriptorCity(city)) continue;
        cityPatches.push({
          id: rec.id,
          fields: { City: city },
          method: "serpapi_google_hotels",
          status: "CITY_CONFIRMED",
        });
        serpCityResolved += 1;
      }
    }
  }

  let stateWritten = 0;
  let cityWritten = 0;
  if (enableWrites) {
    const adapter = createLiveHotelPropertyCensusAdapter({ baseId, token, tableId: CENSUS_TABLE_ID });
    // Prefer city first so subsequent state map can use it — but we already proposed state from current city
    // Apply city patches then re-propose state for those ids if still blank
    if (cityPatches.length) {
      log(`[foundation] applying ${cityPatches.length} City patches…`);
      const res = await adapter.patchRecords(cityPatches.map((p) => ({ id: p.id, fields: p.fields })));
      cityWritten = res.updated || 0;
      if (res.errors?.length) log(`[foundation] city patch errors=${res.errors.length}`);
    }
    // Re-list subset expensive — instead, for city-patched ids, try state from new city
    const extraState = [];
    for (const cp of cityPatches) {
      const rec = records.find((r) => r.id === cp.id);
      if (!rec) continue;
      const trial = {
        ...rec,
        fields: { ...rec.fields, City: cp.fields.City },
      };
      const p = proposeStateRegion(trial, cityStateIndex);
      if (p.patch) extraState.push({ id: cp.id, fields: p.patch });
    }
    const allState = [...statePatches, ...extraState];
    // dedupe by id
    const byId = new Map();
    for (const p of allState) byId.set(p.id, p);
    const uniqState = [...byId.values()];
    if (uniqState.length) {
      log(`[foundation] applying ${uniqState.length} State/Region patches…`);
      const res = await adapter.patchRecords(
        uniqState.map((p) => ({ id: p.id, fields: p.fields }))
      );
      stateWritten = res.updated || 0;
      if (res.errors?.length) log(`[foundation] state patch errors=${res.errors.length}`);
    }
  }

  // Targeted HBX discovery (small budget)
  let shellsThisRun = 0;
  let hbxPaused = false;
  let hbxUsed = 0;
  let hbxNew = 0;
  if (enableWrites && hbxBudget > 0) {
    const cfg = resolveHbxConfig(process.env);
    if (cfg.ok) {
      const gate = await hbxFetchJson(
        contentUrl(cfg, "hotels?fields=code,name&language=ENG&from=1&to=1&useSecondaryLanguage=false"),
        cfg
      );
      hbxUsed += 1;
      log(`[foundation] HBX gate ${gate.status} remaining=${gate.response_headers?.["x-ratelimit-remaining"]}`);
      if (gate.ok) {
        const remainingHdr = Number(gate.response_headers?.["x-ratelimit-remaining"] || hbxBudget);
        const budget = Math.min(hbxBudget, Math.max(0, remainingHdr - 2)); // keep margin
        const limiter = createHbxRequestRateLimiter({
          minIntervalMs: 1200,
          maxRequestsPerRun: budget,
          maxRetriesOn429: 2,
        });
        const pulls = [
          { name: "Chile", code: "CL", max: 1500 },
          { name: "Peru", code: "PE", max: 1500 },
          { name: "Jamaica", code: "JM", max: 800 },
          { name: "Ecuador", code: "EC", max: 800 },
          { name: "Barbados", code: "BB", max: 400 },
          { name: "Uruguay", code: "UY", max: 800 },
          { name: "Trinidad and Tobago", code: "TT", max: 400 },
          // Brazil remainder last — only if budget remains after gap geos
          { name: "Brazil", code: "BR", max: 500, startFrom: 6001 },
        ];
        fs.mkdirSync(CANDIDATES_DIR, { recursive: true });
        for (const pull of pulls) {
          if (limiter.requestCount >= budget) break;
          log(`[foundation] HBX ${pull.name}…`);
          const pulled = await pullCountryHotels(cfg, pull.name, pull.code, {
            batchSize: 1000,
            maxHotelsPerCountry: pull.max,
            delayMs: 1200,
            rateLimiter: limiter,
            fields: "all",
            startFrom: pull.startFrom || 1,
            onBatch: (b) =>
              log(`[foundation] ${pull.name} ${b.from}-${b.to} ${b.pulled}/${b.total ?? "?"}`),
          });
          if (pulled.error?.quota_exceeded) {
            hbxPaused = true;
            log(`[foundation] HBX_PAUSED_QUOTA`);
            break;
          }
          const hotels = [];
          for (const raw of pulled.hotels || []) {
            const h = extractHbxHotel(raw, pull.name);
            if (h.hbx_hotel_code == null) continue;
            h.country = pull.name;
            hotels.push(h);
          }
          hbxNew += hotels.length;
          const gid = normalizeGeographyLabel(pull.name).replace(/\s+/g, "_");
          writeJson(path.join(CANDIDATES_DIR, `${gid}.json`), {
            geography: pull.name,
            count: hotels.length,
            candidates: hotels.map((h) => ({
              hbx_hotel_code: h.hbx_hotel_code,
              name: h.name,
              country: pull.name,
              city: h.city,
              address: h.address,
              postal_code: h.postal_code,
              website: h.website,
              phonehotel: h.phonehotel,
              chain_code: h.chain_code,
              category: h.category,
              latitude: null,
              longitude: null,
              discovery_wave: FOUNDATION_VERSION,
            })),
          });
          if (!pulled.ok && !hotels.length) continue;
        }
        hbxUsed += limiter.requestCount;
        rebuildMergedFromCandidates();
      } else if (/quota/i.test(String(gate.error_message || ""))) {
        hbxPaused = true;
      }
    }
  }

  // Shell apply from existing + newly pulled candidates (even if HBX paused)
  if (enableWrites) {
    rebuildMergedFromCandidates();
    log(`[foundation] shell orchestrator…`);
    const orch = await runFullCala15kShellOrchestratorV1({
      mode: "run",
      enableProductionWrites: true,
      maxBatches: 30,
      coreIdentityMode: true,
      preferCountries: GAP_PREFER_COUNTRIES,
      deprioritizeCountries: ["Brazil", "Mexico", "Argentina"],
      log,
    });
    shellsThisRun = orch.SHELLS_ADDED_THIS_RUN || 0;
    if (orch.FOUNDER_DECISION_REQUIRED === "YES") {
      return {
        ok: false,
        CORE_CENSUS_STATUS:
          "production_census_core_identity_stop_for_founder_review",
        FOUNDER_DECISION_REQUIRED: "YES",
        FOUNDER_DECISION: orch.STOP_REASON || orch.FOUNDER_DECISION,
        CENSUS_BEFORE: censusBefore,
        NEW_SHELLS_THIS_RUN: shellsThisRun,
        generated_at,
      };
    }
  }

  // Re-list for final metrics
  log(`[foundation] re-listing Census for metrics…`);
  const afterRecords = await listCensusFoundation(baseId, token, CENSUS_TABLE_ID);
  const censusAfter = afterRecords.length;
  const comp = completeness(afterRecords);

  log(`[foundation] geography matrix…`);
  const geoAudit = await runFullCalaGeographyCoverageRegistryAuditV1({ log });
  const matrixSrc = geoAudit.FULL_GEOGRAPHY_COVERAGE_MATRIX || [];
  const activeHolds = readJson(HOLDS_FILE, { by_candidate_id: {} });
  const holdByCountry = {};
  for (const h of Object.values(activeHolds.by_candidate_id || {})) {
    holdByCountry[h.country] = (holdByCountry[h.country] || 0) + 1;
  }

  const byCountryStats = {};
  for (const r of afterRecords) {
    const c = String(r.fields?.Country || "").trim() || "UNK";
    byCountryStats[c] = byCountryStats[c] || {
      n: 0,
      city: 0,
      state: 0,
      applicable: 0,
      address: 0,
      phone: 0,
      web: 0,
    };
    const b = byCountryStats[c];
    b.n += 1;
    if (!isBlank(r.fields?.City)) b.city += 1;
    if (isStateApplicable(c)) {
      b.applicable += 1;
      if (!isBlank(r.fields?.["State / Region"]) && !isDirtyStateRegionValue(r.fields["State / Region"]))
        b.state += 1;
    }
    if (!isBlank(r.fields?.Address)) b.address += 1;
    if (!isBlank(r.fields?.Phone)) b.phone += 1;
    if (!isBlank(r.fields?.Website || r.fields?.["Official Property URL"])) b.web += 1;
  }

  function coverageStatus(row, census) {
    if (census === 0) return "ZERO_CONFIRMED_PROPERTIES";
    if (census < 40 && (row.tourism_priority === "S" || row.tourism_priority === "A"))
      return "SOURCE_GAP";
    if (census >= 200) return "CORE_COVERAGE_STRONG";
    if (census >= 80) return "CORE_COVERAGE_MODERATE";
    if (census >= 20) return "CORE_COVERAGE_WEAK";
    return "NEEDS_TARGETED_DISCOVERY";
  }

  const matrix = matrixSrc.map((r) => {
    const name = r.name;
    const st = byCountryStats[name] || { n: 0, city: 0, state: 0, applicable: 0, address: 0, phone: 0, web: 0 };
    const census = st.n || r.census_count || 0;
    return {
      geography: name,
      tourism_priority: r.tourism_priority,
      dealality_region: r.dealality_region,
      census_before: r.census_count,
      census_after: census,
      active_holds: holdByCountry[name] || 0,
      name_pct: census ? 100 : 0,
      state_applicable: st.applicable,
      state_pct_applicable: st.applicable
        ? Math.round((100 * st.state) / st.applicable)
        : null,
      city_pct: census ? Math.round((100 * st.city) / census) : 0,
      address_pct: census ? Math.round((100 * st.address) / census) : 0,
      phone_pct: census ? Math.round((100 * st.phone) / census) : 0,
      website_pct: census ? Math.round((100 * st.web) / census) : 0,
      HBX_SEARCHED: r.HBX_SEARCHED,
      coverage_status: coverageStatus(r, census),
      primary_gap: r.recommended_next_action,
    };
  });
  matrix.sort((a, b) => {
    const rank = {
      ZERO_CONFIRMED_PROPERTIES: 0,
      NEEDS_TARGETED_DISCOVERY: 1,
      SOURCE_GAP: 2,
      CORE_COVERAGE_WEAK: 3,
      DISCOVERY_NOT_COMPLETE: 4,
      CORE_COVERAGE_MODERATE: 5,
      CORE_COVERAGE_STRONG: 6,
    };
    return (rank[a.coverage_status] ?? 5) - (rank[b.coverage_status] ?? 5) || a.census_after - b.census_after;
  });

  const matrixPath =
    "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.json";
  writeJson(path.join(ROOT, matrixPath), {
    generated_at,
    census_before: censusBefore,
    census_after: censusAfter,
    matrix,
  });
  writeMd(
    path.join(ROOT, "reports/research-engine-v2/full-cala-core-identity-52-geography-matrix.md"),
    [
      `# Core Identity 52-Geography Matrix (Foundation Closure)`,
      ``,
      `Census ${censusBefore} → ${censusAfter}`,
      ``,
      `| Geography | Census | Holds | Coverage | City% | State%* |`,
      `| --- | ---: | ---: | --- | ---: | ---: |`,
      ...matrix.map(
        (r) =>
          `| ${r.geography} | ${r.census_after} | ${r.active_holds} | ${r.coverage_status} | ${r.city_pct} | ${r.state_pct_applicable ?? "n/a"} |`
      ),
      ``,
      `\\* State % of applicable records only`,
    ].join("\n")
  );

  const counts = matrix.reduce((a, r) => {
    a[r.coverage_status] = (a[r.coverage_status] || 0) + 1;
    return a;
  }, {});

  const targetGap = Math.max(0, 15000 - censusAfter);
  const coreComplete =
    censusAfter >= 15000 &&
    comp.CITY_COMPLETENESS >= 98 &&
    comp.STATE_REGION_COMPLETENESS_OF_APPLICABLE >= 85 &&
    recon.active_count < 5000;

  const status = coreComplete
    ? "production_census_core_identity_complete"
    : "production_census_core_identity_partial_target_gap";

  const final = {
    ok: true,
    CORE_CENSUS_STATUS: status,
    mode,
    production_writes: enableWrites,
    CENSUS_BEFORE: censusBefore,
    CENSUS_AFTER: censusAfter,
    TARGET_15K_REACHED: censusAfter >= 15000 ? "YES" : "NO",
    TARGET_GAP_IF_ANY: targetGap,
    NEW_SHELLS_THIS_RUN: shellsThisRun,
    ACTIVE_HOLDS_BEFORE: recon.prior_total_remaining_holds,
    ACTIVE_HOLDS_AFTER: enableWrites ? recon.active_count : recon.active_count,
    HBX_HOLDS_UPGRADED_RECONCILED: recon.hbxUpgradeExplain,
    HOLDS_RESOLVED_TO_CENSUS: recon.tallies.RESOLVED_TO_CENSUS,
    HOLDS_RESOLVED_EXISTING_MATCH: recon.tallies.RESOLVED_EXISTING_MATCH,
    HOLDS_RESOLVED_DUPLICATE: recon.tallies.RESOLVED_DUPLICATE,
    HOLDS_INVALID: recon.tallies.INVALID,
    HOLDS_STILL_ACTIVE: recon.active_count,
    HOLD_TALLIES: recon.tallies,
    STATE_PATCHES_PROPOSED: statePatches.length,
    STATE_PATCHES_WRITTEN: stateWritten,
    CITY_PATCHES_PROPOSED: cityPatches.length,
    CITY_PATCHES_WRITTEN: cityWritten,
    SERPAPI_CITY_RESOLVED: serpCityResolved,
    HBX_REQUESTS_USED: hbxUsed,
    HBX_NEW_IDENTITIES: hbxNew,
    HBX_PAUSED_QUOTA: hbxPaused ? "YES" : "NO",
    ...comp,
    GEOGRAPHIES_ASSESSED: `${matrix.length} / 52`,
    STRONG: counts.CORE_COVERAGE_STRONG || 0,
    MODERATE: counts.CORE_COVERAGE_MODERATE || 0,
    WEAK: counts.CORE_COVERAGE_WEAK || 0,
    SOURCE_GAP: counts.SOURCE_GAP || 0,
    ZERO: counts.ZERO_CONFIRMED_PROPERTIES || 0,
    ZERO_CENSUS_GEOGRAPHIES: matrix
      .filter((r) => r.coverage_status === "ZERO_CONFIRMED_PROPERTIES")
      .map((r) => r.geography),
    TOP_10_GEOGRAPHIC_GAPS: matrix.slice(0, 10),
    BRAZIL_SANITY_CHECK_STATUS: brazil.status,
    BRAZIL_SANITY: brazil,
    FULL_52_GEOGRAPHY_MATRIX_PATH: matrixPath,
    FOUNDER_DECISION_REQUIRED: "NO",
    FOUNDER_DECISION: null,
    generated_at,
  };

  writeJson(
    path.join(ROOT, "reports/research-engine-v2/full-cala-core-identity-foundation-final.json"),
    final
  );
  writeMd(
    path.join(ROOT, "reports/research-engine-v2/full-cala-core-identity-foundation-final.md"),
    `# Core Identity Foundation Closure\n\n**Status:** \`${status}\`\n\nCensus ${censusBefore} → ${censusAfter} (15k gap ${targetGap})\n\nActive holds ${recon.prior_total_remaining_holds} → ${recon.active_count}\n\nState applicable completeness: ${comp.STATE_REGION_COMPLETENESS_OF_APPLICABLE}% · City ${comp.CITY_COMPLETENESS}%\n`
  );

  log(
    `[foundation] DONE status=${status} census ${censusBefore}→${censusAfter} active_holds=${recon.active_count}`
  );
  return final;
}
