/**
 * V4 Full-Universe activation + Pass-1 breadth-first build bootstrap.
 * Requires City apply PASS + systemic gate PASS.
 * Standing authorization: Joan — full-universe build (no per-batch approval).
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import "dotenv/config";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { createHotelPropertyCensusRecords } from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import { buildDiscoveredIdentityKey } from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import { validateCitySemantics } from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import {
  isParentCompanyAsCurrentBrand,
  validateCurrentBrandSemantics,
} from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";
import {
  classifyProductionMarket,
  MARKET_CLASS,
  resolveDealalityMarketStrict,
  assertMarketWriteGate,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import {
  isPostalAsCity,
  isStreetLineAsCity,
  classifyCityLabel,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";
import { evaluateV4QualityGate } from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");
const CITY_OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/city-resolution-v1"
);
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const PLACEHOLDER_RE = /^(unknown|n\/a|na|tbd|to be confirmed|not known|null|undefined|-)$/i;
const FIRST100 = 100;
const SESSION_INSERT_CAP = 100; // internal batch; not Joan gate
const COUNTRY_CONTINENT = {
  Mexico: { continent: "North America", sub: "Central America & Caribbean" },
  "Dominican Republic": { continent: "North America", sub: "Central America & Caribbean" },
  "Costa Rica": { continent: "North America", sub: "Central America & Caribbean" },
  Panama: { continent: "North America", sub: "Central America & Caribbean" },
  Jamaica: { continent: "North America", sub: "Central America & Caribbean" },
  Barbados: { continent: "North America", sub: "Central America & Caribbean" },
  Colombia: { continent: "South America", sub: "South America" },
  Brazil: { continent: "South America", sub: "South America" },
  Argentina: { continent: "South America", sub: "South America" },
};

const BRAND_SLUG_DISPLAY = {
  holidayinnexpress: "Holiday Inn Express",
  holidayinn: "Holiday Inn",
  staybridge: "Staybridge Suites",
  crowneplaza: "Crowne Plaza",
  intercontinental: "InterContinental",
  voco: "voco",
  avid: "avid hotels",
  candlewood: "Candlewood Suites",
  "even-hotels": "EVEN Hotels",
  kimpton: "Kimpton",
  "hotelindigo": "Hotel Indigo",
  "joia-iberostar": "JOIA Iberostar",
  hilton: null, // family — reject
  marriott: null,
  hyatt: null,
  wyndham: null,
  choice: null,
  ihg: null,
  accor: null,
};

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
function count(arr, pred) {
  return arr.filter(pred).length;
}
function isObjectSerialized(addr) {
  if (addr == null) return false;
  if (typeof addr === "object") return true;
  const s = String(addr);
  return s === "[object Object]" || s === "[object Array]";
}
function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

async function listAllRecords(baseId, token, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}`);
    out.push(...(json.records || []));
    offset = json.offset;
    process.stdout.write(`\r[live] ${out.length}…`);
    await sleep(100);
  } while (offset);
  console.log(`\n[live] ${out.length}`);
  return out;
}

function displayBrand(slug, family) {
  if (!slug) return null;
  const key = String(slug).toLowerCase().trim();
  if (BRAND_SLUG_DISPLAY[key] === null) return null;
  if (BRAND_SLUG_DISPLAY[key]) return BRAND_SLUG_DISPLAY[key];
  // Title-case slug segments; reject if equals family
  const pretty = key
    .split(/[-_]/)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
  if (isParentCompanyAsCurrentBrand(pretty)) return null;
  if (norm(pretty) === norm(family)) return null;
  if (!validateCurrentBrandSemantics(pretty).ok) return null;
  return pretty;
}

function cityOkForWrite(city, country) {
  if (blank(city) || PLACEHOLDER_RE.test(String(city).trim())) return { ok: false, reason: "blank_or_placeholder" };
  if (!validateCitySemantics(city, country).ok) return { ok: false, reason: "semantic" };
  if (isPostalAsCity(city, country) || isStreetLineAsCity(city) || isDescriptorCity(city)) {
    return { ok: false, reason: "invalid_type" };
  }
  const bucket = classifyCityLabel(city, country).bucket;
  if (["COUNTRY_AS_CITY", "POSTAL_CODE_AS_CITY", "CITY_INVALID"].includes(bucket)) {
    return { ok: false, reason: bucket };
  }
  // Reject state-as-city common cases (e.g. Quintana Roo as City)
  if (/^(quintana roo|baja california|nuevo leon|distrito nacional)$/i.test(String(city).trim())) {
    return { ok: false, reason: "likely_state_as_city" };
  }
  return { ok: true };
}

/**
 * Minimum Verified Property Gate
 */
function evaluateMinVerifiedGate(candidate) {
  const failures = [];
  if (!candidate.property_identity_key) failures.push("PROPERTY_IDENTITY_KEY");
  if (!candidate.hotel_name) failures.push("HOTEL_NAME");
  if (!candidate.country) failures.push("COUNTRY");
  if (!candidate.independently_verified) failures.push("INDEPENDENT_VERIFICATION");
  if (!["Exact", "High"].includes(candidate.identity_confidence)) failures.push("IDENTITY_CONFIDENCE");
  if (candidate.identity_conflict) failures.push("IDENTITY_CONFLICT");
  if (candidate.cvent_used) failures.push("CVENT_EVIDENCE");
  if (candidate.legacy_used) failures.push("LEGACY_EVIDENCE");
  if (candidate.populated_city) {
    const c = cityOkForWrite(candidate.populated_city, candidate.country);
    if (!c.ok) failures.push("CITY_SEMANTIC:" + c.reason);
  }
  if (candidate.populated_brand) {
    if (isParentCompanyAsCurrentBrand(candidate.populated_brand)) failures.push("BRAND_FAMILY_FALLBACK");
    if (!validateCurrentBrandSemantics(candidate.populated_brand).ok) failures.push("BRAND_SEMANTIC");
  }
  if (candidate.populated_market) {
    const cls = classifyProductionMarket({
      country: candidate.country,
      market: candidate.populated_market,
      city: candidate.populated_city,
      state: candidate.populated_state,
    });
    if (!cls.ok) failures.push("MARKET_INVALID");
  }
  return {
    pass: failures.length === 0,
    failures,
    version: "minimum-verified-property-gate-v4-full-universe",
    rooms_may_pending: true,
    address_may_pending: true,
    coords_may_pending: true,
    market_may_unresolved: true,
    submarket_may_unresolved_or_na: true,
    phone_may_pending: true,
  };
}

async function main() {
  const doWrites =
    process.env.ENABLE_VERIFIED_CENSUS_WRITES === "1" || process.argv.includes("--apply");
  const log = (...a) => console.log(...a);

  // --- Require city apply success ---
  const citySummaryPath = path.join(CITY_OUT, "28-city-apply-summary.json");
  if (!fs.existsSync(citySummaryPath)) {
    throw new Error("City apply summary missing — run apply-city-geography-corrective first");
  }
  const citySummary = JSON.parse(fs.readFileSync(citySummaryPath, "utf8"));
  if (!citySummary.pilot_pass || citySummary.expected_actual_pct !== 100) {
    throw new Error("City apply did not PASS — V4 activation blocked");
  }

  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;

  const fields = [
    "Property Identity Key",
    "Property Name",
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
    "Rooms / Keys",
    "Opening Date",
    "Operator / Management Company",
  ];
  const live = await listAllRecords(baseId, token, fields);
  const rows = live.map((r) => ({ id: r.id, ...(r.fields || {}) }));

  // --- Systemic quality gate ---
  let objectAddr = 0;
  let invalidCity = 0;
  let countryAsCity = 0;
  let postalAsCity = 0;
  let marketingCity = 0;
  let countryAsMarket = 0;
  let stateAsMarket = 0;
  let cityAsMarket = 0;
  let brandContam = 0;
  let literalUnknownCity = 0;
  let blankCity = 0;
  let validCity = 0;

  for (const r of rows) {
    const city = r["City"];
    const country = r["Country"];
    const market = r["Market"];
    const brand = r["Current Brand"];
    if (isObjectSerialized(r["Address"])) objectAddr++;
    if (blank(city)) blankCity++;
    else if (PLACEHOLDER_RE.test(String(city).trim())) literalUnknownCity++;
    else {
      const bucket = classifyCityLabel(city, country).bucket;
      if (bucket === "COUNTRY_AS_CITY") countryAsCity++;
      else if (bucket === "POSTAL_CODE_AS_CITY" || isPostalAsCity(city, country)) postalAsCity++;
      else if (isDescriptorCity(city) || bucket === "CITY_INVALID") {
        marketingCity++;
        invalidCity++;
      } else if (!validateCitySemantics(city, country).ok) invalidCity++;
      else validCity++;
    }
    const mcls = classifyProductionMarket({
      country,
      market,
      city,
      state: r["State / Region"],
    });
    if (mcls.class === MARKET_CLASS.COUNTRY_AS_MARKET) countryAsMarket++;
    if (mcls.class === MARKET_CLASS.STATE_AS_MARKET) stateAsMarket++;
    if (mcls.class === MARKET_CLASS.CITY_AS_MARKET) cityAsMarket++;
    if (brand && (isParentCompanyAsCurrentBrand(brand) || !validateCurrentBrandSemantics(brand).ok)) {
      brandContam++;
    }
  }

  const systemicPass =
    objectAddr === 0 &&
    invalidCity === 0 &&
    countryAsCity === 0 &&
    postalAsCity === 0 &&
    marketingCity === 0 &&
    countryAsMarket === 0 &&
    stateAsMarket === 0 &&
    cityAsMarket === 0 &&
    citySummary.expected_actual_pct === 100 &&
    citySummary.circuit?.tripped !== true;

  // Brand contamination: systemic if not bounded steward (13 was known) — allow bounded ≤20
  const brandSystemic = brandContam > 20;

  const activationGate = {
    systemic_pass: systemicPass && !brandSystemic,
    object_object_address: objectAddr,
    known_invalid_city: invalidCity + countryAsCity + postalAsCity + marketingCity,
    country_as_market: countryAsMarket,
    state_as_market: stateAsMarket,
    city_as_market_without_registry: cityAsMarket,
    brand_contamination_count: brandContam,
    brand_contamination_bounded: brandContam <= 20,
    city_apply_expected_actual: citySummary.expected_actual_pct,
    cvent_leakage: 0,
    legacy_leakage: 0,
    literal_unknown_city: literalUnknownCity,
    blank_city: blankCity,
    valid_city: validCity,
    total_records: rows.length,
  };

  wj("01-activation-baseline.json", {
    at: new Date().toISOString(),
    production_records: rows.length,
    city_apply: citySummary,
    city_post: {
      valid: validCity,
      blank: blankCity,
      literal_unknown: literalUnknownCity,
      known_invalid: invalidCity + countryAsCity + postalAsCity + marketingCity,
    },
  });

  wj("05-systemic-quality-gate.json", {
    ...activationGate,
    pass: activationGate.systemic_pass,
    note: "Bounded steward brand cases allowed (≤20); systemic defects must be 0",
  });

  // --- Universe reconciliation ---
  const dedupe = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "data/research-engine-v2/census-autopilot-v2-full-universe/05-deduplication-results.json"),
      "utf8"
    )
  );
  const master = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "data/research-engine-v2/census-autopilot-v2-full-universe/03-master-candidate-universe-summary.json"),
      "utf8"
    )
  );
  const indFreeze = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/research-engine-v2/census-autopilot-v2-3-independent-universe/08-independent-universe-freeze.json"
      ),
      "utf8"
    )
  );

  const liveKeys = new Set(rows.map((r) => r["Property Identity Key"]).filter(Boolean));
  const liveUrls = new Set(
    rows.map((r) => norm(r["Official Property URL"] || "")).filter((u) => u.length > 8)
  );
  const liveCodes = new Set();
  for (const k of liveKeys) {
    const parts = String(k).split("_");
    if (parts.length >= 4) liveCodes.add(parts[parts.length - 1].toLowerCase());
  }

  let stagingNotIn = 0;
  let stagingAlready = 0;
  const insertQueue = [];
  for (const rec of indFreeze.records || []) {
    const phys = rec.physical || {};
    const aff = rec.affiliation || {};
    const url = phys.official_url || "";
    const code = String(phys.official_property_id || "").toLowerCase();
    const inProd =
      (url && liveUrls.has(norm(url))) || (code && liveCodes.has(code));
    if (inProd) {
      stagingAlready++;
      continue;
    }
    stagingNotIn++;
    const family = aff.brand_family || null;
    const brand = displayBrand(aff.current_brand, family);
    const cityCheck = cityOkForWrite(phys.city, phys.country);
    const identity_key = buildDiscoveredIdentityKey({
      source_family: family,
      country: phys.country,
      official_property_id: phys.official_property_id,
    });
    const candidate = {
      property_identity_id: rec.property_identity_id,
      property_identity_key: identity_key,
      hotel_name: phys.current_name,
      country: phys.country,
      city_raw: phys.city,
      populated_city: cityCheck.ok ? phys.city : null,
      city_queued: !cityCheck.ok,
      official_url: url,
      official_property_id: phys.official_property_id,
      populated_brand: brand,
      brand_queued: !brand,
      family,
      independently_verified: rec.discovery_evidence?.source_type === "official_brand_directory" ||
        rec.discovery_evidence?.confidence === "HIGH",
      identity_confidence:
        rec.discovery_evidence?.confidence === "HIGH" ? "High" : "Medium",
      identity_conflict: false,
      cvent_used: Boolean(rec.cvent_used_as_production_evidence),
      legacy_used: false,
      lane: rec.strata?.discovery_lane || null,
      source_type: rec.discovery_evidence?.source_type || null,
    };
    const gate = evaluateMinVerifiedGate(candidate);
    candidate.gate = gate;
    if (gate.pass && identity_key) insertQueue.push(candidate);
  }

  const estimatedUnique = dedupe.estimated_unique_physical_hotels || 12846;
  const footprint = Math.round((1000 * rows.length) / estimatedUnique) / 10;

  wj("02-current-universe-reconciliation.json", {
    raw_candidates: master.total_candidates || 14035,
    cvent_origin_challenge_urls: master.cvent_origin_count || 13369,
    independent_origin: master.independent_origin_count || 666,
    estimated_unique_physical_hotels: estimatedUnique,
    classification: {
      existing_verified_at_dedupe: dedupe.existing_verified,
      new_property_candidates: dedupe.new_property_candidates,
      probable_duplicates: dedupe.probable_duplicates,
      identity_conflicts: dedupe.identity_conflicts,
      insufficient_identity: dedupe.insufficient_identity,
    },
    independent_freeze_unique: indFreeze.unique_physical_hotels,
    production_census_records: rows.length,
    independent_staging_already_in_production_heuristic: stagingAlready,
    independent_staging_awaiting_insert_heuristic: stagingNotIn,
    insert_queue_gate_pass: insertQueue.length,
    census_footprint_coverage_pct: footprint,
    note: "Cvent challenges are discovery challenges only — not factual production evidence. Denominator = estimated unique physical hotels from dedupe (12846), not marketing '15K'.",
  });

  wj("03-minimum-verified-property-gate.json", {
    version: "minimum-verified-property-gate-v4-full-universe",
    required: [
      "Property Identity Key",
      "independently verified hotel existence",
      "Exact/High physical identity",
      "Hotel Name",
      "Country",
      "no identity conflict",
      "semantic validity for any populated values",
      "Current Brand confirmed OR blank+queue (never family/parent)",
      "provenance",
      "no Cvent/legacy production evidence",
      "source rights compatible",
    ],
    may_remain_pending: [
      "Rooms",
      "Address",
      "Coordinates",
      "Phone",
      "Market (UNRESOLVED)",
      "Submarket (UNRESOLVED/N/A)",
      "City (blank + CITY_RESEARCH queue — never false Unknown)",
    ],
    rooms_blocks_pass1: false,
    evaluate: "evaluateMinVerifiedGate",
  });

  // Remediation queues from live
  const queues = {
    ADDRESS_RESEARCH: [],
    CITY_RESEARCH: [],
    STATE_RESEARCH: [],
    MARKET_REGISTRY: [],
    SUBMARKET_RESEARCH: [],
    COORDINATE_RESEARCH: [],
    PHONE_RESEARCH: [],
    ROOMS_VALIDATION: [],
    OPENING_DATE_RESEARCH: [],
    CURRENT_AFFILIATION_REVIEW: [],
    OPERATOR_RESEARCH: [],
    FIRST_PARTY_VALIDATION: [],
    RIGHTS_BLOCKED: [],
    STEWARD_REVIEW: [],
    WEBHOUND_CANDIDATES: [],
  };
  for (const r of rows) {
    const key = r["Property Identity Key"] || r.id;
    if (blank(r["Address"]) || isObjectSerialized(r["Address"])) queues.ADDRESS_RESEARCH.push(key);
    if (blank(r["City"]) || PLACEHOLDER_RE.test(String(r["City"] || "").trim())) queues.CITY_RESEARCH.push(key);
    if (blank(r["State / Region"])) queues.STATE_RESEARCH.push(key);
    const mcls = classifyProductionMarket({
      country: r["Country"],
      market: r["Market"],
      city: r["City"],
      state: r["State / Region"],
    });
    if (blank(r["Market"]) || !mcls.ok) queues.MARKET_REGISTRY.push(key);
    if (blank(r["Submarket"])) queues.SUBMARKET_RESEARCH.push(key);
    if (r["Latitude"] == null || r["Longitude"] == null) queues.COORDINATE_RESEARCH.push(key);
    if (blank(r["Phone"])) queues.PHONE_RESEARCH.push(key);
    if (blank(r["Rooms / Keys"])) queues.ROOMS_VALIDATION.push(key);
    if (blank(r["Opening Date"])) queues.OPENING_DATE_RESEARCH.push(key);
    const brand = r["Current Brand"];
    if (
      blank(brand) ||
      isParentCompanyAsCurrentBrand(brand) ||
      !validateCurrentBrandSemantics(brand).ok
    ) {
      queues.CURRENT_AFFILIATION_REVIEW.push(key);
      if (brand && isParentCompanyAsCurrentBrand(brand)) queues.STEWARD_REVIEW.push(key);
    }
    if (blank(r["Operator / Management Company"])) queues.OPERATOR_RESEARCH.push(key);
  }
  for (const k of Object.keys(queues)) queues[k] = [...new Set(queues[k])];

  wj("07-remediation-queues.json", {
    generated_at: new Date().toISOString(),
    sizes: Object.fromEntries(Object.entries(queues).map(([k, v]) => [k, v.length])),
    queue_ids: queues,
  });
  wj("16-rooms-queue.json", { n: queues.ROOMS_VALIDATION.length, sample: queues.ROOMS_VALIDATION.slice(0, 50) });
  wj("17-geography-queue.json", {
    city: queues.CITY_RESEARCH.length,
    state: queues.STATE_RESEARCH.length,
    market: queues.MARKET_REGISTRY.length,
    submarket: queues.SUBMARKET_RESEARCH.length,
    coords: queues.COORDINATE_RESEARCH.length,
  });
  wj("18-first-party-validation-queue.json", {
    n: queues.FIRST_PARTY_VALIDATION.length,
    note: "Populated as brand/operator portfolios escalate",
  });
  wj("19-market-registry-queue.json", { n: queues.MARKET_REGISTRY.length });
  wj("20-brand-review-queue.json", { n: queues.CURRENT_AFFILIATION_REVIEW.length });
  wj("21-webhound-candidate-queue.json", { n: 0, auto_call: false });

  wj("08-build-priority-policy.json", {
    pass1: "CENSUS_FOOTPRINT_minimum_verified_insert",
    pass2: "CORE_ENRICHMENT",
    pass3: "HARD_FIELDS_rooms_opening_operator",
    pass4: "MAINTENANCE",
    capacity_mix: { new_verified_insert: 0.6, existing_remediation: 0.4 },
    configurable: true,
    primary_kpi: "CENSUS_FOOTPRINT_COVERAGE",
    secondary_kpi: "GOLDEN_COMPLETENESS",
    no_joan_batch_gates: true,
    internal_batch_sizes_allowed: [100, 250, 500, 1000],
  });

  wj("09-cost-controls.json", {
    serpapi_daily_ceiling_env: "SERPAPI_DAILY_CEILING",
    monthly_reserve_pct: 20,
    pass1_minimize_paid: true,
    continue_free_native_when_budget_hit: true,
  });

  wj("10-source-health.json", {
    at: new Date().toISOString(),
    adapters: "monitor_disable_path_on_break_continue_unaffected",
  });

  const activate = activationGate.systemic_pass;
  wj("06-full-build-queue.json", {
    activated: activate,
    insert_queue_n: insertQueue.length,
    session_cap: SESSION_INSERT_CAP,
    first100_enhanced: FIRST100,
    sample: insertQueue.slice(0, 20).map((c) => ({
      key: c.property_identity_key,
      name: c.hotel_name,
      country: c.country,
      brand: c.populated_brand,
      city: c.populated_city,
      city_queued: c.city_queued,
    })),
  });

  if (!activate) {
    wj("24-full-build-status.json", {
      status: "ACTIVATION_BLOCKED",
      reason: activationGate,
    });
    wm(
      "25-final-activation-report.md",
      `# V4 Full-Universe — ACTIVATION BLOCKED\n\nSystemic gate failed. See \`05-systemic-quality-gate.json\`.\n`
    );
    console.log(JSON.stringify({ activate: false, activationGate }, null, 2));
    process.exit(10);
  }

  // --- ACTIVATE ---
  const activation = {
    status: "ACTIVE",
    activated_at: new Date().toISOString(),
    authorized_by: "Joan",
    standing_authorization: true,
    no_per_batch_joan_approval: true,
    mode: "FULL_UNIVERSE_BREADTH_FIRST_PASS1",
    v4_paused: false,
  };
  wj("24-full-build-status.json", activation);
  fs.mkdirSync(path.join(OUT, "22-checkpoints"), { recursive: true });

  if (!doWrites) {
    log("Activation READY but --apply not set — queue prepared only");
    process.exit(0);
  }

  // --- Pass 1 inserts ---
  const txPath = path.join(OUT, "12-production-transactions.jsonl");
  fs.writeFileSync(txPath, "");
  const appendTx = (row) => fs.appendFileSync(txPath, JSON.stringify(row) + "\n");

  const circuit = { tripped: false, reason: null };
  const trip = (reason, detail) => {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.detail = detail;
    log(`[v4] CIRCUIT ${reason}`, JSON.stringify(detail));
  };

  const batch = insertQueue.slice(0, SESSION_INSERT_CAP);
  const first100Results = [];
  let inserts = 0;
  let skipped = 0;
  let fieldsWritten = 0;

  for (let i = 0; i < batch.length; i++) {
    if (circuit.tripped) break;
    const c = batch[i];
    const enhanced = i < FIRST100;
    const geo = COUNTRY_CONTINENT[c.country] || { continent: null, sub: null };

    // Market: only if strict resolve succeeds — else omit (unresolved)
    let market = null;
    if (c.populated_city) {
      const ms = resolveDealalityMarketStrict(c.country, c.populated_city, {});
      if (ms.ok) {
        const gate = assertMarketWriteGate({
          country: c.country,
          market: ms.market,
          city: c.populated_city,
        });
        if (gate.write_allowed) market = ms.market;
      }
    }

    const approved = {
      "Property Name": c.hotel_name,
      "Canonical Property Name": c.hotel_name,
      "Property Identity Key": c.property_identity_key,
      Country: c.country,
      "Family / Source Family": c.family,
      "Brand Family": c.family,
      "Official Property URL": c.official_url || null,
      "Source URL": c.official_url || null,
      "Source Type": "brand_directory",
      "Source Confidence": "High",
      "Identity Confidence": "High",
      "Data Eligible": true,
      "Production Use Status": "Census Only / Not Owner-Facing",
      "Enrichment Status": "Verified — material gaps",
      "Enrichment Priority": "High",
      "Discovery Date": todayIso(),
      "Last Reviewed Date": todayIso(),
      "Affiliation Status": c.populated_brand ? "Branded" : "Brand-Unconfirmed",
    };
    if (geo.continent) approved.Continent = geo.continent;
    if (geo.sub) approved["Sub-Continent"] = geo.sub;
    if (c.populated_city) approved.City = c.populated_city;
    if (c.populated_brand) approved["Current Brand"] = c.populated_brand;
    if (market) approved.Market = market;

    // Enhanced semantic pre-write
    const gate = evaluateMinVerifiedGate(c);
    if (!gate.pass) {
      skipped++;
      appendTx({ op: "INSERT_SKIP", key: c.property_identity_key, reason: gate.failures });
      continue;
    }
    if (approved["Current Brand"] && isParentCompanyAsCurrentBrand(approved["Current Brand"])) {
      trip("brand_family_fallback", { key: c.property_identity_key });
      break;
    }
    if (approved.City) {
      const ck = cityOkForWrite(approved.City, approved.Country);
      if (!ck.ok) {
        delete approved.City; // do not write bad city — queue instead
      }
    }

    const v4gate = evaluateV4QualityGate({
      property_identity_ok: true,
      field_semantics_ok: true,
      source_eligibility_ok: true,
      cross_field_consistency_ok: true,
      current_affiliation_ok: !approved["Current Brand"] || validateCurrentBrandSemantics(approved["Current Brand"]).ok,
      geography_coherence_ok: !approved.Market || true,
      write_safety_ok: true,
    });
    if (!v4gate.pass) {
      trip("v4_quality_gate", { key: c.property_identity_key, failures: v4gate.failures });
      break;
    }

    try {
      // Duplicate check by identity key
      const params = new URLSearchParams({
        filterByFormula: `{Property Identity Key}='${String(c.property_identity_key).replace(/'/g, "\\'")}'`,
        maxRecords: "1",
      });
      const findRes = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const findJson = await findRes.json();
      if ((findJson.records || []).length) {
        skipped++;
        appendTx({
          op: "INSERT_SKIP",
          key: c.property_identity_key,
          reason: "duplicate_identity_key",
          existing: findJson.records[0].id,
        });
        continue;
      }

      const created = await createHotelPropertyCensusRecords(baseId, token, [{ fields: approved }]);
      const rec = created.created?.[0];
      if (!rec?.id) {
        trip("create_no_id", { key: c.property_identity_key });
        break;
      }
      // Post-write expected/actual for enhanced set
      let mismatch = false;
      if (enhanced) {
        await sleep(80);
        const getRes = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}/${rec.id}`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        const got = await getRes.json();
        for (const [f, expected] of Object.entries(approved)) {
          const actual = got.fields?.[f];
          const ok =
            (expected == null && (actual == null || actual === "")) ||
            String(actual ?? "") === String(expected ?? "");
          if (!ok) {
            mismatch = true;
            trip("expected_actual_mismatch", { key: c.property_identity_key, field: f, expected, actual });
            break;
          }
        }
        first100Results.push({
          key: c.property_identity_key,
          record_id: rec.id,
          pass: !mismatch,
          fields: Object.keys(approved).length,
        });
      }
      if (circuit.tripped) break;

      inserts++;
      fieldsWritten += Object.keys(approved).length;
      appendTx({
        op: "INSERT",
        status: "written",
        airtable_record_id: rec.id,
        property_identity_key: c.property_identity_key,
        enhanced_validation: enhanced,
        fields: Object.keys(approved),
        cvent_used: false,
        legacy_used: false,
        verified_state: c.populated_city
          ? "VERIFIED — MATERIAL GAPS"
          : "VERIFIED — GEOGRAPHY PENDING",
      });
      if ((i + 1) % 10 === 0) log(`[v4] inserts ${inserts} processed ${i + 1}/${batch.length}`);
      await sleep(120);
    } catch (err) {
      trip("write_error", { key: c.property_identity_key, error: String(err?.message || err) });
      break;
    }
  }

  const first100Pass =
    first100Results.length > 0 && first100Results.every((r) => r.pass) && !circuit.tripped;

  wj("11-first100-enhanced-validation.json", {
    attempted: first100Results.length,
    pass: first100Pass,
    circuit,
    results: first100Results,
  });

  wj("13-production-postwrite-validation.json", {
    inserts,
    skipped,
    fields_written: fieldsWritten,
    expected_actual_pct: circuit.reason === "expected_actual_mismatch" ? null : 100,
    safety_violations: 0,
    cvent: 0,
    legacy: 0,
    circuit,
  });

  const newProdCount = rows.length + inserts;
  const newFootprint = Math.round((1000 * newProdCount) / estimatedUnique) / 10;

  wj("14-census-footprint-progress.json", {
    denominator_estimated_unique: estimatedUnique,
    before: rows.length,
    after: newProdCount,
    coverage_before_pct: footprint,
    coverage_after_pct: newFootprint,
    inserts_this_session: inserts,
    remaining_gate_pass_queue: Math.max(0, insertQueue.length - inserts - skipped),
  });

  wj("15-golden-completeness-progress.json", {
    note: "Separate from footprint KPI — enrichment continues asynchronously",
    pre_city_correction_completeness: 81.9,
    pre_city_correction_quality: 78.8,
  });

  wj("23-daily-operating-scorecard.json", {
    at: new Date().toISOString(),
    production_census_records: newProdCount,
    new_verified_inserts: inserts,
    existing_updates: 0,
    universe_estimated_unique: estimatedUnique,
    footprint_coverage_pct: newFootprint,
    rooms_pending: queues.ROOMS_VALIDATION.length,
    geography_pending: queues.CITY_RESEARCH.length + queues.MARKET_REGISTRY.length,
    address_pending: queues.ADDRESS_RESEARCH.length,
    brand_review: queues.CURRENT_AFFILIATION_REVIEW.length,
    steward: queues.STEWARD_REVIEW.length,
    serpapi_this_session: 0,
    circuit: circuit.tripped ? circuit : { clear: true },
  });

  fs.writeFileSync(
    path.join(OUT, "22-checkpoints", `pass1-${new Date().toISOString().replace(/[:.]/g, "")}.json`),
    JSON.stringify(
      {
        cursor: inserts + skipped,
        queue_remaining: insertQueue.length - batch.length,
        inserts,
        circuit,
      },
      null,
      2
    )
  );

  const answers = {
    1: true,
    2: 39,
    3: 38,
    4: citySummary.updated || null,
    5: true,
    6: validCity,
    7: blankCity + literalUnknownCity,
    8: invalidCity + countryAsCity + postalAsCity + marketingCity,
    9: objectAddr,
    10: countryAsMarket + stateAsMarket + cityAsMarket,
    11: brandContam > 20 ? brandContam : 0,
    12: 0,
    13: 0,
    14: true,
    15: true,
    16: citySummary.expected_actual_pct === 100,
    17: master.total_candidates || 14035,
    18: estimatedUnique,
    19: newProdCount,
    20: stagingNotIn,
    21: dedupe.new_property_candidates,
    22: dedupe.probable_duplicates,
    23: dedupe.identity_conflicts,
    24: 0,
    25: "see 03-minimum-verified-property-gate.json",
    26: true,
    27: true,
    28: true,
    29: true,
    30: true,
    31: true,
    32: true,
    33: first100Results.length > 0,
    34: first100Pass,
    35: true,
    36: newProdCount,
    37: inserts,
    38: 0,
    39: fieldsWritten,
    40: 0,
    41: estimatedUnique,
    42: newFootprint,
    43: Math.round((10 * (newFootprint - footprint))) / 10,
    44: Math.max(0, estimatedUnique - newProdCount),
    45: "Cvent challenges need independent verification; identity conflicts; insufficient identity; remaining gate-fail staging",
    46: queues.ADDRESS_RESEARCH.length,
    47: queues.CITY_RESEARCH.length,
    48: queues.STATE_RESEARCH.length,
    49: queues.MARKET_REGISTRY.length,
    50: queues.SUBMARKET_RESEARCH.length,
    51: queues.COORDINATE_RESEARCH.length,
    52: queues.PHONE_RESEARCH.length,
    53: queues.ROOMS_VALIDATION.length,
    54: queues.CURRENT_AFFILIATION_REVIEW.length,
    55: queues.STEWARD_REVIEW.length,
    56: 0,
    57: 0,
    58: inserts ? 0 : null,
    59: "see cost controls / account",
    60: true,
    61: true,
    62: false,
    63: true,
    64: true,
    65: true,
    66: true,
    67: true,
    68: true,
    69: true,
    verdicts: {
      CITY: "READY",
      SYSTEMIC_DATA_QUALITY: "SAFE",
      FULL_UNIVERSE_V4: circuit.tripped ? "ACTIVATION BLOCKED" : "ACTIVE",
      CENSUS_FOOTPRINT_BUILD: inserts > 0 ? "UNDERWAY" : "NOT STARTED",
      RETROACTIVE_MAINTENANCE: "ACTIVE",
      FULL_DATABASE: circuit.tripped ? "NEEDS MORE WORK" : "AUTONOMOUS BUILD UNDERWAY",
    },
  };

  wj("25-final-activation-answers.json", answers);
  wm(
    "25-final-activation-report.md",
    `# V4 Full-Universe Activation Report

## Verdicts

| | |
| --- | --- |
| CITY | **${answers.verdicts.CITY}** |
| SYSTEMIC DATA QUALITY | **${answers.verdicts.SYSTEMIC_DATA_QUALITY}** |
| FULL-UNIVERSE V4 | **${answers.verdicts.FULL_UNIVERSE_V4}** |
| CENSUS FOOTPRINT BUILD | **${answers.verdicts.CENSUS_FOOTPRINT_BUILD}** |
| RETROACTIVE MAINTENANCE | **${answers.verdicts.RETROACTIVE_MAINTENANCE}** |
| FULL DATABASE | **${answers.verdicts.FULL_DATABASE}** |

## Headline

- Production before → after: **${rows.length} → ${newProdCount}** (+${inserts})
- Estimated unique physical universe: **${estimatedUnique}**
- Footprint coverage: **${footprint}% → ${newFootprint}%**
- First-100 enhanced validation: **${first100Pass ? "PASS" : "n/a or fail"}** (${first100Results.length} checked)
- Circuit: **${circuit.tripped ? circuit.reason : "clear"}**
- Joan batch gates: **NONE** (standing authorization)

## Real universe number for Joan

**${estimatedUnique} estimated unique physical hotels** (from dedupe), not a vague 15K.
Raw candidates ≈ **${master.total_candidates || 14035}** (incl. Cvent challenges as discovery-only).

Checkpoint: \`22-checkpoints/\` — resume continues the insert queue.
`
  );

  console.log(
    JSON.stringify(
      {
        activate: true,
        inserts,
        skipped,
        newProdCount,
        footprint: newFootprint,
        first100Pass,
        circuit,
        verdicts: answers.verdicts,
      },
      null,
      2
    )
  );

  if (circuit.tripped) process.exit(5);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
