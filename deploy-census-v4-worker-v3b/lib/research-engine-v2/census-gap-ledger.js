/**
 * Census Gap Ledger — per-record / per-field missing-value classification.
 * Persistent output for Autopilot Controller v3.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";
import {
  classifyCensusReviewReasons,
  buildActiveBrandIndex,
} from "./census-brand-governance.js";
import { isDescriptorCity } from "./census-city-state-normalizer.js";
import { resolveStateRegionFromCity } from "./census-city-to-state-map.js";
import { resolveMarketFromCity } from "./census-region-market-map.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const GAP_LEDGER_VERSION = "census-gap-ledger-v1";

export const GAP_REASON = Object.freeze({
  OFFICIAL_SOURCE_MISSING: "official_source_missing",
  OFFICIAL_PROPERTY_URL_MISSING: "official_property_url_missing",
  OFFICIAL_SOURCE_BLOCKED: "official_source_blocked",
  BOT_BLOCKED: "bot_blocked",
  INSUFFICIENT_STRUCTURED_DATA: "insufficient_structured_data",
  CITY_MISSING_OR_AMBIGUOUS: "city_missing_or_ambiguous",
  BRAND_DIRTY: "brand_dirty",
  PARENT_DIRTY: "parent_dirty",
  DUPLICATE_RISK: "duplicate_risk",
  ADDRESS_MISSING: "address_missing",
  ADDRESS_NOT_STREET_LEVEL: "address_not_street_level",
  PHONE_MISSING_FROM_OFFICIAL_SOURCE: "phone_missing_from_official_source",
  ROOMS_MISSING_FROM_OFFICIAL_SOURCE: "rooms_missing_from_official_source",
  MARKET_MAPPING_MISSING: "market_mapping_missing",
  SUBMARKET_MAPPING_MISSING: "submarket_mapping_missing",
  MAPBOX_WAITING_FOR_HIGH_ADDRESS: "mapbox_waiting_for_high_address",
  UNSUPPORTED_OR_AMBIGUOUS: "unsupported_or_ambiguous",
  GOVERNANCE_HOLD_ONLY: "governance_hold_only",
  DATA_QUALITY_HOLD: "data_quality_hold",
  STATE_MAPPING_MISSING: "state_mapping_missing",
  FIELD_COMPLETE: "field_complete",
});

/** Field importance for prioritization (lower = higher priority). */
export const FIELD_PRIORITY = Object.freeze({
  "Official Property URL": 1,
  "Source URL": 1,
  "State / Region": 2,
  Market: 3,
  Address: 4,
  Phone: 5,
  Latitude: 6,
  Longitude: 6,
  "Rooms / Keys": 7,
  Submarket: 8,
  Brand: 0,
  "Brand Family": 0,
  City: 0,
  Country: 0,
  "Clean Core": 0,
});

export const TRACKED_FIELDS = Object.freeze([
  "State / Region",
  "Market",
  "Submarket",
  "Official Property URL",
  "Source URL",
  "Address",
  "Address Source URL",
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
  "Rooms Source URL",
  "Current Brand",
  "Brand Family",
  "City",
  "Country",
]);

function isBlank(v) {
  return v == null || !String(v).trim();
}

function isHttpUrl(v) {
  return /^https?:\/\//i.test(String(v || "").trim());
}

function isOtaOrBlockedUrl(url) {
  const u = String(url || "").toLowerCase();
  return /booking\.com|expedia\.|hotels\.com|tripadvisor\.|google\.(com|co)|maps\.google|kayak\.|trivago\./i.test(
    u
  );
}

/**
 * Classify why a field is missing / weak and what strategy to try next.
 */
export function classifyFieldGap(record, fieldName, opts = {}) {
  const f = record?.fields || {};
  const activeIndex = opts.activeIndex || buildActiveBrandIndex(opts);
  const review =
    opts.review ||
    classifyCensusReviewReasons({ fields: f }, { activeIndex });
  const value = f[fieldName];
  const officialUrl = String(
    f[MAP_FIRST_PASS.officialUrl] || f["Official Property URL"] || ""
  ).trim();
  const sourceUrl = String(f[MAP_FIRST_PASS.sourceUrl] || f["Source URL"] || "").trim();
  const hasOfficial = isHttpUrl(officialUrl) && !isOtaOrBlockedUrl(officialUrl);
  const hasSource = isHttpUrl(sourceUrl) && !isOtaOrBlockedUrl(sourceUrl);
  const family = String(
    f["Brand Family"] || f["Family / Source Family"] || f[MAP_FIRST_PASS.family] || ""
  ).trim();
  const brand = String(f[MAP_FIRST_PASS.currentBrand] || f["Current Brand"] || "").trim();
  const city = String(f.City || "").trim();
  const country = String(f.Country || "").trim();
  const address = String(f.Address || "").trim();
  const addrHigh = String(f["Address Confidence"] || "").toLowerCase() === "high";
  const street = isStreetLevelAddress(address);

  /** @type {string} */
  let reason = GAP_REASON.INSUFFICIENT_STRUCTURED_DATA;
  /** @type {string} */
  let strategy = "official_property_page";
  let autopilotEligible = true;
  let stewardRequired = false;

  if (review.data_quality_review_required) {
    reason = GAP_REASON.DATA_QUALITY_HOLD;
    strategy = "steward_data_quality_review";
    autopilotEligible = false;
    stewardRequired = true;
  } else if (review.governance_only) {
    // Governance hold does not block field enrichment
    reason = GAP_REASON.GOVERNANCE_HOLD_ONLY;
  }

  const complete = (() => {
    if (fieldName === "Address") return street;
    if (fieldName === "Latitude" || fieldName === "Longitude") {
      return f.Latitude != null && f.Longitude != null;
    }
    if (fieldName === "Official Property URL" || fieldName === "Source URL") {
      return isHttpUrl(value) && !isOtaOrBlockedUrl(value);
    }
    return !isBlank(value);
  })();

  if (complete) {
    return {
      missing: false,
      field: fieldName,
      current_value: value ?? null,
      reason: GAP_REASON.FIELD_COMPLETE,
      next_best_source_strategy: null,
      autopilot_eligible: false,
      steward_required: false,
      source_url_available: hasSource,
      official_property_url_available: hasOfficial,
      source_family: family || null,
      confidence_needed: "High",
    };
  }

  if (isDescriptorCity(city) || isBlank(city)) {
    if (["State / Region", "Market", "Address", "Latitude", "Longitude"].includes(fieldName)) {
      reason = GAP_REASON.CITY_MISSING_OR_AMBIGUOUS;
      strategy = "steward_city_resolution";
      autopilotEligible = false;
      stewardRequired = true;
    }
  }

  switch (fieldName) {
    case "Official Property URL":
    case "Source URL":
      reason = GAP_REASON.OFFICIAL_PROPERTY_URL_MISSING;
      strategy = "official_parent_directory";
      break;
    case "State / Region": {
      const mapped = resolveStateRegionFromCity({
        city,
        country,
        state: value,
      });
      if (mapped.ok) {
        reason = GAP_REASON.STATE_MAPPING_MISSING; // blank but map-ready
        strategy = "deterministic_city_state_map";
      } else if (mapped.reason === "city_state_mapping_missing") {
        reason = GAP_REASON.STATE_MAPPING_MISSING;
        strategy = "steward_state_map_addition";
        stewardRequired = true;
        autopilotEligible = false;
      } else if (!hasOfficial && !hasSource) {
        reason = GAP_REASON.OFFICIAL_SOURCE_MISSING;
        strategy = "official_json_ld_addressRegion";
      } else {
        reason = GAP_REASON.INSUFFICIENT_STRUCTURED_DATA;
        strategy = "official_property_page";
      }
      break;
    }
    case "Market": {
      const m = resolveMarketFromCity({ city, country });
      if (m.ok) {
        reason = GAP_REASON.MARKET_MAPPING_MISSING;
        strategy = "approved_market_map";
      } else {
        reason = GAP_REASON.MARKET_MAPPING_MISSING;
        strategy = "steward_market_map_addition";
        stewardRequired = true;
        autopilotEligible = false;
      }
      break;
    }
    case "Submarket":
      reason = GAP_REASON.SUBMARKET_MAPPING_MISSING;
      strategy = "approved_submarket_rules_only";
      // leave blank unless High rule matches — steward if unsure
      stewardRequired = true;
      autopilotEligible = false;
      break;
    case "Address":
      if (!hasOfficial && !hasSource) {
        reason = GAP_REASON.OFFICIAL_PROPERTY_URL_MISSING;
        strategy = "official_parent_directory";
      } else if (!isBlank(address) && !street) {
        reason = GAP_REASON.ADDRESS_NOT_STREET_LEVEL;
        strategy = "official_property_page";
      } else {
        reason = GAP_REASON.ADDRESS_MISSING;
        strategy = "official_json_ld";
      }
      break;
    case "Address Source URL":
      reason = hasOfficial
        ? GAP_REASON.INSUFFICIENT_STRUCTURED_DATA
        : GAP_REASON.OFFICIAL_PROPERTY_URL_MISSING;
      strategy = "official_property_page";
      break;
    case "Latitude":
    case "Longitude":
      if (!(street && addrHigh && !isBlank(f["Address Source URL"]))) {
        reason = GAP_REASON.MAPBOX_WAITING_FOR_HIGH_ADDRESS;
        strategy = "wait_high_address_then_mapbox_permanent";
        autopilotEligible = street && addrHigh;
      } else if (review.data_quality_review_required) {
        reason = GAP_REASON.DATA_QUALITY_HOLD;
        autopilotEligible = false;
        stewardRequired = true;
      } else {
        reason = GAP_REASON.INSUFFICIENT_STRUCTURED_DATA;
        strategy = "mapbox_permanent";
      }
      break;
    case "Phone":
      reason = GAP_REASON.PHONE_MISSING_FROM_OFFICIAL_SOURCE;
      strategy = hasOfficial || hasSource ? "official_property_page" : "official_parent_directory";
      break;
    case "Rooms / Keys":
    case "Rooms Source URL":
      reason = GAP_REASON.ROOMS_MISSING_FROM_OFFICIAL_SOURCE;
      strategy = hasOfficial || hasSource ? "official_factsheet_or_catalog" : "official_parent_directory";
      break;
    case "Current Brand":
      reason = GAP_REASON.BRAND_DIRTY;
      strategy = "steward_brand_resolution";
      autopilotEligible = false;
      stewardRequired = true;
      break;
    case "Brand Family":
      reason = GAP_REASON.PARENT_DIRTY;
      strategy = "parent_company_normalization";
      break;
    case "City":
    case "Country":
      reason = GAP_REASON.CITY_MISSING_OR_AMBIGUOUS;
      strategy = "steward_city_resolution";
      autopilotEligible = false;
      stewardRequired = true;
      break;
    default:
      break;
  }

  if (review.governance?.status === "unsupported_or_ambiguous") {
    reason = GAP_REASON.UNSUPPORTED_OR_AMBIGUOUS;
    autopilotEligible = false;
    stewardRequired = true;
  }

  return {
    missing: true,
    field: fieldName,
    current_value: value ?? null,
    reason,
    next_best_source_strategy: strategy,
    autopilot_eligible: autopilotEligible && !review.data_quality_review_required,
    steward_required: stewardRequired || review.data_quality_review_required,
    source_url_available: hasSource,
    official_property_url_available: hasOfficial,
    source_family: family || null,
    brand: brand || null,
    confidence_needed: "High",
  };
}

/**
 * Build full gap ledger for a Census snapshot.
 * @param {object[]} records
 * @param {object} [opts]
 */
export function buildCensusGapLedger(records = [], opts = {}) {
  const activeIndex = opts.activeIndex || buildActiveBrandIndex(opts);
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary(opts);
  const gaps = [];
  const byField = {};
  const byReason = {};
  const byStrategy = {};
  let cleanCore = 0;
  let governanceHold = 0;
  let dataQualityHold = 0;

  for (const rec of records) {
    const f = rec.fields || {};
    const review = classifyCensusReviewReasons({ fields: f }, { activeIndex });
    const clean = evaluateCleanCorePass(rec, { dictionary, activeIndex });
    if (clean.pass) cleanCore += 1;
    if (review.governance_only || review.governance_review_required) governanceHold += 1;
    if (review.data_quality_review_required) dataQualityHold += 1;

    for (const field of TRACKED_FIELDS) {
      const gap = classifyFieldGap(rec, field, { activeIndex, review });
      if (!gap.missing) continue;
      const row = {
        record_id: rec.id,
        property_name: f["Property Name"] || f[MAP_FIRST_PASS.propertyName] || null,
        brand: f["Current Brand"] || null,
        brand_family: f["Brand Family"] || f["Family / Source Family"] || null,
        country: f.Country || null,
        city: f.City || null,
        missing_field: field,
        ...gap,
        public_hold: String(f["Public Display Review Status"] || "") === "Hold",
        radar_hold: String(f["Radar Display Status"] || "") === "Hold",
        clean_core: Boolean(clean.pass),
        field_priority: FIELD_PRIORITY[field] ?? 99,
      };
      gaps.push(row);
      byField[field] = (byField[field] || 0) + 1;
      byReason[gap.reason] = (byReason[gap.reason] || 0) + 1;
      if (gap.next_best_source_strategy) {
        byStrategy[gap.next_best_source_strategy] =
          (byStrategy[gap.next_best_source_strategy] || 0) + 1;
      }
    }
  }

  return {
    version: GAP_LEDGER_VERSION,
    generated_at: new Date().toISOString(),
    total_records: records.length,
    clean_core: cleanCore,
    governance_hold: governanceHold,
    data_quality_hold: dataQualityHold,
    gap_count: gaps.length,
    by_field: byField,
    by_reason: byReason,
    by_strategy: byStrategy,
    gaps,
  };
}

/**
 * Completion scorecard from Census records.
 */
export function buildCompletionScorecard(records = [], opts = {}) {
  const activeIndex = opts.activeIndex || buildActiveBrandIndex(opts);
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary(opts);
  const n = records.length || 1;
  const counts = {
    total: records.length,
    clean_core: 0,
    state_region: 0,
    market: 0,
    submarket: 0,
    hotel_url: 0,
    address: 0,
    address_high: 0,
    lat_long: 0,
    phone: 0,
    rooms: 0,
    complete_census_v1: 0,
    public_eligible: 0,
    radar_eligible: 0,
    governance_hold: 0,
    data_quality_hold: 0,
  };
  const byParent = {};
  const byCountry = {};

  for (const rec of records) {
    const f = rec.fields || {};
    const review = classifyCensusReviewReasons({ fields: f }, { activeIndex });
    const clean = evaluateCleanCorePass(rec, { dictionary, activeIndex });
    const parent = String(f["Brand Family"] || f["Family / Source Family"] || "Unknown").trim();
    const country = String(f.Country || "Unknown").trim();
    byParent[parent] = byParent[parent] || { total: 0, clean_core: 0, address: 0, phone: 0, rooms: 0 };
    byCountry[country] = byCountry[country] || { total: 0, clean_core: 0, address: 0 };
    byParent[parent].total += 1;
    byCountry[country].total += 1;

    if (clean.pass) {
      counts.clean_core += 1;
      byParent[parent].clean_core += 1;
      byCountry[country].clean_core += 1;
    }
    if (!isBlank(f["State / Region"])) counts.state_region += 1;
    if (!isBlank(f.Market)) counts.market += 1;
    if (!isBlank(f.Submarket)) counts.submarket += 1;
    const url = f["Official Property URL"] || f["Source URL"];
    if (isHttpUrl(url) && !isOtaOrBlockedUrl(url)) counts.hotel_url += 1;
    if (isStreetLevelAddress(f.Address || "")) {
      counts.address += 1;
      byParent[parent].address += 1;
      byCountry[country].address += 1;
    }
    if (String(f["Address Confidence"] || "").toLowerCase() === "high") counts.address_high += 1;
    if (f.Latitude != null && f.Longitude != null) counts.lat_long += 1;
    if (!isBlank(f.Phone)) {
      counts.phone += 1;
      byParent[parent].phone += 1;
    }
    if (!isBlank(f["Rooms / Keys"])) {
      counts.rooms += 1;
      byParent[parent].rooms += 1;
    }
    const complete =
      clean.pass &&
      isStreetLevelAddress(f.Address || "") &&
      f.Latitude != null &&
      f.Longitude != null &&
      !isBlank(f.Phone) &&
      !isBlank(f["Rooms / Keys"]);
    if (complete) counts.complete_census_v1 += 1;
    if (String(f["Public Display Review Status"] || "") !== "Hold") counts.public_eligible += 1;
    if (String(f["Radar Display Status"] || "") !== "Hold") counts.radar_eligible += 1;
    if (review.governance_only || review.governance_review_required) counts.governance_hold += 1;
    if (review.data_quality_review_required) counts.data_quality_hold += 1;
  }

  const pct = (c) => Math.round((1000 * c) / n) / 10;
  return {
    counts,
    percents: {
      clean_core: pct(counts.clean_core),
      state_region: pct(counts.state_region),
      market: pct(counts.market),
      submarket: pct(counts.submarket),
      hotel_url: pct(counts.hotel_url),
      address: pct(counts.address),
      address_high: pct(counts.address_high),
      lat_long: pct(counts.lat_long),
      phone: pct(counts.phone),
      rooms: pct(counts.rooms),
      complete_census_v1: pct(counts.complete_census_v1),
      public_eligible: pct(counts.public_eligible),
      radar_eligible: pct(counts.radar_eligible),
      governance_hold: pct(counts.governance_hold),
      data_quality_hold: pct(counts.data_quality_hold),
    },
    targets: {
      clean_core: 90,
      state_region: 85,
      market: 85,
      hotel_url: 90,
      address_high: 60,
      lat_long: 50,
      phone: 60,
      rooms: 35,
      complete_census_v1: 25,
    },
    by_parent: byParent,
    by_country: byCountry,
  };
}

export function renderGapLedgerMarkdown(ledger, scorecard) {
  const sc = scorecard?.percents || {};
  const lines = [
    `# Census Gap Ledger`,
    ``,
    `**Version:** \`${ledger.version}\``,
    `**Generated:** ${ledger.generated_at}`,
    `**Total records:** ${ledger.total_records}`,
    `**Gap rows:** ${ledger.gap_count}`,
    ``,
    `## Scorecard (%)`,
    ``,
    `| Field | % | Target |`,
    `| --- | ---: | ---: |`,
    `| Clean Core | ${sc.clean_core ?? "—"} | 90 |`,
    `| State / Region | ${sc.state_region ?? "—"} | 85 |`,
    `| Market | ${sc.market ?? "—"} | 85 |`,
    `| Hotel URL | ${sc.hotel_url ?? "—"} | 90 |`,
    `| Address | ${sc.address ?? "—"} | — |`,
    `| Address High | ${sc.address_high ?? "—"} | 60 |`,
    `| Lat/Long | ${sc.lat_long ?? "—"} | 50 |`,
    `| Phone | ${sc.phone ?? "—"} | 60 |`,
    `| Rooms | ${sc.rooms ?? "—"} | 35 |`,
    `| Complete Census v1 | ${sc.complete_census_v1 ?? "—"} | 25 |`,
    ``,
    `## Gaps by field`,
    ``,
  ];
  for (const [field, count] of Object.entries(ledger.by_field || {}).sort(
    (a, b) => b[1] - a[1]
  )) {
    lines.push(`- **${field}:** ${count}`);
  }
  lines.push(``, `## Gaps by reason`, ``);
  for (const [reason, count] of Object.entries(ledger.by_reason || {}).sort(
    (a, b) => b[1] - a[1]
  )) {
    lines.push(`- \`${reason}\`: ${count}`);
  }
  lines.push(``, `## Gaps by next strategy`, ``);
  for (const [strategy, count] of Object.entries(ledger.by_strategy || {}).sort(
    (a, b) => b[1] - a[1]
  )) {
    lines.push(`- \`${strategy}\`: ${count}`);
  }
  lines.push(``);
  return lines.join("\n");
}

export function writeCensusGapLedger(ledger, scorecard, opts = {}) {
  const jsonPath =
    opts.jsonPath || path.join(ROOT, "reports/research-engine-v2/census-gap-ledger.json");
  const mdPath =
    opts.mdPath || path.join(ROOT, "reports/research-engine-v2/census-gap-ledger.md");
  const docsPath =
    opts.docsPath || path.join(ROOT, "docs/data-intelligence/census-gap-ledger.md");

  // Persist summary + capped gap sample for size; full gaps in run dir if provided
  const persist = {
    ...ledger,
    scorecard,
    gaps_sample: (ledger.gaps || []).slice(0, 200),
    gaps_full_count: (ledger.gaps || []).length,
    gaps: undefined,
  };
  fs.mkdirSync(path.dirname(jsonPath), { recursive: true });
  fs.writeFileSync(jsonPath, `${JSON.stringify(persist, null, 2)}\n`, "utf8");
  const md = renderGapLedgerMarkdown(ledger, scorecard);
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  if (opts.runDir) {
    fs.mkdirSync(opts.runDir, { recursive: true });
    fs.writeFileSync(
      path.join(opts.runDir, "census-gap-ledger-full.json"),
      `${JSON.stringify({ ...ledger, scorecard }, null, 2)}\n`,
      "utf8"
    );
  }
  return { jsonPath, mdPath, docsPath };
}

export { isOtaOrBlockedUrl, isHttpUrl };
