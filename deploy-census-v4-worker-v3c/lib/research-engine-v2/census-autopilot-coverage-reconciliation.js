/**
 * Coverage Reconciliation v1 — official brand inventory vs Hotel Property Census.
 *
 * Compares parent/brand official discovery adapters against production Census.
 * High-confidence missing hotels may insert (restricted fields only).
 * Write target: Hotel Property Census (tbl9aY5ijiuIzzWam) only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolveContinentSubContinentFromCountry,
  resolveMarketFromCity,
  resolveSubmarketHighOnly,
  CENSUS_GEO_FIELDS,
} from "./census-region-market-map.js";
import {
  MATCH_CLASS,
  SOURCE_DISCOVERY_QUEUE_ID,
  SOURCE_DISCOVERY_VERSION,
  buildActiveBrandDiscoveryControlList,
  buildOfficialInventoryDiscoveryControlList,
  classifyDiscoveredAgainstCensus,
  discoverCalaProperties,
  INSERT_FORBIDDEN_FIELDS,
  marriottRowToDiscovered,
  hiltonRowToDiscovered,
  choiceRowToDiscovered,
  ihgRowToDiscovered,
  accorRowToDiscovered,
} from "./census-autopilot-source-discovery.js";
import {
  ensureMarriottCalaCountrySitemapCache,
  iterateMarriottDirectoryRows,
  MARRIOTT_CALA_PRIORITY_COUNTRIES,
  MARRIOTT_DISCOVERY_SOURCE,
} from "./census-autopilot-marriott-discovery-adapter.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  buildCanonicalBrandDictionary,
  lookupCanonicalBrand,
} from "./census-brand-canonical-dictionary.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS, PRODUCTION_USE_STATUS } from "./production-census-write.js";
import {
  runDiscoveryInsertApply,
} from "./census-autopilot-discovery-insert-apply.js";
import {
  checkAutopilotApplyEnv,
  isProductionWriteMode,
} from "./census-autopilot-apply-guard.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import { tryCityFromMarriottPropertyUrl } from "./census-marriott-property-url-city-backfill.js";
import { inferCityFromMarriottTitle } from "./clean-census/marriott-mexico-discovery.js";
import { isDescriptorCity, canonicalCalaCity } from "./census-city-state-normalizer.js";
import {
  classifyBrandGovernanceStatus,
  buildNonActiveCensusGovernanceFields,
  BRAND_GOVERNANCE_STATUS,
  CENSUS_ONLY_PRODUCTION_USE_STATUS,
} from "./census-brand-governance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const COVERAGE_RECONCILIATION_VERSION =
  "census-autopilot-coverage-reconciliation-v1";
export const COVERAGE_RECONCILIATION_OBJECTIVE = "coverage-reconciliation-v1";
export const COVERAGE_RECONCILIATION_QUEUE_ID = "coverage_reconciliation";

export const COVERAGE_STATUS = Object.freeze({
  COMPLETE: "production_census_coverage_reconciliation_v1_complete",
  PARTIAL: "production_census_coverage_reconciliation_v1_partial_missing_remaining",
  BLOCKED: "production_census_coverage_reconciliation_v1_blocked",
});

/** User-facing match classes for coverage reports. */
export const COVERAGE_CLASS = Object.freeze({
  EXISTING_EXACT: "existing_exact_match",
  EXISTING_PROBABLE: "existing_probable_match",
  MISSING_HIGH: "missing_high_confidence",
  MISSING_STEWARD: "missing_needs_steward",
  DUPLICATE_RISK: "duplicate_risk",
  SOURCE_BLOCKED: "source_blocked",
  SOURCE_INSUFFICIENT: "source_insufficient",
});

/** Parents with official discovery adapters (no OTA / Maps / Webhound / old Census). */
export const COVERAGE_PARENT_FRAMEWORK = Object.freeze([
  "Marriott",
  "Hilton",
  "IHG",
  "Choice",
  "Accor",
  "Wyndham",
  "Preferred",
]);

/** Secondary / JS-rendered brand pages — not production SoT alone. */
export const SHERATON_DESTINATION_PAGE =
  "https://sheraton.marriott.com/es-XM/destinos-hotel/";

/**
 * Coverage insert allowlist — identity + geography only.
 * Explicitly excludes lat/long/phone/rooms/address.
 */
export const COVERAGE_INSERT_ALLOWED_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Current Brand",
  "Brand Family",
  "Affiliation Status",
  "City",
  "State / Region",
  "Country",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Source Type",
  "Source Confidence",
  "Identity Confidence",
  "Data Confidence Tier",
  "Data Eligible",
  "Production Use Status",
  "Public Display Review Status",
  "Radar Display Status",
  "Radar Display Reason",
  "Enrichment Status",
  "Enrichment Priority",
  "Human Review Required",
  "Last Reviewed Date",
  "Discovery Date",
]);

export const COVERAGE_INSERT_NEVER_FIELDS = Object.freeze([
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Owner Name",
  "Developer",
  "Developer Name",
  "Operator / Management Company",
  "Opening Date",
  "Renovation Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Brand Explorer Slug if mapped",
]);

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

/**
 * Exact brand match (no soft alias collapse that would merge Four Points → Sheraton).
 */
export function brandsEqualExact(a, b) {
  return norm(a) === norm(b);
}

/**
 * Filter discovered rows to an exact brand name when --brand is set.
 */
export function filterDiscoveredByBrand(discovered = [], brandFilter = null) {
  if (!brandFilter) return discovered;
  return discovered.filter((d) => brandsEqualExact(d.brand, brandFilter));
}

/**
 * Count Census records for a brand (Current Brand exact match).
 */
export function countCensusByBrand(censusRecords = [], brandFilter = null) {
  if (!brandFilter) {
    return {
      count: censusRecords.length,
      records: censusRecords,
    };
  }
  const records = censusRecords.filter((r) =>
    brandsEqualExact(r.fields?.[MAP_FIRST_PASS.currentBrand], brandFilter)
  );
  return { count: records.length, records };
}

/**
 * Map discovery MATCH_CLASS → coverage class.
 */
export function toCoverageClass(matchClass, discovered = {}) {
  if (discovered.source_blocked) return COVERAGE_CLASS.SOURCE_BLOCKED;
  switch (matchClass) {
    case MATCH_CLASS.EXISTING_EXACT:
    case COVERAGE_CLASS.EXISTING_EXACT:
      return COVERAGE_CLASS.EXISTING_EXACT;
    case MATCH_CLASS.EXISTING_PROBABLE:
    case COVERAGE_CLASS.EXISTING_PROBABLE:
      return COVERAGE_CLASS.EXISTING_PROBABLE;
    case MATCH_CLASS.NEW_CANDIDATE:
      return COVERAGE_CLASS.MISSING_HIGH;
    case MATCH_CLASS.STEWARD:
      return COVERAGE_CLASS.MISSING_STEWARD;
    case MATCH_CLASS.DUPLICATE_RISK:
      return COVERAGE_CLASS.DUPLICATE_RISK;
    case MATCH_CLASS.SOURCE_INSUFFICIENT:
      return COVERAGE_CLASS.SOURCE_INSUFFICIENT;
    case MATCH_CLASS.IDENTITY_CONFLICT:
      return COVERAGE_CLASS.MISSING_STEWARD;
    default:
      return COVERAGE_CLASS.SOURCE_INSUFFICIENT;
  }
}

/**
 * Sanitize coverage insert fields (stricter than general discovery inserts).
 */
export function sanitizeCoverageInsertFields(fields = {}) {
  const out = {};
  const dropped = [];
  for (const [k, v] of Object.entries(fields || {})) {
    if (
      COVERAGE_INSERT_NEVER_FIELDS.includes(k) ||
      INSERT_FORBIDDEN_FIELDS.includes(k) ||
      isForbiddenAutopilotField(k)
    ) {
      dropped.push({ field: k, reason: "forbidden_on_coverage_insert" });
      continue;
    }
    if (!COVERAGE_INSERT_ALLOWED_FIELDS.includes(k)) {
      dropped.push({ field: k, reason: "not_allowlisted_for_coverage_insert" });
      continue;
    }
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return { fields: out, dropped };
}

/**
 * Build restricted insert payload for missing_high_confidence official hotels.
 */
export function buildCoverageInsertFields(discovered, opts = {}) {
  const country = discovered.country || null;
  const city = discovered.city || null;
  const geo = resolveContinentSubContinentFromCountry(country);
  const market = resolveMarketFromCity({ city, country });
  const submarket = resolveSubmarketHighOnly({
    city,
    country,
    market: market?.ok ? market.market : undefined,
    propertyName: discovered.property_name,
  });

  // Canonical Brand dictionary — avoid inserting alias/misspelled Active brands
  const dictionary = opts.brandDictionary || buildCanonicalBrandDictionary(opts);
  const brandLookup = lookupCanonicalBrand(discovered.brand, dictionary, {
    propertyName: discovered.property_name,
    sourceUrl: discovered.official_property_url || discovered.official_directory_url,
  });
  const canonicalBrand =
    brandLookup.ok && brandLookup.canonical ? brandLookup.canonical : discovered.brand;
  const brandFamily =
    (brandLookup.ok && brandLookup.entry?.parent_company) ||
    discovered.parent_company ||
    discovered.source_family;

  const gov = classifyBrandGovernanceStatus(
    {
      brand: canonicalBrand,
      brand_slug: discovered.brand_slug,
      property_name: discovered.property_name,
      official_property_url: discovered.official_property_url,
      source_url: discovered.official_directory_url || discovered.official_property_url,
      parent_company: brandFamily,
      source_family: discovered.source_family,
    },
    opts
  );
  const nonActiveFields = buildNonActiveCensusGovernanceFields(gov, {
    explicitly_approved: opts.explicitly_approved === true,
  });
  const ownerFacing = gov.status === BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP;
  const useStatus = ownerFacing
    ? PRODUCTION_USE_STATUS || CENSUS_ONLY_PRODUCTION_USE_STATUS
    : CENSUS_ONLY_PRODUCTION_USE_STATUS;

  /** @type {Record<string, unknown>} */
  const fields = {
    "Property Name": discovered.property_name,
    "Canonical Property Name": discovered.property_name,
    "Property Identity Key": discovered.identity_key,
    "Current Brand": canonicalBrand,
    "Brand Family": brandFamily,
    "Affiliation Status": "Branded",
    City: city || "Unknown",
    Country: country || "Unknown",
    "Source URL": discovered.official_directory_url || discovered.official_property_url,
    "Official Property URL": discovered.official_property_url,
    "Family / Source Family": discovered.source_family,
    "Source Type": "official_brand_directory",
    "Source Confidence": discovered.source_confidence || "High",
    "Identity Confidence": discovered.identity_confidence || "High",
    "Data Confidence Tier": "High",
    "Data Eligible": true,
    "Production Use Status": useStatus,
    "Enrichment Status": "Discovered — pending enrichment",
    "Enrichment Priority": "High",
    "Human Review Required":
      opts.human_review_required === true ||
      nonActiveFields["Human Review Required"] === true,
    "Last Reviewed Date": todayIsoDate(),
    "Discovery Date": discovered.discovered_date || todayIsoDate(),
    ...nonActiveFields,
  };

  if (discovered.state_region) fields["State / Region"] = discovered.state_region;
  if (geo?.continent) fields[CENSUS_GEO_FIELDS.continent] = geo.continent;
  if (geo?.subContinent) fields[CENSUS_GEO_FIELDS.subContinent] = geo.subContinent;
  if (market?.ok && market.market) fields[CENSUS_GEO_FIELDS.market] = market.market;
  if (submarket?.ok && submarket.submarket) {
    fields[CENSUS_GEO_FIELDS.submarket] = submarket.submarket;
  }

  return sanitizeCoverageInsertFields(fields);
}

/**
 * Build insert approval bundle for coverage High missing hotels.
 * Compatible with discovery insert apply (source_discovery queue + allowlist intersect).
 */
export function buildCoverageInsertApprovalBundle(ctx = {}) {
  const missingHigh = (ctx.classified || []).filter(
    (c) =>
      c.coverage_class === COVERAGE_CLASS.MISSING_HIGH &&
      String(c.identity_confidence) === "High"
  );

  const proposed_inserts = [];
  for (const c of missingHigh) {
    const sanitized = buildCoverageInsertFields(c, { human_review_required: false });
    if (!sanitized.fields["Property Name"] || !sanitized.fields["Property Identity Key"]) {
      continue;
    }
    // Insert-apply allowlist is broader discovery set — intersect so apply path accepts.
    // Coverage never fields already stripped by sanitizeCoverageInsertFields.
    const applyFields = { ...sanitized.fields };
    // Discovery apply allowlist may not yet include geo fields — keep only if present there
    // by re-checking via dynamic import constants; drop geo if not in discovery allowlist
    // is handled at apply time. We also add Canonical / Data Confidence / geo to discovery allowlist.
    proposed_inserts.push({
      action: "insert",
      queue: SOURCE_DISCOVERY_QUEUE_ID,
      coverage_queue: COVERAGE_RECONCILIATION_QUEUE_ID,
      confidence: "High",
      identity_key: c.identity_key,
      property_name: c.property_name,
      brand: c.brand,
      source_family: c.source_family,
      official_property_id: c.official_property_id,
      fields: applyFields,
      field_keys: Object.keys(applyFields),
      dropped: sanitized.dropped,
      discovery: {
        official_property_url: c.official_property_url,
        official_directory_url: c.official_directory_url,
        city: c.city,
        country: c.country,
        match_classification: c.coverage_class,
        coverage_objective: COVERAGE_RECONCILIATION_OBJECTIVE,
      },
    });
  }

  return {
    type: "hotel_property_census_insert_approval_bundle",
    version: COVERAGE_RECONCILIATION_VERSION,
    source_discovery_version: SOURCE_DISCOVERY_VERSION,
    objective: COVERAGE_RECONCILIATION_OBJECTIVE,
    queue: SOURCE_DISCOVERY_QUEUE_ID,
    coverage_queue: COVERAGE_RECONCILIATION_QUEUE_ID,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    brand_setup_writes: false,
    brand_explorer_writes: false,
    fuzzy_auto_insert: false,
    hotel_name_only_insert: false,
    lat_long_on_insert: false,
    phone_on_insert: false,
    rooms_on_insert: false,
    proposed_inserts,
    proposed_insert_count: proposed_inserts.length,
    created_at: new Date().toISOString(),
    notes: [
      "Coverage reconciliation High-confidence missing only",
      "No fuzzy / name-only inserts",
      "No lat/long/phone/rooms/address on insert",
    ],
  };
}

/**
 * Attach coverage_class to classified discovery rows.
 */
export function attachCoverageClasses(matchResult) {
  const classified = (matchResult.classified || []).map((row) => {
    const coverage_class = toCoverageClass(row.classification, row);
    return { ...row, coverage_class };
  });
  const by_coverage_class = Object.fromEntries(
    Object.values(COVERAGE_CLASS).map((k) => [k, []])
  );
  for (const row of classified) {
    if (!by_coverage_class[row.coverage_class]) by_coverage_class[row.coverage_class] = [];
    by_coverage_class[row.coverage_class].push(row);
  }
  const counts = Object.fromEntries(
    Object.entries(by_coverage_class).map(([k, v]) => [k, v.length])
  );
  return { ...matchResult, classified, by_coverage_class, coverage_counts: counts };
}

/**
 * Fill city from official Marriott property URL (High) or title inference (Medium→steward if only title).
 * Demote missing_high → missing_needs_steward when city remains blank/Unknown/descriptor.
 */
export function enrichAndGateCoverageCities(classified = []) {
  return classified.map((row) => {
    const next = { ...row };
    let city = String(next.city || "").trim();
    if (
      !city ||
      /^unknown$/i.test(city) ||
      brandsEqualExact(city, next.country) ||
      isDescriptorCity(city)
    ) {
      const urlTry = tryCityFromMarriottPropertyUrl(
        next.official_property_url,
        next.country || ""
      );
      if (urlTry.ok && urlTry.city) {
        city = canonicalCalaCity(urlTry.city) || urlTry.city;
        next.city = city;
        next.city_enrichment = urlTry.reason || "marriott_property_url_city";
      } else {
        const titleCity = inferCityFromMarriottTitle(next.property_name, next.brand);
        if (titleCity && !isDescriptorCity(titleCity) && !brandsEqualExact(titleCity, next.country)) {
          // Title inference alone is not enough for High insert — keep for steward context
          next.city_title_hint = titleCity;
        }
      }
    }

    if (next.coverage_class === COVERAGE_CLASS.MISSING_HIGH) {
      const finalCity = String(next.city || "").trim();
      if (
        !finalCity ||
        /^unknown$/i.test(finalCity) ||
        brandsEqualExact(finalCity, next.country) ||
        isDescriptorCity(finalCity)
      ) {
        next.coverage_class = COVERAGE_CLASS.MISSING_STEWARD;
        next.match_reason = next.match_reason || "missing_city_for_coverage_insert";
        next.identity_confidence = "Medium";
      }
    }
    return next;
  });
}

/**
 * Recompute by_coverage_class + counts after city gate.
 */
export function reindexCoverageClasses(classified = []) {
  const by_coverage_class = Object.fromEntries(
    Object.values(COVERAGE_CLASS).map((k) => [k, []])
  );
  for (const row of classified) {
    if (!by_coverage_class[row.coverage_class]) by_coverage_class[row.coverage_class] = [];
    by_coverage_class[row.coverage_class].push(row);
  }
  const coverage_counts = Object.fromEntries(
    Object.entries(by_coverage_class).map(([k, v]) => [k, v.length])
  );
  return { classified, by_coverage_class, coverage_counts };
}

/**
 * Roll up coverage stats by brand.
 */
export function buildBrandCoverageRollup(classified = [], censusRecords = [], opts = {}) {
  const brandFilter = opts.brand || null;
  const byBrand = new Map();

  const ensure = (brand) => {
    const key = brand || "(unknown)";
    if (!byBrand.has(key)) {
      byBrand.set(key, {
        brand: key,
        official_inventory_count: 0,
        census_inventory_count: 0,
        existing_exact: 0,
        existing_probable: 0,
        missing_high_confidence: 0,
        missing_needs_steward: 0,
        duplicate_risk: 0,
        source_blocked: 0,
        source_insufficient: 0,
        missing_official_property_urls: [],
        recommended_action: null,
      });
    }
    return byBrand.get(key);
  };

  for (const row of classified) {
    if (brandFilter && !brandsEqualExact(row.brand, brandFilter)) continue;
    const b = ensure(row.brand);
    b.official_inventory_count += 1;
    const cls = row.coverage_class;
    if (cls === COVERAGE_CLASS.EXISTING_EXACT) b.existing_exact += 1;
    else if (cls === COVERAGE_CLASS.EXISTING_PROBABLE) b.existing_probable += 1;
    else if (cls === COVERAGE_CLASS.MISSING_HIGH) {
      b.missing_high_confidence += 1;
      if (row.official_property_url) b.missing_official_property_urls.push(row.official_property_url);
    } else if (cls === COVERAGE_CLASS.MISSING_STEWARD) {
      b.missing_needs_steward += 1;
      if (row.official_property_url) b.missing_official_property_urls.push(row.official_property_url);
    } else if (cls === COVERAGE_CLASS.DUPLICATE_RISK) b.duplicate_risk += 1;
    else if (cls === COVERAGE_CLASS.SOURCE_BLOCKED) b.source_blocked += 1;
    else if (cls === COVERAGE_CLASS.SOURCE_INSUFFICIENT) b.source_insufficient += 1;
  }

  // Census counts by brand (exact Current Brand)
  const censusBrandCounts = new Map();
  for (const rec of censusRecords) {
    const brand = String(rec.fields?.[MAP_FIRST_PASS.currentBrand] || "").trim() || "(blank)";
    if (brandFilter && !brandsEqualExact(brand, brandFilter)) continue;
    censusBrandCounts.set(brand, (censusBrandCounts.get(brand) || 0) + 1);
  }
  for (const [brand, count] of censusBrandCounts) {
    const b = ensure(brand);
    b.census_inventory_count = count;
  }

  const rows = [...byBrand.values()].map((b) => {
    const missing = b.missing_high_confidence + b.missing_needs_steward;
    const covered = b.existing_exact + b.existing_probable;
    const denom = b.official_inventory_count || 0;
    const coverage_pct =
      denom > 0 ? Math.round((covered / denom) * 1000) / 10 : null;
    let recommended_action = "none";
    if (b.source_blocked > 0 && denom === 0) recommended_action = "unblock_official_adapter";
    else if (b.missing_high_confidence > 0) recommended_action = "insert_high_confidence_missing";
    else if (b.missing_needs_steward > 0 || b.duplicate_risk > 0)
      recommended_action = "steward_review";
    else if (denom > 0 && covered === denom) recommended_action = "coverage_complete";
    return {
      ...b,
      missing_count: missing,
      coverage_percentage: coverage_pct,
      recommended_action,
    };
  });

  rows.sort((a, b) => (b.missing_count || 0) - (a.missing_count || 0));
  return rows;
}

/**
 * Resolve final coverage status.
 */
export function resolveCoverageStatus(summary = {}) {
  if (summary.blocked_hard) return COVERAGE_STATUS.BLOCKED;
  const missing =
    (summary.coverage_counts?.[COVERAGE_CLASS.MISSING_HIGH] || 0) +
    (summary.coverage_counts?.[COVERAGE_CLASS.MISSING_STEWARD] || 0);
  const dup = summary.coverage_counts?.[COVERAGE_CLASS.DUPLICATE_RISK] || 0;
  if (missing > 0 || dup > 0) return COVERAGE_STATUS.PARTIAL;
  if ((summary.official_inventory_count || 0) === 0 && (summary.source_blockers || []).length) {
    return COVERAGE_STATUS.BLOCKED;
  }
  return COVERAGE_STATUS.COMPLETE;
}

async function listCensusForCoverage(baseId, token, tableId) {
  const fields = [
    "Property Identity Key",
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "Brand Family",
    "Country",
    "City",
    "State / Region",
    "Address",
    "Source URL",
    "Official Property URL",
    "Family / Source Family",
    "Identity Confidence",
    "Data Confidence Tier",
    "Production Use Status",
    "Continent",
    "Sub-Continent",
    "Market",
    "Submarket",
  ];
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
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

function renderCoverageMarkdown(report) {
  const brands = report.brand_rollups || [];
  const missing = report.missing_hotels || [];
  const lines = [
    `# Production Census Coverage Reconciliation v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Region:** ${report.region}`,
    `**Parent company:** ${report.parent_company || "(all active parents in scope)"}`,
    `**Brand filter:** ${report.brand || "(all Active/Live brands in parent scope)"}`,
    `**Write target:** ${report.write_target?.table} (\`${report.write_target?.table_id}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no (controlled)"}`,
    ``,
    `## Summary`,
    ``,
    `- Official inventory count: **${report.official_inventory_count}**`,
    `- Census inventory count (scoped brand): **${report.census_inventory_count}**`,
    `- Exact matches: **${report.coverage_counts?.[COVERAGE_CLASS.EXISTING_EXACT] || 0}**`,
    `- Probable matches: **${report.coverage_counts?.[COVERAGE_CLASS.EXISTING_PROBABLE] || 0}**`,
    `- Missing High: **${report.coverage_counts?.[COVERAGE_CLASS.MISSING_HIGH] || 0}**`,
    `- Missing steward: **${report.coverage_counts?.[COVERAGE_CLASS.MISSING_STEWARD] || 0}**`,
    `- Duplicate risks: **${report.coverage_counts?.[COVERAGE_CLASS.DUPLICATE_RISK] || 0}**`,
    `- Source blocked: **${report.coverage_counts?.[COVERAGE_CLASS.SOURCE_BLOCKED] || 0}**`,
    `- Source insufficient: **${report.coverage_counts?.[COVERAGE_CLASS.SOURCE_INSUFFICIENT] || 0}**`,
    `- Inserted: **${report.inserted_count || 0}**`,
    `- Stewarded (held): **${report.stewarded_count || 0}**`,
    ``,
    `## Official sources used`,
    ``,
  ];
  for (const s of report.official_sources || []) {
    lines.push(`- ${s.family || s.parent}: ${s.note || s.source || s.url || "(adapter)"}`);
  }
  if (report.brand && /sheraton/i.test(report.brand)) {
    lines.push(
      `- Sheraton destination page (secondary / JS shell — not sole SoT): ${SHERATON_DESTINATION_PAGE}`
    );
    lines.push(
      `- Primary Marriott SoT: country hotel-sitemaps (marriott_country_hotel_sitemap)`
    );
  }
  lines.push(``, `## Brand rollup`, ``);
  lines.push(
    `| Brand | Official | Census | Missing | Coverage % | Action |`
  );
  lines.push(`| --- | ---: | ---: | ---: | ---: | --- |`);
  for (const b of brands.slice(0, 80)) {
    lines.push(
      `| ${b.brand} | ${b.official_inventory_count} | ${b.census_inventory_count} | ${b.missing_count} | ${b.coverage_percentage ?? "—"} | ${b.recommended_action} |`
    );
  }
  lines.push(``, `## Missing hotels (sample)`, ``);
  for (const m of missing.slice(0, 40)) {
    lines.push(
      `- **${m.property_name}** (${m.brand}, ${m.city || "?"}, ${m.country || "?"}) — \`${m.coverage_class}\` — ${m.official_property_url || "no URL"} — MARSHA/code: ${m.official_property_id || "—"}`
    );
  }
  if (report.source_blockers?.length) {
    lines.push(``, `## Source blockers`, ``);
    for (const b of report.source_blockers) {
      lines.push(`- ${typeof b === "string" ? b : JSON.stringify(b)}`);
    }
  }
  lines.push(
    ``,
    `## Safety`,
    ``,
    `- Hotel Property Census only`,
    `- Brand Setup / Brand Explorer untouched`,
    `- No owner/operator/date / Recent Momentum / Company Validated writes`,
    `- No fuzzy auto-insert; no hotel-name-only insert`,
    `- No lat/long/phone/rooms on coverage inserts`,
    ``
  );
  return lines.join("\n");
}

function renderSheratonMarkdown(report) {
  const missing = report.missing_hotels || [];
  return `# Sheraton CALA Coverage Reconciliation

**Status:** \`${report.status}\`  
**Official inventory:** ${report.official_inventory_count}  
**Census (Brand = Sheraton):** ${report.census_inventory_count}  
**Exact matches:** ${report.coverage_counts?.[COVERAGE_CLASS.EXISTING_EXACT] || 0}  
**Missing High:** ${report.coverage_counts?.[COVERAGE_CLASS.MISSING_HIGH] || 0}  
**Missing steward:** ${report.coverage_counts?.[COVERAGE_CLASS.MISSING_STEWARD] || 0}  
**Inserted:** ${report.inserted_count || 0}  
**Duplicate risks:** ${report.coverage_counts?.[COVERAGE_CLASS.DUPLICATE_RISK] || 0}

## Primary official source
Marriott country hotel-sitemaps (MARSHA + property URL).  
Secondary reference (JS-rendered, not sole SoT): ${SHERATON_DESTINATION_PAGE}

## Missing candidates
${
  missing.length
    ? missing
        .map(
          (m) =>
            `- ${m.property_name} | ${m.city || "?"}, ${m.country || "?"} | ${m.official_property_id || "—"} | ${m.coverage_class} | ${m.official_property_url || ""}`
        )
        .join("\n")
    : "_None_"
}

## Before / after Census count
- Before: ${report.census_inventory_count_before ?? report.census_inventory_count}
- After: ${report.census_inventory_count_after ?? report.census_inventory_count}
- Inserted this run: ${report.inserted_count || 0}
`;
}

/**
 * Core coverage reconciliation run (controlled or apply).
 */
export async function runCoverageReconciliation(opts = {}) {
  const log = opts.log || (() => {});
  const region = opts.region || "CALA";
  const parentCompany = opts.parentCompany || opts.parent_company || null;
  const brand = opts.brand || null;
  const mode = opts.mode || "controlled";
  const doWrite =
    Boolean(opts.enableProductionWrites) &&
    isProductionWriteMode(mode) &&
    Boolean(opts.allApplyConfirms);

  const writeTargetCheck = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: productionHotelPropertyCensus.tableId || TABLE_IDS["Hotel Property Census"],
  });
  if (!writeTargetCheck.ok) {
    return {
      ok: false,
      status: COVERAGE_STATUS.BLOCKED,
      objective: COVERAGE_RECONCILIATION_OBJECTIVE,
      blocked_hard: true,
      blocked_reason: writeTargetCheck.reason || "wrong_write_target",
      airtable_writes: false,
    };
  }

  let censusRecords = opts.censusRecords || null;
  if (!censusRecords) {
    const token = resolvePat();
    const bases = resolveTargetBase();
    const baseId = bases?.target_base_id;
    if (!token || !baseId) {
      return {
        ok: false,
        status: COVERAGE_STATUS.BLOCKED,
        objective: COVERAGE_RECONCILIATION_OBJECTIVE,
        blocked_hard: true,
        blocked_reason: "missing_airtable_credentials",
        airtable_writes: false,
      };
    }
    log(`[coverage] listing Hotel Property Census…`);
    censusRecords = await listCensusForCoverage(
      baseId,
      token,
      TABLE_IDS["Hotel Property Census"]
    );
  }

  const censusBrand = countCensusByBrand(censusRecords, brand);
  log(
    `[coverage] census total=${censusRecords.length} brand_scoped=${censusBrand.count} brand=${brand || "(all)"}`
  );

  const controlList = opts.controlList ||
    (opts.discoverAllOfficialParents === false
      ? buildActiveBrandDiscoveryControlList({
          region,
          parentCompany,
        })
      : buildOfficialInventoryDiscoveryControlList({
          region,
          parentCompany,
        }));

  // Never narrow the control list to a single --brand before discovery:
  // marriottRowToDiscovered would overwrite every row's brand with that control brand.
  // Discover at parent / official inventory scope, then filter exact brand after mapping.
  const scopedControl = controlList;

  log(
    `[coverage] discovering official inventory parent=${parentCompany || "(all)"} brand=${brand || "(all)"} scope=${controlList.purpose || "official"}…`
  );

  const discovery =
    opts.discoveryResult ||
    (await discoverCalaProperties({
      controlList: scopedControl,
      parentCompany,
      region,
      country: opts.country || null,
      discoveryCountries: opts.discoveryCountries || null,
      forceMarriottDiscovery: /marriott/i.test(String(parentCompany || "")),
      discoverAllOfficialParents: opts.discoverAllOfficialParents !== false,
      // Keep directory-mapped brands; do not drop non-Active official inventory
      requireBrandMatch: false,
      marriottCache: opts.marriottCache,
      hiltonCache: opts.hiltonCache,
      delayMs: opts.delayMs ?? 150,
    }));

  let discovered = filterDiscoveredByBrand(discovery.discovered || [], brand);

  const sourceBlockers = [
    ...(discovery.sourceReport?.blocked_source_families || []).map((f) => ({
      family: f,
      reason: "adapter_blocked",
    })),
    ...(discovery.sourceReport?.adapter_errors || []),
  ];

  const matchRaw = classifyDiscoveredAgainstCensus(discovered, censusRecords, {
    vicEvidence: discovery.vicEvidence,
  });
  let match = attachCoverageClasses(matchRaw);
  const gated = enrichAndGateCoverageCities(match.classified);
  const reindexed = reindexCoverageClasses(gated);
  match = { ...match, ...reindexed };

  const brandRollups = buildBrandCoverageRollup(match.classified, censusRecords, { brand });
  const missingHotels = match.classified.filter(
    (r) =>
      r.coverage_class === COVERAGE_CLASS.MISSING_HIGH ||
      r.coverage_class === COVERAGE_CLASS.MISSING_STEWARD
  );
  const stewarded = match.classified.filter(
    (r) =>
      r.coverage_class === COVERAGE_CLASS.MISSING_STEWARD ||
      r.coverage_class === COVERAGE_CLASS.DUPLICATE_RISK
  );

  const insertBundle = buildCoverageInsertApprovalBundle({ classified: match.classified });

  const runDir =
    opts.runDir ||
    path.join(
      ROOT,
      "reports/research-engine-v2/autopilot",
      `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-coverage-reconciliation-v1`
    );
  fs.mkdirSync(runDir, { recursive: true });
  writeJson(path.join(runDir, "coverage-classified.json"), match.classified);
  writeJson(path.join(runDir, "coverage-brand-rollups.json"), brandRollups);
  writeJson(path.join(runDir, "coverage-insert-approval-bundle.json"), insertBundle);
  writeJson(path.join(runDir, "coverage-source-report.json"), discovery.sourceReport || {});

  let insertedCount = 0;
  let insertApplyResult = null;
  const envCheck = checkAutopilotApplyEnv(opts.env || process.env);
  const canApply =
    doWrite &&
    Boolean(opts.allApplyConfirms) &&
    envCheck.allOk &&
    insertBundle.proposed_insert_count > 0;

  if (canApply) {
    log(
      `[coverage] applying ${insertBundle.proposed_insert_count} High-confidence inserts to Hotel Property Census…`
    );
    const bundlePath = path.join(runDir, "coverage-insert-approval-bundle.json");
    insertApplyResult = await runDiscoveryInsertApply({
      args: {
        apply: true,
        allConfirmsOk: true,
        approvalBundlePath: bundlePath,
        batchSize: opts.batchSize || 100,
        confirms: opts.confirms || {},
      },
      bundlePath,
      censusRecords,
      doWrite: true,
      useLiveAirtable: opts.useLiveAirtable !== false,
      createRecords: opts.createRecords || undefined,
      env: opts.env || process.env,
    });
    insertedCount =
      insertApplyResult?.created_count ||
      insertApplyResult?.inserted_count ||
      (Array.isArray(insertApplyResult?.created) ? insertApplyResult.created.length : 0) ||
      0;
  } else if (doWrite && insertBundle.proposed_insert_count > 0) {
    log(
      `[coverage] writes requested but confirms/env incomplete — controlled only (proposed=${insertBundle.proposed_insert_count})`
    );
  }

  const censusAfter =
    insertedCount > 0 && !opts.censusRecords
      ? await (async () => {
          try {
            const token = resolvePat();
            const bases = resolveTargetBase();
            return await listCensusForCoverage(
              bases.target_base_id,
              token,
              TABLE_IDS["Hotel Property Census"]
            );
          } catch {
            return censusRecords;
          }
        })()
      : censusRecords;
  const censusBrandAfter = countCensusByBrand(censusAfter, brand);

  const officialSources = [];
  if (/marriott/i.test(String(parentCompany || "")) || !parentCompany) {
    officialSources.push({
      family: "Marriott",
      source:
        typeof MARRIOTT_DISCOVERY_SOURCE === "string"
          ? MARRIOTT_DISCOVERY_SOURCE
          : MARRIOTT_DISCOVERY_SOURCE?.source || "marriott_country_hotel_sitemap",
      note: "Official country hotel-sitemaps (MARSHA + property URL); HQV not used for discovery",
      adapter: "marriott_cala_country_sitemap",
    });
  }
  for (const f of discovery.sourceReport?.families_used || []) {
    if (!officialSources.some((s) => s.family === f)) {
      officialSources.push({ family: f, note: "official_family_directory_adapter" });
    }
  }

  const summary = {
    ok: true,
    version: COVERAGE_RECONCILIATION_VERSION,
    objective: COVERAGE_RECONCILIATION_OBJECTIVE,
    region,
    parent_company: parentCompany,
    brand,
    mode,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    brand_setup_writes: false,
    brand_explorer_writes: false,
    official_inventory_count: discovered.length,
    census_inventory_count: censusBrand.count,
    census_inventory_count_before: censusBrand.count,
    census_inventory_count_after: censusBrandAfter.count,
    census_total_records: censusRecords.length,
    coverage_counts: match.coverage_counts,
    brand_rollups: brandRollups,
    missing_hotels: missingHotels.map((m) => ({
      property_name: m.property_name,
      brand: m.brand,
      city: m.city,
      country: m.country,
      official_property_id: m.official_property_id,
      official_property_url: m.official_property_url,
      official_directory_url: m.official_directory_url,
      coverage_class: m.coverage_class,
      identity_confidence: m.identity_confidence,
      identity_key: m.identity_key,
    })),
    inserted_hotels: insertApplyResult?.created || insertApplyResult?.inserted || [],
    inserted_count: insertedCount,
    stewarded_count: stewarded.length,
    stewarded_hotels: stewarded.slice(0, 100).map((m) => ({
      property_name: m.property_name,
      brand: m.brand,
      coverage_class: m.coverage_class,
      reason: m.match_reason || m.reason || null,
      official_property_url: m.official_property_url,
    })),
    duplicate_risks: (match.by_coverage_class?.[COVERAGE_CLASS.DUPLICATE_RISK] || []).map((m) => ({
      property_name: m.property_name,
      brand: m.brand,
      census_record_id: m.census_record_id,
      official_property_url: m.official_property_url,
    })),
    source_blockers: sourceBlockers,
    official_sources: officialSources,
    proposed_insert_count: insertBundle.proposed_insert_count,
    insert_apply: insertApplyResult
      ? {
          status: insertApplyResult.status,
          inserted_count: insertedCount,
          blocked_reason: insertApplyResult.blocked_reason || null,
        }
      : null,
    airtable_writes: Boolean(canApply && insertedCount >= 0 && doWrite),
    run_dir: runDir,
    parent_framework: COVERAGE_PARENT_FRAMEWORK,
    next_recommended_action: null,
  };

  summary.blocked_hard = false;
  if (discovered.length === 0 && sourceBlockers.length) {
    summary.blocked_hard = true;
  }
  // After successful High inserts, treat remaining missing_high as cleared when all proposed were written
  if (
    insertedCount > 0 &&
    insertedCount >= (summary.proposed_insert_count || 0) &&
    (summary.coverage_counts?.[COVERAGE_CLASS.MISSING_STEWARD] || 0) === 0 &&
    (summary.coverage_counts?.[COVERAGE_CLASS.DUPLICATE_RISK] || 0) === 0
  ) {
    summary.coverage_counts = {
      ...summary.coverage_counts,
      [COVERAGE_CLASS.MISSING_HIGH]: Math.max(
        0,
        (summary.coverage_counts?.[COVERAGE_CLASS.MISSING_HIGH] || 0) - insertedCount
      ),
      [COVERAGE_CLASS.EXISTING_EXACT]:
        (summary.coverage_counts?.[COVERAGE_CLASS.EXISTING_EXACT] || 0) + insertedCount,
    };
    summary.missing_hotels = (summary.missing_hotels || []).filter(
      (m) => m.coverage_class !== COVERAGE_CLASS.MISSING_HIGH
    );
  }
  summary.status = resolveCoverageStatus(summary);
  // Prefer post-insert census brand count in headline metric
  summary.census_inventory_count = summary.census_inventory_count_after ?? summary.census_inventory_count;
  summary.next_recommended_action =
    summary.status === COVERAGE_STATUS.COMPLETE
      ? brand
        ? "expand_to_all_active_marriott_brands_then_other_parents"
        : "coverage_reconciliation_complete_for_scope"
      : summary.status === COVERAGE_STATUS.PARTIAL
        ? "review_steward_and_insert_remaining_high_or_expand_sources"
        : "unblock_official_adapters_or_credentials";

  // Public reports
  const reportJsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-coverage-reconciliation-v1.json"
  );
  const reportMdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-coverage-reconciliation-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-coverage-reconciliation-v1.md"
  );
  writeJson(reportJsonPath, summary);
  writeText(reportMdPath, renderCoverageMarkdown(summary));
  writeText(docsPath, renderCoverageMarkdown(summary));
  writeJson(path.join(runDir, "coverage-summary.json"), summary);
  writeText(path.join(runDir, "coverage-summary.md"), renderCoverageMarkdown(summary));

  if (brand && /sheraton/i.test(brand)) {
    const sheratonJson = path.join(
      ROOT,
      "reports/research-engine-v2/production-census-sheraton-coverage-reconciliation.json"
    );
    const sheratonMd = path.join(
      ROOT,
      "reports/research-engine-v2/production-census-sheraton-coverage-reconciliation.md"
    );
    writeJson(sheratonJson, summary);
    writeText(sheratonMd, renderSheratonMarkdown(summary));
  }

  log(
    `[coverage] status=${summary.status} official=${summary.official_inventory_count} census=${summary.census_inventory_count} missing_high=${summary.coverage_counts?.[COVERAGE_CLASS.MISSING_HIGH] || 0} inserted=${insertedCount}`
  );

  return summary;
}

/** Mission / controlled entry used by census-autopilot.mjs */
export async function runCoverageReconciliationMission(opts = {}) {
  const args = opts.args || {};
  const env = opts.env || process.env;
  const envCheck = checkAutopilotApplyEnv(env);
  const allApplyConfirms = Boolean(args.allApplyConfirms);
  const enableProductionWrites = Boolean(opts.enableProductionWrites);
  const mode = args.mode || "controlled";

  return runCoverageReconciliation({
    ...opts,
    region: args.region || "CALA",
    parentCompany: args.parentCompany || null,
    brand: args.brand || null,
    country: args.country || null,
    mode,
    batchSize: args.batchSize || 100,
    enableProductionWrites,
    allApplyConfirms,
    confirms: args.confirms,
    env,
    log: opts.log,
  });
}

// Re-export helpers useful in tests
export {
  marriottRowToDiscovered,
  hiltonRowToDiscovered,
  choiceRowToDiscovered,
  ihgRowToDiscovered,
  accorRowToDiscovered,
  ensureMarriottCalaCountrySitemapCache,
  iterateMarriottDirectoryRows,
  MARRIOTT_CALA_PRIORITY_COUNTRIES,
};
