/**
 * Autopilot source_discovery queue — CALA Discovery + Insert Mode.
 *
 * Discovers official parent-company inventory in the selected region (not limited
 * to Brand Setup Active/Live), matches against production Hotel Property Census,
 * classifies Brand Governance Status, and builds insert approval bundles.
 *
 * Owner-facing / public / product use remains Active/Live (or explicitly approved).
 * Controlled mode: never writes Airtable.
 * VIC = evidence / dedupe only. Brand Setup / Brand Explorer = read-only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { buildActiveBrandSetupControlList } from "./census-autopilot-active-brand-scope.js";
import {
  buildOfficialParentInventoryDiscoveryControlList,
  classifyBrandGovernanceStatus,
  buildNonActiveCensusGovernanceFields,
  BRAND_GOVERNANCE_STATUS,
  CENSUS_ONLY_PRODUCTION_USE_STATUS,
} from "./census-brand-governance.js";
import {
  APPROVED_SOFT_BRAND_MAPPINGS,
} from "./census-autopilot-brand-census-matcher.js";
import {
  getFamilyAdapterCacheStats,
} from "./census-autopilot-family-directory-adapters.js";
import {
  ensureMarriottCalaCountrySitemapCache,
  iterateMarriottDirectoryRows,
  marriottDiscoveryCountryShort,
  isDeprecatedMarriottSitemapHotelsXml,
  MARRIOTT_DISCOVERY_SOURCE,
  MARRIOTT_CALA_PRIORITY_COUNTRIES,
  listMarriottDiscoveryCountries,
} from "./census-autopilot-marriott-discovery-adapter.js";
import {
  ensureHiltonCalaDirectoryCache,
  iterateHiltonDirectoryRows,
  HILTON_DISCOVERY_SOURCE,
} from "./census-autopilot-hilton-cala-discovery-adapter.js";
import {
  ensureChoiceCalaRegionalCache,
  iterateChoiceDirectoryRows,
  CHOICE_DISCOVERY_SOURCE,
} from "./census-autopilot-choice-cala-discovery-adapter.js";
import {
  ensureIhgCalaDestinationCache,
  iterateIhgDirectoryRows,
  IHG_DISCOVERY_SOURCE,
} from "./census-autopilot-ihg-cala-discovery-adapter.js";
import {
  ensureAccorCalaDirectoryCache,
  iterateAccorDirectoryRows,
  ACCOR_DISCOVERY_SOURCE,
} from "./census-autopilot-accor-cala-discovery-adapter.js";
import {
  ensureWyndhamCalaDirectoryCache,
  iterateWyndhamDirectoryRows,
  WYNDHAM_DISCOVERY_SOURCE,
} from "./census-autopilot-wyndham-cala-discovery-adapter.js";
import {
  ensurePreferredCalaDirectoryCache,
  iteratePreferredDirectoryRows,
  PREFERRED_DISCOVERY_SOURCE,
} from "./census-autopilot-preferred-directory-discovery-adapter.js";
import { CALA_DISCOVERY_PRIORITY_COUNTRIES } from "./census-autopilot-cala-discovery-shared.js";
import {
  AUTOPILOT_FORBIDDEN_FIELDS,
  isForbiddenAutopilotField,
} from "./census-autopilot-field-allowlist.js";
import {
  buildCalaDiscoveryRegionPlan,
  isDiscoveryAdapterReady,
  listCountriesWithDiscoveryAdapter,
} from "./production-census-cala-region-config.js";
import {
  MAP_FIRST_PASS,
  loadVicClaimIndex,
} from "./production-census-first-pass-enrichment.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  getProductionCensusSourceOfTruthSnapshot,
  PRECISE_MATCH_SUMMARY_LINE,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import { PRODUCTION_USE_STATUS as WRITE_PRODUCTION_USE_STATUS } from "./production-census-write.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const SOURCE_DISCOVERY_VERSION = "census-autopilot-source-discovery-v4";
export const SOURCE_DISCOVERY_QUEUE_ID = "source_discovery";

export const DISCOVERY_STATUS = Object.freeze({
  READY: "production_census_cala_discovery_mode_ready",
  READY_NEEDS_ADAPTER: "production_census_cala_discovery_mode_ready_needs_source_adapter",
  NEEDS_REFACTOR: "production_census_cala_discovery_mode_needs_refactor",
  BLOCKED: "production_census_cala_discovery_mode_blocked",
});

export const MATCH_CLASS = Object.freeze({
  EXISTING_EXACT: "existing_exact_match",
  EXISTING_PROBABLE: "existing_probable_match",
  NEW_CANDIDATE: "new_property_candidate",
  DUPLICATE_RISK: "duplicate_risk",
  STEWARD: "steward_review_required",
  IDENTITY_CONFLICT: "blocked_identity_conflict",
  SOURCE_INSUFFICIENT: "source_insufficient",
});

/** Core + optional High insert fields (Hotel Property Census only). */
export const INSERT_CORE_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Current Brand",
  "Brand Family",
  "Brand Explorer Slug if mapped",
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
  "Phone",
]);

export const INSERT_ADDRESS_FIELDS = Object.freeze([
  "Address",
  "Address Confidence",
  "Address Source URL",
]);

export const INSERT_OPTIONAL_HIGH_FIELDS = Object.freeze([
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "Property Type",
  "Asset Context",
  "Market / Submarket",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Reviewed Date",
  "Rooms Notes",
]);

export const INSERT_ALLOWED_FIELDS = Object.freeze([
  ...INSERT_CORE_FIELDS,
  ...INSERT_ADDRESS_FIELDS,
  ...INSERT_OPTIONAL_HIGH_FIELDS,
]);

export const INSERT_FORBIDDEN_FIELDS = Object.freeze([
  ...AUTOPILOT_FORBIDDEN_FIELDS,
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
]);

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normUrl(u) {
  try {
    const x = new URL(String(u || "").trim());
    x.hash = "";
    let p = x.pathname.replace(/\/+$/, "");
    return `${x.origin}${p}`.toLowerCase();
  } catch {
    return norm(u);
  }
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
 * Active/Live discovery control list (Brand Setup read-only).
 */
function adapterNameForFamilyCountry(family, country) {
  if (family === "Hilton") return "hilton_cala_locations";
  if (family === "Choice") return "choice_cala_regional";
  if (family === "Marriott") return "marriott_cala_country_sitemap";
  if (family === "IHG") return "ihg_cala_destination_directory";
  if (family === "Accor") return "accor_cala_continent_catalog";
  if (family === "Wyndham") return "wyndham_cala_property_sitemap";
  if (family === "Preferred") return "preferred_directory";
  return null;
}

function attachDiscoveryAdapters(brands = []) {
  return brands.map((b) => {
    const family = b.brand_family || b.extractor_family || "generic";
    const readyCountries = listCountriesWithDiscoveryAdapter(family);
    const discovery_adapters = Object.fromEntries(
      readyCountries.map((c) => [c, adapterNameForFamilyCountry(family, c)])
    );
    return {
      ...b,
      discovery_adapter_available: readyCountries.length > 0,
      discovery_adapter_countries: readyCountries,
      discovery_adapters,
      source_family: family,
      extractor_family: family,
    };
  });
}

/**
 * Active/Live-only discovery control list (legacy / owner-facing enrichment scope).
 * Prefer buildOfficialInventoryDiscoveryControlList for discovery + coverage.
 */
export function buildActiveBrandDiscoveryControlList(opts = {}) {
  const base = buildActiveBrandSetupControlList(opts);
  const brands = attachDiscoveryAdapters(base.brands || []);

  return {
    ...base,
    version: "census-autopilot-active-brand-discovery-control-list-v2",
    purpose: "source_discovery_active_only",
    discover_all_official_parents: false,
    require_brand_match_default: true,
    brand_setup_read_only: true,
    brand_explorer_untouched: true,
    marriott_hqv_required_for_discovery: false,
    marriott_discovery_source: MARRIOTT_DISCOVERY_SOURCE,
    brands,
    adapters_available_count: brands.filter((b) => b.discovery_adapter_available).length,
    adapters_missing_count: brands.filter((b) => !b.discovery_adapter_available).length,
  };
}

/**
 * Official parent-company inventory discovery control list.
 * Includes Active/Live + all census-official brands for ready parent families.
 */
export function buildOfficialInventoryDiscoveryControlList(opts = {}) {
  const base = buildOfficialParentInventoryDiscoveryControlList(opts);
  const brands = attachDiscoveryAdapters(base.brands || []);
  return {
    ...base,
    brands,
    marriott_hqv_required_for_discovery: false,
    marriott_discovery_source: MARRIOTT_DISCOVERY_SOURCE,
    adapters_available_count: brands.filter((b) => b.discovery_adapter_available).length,
    adapters_missing_count: brands.filter((b) => !b.discovery_adapter_available).length,
  };
}

function annotateDiscoveredGovernance(row, opts = {}) {
  const gov = classifyBrandGovernanceStatus(
    {
      brand: row.brand,
      brand_slug: row.brand_slug,
      property_name: row.property_name,
      official_property_url: row.official_property_url,
      source_url: row.official_directory_url || row.official_property_url,
      parent_company: row.parent_company,
      source_family: row.source_family,
    },
    opts
  );
  return {
    ...row,
    brand_governance_status: gov.status,
    owner_facing_eligible: gov.owner_facing_eligible,
    brand_setup_promotion_candidate: Boolean(gov.brand_setup_promotion_candidate),
    in_active_brand_setup: gov.in_active_brand_setup,
    in_official_registry: gov.in_official_registry,
    governance_reasons: gov.reasons,
  };
}

export function writeActiveBrandDiscoveryControlList(runDir, controlList) {
  const fp = path.join(runDir, "active-brand-discovery-control-list.json");
  writeJson(fp, controlList);
  return fp;
}

/**
 * Sanitize insert fields — fail closed on forbidden.
 * @param {Record<string, unknown>} fields
 */
export function sanitizeInsertFields(fields = {}) {
  const out = {};
  const dropped = [];
  for (const [k, v] of Object.entries(fields || {})) {
    if (INSERT_FORBIDDEN_FIELDS.includes(k) || isForbiddenAutopilotField(k)) {
      dropped.push({ field: k, reason: "forbidden_on_insert" });
      continue;
    }
    if (!INSERT_ALLOWED_FIELDS.includes(k)) {
      dropped.push({ field: k, reason: "not_allowlisted_for_insert" });
      continue;
    }
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return { fields: out, dropped };
}

/**
 * Build stable identity key for discovered property.
 */
export function buildDiscoveredIdentityKey(row) {
  const family = String(row.source_family || row.family || "").toLowerCase();
  const countryShort = marriottDiscoveryCountryShort(row.country || "Mexico");
  const code = String(row.official_property_id || row.property_code || "")
    .trim()
    .toLowerCase();
  if (family === "hilton" && code) return `ind_hilton_${countryShort}_${code}`;
  if (family === "choice" && code) return `ind_choice_${countryShort}_${code}`;
  if (family === "marriott" && code) return `ind_marriott_${countryShort}_${code}`;
  if (family === "ihg" && code) return `ind_ihg_${countryShort}_${code}`;
  if (family === "accor" && code) return `ind_accor_${countryShort}_${code.toLowerCase()}`;
  if (family === "wyndham" && code) {
    const safe = code.toLowerCase().replace(/[^a-z0-9_-]/g, "").slice(0, 48);
    return safe ? `ind_wyndham_${countryShort}_${safe}` : null;
  }
  if (family === "preferred" && code) return `ind_preferred_${countryShort}_${code.toLowerCase()}`;
  return null;
}

function brandMatchesControl(brandName, controlBrands) {
  const n = norm(brandName);
  if (!n) return null;
  for (const b of controlBrands) {
    if (norm(b.brand_name) === n) return b;
    if ((b.census_matching_aliases || []).some((a) => norm(a) === n)) return b;
    const soft = APPROVED_SOFT_BRAND_MAPPINGS[n];
    if (soft && soft === b.brand_slug) return b;
  }
  return null;
}

/**
 * Convert Hilton directory row → discovered property.
 */
export function hiltonRowToDiscovered(row, controlBrand = null) {
  const code = String(row.ctyhocn || "").trim().toUpperCase();
  const brand = controlBrand?.brand_name || row.affiliation || row.brand || null;
  const discovered = {
    official_property_id: code,
    property_code: code,
    property_name: row.name || row.hotelName || null,
    brand,
    brand_slug: controlBrand?.brand_slug || null,
    parent_company: controlBrand?.parent_company || row.parent || "Hilton",
    city: row.city || null,
    state_region: row.state || row.stateName || null,
    country: row.country || "Mexico",
    address: row.addressLine1 || row.address || null,
    official_property_url: row.propertyUrl || row.url || null,
    official_directory_url: row.sourceUrl || row.source_url || null,
    source_family: "Hilton",
    source_type: "official_brand_directory",
    source_confidence: code && (row.name || row.hotelName) ? "High" : "Medium",
    identity_confidence: code && (row.name || row.hotelName) && (row.city || row.addressLine1) ? "High" : "Medium",
    latitude: row.latitude ?? null,
    longitude: row.longitude ?? null,
    discovered_date: todayIsoDate(),
  };
  discovered.identity_key = buildDiscoveredIdentityKey(discovered);
  return discovered;
}

/**
 * Convert Marriott country-sitemap row → discovered property.
 * HQV is never consulted here.
 */
export function marriottRowToDiscovered(row, controlBrand = null) {
  const code = String(row.marshaCode || row.propertyId || "").trim().toUpperCase();
  const brandMapped = row.brand || row.affiliation || null;
  const brandUnconfirmed = /brand unconfirmed/i.test(String(brandMapped || ""));
  const brand =
    controlBrand?.brand_name && !brandUnconfirmed
      ? controlBrand.brand_name
      : brandMapped;
  const city = row.city || null;
  const url = row.propertyUrl || row.website || null;
  const directoryUrl = row.sourceUrl || row.source_url || null;
  if (directoryUrl && isDeprecatedMarriottSitemapHotelsXml(directoryUrl)) {
    return null;
  }
  const identityHigh =
    Boolean(code) &&
    Boolean(row.name) &&
    Boolean(brand) &&
    !brandUnconfirmed &&
    Boolean(row.country) &&
    Boolean(url);
  const discovered = {
    official_property_id: code,
    property_code: code,
    property_name: row.name || null,
    brand,
    brand_slug: controlBrand?.brand_slug || null,
    parent_company: controlBrand?.parent_company || row.parent || "Marriott",
    city,
    state_region: row.state || null,
    country: row.country || null,
    address: row.addressLine1 || row.address || null,
    official_property_url: url,
    official_directory_url: directoryUrl,
    source_family: "Marriott",
    source_type: "official_brand_directory",
    source_confidence: code && row.name ? "High" : "Medium",
    identity_confidence: identityHigh ? "High" : code && row.name ? "Medium" : "Low",
    latitude: null,
    longitude: null,
    hqv_used_for_discovery: false,
    discovered_date: todayIsoDate(),
  };
  discovered.identity_key = buildDiscoveredIdentityKey(discovered);
  return discovered;
}

/**
 * Convert Choice regional row → discovered property.
 */
export function choiceRowToDiscovered(row, controlBrand = null) {
  const code = String(row.propertyId || "").trim().toUpperCase();
  const brand = controlBrand?.brand_name || row.brand || null;
  const address = [row.addressLine1, row.addressLine2].filter(Boolean).join(", ") || null;
  const propertyUrl = row.propertyUrl || null;
  // Never promote regional placeId URL as official property URL
  const safeUrl =
    propertyUrl && /regional-hotels/i.test(propertyUrl) && /placeId=/i.test(propertyUrl)
      ? null
      : propertyUrl;
  const discovered = {
    official_property_id: code,
    property_code: code,
    property_name: row.name || null,
    brand,
    brand_slug: controlBrand?.brand_slug || null,
    parent_company: controlBrand?.parent_company || "Choice",
    city: row.city || null,
    state_region: row.state || null,
    country: row.country || "Mexico",
    address,
    official_property_url: safeUrl,
    official_directory_url: row.source_url || null,
    source_family: "Choice",
    source_type: "official_brand_directory",
    source_confidence: code && row.name ? "High" : code ? "Medium" : "Low",
    identity_confidence: code && row.name && (row.city || address || safeUrl) ? "High" : "Medium",
    latitude: row.latitude ?? null,
    longitude: row.longitude ?? null,
    discovered_date: todayIsoDate(),
  };
  discovered.identity_key = buildDiscoveredIdentityKey(discovered);
  return discovered;
}

/**
 * Convert IHG destination directory row → discovered property.
 */
export function ihgRowToDiscovered(row, controlBrand = null) {
  const code = String(row.propertyId || row.mnemonic || "").trim().toUpperCase();
  const brand = controlBrand?.brand_name || row.brand || null;
  const address = row.addressLine1 || row.addressText || null;
  const url = row.propertyUrl || row.website || null;
  const identityHigh =
    Boolean(code) && Boolean(row.name) && Boolean(brand) && Boolean(row.country) && Boolean(url);
  const discovered = {
    official_property_id: code,
    property_code: code,
    property_name: row.name || null,
    brand,
    brand_slug: controlBrand?.brand_slug || null,
    parent_company: controlBrand?.parent_company || row.parent || "IHG",
    city: row.city || null,
    state_region: row.state || null,
    country: row.country || null,
    address,
    official_property_url: url,
    official_directory_url: row.sourceUrl || null,
    source_family: "IHG",
    source_type: "official_brand_directory",
    source_confidence: code && row.name ? "High" : "Medium",
    identity_confidence: identityHigh ? "High" : code && row.name ? "Medium" : "Low",
    latitude: null,
    longitude: null,
    discovered_date: todayIsoDate(),
  };
  discovered.identity_key = buildDiscoveredIdentityKey(discovered);
  return discovered;
}

/**
 * Convert Accor catalog/continent row → discovered property.
 */
export function accorRowToDiscovered(row, controlBrand = null) {
  const code = String(row.propertyId || "").trim().toUpperCase();
  const brand = controlBrand?.brand_name || row.brand || null;
  const url = row.propertyUrl || null;
  const identityHigh =
    Boolean(code) && Boolean(row.name) && Boolean(brand) && Boolean(row.country) && Boolean(url);
  const discovered = {
    official_property_id: code,
    property_code: code,
    property_name: row.name || null,
    brand,
    brand_slug: controlBrand?.brand_slug || null,
    parent_company: controlBrand?.parent_company || row.parent || "Accor",
    city: row.city || null,
    state_region: row.state || null,
    country: row.country || null,
    address: row.addressLine1 || row.address || null,
    official_property_url: url,
    official_directory_url: row.sourceUrl || null,
    source_family: "Accor",
    source_type: "official_brand_directory",
    source_confidence: code && row.name ? "High" : "Medium",
    identity_confidence: identityHigh ? "High" : code && row.name ? "Medium" : "Low",
    latitude: row.latitude ?? null,
    longitude: row.longitude ?? null,
    discovered_date: todayIsoDate(),
  };
  discovered.identity_key = buildDiscoveredIdentityKey(discovered);
  return discovered;
}

/**
 * Convert Wyndham sitemap/JSON-LD row → discovered property.
 */
export function wyndhamRowToDiscovered(row, controlBrand = null) {
  const code = String(row.propertyId || row.propertySlug || "").trim();
  const brand = controlBrand?.brand_name || row.brand || null;
  const url = row.propertyUrl || null;
  const countryConfirmed = Boolean(row.country) && row.calaFilterStatus !== "excluded_non_cala";
  const identityHigh =
    Boolean(code) &&
    Boolean(row.name) &&
    Boolean(brand) &&
    countryConfirmed &&
    Boolean(url);
  const discovered = {
    official_property_id: code,
    property_code: code,
    property_name: row.name || null,
    brand,
    brand_slug: controlBrand?.brand_slug || row.brandSlug || null,
    parent_company: controlBrand?.parent_company || row.parent || "Wyndham",
    city: row.city || null,
    state_region: row.state || null,
    country: row.country || null,
    address: row.addressLine1 || row.address || null,
    official_property_url: url,
    official_directory_url: row.sourceUrl || WYNDHAM_DISCOVERY_SOURCE.sitemap_index,
    source_family: "Wyndham",
    source_type: "official_brand_directory",
    source_confidence: code && row.name && countryConfirmed ? "High" : "Medium",
    identity_confidence: identityHigh ? "High" : code && row.name ? "Medium" : "Low",
    latitude: row.latitude ?? null,
    longitude: row.longitude ?? null,
    discovered_date: todayIsoDate(),
  };
  discovered.identity_key = buildDiscoveredIdentityKey(discovered);
  return discovered;
}

/**
 * Convert Preferred directory row → discovered property.
 * Collections are metadata only — brand stays Preferred Hotels & Resorts.
 */
export function preferredRowToDiscovered(row, controlBrand = null) {
  const code = String(row.propertyId || row.nid || "").trim();
  const brand =
    controlBrand?.brand_name ||
    row.brand ||
    PREFERRED_DISCOVERY_SOURCE.brand_display;
  const url = row.propertyUrl || null;
  const identityHigh =
    Boolean(code) && Boolean(row.name) && Boolean(brand) && Boolean(row.country) && Boolean(url);
  const discovered = {
    official_property_id: code,
    property_code: code,
    property_name: row.name || null,
    brand,
    brand_slug: controlBrand?.brand_slug || "preferred-hotels-and-resorts",
    parent_company:
      controlBrand?.parent_company || row.parent || PREFERRED_DISCOVERY_SOURCE.parent_company,
    city: row.city || null,
    state_region: row.state || null,
    country: row.country || null,
    address: row.addressLine1 || null,
    official_property_url: url,
    official_directory_url: row.sourceUrl || PREFERRED_DISCOVERY_SOURCE.directory_url,
    source_family: "Preferred",
    source_type: "official_brand_directory",
    source_confidence: code && row.name ? "High" : "Medium",
    identity_confidence: identityHigh ? "High" : code && row.name ? "Medium" : "Low",
    collection_labels: row.collections || [],
    latitude: null,
    longitude: null,
    discovered_date: todayIsoDate(),
  };
  discovered.identity_key = buildDiscoveredIdentityKey(discovered);
  return discovered;
}

/**
 * VIC claim → evidence-only discovered signal (never production insert by itself unless High identity + directory-backed).
 */
export function vicClaimToEvidenceRow(claim, family) {
  return {
    identity_key: claim.independent_record_id,
    official_property_id: null,
    property_name: claim.name || null,
    brand: claim.brand || null,
    city: claim.city || null,
    country: claim.country || "Mexico",
    official_property_url: claim.official_property_url || null,
    source_family: family || claim.family || "VIC",
    source_type: "vic_source_claim",
    source_confidence: "Medium",
    identity_confidence: "Medium",
    evidence_only: true,
    discovered_date: todayIsoDate(),
  };
}

/**
 * Index Hotel Property Census for matching.
 * @param {Array<{id: string, fields?: object}>} censusRecords
 */
export function indexHotelPropertyCensus(censusRecords = []) {
  const byIdentity = new Map();
  const byUrl = new Map();
  const byCode = new Map();
  const byNameBrandCityCountry = new Map();
  const byAddress = new Map();

  for (const row of censusRecords) {
    const f = row.fields || {};
    const identity = String(f[MAP_FIRST_PASS.identityKey] || "").trim();
    const url = normUrl(f[MAP_FIRST_PASS.officialUrl] || f[MAP_FIRST_PASS.sourceUrl] || "");
    const name = norm(f[MAP_FIRST_PASS.propertyName]);
    const brand = norm(f[MAP_FIRST_PASS.currentBrand]);
    const city = norm(f[MAP_FIRST_PASS.city]);
    const country = norm(f[MAP_FIRST_PASS.country]);
    const address = norm(f[MAP_FIRST_PASS.address]);
    const entry = { record_id: row.id, fields: f, identity_key: identity };

    if (identity) byIdentity.set(norm(identity), entry);
    if (url) byUrl.set(url, entry);
    // Hilton/Choice codes embedded in identity key
    const hilton = identity.match(/ind_hilton_[a-z]+_([a-z0-9]+)/i);
    const choice = identity.match(/ind_choice_[a-z]+_([a-z]{2}\d+)/i);
    const marriott = identity.match(/ind_marriott_[a-z]+_([a-z0-9]+)/i);
    const ihg = identity.match(/ind_ihg_[a-z]+_([a-z0-9]+)/i);
    if (hilton) byCode.set(`hilton:${hilton[1].toLowerCase()}`, entry);
    if (choice) byCode.set(`choice:${choice[1].toLowerCase()}`, entry);
    if (marriott) byCode.set(`marriott:${marriott[1].toLowerCase()}`, entry);
    if (ihg) byCode.set(`ihg:${ihg[1].toLowerCase()}`, entry);
    if (name && brand && city && country) {
      byNameBrandCityCountry.set(`${name}|${brand}|${city}|${country}`, entry);
    }
    if (address && country) byAddress.set(`${address}|${country}`, entry);
  }

  return { byIdentity, byUrl, byCode, byNameBrandCityCountry, byAddress, count: censusRecords.length };
}

/**
 * Match one discovered property against Hotel Property Census index.
 */
export function matchDiscoveredProperty(discovered, index) {
  if (!discovered?.property_name && !discovered?.official_property_id && !discovered?.official_property_url) {
    return {
      classification: MATCH_CLASS.SOURCE_INSUFFICIENT,
      match_method: null,
      census_record_id: null,
      reason: "missing_identity_signals",
    };
  }

  const fam = String(discovered.source_family || "").toLowerCase();
  const code = String(discovered.official_property_id || "").toLowerCase();
  if (code && fam) {
    const hit = index.byCode.get(`${fam}:${code}`);
    if (hit) {
      return {
        classification: MATCH_CLASS.EXISTING_EXACT,
        match_method: "official_property_id",
        census_record_id: hit.record_id,
        census_identity_key: hit.identity_key,
      };
    }
  }

  if (discovered.identity_key) {
    const hit = index.byIdentity.get(norm(discovered.identity_key));
    if (hit) {
      return {
        classification: MATCH_CLASS.EXISTING_EXACT,
        match_method: "identity_key",
        census_record_id: hit.record_id,
        census_identity_key: hit.identity_key,
      };
    }
  }

  const url = normUrl(discovered.official_property_url);
  if (url) {
    const hit = index.byUrl.get(url);
    if (hit) {
      return {
        classification: MATCH_CLASS.EXISTING_EXACT,
        match_method: "exact_official_url",
        census_record_id: hit.record_id,
        census_identity_key: hit.identity_key,
      };
    }
  }

  const name = norm(discovered.property_name);
  const brand = norm(discovered.brand);
  const city = norm(discovered.city);
  const country = norm(discovered.country);
  if (name && brand && city && country) {
    const hit = index.byNameBrandCityCountry.get(`${name}|${brand}|${city}|${country}`);
    if (hit) {
      return {
        classification: MATCH_CLASS.EXISTING_EXACT,
        match_method: "name_brand_city_country",
        census_record_id: hit.record_id,
        census_identity_key: hit.identity_key,
      };
    }
  }

  if (discovered.address && country) {
    const hit = index.byAddress.get(`${norm(discovered.address)}|${country}`);
    if (hit) {
      // Address match without ID/URL → probable / duplicate risk if names differ
      const censusName = norm(hit.fields?.[MAP_FIRST_PASS.propertyName]);
      if (name && censusName && name !== censusName) {
        return {
          classification: MATCH_CLASS.DUPLICATE_RISK,
          match_method: "address_match_name_conflict",
          census_record_id: hit.record_id,
          census_identity_key: hit.identity_key,
          reason: "address_matches_different_name",
        };
      }
      return {
        classification: MATCH_CLASS.EXISTING_PROBABLE,
        match_method: "address_match",
        census_record_id: hit.record_id,
        census_identity_key: hit.identity_key,
      };
    }
  }

  // Soft brand alias with name+city but weak brand string → steward
  if (name && city && country && !brand) {
    return {
      classification: MATCH_CLASS.STEWARD,
      match_method: null,
      reason: "missing_brand_for_insert",
    };
  }

  if (discovered.evidence_only) {
    return {
      classification: MATCH_CLASS.SOURCE_INSUFFICIENT,
      match_method: "vic_evidence_only",
      reason: "vic_not_production_insert_source",
    };
  }

  const identityHigh =
    String(discovered.identity_confidence || "") === "High" &&
    Boolean(discovered.identity_key) &&
    Boolean(discovered.property_name) &&
    Boolean(discovered.brand) &&
    Boolean(discovered.country) &&
    Boolean(discovered.official_property_id || discovered.official_property_url);

  if (!identityHigh) {
    if (!discovered.property_name || !discovered.official_property_id) {
      return {
        classification: MATCH_CLASS.SOURCE_INSUFFICIENT,
        match_method: null,
        reason: "identity_not_high",
      };
    }
    return {
      classification: MATCH_CLASS.STEWARD,
      match_method: null,
      reason: "identity_below_high_for_insert",
    };
  }

  return {
    classification: MATCH_CLASS.NEW_CANDIDATE,
    match_method: null,
    census_record_id: null,
    identity_confidence: "High",
  };
}

/**
 * Build insert field payload for a High new_property_candidate.
 * Evidence-backed non-active brands → Census Only / Hold / Human Review.
 */
export function buildInsertFieldsFromDiscovered(discovered, opts = {}) {
  const gov =
    opts.governance ||
    classifyBrandGovernanceStatus(
      {
        brand: discovered.brand,
        brand_slug: discovered.brand_slug,
        property_name: discovered.property_name,
        official_property_url: discovered.official_property_url,
        source_url: discovered.official_directory_url || discovered.official_property_url,
        parent_company: discovered.parent_company,
        source_family: discovered.source_family,
      },
      opts
    );

  const ownerFacing = gov.status === BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP;
  const useStatus = ownerFacing
    ? WRITE_PRODUCTION_USE_STATUS || CENSUS_ONLY_PRODUCTION_USE_STATUS
    : CENSUS_ONLY_PRODUCTION_USE_STATUS;

  const nonActiveFields = buildNonActiveCensusGovernanceFields(gov, {
    explicitly_approved: opts.explicitly_approved === true,
  });
  const humanReview =
    opts.human_review_required === true ||
    nonActiveFields["Human Review Required"] === true;

  /** @type {Record<string, unknown>} */
  const fields = {
    "Property Name": discovered.property_name,
    "Property Identity Key": discovered.identity_key,
    "Current Brand": discovered.brand,
    "Brand Family": discovered.parent_company || discovered.source_family,
    "Affiliation Status": "Branded",
    City: discovered.city || "Unknown",
    Country: discovered.country || "Unknown",
    "Source URL": discovered.official_directory_url || discovered.official_property_url,
    "Official Property URL": discovered.official_property_url,
    "Family / Source Family": discovered.source_family,
    "Source Type": "brand_directory",
    "Source Confidence": discovered.source_confidence || "High",
    "Identity Confidence": discovered.identity_confidence || "High",
    "Data Eligible": true,
    "Production Use Status": useStatus,
    "Enrichment Status": "Discovered — pending enrichment",
    "Enrichment Priority": "High",
    "Last Reviewed Date": todayIsoDate(),
    "Discovery Date": discovered.discovered_date || todayIsoDate(),
    ...nonActiveFields,
  };
  // Only set HR for true data-quality / forced cases — governance Holds alone do not set HR
  if (humanReview) fields["Human Review Required"] = true;
  if (discovered.state_region) fields["State / Region"] = discovered.state_region;
  // Do not set Brand Explorer Slug on Census insert — Autopilot forbids BE-facing fields;
  // brand_slug remains on the discovery row for matching/reporting only.

  if (discovered.address && discovered.identity_confidence === "High") {
    fields.Address = discovered.address;
    fields["Address Confidence"] = "High";
    fields["Address Source URL"] =
      discovered.official_property_url || discovered.official_directory_url;
  }

  // Official directory coordinates only (no geocode provider invent)
  if (
    opts.allowOfficialCoordinates !== false &&
    discovered.latitude != null &&
    discovered.longitude != null &&
    Number.isFinite(Number(discovered.latitude)) &&
    Number.isFinite(Number(discovered.longitude))
  ) {
    fields.Latitude = Number(discovered.latitude);
    fields.Longitude = Number(discovered.longitude);
    fields["Coordinate Source Type"] = "official_brand_directory";
    fields["Coordinate Confidence"] = "High";
  }

  return sanitizeInsertFields(fields);
}

/**
 * Discover properties from ready adapters (+ VIC evidence).
 * Default: official parent-company inventory (not Active/Live-only).
 */
export async function discoverCalaProperties(opts = {}) {
  const discoverAllOfficial = opts.discoverAllOfficialParents !== false;
  // When a parent is scoped, only run that family's adapters (still include non-Active brands).
  const parentScopedDiscovery = Boolean(opts.parentCompany);
  const allowFamily = (parentScopedFlag) =>
    !parentScopedDiscovery || parentScopedFlag || opts.forceAllFamilies === true;
  const controlList =
    opts.controlList ||
    (discoverAllOfficial
      ? buildOfficialInventoryDiscoveryControlList(opts)
      : buildActiveBrandDiscoveryControlList(opts));
  const requireBrandMatch =
    opts.requireBrandMatch !== undefined
      ? opts.requireBrandMatch !== false
      : controlList.require_brand_match_default === true && !discoverAllOfficial;
  const regionPlan = opts.regionPlan || buildCalaDiscoveryRegionPlan(opts);
  const discovered = [];
  const sourceReport = {
    families_used: [],
    blocked_source_families: [],
    adapter_errors: [],
    vic_evidence_rows: 0,
    discover_all_official_parents: discoverAllOfficial,
    parent_scoped_discovery: parentScopedDiscovery,
    require_brand_match: requireBrandMatch,
    control_list_purpose: controlList.purpose || null,
  };

  const hiltonBrands = controlList.brands.filter((b) => b.brand_family === "Hilton");
  const choiceBrands = controlList.brands.filter((b) => b.brand_family === "Choice");
  const marriottBrands = controlList.brands.filter((b) => b.brand_family === "Marriott");
  const ihgBrands = controlList.brands.filter((b) => b.brand_family === "IHG");
  const accorBrands = controlList.brands.filter((b) => b.brand_family === "Accor");
  const wyndhamBrands = controlList.brands.filter((b) => b.brand_family === "Wyndham");
  const preferredBrands = controlList.brands.filter(
    (b) => b.brand_family === "Preferred" || /preferred/i.test(String(b.parent_company || ""))
  );

  const countryFilter = opts.country ? String(opts.country).trim() : null;
  const priorityCountries = opts.discoveryCountries?.length
    ? opts.discoveryCountries
    : CALA_DISCOVERY_PRIORITY_COUNTRIES;

  const hiltonParentScoped = opts.parentCompany && /hilton/i.test(String(opts.parentCompany));
  const choiceParentScoped = opts.parentCompany && /choice/i.test(String(opts.parentCompany));
  const ihgParentScoped = opts.parentCompany && /ihg/i.test(String(opts.parentCompany));
  const accorParentScoped = opts.parentCompany && /accor/i.test(String(opts.parentCompany));
  const wyndhamParentScoped = opts.parentCompany && /wyndham/i.test(String(opts.parentCompany));
  const preferredParentScoped =
    opts.parentCompany && /preferred/i.test(String(opts.parentCompany));

  const hiltonCountries = (countryFilter ? [countryFilter] : priorityCountries).filter((c) =>
    isDiscoveryAdapterReady(c, "Hilton")
  );
  const runHilton =
    allowFamily(hiltonParentScoped) &&
    (hiltonBrands.length > 0 || hiltonParentScoped || discoverAllOfficial) &&
    hiltonCountries.length > 0;

  if (runHilton) {
    try {
      const cache =
        opts.hiltonCache ||
        (await ensureHiltonCalaDirectoryCache({
          countries: hiltonCountries,
          country: countryFilter,
          delayMs: opts.delayMs ?? 100,
        }));
      sourceReport.families_used.push("Hilton");
      sourceReport.hilton_discovery = {
        source: HILTON_DISCOVERY_SOURCE,
        countries: hiltonCountries,
        meta: cache._meta || null,
      };
      for (const row of iterateHiltonDirectoryRows(cache)) {
        const brandHit =
          brandMatchesControl(row.affiliation || row.brand, hiltonBrands) ||
          (hiltonBrands.length === 1 && requireBrandMatch ? hiltonBrands[0] : null);
        const scoped =
          brandHit ||
          (hiltonParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.affiliation || row.brand,
                brand_slug: null,
                parent_company: "Hilton",
              }
            : null);
        if (!scoped) continue;
        if (!brandHit && requireBrandMatch && !hiltonParentScoped && !discoverAllOfficial) {
          continue;
        }
        discovered.push(hiltonRowToDiscovered(row, scoped));
      }
    } catch (err) {
      sourceReport.blocked_source_families.push("Hilton");
      sourceReport.adapter_errors.push({ family: "Hilton", error: err?.message || String(err) });
    }
  } else if (
    allowFamily(hiltonParentScoped) &&
    (hiltonBrands.length || hiltonParentScoped)
  ) {
    sourceReport.blocked_source_families.push("Hilton");
  }

  const choiceCountries = (countryFilter ? [countryFilter] : priorityCountries).filter((c) =>
    isDiscoveryAdapterReady(c, "Choice")
  );
  const runChoice =
    allowFamily(choiceParentScoped) &&
    (choiceBrands.length > 0 || choiceParentScoped || discoverAllOfficial) &&
    choiceCountries.length > 0;

  if (runChoice) {
    try {
      const cache =
        opts.choiceCache ||
        (await ensureChoiceCalaRegionalCache({
          countries: choiceCountries,
          country: countryFilter,
          timeoutMs: opts.timeoutMs || 60000,
        }));
      sourceReport.families_used.push("Choice");
      sourceReport.choice_discovery = {
        source: CHOICE_DISCOVERY_SOURCE,
        countries: choiceCountries,
        meta: cache._meta || null,
      };
      for (const row of iterateChoiceDirectoryRows(cache)) {
        const brandHit = brandMatchesControl(row.brand, choiceBrands);
        const activeChoice = choiceBrands.filter((b) => b.in_active_brand_setup);
        const scoped =
          brandHit ||
          (choiceParentScoped && activeChoice.length === 1 ? activeChoice[0] : null) ||
          (choiceParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.brand || null,
                brand_slug: null,
                parent_company: "Choice",
              }
            : choiceBrands.length === 1
              ? choiceBrands[0]
              : null);
        if (!scoped) continue;
        discovered.push(choiceRowToDiscovered(row, scoped));
      }
    } catch (err) {
      sourceReport.blocked_source_families.push("Choice");
      sourceReport.adapter_errors.push({ family: "Choice", error: err?.message || String(err) });
    }
  } else if (choiceBrands.length || choiceParentScoped) {
    sourceReport.blocked_source_families.push("Choice");
  }

  // Marriott CALA country hotel-sitemaps (HQV never required)
  // Default crawl = priority five (live-probed); expand via opts.marriottCountries.
  const defaultMarriottCountries = opts.marriottCountries?.length
    ? opts.marriottCountries
    : MARRIOTT_CALA_PRIORITY_COUNTRIES;
  const marriottCountriesWanted = listMarriottDiscoveryCountries({
    country: countryFilter,
    countries: countryFilter ? null : defaultMarriottCountries.filter((c) =>
      isDiscoveryAdapterReady(c, "Marriott")
    ),
  });
  const marriottParentScoped =
    opts.parentCompany && /marriott/i.test(String(opts.parentCompany));
  const runMarriott =
    allowFamily(marriottParentScoped) &&
    (marriottBrands.length > 0 ||
      marriottParentScoped ||
      opts.forceMarriottDiscovery === true ||
      discoverAllOfficial);

  if (runMarriott && marriottCountriesWanted.length) {
    try {
      const cache =
        opts.marriottCache ||
        (await ensureMarriottCalaCountrySitemapCache({
          countries: marriottCountriesWanted,
          delayMs: opts.delayMs ?? 200,
        }));
      sourceReport.families_used.push("Marriott");
      sourceReport.marriott_discovery = {
        source: MARRIOTT_DISCOVERY_SOURCE,
        countries: marriottCountriesWanted,
        hqv_required_for_discovery: false,
        deprecated_sitemap_hotels_xml_blocked: true,
        meta: cache._meta || null,
      };
      for (const row of iterateMarriottDirectoryRows(cache)) {
        if (row.sourceUrl && isDeprecatedMarriottSitemapHotelsXml(row.sourceUrl)) continue;
        // Never force a single control-list brand onto every Marriott row — brand must
        // come from directory mapping (mapMarriottMexicoBrand) or an exact control hit.
        const brandHit = brandMatchesControl(row.brand || row.affiliation, marriottBrands);
        const scoped =
          brandHit ||
          (marriottParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.brand || row.affiliation,
                brand_slug: null,
                parent_company: "Marriott",
              }
            : null);
        if (!scoped) continue;
        const disc = marriottRowToDiscovered(row, scoped);
        if (disc) discovered.push(disc);
      }
    } catch (err) {
      sourceReport.blocked_source_families.push("Marriott");
      sourceReport.adapter_errors.push({
        family: "Marriott",
        error: err?.message || String(err),
      });
    }
  } else if (marriottBrands.length || marriottParentScoped) {
    sourceReport.blocked_source_families.push("Marriott");
    sourceReport.adapter_errors.push({
      family: "Marriott",
      error: "no_ready_marriott_countries_in_scope",
      note: "Marriott country hotel-sitemap adapter ready for mapped CALA slugs; check country filter",
    });
  }

  const ihgCountries = (countryFilter ? [countryFilter] : priorityCountries).filter((c) =>
    isDiscoveryAdapterReady(c, "IHG")
  );
  const runIhg =
    allowFamily(ihgParentScoped) &&
    (ihgBrands.length > 0 || ihgParentScoped || discoverAllOfficial) &&
    ihgCountries.length > 0;

  if (runIhg) {
    try {
      const cache =
        opts.ihgCache ||
        (await ensureIhgCalaDestinationCache({
          countries: ihgCountries,
          country: countryFilter,
          delayMs: opts.delayMs ?? 200,
        }));
      sourceReport.families_used.push("IHG");
      sourceReport.ihg_discovery = {
        source: IHG_DISCOVERY_SOURCE,
        countries: ihgCountries,
        meta: cache._meta || null,
      };
      for (const row of iterateIhgDirectoryRows(cache)) {
        const brandHit =
          brandMatchesControl(row.brand, ihgBrands) ||
          (ihgBrands.length === 1 && requireBrandMatch ? ihgBrands[0] : null);
        const scoped =
          brandHit ||
          (ihgParentScoped || discoverAllOfficial || !requireBrandMatch
            ? { brand_name: row.brand, brand_slug: null, parent_company: "IHG" }
            : null);
        if (!scoped) continue;
        discovered.push(ihgRowToDiscovered(row, scoped));
      }
    } catch (err) {
      sourceReport.blocked_source_families.push("IHG");
      sourceReport.adapter_errors.push({ family: "IHG", error: err?.message || String(err) });
    }
  } else if (ihgBrands.length || ihgParentScoped) {
    sourceReport.blocked_source_families.push("IHG");
    sourceReport.adapter_errors.push({
      family: "IHG",
      error: "no_ready_ihg_countries_in_scope",
    });
  }

  // Accor CALA continent browse + Catalog hydrate
  const accorCountries = (countryFilter ? [countryFilter] : priorityCountries).filter((c) =>
    isDiscoveryAdapterReady(c, "Accor")
  );
  const runAccor =
    allowFamily(accorParentScoped) &&
    (accorBrands.length > 0 ||
      accorParentScoped ||
      opts.forceAccorDiscovery === true ||
      discoverAllOfficial) &&
    accorCountries.length > 0;

  if (runAccor) {
    try {
      const cache =
        opts.accorCache ||
        (await ensureAccorCalaDirectoryCache({
          countries: accorCountries,
          country: countryFilter,
          delayMs: opts.delayMs ?? 120,
          maxContinentPages: opts.accorMaxContinentPages,
        }));
      sourceReport.families_used.push("Accor");
      sourceReport.accor_discovery = {
        source: ACCOR_DISCOVERY_SOURCE,
        countries: accorCountries,
        meta: cache._meta || null,
      };
      for (const row of iterateAccorDirectoryRows(cache)) {
        const brandHit =
          brandMatchesControl(row.brand, accorBrands) ||
          (accorBrands.length === 1 && requireBrandMatch ? accorBrands[0] : null);
        const scoped =
          brandHit ||
          (accorParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.brand,
                brand_slug: null,
                parent_company: "Accor",
              }
            : null);
        if (!scoped) continue;
        const disc = accorRowToDiscovered(row, scoped);
        if (disc) discovered.push(disc);
      }
    } catch (err) {
      sourceReport.blocked_source_families.push("Accor");
      sourceReport.adapter_errors.push({ family: "Accor", error: err?.message || String(err) });
    }
  } else if (accorBrands.length || accorParentScoped) {
    sourceReport.blocked_source_families.push("Accor");
    sourceReport.adapter_errors.push({
      family: "Accor",
      error: "no_ready_accor_countries_in_scope",
    });
  }

  // Wyndham property sitemap + JSON-LD country filter
  const wyndhamCountries = (countryFilter ? [countryFilter] : priorityCountries).filter((c) =>
    isDiscoveryAdapterReady(c, "Wyndham")
  );
  const runWyndham =
    allowFamily(wyndhamParentScoped) &&
    (wyndhamBrands.length > 0 ||
      wyndhamParentScoped ||
      opts.forceWyndhamDiscovery === true ||
      discoverAllOfficial) &&
    wyndhamCountries.length > 0;

  if (runWyndham) {
    try {
      const cache =
        opts.wyndhamCache ||
        (await ensureWyndhamCalaDirectoryCache({
          countries: wyndhamCountries,
          country: countryFilter,
          delayMs: opts.delayMs ?? 80,
          maxMetadataFetch: opts.wyndhamMaxMetadataFetch,
          maxProperties: opts.wyndhamMaxProperties,
        }));
      sourceReport.families_used.push("Wyndham");
      sourceReport.wyndham_discovery = {
        source: WYNDHAM_DISCOVERY_SOURCE,
        countries: wyndhamCountries,
        meta: cache._meta || null,
      };
      for (const row of iterateWyndhamDirectoryRows(cache)) {
        const brandHit =
          brandMatchesControl(row.brand, wyndhamBrands) ||
          (wyndhamBrands.length === 1 && requireBrandMatch ? wyndhamBrands[0] : null);
        const scoped =
          brandHit ||
          (wyndhamParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: row.brand,
                brand_slug: row.brandSlug || null,
                parent_company: "Wyndham",
              }
            : null);
        if (!scoped) continue;
        const disc = wyndhamRowToDiscovered(row, scoped);
        if (disc) discovered.push(disc);
      }
    } catch (err) {
      sourceReport.blocked_source_families.push("Wyndham");
      sourceReport.adapter_errors.push({ family: "Wyndham", error: err?.message || String(err) });
    }
  } else if (wyndhamBrands.length || wyndhamParentScoped) {
    sourceReport.blocked_source_families.push("Wyndham");
    sourceReport.adapter_errors.push({
      family: "Wyndham",
      error: "no_ready_wyndham_countries_in_scope",
    });
  }

  // Preferred official /directory
  const preferredCountries = (countryFilter ? [countryFilter] : priorityCountries).filter((c) =>
    isDiscoveryAdapterReady(c, "Preferred")
  );
  const runPreferred =
    allowFamily(preferredParentScoped) &&
    (preferredBrands.length > 0 ||
      preferredParentScoped ||
      opts.forcePreferredDiscovery === true ||
      discoverAllOfficial) &&
    preferredCountries.length > 0;

  if (runPreferred) {
    try {
      const cache =
        opts.preferredCache ||
        (await ensurePreferredCalaDirectoryCache({
          countries: preferredCountries,
          country: countryFilter,
        }));
      sourceReport.families_used.push("Preferred");
      sourceReport.preferred_discovery = {
        source: PREFERRED_DISCOVERY_SOURCE,
        countries: preferredCountries,
        meta: cache._meta || null,
      };
      for (const row of iteratePreferredDirectoryRows(cache)) {
        const brandHit =
          brandMatchesControl(row.brand, preferredBrands) ||
          (preferredBrands.length === 1 && requireBrandMatch ? preferredBrands[0] : null);
        const scoped =
          brandHit ||
          (preferredParentScoped || discoverAllOfficial || !requireBrandMatch
            ? {
                brand_name: PREFERRED_DISCOVERY_SOURCE.brand_display,
                brand_slug: "preferred-hotels-and-resorts",
                parent_company: PREFERRED_DISCOVERY_SOURCE.parent_company,
              }
            : null);
        if (!scoped) continue;
        const disc = preferredRowToDiscovered(row, scoped);
        if (disc) discovered.push(disc);
      }
    } catch (err) {
      sourceReport.blocked_source_families.push("Preferred");
      sourceReport.adapter_errors.push({
        family: "Preferred",
        error: err?.message || String(err),
      });
    }
  } else if (preferredBrands.length || preferredParentScoped) {
    sourceReport.blocked_source_families.push("Preferred");
    sourceReport.adapter_errors.push({
      family: "Preferred",
      error: "no_ready_preferred_countries_in_scope",
    });
  }

  // VIC evidence / dedupe support (never sole insert source)
  let vicEvidence = [];
  if (opts.includeVicEvidence !== false) {
    const vic = opts.vicIndex || loadVicClaimIndex();
    for (const claim of vic.byId.values()) {
      const fam = claim.family;
      if (
        controlList.brands.some((b) => b.brand_family === fam) ||
        !opts.parentCompany
      ) {
        vicEvidence.push(vicClaimToEvidenceRow(claim, fam));
      }
    }
    sourceReport.vic_evidence_rows = vicEvidence.length;
    sourceReport.families_used.push("VIC_evidence");
  }

  const annotated = discovered.map((row) => annotateDiscoveredGovernance(row, opts));
  const governanceCounts = {};
  for (const row of annotated) {
    const key = row.brand_governance_status || "unknown";
    governanceCounts[key] = (governanceCounts[key] || 0) + 1;
  }

  sourceReport.cache_stats = getFamilyAdapterCacheStats();
  sourceReport.discovered_count = annotated.length;
  sourceReport.brand_governance_counts = governanceCounts;
  sourceReport.owner_facing_eligible_count = annotated.filter(
    (r) => r.owner_facing_eligible
  ).length;
  sourceReport.promotion_candidate_count = annotated.filter(
    (r) => r.brand_setup_promotion_candidate
  ).length;

  return {
    discovered: annotated,
    vicEvidence,
    sourceReport,
    controlList,
    regionPlan,
  };
}

/**
 * Classify all discovered rows against Census.
 */
export function classifyDiscoveredAgainstCensus(discovered = [], censusRecords = [], opts = {}) {
  const index = indexHotelPropertyCensus(censusRecords);
  const classified = [];
  const byClass = {
    [MATCH_CLASS.EXISTING_EXACT]: [],
    [MATCH_CLASS.EXISTING_PROBABLE]: [],
    [MATCH_CLASS.NEW_CANDIDATE]: [],
    [MATCH_CLASS.DUPLICATE_RISK]: [],
    [MATCH_CLASS.STEWARD]: [],
    [MATCH_CLASS.IDENTITY_CONFLICT]: [],
    [MATCH_CLASS.SOURCE_INSUFFICIENT]: [],
  };

  const seenIdentity = new Map();

  for (const d of discovered) {
    const match = matchDiscoveredProperty(d, index);
    let classification = match.classification;

    // Intra-batch duplicate identity keys
    if (d.identity_key && seenIdentity.has(norm(d.identity_key))) {
      classification = MATCH_CLASS.DUPLICATE_RISK;
      match.reason = "duplicate_within_discovery_batch";
      match.prior_discovery = seenIdentity.get(norm(d.identity_key));
    } else if (d.identity_key) {
      seenIdentity.set(norm(d.identity_key), d.property_name);
    }

    // No fuzzy auto-insert: probable never becomes new
    if (classification === MATCH_CLASS.NEW_CANDIDATE) {
      if (String(d.identity_confidence) !== "High") {
        classification = MATCH_CLASS.STEWARD;
        match.reason = "new_candidate_requires_high_identity";
      }
    }

    const row = {
      ...d,
      classification,
      match_method: match.match_method,
      census_record_id: match.census_record_id || null,
      census_identity_key: match.census_identity_key || null,
      match_reason: match.reason || null,
    };
    classified.push(row);
    if (!byClass[classification]) byClass[classification] = [];
    byClass[classification].push(row);
  }

  // VIC evidence used only to strengthen dedupe notes (optional)
  if (opts.vicEvidence?.length) {
    const vicUrls = new Set(
      opts.vicEvidence.map((v) => normUrl(v.official_property_url)).filter(Boolean)
    );
    for (const row of byClass[MATCH_CLASS.NEW_CANDIDATE] || []) {
      if (row.official_property_url && vicUrls.has(normUrl(row.official_property_url))) {
        row.vic_evidence_url_match = true;
      }
    }
  }

  return {
    index_size: index.count,
    classified,
    by_class: byClass,
    counts: Object.fromEntries(Object.entries(byClass).map(([k, v]) => [k, v.length])),
  };
}

/**
 * Build insert approval bundle (controlled — no Airtable writes).
 */
export function buildDiscoveryInsertApprovalBundle(ctx = {}) {
  const newCandidates = (ctx.new_property_candidates || []).filter(
    (c) => c.classification === MATCH_CLASS.NEW_CANDIDATE && c.identity_confidence === "High"
  );

  const proposed_inserts = [];
  for (const c of newCandidates) {
    const sanitized = buildInsertFieldsFromDiscovered(c, { human_review_required: false });
    if (!sanitized.fields["Property Name"] || !sanitized.fields["Property Identity Key"]) continue;
    // Only abort when a core identity/write field is forbidden — ignore optional drops
    const fatalForbidden = sanitized.dropped.filter(
      (d) =>
        d.reason === "forbidden_on_insert" &&
        ["Property Name", "Property Identity Key", "Current Brand", "Country"].includes(d.field)
    );
    if (fatalForbidden.length) continue;
    proposed_inserts.push({
      action: "insert",
      queue: SOURCE_DISCOVERY_QUEUE_ID,
      confidence: "High",
      identity_key: c.identity_key,
      property_name: c.property_name,
      brand: c.brand,
      source_family: c.source_family,
      official_property_id: c.official_property_id,
      fields: sanitized.fields,
      field_keys: Object.keys(sanitized.fields),
      dropped: sanitized.dropped,
      discovery: {
        official_property_url: c.official_property_url,
        official_directory_url: c.official_directory_url,
        city: c.city,
        country: c.country,
        match_classification: c.classification,
      },
    });
  }

  const sot = getProductionCensusSourceOfTruthSnapshot();
  const targetCheck = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: productionHotelPropertyCensus.tableId,
  });

  return {
    version: SOURCE_DISCOVERY_VERSION,
    type: "hotel_property_census_insert_approval_bundle",
    status: "awaiting_founder_approval",
    stop_before_writes: true,
    airtable_writes: false,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    vic_writes: false,
    run_id: ctx.run_id || null,
    mode: "controlled",
    scope: ctx.scope || "active-brand-setup",
    region: ctx.region || "CALA",
    strategy: ctx.strategy || "fastest-safe",
    queue: SOURCE_DISCOVERY_QUEUE_ID,
    production_target: sot.productionHotelPropertyCensus,
    write_target_ok: targetCheck.ok,
    write_target_check: targetCheck,
    records_proposed_for_insert: proposed_inserts.length,
    proposed_inserts,
    // Compatibility with multi-queue apply loaders (empty patches; inserts separate)
    proposed_writes: [],
    proposed_writes_by_queue: { [SOURCE_DISCOVERY_QUEUE_ID]: [] },
    duplicate_risks: ctx.duplicate_risks || [],
    steward_review_cases: ctx.steward_review_cases || [],
    existing_exact_matches: ctx.existing_exact_count ?? null,
    safety_status: {
      controlled_mode: true,
      production_writes: false,
      hotel_property_census_only: true,
      owner_operator_date_blocked: true,
      company_validated_blocked: true,
      brand_verified_blocked: true,
      recent_momentum_blocked: true,
      no_fuzzy_auto_insert: true,
      high_identity_required: true,
    },
    recommended_apply_command: ctx.run_id
      ? [
          "ALLOW_CENSUS_AUTOPILOT_APPLY=1",
          "CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1",
          "CONFIRM_NO_BRAND_EXPLORER_WRITES=1",
          "CONFIRM_NO_OWNER_OPERATOR_WRITES=1",
          "npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply",
          "--strategy fastest-safe --queue source_discovery --run-until-complete --batch-size 100",
          `--approval-bundle reports/research-engine-v2/autopilot/${ctx.run_id}/approval-bundle.json`,
          "--confirm-safe-writes --confirm-write-to-production-census",
          "--confirm-no-brand-explorer-writes --confirm-no-owner-operator",
          "--confirm-no-date-writes --confirm-no-recent-momentum",
          "--confirm-no-company-validation --confirm-webhound-not-production-source",
          "--enable-production-writes",
        ].join(" \\\n  ")
      : null,
  };
}

function toCsv(rows) {
  const headers = [
    "identity_key",
    "official_property_id",
    "property_name",
    "brand",
    "parent_company",
    "city",
    "state_region",
    "country",
    "address",
    "official_property_url",
    "source_family",
    "source_confidence",
    "identity_confidence",
    "classification",
    "match_method",
    "census_record_id",
  ];
  const esc = (v) => {
    const s = v == null ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [headers.join(","), ...rows.map((r) => headers.map((h) => esc(r[h])).join(","))].join("\n");
}

/**
 * Persist discovery run artifacts.
 */
export function writeDiscoveryRunArtifacts(runDir, report) {
  fs.mkdirSync(runDir, { recursive: true });
  writeJson(path.join(runDir, "source-of-truth-check.json"), report.source_of_truth_check);
  writeJson(path.join(runDir, "active-brand-discovery-control-list.json"), report.control_list);
  writeJson(path.join(runDir, "discovery-source-report.json"), report.source_report);
  writeText(path.join(runDir, "discovery-source-report.md"), report.source_report_md);
  writeText(path.join(runDir, "discovered-properties.csv"), report.discovered_csv);
  writeJson(path.join(runDir, "brand-to-census-match-report.json"), report.match_report);
  writeText(path.join(runDir, "brand-to-census-match-report.md"), report.match_report_md);
  writeJson(path.join(runDir, "new-property-candidates.json"), report.new_property_candidates);
  writeJson(path.join(runDir, "duplicate-risk.json"), report.duplicate_risks);
  writeJson(path.join(runDir, "steward-review-queue.json"), report.steward_review_cases);
  writeJson(path.join(runDir, "webhound-candidates.json"), report.webhound_candidates);
  writeJson(path.join(runDir, "approval-bundle.json"), report.approval_bundle);
  writeJson(path.join(runDir, "summary.json"), report.summary);
  writeText(path.join(runDir, "summary.md"), report.summary_md);
  writeJson(path.join(runDir, "region-config-plan.json"), report.region_plan);
  return runDir;
}

function renderSourceReportMd(sourceReport, regionPlan) {
  return [
    `# Discovery Source Report`,
    ``,
    `- **Families used:** ${(sourceReport.families_used || []).join(", ") || "(none)"}`,
    `- **Blocked source families:** ${(sourceReport.blocked_source_families || []).join(", ") || "(none)"}`,
    `- **Discovered (directory):** ${sourceReport.discovered_count ?? 0}`,
    `- **VIC evidence rows:** ${sourceReport.vic_evidence_rows ?? 0}`,
    ``,
    `## Region plan`,
    ``,
    `- Status: \`${regionPlan.status}\``,
    `- Ready countries: ${(regionPlan.ready_countries || []).map((c) => c.country).join(", ") || "(none)"}`,
    `- Needs adapter: ${(regionPlan.needs_adapter_plan || []).length}`,
    ``,
    regionPlan.operating_note,
    ``,
    `## Adapter errors`,
    ``,
    ...((sourceReport.adapter_errors || []).length
      ? sourceReport.adapter_errors.map((e) => `- **${e.family}:** ${e.error}${e.note ? ` — ${e.note}` : ""}`)
      : ["- (none)"]),
    ``,
  ].join("\n");
}

function renderMatchReportMd(match) {
  return [
    `# Brand Setup → Hotel Property Census Discovery Match`,
    ``,
    PRECISE_MATCH_SUMMARY_LINE,
    ``,
    `- **Census index size:** ${match.index_size}`,
    `- **Existing exact:** ${match.counts[MATCH_CLASS.EXISTING_EXACT] || 0}`,
    `- **Existing probable:** ${match.counts[MATCH_CLASS.EXISTING_PROBABLE] || 0}`,
    `- **New property candidates:** ${match.counts[MATCH_CLASS.NEW_CANDIDATE] || 0}`,
    `- **Duplicate risk:** ${match.counts[MATCH_CLASS.DUPLICATE_RISK] || 0}`,
    `- **Steward review:** ${match.counts[MATCH_CLASS.STEWARD] || 0}`,
    `- **Source insufficient:** ${match.counts[MATCH_CLASS.SOURCE_INSUFFICIENT] || 0}`,
    ``,
  ].join("\n");
}

function renderSummaryMd(summary) {
  return [
    `# Hotel Property Census — CALA Discovery Summary`,
    ``,
    summary.match_summary_line,
    ``,
    `- **Status:** \`${summary.status}\``,
    `- **Active brands searched:** ${summary.active_brands_searched}`,
    `- **Parent companies searched:** ${(summary.parent_companies_searched || []).join(", ") || "(n/a)"}`,
    `- **Countries covered (ready adapters):** ${(summary.countries_covered || []).join(", ") || "(none)"}`,
    `- **Discovered properties:** ${summary.discovered_properties}`,
    `- **Existing Hotel Property Census matches:** ${summary.existing_hotel_property_census_matches}`,
    `- **New property candidates:** ${summary.new_property_candidates}`,
    `- **Duplicate risks:** ${summary.duplicate_risks}`,
    `- **Steward review cases:** ${summary.steward_review_cases}`,
    `- **Source families used:** ${(summary.source_families_used || []).join(", ") || "(none)"}`,
    `- **Blocked source families:** ${(summary.blocked_source_families || []).join(", ") || "(none)"}`,
    `- **Marriott countries searched:** ${(summary.marriott_countries_searched || []).join(", ") || "(none)"}`,
    `- **Marriott HQV required for discovery:** false`,
    `- **Marriott MARSHA coverage:** ${summary.marriott_marsha_coverage?.with_marsha ?? 0}/${summary.marriott_marsha_coverage?.discovered ?? 0}`,
    `- **Estimated insert count if applied:** ${summary.estimated_insert_count}`,
    `- **Airtable writes:** false`,
    ``,
    `## Recommended apply command`,
    ``,
    "```bash",
    summary.recommended_apply_command || "(n/a — no High inserts)",
    "```",
    ``,
    `## Recommended next enrichment run after insert`,
    ``,
    "```bash",
    summary.recommended_next_enrichment_command,
    "```",
    ``,
  ].join("\n");
}

/**
 * Controlled source discovery run (no Airtable writes).
 */
export async function runSourceDiscoveryControlled(opts = {}) {
  const started = Date.now();
  const region = opts.region || "CALA";
  const runId =
    opts.runId ||
    `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-source-discovery`;
  const runDir =
    opts.runDir ||
    path.join(ROOT, "reports/research-engine-v2/autopilot", runId);

  const sotCheck = {
    ...getProductionCensusSourceOfTruthSnapshot(),
    assert: assertProductionCensusWriteTarget({
      baseName: productionHotelPropertyCensus.baseName,
      tableName: productionHotelPropertyCensus.tableName,
      tableId: productionHotelPropertyCensus.tableId,
    }),
    legacy_census_write_blocked: true,
    vic_write_blocked: true,
    brand_setup_write_blocked: true,
    brand_explorer_write_blocked: true,
  };
  if (!sotCheck.assert.ok) {
    return {
      ok: false,
      status: DISCOVERY_STATUS.BLOCKED,
      error: BLOCKED_WRONG_CENSUS_TARGET,
      source_of_truth_check: sotCheck,
      airtable_writes: false,
    };
  }

  const controlList =
    opts.controlList ||
    (opts.discoverAllOfficialParents === false
      ? buildActiveBrandDiscoveryControlList({
          region,
          parentCompany: opts.parentCompany || null,
          brands: opts.brands,
          skipUniverseLoad: opts.skipUniverseLoad,
        })
      : buildOfficialInventoryDiscoveryControlList({
          region,
          parentCompany: opts.parentCompany || null,
          brands: opts.brands,
          skipUniverseLoad: opts.skipUniverseLoad,
        }));
  const regionPlan = buildCalaDiscoveryRegionPlan({
    region,
    country: opts.country || null,
  });

  const { discovered, vicEvidence, sourceReport } = await discoverCalaProperties({
    ...opts,
    controlList,
    regionPlan,
    discoverAllOfficialParents: opts.discoverAllOfficialParents !== false,
  });

  const censusRecords = opts.censusRecords || [];
  const match = classifyDiscoveredAgainstCensus(discovered, censusRecords, { vicEvidence });

  const newCandidates = match.by_class[MATCH_CLASS.NEW_CANDIDATE] || [];
  const duplicateRisks = match.by_class[MATCH_CLASS.DUPLICATE_RISK] || [];
  const steward = [
    ...(match.by_class[MATCH_CLASS.STEWARD] || []),
    ...(match.by_class[MATCH_CLASS.EXISTING_PROBABLE] || []).map((r) => ({
      ...r,
      steward_reason: "existing_probable_match_no_auto_insert",
    })),
  ];

  const approvalBundle = buildDiscoveryInsertApprovalBundle({
    run_id: runId,
    region,
    scope: opts.scope || "official-parent-inventory",
    strategy: opts.strategy || "fastest-safe",
    new_property_candidates: newCandidates,
    duplicate_risks: duplicateRisks,
    steward_review_cases: steward,
    existing_exact_count: match.counts[MATCH_CLASS.EXISTING_EXACT] || 0,
  });

  const status =
    regionPlan.needs_adapter_plan?.length > 0 ||
    (sourceReport.blocked_source_families || []).length > 0
      ? DISCOVERY_STATUS.READY_NEEDS_ADAPTER
      : DISCOVERY_STATUS.READY;

  const summary = {
    status,
    match_summary_line: PRECISE_MATCH_SUMMARY_LINE,
    active_brands_searched: controlList.active_brands_in_scope,
    parent_companies_searched: controlList.parent_companies_in_scope,
    countries_covered: (regionPlan.ready_countries || []).map((c) => c.country),
    countries_needing_adapters: (regionPlan.needs_adapter_plan || []).map((c) => c.country),
    marriott_countries_searched: sourceReport.marriott_discovery?.countries || [],
    marriott_hqv_required_for_discovery: false,
    marriott_marsha_coverage: (() => {
      const rows = (match.classified || []).filter((r) => r.source_family === "Marriott");
      const withMarsha = rows.filter((r) => r.official_property_id).length;
      const withUrl = rows.filter((r) => r.official_property_url).length;
      return {
        discovered: rows.length,
        with_marsha: withMarsha,
        with_official_url: withUrl,
        marsha_pct: rows.length ? Math.round((withMarsha / rows.length) * 100) : null,
        url_pct: rows.length ? Math.round((withUrl / rows.length) * 100) : null,
      };
    })(),
    discovered_properties: discovered.length,
    existing_hotel_property_census_matches: match.counts[MATCH_CLASS.EXISTING_EXACT] || 0,
    new_property_candidates: newCandidates.length,
    duplicate_risks: duplicateRisks.length,
    steward_review_cases: steward.length,
    source_families_used: sourceReport.families_used,
    blocked_source_families: [...new Set(sourceReport.blocked_source_families || [])],
    estimated_insert_count: approvalBundle.records_proposed_for_insert,
    recommended_apply_command: approvalBundle.recommended_apply_command,
    recommended_next_enrichment_command: [
      "npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled",
      "--strategy fastest-safe --run-until-complete --batch-size 250",
    ].join(" \\\n  "),
    runtime_ms: Date.now() - started,
    airtable_writes: false,
    production_target: productionHotelPropertyCensus,
  };

  const report = {
    ok: true,
    status,
    run_id: runId,
    run_dir: runDir,
    airtable_writes: false,
    source_of_truth_check: sotCheck,
    control_list: controlList,
    region_plan: regionPlan,
    source_report: sourceReport,
    source_report_md: renderSourceReportMd(sourceReport, regionPlan),
    discovered_csv: toCsv(match.classified),
    match_report: {
      version: SOURCE_DISCOVERY_VERSION,
      production_target: productionHotelPropertyCensus,
      match_summary_line: PRECISE_MATCH_SUMMARY_LINE,
      ...match.counts,
      index_size: match.index_size,
      counts: match.counts,
    },
    match_report_md: renderMatchReportMd(match),
    new_property_candidates: newCandidates,
    duplicate_risks: duplicateRisks,
    steward_review_cases: steward,
    webhound_candidates: { candidates: [], capped_at: 25, note: "Webhound not used for production discovery" },
    approval_bundle: approvalBundle,
    summary,
    summary_md: renderSummaryMd(summary),
    classified: match.classified,
  };

  if (opts.writeArtifacts !== false) {
    writeDiscoveryRunArtifacts(runDir, report);
  }

  return report;
}
