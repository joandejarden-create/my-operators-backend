/**
 * CALA region configuration for Autopilot source discovery.
 * Does not invent country coverage — incomplete adapters are explicit.
 */

import { COUNTRY_CONFIGS } from "../radar-buildout/country-configs.js";
import {
  classifyMarriottCountryDiscoveryReadiness,
  MARRIOTT_CALA_PRIORITY_COUNTRIES,
} from "./census-autopilot-marriott-discovery-adapter.js";
import { classifyHiltonCountryDiscoveryReadiness } from "./census-autopilot-hilton-cala-discovery-adapter.js";
import { classifyChoiceCountryDiscoveryReadiness } from "./census-autopilot-choice-cala-discovery-adapter.js";
import { classifyIhgCountryDiscoveryReadiness } from "./census-autopilot-ihg-cala-discovery-adapter.js";
import { classifyAccorCountryDiscoveryReadiness } from "./census-autopilot-accor-cala-discovery-adapter.js";
import { classifyWyndhamCountryDiscoveryReadiness } from "./census-autopilot-wyndham-cala-discovery-adapter.js";
import { classifyPreferredCountryDiscoveryReadiness } from "./census-autopilot-preferred-directory-discovery-adapter.js";
import { CALA_DISCOVERY_PRIORITY_COUNTRIES } from "./census-autopilot-cala-discovery-shared.js";

export const CALA_REGION_CONFIG_VERSION = "production-census-cala-region-config-v4";

/** Countries registered in Dealality CALA radar configs. */
export function listCalaCountriesFromRadar() {
  return Object.keys(COUNTRY_CONFIGS || {}).sort();
}

function toCoverageEntry(r) {
  return {
    ready: r.ready,
    readiness: r.readiness,
    adapter: r.adapter,
    note: r.note,
    source_url: r.sitemap_url || r.locations_url || r.regional_url || r.destination_url || null,
    hqv_required_for_discovery: false,
  };
}

function baseFamilyCoverage(country) {
  return {
    Hilton: toCoverageEntry(classifyHiltonCountryDiscoveryReadiness(country)),
    Choice: toCoverageEntry(classifyChoiceCountryDiscoveryReadiness(country)),
    Marriott: (() => {
      const r = classifyMarriottCountryDiscoveryReadiness(country);
      return {
        ...toCoverageEntry(r),
        sitemap_url: r.sitemap_url || null,
      };
    })(),
    IHG: toCoverageEntry(classifyIhgCountryDiscoveryReadiness(country)),
    Accor: toCoverageEntry(classifyAccorCountryDiscoveryReadiness(country)),
    Wyndham: toCoverageEntry(classifyWyndhamCountryDiscoveryReadiness(country)),
    Preferred: toCoverageEntry(classifyPreferredCountryDiscoveryReadiness(country)),
    VIC: {
      ready: true,
      role: "evidence_dedupe_only",
      note: "VIC source claims — read-only evidence / dedupe support",
    },
  };
}

/**
 * Per-country discovery adapter readiness (multi-parent CALA sprint).
 */
function buildCalaDiscoveryAdapterCoverage() {
  /** @type {Record<string, object>} */
  const out = {};
  const countries = new Set([
    ...listCalaCountriesFromRadar(),
    ...CALA_DISCOVERY_PRIORITY_COUNTRIES,
    ...MARRIOTT_CALA_PRIORITY_COUNTRIES,
  ]);
  for (const country of [...countries].sort()) {
    out[country] = Object.freeze(baseFamilyCoverage(country));
  }
  return Object.freeze(out);
}

export const CALA_DISCOVERY_ADAPTER_COVERAGE = buildCalaDiscoveryAdapterCoverage();

/**
 * Build parent × country adapter matrix for reporting.
 */
export function buildCalaParentCountryAdapterMatrix(opts = {}) {
  const countries = opts.countries?.length
    ? opts.countries
    : CALA_DISCOVERY_PRIORITY_COUNTRIES;
  const parents = ["Marriott", "IHG", "Hilton", "Choice", "Accor", "Wyndham", "Preferred"];
  const rows = [];
  for (const parent of parents) {
    for (const country of countries) {
      const cov = CALA_DISCOVERY_ADAPTER_COVERAGE[country]?.[parent] || {
        ready: false,
        readiness: "needs_adapter",
        note: "No coverage entry",
      };
      rows.push({
        parent_company: parent,
        country,
        readiness: cov.readiness || (cov.ready ? "supported" : "needs_adapter"),
        ready: Boolean(cov.ready),
        adapter: cov.adapter || null,
        source_url_pattern: cov.source_url || cov.sitemap_url || null,
        note: cov.note || "",
        hqv_required_for_discovery: false,
        next_action: cov.ready
          ? "run_controlled_source_discovery"
          : "implement_or_probe_official_directory",
      });
    }
  }
  return {
    version: CALA_REGION_CONFIG_VERSION,
    priority_countries: [...CALA_DISCOVERY_PRIORITY_COUNTRIES],
    parents,
    rows,
    summary: {
      supported: rows.filter((r) => r.readiness === "supported").length,
      needs_adapter: rows.filter((r) => r.readiness === "needs_adapter").length,
      blocked: rows.filter((r) => r.readiness === "blocked").length,
    },
  };
}

/**
 * Build region plan for Autopilot discovery.
 * @param {{ region?: string, country?: string|null }} [opts]
 */
export function buildCalaDiscoveryRegionPlan(opts = {}) {
  const region = opts.region || "CALA";
  const countryFilter = opts.country ? String(opts.country).trim() : null;
  const radarCountries = listCalaCountriesFromRadar();
  const countries = countryFilter
    ? radarCountries.filter((c) => c.toLowerCase() === countryFilter.toLowerCase())
    : radarCountries;

  const readyCountries = [];
  const needsAdapterPlan = [];

  for (const country of countries.length ? countries : radarCountries) {
    const coverage = CALA_DISCOVERY_ADAPTER_COVERAGE[country];
    if (!coverage) {
      needsAdapterPlan.push({
        country,
        status: "needs_directory_adapter",
        families: ["Hilton", "Choice", "Marriott", "IHG", "Accor", "Wyndham", "Preferred"],
        note: "No CALA discovery directory adapter registered for this country yet",
      });
      continue;
    }
    const readyFamilies = Object.entries(coverage)
      .filter(([, v]) => v.ready === true && v.role !== "evidence_dedupe_only")
      .map(([k]) => k);
    const pendingFamilies = Object.entries(coverage)
      .filter(([, v]) => v.ready === false)
      .map(([k, v]) => ({
        family: k,
        readiness: v.readiness || "needs_adapter",
        note: v.note,
      }));

    if (readyFamilies.length) {
      readyCountries.push({
        country,
        ready_families: readyFamilies,
        pending_families: pendingFamilies,
        vic_evidence: Boolean(coverage.VIC?.ready),
      });
    }
    if (pendingFamilies.length || !readyFamilies.length) {
      needsAdapterPlan.push({
        country,
        status: readyFamilies.length ? "partial_adapters" : "needs_directory_adapter",
        ready_families: readyFamilies,
        pending_families: pendingFamilies,
      });
    }
  }

  return {
    version: CALA_REGION_CONFIG_VERSION,
    region,
    country_filter: countryFilter,
    radar_countries: radarCountries,
    countries_in_scope: countries.length ? countries : radarCountries,
    ready_countries: readyCountries,
    needs_adapter_plan: needsAdapterPlan,
    priority_countries: [...CALA_DISCOVERY_PRIORITY_COUNTRIES],
    parent_country_matrix: buildCalaParentCountryAdapterMatrix({
      countries: countryFilter
        ? [countryFilter]
        : CALA_DISCOVERY_PRIORITY_COUNTRIES,
    }),
    operating_note:
      "Autopilot discovery runs ready adapters for Marriott/IHG/Hilton/Choice/Accor/Wyndham/Preferred on priority CALA countries. HQV is enrichment-only. VIC is evidence only.",
    status:
      readyCountries.length > 0
        ? "cala_discovery_region_partial_ready"
        : "cala_discovery_region_blocked_no_adapters",
  };
}

/**
 * Whether a family/country pair can run listing discovery.
 * @param {string} country
 * @param {string} family
 */
export function isDiscoveryAdapterReady(country, family) {
  const cov = CALA_DISCOVERY_ADAPTER_COVERAGE[country]?.[family];
  return Boolean(cov?.ready && cov.role !== "evidence_dedupe_only");
}

/**
 * List countries where a family adapter is ready.
 * @param {string} family
 */
export function listCountriesWithDiscoveryAdapter(family) {
  return Object.entries(CALA_DISCOVERY_ADAPTER_COVERAGE)
    .filter(([, cov]) => cov?.[family]?.ready && cov[family].role !== "evidence_dedupe_only")
    .map(([country]) => country)
    .sort();
}
