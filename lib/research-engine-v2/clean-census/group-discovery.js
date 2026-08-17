/**
 * Independent group discovery adapters for reconstruction waves.
 * Official directories only — never legacy census.
 */

import { loadIhgDirectoryRows } from "../adapters/ihg.js";
import { loadMarriottSoftBrandDirectoryRows } from "../adapters/marriott.js";
import { loadChoiceSitemapDirectoryRows } from "../../hotel-census/plan-choice-census-sitemap-match.js";
import { canonicalizeObservedBrand, defaultParentForFamily } from "../brand-family.js";
import { RESEARCH_MODES_CLEAN } from "./provenance.js";
import { existsSync } from "node:fs";

/**
 * Map IHG directory brand tokens → census Affiliation labels.
 * @param {string} brandRaw
 */
export function mapIhgDirectoryBrand(brandRaw) {
  const b = String(brandRaw || "").toLowerCase();
  if (/kimpton/.test(b)) return "Kimpton";
  if (/hotelindigo|indigo/.test(b)) return "Hotel Indigo";
  if (/holidayinnexpress/.test(b)) return "Holiday Inn Express";
  if (/holidayinnclub/.test(b)) return "Holiday Inn Club Vacations";
  if (/holidayinnresort/.test(b)) return "Holiday Inn Resort";
  if (/holidayinn/.test(b)) return "Holiday Inn";
  if (/crowneplaza|crowne/.test(b)) return "Crowne Plaza";
  if (/intercontinental/.test(b)) return "InterContinental";
  if (/staybridge/.test(b)) return "Staybridge Suites";
  if (/candlewood/.test(b)) return "Candlewood Suites";
  if (/avid/.test(b)) return "avid hotels";
  if (/^voco|voco-/.test(b)) return "voco";
  if (/garner/.test(b)) return "Garner";
  if (/joia.?iberostar|iberostar.?joia/.test(b)) return "JOIA Iberostar";
  if (/iberostar/.test(b)) return "Iberostar";
  if (/spnd/.test(b)) return "IHG Partner / Spnd";
  return canonicalizeObservedBrand(brandRaw) || brandRaw || "IHG Brand";
}

function mexicoFilter(r) {
  return (
    /Mexico/i.test(String(r.country || "")) ||
    String(r.countryCode || "").toUpperCase() === "MX" ||
    /Mexico/i.test(String(r.addressText || ""))
  );
}

/**
 * Discover ALL IHG Mexico properties from official directory.
 * @param {object} firewall
 * @param {{ directoryPath?: string, brands?: string[]|null }} [opts]
 */
export function discoverIhgMexicoAll(firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);
  const rows = loadIhgDirectoryRows(opts.directoryPath);
  const discoveredAt = new Date().toISOString();
  let mexico = rows.filter(mexicoFilter);

  if (Array.isArray(opts.brands) && opts.brands.length) {
    const want = opts.brands.map((b) => String(b).toLowerCase());
    mexico = mexico.filter((r) => {
      const mapped = mapIhgDirectoryBrand(r.brand).toLowerCase();
      const blob = `${r.brand || ""} ${mapped}`.toLowerCase();
      return want.some((w) => blob.includes(w) || mapped.includes(w));
    });
  }

  const discoveries = mexico.map((row, idx) => {
    const affiliation = mapIhgDirectoryBrand(row.brand);
    const mnemonic = String(row.mnemonic || row.propertyId || idx).toUpperCase();
    return {
      independent_record_id: `ind_ihg_mx_${mnemonic.toLowerCase()}`,
      discovery_source: row.sourceUrl || "https://www.ihg.com/mexico",
      discovery_source_type: "Official Parent Company Directory",
      discovery_adapter: "ihg_destination_directory",
      first_independently_discovered_at: discoveredAt,
      research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
      legacy_used_as_source: false,
      directory_row: {
        propertyId: row.propertyId || row.mnemonic || null,
        mnemonic: row.mnemonic || row.propertyId || null,
        name: row.name || row.inferredHotelName || null,
        brand: affiliation,
        brandToken: row.brand || null,
        parent: defaultParentForFamily("ihg"),
        city: row.citySlug ? String(row.citySlug).replace(/-/g, " ") : row.city || null,
        cityRaw: row.city || null,
        country: row.country || "Mexico",
        countryCode: row.countryCode || "MX",
        addressText: row.addressText || null,
        propertyUrl: row.propertyUrl || row.website || null,
        source: row.source || "ihg_destination_directory",
      },
    };
  });

  const byBrand = {};
  for (const d of discoveries) {
    byBrand[d.directory_row.brand] = (byBrand[d.directory_row.brand] || 0) + 1;
  }

  return {
    research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    cohort: "ihg_mexico_all_brands",
    discovery_basis: "official_ihg_directory_mexico_all_brands",
    discovery_sources: [
      {
        name: "IHG destination directory — Mexico",
        type: "Official Parent Company Directory",
        url: "https://www.ihg.com/mexico",
        extract: opts.directoryPath || "reports/ihg-cala-directory-extract.json",
        note: "Full Mexico propertyRows — no legacy census seed",
      },
    ],
    discoveredAt,
    mexicoDirectoryRowCount: discoveries.length,
    brandBreakdown: byBrand,
    discoveries,
    legacy_used_as_source: false,
  };
}

/**
 * Adapter inventory for reconstruction waves.
 */
export function getGroupAdapterInventory() {
  const choicePath = "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";
  return [
    {
      group: "IHG",
      adapter: "ihg_destination_directory",
      discovery: "scalable",
      status: "production_ready_for_reconstruction",
      extract: "reports/ihg-cala-directory-extract.json",
      supports: ["name", "brand", "city", "country", "url", "propertyId", "parent", "status_via_hoteldetail"],
      notes: "Pilot + VIC wave 1 benchmark",
    },
    {
      group: "Marriott",
      adapter: "marriott_mexico_country_hotel_sitemap",
      discovery: "scalable",
      status: "production_ready_for_reconstruction",
      extract: "live https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap",
      supports: ["name", "brand_from_title_url", "country", "url", "marsha", "status_listed"],
      notes: "Wave 1D — overview pages often Akamai-blocked; sitemap is primary. Property Identity V1 + Temporal Affiliation V1",
    },
    {
      group: "Hilton",
      adapter: "hilton_locations_mexico + hilton_graphql_ctyhocn",
      discovery: "scalable",
      status: "production_ready_for_reconstruction",
      extract: "live https://www.hilton.com/en/locations/mexico/{brand-slug}/",
      supports: [
        "name",
        "brand",
        "city",
        "country",
        "url",
        "ctyhocn",
        "status",
        "openDate",
        "lat",
        "lng",
        "amenityIds",
        "address",
        "phone",
      ],
      notes: "Wave 1B — Mexico brand pages; GraphQL corroborates status/openDate",
    },
    {
      group: "Choice",
      adapter: "choice_mexico_regional_jsonld + mx_sitemap_union",
      discovery: "scalable",
      status: "production_ready_for_reconstruction",
      extract: "live Choice Mexico regional + reports/independent-census-choice-property-url-extract-cala-2026-05-20.json",
      supports: [
        "name",
        "brand",
        "city",
        "country",
        "url",
        "propertyId",
        "address",
        "lat",
        "lng",
        "amenityGroups",
        "status_listed",
      ],
      notes: "Wave 1C — 403 property pages remain Blocked ≠ reflag; Property Identity V1 + Temporal Affiliation V1",
    },
    {
      group: "Hyatt",
      adapter: "hyatt_directory_match",
      discovery: "planned",
      status: "enrichment_scripts_exist",
      notes: "Reuse plan-hyatt-census-enrichment patterns for discovery extract",
    },
    {
      group: "Accor",
      adapter: "accor_directory",
      discovery: "planned",
      status: "partial_lib_exists",
      notes: "accor-directory-name-normalize.js present",
    },
    {
      group: "Wyndham",
      adapter: "wyndham_directory",
      discovery: "planned",
      status: "not_wired_to_re_v2",
    },
    {
      group: "Minor Hotels",
      adapter: "minor_avani_etc",
      discovery: "planned",
      status: "brand_pages_bot_blocked_often",
      notes: "Census Open hotels can corroborate existence post-rules; homepage often 403",
    },
    {
      group: "Radisson / Choice regional",
      adapter: "choice_radisson_individuals",
      discovery: "partial",
      status: "individuals_gap_engine_exists",
    },
  ];
}

// Keep Indigo/Kimpton helper as thin wrapper
export { discoverIhgIndigoKimptonMexico } from "./independent-discovery.js";
