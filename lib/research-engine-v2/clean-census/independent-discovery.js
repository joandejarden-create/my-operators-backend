/**
 * Independent census discovery — official directories only.
 * NEVER accepts legacy census hotels as seed.
 */

import { loadIhgDirectoryRows } from "../adapters/ihg.js";
import { canonicalizeObservedBrand, defaultParentForFamily } from "../brand-family.js";
import { RESEARCH_MODES_CLEAN } from "./provenance.js";

/**
 * Discover Hotel Indigo + Kimpton Mexico from official IHG directory extract.
 * @param {object} firewall
 * @param {{ directoryPath?: string }} [opts]
 */
export function discoverIhgIndigoKimptonMexico(firewall, opts = {}) {
  firewall.assertNoLegacyInContext(opts);

  const rows = loadIhgDirectoryRows(opts.directoryPath);
  const discoveredAt = new Date().toISOString();

  const mexico = rows.filter(
    (r) =>
      /Mexico/i.test(String(r.country || "")) ||
      String(r.countryCode || "").toUpperCase() === "MX" ||
      /Mexico/i.test(String(r.addressText || ""))
  );

  const cohort = mexico.filter((r) => {
    const blob = `${r.brand || ""} ${r.propertyUrl || ""} ${r.name || ""} ${r.inferredHotelName || ""}`.toLowerCase();
    return /hotelindigo|indigo/.test(blob) || /kimpton/.test(blob);
  });

  const discoveries = cohort.map((row, idx) => {
    const brandRaw = String(row.brand || "");
    const affiliation = /kimpton/i.test(brandRaw)
      ? "Kimpton"
      : /indigo/i.test(brandRaw)
        ? "Hotel Indigo"
        : canonicalizeObservedBrand(brandRaw) || brandRaw;
    return {
      independent_record_id: `ind_ihg_mx_${String(row.mnemonic || row.propertyId || idx).toLowerCase()}`,
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

  return {
    research_mode: RESEARCH_MODES_CLEAN.CLEAN_CENSUS_RECONSTRUCTION,
    cohort: "hotel_indigo_kimpton_mexico",
    discovery_basis: "official_ihg_directory_mexico_filter",
    discovery_sources: [
      {
        name: "IHG destination directory — Mexico",
        type: "Official Parent Company Directory",
        url: "https://www.ihg.com/mexico",
        extract: opts.directoryPath || "reports/ihg-cala-directory-extract.json",
        note: "Property universe filtered by brand tokens hotelindigo|kimpton within Mexico rows only — no legacy census seed",
      },
    ],
    discoveredAt,
    mexicoDirectoryRowCount: mexico.length,
    discoveries,
    note: "legacy_used_as_source=false for all discoveries",
  };
}
