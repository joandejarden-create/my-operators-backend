/**
 * Packet 2.8B — Hotel Explorer Assembler (generic).
 * No hotel-name / hotel-ID switch statements.
 */

import { createDefaultResolvers } from "./resolvers/create-default-resolvers.js";

export const HOTEL_EXPLORER_ASSEMBLER_VERSION = "hotel-explorer-assembler-v2";

/**
 * Shared view-model shape — every hotel produces the same schema.
 */
export function createEmptyHotelExplorerViewModel(hotelId) {
  return {
    assembler_version: HOTEL_EXPLORER_ASSEMBLER_VERSION,
    hotel_id: hotelId || null,
    identity: null,
    hero: null,
    hotelOverview: null,
    ownership: null,
    organizationPortfolio: null,
    relationships: null,
    submarket: null,
    areaHotels: null,
    demandDrivers: null,
    accessConnectivity: null,
    brandOperator: null,
    people: null,
    sourcesEvidence: null,
    research: null,
    completeness: null,
    provenance: {
      note: "Full Investigation is a presentation artifact — Explorer consumes structured intelligence.",
      auto_promote: false,
      property_specific_presentation: false,
    },
  };
}

/**
 * Assemble one HotelExplorerViewModel via shared domain resolvers.
 * Inject resolvers for tests; default = createDefaultResolvers() (no hotel switches).
 */
export function assembleHotelExplorerViewModel({ hotel_id, resolvers = null } = {}) {
  const id = String(hotel_id || "").trim();
  const vm = createEmptyHotelExplorerViewModel(id);
  const r = resolvers || createDefaultResolvers();

  const map = {
    identity: r.resolveIdentity,
    hotelOverview: r.resolvePropertyFundamentals,
    ownership: r.resolveOwnership,
    organizationPortfolio: r.resolveOrganizationPortfolio,
    relationships: r.resolveRelationships,
    people: r.resolvePeople,
    submarket: r.resolveMarket,
    areaHotels: r.resolveAreaHotels,
    demandDrivers: r.resolveDemandDrivers,
    accessConnectivity: r.resolveAccessConnectivity,
    brandOperator: r.resolveBrandOperator,
    sourcesEvidence: r.resolveSourcesEvidence,
    research: r.resolveResearchArchive,
    completeness: r.resolveCompleteness,
  };

  for (const [key, fn] of Object.entries(map)) {
    if (typeof fn !== "function") continue;
    try {
      vm[key] = fn.call(r, { hotel_id: id });
    } catch (err) {
      vm[key] = {
        status: "error",
        error: err?.message || String(err),
        data: null,
        provenance: {},
        gaps: ["resolver_error"],
        conflicts: [],
      };
    }
  }

  // Hero is derived from identity — same for all hotels
  const ident = vm.identity?.data || {};
  vm.hero = {
    status: ident.name ? "STRONG" : "MISSING",
    data: {
      title: ident.name || null,
      location_line: [ident.city, ident.country].filter(Boolean).join(", ") || null,
    },
    provenance: { derived_from: "identity" },
    gaps: ident.name ? [] : ["hero_title"],
    conflicts: [],
  };

  vm.provenance = {
    ...vm.provenance,
    assembled_at: new Date().toISOString(),
    ownership_provider_id: vm.ownership?.provenance?.provider_id || null,
    property_specific_presentation: false,
  };

  return vm;
}

/**
 * Assert assembler source has no hotel-ID / hotel-name switch anti-patterns.
 * Used by release gates (static check on this module + resolvers).
 */
export function assemblerForbiddenPatterns() {
  return [
    "recUNycnMwOVFX0hc",
    "recIwaP1etgx2g9nA",
    "recsYJb2R1jarPpK3",
    "recTYaiA4S6fR6ixx",
    "is this KGPV",
    "promptForKGPV",
  ];
}
