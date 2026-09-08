/**
 * Packet 2.8B — shared domain resolvers for HotelExplorerAssembler.
 * No hotel-name / hotel-ID switch statements in this file.
 */

import { buildHotelOwnershipReportViaProviders } from "../providers/ownership-provider-registry.js";
import { getHotelSeed } from "../stores/pilot-hotel-seed-store.js";
import { evaluateHotelIntelligenceCompleteness } from "../completeness-evaluator.js";

function domainResult({ data, status, provenance, gaps = [], conflicts = [] }) {
  return { data, status, provenance, gaps, conflicts };
}

function ownershipConfidence(report) {
  const oac = report?.report?.ownership_and_control || {};
  const ownerKnown = oac.economic_owner_or_group?.known === true;
  const bucket = String(report?.report?.executive_summary?.verification_bucket || "").toUpperCase();
  if (ownerKnown && (bucket === "HIGH" || bucket === "VERIFIED")) return "HIGH";
  if (ownerKnown) return "PROBABLE";
  return "UNKNOWN";
}

export function createDefaultResolvers(opts = {}) {
  const cache = new Map();

  function ownershipPayload(hotel_id) {
    if (cache.has(hotel_id)) return cache.get(hotel_id);
    const payload = buildHotelOwnershipReportViaProviders(hotel_id);
    cache.set(hotel_id, payload);
    return payload;
  }

  return {
    resolveIdentity({ hotel_id }) {
      const seed = getHotelSeed(hotel_id);
      const own = ownershipPayload(hotel_id);
      const hotel = own.hotel || {};
      const name = hotel.name || hotel.display_name || seed.name || null;
      const status = name ? "STRONG" : "MISSING";
      return domainResult({
        data: {
          hotel_id,
          name,
          country: hotel.country || seed.country || null,
          city: hotel.city || hotel.market || seed.city || null,
          address: hotel.address || null,
          aliases: hotel.former_names || [],
        },
        status,
        provenance: {
          sources: [own.provider_id, seed.source].filter(Boolean),
          provider_id: own.provider_id,
        },
        gaps: name ? [] : ["canonical_display_name"],
      });
    },

    resolvePropertyFundamentals({ hotel_id }) {
      const seed = getHotelSeed(hotel_id);
      const own = ownershipPayload(hotel_id);
      const hotel = own.hotel || {};
      const deep = own.deep_research?.property_profile || {};
      const rooms = hotel.rooms ?? deep.rooms ?? seed.rooms ?? null;
      const chain = hotel.chain_scale || deep.chain_scale || seed.chain_scale || null;
      const gaps = [];
      if (rooms == null) gaps.push("rooms");
      if (!chain) gaps.push("chain_scale");
      let status = "MISSING";
      if (rooms != null && chain) status = "STRONG";
      else if (rooms != null || chain || hotel.address) status = "PARTIAL";
      return domainResult({
        data: {
          rooms,
          chain_scale: chain,
          status: hotel.hotel_status || hotel.status || null,
          website: hotel.website || null,
          address: hotel.address || deep.address || null,
        },
        status,
        provenance: { sources: ["ownership_payload", "pilot_seed"], rooms_source: rooms != null ? "structured" : null },
        gaps,
      });
    },

    resolveOwnership({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const oac = own.report?.ownership_and_control || {};
      const conf = ownershipConfidence(own);
      const known = oac.economic_owner_or_group?.known === true;
      let status = "MISSING";
      if (known && conf === "HIGH") status = "STRONG";
      else if (known) status = "PARTIAL";
      else if (own.in_cohort) status = "PARTIAL";
      return domainResult({
        data: {
          provider_id: own.provider_id,
          in_cohort: own.in_cohort,
          intelligence_case: own.intelligence_case || own.report?.case || null,
          economic_owner: oac.economic_owner_or_group || null,
          propco: oac.legal_property_owner_propco || null,
          executive_summary: own.report?.executive_summary || null,
          confidence: conf,
        },
        status,
        provenance: { provider_id: own.provider_id, legacy: own.provider_legacy },
        gaps: known ? [] : ["economic_owner"],
      });
    },

    resolveOrganizationPortfolio({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const org = own.organization || own.ownership_group || null;
      const status = org ? "PARTIAL" : "MISSING";
      return domainResult({
        data: org,
        status,
        provenance: { provider_id: own.provider_id },
        gaps: org ? [] : ["organization_profile"],
      });
    },

    resolveRelationships({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const rels = own.deep_research?.relationships || own.report?.relationships || [];
      const status = Array.isArray(rels) && rels.length ? "STRONG" : "MISSING";
      return domainResult({
        data: { relationships: rels },
        status,
        provenance: { provider_id: own.provider_id },
        gaps: status === "MISSING" ? ["relationships"] : [],
      });
    },

    resolvePeople({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const people =
        own.report?.decision_authority?.people ||
        own.deep_research?.people ||
        [];
      const status = people.length ? "STRONG" : "MISSING";
      return domainResult({
        data: { people },
        status,
        provenance: { provider_id: own.provider_id, count: people.length },
        gaps: people.length ? [] : ["people"],
      });
    },

    resolveMarket({ hotel_id }) {
      const seed = getHotelSeed(hotel_id);
      const own = ownershipPayload(hotel_id);
      const market =
        own.deep_research?.market_intelligence?.submarket ||
        own.hotel?.market ||
        seed.city ||
        seed.country ||
        null;
      const status = market ? "PARTIAL" : "MISSING";
      return domainResult({
        data: { market, country: seed.country || own.hotel?.country || null },
        status,
        provenance: { sources: ["seed", "deep_research"] },
        gaps: market ? [] : ["market"],
      });
    },

    resolveAreaHotels({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const area = own.deep_research?.area_hotels || own.deep_research?.market_intelligence?.competitors || [];
      const status = Array.isArray(area) && area.length ? "PARTIAL" : "MISSING";
      return domainResult({
        data: { area_hotels: area },
        status,
        provenance: { note: "Census nearby engine preferred for scale; deep research when present" },
        gaps: status === "MISSING" ? ["area_hotels"] : [],
      });
    },

    resolveDemandDrivers({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const demand = own.deep_research?.market_intelligence?.demand_drivers || null;
      const status = demand ? "PARTIAL" : "MISSING";
      return domainResult({
        data: { demand_drivers: demand },
        status,
        provenance: {},
        gaps: demand ? [] : ["demand_drivers"],
      });
    },

    resolveAccessConnectivity({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const access = own.deep_research?.market_intelligence?.access || own.hotel?.address || null;
      const status = access ? "PARTIAL" : "MISSING";
      return domainResult({
        data: { access },
        status,
        provenance: {},
        gaps: access ? [] : ["access"],
      });
    },

    resolveBrandOperator({ hotel_id }) {
      const seed = getHotelSeed(hotel_id);
      const own = ownershipPayload(hotel_id);
      const oac = own.report?.ownership_and_control || {};
      const brand = oac.brand?.name || own.deep_research?.brand_resolution?.current_brand || seed.brand || null;
      const operator = oac.operator?.name || own.deep_research?.operator_resolution?.current_operator?.name || null;
      let status = "MISSING";
      if (brand && operator) status = "STRONG";
      else if (brand || operator) status = "PARTIAL";
      return domainResult({
        data: { brand, operator, brand_status: oac.brand?.status || null },
        status,
        provenance: { provider_id: own.provider_id },
        gaps: [
          ...(brand ? [] : ["brand"]),
          ...(operator ? [] : ["operator"]),
        ],
      });
    },

    resolveSourcesEvidence({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const sources = own.deep_research?.sources || own.report?.sources || [];
      const status = sources.length ? "STRONG" : "MISSING";
      return domainResult({
        data: { sources, source_count: sources.length },
        status,
        provenance: {},
        gaps: sources.length ? [] : ["sources"],
      });
    },

    resolveResearchArchive({ hotel_id }) {
      const own = ownershipPayload(hotel_id);
      const hasDeep = Boolean(own.deep_research);
      return domainResult({
        data: {
          has_deep_research: hasDeep,
          research_status: own.report?.research_status || null,
          dossier_hint: null,
        },
        status: hasDeep ? "STRONG" : "MISSING",
        provenance: { provider_id: own.provider_id },
        gaps: hasDeep ? [] : ["deep_research"],
      });
    },

    resolveCompleteness({ hotel_id }) {
      const resolvers = this;
      const domains = {
        IDENTITY: mapStatus(resolvers.resolveIdentity({ hotel_id })),
        PROPERTY_FUNDAMENTALS: mapStatus(resolvers.resolvePropertyFundamentals({ hotel_id })),
        OWNERSHIP: mapStatus(resolvers.resolveOwnership({ hotel_id })),
        OPERATOR: mapStatus(resolvers.resolveBrandOperator({ hotel_id }), "operator"),
        BRAND: mapStatus(resolvers.resolveBrandOperator({ hotel_id }), "brand"),
        ORGANIZATION: mapStatus(resolvers.resolveOrganizationPortfolio({ hotel_id })),
        PORTFOLIO: mapStatus(resolvers.resolveOrganizationPortfolio({ hotel_id })),
        PEOPLE: mapStatus(resolvers.resolvePeople({ hotel_id })),
        RELATIONSHIPS: mapStatus(resolvers.resolveRelationships({ hotel_id })),
        DEVELOPMENT: { status: "MISSING", what_is_missing: "development" },
        TRANSACTIONS: { status: "MISSING", what_is_missing: "transactions" },
        MARKET: mapStatus(resolvers.resolveMarket({ hotel_id })),
        AREA_HOTELS: mapStatus(resolvers.resolveAreaHotels({ hotel_id })),
        DEMAND: mapStatus(resolvers.resolveDemandDrivers({ hotel_id })),
        ACCESS: mapStatus(resolvers.resolveAccessConnectivity({ hotel_id })),
        SOURCES: mapStatus(resolvers.resolveSourcesEvidence({ hotel_id })),
        RESEARCH: mapStatus(resolvers.resolveResearchArchive({ hotel_id })),
      };
      const evaled = evaluateHotelIntelligenceCompleteness({
        hotel_id,
        hotel_name: getHotelSeed(hotel_id).name,
        domains,
      });
      return domainResult({
        data: evaled,
        status: "STRONG",
        provenance: { engine: evaled.engine_version },
        gaps: evaled.actionable_gaps?.map((g) => g.domain) || [],
      });
    },
  };
}

function mapStatus(domainResultObj, field) {
  const st = domainResultObj?.status || "MISSING";
  const mapped = {
    status: st === "STRONG" ? "STRONG" : st === "PARTIAL" ? "PARTIAL" : st === "COMPLETE" ? "COMPLETE" : "MISSING",
    what_is_missing: (domainResultObj?.gaps || [])[0] || null,
    confidence: domainResultObj?.data?.confidence || null,
    data_available: st !== "MISSING",
  };
  if (field === "operator" && domainResultObj?.data?.operator) {
    mapped.status = "STRONG";
    mapped.data_available = true;
  }
  if (field === "brand" && domainResultObj?.data?.brand) {
    mapped.status = "STRONG";
    mapped.data_available = true;
  }
  return mapped;
}
