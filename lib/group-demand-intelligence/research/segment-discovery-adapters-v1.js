/**
 * Per-segment discovery query adapters for the 14 PowerShell FullE2E workflows.
 *
 * Hotel-agnostic query generation from existing builders. Does not call providers.
 * Does not invent candidates. Bethesda-biased V4/V5 templates are avoided for
 * international hotels — prefer GDI / base-of-demand / segment-specific builders.
 */

import { buildAssociationEventsDiscoveryStrategies } from "./association-events-discovery-strategies-v1.js";
import {
  buildAdaptiveScalingDiscoveryStrategies,
  buildDestinationCorporateDemandStrategies,
} from "./scaling-destination-demand-discovery-v1.js";
import { buildGdiDiscoveryQueries } from "../discovery/gdi-discovery-queries-v1.js";
import { buildBaseQueries } from "../ten-bases-of-demand-v1/base-queries.js";
import { GDI_BASE_OF_DEMAND } from "../ten-bases-of-demand-v1/taxonomy.js";
import {
  buildLocalMeetingsCateringResearchQueries,
  buildProfessionalPlannerResearchQueries,
} from "../workflow/local-meetings-catering-research-v1.js";
import { buildPreferredLodgingDiscoveryPrompt } from "../preferred-lodging-partnership/exa-preferred-lodging-contracts-v1.js";
import { buildProfessionalEventPlannerDiscoveryPrompt } from "../professional-event-planners/exa-professional-event-planner-contracts-v1.js";
import { buildVenueDiscoveryQueries } from "../private-events/live-venue-discovery.js";

export const SEGMENT_DISCOVERY_ADAPTERS_V1 = "gdi-segment-discovery-adapters-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hotelGeo(hotel = {}) {
  const city = text(hotel.city);
  const market = text(hotel.market || hotel.demandTerritory?.label || city);
  return {
    displayName: text(hotel.displayName || hotel.name),
    city,
    country: text(hotel.country),
    market,
    territoryIncludes: hotel.demandTerritory?.includes || [],
  };
}

/** Shape expected by ten-bases buildBaseQueries */
function hotelForBaseQueries(hotel = {}) {
  const geo = hotelGeo(hotel);
  const country = geo.country.toLowerCase();
  const languages =
    country === "mexico" || country === "méxico" || country === "mx"
      ? ["es", "en"]
      : country.includes("dominican")
        ? ["es", "en"]
        : ["en"];
  return {
    ...hotel,
    placeNames: [geo.city, geo.market].filter(Boolean),
    destinationMarket: geo.market || geo.city,
    market: geo.market || geo.city,
    languages,
  };
}

function fromStrategies(strategies = [], segmentId) {
  return (strategies || [])
    .map((s, i) => ({
      id: `${segmentId}_q${i + 1}`,
      segmentId,
      intent: s.intent || s.pathway || s.domain || null,
      query: text(s.researchQuestion || s.query || s.prompt || ""),
      language: s.language || null,
      sourceTypes: s.sourceTypes || null,
      weight: s.discoveryWeight ?? null,
    }))
    .filter((q) => q.query.length >= 12);
}

function fromGdiQueries(hotel, segmentHint) {
  try {
    const built = buildGdiDiscoveryQueries({
      hotel,
      basesOfDemand: segmentHint ? [segmentHint] : undefined,
      maxQueries: 8,
    });
    const rows = Array.isArray(built) ? built : built?.queries || [];
    return rows
      .map((q, i) => ({
        id: `gdi_${segmentHint || "gen"}_q${i + 1}`,
        segmentId: segmentHint || null,
        intent: q.baseOfDemand || q.intent || null,
        query: text(q.query || q.text || q.q || ""),
        language: q.queryLanguage || q.language || null,
        locale: q.serpLocale || null,
      }))
      .filter((q) => q.query.length >= 12);
  } catch {
    return [];
  }
}

function fromBase(hotel, baseKey, segmentId) {
  try {
    const base = GDI_BASE_OF_DEMAND[baseKey] || baseKey;
    const rows = buildBaseQueries(hotelForBaseQueries(hotel), base, {
      maxQueries: 4,
      priority: "HIGH",
    }) || [];
    return rows
      .map((q, i) => ({
        id: `${segmentId}_base_q${i + 1}`,
        segmentId,
        intent: baseKey,
        query: text(typeof q === "string" ? q : q.query || q.text || ""),
        language: q.language || null,
      }))
      .filter((q) => q.query.length >= 12);
  } catch {
    return [];
  }
}

/**
 * Build segment-specific discovery queries for a resolved hotel profile.
 * @returns {{ segmentId: string, queries: object[], builder: string, readiness: string }}
 */
export function buildSegmentDiscoveryQueries(segmentId, hotel = {}) {
  const geo = hotelGeo(hotel);
  const sid = String(segmentId || "").trim();

  switch (sid) {
    case "associations_conferences_events": {
      const strategies = buildAssociationEventsDiscoveryStrategies(hotel);
      return {
        segmentId: sid,
        builder: "buildAssociationEventsDiscoveryStrategies",
        readiness: "EXECUTOR_READY",
        queries: fromStrategies(strategies, sid).slice(0, 10),
        geo,
      };
    }
    case "corporate_accounts_scaling": {
      const strategies = buildAdaptiveScalingDiscoveryStrategies(hotel);
      return {
        segmentId: sid,
        builder: "buildAdaptiveScalingDiscoveryStrategies",
        readiness: "EXECUTOR_READY",
        queries: fromStrategies(strategies, sid).slice(0, 10),
        geo,
      };
    }
    case "corporate_meetings_incentives": {
      const strategies = buildDestinationCorporateDemandStrategies(hotel);
      return {
        segmentId: sid,
        builder: "buildDestinationCorporateDemandStrategies",
        readiness: "EXECUTOR_READY",
        queries: fromStrategies(strategies, sid).slice(0, 8),
        geo,
      };
    }
    case "government_public_sector": {
      const city = geo.city || "destination";
      const country = geo.country || "";
      const manual = [
        `${city} ${country} government conference lodging OR hotel block 2026 OR 2027`,
        `${city} ministerio OR gobierno convocatoria reunión hotel`,
        `${city} public sector training workshop accommodation RFP`,
      ].map((query, i) => ({
        id: `gov_q${i + 1}`,
        segmentId: sid,
        intent: "government_public_meetings",
        query,
        language: i === 1 ? "es" : "en",
      }));
      return {
        segmentId: sid,
        builder: "government_manual_plus_gdi",
        readiness: "EXECUTOR_READY",
        queries: [...manual, ...fromGdiQueries(hotel, "institutional")].slice(0, 8),
        geo,
      };
    }
    case "sports_entertainment": {
      return {
        segmentId: sid,
        builder: "buildBaseQueries:SPORTS_ENTERTAINMENT",
        readiness: "EXECUTOR_READY",
        queries: fromBase(
          hotel,
          "SPORTS_ENTERTAINMENT_PRODUCTION",
          sid,
        ).slice(0, 8),
        geo,
      };
    }
    case "education_scientific": {
      const assoc = buildAssociationEventsDiscoveryStrategies(hotel).filter(
        (s) => /medical|scientific|academic|medico|salud/i.test(`${s.intent} ${s.domain} ${s.researchQuestion}`),
      );
      return {
        segmentId: sid,
        builder: "assoc_medical_filter_plus_pharma_base",
        readiness: "EXECUTOR_READY",
        queries: [
          ...fromStrategies(assoc, sid),
          ...fromBase(hotel, "PHARMA_MEDICAL_ECOSYSTEM", sid),
        ].slice(0, 8),
        geo,
      };
    }
    case "projects_infrastructure": {
      return {
        segmentId: sid,
        builder: "buildBaseQueries:PROJECT_WORKFORCE",
        readiness: "EXECUTOR_READY",
        queries: fromBase(hotel, "PROJECT_WORKFORCE_DEMAND", sid).slice(0, 8),
        geo,
      };
    }
    case "social_special_events": {
      let venueQs = [];
      try {
        venueQs = (buildVenueDiscoveryQueries(hotel) || []).map((q, i) => ({
          id: `social_venue_q${i + 1}`,
          segmentId: sid,
          intent: "social_venue",
          query: text(typeof q === "string" ? q : q.query || q.text || ""),
        }));
      } catch {
        venueQs = [];
      }
      const city = geo.city || "destination";
      const fallback = [
        {
          id: "social_q1",
          segmentId: sid,
          intent: "destination_wedding",
          query: `${city} destination wedding hotel block OR wedding packages 2026 2027`,
        },
        {
          id: "social_q2",
          segmentId: sid,
          intent: "social_gala",
          query: `${city} gala OR fundraising dinner hotel venue`,
        },
      ];
      return {
        segmentId: sid,
        builder: "buildVenueDiscoveryQueries_or_fallback",
        readiness: "EXECUTOR_READY",
        queries: (venueQs.filter((q) => q.query.length >= 12).length
          ? venueQs
          : fallback
        ).slice(0, 8),
        geo,
      };
    }
    case "local_catering_day_meetings": {
      const rows = buildLocalMeetingsCateringResearchQueries(hotel) || [];
      return {
        segmentId: sid,
        builder: "buildLocalMeetingsCateringResearchQueries",
        readiness: "EXECUTOR_READY",
        queries: rows
          .map((q, i) => ({
            id: `local_q${i + 1}`,
            segmentId: sid,
            intent: q.intent || q.category || null,
            query: text(q.query || q.researchQuestion || q.text || ""),
          }))
          .filter((q) => q.query.length >= 12)
          .slice(0, 8),
        geo,
      };
    }
    case "corporate_celebrations_private_events": {
      const dest = buildDestinationCorporateDemandStrategies(hotel).filter((s) =>
        /recognition|celebration|president|incentive|kickoff|gala/i.test(
          `${s.intent} ${s.researchQuestion}`,
        ),
      );
      return {
        segmentId: sid,
        builder: "destination_recognition_filter",
        readiness: "EXECUTOR_READY",
        queries: fromStrategies(dest, sid).slice(0, 8),
        geo,
      };
    }
    case "travel_trade_group_intermediaries": {
      const rows = buildProfessionalPlannerResearchQueries(hotel) || [];
      return {
        segmentId: sid,
        builder: "buildProfessionalPlannerResearchQueries",
        readiness: "EXECUTOR_READY",
        queries: rows
          .map((q, i) => ({
            id: `trade_q${i + 1}`,
            segmentId: sid,
            intent: q.intent || q.category || "travel_trade",
            query: text(q.query || q.researchQuestion || q.text || ""),
          }))
          .filter((q) => q.query.length >= 12)
          .slice(0, 8),
        geo,
      };
    }
    case "relocation_extended_stay": {
      const city = geo.city || "destination";
      const queries = [
        `${city} corporate relocation temporary housing OR extended stay group`,
        `${city} project team mobilization lodging OR workforce camp hotel`,
        `${city} mudanza corporativa alojamiento temporal hotel`,
      ].map((query, i) => ({
        id: `reloc_q${i + 1}`,
        segmentId: sid,
        intent: "relocation_extended_stay",
        query,
        language: i === 2 ? "es" : "en",
      }));
      return {
        segmentId: sid,
        builder: "relocation_manual_templates",
        readiness: "EXECUTOR_READY",
        queries,
        geo,
      };
    }
    case "preferred_lodging_partnerships": {
      let prompt = "";
      try {
        prompt = text(buildPreferredLodgingDiscoveryPrompt(hotel) || "");
      } catch {
        prompt = "";
      }
      const city = geo.city || "destination";
      const queries = prompt
        ? [{ id: "pref_prompt", segmentId: sid, intent: "preferred_lodging_prompt", query: prompt.slice(0, 1200) }]
        : [
            {
              id: "pref_q1",
              segmentId: sid,
              intent: "preferred_lodging",
              query: `${city} preferred hotel program OR negotiated rate hotel partnership venue`,
            },
          ];
      return {
        segmentId: sid,
        builder: "buildPreferredLodgingDiscoveryPrompt",
        readiness: "EXECUTOR_READY",
        queries,
        geo,
      };
    }
    case "professional_event_planners": {
      let prompt = "";
      try {
        prompt = text(buildProfessionalEventPlannerDiscoveryPrompt(hotel) || "");
      } catch {
        prompt = "";
      }
      const city = geo.city || "destination";
      const queries = prompt
        ? [
            {
              id: "pep_prompt",
              segmentId: sid,
              intent: "professional_event_planner_prompt",
              query: prompt.slice(0, 1200),
            },
          ]
        : [
            {
              id: "pep_q1",
              segmentId: sid,
              intent: "professional_event_planner",
              query: `${city} professional conference organizer OR PCO OR DMC hotel block`,
            },
          ];
      return {
        segmentId: sid,
        builder: "buildProfessionalEventPlannerDiscoveryPrompt",
        readiness: "EXECUTOR_READY",
        queries,
        geo,
      };
    }
    default:
      return {
        segmentId: sid,
        builder: "unknown",
        readiness: "EXECUTOR_NOT_IMPLEMENTED",
        queries: [],
        geo,
      };
  }
}
