/**
 * GDI Leisure Demand Intelligence (LDI) V1 — module registry.
 *
 * LDI modules are capabilities layered on the canonical 14-segment Full E2E
 * discovery handlers — not a competing product or opportunity database.
 */

export const GDI_LEISURE_MODULE_REGISTRY_V1 = "gdi-leisure-module-registry-v1";

/** @typedef {"EXISTS"|"PARTIAL"|"OVERLAP_WRAPPER"|"GAP"} CoverageClass */

export const LEISURE_COMMERCIAL_PATHWAY = Object.freeze({
  DIRECTLY_ADDRESSABLE: "DIRECTLY_ADDRESSABLE",
  PARTNERSHIP_DRIVEN: "PARTNERSHIP_DRIVEN",
  CAMPAIGN_DRIVEN: "CAMPAIGN_DRIVEN",
});

export const LEISURE_MODULE = Object.freeze({
  EVENTS: Object.freeze({
    id: "leisure_events",
    shortId: "events",
    label: "Festivals, concerts and cultural events",
    powershell: "scripts/gdi-leisure/Invoke-GdiLeisureEvents.ps1",
    primarySegments: Object.freeze([
      "associations_conferences_events",
      "social_special_events",
      "corporate_celebrations_private_events",
    ]),
    secondarySegments: Object.freeze(["professional_event_planners"]),
    coverage: "PARTIAL",
    implementationMode: "WRAPPER_PLUS_QUALIFICATION",
    reuseHandlers: Object.freeze([
      "runSegmentDiscoveryExecutor:associations_conferences_events",
      "runSegmentDiscoveryExecutor:social_special_events",
    ]),
    leisureSubtype: "cultural_events_festivals_concerts",
    preferredPathways: Object.freeze([
      LEISURE_COMMERCIAL_PATHWAY.DIRECTLY_ADDRESSABLE,
      LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN,
    ]),
    notes:
      "Reuses events/social discovery; leisure specialization = nonlocal leisure visitor evidence + festival/concert filters (not association MICE-only).",
  }),
  SPORTS: Object.freeze({
    id: "leisure_sports",
    shortId: "sports",
    label: "Participatory sports and recreational travel",
    powershell: "scripts/gdi-leisure/Invoke-GdiLeisureSports.ps1",
    primarySegments: Object.freeze(["sports_entertainment"]),
    secondarySegments: Object.freeze([
      "preferred_lodging_partnerships",
      "travel_trade_group_intermediaries",
    ]),
    coverage: "OVERLAP_WRAPPER",
    implementationMode: "WRAPPER_PLUS_QUALIFICATION",
    reuseHandlers: Object.freeze([
      "runSegmentDiscoveryExecutor:sports_entertainment",
      "Invoke-GdiSportsHousingHunter.ps1",
    ]),
    leisureSubtype: "participatory_sports_recreation",
    preferredPathways: Object.freeze([
      LEISURE_COMMERCIAL_PATHWAY.DIRECTLY_ADDRESSABLE,
      LEISURE_COMMERCIAL_PATHWAY.PARTNERSHIP_DRIVEN,
    ]),
    notes:
      "Wraps sports_entertainment + sports housing hunter; filters participatory / recreational travel (not spectator-only unless lodging mechanism proven).",
  }),
  ATTRACTIONS: Object.freeze({
    id: "leisure_attractions",
    shortId: "attractions",
    label: "Attractions and destination experiences",
    powershell: "scripts/gdi-leisure/Invoke-GdiLeisureAttractions.ps1",
    primarySegments: Object.freeze([
      "preferred_lodging_partnerships",
      "travel_trade_group_intermediaries",
    ]),
    secondarySegments: Object.freeze(["social_special_events"]),
    coverage: "PARTIAL",
    implementationMode: "WRAPPER_PLUS_QUALIFICATION",
    reuseHandlers: Object.freeze([
      "runSegmentDiscoveryExecutor:preferred_lodging_partnerships",
      "Invoke-GdiVenueLodgingPartnershipsTrack.ps1",
    ]),
    leisureSubtype: "attractions_destination_experiences",
    preferredPathways: Object.freeze([
      LEISURE_COMMERCIAL_PATHWAY.PARTNERSHIP_DRIVEN,
      LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN,
    ]),
    notes:
      "Partnership-first attractions/experiences; reuses preferred lodging + venue lodging tracks with leisure partnership qualification.",
  }),
  UNIVERSITIES: Object.freeze({
    id: "leisure_universities",
    shortId: "universities",
    label: "Universities and visiting-friends-and-relatives demand",
    powershell: "scripts/gdi-leisure/Invoke-GdiLeisureUniversities.ps1",
    primarySegments: Object.freeze(["education_scientific"]),
    secondarySegments: Object.freeze([
      "relocation_extended_stay",
      "social_special_events",
    ]),
    coverage: "PARTIAL",
    implementationMode: "WRAPPER_PLUS_QUALIFICATION",
    reuseHandlers: Object.freeze([
      "runSegmentDiscoveryExecutor:education_scientific",
      "Invoke-GdiUniversityDemandHunter.ps1",
    ]),
    leisureSubtype: "university_vfr",
    preferredPathways: Object.freeze([
      LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN,
      LEISURE_COMMERCIAL_PATHWAY.DIRECTLY_ADDRESSABLE,
    ]),
    notes:
      "Education segment is conference-heavy; leisure module specializes graduation / parents weekend / VFR / alumni leisure travel.",
  }),
  DESTINATION: Object.freeze({
    id: "leisure_destination",
    shortId: "destination",
    label: "Destination demand shifts and special occasions",
    powershell: "scripts/gdi-leisure/Invoke-GdiLeisureDestination.ps1",
    primarySegments: Object.freeze([
      "social_special_events",
      "corporate_celebrations_private_events",
    ]),
    secondarySegments: Object.freeze(["government_public_sector"]),
    coverage: "GAP",
    implementationMode: "SPECIALIZATION_OVER_EXISTING_QUERIES",
    reuseHandlers: Object.freeze([
      "runSegmentDiscoveryExecutor:social_special_events",
      "need-date fourteen strategy relevance",
    ]),
    leisureSubtype: "destination_demand_special_occasions",
    preferredPathways: Object.freeze([
      LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN,
      LEISURE_COMMERCIAL_PATHWAY.DIRECTLY_ADDRESSABLE,
    ]),
    notes:
      "Largest net-new leisure framing (shoulder/holiday/occasion shifts); still uses shared discovery executor + need-period contract — no new DB.",
  }),
  DISTRIBUTION: Object.freeze({
    id: "leisure_distribution",
    shortId: "distribution",
    label: "Leisure distribution and referral relationships",
    powershell: "scripts/gdi-leisure/Invoke-GdiLeisureDistribution.ps1",
    primarySegments: Object.freeze([
      "travel_trade_group_intermediaries",
      "preferred_lodging_partnerships",
      "professional_event_planners",
    ]),
    secondarySegments: Object.freeze([]),
    coverage: "OVERLAP_WRAPPER",
    implementationMode: "WRAPPER_PLUS_QUALIFICATION",
    reuseHandlers: Object.freeze([
      "runSegmentDiscoveryExecutor:travel_trade_group_intermediaries",
      "Invoke-GdiPlannerPartnershipsTrack.ps1",
    ]),
    leisureSubtype: "leisure_distribution_referral",
    preferredPathways: Object.freeze([
      LEISURE_COMMERCIAL_PATHWAY.PARTNERSHIP_DRIVEN,
    ]),
    notes:
      "DMC / OTA leisure / destination bureau / referral partners — wraps travel trade + preferred lodging + planner tracks.",
  }),
});

export const LEISURE_MODULES_ORDERED = Object.freeze(
  Object.values(LEISURE_MODULE),
);

export function getLeisureModule(idOrShort) {
  const key = String(idOrShort || "")
    .trim()
    .toLowerCase()
    .replace(/^leisure_/, "");
  return (
    LEISURE_MODULES_ORDERED.find(
      (m) =>
        m.id === idOrShort ||
        m.shortId === key ||
        m.id === `leisure_${key}` ||
        m.shortId === idOrShort,
    ) || null
  );
}

export function listLeisureModuleIds() {
  return LEISURE_MODULES_ORDERED.map((m) => m.id);
}

/** Coverage matrix rows for founder report / AuditOnly. */
export function buildLeisureFourteenSegmentCoverageMatrix() {
  return LEISURE_MODULES_ORDERED.map((m) => ({
    leisureModuleId: m.id,
    label: m.label,
    coverage: m.coverage,
    implementationMode: m.implementationMode,
    primarySegments: [...m.primarySegments],
    secondarySegments: [...m.secondarySegments],
    reuseHandlers: [...m.reuseHandlers],
    preferredPathways: [...m.preferredPathways],
    notes: m.notes,
  }));
}
