/**
 * Panama countrywide Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Panama";
const REGION = "Central America";
const MARKET = "Panama Countrywide";

function ti(v) {
  return {
    name: v.name,
    pointType: v.pointType,
    pointSubtype: v.pointSubtype || "",
    city: v.city,
    country: COUNTRY,
    region: REGION,
    submarket: v.submarket || "Other",
    latitude: v.latitude,
    longitude: v.longitude,
    source: "Public Source",
    sourceReference: v.sourceReference,
    dataConfidence: v.dataConfidence || "High",
    demandRelevance: v.demandRelevance || "High",
    includeOnRadarMap: true,
    notes: v.notes || "",
    scopeLevel: "Country",
    relevanceTier: v.relevanceTier || "Tier 1",
    useCaseTags: v.useCaseTags || ["Airport / Transit", "Industrial / Logistics"],
    defaultMapVisibility: "Visible",
    externalVisibilityLevel: "Member",
    projectRelevanceLogic: `${MARKET} corridor TI build — ${v.name}.`,
  };
}

/** @type {ReturnType<typeof ti>[]} */
export const PANAMA_TI_DELTA_RECORDS = [
  ti({
    name: "Miraflores Locks Panama Canal Access",
    pointType: "Port / Maritime",
    pointSubtype: "Canal Access",
    city: "Panama City",
    submarket: "Canal / Logistics Corridor",
    latitude: 8.9972715,
    longitude: -79.5913604,
    sourceReference: "https://pancanal.com/",
    notes: "Primary canal visitor and maritime logistics access node on the Pacific side.",
    useCaseTags: ["Industrial / Logistics", "Resort / Leisure"],
  }),
  ti({
    name: "Port of Balboa",
    pointType: "Port / Maritime",
    pointSubtype: "Container Port",
    city: "Panama City",
    submarket: "Canal / Logistics Corridor",
    latitude: 8.9488,
    longitude: -79.5688,
    sourceReference: "https://www.pancanal.com/",
    notes: "Pacific-side container port supporting maritime crew and logistics hotel demand.",
  }),
  ti({
    name: "Port of Cristóbal",
    pointType: "Port / Maritime",
    pointSubtype: "Container Port",
    city: "Colón",
    submarket: "Canal / Logistics Corridor",
    latitude: 9.3488,
    longitude: -79.9012,
    sourceReference: "https://www.pancanal.com/",
    notes: "Atlantic-side port node paired with Colón cruise and free-zone logistics flows.",
  }),
  ti({
    name: "Manzanillo International Terminal",
    pointType: "Port / Maritime",
    pointSubtype: "Container Terminal",
    city: "Colón",
    submarket: "Canal / Logistics Corridor",
    latitude: 9.3651477,
    longitude: -79.8811514,
    sourceReference: "https://www.mitradel.gob.pa/",
    notes: "Major Caribbean-side container terminal for Panama logistics corridor.",
  }),
  ti({
    name: "Colón Free Zone Logistics Access",
    pointType: "Highway Access",
    pointSubtype: "Logistics Corridor",
    city: "Colón",
    submarket: "Canal / Logistics Corridor",
    latitude: 9.3500294,
    longitude: -79.8824052,
    sourceReference: "https://www.colonfreezone.com/",
    notes: "Free-zone logistics gateway supporting trade, distribution, and extended-stay demand.",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),
  ti({
    name: "Amador Causeway Marina & Ferry Access",
    pointType: "Ferry Terminal",
    pointSubtype: "Marina Access",
    city: "Panama City",
    submarket: "Casco Viejo / Waterfront",
    latitude: 8.9136,
    longitude: -79.5349,
    sourceReference: "https://www.visitpanama.com/",
    notes: "Waterfront marina and causeway access supporting cruise, leisure, and event hotel demand.",
    useCaseTags: ["Resort / Leisure", "Airport / Transit"],
  }),
  ti({
    name: "Bocas del Toro Water Taxi Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Island Ferry",
    city: "Bocas del Toro",
    submarket: "Bocas del Toro",
    latitude: 9.3402,
    longitude: -82.2412,
    sourceReference: "https://www.visitpanama.com/destination/bocas-del-toro/",
    notes: "Island water-taxi hub linking Bocas archipelago leisure lodging markets.",
    useCaseTags: ["Resort / Leisure", "Airport / Transit"],
  }),
];

export const PANAMA_TI_CORRECTIONS = [
  {
    existingName: "Panama City Tocumen Airport",
    issue: "Name does not match Tocumen International Airport pattern",
    recommendedAction: "optional_rename",
    notes: "Existing record covers PTY; no duplicate import required.",
  },
];

export function buildPanamaTiDeltaFixture() {
  return {
    market: MARKET,
    country: COUNTRY,
    region: REGION,
    buildBatch: "delta",
    status: "verified_ready",
    generatedAt: new Date().toISOString(),
    verification: {
      method: "Source-backed Travel Infrastructure delta; no Google fields on points",
      verifiedAt: new Date().toISOString(),
      verifiedRecords: PANAMA_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: PANAMA_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Panama canal, port, free-zone, and ferry gap fill from TI audit.",
    },
    corrections: PANAMA_TI_CORRECTIONS,
    summary: {
      totalPoints: PANAMA_TI_DELTA_RECORDS.length,
      byPointType: PANAMA_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: PANAMA_TI_DELTA_RECORDS,
  };
}
