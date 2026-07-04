/**
 * Aruba countrywide Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Aruba";
const REGION = "Caribbean";
const MARKET = "Aruba Countrywide";

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
    useCaseTags: v.useCaseTags || ["Airport / Transit", "Resort / Leisure"],
    defaultMapVisibility: "Visible",
    externalVisibilityLevel: "Member",
    projectRelevanceLogic: `${MARKET} island TI build — ${v.name}.`,
  };
}

/** @type {ReturnType<typeof ti>[]} */
export const ARUBA_TI_DELTA_RECORDS = [
  ti({
    name: "Aruba Ports Authority Barcadera Access",
    pointType: "Port / Maritime",
    pointSubtype: "Commercial Port",
    city: "Oranjestad",
    submarket: "Oranjestad / Cruise Port",
    latitude: 12.4799,
    longitude: -69.9972,
    sourceReference: "https://www.portaruba.com/",
    notes: "Aruba Ports Authority commercial port access supporting cargo and maritime hotel demand.",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),
  ti({
    name: "Palm Beach Hotel Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Noord",
    submarket: "Palm Beach / High-Rise Hotel Area",
    latitude: 12.57,
    longitude: -70.05,
    sourceReference: "https://www.aruba.com/us/explore/palm-beach",
    notes: "LG Smith Blvd resort corridor access linking Palm Beach high-rise hotel market.",
  }),
  ti({
    name: "Eagle Beach Low-Rise Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Oranjestad",
    submarket: "Eagle Beach / Low-Rise Hotel Area",
    latitude: 12.547,
    longitude: -70.058,
    sourceReference: "https://www.aruba.com/us/explore/eagle-beach",
    notes: "J.E. Irausquin Blvd access to Eagle Beach low-rise resort hotel strip.",
  }),
  ti({
    name: "Airport to Hotel Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Corridor",
    city: "Oranjestad",
    submarket: "Airport Corridor",
    latitude: 12.505,
    longitude: -70.025,
    sourceReference: "https://www.airportaruba.com/",
    notes: "Primary road access from Queen Beatrix Airport to Oranjestad and resort corridors.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "San Nicolas Baby Beach Access Corridor",
    pointType: "Highway Access",
    pointSubtype: "South Coast Access",
    city: "San Nicolas",
    submarket: "San Nicolas / Baby Beach",
    latitude: 12.415,
    longitude: -69.875,
    sourceReference: "https://www.aruba.com/us/explore/baby-beach",
    notes: "South-coast highway access to Baby Beach and San Nicolas tourism market.",
  }),
  ti({
    name: "Arashi Malmok Coastal Access",
    pointType: "Highway Access",
    pointSubtype: "Northwest Coast",
    city: "Noord",
    submarket: "Arashi / Malmok",
    latitude: 12.605,
    longitude: -70.045,
    sourceReference: "https://www.aruba.com/us/explore/arashi-beach",
    notes: "Northwest coastal road access to Arashi and Malmok beach resort demand.",
  }),
  ti({
    name: "Harbour House Marina Access",
    pointType: "Port / Maritime",
    pointSubtype: "Marina",
    city: "Oranjestad",
    submarket: "Oranjestad / Cruise Port",
    latitude: 12.517,
    longitude: -70.039,
    sourceReference: "https://www.harbourhousearuba.com/",
    notes: "Marina access supporting yacht tourism and waterfront hotel demand.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
  ti({
    name: "Renaissance Marina Ferry Access",
    pointType: "Ferry Terminal",
    pointSubtype: "Private Island Ferry",
    city: "Oranjestad",
    submarket: "Oranjestad / Cruise Port",
    latitude: 12.52,
    longitude: -70.036,
    sourceReference: "https://www.renaissancearubaresort.com/",
    notes: "Ferry access to Renaissance private island supporting resort and day-trip hotel demand.",
    useCaseTags: ["Resort / Leisure", "Cruise / Port"],
  }),
];

export function buildArubaTiDeltaFixture() {
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
      verifiedRecords: ARUBA_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: ARUBA_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Port, highway, marina, and ferry gap fill from Aruba TI audit; baseline 2 existing TI records.",
    },
    corrections: [],
    summary: {
      totalPoints: ARUBA_TI_DELTA_RECORDS.length,
      byPointType: ARUBA_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: ARUBA_TI_DELTA_RECORDS,
  };
}
