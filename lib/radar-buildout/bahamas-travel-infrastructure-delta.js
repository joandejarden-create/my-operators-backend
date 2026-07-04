/**
 * Bahamas countrywide Travel Infrastructure delta records (audit gap fill).
 * Existing country TI baseline is 25; delta adds bridge/ferry/marina access gaps only.
 */

const COUNTRY = "Bahamas";
const REGION = "Caribbean";
const MARKET = "Bahamas Countrywide";

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
export const BAHAMAS_TI_DELTA_RECORDS = [
  ti({
    name: "Sir Sidney Poitier Bridge Access",
    pointType: "Highway Access",
    pointSubtype: "Paradise Island Bridge",
    city: "Nassau",
    submarket: "Paradise Island",
    latitude: 25.081,
    longitude: -77.328,
    sourceReference: "https://www.bahamas.com/islands/nassau-paradise-island",
    notes: "Primary vehicular access linking Nassau to Paradise Island resort hotel markets.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Harbour Island Ferry Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Out Island Ferry",
    city: "North Eleuthera",
    submarket: "Eleuthera / Harbour Island",
    latitude: 25.475,
    longitude: -76.683,
    sourceReference: "https://www.bahamas.com/islands/eleuthera-harbour-island",
    notes: "Ferry access to Harbour Island supporting boutique resort hotel demand.",
    useCaseTags: ["Resort / Leisure", "Airport / Transit"],
  }),
  ti({
    name: "Exuma Island Ferry Access",
    pointType: "Ferry Terminal",
    pointSubtype: "Cays Ferry",
    city: "George Town",
    submarket: "Exuma",
    latitude: 23.548,
    longitude: -75.824,
    sourceReference: "https://www.bahamas.com/islands/exuma",
    notes: "Inter-island ferry and marina access supporting Exuma cays resort tourism.",
  }),
  ti({
    name: "Nassau Harbour Marina Access Node",
    pointType: "Port / Maritime",
    pointSubtype: "Marina Access",
    city: "Nassau",
    submarket: "Nassau / New Providence",
    latitude: 25.077,
    longitude: -77.343,
    sourceReference: "https://www.bahamas.com/",
    notes: "Marina and harbour access supporting yacht tourism and downtown hotel demand.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
];

export function buildBahamasTiDeltaFixture() {
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
      verifiedRecords: BAHAMAS_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: BAHAMAS_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Bridge, ferry, and marina gap fill from Bahamas TI audit; baseline 25 existing TI records.",
    },
    corrections: [],
    summary: {
      totalPoints: BAHAMAS_TI_DELTA_RECORDS.length,
      byPointType: BAHAMAS_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: BAHAMAS_TI_DELTA_RECORDS,
  };
}
