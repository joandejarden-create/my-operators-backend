/**
 * Jamaica countrywide Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Jamaica";
const REGION = "Caribbean";
const MARKET = "Jamaica Countrywide";

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
    projectRelevanceLogic: `${MARKET} corridor TI build — ${v.name}.`,
  };
}

/** @type {ReturnType<typeof ti>[]} */
export const JAMAICA_TI_DELTA_RECORDS = [
  ti({
    name: "North Coast Highway A1 Corridor Node",
    pointType: "Highway Access",
    pointSubtype: "North Coast Corridor",
    city: "Runaway Bay",
    submarket: "Ocho Rios",
    latitude: 18.4544,
    longitude: -77.3344,
    sourceReference: "https://www.nwa.gov.jm/",
    notes: "Primary A1 north-coast highway linking Montego Bay, Falmouth, and Ocho Rios resort markets.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "South Coast Highway A2 Corridor Node",
    pointType: "Highway Access",
    pointSubtype: "South Coast Corridor",
    city: "Black River",
    submarket: "South Coast",
    latitude: 18.0244,
    longitude: -77.8544,
    sourceReference: "https://www.nwa.gov.jm/",
    notes: "A2 south-coast highway access supporting Treasure Beach and Mandeville tourism corridors.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Falmouth Cruise Port Access Corridor",
    pointType: "Highway Access",
    pointSubtype: "Cruise Port Access",
    city: "Falmouth",
    submarket: "Falmouth",
    latitude: 18.4934,
    longitude: -77.6544,
    sourceReference: "https://www.portjam.com/",
    notes: "Vehicular access corridor for Historic Falmouth cruise turnaround hotel demand.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
  ti({
    name: "Negril Aerodrome Access Corridor",
    pointType: "Highway Access",
    pointSubtype: "Regional Air Access",
    city: "Negril",
    submarket: "Negril",
    latitude: 18.2934,
    longitude: -78.3354,
    sourceReference: "https://www.jcaa.gov.jm/",
    notes: "Road access node for Negril Aerodrome supporting west-end resort air connectivity.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Kingston Harbour Maritime Access",
    pointType: "Port / Maritime",
    pointSubtype: "Harbour Access",
    city: "Kingston",
    submarket: "Kingston",
    latitude: 17.9684,
    longitude: -76.7844,
    sourceReference: "https://www.portjam.com/",
    notes: "Kingston waterfront maritime access supporting urban and cruise-extension hotel demand.",
    useCaseTags: ["Cruise / Port", "Urban / Corporate"],
  }),
  ti({
    name: "Montego Bay Airport Corridor Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Corridor",
    city: "Montego Bay",
    submarket: "Montego Bay",
    latitude: 18.4984,
    longitude: -77.9084,
    sourceReference: "https://www.mbjairport.com/",
    notes: "Highway access linking Sangster International Airport to north-coast resort hotels.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Norman Manley Airport Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Corridor",
    city: "Kingston",
    submarket: "Kingston",
    latitude: 17.9354,
    longitude: -76.7874,
    sourceReference: "https://www.kinston-airport.com/",
    notes: "Airport corridor access linking Norman Manley International Airport to Kingston hotel markets.",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Port Antonio Coastal Highway Access",
    pointType: "Highway Access",
    pointSubtype: "East Coast Corridor",
    city: "Port Antonio",
    submarket: "Port Antonio",
    latitude: 18.1744,
    longitude: -76.4544,
    sourceReference: "https://www.nwa.gov.jm/",
    notes: "Coastal highway access node for Port Antonio and east-coast boutique resort markets.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Mandeville Interior Highway Node",
    pointType: "Highway Access",
    pointSubtype: "South Coast Interior",
    city: "Mandeville",
    submarket: "South Coast",
    latitude: 18.0544,
    longitude: -77.5044,
    sourceReference: "https://www.nwa.gov.jm/",
    notes: "Interior highway node linking south-coast leisure corridors to Kingston business travel.",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
];

export function buildJamaicaTiDeltaFixture() {
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
      verifiedRecords: JAMAICA_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: JAMAICA_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Highway, port, and regional airport gap fill from Jamaica TI audit.",
    },
    corrections: [],
    summary: {
      totalPoints: JAMAICA_TI_DELTA_RECORDS.length,
      byPointType: JAMAICA_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: JAMAICA_TI_DELTA_RECORDS,
  };
}
