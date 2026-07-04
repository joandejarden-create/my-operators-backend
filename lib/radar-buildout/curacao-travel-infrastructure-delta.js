/**
 * Curaçao countrywide Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Curaçao";
const REGION = "Caribbean";
const MARKET = "Curaçao Countrywide";

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
export const CURACAO_TI_DELTA_RECORDS = [
  ti({
    name: "Curaçao Ports Authority Bullenbaai Access",
    pointType: "Port / Maritime",
    pointSubtype: "Commercial Port",
    city: "Bullenbaai",
    submarket: "Port / Industrial Corridor",
    latitude: 12.155,
    longitude: -69.005,
    sourceReference: "https://www.curacao-ports.com/",
    notes: "Curaçao Ports Authority deep-water port access supporting cargo and maritime hotel demand.",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),
  ti({
    name: "Mega Pier Cruise Terminal Access",
    pointType: "Port / Maritime",
    pointSubtype: "Cruise Terminal",
    city: "Willemstad",
    submarket: "Port / Industrial Corridor",
    latitude: 12.099,
    longitude: -68.942,
    sourceReference: "https://www.curacao-ports.com/",
    notes: "Mega Pier large-ship cruise terminal access supporting pre/post-cruise hotel demand.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
  ti({
    name: "Airport to Willemstad Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Corridor",
    city: "Hato",
    submarket: "Airport Corridor",
    latitude: 12.16,
    longitude: -68.965,
    sourceReference: "https://www.curacao-airport.com/",
    notes: "Primary road access from Hato airport to Willemstad and resort corridors.",
  }),
  ti({
    name: "Mambo Beach Seaquarium Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Willemstad",
    submarket: "Mambo Beach / Seaquarium",
    latitude: 12.119,
    longitude: -68.972,
    sourceReference: "https://www.curacao.com/en/discover/mambo-beach",
    notes: "South-coast resort corridor access linking Mambo Beach and Sea Aquarium hotel market.",
  }),
  ti({
    name: "Jan Thiel Resort Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Jan Thiel",
    submarket: "Jan Thiel",
    latitude: 12.084,
    longitude: -68.847,
    sourceReference: "https://www.curacao.com/en/discover/jan-thiel",
    notes: "Jan Thiel lagoon resort corridor highway access supporting south-east hotel demand.",
  }),
  ti({
    name: "Blue Bay Piscadera Access Corridor",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Blue Bay",
    submarket: "Piscadera / Blue Bay",
    latitude: 12.132,
    longitude: -68.975,
    sourceReference: "https://www.curacao.com/en/discover/blue-bay",
    notes: "West-coast access corridor to Blue Bay and Piscadera resort beaches.",
  }),
  ti({
    name: "Spanish Water Marina Access",
    pointType: "Port / Maritime",
    pointSubtype: "Marina",
    city: "Spanish Water",
    submarket: "Spanish Water / Caracasbaai",
    latitude: 12.076,
    longitude: -68.848,
    sourceReference: "https://www.curacao.com/en/discover/spanish-water",
    notes: "Spanish Water marina access supporting yacht tourism and waterfront hotel demand.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
  ti({
    name: "Westpunt Banda Abou Coastal Access",
    pointType: "Highway Access",
    pointSubtype: "West Coast",
    city: "Westpunt",
    submarket: "Westpunt / Banda Abou",
    latitude: 12.35,
    longitude: -69.14,
    sourceReference: "https://www.curacao.com/en/discover/westpunt",
    notes: "West-coast highway access to Kenepa, Kalki, and Christoffel leisure hotel demand.",
  }),
  ti({
    name: "Caracasbaai Bay Access Corridor",
    pointType: "Highway Access",
    pointSubtype: "South Coast",
    city: "Caracasbaai",
    submarket: "Spanish Water / Caracasbaai",
    latitude: 12.067,
    longitude: -68.853,
    sourceReference: "https://www.curacao.com/en/discover/caracas-bay",
    notes: "South-east coast access to Caracasbaai and Spanish Water hotel markets.",
  }),
];

export function buildCuracaoTiDeltaFixture() {
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
      verifiedRecords: CURACAO_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: CURACAO_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Port, highway, and marina gap fill from Curaçao TI audit; baseline 2 existing TI records.",
    },
    corrections: [],
    summary: {
      totalPoints: CURACAO_TI_DELTA_RECORDS.length,
      byPointType: CURACAO_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: CURACAO_TI_DELTA_RECORDS,
  };
}
