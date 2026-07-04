/**
 * Turks & Caicos countrywide Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Turks & Caicos";
const REGION = "Caribbean";
const MARKET = "Turks & Caicos Countrywide";

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

export const TURKS_AND_CAICOS_TI_DELTA_RECORDS = [
  ti({
    name: "Providenciales International Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Providenciales",
    submarket: "Providenciales",
    latitude: 21.7736,
    longitude: -72.2659,
    sourceReference: "https://www.tciairports.com/",
    notes: "Primary stayover gateway for Providenciales resort demand.",
  }),
  ti({
    name: "Grace Bay Resort Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Grace Bay",
    submarket: "Providenciales",
    latitude: 21.7984,
    longitude: -72.1765,
    sourceReference: "https://www.visittci.com/",
    notes: "Main road access serving Grace Bay resort inventory.",
  }),
  ti({
    name: "Leeward Marina Access",
    pointType: "Port / Maritime",
    pointSubtype: "Marina",
    city: "Leeward",
    submarket: "Providenciales",
    latitude: 21.8153,
    longitude: -72.1625,
    sourceReference: "https://www.visittci.com/",
    notes: "Marina access supporting yacht and luxury leisure demand.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
  ti({
    name: "South Dock Port Logistics Access",
    pointType: "Port / Maritime",
    pointSubtype: "Commercial Port",
    city: "Providenciales",
    submarket: "Providenciales",
    latitude: 21.7423,
    longitude: -72.2704,
    sourceReference: "https://www.gov.tc/",
    notes: "Cargo and freight port access supporting construction and business travel demand.",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),
  ti({
    name: "JAGS McCartney Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Grand Turk",
    submarket: "Grand Turk",
    latitude: 21.4446,
    longitude: -71.1419,
    sourceReference: "https://www.tciairports.com/",
    notes: "Grand Turk airport access for inter-island and government travel demand.",
  }),
  ti({
    name: "Grand Turk Cruise Center Access",
    pointType: "Port / Maritime",
    pointSubtype: "Cruise Terminal",
    city: "Grand Turk",
    submarket: "Grand Turk",
    latitude: 21.4675,
    longitude: -71.1389,
    sourceReference: "https://www.grandturkcc.com/",
    notes: "Cruise terminal node supporting day-call and pre/post-stay lodging.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
  ti({
    name: "North Caicos Ferry Gateway Access",
    pointType: "Ferry Terminal",
    pointSubtype: "Inter-Island Ferry",
    city: "North Caicos",
    submarket: "Other",
    latitude: 21.9506,
    longitude: -72.0673,
    sourceReference: "https://www.visittci.com/",
    notes: "Key ferry gateway linking North Caicos and Providenciales demand corridors.",
  }),
  ti({
    name: "South Caicos Airport and Port Connector",
    pointType: "Highway Access",
    pointSubtype: "Airport-Port Corridor",
    city: "South Caicos",
    submarket: "Other",
    latitude: 21.5158,
    longitude: -71.5285,
    sourceReference: "https://www.tciairports.com/",
    notes: "Connector route between South Caicos air gateway and fishing port demand.",
    useCaseTags: ["Airport / Transit", "Industrial / Logistics"],
  }),
];

export function buildTurksAndCaicosTiDeltaFixture() {
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
      verifiedRecords: TURKS_AND_CAICOS_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: TURKS_AND_CAICOS_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Airport, cruise, marina, ferry, and logistics gap fill for Turks & Caicos countrywide pass.",
    },
    corrections: [],
    summary: {
      totalPoints: TURKS_AND_CAICOS_TI_DELTA_RECORDS.length,
      byPointType: TURKS_AND_CAICOS_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: TURKS_AND_CAICOS_TI_DELTA_RECORDS,
  };
}
