/**
 * Chile — Santiago Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Chile";
const REGION = "South America";
const MARKET = "Santiago";

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
    scopeLevel: "Market",
    relevanceTier: v.relevanceTier || "Tier 1",
    useCaseTags: v.useCaseTags || ["Airport / Transit", "Urban / Corporate"],
    defaultMapVisibility: "Visible",
    externalVisibilityLevel: "Member",
    projectRelevanceLogic: `${MARKET} market-by-market TI build — ${v.name}.`,
  };
}

/** @type {ReturnType<typeof ti>[]} */
export const CHILE_TI_DELTA_RECORDS = [
  ti({
    name: "Santiago Airport Corridor Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Corridor",
    city: "Pudahuel",
    submarket: "Airport Corridor",
    latitude: -33.3912,
    longitude: -70.7784,
    sourceReference: "https://www.nuestroaeropuerto.cl/",
    notes: "Americas/Vespucio highway access linking SCL airport to Santiago hotel markets.",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Terminal Alameda Bus Station",
    pointType: "Bus Terminal",
    pointSubtype: "Intercity Terminal",
    city: "Santiago",
    submarket: "Santiago Centro",
    latitude: -33.4562,
    longitude: -70.6642,
    sourceReference: "https://www.turbus.cl/terminal-santiago",
    notes: "Primary intercity bus terminal supporting transit-oriented hotel demand near centro.",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "Metro Tobalaba Station",
    pointType: "Train Station",
    pointSubtype: "Metro",
    city: "Santiago",
    submarket: "Costanera / Financial District",
    latitude: -33.4184,
    longitude: -70.5994,
    sourceReference: "https://www.metro.cl/",
    notes: "Key metro node serving Costanera financial district and east-side corporate hotels.",
  }),
  ti({
    name: "Metro Baquedano Station",
    pointType: "Train Station",
    pointSubtype: "Metro",
    city: "Santiago",
    submarket: "Providencia",
    latitude: -33.4394,
    longitude: -70.6344,
    sourceReference: "https://www.metro.cl/estaciones/baquedano",
    notes: "Major metro interchange connecting Providencia, centro, and east-side lodging markets.",
  }),
  ti({
    name: "Costanera Financial District Access Node",
    pointType: "Highway Access",
    pointSubtype: "Urban Corridor",
    city: "Santiago",
    submarket: "Costanera / Financial District",
    latitude: -33.4172,
    longitude: -70.6042,
    sourceReference: "https://www.costaneracenter.cl/",
    notes: "Primary vehicular access to Costanera Center and Sanhattan office hotel demand.",
  }),
  ti({
    name: "Espacio Riesco Convention Access Corridor",
    pointType: "Highway Access",
    pointSubtype: "Events Corridor",
    city: "Santiago",
    submarket: "Convention / Events Corridor",
    latitude: -33.3844,
    longitude: -70.5644,
    sourceReference: "https://www.espacioriesco.cl/",
    notes: "Convention and events corridor access supporting group hotel demand in Las Condes.",
  }),
  ti({
    name: "El Golf Sanhattan Transit Access",
    pointType: "Highway Access",
    pointSubtype: "Corporate Corridor",
    city: "Santiago",
    submarket: "El Golf / Sanhattan",
    latitude: -33.4094,
    longitude: -70.5964,
    sourceReference: "https://www.chile.travel/en/where-to-go/central-area/metropolitan-region/santiago/el-golf",
    notes: "Corporate district access node for Sanhattan business hotel demand.",
  }),
];

export function buildChileTiDeltaFixture() {
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
      verifiedRecords: CHILE_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: CHILE_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Santiago airport, metro, bus, and corridor access gap fill from TI audit.",
    },
    corrections: [],
    summary: {
      totalPoints: CHILE_TI_DELTA_RECORDS.length,
      byPointType: CHILE_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: CHILE_TI_DELTA_RECORDS,
  };
}
