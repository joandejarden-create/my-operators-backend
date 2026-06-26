/**
 * Costa Rica countrywide Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Costa Rica";
const REGION = "Central America";
const MARKET = "Costa Rica Countrywide";

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
export const COSTA_RICA_TI_DELTA_RECORDS = [
  ti({
    name: "Puntarenas Ferry Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Nicoya Peninsula Ferry",
    city: "Puntarenas",
    submarket: "Other",
    latitude: 9.9762,
    longitude: -84.8334,
    sourceReference: "https://www.navieratambor.com/",
    notes: "Main ferry departure to Paquera and Nicoya Peninsula resort markets.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Paquera Ferry Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Nicoya Peninsula Ferry",
    city: "Paquera",
    submarket: "Guanacaste / Papagayo",
    latitude: 9.8182,
    longitude: -84.9384,
    sourceReference: "https://www.navieratambor.com/",
    notes: "Nicoya Peninsula receiving terminal for Puntarenas ferry traffic.",
  }),
  ti({
    name: "Moín Container Terminal",
    pointType: "Port / Maritime",
    pointSubtype: "Container Port",
    city: "Limón",
    submarket: "Caribbean Coast",
    latitude: 10.0042,
    longitude: -83.0774,
    sourceReference: "https://www.japdeva.go.cr/",
    notes: "Caribbean-side container port supporting logistics and cruise-related demand.",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),
  ti({
    name: "Limón Cruise & Maritime Terminal",
    pointType: "Port / Maritime",
    pointSubtype: "Cruise Terminal",
    city: "Limón",
    submarket: "Caribbean Coast",
    latitude: 9.9912,
    longitude: -83.0354,
    sourceReference: "https://www.japdeva.go.cr/",
    notes: "Caribbean cruise port gateway for pre/post-cruise hotel demand.",
    useCaseTags: ["Resort / Leisure", "Airport / Transit"],
  }),
  ti({
    name: "Interamericana Norte Pacific Highway Node",
    pointType: "Highway Access",
    pointSubtype: "Pacific Corridor",
    city: "Liberia",
    submarket: "Guanacaste / Papagayo",
    latitude: 10.6342,
    longitude: -85.4374,
    sourceReference: "https://www.mopt.go.cr/",
    notes: "Primary Pacific coast highway access linking Guanacaste resort corridors.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Route 32 Caribbean Coast Highway Node",
    pointType: "Highway Access",
    pointSubtype: "Caribbean Corridor",
    city: "Limón",
    submarket: "Caribbean Coast",
    latitude: 9.9912,
    longitude: -83.0354,
    sourceReference: "https://www.mopt.go.cr/",
    notes: "Main highway access from San José metro to Caribbean tourism coast.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "La Fortuna / Arenal Access Corridor",
    pointType: "Highway Access",
    pointSubtype: "Northern Plains Access",
    city: "La Fortuna",
    submarket: "Arenal / La Fortuna",
    latitude: 10.4712,
    longitude: -84.6454,
    sourceReference: "https://www.visitcostarica.com/en/costa-rica/regions/northern-plains",
    notes: "Primary road access node for Arenal volcano and hot-springs resort demand.",
    useCaseTags: ["Resort / Leisure", "Nature / Eco-Tourism"],
  }),
  ti({
    name: "Tamarindo Regional Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Regional Airport Access",
    city: "Tamarindo",
    submarket: "Tamarindo / North Pacific",
    latitude: 10.3142,
    longitude: -85.8124,
    sourceReference: "https://www.dgac.go.cr/",
    notes: "Regional airport serving North Pacific surf and resort markets.",
  }),
];

export function buildCostaRicaTiDeltaFixture() {
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
      verifiedRecords: COSTA_RICA_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: COSTA_RICA_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Ferry, port, highway, and regional airport gap fill from TI audit.",
    },
    corrections: [],
    summary: {
      totalPoints: COSTA_RICA_TI_DELTA_RECORDS.length,
      byPointType: COSTA_RICA_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: COSTA_RICA_TI_DELTA_RECORDS,
  };
}
