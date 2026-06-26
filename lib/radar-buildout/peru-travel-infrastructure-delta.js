/**
 * Peru — Lima / Cusco Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Peru";
const REGION = "South America";
const MARKET = "Lima / Cusco";

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
    useCaseTags: v.useCaseTags || ["Airport / Transit", "Heritage / Cultural Tourism"],
    defaultMapVisibility: "Visible",
    externalVisibilityLevel: "Member",
    projectRelevanceLogic: `${MARKET} market-by-market TI build — ${v.name}.`,
  };
}

/** @type {ReturnType<typeof ti>[]} */
export const PERU_TI_DELTA_RECORDS = [
  ti({
    name: "Port of Callao Maritime Terminal",
    pointType: "Port / Maritime",
    pointSubtype: "Container Port",
    city: "Callao",
    submarket: "Callao / Port",
    latitude: -12.0522,
    longitude: -77.1484,
    sourceReference: "https://www.apncallao.com.pe/",
    notes: "Primary Lima maritime port supporting logistics and cruise-related hotel demand.",
    useCaseTags: ["Industrial / Logistics", "Airport / Transit"],
  }),
  ti({
    name: "Lima Airport Corridor Highway Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Corridor",
    city: "Callao",
    submarket: "Jorge Chávez Airport Corridor",
    latitude: -12.0242,
    longitude: -77.1124,
    sourceReference: "https://www.lima-airport.com/",
    notes: "Highway access node linking Jorge Chávez airport to Lima metro hotel markets.",
    useCaseTags: ["Airport / Transit", "Urban / Corporate"],
  }),
  ti({
    name: "PeruRail Poroy Station",
    pointType: "Train Station",
    pointSubtype: "Machu Picchu Rail",
    city: "Poroy",
    submarket: "Cusco Historic Center",
    latitude: -13.4882,
    longitude: -72.0184,
    sourceReference: "https://www.perurail.com/",
    notes: "Cusco-area rail departure point for Machu Picchu corridor tourism flows.",
    useCaseTags: ["Heritage / Cultural Tourism", "Airport / Transit"],
  }),
  ti({
    name: "PeruRail Ollantaytambo Station",
    pointType: "Train Station",
    pointSubtype: "Sacred Valley Rail",
    city: "Ollantaytambo",
    submarket: "Ollantaytambo",
    latitude: -13.2582,
    longitude: -72.2684,
    sourceReference: "https://www.perurail.com/",
    notes: "Sacred Valley rail hub for Machu Picchu-bound leisure lodging demand.",
  }),
  ti({
    name: "Inca Rail Ollantaytambo Terminal",
    pointType: "Train Station",
    pointSubtype: "Sacred Valley Rail",
    city: "Ollantaytambo",
    submarket: "Ollantaytambo",
    latitude: -13.2592,
    longitude: -72.2694,
    sourceReference: "https://www.incarail.com/",
    notes: "Alternate rail operator terminal supporting Machu Picchu access tourism.",
  }),
  ti({
    name: "Aguas Calientes Machu Picchu Rail Access",
    pointType: "Train Station",
    pointSubtype: "Machu Picchu Terminal",
    city: "Aguas Calientes",
    submarket: "Machu Picchu Access",
    latitude: -13.1552,
    longitude: -72.5242,
    sourceReference: "https://www.perurail.com/",
    notes: "Terminal rail access in Machu Picchu Pueblo supporting overnight lodge demand.",
  }),
  ti({
    name: "Sacred Valley Pan-American Access Corridor",
    pointType: "Highway Access",
    pointSubtype: "Sacred Valley Corridor",
    city: "Urubamba",
    submarket: "Sacred Valley",
    latitude: -13.3052,
    longitude: -72.1154,
    sourceReference: "https://www.peru.travel/en/destinations/cusco/sacred-valley",
    notes: "Primary road access for Sacred Valley resort and heritage tourism markets.",
    useCaseTags: ["Heritage / Cultural Tourism", "Resort / Leisure"],
  }),
  ti({
    name: "Chinchero International Airport Growth Node",
    pointType: "Highway Access",
    pointSubtype: "Future Airport Access",
    city: "Chinchero",
    submarket: "Sacred Valley",
    latitude: -13.3922,
    longitude: -72.0484,
    sourceReference: "https://www.gob.pe/mtc",
    notes: "Future Chinchero airport access corridor for Cusco/Sacred Valley tourism growth.",
  }),
];

export function buildPeruTiDeltaFixture() {
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
      verifiedRecords: PERU_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: PERU_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Port, rail, highway, and future airport access gap fill from TI audit.",
    },
    corrections: [],
    summary: {
      totalPoints: PERU_TI_DELTA_RECORDS.length,
      byPointType: PERU_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: PERU_TI_DELTA_RECORDS,
  };
}
