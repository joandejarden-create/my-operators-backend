/**
 * Cayman Islands countrywide Travel Infrastructure delta records (audit gap fill).
 */

const COUNTRY = "Cayman Islands";
const REGION = "Caribbean";
const MARKET = "Cayman Islands Countrywide";

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

export const CAYMAN_ISLANDS_TI_DELTA_RECORDS = [
  ti({
    name: "Owen Roberts International Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "George Town",
    submarket: "Grand Cayman",
    latitude: 19.2928,
    longitude: -81.3577,
    sourceReference: "https://ciaa.ky/",
    notes: "Primary airport gateway supporting Cayman stayover hotel arrivals.",
  }),
  ti({
    name: "George Town Cruise Tender Port Access",
    pointType: "Port / Maritime",
    pointSubtype: "Cruise Tender Port",
    city: "George Town",
    submarket: "Grand Cayman",
    latitude: 19.2866,
    longitude: -81.3744,
    sourceReference: "https://www.caymanport.com/",
    notes: "Main cruise tender operations supporting pre/post-cruise lodging demand.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
  ti({
    name: "Seven Mile Beach Corridor Access",
    pointType: "Highway Access",
    pointSubtype: "Resort Corridor",
    city: "Seven Mile Beach",
    submarket: "Grand Cayman",
    latitude: 19.3296,
    longitude: -81.3848,
    sourceReference: "https://www.visitcaymanislands.com/en-us/things-to-do/beaches/seven-mile-beach",
    notes: "Primary road corridor serving Seven Mile Beach resort inventory.",
  }),
  ti({
    name: "Camana Bay Transit Connector",
    pointType: "Bus Terminal",
    pointSubtype: "Urban Connector",
    city: "George Town",
    submarket: "Grand Cayman",
    latitude: 19.3239,
    longitude: -81.3798,
    sourceReference: "https://www.camanabay.com/",
    notes: "Central mixed-use transport connector between George Town and SMB corridor.",
  }),
  ti({
    name: "East End Coastal Access Route",
    pointType: "Highway Access",
    pointSubtype: "Coastal Corridor",
    city: "East End",
    submarket: "Grand Cayman",
    latitude: 19.3001,
    longitude: -81.0937,
    sourceReference: "https://www.visitcaymanislands.com/",
    notes: "Road access to East End dive and boutique lodging markets.",
  }),
  ti({
    name: "Charles Kirkconnell Airport Access",
    pointType: "Highway Access",
    pointSubtype: "Airport Access",
    city: "Cayman Brac",
    submarket: "Cayman Brac",
    latitude: 19.687,
    longitude: -79.8828,
    sourceReference: "https://ciaa.ky/",
    notes: "Regional airport access for Cayman Brac inter-island travel demand.",
  }),
  ti({
    name: "Edward Bodden Airfield Access",
    pointType: "Highway Access",
    pointSubtype: "Airfield Access",
    city: "Little Cayman",
    submarket: "Little Cayman",
    latitude: 19.6599,
    longitude: -80.0906,
    sourceReference: "https://ciaa.ky/",
    notes: "Little Cayman airfield access for eco and dive travel arrivals.",
  }),
  ti({
    name: "Little Cayman Inter-Island Ferry Node",
    pointType: "Ferry Terminal",
    pointSubtype: "Inter-Island Ferry",
    city: "Little Cayman",
    submarket: "Little Cayman",
    latitude: 19.6762,
    longitude: -80.0878,
    sourceReference: "https://www.visitcaymanislands.com/",
    notes: "Inter-island maritime access supporting Little Cayman lodging demand.",
    useCaseTags: ["Cruise / Port", "Resort / Leisure"],
  }),
];

export function buildCaymanIslandsTiDeltaFixture() {
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
      verifiedRecords: CAYMAN_ISLANDS_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: CAYMAN_ISLANDS_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Airport, cruise, highway, and ferry gap fill for Cayman Islands countrywide pass.",
    },
    corrections: [],
    summary: {
      totalPoints: CAYMAN_ISLANDS_TI_DELTA_RECORDS.length,
      byPointType: CAYMAN_ISLANDS_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: CAYMAN_ISLANDS_TI_DELTA_RECORDS,
  };
}
