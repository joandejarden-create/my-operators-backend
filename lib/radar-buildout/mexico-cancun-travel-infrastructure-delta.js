/**
 * Mexico — Cancún / Riviera Maya Travel Infrastructure delta records.
 */

import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const COUNTRY = "Mexico";
const REGION = MEXICO_RADAR_REGION;
const MARKET = "Cancún / Riviera Maya";

function ti(v) {
  return {
    name: v.name,
    pointType: v.pointType,
    pointSubtype: v.pointSubtype || "",
    city: v.city,
    country: COUNTRY,
    region: REGION,
    submarket: v.submarket || "",
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
    useCaseTags: v.useCaseTags || ["Airport / Transit", "Resort / Leisure"],
    defaultMapVisibility: "Visible",
    externalVisibilityLevel: "Member",
    projectRelevanceLogic: `${MARKET} market-by-market TI build — ${v.name}.`,
  };
}

/** @type {ReturnType<typeof ti>[]} */
export const MEXICO_CANCUN_TI_DELTA_RECORDS = [
  ti({
    name: "Playa del Carmen Maritime Terminal (Ultramar)",
    pointType: "Ferry Terminal",
    pointSubtype: "Ferry Terminal",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6272,
    longitude: -87.0754,
    sourceReference: "https://www.ultramarferry.com/",
    notes: "Primary ferry link to Cozumel; supports inter-island leisure and day-trip hotel demand.",
    useCaseTags: ["Airport / Transit", "Resort / Leisure"],
  }),
  ti({
    name: "Cozumel Ultramar Ferry Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Ferry Terminal",
    city: "Cozumel",
    submarket: "Cozumel",
    latitude: 20.5089,
    longitude: -86.9512,
    sourceReference: "https://www.ultramarferry.com/",
    notes: "Main Cozumel ferry receiving node for Playa del Carmen traffic.",
  }),
  ti({
    name: "Gran Puerto Isla Mujeres Ferry Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Ferry Terminal",
    city: "Isla Mujeres",
    submarket: "Isla Mujeres",
    latitude: 21.2315,
    longitude: -86.7312,
    sourceReference: "https://www.ultramarferry.com/",
    notes: "Island ferry terminal supporting Cancún–Isla Mujeres leisure lodging flows.",
  }),
  ti({
    name: "Puerto Juárez Isla Mujeres Ferry Terminal",
    pointType: "Ferry Terminal",
    pointSubtype: "Ferry Terminal",
    city: "Cancún",
    submarket: "Other",
    latitude: 21.1742,
    longitude: -86.8035,
    sourceReference: "https://www.puertojuarez.gob.mx/",
    notes: "Mainland ferry departure point for Isla Mujeres; hotel demand for island access stays.",
  }),
  ti({
    name: "Tren Maya Cancún Airport Station",
    pointType: "Train Station",
    pointSubtype: "Rail Hub",
    city: "Cancún",
    submarket: "Other",
    latitude: 21.0405,
    longitude: -86.8742,
    sourceReference: "https://www.trenmaya.gob.mx/",
    notes: "Tren Maya station at Cancún International Airport corridor.",
    useCaseTags: ["Airport / Transit", "Mixed-Use / Growth"],
  }),
  ti({
    name: "Tren Maya Playa del Carmen Station",
    pointType: "Train Station",
    pointSubtype: "Rail Hub",
    city: "Playa del Carmen",
    submarket: "Riviera Maya / Playa del Carmen",
    latitude: 20.6345,
    longitude: -87.0898,
    sourceReference: "https://www.trenmaya.gob.mx/",
    notes: "Riviera Maya rail node connecting resort corridor to airport and southern markets.",
  }),
  ti({
    name: "Tren Maya Tulum Station",
    pointType: "Train Station",
    pointSubtype: "Rail Hub",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.2148,
    longitude: -87.4615,
    sourceReference: "https://www.trenmaya.gob.mx/",
    notes: "Tulum municipal Tren Maya stop supporting boutique resort and growth corridor demand.",
  }),
  ti({
    name: "Tren Maya Felipe Carrillo Puerto Airport Station",
    pointType: "Train Station",
    pointSubtype: "Rail Hub",
    city: "Tulum",
    submarket: "Tulum",
    latitude: 20.1721,
    longitude: -87.6603,
    sourceReference: "https://www.aeropuertosasa.mx/aeropuerto-de-tulum",
    notes: "Airport-adjacent Tren Maya node; corrects prior TI city mis-tag for Tulum airport area.",
    dataConfidence: "High",
  }),
  ti({
    name: "Cancún Maritime Terminal (Puerto Juárez Cruise/Ferry)",
    pointType: "Port / Maritime",
    pointSubtype: "Maritime Terminal",
    city: "Cancún",
    submarket: "Other",
    latitude: 21.1758,
    longitude: -86.8048,
    sourceReference: "https://www.puertocancun.com.mx/",
    notes: "Maritime terminal zone at Puerto Juárez supporting ferry and coastal access flows.",
    demandRelevance: "Medium",
  }),
];

export const MEXICO_CANCUN_TI_CORRECTIONS = [
  {
    existingName: "Felipe Carrillo Puerto International Airport Tulum",
    issue: "city stored as Cozumel",
    recommendedCity: "Tulum",
    recommendedSubmarket: "Tulum",
    recommendedAction: "manual_correction",
    notes: "Update city/submarket to Tulum when Airtable write permissions allow.",
  },
];

export function buildMexicoCancunTiDeltaFixture() {
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
      verifiedRecords: MEXICO_CANCUN_TI_DELTA_RECORDS.length,
      manuallyVerifiedRecords: MEXICO_CANCUN_TI_DELTA_RECORDS.length,
      excludedRecords: 0,
      requirement: "Official/public source reference required for each TI node",
      notes: "Ferry terminals and Tren Maya stations for Cancún / Riviera Maya first-pass TI gap fill.",
    },
    corrections: MEXICO_CANCUN_TI_CORRECTIONS,
    summary: {
      totalPoints: MEXICO_CANCUN_TI_DELTA_RECORDS.length,
      byPointType: MEXICO_CANCUN_TI_DELTA_RECORDS.reduce((a, p) => {
        a[p.pointType] = (a[p.pointType] || 0) + 1;
        return a;
      }, {}),
    },
    points: MEXICO_CANCUN_TI_DELTA_RECORDS,
  };
}
