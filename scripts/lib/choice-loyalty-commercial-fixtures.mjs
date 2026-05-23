/**
 * Loyalty & Commercial payloads for Choice Hotels International brands.
 * Percents are 0–100 (display scale); apply script converts to Airtable decimals.
 */

import { FDD_ITEM19 } from "./choice-fdd-item19.mjs";

export const PROGRAM = "Choice Privileges";

/** Apply FDD Item 19 loyalty / CRS (or proprietary) overrides when present. */
function withFdd(brandName, fields) {
  const fdd = FDD_ITEM19[brandName];
  if (!fdd) return fields;
  const out = { ...fields };
  if (fdd.loyaltyPct != null) {
    out["Typical % of Rooms from Loyalty (est.)"] = fdd.loyaltyPct;
  }
  const crs = fdd.enterprisePct ?? fdd.proprietaryPct;
  if (crs != null) {
    out["CRS Usage (% of bookings flowing through)"] = crs;
  }
  return out;
}

/** System-wide member base cited in Choice development materials (2024–2025). */
export const GLOBAL_MEMBERS_M = 70;
export const REGIONAL_MEMBERS_M = {
  na: 50,
  cala: 4,
  eu: 11,
  mea: 2,
  apac: 3,
};

const SEGMENT = {
  economy: {
    loyaltyRooms: 28,
    direct: 34,
    ota: 42,
    crs: 68,
    otaCommission: 15,
    loyaltyCostPerStay: 5,
    distributionCost: 16,
    websiteConv: 3.2,
    cac: 38,
  },
  midscale: {
    loyaltyRooms: 38,
    direct: 42,
    ota: 32,
    crs: 74,
    otaCommission: 15,
    loyaltyCostPerStay: 9,
    distributionCost: 20,
    websiteConv: 3.5,
    cac: 42,
  },
  upperMidscale: {
    loyaltyRooms: 42,
    direct: 45,
    ota: 30,
    crs: 76,
    otaCommission: 16,
    loyaltyCostPerStay: 11,
    distributionCost: 22,
    websiteConv: 3.6,
    cac: 45,
  },
  upscale: {
    loyaltyRooms: 46,
    direct: 48,
    ota: 28,
    crs: 80,
    otaCommission: 17,
    loyaltyCostPerStay: 14,
    distributionCost: 26,
    websiteConv: 3.8,
    cac: 52,
  },
  extendedStay: {
    loyaltyRooms: 45,
    direct: 40,
    ota: 26,
    crs: 72,
    otaCommission: 15,
    loyaltyCostPerStay: 12,
    distributionCost: 18,
    websiteConv: 3.4,
    cac: 40,
  },
};

function baseFields(segment, overrides = {}) {
  const s = { ...SEGMENT[segment], ...overrides };
  return {
    "Typical Loyalty Program Name": PROGRAM,
    "Typical % of Rooms from Loyalty (est.)": s.loyaltyRooms,
    "Typical Direct Booking % (est.)": s.direct,
    "Typical OTA Reliance % (est.)": s.ota,
    "Total Global Members (Approx. Millions)": GLOBAL_MEMBERS_M,
    "Regional Members - NA (Millions)": REGIONAL_MEMBERS_M.na,
    "Regional Members - CALA (Millions)": REGIONAL_MEMBERS_M.cala,
    "Regional Members - EU (Millions)": REGIONAL_MEMBERS_M.eu,
    "Regional Members - MEA (Millions)": REGIONAL_MEMBERS_M.mea,
    "Regional Members - APAC (Millions)": REGIONAL_MEMBERS_M.apac,
    "Loyalty Program Cost per Stay (Approximate)": s.loyaltyCostPerStay,
    "OTA Commission (Typical % of Reservation)": s.otaCommission,
    "CRS Usage (% of bookings flowing through)": s.crs,
    "Distribution Cost (Per Reservation)": s.distributionCost,
    "Website/App Conv. Rates (%)": s.websiteConv,
    "Avg. Cost of Cust. Acquisition": s.cac,
  };
}

/** @type {Record<string, ReturnType<typeof baseFields>>} */
export const BRAND_LOYALTY = {
  "Ascend Hotel Collection": withFdd(
    "Ascend Hotel Collection",
    baseFields("upscale", { direct: 44 })
  ),
  "Cambria Hotels": withFdd(
    "Cambria Hotels",
    baseFields("upscale", { direct: 46, ota: 24 })
  ),
  Clarion: withFdd("Clarion", baseFields("midscale", { direct: 40, ota: 34 })),
  "Clarion Pointe": withFdd("Clarion Pointe", baseFields("midscale")),
  "Comfort Inn & Suites": withFdd("Comfort Inn & Suites", baseFields("upperMidscale")),
  "Country Inn & Suites by Radisson (Choice)": withFdd(
    "Country Inn & Suites by Radisson (Choice)",
    baseFields("upperMidscale")
  ),
  "Econo Lodge": withFdd("Econo Lodge", baseFields("economy")),
  "Everhome Suites": baseFields("extendedStay", { loyaltyCostPerStay: 10 }),
  "MainStay Suites": withFdd(
    "MainStay Suites",
    baseFields("extendedStay", { loyaltyCostPerStay: 10 })
  ),
  "Park Inn by Radisson (Choice)": withFdd(
    "Park Inn by Radisson (Choice)",
    baseFields("upperMidscale")
  ),
  "Park Plaza (Choice)": baseFields("upscale"),
  "Quality Inn": withFdd("Quality Inn", baseFields("midscale")),
  "Radisson (Choice)": withFdd("Radisson (Choice)", baseFields("upscale")),
  "Radisson Blu (Choice)": withFdd("Radisson Blu (Choice)", baseFields("upscale")),
  "Radisson Collection  (Choice)": baseFields("upscale", { loyaltyRooms: 48 }),
  "Radisson Individual (Choice)": baseFields("upscale", { loyaltyRooms: 42, direct: 46 }),
  "Radisson Inn & Suites": baseFields("upperMidscale"),
  "Radisson RED  (Choice)": baseFields("upscale", { loyaltyRooms: 40, direct: 44, ota: 30 }),
  "Rodeway Inn": withFdd("Rodeway Inn", baseFields("economy")),
  "Sleep Inn": withFdd("Sleep Inn", baseFields("midscale")),
  "Suburban Studios": withFdd(
    "Suburban Studios",
    baseFields("extendedStay", { loyaltyCostPerStay: 9 })
  ),
  "WoodSpring Suites": baseFields("extendedStay", {
    loyaltyRooms: 45,
    loyaltyCostPerStay: 9,
  }),
};

export const TARGET_BRANDS = Object.keys(BRAND_LOYALTY);

export function buildLoyaltyFieldsForBrand(brandName) {
  return BRAND_LOYALTY[brandName] || null;
}
