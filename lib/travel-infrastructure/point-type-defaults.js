/**
 * Default radar / demand field values by Point Type.
 */

import {
  RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
  POINT_TYPE_TO_MAP_ICON,
} from "./airtable-travel-infrastructure-fields.js";

/** @typedef {import('./normalize-radar-map-point.js').RadarMapPointInput} RadarMapPointInput */

/**
 * @param {string} pointType
 * @returns {Partial<RadarMapPointInput>}
 */
export function getPointTypeDefaults(pointType) {
  const type = String(pointType || "").trim();
  const base = {
    radarCategory: RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
    mapLayer: RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
    mapIconType: POINT_TYPE_TO_MAP_ICON[type] || null,
    includeOnRadarMap: true,
  };

  switch (type) {
    case "Airport":
      return {
        ...base,
        mapIconType: "Airport",
        demandRelevance: "High",
        demandPattern: [
          "Transient",
          "Year-Round",
          "Weekday",
          "Weekend",
          "Business",
          "Leisure",
        ],
        relevantHotelTypes: [
          "Airport",
          "Select-Service",
          "Full-Service",
          "Extended-Stay",
          "Upscale",
        ],
        hotelDemandRationale:
          "Supports air access, transient demand, business and leisure travel, crew demand, and short-stay demand depending on flight volume and route mix.",
      };
    case "Cruise Port":
      return {
        ...base,
        mapIconType: "Cruise Port",
        demandRelevance: "High",
        demandPattern: ["Leisure", "Group", "Seasonal", "Weekend", "Transient"],
        relevantHotelTypes: [
          "Full-Service",
          "Resort",
          "Lifestyle",
          "Upper-Upscale",
          "Select-Service",
        ],
        hotelDemandRationale:
          "Supports pre- and post-cruise hotel stays, leisure demand, group movement, and compression around sailing schedules.",
      };
    case "Train Station":
      return {
        ...base,
        mapIconType: "Train",
        demandRelevance: "Medium",
        demandPattern: ["Transient", "Weekday", "Weekend", "Business", "Leisure"],
        relevantHotelTypes: ["Urban", "Select-Service", "Full-Service", "Upscale", "Midscale"],
        hotelDemandRationale:
          "Improves market accessibility and can support business, leisure, and short-stay demand, especially in urban or connected markets.",
      };
    case "Highway Access":
      return {
        ...base,
        mapIconType: "Highway",
        demandRelevance: "Medium",
        demandPattern: ["Drive-To", "Transient", "Weekday", "Weekend"],
        relevantHotelTypes: [
          "Roadside",
          "Select-Service",
          "Midscale",
          "Upper-Midscale",
          "Extended-Stay",
        ],
        hotelDemandRationale:
          "Supports drive-to demand, regional business travel, transient stays, and select-service or midscale lodging demand.",
      };
    case "Bus Terminal":
      return {
        ...base,
        mapIconType: "Bus",
        demandRelevance: "Low",
        demandPattern: ["Transient", "Price-Sensitive", "Weekday", "Weekend"],
        relevantHotelTypes: ["Economy", "Midscale", "Select-Service"],
        hotelDemandRationale:
          "May support price-sensitive transient demand, especially in urban or intercity transport nodes.",
      };
    case "Ferry Terminal":
      return {
        ...base,
        mapIconType: "Ferry",
        demandRelevance: "Medium",
        demandPattern: ["Leisure", "Seasonal", "Transient", "Weekend"],
        relevantHotelTypes: ["Resort", "Select-Service", "Midscale", "Marina / Waterfront"],
        hotelDemandRationale:
          "Supports island access, weekend leisure, transfer demand, and pre/post-ferry stays.",
      };
    case "Port / Maritime":
      return {
        ...base,
        mapIconType: "Port",
        demandRelevance: "Medium",
        demandPattern: ["Crew", "Industrial", "Business", "Extended-Stay", "Transient"],
        relevantHotelTypes: [
          "Extended-Stay",
          "Midscale",
          "Select-Service",
          "Marina / Waterfront",
        ],
        hotelDemandRationale:
          "Supports maritime, crew, logistics, industrial, marina, and waterfront-related hotel demand depending on the port type.",
      };
    case "Convention Center":
      return {
        ...base,
        mapIconType: "Convention",
        demandRelevance: "High",
        demandPattern: ["Group", "Weekday", "Weekend", "Seasonal", "Transient"],
        relevantHotelTypes: ["Full-Service", "Upscale", "Upper-Upscale", "Select-Service"],
        hotelDemandRationale:
          "Supports group and event demand, compression around major conventions, and weekday/weekend meeting travel.",
      };
    default:
      return base;
  }
}

/**
 * Infer Point Subtype from Point Type when missing.
 * @param {string} pointType
 * @param {string} [pointSubtype]
 * @param {string} [infrastructureRole]
 * @param {string} [airportType]
 */
export function inferPointSubtype(pointType, pointSubtype, infrastructureRole, airportType) {
  const explicit = String(pointSubtype || "").trim();
  if (explicit) return explicit;

  const role = String(infrastructureRole || "").trim();
  if (role.includes("International Hub")) return "International Airport";
  if (role.includes("Domestic") || role.includes("Regional") || role.includes("Secondary")) {
    return "Regional Airport";
  }
  if (role.includes("Cruise")) return "Cruise Terminal";

  const at = String(airportType || "").trim();
  if (at.includes("Large")) return "International Airport";
  if (at.includes("Medium") || at.includes("Small")) return "Regional Airport";

  switch (String(pointType || "").trim()) {
    case "Airport":
      return "Regional Airport";
    case "Cruise Port":
      return "Cruise Terminal";
    case "Train Station":
      return "Rail Hub";
    case "Highway Access":
      return "Highway Exit";
    case "Bus Terminal":
      return "Intercity Bus Terminal";
    case "Ferry Terminal":
      return "Ferry Terminal";
    case "Port / Maritime":
      return "Cargo Port";
    case "Convention Center":
      return "Convention Center";
    default:
      return "Unknown";
  }
}

/**
 * Apply defaults to import/create payload (does not overwrite provided values).
 * @param {Record<string, unknown>} input
 */
export function applyPointTypeDefaults(input) {
  const out = { ...(input || {}) };
  const pointType =
    out.pointType || out.type || out.PointType || out.Type || "";
  const defaults = getPointTypeDefaults(String(pointType));
  for (const [key, val] of Object.entries(defaults)) {
    if (out[key] == null || out[key] === "" || (Array.isArray(out[key]) && !out[key].length)) {
      out[key] = val;
    }
  }
  if (!out.pointSubtype) {
    out.pointSubtype = inferPointSubtype(
      pointType,
      out.pointSubtype,
      out.infrastructureRole,
      out.airportType
    );
  }
  return out;
}
