/**
 * Default radar / demand field values by Demand Anchor Point Type.
 */

import {
  RADAR_CATEGORY_DEMAND_ANCHORS,
  POINT_TYPE_TO_MAP_ICON,
  DEMAND_ANCHORS_LAYER_NAME,
} from "./airtable-demand-anchors-fields.js";

/**
 * @param {string} pointType
 * @returns {Partial<import('./normalize-demand-anchor.js').DemandAnchorPointInput>}
 */
export function getPointTypeDefaults(pointType) {
  const type = String(pointType || "").trim();
  const base = {
    radarCategory: RADAR_CATEGORY_DEMAND_ANCHORS,
    mapLayer: DEMAND_ANCHORS_LAYER_NAME,
    mapIconType: POINT_TYPE_TO_MAP_ICON[type] || null,
    includeOnRadarMap: true,
  };

  switch (type) {
    case "Convention Center":
      return {
        ...base,
        mapIconType: "Event",
        demandSegment: "Group / Event",
        demandRelevance: "High",
        demandPattern: ["Group", "Event-Based", "Weekday", "Weekend", "Seasonal"],
        relevantHotelTypes: ["Full-Service", "Upper-Upscale", "Lifestyle", "Select-Service"],
        hotelDemandRationale:
          "Supports group demand, event compression, meeting-related room nights, and brand/operator evaluation for meeting-oriented hotels.",
      };
    case "Medical Campus":
      return {
        ...base,
        mapIconType: "Medical",
        demandSegment: "Medical",
        demandRelevance: "High",
        demandPattern: ["Weekday", "Year-Round", "Extended-Stay", "Transient"],
        relevantHotelTypes: ["Extended-Stay", "Select-Service", "Midscale", "Upper-Midscale"],
        hotelDemandRationale:
          "Supports patient, family, visiting specialist, institutional, and extended-stay hotel demand.",
      };
    case "University / College":
      return {
        ...base,
        mapIconType: "Education",
        demandSegment: "Education",
        demandRelevance: "Medium",
        demandPattern: ["Seasonal", "Event-Based", "Weekend", "Year-Round"],
        relevantHotelTypes: ["Select-Service", "Extended-Stay", "Midscale", "Upper-Midscale"],
        hotelDemandRationale:
          "Supports visiting family, academic travel, sports, conferences, graduations, and seasonal compression.",
      };
    case "Sports Venue":
      return {
        ...base,
        mapIconType: "Sports",
        demandSegment: "Group / Event",
        demandRelevance: "Medium",
        demandPattern: ["Event-Based", "Weekend", "Seasonal", "Group"],
        relevantHotelTypes: ["Select-Service", "Full-Service", "Lifestyle", "Upper-Midscale"],
        hotelDemandRationale:
          "Supports event-driven demand, compression nights, team travel, concerts, and sports-related hotel stays.",
      };
    case "Entertainment District":
      return {
        ...base,
        mapIconType: "Entertainment",
        demandSegment: "Leisure",
        demandRelevance: "High",
        demandPattern: ["Weekend", "Leisure", "Year-Round", "Seasonal"],
        relevantHotelTypes: ["Lifestyle", "Full-Service", "Upscale", "Upper-Upscale", "Select-Service"],
        hotelDemandRationale:
          "Supports nightlife, dining, entertainment, weekend leisure, and experience-driven hotel demand.",
      };
    case "Tourist Attraction":
      return {
        ...base,
        mapIconType: "Attraction",
        demandSegment: "Leisure",
        demandRelevance: "Medium",
        demandPattern: ["Leisure", "Seasonal", "Weekend", "Year-Round"],
        relevantHotelTypes: ["Resort", "Lifestyle", "Select-Service", "Full-Service", "Upscale"],
        hotelDemandRationale:
          "Supports visitor demand, sightseeing, leisure trips, and tourism-led hotel positioning.",
      };
    case "Beach / Waterfront":
      return {
        ...base,
        mapIconType: "Beach",
        demandSegment: "Leisure",
        demandRelevance: "High",
        demandPattern: ["Leisure", "Weekend", "Seasonal", "Year-Round"],
        relevantHotelTypes: ["Resort", "Lifestyle", "Luxury", "Upper-Upscale", "Marina / Waterfront"],
        hotelDemandRationale:
          "Supports leisure, resort, waterfront, and experience-driven demand, often influencing rate potential and brand positioning.",
      };
    case "Business District":
      return {
        ...base,
        mapIconType: "Business",
        demandSegment: "Corporate",
        demandRelevance: "High",
        demandPattern: ["Weekday", "Business", "Year-Round", "Transient"],
        relevantHotelTypes: ["Select-Service", "Full-Service", "Extended-Stay", "Upscale", "Upper-Upscale"],
        hotelDemandRationale:
          "Supports weekday corporate demand, negotiated accounts, meetings, and business transient room nights.",
      };
    case "Industrial / Logistics Zone":
      return {
        ...base,
        mapIconType: "Industrial",
        demandSegment: "Industrial",
        demandRelevance: "Medium",
        demandPattern: ["Weekday", "Extended-Stay", "Project-Based", "Crew"],
        relevantHotelTypes: ["Extended-Stay", "Midscale", "Select-Service", "Roadside"],
        hotelDemandRationale:
          "Supports crew, contractor, project, logistics, and industrial-related lodging demand.",
      };
    case "Government / Civic Center":
      return {
        ...base,
        mapIconType: "Government",
        demandSegment: "Government",
        demandRelevance: "Medium",
        demandPattern: ["Weekday", "Year-Round", "Business", "Transient"],
        relevantHotelTypes: ["Select-Service", "Full-Service", "Extended-Stay", "Midscale"],
        hotelDemandRationale:
          "Supports public-sector, legal, administrative, diplomatic, and institutional travel demand.",
      };
    case "Mixed-Use Development":
      return {
        ...base,
        mapIconType: "Mixed-Use",
        demandSegment: "Mixed-Use",
        demandRelevance: "Medium",
        demandPattern: ["Weekday", "Weekend", "Leisure", "Business", "Year-Round"],
        relevantHotelTypes: ["Lifestyle", "Select-Service", "Full-Service", "Upscale", "Mixed-Use"],
        hotelDemandRationale:
          "Supports blended demand from retail, office, residential, entertainment, and visitor activity.",
      };
    case "Future Growth Node":
      return {
        ...base,
        mapIconType: "Growth Node",
        demandSegment: "Future Growth",
        demandRelevance: "Medium",
        demandPattern: ["Project-Based", "Business", "Leisure"],
        relevantHotelTypes: ["Select-Service", "Full-Service", "Lifestyle", "Mixed-Use", "Extended-Stay"],
        hotelDemandRationale:
          "Indicates future demand potential tied to planned infrastructure, mixed-use development, tourism zones, or major real estate growth.",
      };
    default:
      return base;
  }
}

/**
 * @param {string} pointType
 * @param {string} [hint]
 */
export function inferPointSubtype(pointType, hint) {
  const h = String(hint || "").trim();
  if (h) return h;
  const type = String(pointType || "").trim();
  if (type === "Convention Center") return "Convention Facility";
  if (type === "Medical Campus") return "Hospital / Medical District";
  if (type === "University / College") return "University Campus";
  if (type === "Sports Venue") return "Arena / Stadium";
  if (type === "Entertainment District") return "Entertainment Zone";
  if (type === "Tourist Attraction") return "Visitor Attraction";
  if (type === "Beach / Waterfront") return "Beach / Marina";
  if (type === "Business District") return "CBD / Office Core";
  if (type === "Industrial / Logistics Zone") return "Industrial Park";
  if (type === "Government / Civic Center") return "Civic / Government";
  if (type === "Mixed-Use Development") return "Mixed-Use District";
  if (type === "Future Growth Node") return "Planned Growth Area";
  return "Unknown";
}

/**
 * @param {Partial<import('./normalize-demand-anchor.js').DemandAnchorPointInput>} input
 */
export function applyPointTypeDefaults(input) {
  const pointType = String(input?.pointType || input?.type || "").trim();
  const defaults = getPointTypeDefaults(pointType);
  return {
    ...defaults,
    ...input,
    pointType: pointType || defaults.pointType || "",
    pointSubtype:
      input?.pointSubtype ||
      inferPointSubtype(pointType, input?.pointSubtype) ||
      defaults.pointSubtype ||
      "",
    radarCategory: input?.radarCategory || defaults.radarCategory,
    mapLayer: input?.mapLayer || defaults.mapLayer,
    mapIconType: input?.mapIconType || defaults.mapIconType,
  };
}
