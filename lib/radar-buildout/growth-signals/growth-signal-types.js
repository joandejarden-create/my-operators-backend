/**
 * CALA submarket growth signal taxonomy — Tier 2 early-entry indicators for owners/brands.
 * Metadata only; attach to submarkets or Future Growth Node anchors (by name reference).
 */

export const GROWTH_SIGNAL_TYPES = {
  air_seat_growth: {
    id: "air_seat_growth",
    label: "Air Seat Growth",
    buildImplication: "New fly-in resort corridors; airport-adjacent select-service / extended-stay",
    defaultHotelTypes: ["Select-Service", "Resort", "Extended-Stay", "Full-Service"],
    reviewCadenceMonths: 12,
  },
  cruise_turnaround_growth: {
    id: "cruise_turnaround_growth",
    label: "Cruise Call / Turnaround Growth",
    buildImplication: "Urban or port-adjacent select-service / full-service near terminal",
    defaultHotelTypes: ["Select-Service", "Full-Service", "Lifestyle", "Boutique"],
    reviewCadenceMonths: 12,
  },
  master_planned_community: {
    id: "master_planned_community",
    label: "Master-Planned Community Phase",
    buildImplication: "Anchor-tenant hotel timing before secondary phases fill",
    defaultHotelTypes: ["Resort", "Full-Service", "Lifestyle", "Mixed-Use"],
    reviewCadenceMonths: 18,
  },
  tourism_zone_expansion: {
    id: "tourism_zone_expansion",
    label: "Government Tourism Zone Expansion",
    buildImplication: "Entitlement-favored greenfield; early mover advantage in new zones",
    defaultHotelTypes: ["Resort", "Select-Service", "Full-Service", "Lifestyle"],
    reviewCadenceMonths: 24,
  },
  employer_free_zone_expansion: {
    id: "employer_free_zone_expansion",
    label: "Major Employer / Free Zone Expansion",
    buildImplication: "Extended-stay and select-service near employment / logistics nodes",
    defaultHotelTypes: ["Extended-Stay", "Select-Service", "Midscale", "Upper-Midscale"],
    reviewCadenceMonths: 18,
  },
  mice_capacity_growth: {
    id: "mice_capacity_growth",
    label: "Conference / MICE Capacity Growth",
    buildImplication: "Full-service and upper-upscale near convention / events nodes",
    defaultHotelTypes: ["Full-Service", "Upper-Upscale", "Convention", "Lifestyle"],
    reviewCadenceMonths: 18,
  },
};

export const GROWTH_SIGNAL_TYPE_IDS = Object.keys(GROWTH_SIGNAL_TYPES);

export const GROWTH_DIRECTION_OPTIONS = [
  "emerging",
  "accelerating",
  "stable",
  "declining",
  "unknown",
];

export const GROWTH_PROFILE_STATUS = {
  RESEARCHED: "researched",
  SKELETON: "skeleton",
  PLANNED: "planned",
};

export const EARLY_ENTRY_OPPORTUNITY = ["low", "medium", "medium-high", "high", "unknown"];

/**
 * @param {string} signalTypeId
 */
export function getGrowthSignalTypeMeta(signalTypeId) {
  return GROWTH_SIGNAL_TYPES[String(signalTypeId || "").trim()] || null;
}
