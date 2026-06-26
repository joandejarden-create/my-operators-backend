/**
 * CALA Radar Build Plans — Airtable field map.
 */

export const RADAR_BUILD_PLANS_TABLE =
  process.env.AIRTABLE_TABLE_RADAR_BUILD_PLANS || "CALA Radar Build Plans";

export const RADAR_BUILD_PLANS_FIELDS = {
  country: "Country",
  region: "Region",
  buildStrategy: "Build Strategy",
  priorityTier: "Priority Tier",
  buildStatus: "Build Status",
  targetDemandAnchors: "Target Demand Anchors",
  currentDemandAnchors: "Current Demand Anchors",
  targetTravelInfrastructure: "Target Travel Infrastructure",
  currentTravelInfrastructure: "Current Travel Infrastructure",
  targetTotalRadarPoints: "Target Total Radar Points",
  currentTotalRadarPoints: "Current Total Radar Points",
  submarketsCorridors: "Submarkets / Corridors",
  primaryHotelDemandProfile: "Primary Hotel Demand Profile",
  sourceCoveragePct: "Source Coverage %",
  coordinateCoveragePct: "Coordinate Coverage %",
  dataConfidenceMix: "Data Confidence Mix",
  lastBuildDate: "Last Build Date",
  lastQaDate: "Last QA Date",
  nextRecommendedAction: "Next Recommended Action",
  notes: "Notes",
  recommendedBuildSequence: "Recommended Build Sequence",
  nextBuildMarket: "Next Build Market",
  buildApproachNotes: "Build Approach Notes",
  firstPassTargetDescription: "First-Pass Target Description",
};

export const PRIORITY_TIER_OPTIONS = ["Tier 1", "Tier 2", "Tier 3", "Future"];

export const BUILD_STRATEGY_OPTIONS = [
  "Island / Compact Countrywide",
  "Corridor-Based Resort Country",
  "Large Country / Market-by-Market",
];

export const BUILD_STATUS_OPTIONS = [
  "Not Started",
  "Planned",
  "Seeded",
  "Market Ready",
  "Deal Ready",
  "Intelligence Ready",
  "Needs Review",
];

export const PRIMARY_HOTEL_DEMAND_PROFILE_OPTIONS = [
  "Resort / Leisure",
  "Luxury / Resort",
  "Urban / Corporate",
  "Airport / Transit",
  "Cruise / Port",
  "Industrial / Logistics",
  "Government / Institutional",
  "Medical / Education",
  "Mixed-Use / Growth",
  "Nature / Eco-Tourism",
  "Group / Convention",
  "Heritage / Cultural Tourism",
  "Future Growth",
];
