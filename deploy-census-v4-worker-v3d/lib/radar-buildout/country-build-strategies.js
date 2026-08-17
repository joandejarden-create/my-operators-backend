/**
 * CALA Radar Buildout — country build strategy definitions.
 */

export const BUILD_STRATEGY_TYPES = {
  ISLAND_COUNTRYWIDE: "Island / Compact Countrywide",
  CORRIDOR_BASED: "Corridor-Based Resort Country",
  MARKET_BY_MARKET: "Large Country / Market-by-Market",
};

export const COVERAGE_STATUS = {
  NOT_STARTED: "Not Started",
  PLANNED: "Planned",
  SEEDED: "Seeded",
  MARKET_READY: "Market Ready",
  DEAL_READY: "Deal Ready",
  INTELLIGENCE_READY: "Intelligence Ready",
  NEEDS_REVIEW: "Needs Review",
};

/** @typedef {typeof BUILD_STRATEGY_TYPES[keyof typeof BUILD_STRATEGY_TYPES]} BuildStrategyType */

/**
 * @type {Record<string, object>}
 */
export const BUILD_STRATEGY_DEFINITIONS = {
  [BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE]: {
    id: "ISLAND_COUNTRYWIDE",
    label: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
    recommendedSubmarketApproach:
      "Define 6–10 island-wide corridors/submarkets; build countrywide in one pass when population < 5M and geography is compact.",
    targetDemandAnchors: { min: 40, max: 80, firstPass: { min: 30, max: 50 } },
    targetTravelInfrastructure: { min: 15, max: 30, firstPass: { min: 10, max: 20 } },
    targetTotalRadarPoints: { min: 55, max: 110, firstPass: { min: 40, max: 70 } },
    buildPhases: [
      "Define submarkets/corridors",
      "Seed capital + primary resort/airport anchors",
      "Countrywide demand anchor pass",
      "Travel infrastructure gap fill (ferry, highway, regional ports)",
      "Submarket tagging + QA",
      "Deal-ready density review",
    ],
    readinessCriteria: {
      seeded: { minTotalPoints: 10, requireSourceBacked: true },
      marketReady: { minTotalPoints: 40, maxTotalPoints: 80, minDemandAnchors: 30, minTravelInfra: 10 },
      dealReady: { minTotalPoints: 55, minDemandAnchors: 40, minTravelInfra: 15, requireSubmarketTagging: true },
      intelligenceReady: {
        minTotalPoints: 70,
        minDemandAnchors: 50,
        minPointTypes: 8,
        requireConfidenceMix: true,
        requireCleanDuplicates: true,
      },
    },
    exampleCountries: [
      "Puerto Rico",
      "Aruba",
      "Curaçao",
      "Barbados",
      "Cayman Islands",
      "Turks & Caicos",
      "Saint Lucia",
      "Antigua and Barbuda",
      "Grenada",
      "Saint Vincent and the Grenadines",
      "Dominica",
      "Saint Kitts and Nevis",
      "Trinidad and Tobago",
      "British Virgin Islands",
    ],
  },
  [BUILD_STRATEGY_TYPES.CORRIDOR_BASED]: {
    id: "CORRIDOR_BASED",
    label: BUILD_STRATEGY_TYPES.CORRIDOR_BASED,
    recommendedSubmarketApproach:
      "Build by resort/corridor clusters; prioritize beach gateways, cruise ports, and metro anchors before secondary corridors.",
    targetDemandAnchors: { min: 50, max: 140, firstPass: { min: 50, max: 70 } },
    targetTravelInfrastructure: { min: 15, max: 45, firstPass: { min: 15, max: 25 } },
    targetTotalRadarPoints: { min: 65, max: 185, firstPass: { min: 65, max: 95 } },
    buildPhases: [
      "Corridor inventory + priority tier",
      "Tier-1 corridor anchor pass (airport, beach, entertainment)",
      "Tier-2 corridor expansion",
      "Infrastructure connectors between corridors",
      "Submarket QA + duplicate sweep",
      "Mature coverage pass (100+ demand anchors)",
    ],
    readinessCriteria: {
      seeded: { minTotalPoints: 10, requireSourceBacked: true },
      marketReady: { minTotalPoints: 40, minDemandAnchors: 30, minTravelInfra: 8, minCorridorsCovered: 2 },
      dealReady: { minTotalPoints: 65, minDemandAnchors: 50, minTravelInfra: 15, minCorridorsCovered: 4 },
      intelligenceReady: {
        minTotalPoints: 125,
        minDemandAnchors: 100,
        minPointTypes: 10,
        minCorridorsCovered: 6,
      },
    },
    exampleCountries: ["Dominican Republic", "Costa Rica", "Jamaica", "Belize", "Honduras"],
  },
  [BUILD_STRATEGY_TYPES.MARKET_BY_MARKET]: {
    id: "MARKET_BY_MARKET",
    label: BUILD_STRATEGY_TYPES.MARKET_BY_MARKET,
    recommendedSubmarketApproach:
      "Treat each primary city/resort market as a build unit; avoid one flat national dataset until Tier-1 markets are Deal Ready.",
    targetDemandAnchors: { min: 80, max: 400, firstPass: { min: 20, max: 40, perMarket: true } },
    targetTravelInfrastructure: { min: 20, max: 120, firstPass: { min: 8, max: 15, perMarket: true } },
    targetTotalRadarPoints: { min: 100, max: 520, firstPass: { min: 28, max: 55, perMarket: true } },
    buildPhases: [
      "Tier-1 market selection",
      "Per-market demand anchor seed",
      "Per-market travel infrastructure gap fill",
      "National rollup QA (no flat overpopulation)",
      "Tier-2 market rollout",
      "Intelligence-ready category diversity check",
    ],
    readinessCriteria: {
      seeded: { minTotalPoints: 10, requireSourceBacked: true },
      marketReady: { minTotalPoints: 28, minDemandAnchors: 20, minTravelInfra: 5, minMarketsCovered: 1 },
      dealReady: { minTotalPoints: 80, minDemandAnchors: 60, minTravelInfra: 15, minMarketsCovered: 2 },
      intelligenceReady: {
        minTotalPoints: 200,
        minDemandAnchors: 150,
        minMarketsCovered: 4,
        minPointTypes: 10,
      },
    },
    exampleCountries: [
      "Mexico",
      "Brazil",
      "Colombia",
      "Argentina",
      "Chile",
      "Peru",
      "Ecuador",
    ],
  },
};

/**
 * @param {string} strategyType
 */
export function getBuildStrategyDefinition(strategyType) {
  return BUILD_STRATEGY_DEFINITIONS[strategyType] || null;
}

/**
 * Resolve numeric targets for a country config.
 * @param {object} config — country config entry
 */
export function resolveStrategyTargets(config) {
  const def = getBuildStrategyDefinition(config.buildStrategy);
  if (!def) {
    return {
      demandAnchors: config.targets?.demandAnchors?.mature?.max || 50,
      travelInfrastructure: config.targets?.travelInfrastructure?.mature?.max || 20,
      totalRadarPoints: config.targets?.totalRadarPoints?.mature?.max || 70,
    };
  }
  const custom = config.targets || {};
  const da =
    custom.demandAnchors?.firstPass?.max ||
    custom.demandAnchors?.mature?.min ||
    def.targetDemandAnchors.firstPass?.max ||
    def.targetDemandAnchors.min;
  const ti =
    custom.travelInfrastructure?.firstPass?.max ||
    custom.travelInfrastructure?.mature?.min ||
    def.targetTravelInfrastructure.firstPass?.max ||
    def.targetTravelInfrastructure.min;
  const total =
    custom.totalRadarPoints?.firstPass?.max ||
    custom.totalRadarPoints?.mature?.min ||
    def.targetTotalRadarPoints.firstPass?.max ||
    da + ti;
  return { demandAnchors: da, travelInfrastructure: ti, totalRadarPoints: total };
}
