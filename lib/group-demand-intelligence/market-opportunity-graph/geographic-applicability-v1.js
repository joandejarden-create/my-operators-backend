/**
 * Hotel geographic applicability for a market opportunity (generic).
 * Uses territory keywords + hierarchy overlap — not distance-only.
 */

import {
  GEO_APPLICABILITY,
  MARKET_LEVEL,
  classifyOpportunityGeography,
} from "./market-geography-v1.js";
import { classifyDemandTerritoryFitFromConfig } from "../demand-territory.js";
import { toTerritoryRole, TERRITORY_ROLE } from "../demand-territory.js";

/**
 * @returns {{ applicability, territoryClass, territoryRole, reasons, oppGeo }}
 */
export function evaluateHotelGeographicApplicability(opp = {}, hotelProfile = {}) {
  const reasons = [];
  const oppGeo = classifyOpportunityGeography(opp);
  const cfg = hotelProfile.config || null;

  // Cross-hotel: never inherit another hotel's locked demandTerritoryFit
  const oppForTerritory = {
    ...opp,
    demandTerritoryFitLocked: false,
    demandTerritoryFit: undefined,
    demandTerritoryRationale: undefined,
  };

  const territory = cfg
    ? classifyDemandTerritoryFitFromConfig(oppForTerritory, cfg)
    : null;
  const territoryClass = territory?.demandTerritoryFit || null;
  const role = toTerritoryRole(territoryClass);

  // Hierarchy overlap
  const sameMicro =
    oppGeo.microArea &&
    hotelProfile.microArea &&
    normalize(oppGeo.microArea) === normalize(hotelProfile.microArea);
  const sameSub =
    oppGeo.submarket &&
    hotelProfile.submarket &&
    normalize(oppGeo.submarket) === normalize(hotelProfile.submarket);
  const sameBorough =
    oppGeo.borough &&
    hotelProfile.borough &&
    normalize(oppGeo.borough) === normalize(hotelProfile.borough);
  const brooklynOpp = /brooklyn/i.test(
    `${oppGeo.borough || ""} ${oppGeo.submarket || ""} ${oppGeo.microArea || ""}`
  );
  const midtownHotel = /times square|midtown/i.test(
    `${hotelProfile.submarket || ""} ${hotelProfile.microArea || ""}`
  );

  if (brooklynOpp && midtownHotel && oppGeo.level !== MARKET_LEVEL.METRO) {
    reasons.push("Brooklyn-specific opportunity — Midtown hotel not auto-applicable");
    return {
      applicability: GEO_APPLICABILITY.NONE,
      territoryClass,
      territoryRole: role,
      reasons,
      oppGeo,
      sameMicro,
      sameSub,
      sameBorough,
    };
  }

  if (role === TERRITORY_ROLE.OUTSIDE) {
    reasons.push("Demand territory OUTSIDE for hotel");
    return {
      applicability: GEO_APPLICABILITY.NONE,
      territoryClass,
      territoryRole: role,
      reasons,
      oppGeo,
      sameMicro,
      sameSub,
      sameBorough,
    };
  }

  if (sameMicro || role === TERRITORY_ROLE.CORE) {
    reasons.push(sameMicro ? "Same micro-area" : "Territory CORE");
    return {
      applicability: GEO_APPLICABILITY.DIRECT,
      territoryClass,
      territoryRole: role,
      reasons,
      oppGeo,
      sameMicro,
      sameSub,
      sameBorough,
    };
  }
  if (sameSub || role === TERRITORY_ROLE.NEARBY) {
    reasons.push(sameSub ? "Same submarket" : "Territory NEARBY");
    return {
      applicability: GEO_APPLICABILITY.STRONG,
      territoryClass,
      territoryRole: role,
      reasons,
      oppGeo,
      sameMicro,
      sameSub,
      sameBorough,
    };
  }
  if (role === TERRITORY_ROLE.COMPETITIVE || (sameBorough && oppGeo.level >= MARKET_LEVEL.BOROUGH_SECTOR)) {
    reasons.push("Same borough / COMPETITIVE territory");
    return {
      applicability: GEO_APPLICABILITY.PLAUSIBLE,
      territoryClass,
      territoryRole: role,
      reasons,
      oppGeo,
      sameMicro,
      sameSub,
      sameBorough,
    };
  }
  if (role === TERRITORY_ROLE.STRETCH) {
    reasons.push("Territory STRETCH");
    return {
      applicability: GEO_APPLICABILITY.WEAK,
      territoryClass,
      territoryRole: role,
      reasons,
      oppGeo,
      sameMicro,
      sameSub,
      sameBorough,
    };
  }

  if (oppGeo.confidence === "METRO_ONLY" || oppGeo.confidence === "UNKNOWN") {
    reasons.push("NYC/metro label only — insufficient for hotel applicability");
    return {
      applicability: GEO_APPLICABILITY.UNKNOWN,
      territoryClass,
      territoryRole: role,
      reasons,
      oppGeo,
      sameMicro,
      sameSub,
      sameBorough,
    };
  }

  reasons.push("No clear geographic overlap");
  return {
    applicability: GEO_APPLICABILITY.WEAK,
    territoryClass,
    territoryRole: role,
    reasons,
    oppGeo,
    sameMicro,
    sameSub,
    sameBorough,
  };
}

function normalize(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function applicabilityIsCandidate(applicability) {
  return (
    applicability === GEO_APPLICABILITY.DIRECT ||
    applicability === GEO_APPLICABILITY.STRONG ||
    applicability === GEO_APPLICABILITY.PLAUSIBLE
  );
}
