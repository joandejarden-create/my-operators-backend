/**
 * NYC market geography + bounded discovery queries for GDI market-first canary V1.
 * Hotel-agnostic. No hotel-branched SERP.
 */

import { NYC_BOROUGHS, NYC_SUBMARKETS, NYC_MICRO_AREAS } from "./market-geography-v1.js";
import { NYC_CONTROL_HOTELS } from "./hotel-geography-profile-v1.js";

export { NYC_CONTROL_HOTELS };

export const NYC_HOTELS = Object.freeze({
  RENAISSANCE: NYC_CONTROL_HOTELS.RENAISSANCE,
  HILTON: NYC_CONTROL_HOTELS.HILTON,
  NOW_NOW: NYC_CONTROL_HOTELS.NOW_NOW,
});

export function nycMarketHints() {
  return {
    metroLabel: "New York City",
    metroPatterns: [/\bnew york city\b/, /\bnyc\b/, /\bmanhattan\b/, /\bnew york,?\s*ny\b/],
    boroughs: NYC_BOROUGHS,
    submarkets: NYC_SUBMARKETS,
    microAreas: NYC_MICRO_AREAS,
  };
}

export function isNycInMarket(text = "") {
  const t = String(text || "").toLowerCase();
  if (!t) return false;
  if (/\b(los angeles|chicago|miami|boston|philadelphia|washington,? d\.?c\.?|las vegas|orlando|atlanta|dallas|houston|san francisco|seattle|denver)\b/.test(t) && !/\b(new york|nyc|manhattan|times square|javits|brooklyn)\b/.test(t)) {
    return false;
  }
  return /\b(new york city|nyc|manhattan|times square|midtown|javits|broadway|noho|soho|brooklyn|long island city|new york,?\s*ny)\b/.test(
    t
  );
}

export function inferNycDestination(text = "", title = "") {
  const blob = `${title} ${text}`.toLowerCase();
  if (/\btimes square\b|\btheater district\b/.test(blob)) return "New York City / Times Square";
  if (/\bjavits\b|\bhudson yards\b/.test(blob)) return "New York City / Hudson Yards–Javits";
  if (/\bnoho\b|\bsoho\b|\bgreenwich village\b|\bwashington square\b|\bnyu\b/.test(blob)) {
    return "New York City / NoHo–Village";
  }
  if (/\bmidtown\b|\b42nd street\b|\bbryant park\b|\bgrand central\b/.test(blob)) {
    return "New York City / Midtown";
  }
  if (/\bbrooklyn\b|\bbarclays\b/.test(blob)) return "New York City / Brooklyn";
  if (/\bmanhattan\b|\bnew york city\b|\bnyc\b|\bnew york\b/.test(blob)) return "New York City";
  return null;
}

/**
 * Highest-yield NYC archetypes for lodging-plausible public research.
 */
export const NYC_DEMAND_ARCHETYPES = Object.freeze([
  "URBAN_ASSOCIATION",
  "MEDICAL_SCIENTIFIC",
  "OVERFLOW_JAVITS",
  "EXHIBITOR_VENDOR",
  "UNIVERSITY",
  "ENTERTAINMENT_PRODUCTION",
  "CORPORATE_PROGRAM",
  "SPORTS_TEAM",
]);

/**
 * Bounded query set for NYC canary (quality over breadth).
 */
export function buildNycMarketQueries({ max = 22 } = {}) {
  const y1 = 2026;
  const y2 = 2027;
  const y3 = 2028;
  const queries = [];
  const push = (family, archetype, query) => {
    if (queries.length >= max) return;
    queries.push({
      family,
      archetype,
      query,
      queryId: `nyc_${family}_${queries.length}`,
    });
  };

  // Association future meetings + housing
  push(
    "ASSOCIATION_CALENDAR",
    "URBAN_ASSOCIATION",
    `"New York" OR NYC OR Manhattan (${y2} OR ${y3}) ("annual meeting" OR "annual conference") (housing OR "hotel block" OR "host hotel" OR accommodation)`
  );
  push(
    "HOUSING_PAGE",
    "URBAN_ASSOCIATION",
    `"New York City" OR Manhattan (${y2} OR ${y3}) ("official hotel" OR "housing bureau" OR "room block") conference OR meeting`
  );
  push(
    "OFFICIAL_EVENT_PAGE",
    "URBAN_ASSOCIATION",
    `"Times Square" OR Midtown (${y2} OR ${y3}) (association OR society) (meeting OR conference) (hotel OR housing)`
  );

  // Medical / scientific
  push(
    "ASSOCIATION_CALENDAR",
    "MEDICAL_SCIENTIFIC",
    `"New York" OR NYC (${y2} OR ${y3}) (medical OR clinical OR scientific) (symposium OR congress OR "annual meeting") (housing OR hotel OR accommodation)`
  );
  push(
    "HOUSING_PAGE",
    "MEDICAL_SCIENTIFIC",
    `NYC OR Manhattan (${y2} OR ${y3}) (ASCO OR radiology OR cardiology OR neurology OR oncology) (housing OR "hotel block")`
  );

  // Javits / overflow / exhibitor sub-demand
  push(
    "CONVENTION_BUREAU",
    "OVERFLOW_JAVITS",
    `"Javits" OR "Javits Center" (${y2} OR ${y3}) (housing OR hotel OR overflow OR "room block" OR accommodation)`
  );
  push(
    "EXHIBITOR_DIRECTORY",
    "EXHIBITOR_VENDOR",
    `"Javits" (${y2} OR ${y3}) (exhibitor OR sponsor) (hotel OR housing OR "preferred hotel")`
  );
  push(
    "EVENT_MANUAL",
    "EXHIBITOR_VENDOR",
    `"New York" OR NYC (${y2} OR ${y3}) ("exhibitor manual" OR "exhibitor kit") (hotel OR housing OR accommodation)`
  );

  // University
  push(
    "UNIVERSITY_CALENDAR",
    "UNIVERSITY",
    `(NYU OR Columbia OR "New York University") (${y2} OR ${y3}) (conference OR symposium OR commencement OR reunion) (hotel OR housing OR accommodation)`
  );

  // Broadway / production / tour-adjacent lodging signals
  push(
    "PRODUCTION_SOURCE",
    "ENTERTAINMENT_PRODUCTION",
    `"New York" OR Broadway (${y2} OR ${y3}) (production OR "company housing" OR "cast hotel" OR "crew lodging") hotel`
  );
  push(
    "TOUR_SERIES_SOURCE",
    "ENTERTAINMENT_PRODUCTION",
    `"New York City" (${y2} OR ${y3}) (tour group OR "group travel") (hotel OR "room block") Broadway OR Midtown`
  );

  // Corporate / industry midtown
  push(
    "CORPORATE_EVENT_PAGE",
    "CORPORATE_PROGRAM",
    `Manhattan OR "New York City" (${y2} OR ${y3}) ("leadership summit" OR "executive offsite" OR "board meeting") (hotel OR lodging)`
  );
  push(
    "INDUSTRY_DAY",
    "CORPORATE_PROGRAM",
    `"New York" OR NYC (${y2} OR ${y3}) ("industry day" OR "partner summit" OR "user conference") (housing OR hotel)`
  );

  // Sports
  push(
    "SPORTS_SCHEDULE",
    "SPORTS_TEAM",
    `"New York" OR NYC (${y2} OR ${y3}) (tournament OR championship) (hotel OR "team hotel" OR housing)`
  );

  // Sub-demand under citywide: satellite / board / delegation
  push(
    "OFFICIAL_EVENT_PAGE",
    "URBAN_ASSOCIATION",
    `"New York" OR NYC (${y2} OR ${y3}) (satellite meeting OR "board meeting" OR delegation) (hotel OR housing)`
  );
  push(
    "REGISTRATION_PAGE",
    "URBAN_ASSOCIATION",
    `NYC OR Manhattan (${y2} OR ${y3}) conference (registration) (travel OR accommodation OR hotel)`
  );

  // Future host / TBD hotel
  push(
    "FUTURE_HOST_SOURCE",
    "URBAN_ASSOCIATION",
    `"New York" OR NYC (${y2} OR ${y3}) ("hotel TBD" OR "housing TBD" OR "venue TBD" OR "host city") conference OR meeting`
  );

  // CVB / nycgo style
  push(
    "CONVENTION_BUREAU",
    "OVERFLOW_JAVITS",
    `"NYC & Company" OR nycgo OR "New York Convention" (${y2} OR ${y3}) (meeting OR conference) (hotel OR housing)`
  );

  return queries.slice(0, max);
}

export function nycMarketDevelopmentState({
  lodgingEvidence,
  commercialStatus,
  validEntity,
  futureCycle,
} = {}) {
  if (!validEntity) return "DISCOVERED";
  if (!futureCycle) return "IDENTITY_VALIDATED";
  const lodgingOk =
    lodgingEvidence &&
    lodgingEvidence !== "NONE" &&
    lodgingEvidence !== "UNKNOWN";
  if (!lodgingOk) return "FUTURE_VALIDATED";
  const status = String(commercialStatus || "");
  const open =
    /OPEN|TBD|RFP|UNRESOLVED|OVERFLOW|PARTIALLY|DESTINATION_SET|HOTEL_SELECTION|HOTEL \/ VENUE/i.test(
      status
    );
  if (open || lodgingOk) return "HOTEL_EVALUATION_READY";
  return "COMMERCIAL_SIGNAL_VALIDATED";
}

export function decideNycMarketResearchAction(ctx = {}) {
  const attempted = new Set(ctx.sourcesAttempted || []);
  if (!attempted.has("ASSOCIATION_CALENDAR") && !attempted.has("OFFICIAL_EVENT_PAGE")) {
    return {
      action: "SEARCH_ASSOCIATION_FUTURE_MEETINGS",
      family: "ASSOCIATION_CALENDAR",
      rationale: "Prioritize association future meetings with housing language",
    };
  }
  if (!attempted.has("HOUSING_PAGE") && !attempted.has("CONVENTION_BUREAU")) {
    return {
      action: "SEARCH_HOUSING_PAGES",
      family: "HOUSING_PAGE",
      rationale: "Lodging evidence is the highest leverage filter",
    };
  }
  if (!attempted.has("EXHIBITOR_DIRECTORY") && !attempted.has("EVENT_MANUAL")) {
    return {
      action: "SEARCH_EXHIBITOR_SUBGROUPS",
      family: "EXHIBITOR_DIRECTORY",
      rationale: "Go below citywide Javits/trade demand into exhibitor/vendor lodging",
    };
  }
  if (ctx.candidateNeedsFutureCycle) {
    return {
      action: "VERIFY_FUTURE_CYCLE",
      family: "OFFICIAL_EVENT_PAGE",
      rationale: "Validate future cycle before hotel evaluation",
    };
  }
  if (ctx.candidateNeedsLodging) {
    return {
      action: "VERIFY_LODGING_STATUS",
      family: "HOUSING_PAGE",
      rationale: "Lodging language needs evidence upgrade",
    };
  }
  return {
    action: "STOP_NO_PUBLIC_PATH",
    family: null,
    rationale: "Bounded NYC source ladder exhausted for this route",
  };
}
