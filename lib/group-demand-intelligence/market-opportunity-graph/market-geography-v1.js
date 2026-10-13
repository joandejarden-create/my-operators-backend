/**
 * GDI market geography hierarchy V1 (hotel-agnostic).
 * MARKET_LEVEL_0 metro → L1 borough/sector → L2 submarket → L3 micro-area / demand node.
 */

export const MARKET_LEVEL = Object.freeze({
  METRO: 0,
  BOROUGH_SECTOR: 1,
  SUBMARKET: 2,
  MICRO_AREA: 3,
});

export const GEO_APPLICABILITY = Object.freeze({
  DIRECT: "DIRECT",
  STRONG: "STRONG",
  PLAUSIBLE: "PLAUSIBLE",
  WEAK: "WEAK",
  NONE: "NONE",
  UNKNOWN: "UNKNOWN",
});

/** Fan-out policy: when to auto-evaluate other hotels for a market opportunity. */
export const GEO_FANOUT_POLICY = Object.freeze({
  SAME_MICRO_AREA: "AUTO_EVALUATE",
  SAME_SUBMARKET: "AUTO_EVALUATE",
  SAME_BOROUGH: "EVALUATE_IF_DEMAND_PATTERN_SUPPORTS",
  SAME_METRO_ONLY: "NO_BLIND_FANOUT",
  CROSS_BOROUGH: "REQUIRE_STRONGER_EVIDENCE",
});

/**
 * Stable market opportunity identity from hotel-neutral event facts.
 */
export function computeMarketOpportunityId({
  organizationName,
  eventName,
  eventStartDate,
  eventYear,
  venueOrLocation,
} = {}) {
  const seed = [
    String(organizationName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim(),
    String(eventName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim(),
    String(eventStartDate || eventYear || "")
      .toLowerCase()
      .trim(),
    String(venueOrLocation || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim(),
  ].join("|");
  let h = 0;
  for (let i = 0; i < seed.length; i++) h = (h * 31 + seed.charCodeAt(i)) >>> 0;
  return `gdi_mkt_${h.toString(16).padStart(8, "0")}_${seed
    .split("|")
    .map((p) => p.slice(0, 12).replace(/\s+/g, "_"))
    .filter(Boolean)
    .slice(0, 2)
    .join("_")
    .slice(0, 40)}`;
}

/**
 * Infer opportunity geography attachment level from evidence text.
 * Does not invent precision beyond evidence.
 */
export function classifyOpportunityGeography(opp = {}, marketHints = {}) {
  const blob = [
    opp.title,
    opp.destinationStatus,
    opp.eventLocation,
    opp.location,
    opp.venueStatus,
    opp.summaryWhat,
    opp.organizationName,
    ...(Array.isArray(opp.meetingHistory)
      ? opp.meetingHistory.map((h) => `${h?.city || ""} ${h?.venue || ""}`)
      : []),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  const micro = matchFirst(blob, marketHints.microAreas || NYC_MICRO_AREAS);
  const sub = matchFirst(blob, marketHints.submarkets || NYC_SUBMARKETS);
  const borough = matchFirst(blob, marketHints.boroughs || NYC_BOROUGHS);

  let level = MARKET_LEVEL.METRO;
  let label = marketHints.metroLabel || "New York City";
  if (micro) {
    level = MARKET_LEVEL.MICRO_AREA;
    label = micro.label;
  } else if (sub) {
    level = MARKET_LEVEL.SUBMARKET;
    label = sub.label;
  } else if (borough) {
    level = MARKET_LEVEL.BOROUGH_SECTOR;
    label = borough.label;
  } else if (/\b(nyc|new york city|new york)\b/i.test(blob)) {
    level = MARKET_LEVEL.METRO;
    label = "New York City";
  } else if (
    marketHints.metroLabel &&
    (marketHints.metroPatterns
      ? marketHints.metroPatterns.some((p) => p.test(blob))
      : new RegExp(
          String(marketHints.metroLabel).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
          "i"
        ).test(blob))
  ) {
    level = MARKET_LEVEL.METRO;
    label = marketHints.metroLabel;
  } else {
    return {
      level: null,
      label: null,
      metro: marketHints.metroLabel || "New York City",
      borough: null,
      submarket: null,
      microArea: null,
      confidence: "UNKNOWN",
      evidenceBlobSample: blob.slice(0, 120),
    };
  }

  return {
    level,
    label,
    metro: marketHints.metroLabel || (label === "New York City" ? "New York City" : label),
    borough: borough?.label || (level >= MARKET_LEVEL.BOROUGH_SECTOR ? inferBoroughFromSub(sub || micro) : null),
    submarket: sub?.label || micro?.submarket || null,
    microArea: micro?.label || null,
    confidence: micro || sub ? "SPECIFIC" : borough ? "BOROUGH" : "METRO_ONLY",
    evidenceBlobSample: blob.slice(0, 120),
  };
}

function matchFirst(blob, entries) {
  for (const e of entries) {
    if (e.patterns.some((p) => p.test(blob))) return e;
  }
  return null;
}

function inferBoroughFromSub(entry) {
  if (!entry) return null;
  return entry.borough || null;
}

export const NYC_BOROUGHS = Object.freeze([
  { label: "Brooklyn", borough: "Brooklyn", patterns: [/\bbrooklyn\b/, /\bwilliamsburg\b/, /\bdumbo\b/] },
  { label: "Queens", borough: "Queens", patterns: [/\bqueens\b/, /\blong island city\b|\blic\b/] },
  { label: "Bronx", borough: "Bronx", patterns: [/\bbronx\b/] },
  { label: "Staten Island", borough: "Staten Island", patterns: [/\bstaten island\b/] },
  { label: "Manhattan", borough: "Manhattan", patterns: [/\bmanhattan\b/, /\bmidtown\b/, /\btimes square\b/, /\bnoho\b/, /\bsoho\b/, /\bfidi\b/, /\bfinancial district\b/] },
]);

export const NYC_SUBMARKETS = Object.freeze([
  {
    label: "Times Square / Midtown West",
    borough: "Manhattan",
    patterns: [/\btimes square\b/, /\btheater district\b/, /\bmidtown west\b/, /\b42nd street\b/, /\bbroadway\b.*\b(hotel|nyc|new york)/],
  },
  {
    label: "Midtown East",
    borough: "Manhattan",
    patterns: [/\bmidtown east\b/, /\bgrand central\b/, /\bryant park\b/],
  },
  {
    label: "Hudson Yards / Javits",
    borough: "Manhattan",
    patterns: [/\bjavits\b/, /\bhudson yards\b/, /\bjavits center\b/],
  },
  {
    label: "NoHo / SoHo / Village",
    borough: "Manhattan",
    patterns: [/\bnoho\b/, /\bsoho\b/, /\bgreenwich village\b/, /\bwashington square\b/, /\blafayette\b/],
  },
  {
    label: "Financial District",
    borough: "Manhattan",
    patterns: [/\bfidi\b/, /\bfinancial district\b/, /\bwall street\b/],
  },
  {
    label: "Upper Manhattan",
    borough: "Manhattan",
    patterns: [/\bupper west\b/, /\bupper east\b/, /\bcolumbia\b/, /\bharlem\b/],
  },
  {
    label: "Downtown Brooklyn",
    borough: "Brooklyn",
    patterns: [/\bdowntown brooklyn\b/, /\bbarclays\b/],
  },
  {
    label: "Williamsburg",
    borough: "Brooklyn",
    patterns: [/\bwilliamsburg\b/],
  },
  {
    label: "Long Island City",
    borough: "Queens",
    patterns: [/\blong island city\b/, /\blic\b/],
  },
]);

export const NYC_MICRO_AREAS = Object.freeze([
  {
    label: "Times Square Theater Cluster",
    submarket: "Times Square / Midtown West",
    borough: "Manhattan",
    patterns: [/\btimes square\b/, /\btheater district\b/, /\bbroadway theatre\b|\bbroadway theater\b/],
  },
  {
    label: "Javits Convention Node",
    submarket: "Hudson Yards / Javits",
    borough: "Manhattan",
    patterns: [/\bjavits\b/],
  },
  {
    label: "NoHo Lafayette Corridor",
    submarket: "NoHo / SoHo / Village",
    borough: "Manhattan",
    patterns: [/\bnoho\b/, /\blafayette\b/],
  },
  {
    label: "NYU / Washington Square",
    submarket: "NoHo / SoHo / Village",
    borough: "Manhattan",
    patterns: [/\bnyu\b/, /\bwashington square\b/],
  },
]);

/**
 * Should hotels in the same metro auto-evaluate this opportunity?
 */
export function fanoutDecisionForGeography(oppGeo = {}) {
  if (oppGeo.confidence === "UNKNOWN" || oppGeo.level == null) {
    return {
      policy: GEO_FANOUT_POLICY.SAME_METRO_ONLY,
      autoEvaluateSameSubmarket: false,
      autoEvaluateSameBorough: false,
      autoEvaluateMetroWide: false,
      reason: "Insufficient geographic precision — no blind metro fan-out",
    };
  }
  if (oppGeo.level >= MARKET_LEVEL.MICRO_AREA || oppGeo.level === MARKET_LEVEL.SUBMARKET) {
    return {
      policy: GEO_FANOUT_POLICY.SAME_SUBMARKET,
      autoEvaluateSameSubmarket: true,
      autoEvaluateSameBorough: false,
      autoEvaluateMetroWide: false,
      reason: "Specific submarket/micro-area — evaluate hotels in same submarket",
    };
  }
  if (oppGeo.level === MARKET_LEVEL.BOROUGH_SECTOR) {
    return {
      policy: GEO_FANOUT_POLICY.SAME_BOROUGH,
      autoEvaluateSameSubmarket: false,
      autoEvaluateSameBorough: true,
      autoEvaluateMetroWide: false,
      reason: "Borough-level only — evaluate if demand pattern supports",
    };
  }
  return {
    policy: GEO_FANOUT_POLICY.SAME_METRO_ONLY,
    autoEvaluateSameSubmarket: false,
    autoEvaluateSameBorough: false,
    autoEvaluateMetroWide: false,
    reason: "Metro-only label — do not blind fan-out",
  };
}

/**
 * Brooklyn safeguard: would a Brooklyn-specific opportunity automatically
 * reach Midtown hotels? Product answer must be NO unless metro-wide evidence.
 */
export function wouldAutomaticallyFanoutBrooklynToMidtown(oppGeo = {}) {
  const brooklyn =
    /brooklyn|williamsburg/i.test(String(oppGeo.borough || "")) ||
    /brooklyn|williamsburg/i.test(String(oppGeo.submarket || "")) ||
    /brooklyn|williamsburg/i.test(String(oppGeo.microArea || ""));
  if (!brooklyn) return false;
  // Even metro-labeled Brooklyn demand does not blind-fanout to Midtown
  return false;
}

/** @deprecated use wouldAutomaticallyFanoutBrooklynToMidtown */
export function wouldBlindFanoutToMidtown(oppGeo = {}) {
  return wouldAutomaticallyFanoutBrooklynToMidtown(oppGeo);
}
