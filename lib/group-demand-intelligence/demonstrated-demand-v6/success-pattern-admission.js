/**
 * Success-pattern admission — research admission only, not customer-facing score.
 * Requires DEMONSTRATED_DEMAND_LEAD + STRONG or PLAUSIBLE match to Bethesda/NYC controls.
 */

export const SUCCESS_MATCH = Object.freeze({
  STRONG: "STRONG",
  PLAUSIBLE: "PLAUSIBLE",
  WEAK: "WEAK",
  NONE: "NONE",
});

/** Canonical success traits from Bethesda / NYC ready-time pattern. */
export const SUCCESS_TRAITS = Object.freeze([
  "NAMED_ORGANIZER_BUYER_ENTITY",
  "FUTURE_DATE_OR_CYCLE_DECISION_WINDOW",
  "EXPLICIT_HOTEL_MOTION_THESIS",
  "PUBLIC_CONTACT_OR_HOUSING_PATH",
  "MARKET_CREDIBLE_VENUE_OR_DESTINATION",
  "LODGING_OR_HOUSING_EVIDENCE",
  "SOURCE_AUTHORITY_OR_OFFICIAL_PAGE",
  "REPEAT_OR_COMPETITOR_HOST_PATTERN",
]);

/**
 * Score a demonstrated-demand lead against successful opportunity traits.
 * @returns {{ match, score, maxScore, traitsPresent, traitsMissing, admit }}
 */
export function successfulPatternMatchScore(lead = {}, opts = {}) {
  const present = [];
  const missing = [];
  const b = [
    lead.title,
    lead.organizationName || lead.organization,
    lead.hotelOpportunityThesis,
    lead.summaryWhat,
    lead.fact,
    lead.officialSource || lead.source,
  ]
    .map((x) => String(x || ""))
    .join(" ");

  const checks = [
    [
      "NAMED_ORGANIZER_BUYER_ENTITY",
      Boolean(lead.organizationName || lead.organization || lead.buyerEntity) &&
        String(lead.organizationName || lead.organization || "").length >= 6,
    ],
    [
      "FUTURE_DATE_OR_CYCLE_DECISION_WINDOW",
      Boolean(
        lead.eventStartDate ||
          lead.eventYear ||
          lead.nextKnownCycle ||
          lead.nextExpectedCycle ||
          lead.nextDecisionWindow ||
          /202[6-9]|203[0-2]|site selection|RFP/i.test(b)
      ),
    ],
    [
      "EXPLICIT_HOTEL_MOTION_THESIS",
      /overflow|housing|room block|official hotel|host hotel|primary|compete|delegation|group lodging/i.test(
        `${lead.hotelOpportunityThesis || ""} ${b}`
      ) || Boolean(lead.competitorHotel || lead.evidenceClass),
    ],
    [
      "PUBLIC_CONTACT_OR_HOUSING_PATH",
      Boolean(
        lead.organizationContactUrl ||
          lead.functionalContactEmail ||
          lead.publicContactPath ||
          lead.lodgingEvidence ||
          /\/contact|housing|accommodation/i.test(String(lead.officialSource || lead.source || ""))
      ),
    ],
    [
      "MARKET_CREDIBLE_VENUE_OR_DESTINATION",
      Boolean(lead.market || lead.eventLocationSummary || lead.lodgingMarket) ||
        (opts.geoTokens || []).some((t) => b.toLowerCase().includes(String(t).toLowerCase())),
    ],
    [
      "LODGING_OR_HOUSING_EVIDENCE",
      Boolean(lead.lodgingEvidence) ||
        /hotel block|official hotel|housing|accommodation|hébergement|alojamiento/i.test(b) ||
        lead.evidenceClass === "DIRECT_CONFIRMED",
    ],
    [
      "SOURCE_AUTHORITY_OR_OFFICIAL_PAGE",
      /\.(org|gov|edu|int)\b|association|society|federation|issuu|congress/i.test(
        String(lead.officialSource || lead.source || "")
      ) || lead.evidenceClass === "DIRECT_CONFIRMED",
    ],
    [
      "REPEAT_OR_COMPETITOR_HOST_PATTERN",
      Boolean(lead.competitorHotel || lead.historicHotels || lead.repeatCadence) ||
        /annual|series|rotation|repeat/i.test(b),
    ],
  ];

  for (const [trait, ok] of checks) {
    if (ok) present.push(trait);
    else missing.push(trait);
  }

  const score = present.length;
  const maxScore = SUCCESS_TRAITS.length;
  let match = SUCCESS_MATCH.NONE;
  if (score >= 6) match = SUCCESS_MATCH.STRONG;
  else if (score >= 4) match = SUCCESS_MATCH.PLAUSIBLE;
  else if (score >= 2) match = SUCCESS_MATCH.WEAK;

  const demonstrated = opts.demonstratedDemand === true;
  const admit =
    demonstrated && (match === SUCCESS_MATCH.STRONG || match === SUCCESS_MATCH.PLAUSIBLE);

  return {
    match,
    score,
    maxScore,
    pct: Number((score / maxScore).toFixed(2)),
    traitsPresent: present,
    traitsMissing: missing,
    admit,
    admissionClass: admit
      ? "SUCCESS_PATTERN_ADMITTED"
      : demonstrated
        ? "DEMONSTRATED_NOT_ADMITTED"
        : "NOT_DEMONSTRATED",
  };
}

/**
 * Build SUCCESS_PATTERN_MODEL markdown traits from control aggregate.
 */
export function buildSuccessPatternModelDoc(patternResult = {}) {
  const agg = patternResult.aggregate || {};
  const traits = (patternResult.topDifferencesVsZeroYield || SUCCESS_TRAITS).map(
    (t, i) => `SUCCESS TRAIT ${i + 1}: ${t}`
  );
  return {
    traits,
    traitIds: SUCCESS_TRAITS,
    aggregate: agg,
    patternRules: patternResult.patternRules || {},
  };
}
