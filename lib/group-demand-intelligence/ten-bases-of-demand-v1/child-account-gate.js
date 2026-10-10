/**
 * Account-level opportunity rules + anti-speculation limits.
 * Do NOT create hundreds of speculative child opportunities.
 */

const PARTICIPATION_SIGNALS = [
  /sponsor/i,
  /exhibitor/i,
  /speaker/i,
  /delegation/i,
  /pavilion/i,
  /partner/i,
  /vendor/i,
  /production/i,
  /broadcast/i,
  /crew/i,
  /secretariat/i,
  /national\s+(committee|delegation)/i,
  /housing|hotel block|accommodation/i,
  /advisory board|investigator/i,
];

/**
 * Score a child account under a demand generator.
 * Returns RESEARCH_LEAD only when named entity + participation + travel + market relevance
 * and at least one supporting travel/participation cue.
 */
export function scoreChildAccount(child = {}, generator = {}, hotel = {}) {
  const org = String(child.organization || child.namedEntity || "").trim();
  const blob = [
    org,
    child.role,
    child.participantType,
    child.evidence,
    child.hotelMotionHypothesis,
    generator.title,
    generator.organization,
  ]
    .map((x) => String(x || ""))
    .join(" ");

  const named = org.length >= 4 && !/^https?:/i.test(org) && !/^(sponsor|exhibitor|tbd|unknown)$/i.test(org);
  const participation = Boolean(child.participationEvidence || child.role || child.evidenceSource);
  const travelPlausible =
    Boolean(child.plausibleTravelingGroup) ||
    PARTICIPATION_SIGNALS.some((re) => re.test(blob)) ||
    /delegation|team|crew|board|training/i.test(blob);
  const marketRelevant =
    Boolean(child.marketRelevant) ||
    hotel.geoTokens?.some((t) => blob.toLowerCase().includes(String(t).toLowerCase())) ||
    Boolean(generator.marketRelevant !== false);

  const supports = [
    child.multipleAttendees || child.delegationEvidence,
    /sponsor|exhibitor/i.test(String(child.role || "")),
    /speaker|chair|faculty/i.test(String(child.role || "")),
    /organizer|vendor|secretariat|agency/i.test(String(child.role || "")),
    child.historicAttendance,
    child.knownTravelPattern,
    child.accommodationSignal,
    child.ancillaryMeetingEvidence,
  ].filter(Boolean).length;

  let score = 0;
  if (named) score += 25;
  if (participation) score += 20;
  if (travelPlausible) score += 20;
  if (marketRelevant) score += 15;
  score += Math.min(supports, 4) * 5;
  if (child.buyerClarity) score += 10;
  if (child.futureTiming) score += 10;
  if (child.hotelMotion) score += 10;

  const admitAsLead =
    named &&
    participation &&
    travelPlausible &&
    marketRelevant &&
    supports >= 1 &&
    score >= 55;

  return {
    score,
    admitAsLead,
    named,
    participation,
    travelPlausible,
    marketRelevant,
    supports,
    class: admitAsLead ? "RESEARCH_LEAD" : "GENERATOR_INTELLIGENCE",
    note: admitAsLead
      ? "Independent account lead under generator"
      : "Retained as generator intelligence — not auto-promoted",
  };
}

/** Cap child RESEARCH_LEADs per generator to prevent speculative explosion. */
export function selectHighValueChildLeads(scoredChildren = [], { maxPerGenerator = 5 } = {}) {
  return [...scoredChildren]
    .filter((c) => c.admitAsLead)
    .sort((a, b) => b.score - a.score)
    .slice(0, maxPerGenerator);
}
