/**
 * Event geography for hotel fit — Midtown hotels only match NYC-relevant events.
 */

const NYC_EVENT_RE =
  /\b(new\s*york|nyc|manhattan|midtown|times\s*square|javits|jacob\s*javits|brooklyn|queens|bronx|long\s*island|hudson\s*yards|broadway)\b/i;

const NON_NYC_EVENT_RE =
  /\b(istanbul|london|paris|berlin|dubai|singapore|hong\s*kong|tokyo|shanghai|beijing|mumbai|delhi|sydney|melbourne|toronto|montreal|vancouver|mexico\s*city|são\s*paulo|sao\s*paulo|las\s*vegas|orlando|miami|chicago|dallas|houston|atlanta|los\s*angeles|san\s*francisco|seattle|boston|washington|d\.?c\.?)\b/i;

/**
 * @returns {{ geography: 'NYC_MIDTOWN'|'NON_NYC'|'UNKNOWN', reason: string, destination: string|null }}
 */
export function classifyEventGeography(entity = {}) {
  const blob = [
    entity.demandGeneratorName,
    entity.eventName,
    entity.destination,
    entity.city,
    entity.sourceURL,
    entity.evidenceSnippet,
  ]
    .filter(Boolean)
    .join(" ");

  if (NON_NYC_EVENT_RE.test(blob) && !NYC_EVENT_RE.test(blob)) {
    const m = blob.match(NON_NYC_EVENT_RE);
    return {
      geography: "NON_NYC",
      reason: `event_city_${(m?.[0] || "non_nyc").toLowerCase().replace(/\s+/g, "_")}`,
      destination: m?.[0] || null,
    };
  }
  if (NYC_EVENT_RE.test(blob)) {
    return {
      geography: "NYC_MIDTOWN",
      reason: "nyc_event_keyword",
      destination: "New York Midtown",
    };
  }
  return {
    geography: "UNKNOWN",
    reason: "event_geography_unknown",
    destination: null,
  };
}

export function eventEligibleForMidtownHotels(entity = {}) {
  return classifyEventGeography(entity).geography === "NYC_MIDTOWN";
}
