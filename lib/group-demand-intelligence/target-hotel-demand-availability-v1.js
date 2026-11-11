/**
 * Target-hotel demand availability.
 *
 * Independent evidence that a competitor / official host hotel is booked does
 * NOT automatically create Sales Ready demand for the evaluating hotel.
 * Overflow / external room blocks / open sourcing must be evidenced separately.
 */

export const TARGET_HOTEL_DEMAND_AVAILABILITY_V1 =
  "gdi-target-hotel-demand-availability-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const COMPETITOR_HOST_RE =
  /\b((?:crystal gateway|college park|westin|sheraton|hyatt|hilton|omni|grand hyatt)[\w\s&'./-]{0,40}(?:marriott|hilton|hyatt|westin|sheraton|hotel|inn)?|(?:marriott|hilton|hyatt|westin|sheraton)[\w\s&'./-]{0,40})\b[\s\S]{0,40}\b(named|contracted|host hotel|home base|official hotel|group block)\b|\b(named|contracted|host hotel|home base|official hotel)\b[\s\S]{0,40}\b((?:crystal gateway|college park|westin|sheraton|hyatt|hilton|omni)[\w\s&'./-]{0,40}|marriott|hilton|hyatt|westin|sheraton)\b/i;

const OPEN_TARGET_PATH_RE =
  /\b(stay[- ]to[- ]play|housing bureau|housing system|external room blocks? (?:available|open)|secondary housing open|overflow hotel (?:named|open|available)|hotel package not yet|hotel not yet (?:named|published)|unresolved venue|no venue\/hotel listed|actively sourced)\b/i;
const HYPOTHESIZED_OVERFLOW_ONLY_RE =
  /\b(no official overflow|sold out with no official overflow|explore overflow|compete for overflow|overflow \/ housing \/ stay-to-play rather than winning)\b/i;

const SPONSORSHIP_ONLY_RE =
  /\b(sponsorship\/relationship only|not group-room pursuit|not a current primary sell|monitor for a future booking cycle)\b/i;
const STRETCH_ONLY_RE =
  /\b(not a natural .+ event|exploratory \/ stretch|stretch path|stretch only|no supported primary-host advantage|suburban overflow \/ satellite meetings only)\b/i;

function collectAvailabilityBlob(opportunity = {}) {
  return [
    ...(Array.isArray(opportunity.evidence)
      ? opportunity.evidence.map((item) => [item?.value, item?.sourceTitle, item?.quote].filter(Boolean).join(" "))
      : []),
    ...(Array.isArray(opportunity.competitors)
      ? opportunity.competitors.map((row) => [row?.name, row?.reason].filter(Boolean).join(" "))
      : []),
    opportunity.eventLocation,
    opportunity.eventLocationSummary,
    opportunity.destinationStatus,
    opportunity.hotelOpportunityThesis,
    opportunity.hotelDemandThesis,
    opportunity.summaryWhyHotel,
    opportunity.fitExplanation,
    opportunity.bethesdaWinThesis,
  ]
    .map(text)
    .filter(Boolean)
    .join("\n");
}

function targetHotelTokens(hotel = {}, opportunity = {}) {
  const name = text(
    hotel.name ||
      hotel.hotelName ||
      opportunity.targetHotelName ||
      opportunity.hotelName ||
      "",
  );
  return name
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((token) => token.length >= 4);
}

/**
 * @returns {{ ok: boolean, reason: string, competitorHostDetected: boolean, overflowEvidenced: boolean, sponsorshipOnly: boolean }}
 */
export function assessTargetHotelDemandAvailability(opportunity = {}, hotel = {}) {
  const blob = collectAvailabilityBlob(opportunity);
  const sponsorshipOnly = SPONSORSHIP_ONLY_RE.test(blob);
  if (sponsorshipOnly) {
    return {
      version: TARGET_HOTEL_DEMAND_AVAILABILITY_V1,
      ok: false,
      reason: "sponsorship_or_monitor_only_not_group_room_demand",
      competitorHostDetected: false,
      overflowEvidenced: false,
      sponsorshipOnly: true,
    };
  }

  // Stay-to-play / housing-bureau paths remain eligible even when geography is stretch.
  const hasHousingChannel = /\b(stay[- ]to[- ]play|housing bureau|housing system)\b/i.test(blob);
  if (STRETCH_ONLY_RE.test(blob) && !hasHousingChannel) {
    return {
      version: TARGET_HOTEL_DEMAND_AVAILABILITY_V1,
      ok: false,
      reason: "stretch_only_without_evidenced_housing_channel",
      competitorHostDetected: false,
      overflowEvidenced: false,
      sponsorshipOnly: false,
    };
  }

  const explicitHostContracted = opportunity.hostHotelContracted === true;
  const competitorHostDetected =
    explicitHostContracted ||
    COMPETITOR_HOST_RE.test(blob) ||
    (Array.isArray(opportunity.competitors) &&
      opportunity.competitors.some((row) =>
        /contracted host|named home-base|named home base|historical.*host|host hotel/i.test(
          text(row?.reason),
        ),
      ));

  const openTargetPath =
    opportunity.externalRoomBlocksAvailable === true ||
    OPEN_TARGET_PATH_RE.test(blob);
  const hypothesizedOverflowOnly =
    HYPOTHESIZED_OVERFLOW_ONLY_RE.test(blob) && !openTargetPath;

  // If the named host string clearly is the target hotel, do not treat as competitor-only.
  const tokens = targetHotelTokens(hotel, opportunity);
  const hostMentionsTarget =
    tokens.length > 0 &&
    tokens.some((token) => blob.toLowerCase().includes(token)) &&
    /\b(named|contracted|host hotel|home base|official hotel)\b/i.test(blob);

  if (hypothesizedOverflowOnly && competitorHostDetected && !hostMentionsTarget) {
    return {
      version: TARGET_HOTEL_DEMAND_AVAILABILITY_V1,
      ok: false,
      reason: "competitor_host_overflow_only_hypothesized",
      competitorHostDetected: true,
      overflowEvidenced: false,
      sponsorshipOnly: false,
    };
  }

  if (competitorHostDetected && !hostMentionsTarget && !openTargetPath) {
    return {
      version: TARGET_HOTEL_DEMAND_AVAILABILITY_V1,
      ok: false,
      reason: "competitor_or_official_host_without_evidenced_overflow",
      competitorHostDetected: true,
      overflowEvidenced: false,
      sponsorshipOnly: false,
    };
  }

  if (
    explicitHostContracted &&
    opportunity.externalRoomBlocksAvailable !== true
  ) {
    return {
      version: TARGET_HOTEL_DEMAND_AVAILABILITY_V1,
      ok: false,
      reason: "host_contracted_overflow_unproven",
      competitorHostDetected: true,
      overflowEvidenced: false,
      sponsorshipOnly: false,
    };
  }

  return {
    version: TARGET_HOTEL_DEMAND_AVAILABILITY_V1,
    ok: true,
    reason: competitorHostDetected
      ? "competitor_host_with_evidenced_open_target_path"
      : "no_competitor_host_block",
    competitorHostDetected,
    overflowEvidenced: openTargetPath,
    sponsorshipOnly: false,
  };
}

export default {
  TARGET_HOTEL_DEMAND_AVAILABILITY_V1,
  assessTargetHotelDemandAvailability,
};
