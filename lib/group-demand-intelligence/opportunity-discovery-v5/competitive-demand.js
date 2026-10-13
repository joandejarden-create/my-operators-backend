/**
 * buildGdiCompetitiveDemandLead() — competitor hotel use → future compete thesis.
 */

const YEAR_RE = /\b(20(?:2[3-9]|3[0-2]))\b/g;
const SIZE_RE = /\b(\d{2,4})\s*(?:rooms?|attendees|delegates|participants|guests)\b/i;

/**
 * @returns competitive demand lead structure or null if too thin
 */
export function buildGdiCompetitiveDemandLead(signal = {}, hotel = {}, opts = {}) {
  const title = String(signal.title || "").trim();
  const snippet = String(signal.snippet || signal.summaryWhat || "").trim();
  const blob = `${title} ${snippet} ${signal.officialSource || ""}`;
  const competitorHotel =
    signal.competitorHotel ||
    (hotel.competitors || []).find((c) => blob.toLowerCase().includes(String(c).toLowerCase().slice(0, 12))) ||
    null;

  if (!competitorHotel && !/host hotel|official hotel|room block|stayed at|accommodated/i.test(blob)) {
    return null;
  }

  const years = [...new Set((blob.match(YEAR_RE) || []).map(String))].sort();
  const sizeMatch = blob.match(SIZE_RE);
  const org =
    String(signal.organizationName || signal.organization || "").trim() ||
    title.split(/[|\-—:]/)[0].trim().slice(0, 120);

  const meetingType = (() => {
    if (/congress|congrès|congreso/i.test(blob)) return "CONGRESS";
    if (/conference|summit|symposium/i.test(blob)) return "CONFERENCE";
    if (/kickoff|advisory|investigator|pharma|medical/i.test(blob)) return "PHARMA_OR_MEETING";
    if (/tournament|team|federation|sports/i.test(blob)) return "SPORTS";
    if (/crew|production|broadcast/i.test(blob)) return "CREW";
    if (/incentive|retreat|corporate/i.test(blob)) return "CORPORATE_GROUP";
    return "GROUP_PROGRAM";
  })();

  const lodgingEvidence =
    /host hotel|official hotel|room block|housing|accommodation|hébergement|alojamiento/i.test(blob);
  const repeatHistory = years.length >= 2 ? `YEARS_SEEN:${years.join("|")}` : years[0] || null;

  const futureCycle = years.find((y) => Number(y) >= 2026) || null;
  const canCompeteNext =
    lodgingEvidence &&
    Boolean(org) &&
    (Boolean(futureCycle) || /annual|series|recurring|édition|rotation/i.test(blob));

  return {
    id: signal.id || `comp_${hotel.hotelKey}_${String(org).slice(0, 24).replace(/\W+/g, "_")}`,
    hotelKey: hotel.hotelKey,
    hotelId: hotel.hotelId,
    organization: org,
    historicHotel: competitorHotel || "COMPETITOR_HOST_PATTERN",
    meetingType,
    yearDate: years.join("|") || null,
    groupSize: sizeMatch ? sizeMatch[1] : null,
    agencyOrganizer: signal.agency || signal.organizer || null,
    repeatHistory,
    lodgingEvidence: lodgingEvidence ? "HINT" : "NONE",
    whyCompetitorLikelyFit: competitorHotel
      ? `${competitorHotel} appears in public host/housing/group context for ${org || title}.`
      : "Host/official hotel pattern without named competitor property.",
    futureCycle: futureCycle || (/annual|rotation|recurring/i.test(blob) ? "RECURRING_EXPECTED" : null),
    buyerPath: signal.buyerEntity || org || null,
    couldTargetCompeteNext: canCompeteNext,
    targetHotelCompeteThesis: canCompeteNext
      ? `${hotel.hotelName || hotel.label} could compete for next cycle of ${org || title} if buyer/housing decision is open and fit holds.`
      : `${hotel.hotelName || hotel.label} needs future cycle + lodging buyer confirmation before compete thesis.`,
    officialSource: signal.officialSource || signal.source || signal.url,
    engine: "COMPETITIVE",
    opportunityType: "OVERFLOW_HOUSING",
    title: title || `${org} @ ${competitorHotel || "competitor"}`,
    summaryWhat: snippet.slice(0, 280),
    hotelOpportunityThesis: canCompeteNext
      ? `${hotel.hotelName || hotel.label}: competitive displacement / overflow vs ${competitorHotel || "host hotel"} for ${org}.`
      : `${title} shows competitor lodging pattern — confirm repeat cycle before pursuit.`,
    hotelFitScore: hotel.defaultFitScore ?? 48,
    gdiDiscoveryVersion: "opportunity_discovery_v5",
  };
}
