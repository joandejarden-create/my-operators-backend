/**
 * Hard consumer/travel fluff exclusion for Market Alerts (Pre-Opening Capture V1).
 * Reject before Airtable — business implication required to keep borderline travel topics.
 */

export const CONSUMER_FLUFF_REASONS = Object.freeze({
  CONSUMER_TRAVEL: "CONSUMER_TRAVEL",
  LISTICLE: "LISTICLE",
  POINTS_HACKING: "POINTS_HACKING",
  FNB_AMENITY: "FNB_AMENITY",
  AWARD_FLUFF: "AWARD_FLUFF",
  GENERIC_AIRLINE: "GENERIC_AIRLINE",
  GENERIC_CRUISE: "GENERIC_CRUISE",
  GENERIC_TRAVEL_APP: "GENERIC_TRAVEL_APP",
  OTHER_NOISE: "OTHER_NOISE",
});

/** Material hotel-business implication that can rescue an otherwise consumer-adjacent story. */
const MATERIAL_HOTEL_BUSINESS_RE =
  /\b(?:(?:materially|significantly|expected to)\s+(?:increase|boost|drive|support|add)|hotel\s+demand|room\s+supply|additional\s+hotel\s+rooms?|hotel\s+pipeline|resort\s+development|operating\s+(?:license|licence|requirement)|franchise\s+economics|distribution\s+fee|ota\s+commission|elite\s+qualification|loyalty\s+program\s+(?:change|threshold)|repositioning|\$[\d.,]+\s*(?:m|mm|million|billion).{0,40}(?:renovat|reposition|capex|hotel)|mandatory\s+(?:hotel|operating)|destination\s+tax|convention\s+center|cruise\s+terminal.{0,80}hotel|airport.{0,60}(?:hotel|resort|rooms?)|airlift.{0,60}(?:hotel|demand|visitors?))\b/i;

const LISTICLE_RE =
  /\b(?:top\s*\d+|best\s+\d+\s+hotels?|best hotels?(?!\s+points)|best resorts?(?! for families)|hotel roundup|hotels? (?:to|for) (?:book|stay|visit)|romantic hotels?|family hotels?|where to stay|things to do|readers['']?\s*choice|tripadvisor.?style|best resorts for)\b/i;

const POINTS_HACKING_RE =
  /\b(?:points?\s+redemptions?|points? (?:redemption|hack|maxim)|hotel points?|maximize (?:your )?(?:points|miles)|sweet spots?|how to (?:use|redeem|maximize).{0,40}(?:points|miles|bonvoy|hilton honors)|best (?:ways? to )?(?:redeem|use) .{0,30}(?:points|miles)|best hotel points|travel hacks?|credit[- ]card points|travel card advice)\b/i;

const FNB_AMENITY_RE =
  /\b(?:(?:hotel|resort).{0,40}(?:chef|menu|tasting menu|culinary|spa treatment|wellness package|amenity)|(?:chef|spa|wellness).{0,40}(?:launches?|unveils?|introduces?)|(?:launches?|unveils?).{0,40}(?:menu|spa treatment|wellness package|signature treatment)|new summer menu|new tasting menu)\b/i;

const AWARD_FLUFF_RE =
  /\b(?:wins? (?:a |an )?(?:award|design award|travel award|luxury award|world travel award)|named .{0,40}(?:best hotel|hotel of the year)|hotel rankings?|design awards?|luxury travel awards?|celebrity stays?|influencer)\b/i;

const CONSUMER_TRAVEL_RE =
  /\b(?:tour operator|weekend trips?|vacation packages?|guided trips?|tour packages?|travel guide|city guide|destination guide|flash sales?|seasonal packages?|consumer itinerar|weroad|best places to stay)\b/i;

const GENERIC_AIRLINE_RE =
  /\b(?:airline|airlines|airways)\b.{0,80}\b(?:launches?|adds?|announces?).{0,60}\b(?:route|flight|service)\b/i;

const GENERIC_CRUISE_RE =
  /\b(?:cruise (?:line|ship)|cruise)\b.{0,80}\b(?:launches?|unveils?|debuts?).{0,40}\b(?:ship|itinerary|vessel)\b/i;

const GENERIC_TRAVEL_APP_RE =
  /\b(?:travel app|booking app|hotel booking (?:app|feature)|app launches?.{0,40}hotel booking)\b/i;

/**
 * @param {{ title?: string, summary?: string }} input
 * @returns {{ reject: boolean, reason: string|null }}
 */
export function assessConsumerFluff(input = {}) {
  const title = String(input.title || "").trim();
  const summary = String(input.summary || "").trim();
  const text = `${title} ${summary}`;
  if (!title) return { reject: false, reason: null };

  const material = MATERIAL_HOTEL_BUSINESS_RE.test(text);

  // More specific reason classes first.
  if (POINTS_HACKING_RE.test(text) && !material) {
    return { reject: true, reason: CONSUMER_FLUFF_REASONS.POINTS_HACKING };
  }
  if (CONSUMER_TRAVEL_RE.test(text) && !material) {
    return { reject: true, reason: CONSUMER_FLUFF_REASONS.CONSUMER_TRAVEL };
  }
  // "Top 25 Markets" in RevPAR/performance reports is not a consumer listicle.
  const hotelPerformance =
    /\b(?:revpar|occupancy|adr\b|hotel\s+performance|lodging\s+performance|hotel\s+results?|room\s+rates?|booking\s+pace)\b/i.test(
      text
    );
  if (LISTICLE_RE.test(text) && !material && !hotelPerformance) {
    return { reject: true, reason: CONSUMER_FLUFF_REASONS.LISTICLE };
  }
  if (FNB_AMENITY_RE.test(text) && !material) {
    return { reject: true, reason: CONSUMER_FLUFF_REASONS.FNB_AMENITY };
  }
  if (AWARD_FLUFF_RE.test(text) && !material) {
    return { reject: true, reason: CONSUMER_FLUFF_REASONS.AWARD_FLUFF };
  }
  if (GENERIC_TRAVEL_APP_RE.test(text) && !material) {
    return { reject: true, reason: CONSUMER_FLUFF_REASONS.GENERIC_TRAVEL_APP };
  }
  if (GENERIC_AIRLINE_RE.test(text) && !material) {
    return { reject: true, reason: CONSUMER_FLUFF_REASONS.GENERIC_AIRLINE };
  }
  if (GENERIC_CRUISE_RE.test(text) && !material) {
    return { reject: true, reason: CONSUMER_FLUFF_REASONS.GENERIC_CRUISE };
  }

  return { reject: false, reason: null };
}

export function isConsumerFluff(input = {}) {
  return assessConsumerFluff(input).reject;
}
