/**
 * Tripadvisor Profile Pack — normalized observational intelligence (not census core).
 */

import { competitiveRankPercentile, ratingHistogramShares } from "./metrics.js";
import { TRIPADVISOR_CENSUS_PROFILE_PACK_VERSION } from "./census-map.js";

/**
 * @param {object} taItem Actor hotel item
 * @param {object} [matchMeta]
 */
export function buildTripadvisorProfilePack(taItem, matchMeta = {}) {
  const rankingDenominator =
    taItem?.rankingDenominator != null
      ? Number(taItem.rankingDenominator)
      : null;
  const rankingPosition =
    taItem?.rankingPosition != null ? Number(taItem.rankingPosition) : null;
  const guestRankingPercentile = competitiveRankPercentile(
    rankingPosition,
    rankingDenominator
  );

  return {
    schema_version: TRIPADVISOR_CENSUS_PROFILE_PACK_VERSION,
    layer: "hotel_intelligence",
    not_census_core: true,
    provider: "tripadvisor_apify",
    provider_property_id: taItem?.id != null ? String(taItem.id) : null,
    source_url: taItem?.webUrl || null,
    retrieved_at: matchMeta.retrieved_at || new Date().toISOString(),
    match_confidence: matchMeta.confidence || matchMeta.match_confidence || null,
    match_score: matchMeta.score ?? null,

    identity: {
      name: taItem?.name || null,
      local_name: taItem?.localName || null,
      type: taItem?.type || null,
      category: taItem?.category || null,
      subcategories: Array.isArray(taItem?.subcategories)
        ? taItem.subcategories
        : [],
    },

    guest_reputation: {
      rating: taItem?.rating ?? null,
      number_of_reviews: taItem?.numberOfReviews ?? null,
      rating_histogram: taItem?.ratingHistogram || null,
      histogram_shares: ratingHistogramShares(taItem?.ratingHistogram || {}),
      category_review_scores: Array.isArray(taItem?.categoryReviewScores)
        ? taItem.categoryReviewScores.map((c) => ({
            category: c.categoryName,
            score: c.score ?? null,
            rounded_score: c.roundedScore ?? null,
          }))
        : [],
    },

    competitive_standing: {
      ranking_position: rankingPosition,
      ranking_denominator: rankingDenominator,
      ranking_string: taItem?.rankingString || null,
      ranking_source: taItem?.rankingSource || null,
      /** Owner-facing name for competitive_rank_percentile */
      guest_ranking_percentile: guestRankingPercentile,
      guest_ranking_percentile_formula:
        "100 * (rankingDenominator - rankingPosition + 1) / rankingDenominator",
      guest_ranking_percentile_note:
        "Higher = stronger Tripadvisor guest ranking within rankingString geography — not overall hotel performance.",
    },

    product: {
      hotel_class: taItem?.hotelClass ?? null,
      hotel_class_attribution: taItem?.hotelClassAttribution || null,
      number_of_rooms_candidate:
        taItem?.numberOfRooms != null ? Number(taItem.numberOfRooms) : null,
      amenities: Array.isArray(taItem?.amenities) ? taItem.amenities : [],
      traveler_choice_award: taItem?.travelerChoiceAward ?? null,
      photo_count: taItem?.photoCount ?? null,
    },

    price_position_directional: {
      price_level: taItem?.priceLevel || null,
      price_range: taItem?.priceRange || null,
      not_adr_or_revpar: true,
    },

    contact_observations: {
      website: taItem?.website || null,
      phone: taItem?.phone || null,
      email: taItem?.email || null,
      address: taItem?.address || null,
      latitude: taItem?.latitude ?? null,
      longitude: taItem?.longitude ?? null,
    },
  };
}

/**
 * Coverage flags for a pack.
 */
export function profilePackCoverageFlags(pack) {
  const g = pack?.guest_reputation || {};
  const c = pack?.competitive_standing || {};
  const p = pack?.product || {};
  const price = pack?.price_position_directional || {};
  const contact = pack?.contact_observations || {};
  return {
    RATING_COVERAGE: g.rating != null,
    REVIEW_COUNT_COVERAGE: g.number_of_reviews != null,
    RANKING_COVERAGE: c.ranking_position != null,
    RANK_DENOMINATOR_COVERAGE: c.ranking_denominator != null,
    GUEST_RANK_PERCENTILE_COVERAGE: c.guest_ranking_percentile != null,
    AMENITY_COVERAGE: Array.isArray(p.amenities) && p.amenities.length > 0,
    CATEGORY_SCORE_COVERAGE:
      Array.isArray(g.category_review_scores) &&
      g.category_review_scores.length > 0,
    PRICE_POSITION_COVERAGE: Boolean(price.price_level || price.price_range),
    HOTEL_CLASS_COVERAGE: p.hotel_class != null && p.hotel_class !== "",
    CONTACT_COVERAGE: Boolean(
      contact.website || contact.phone || contact.email
    ),
  };
}
