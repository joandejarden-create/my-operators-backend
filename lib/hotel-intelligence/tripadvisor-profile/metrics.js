/**
 * Tripadvisor hotel profile / market intelligence — derived metrics (read-only).
 * Does not write Airtable / census. Does not treat Tripadvisor as authoritative.
 */

export const TRIPADVISOR_PROFILE_INTEL_VERSION =
  "tripadvisor-hotel-profile-intelligence-v1";

/**
 * Higher = stronger competitive standing within the Tripadvisor ranking universe.
 *
 * Formula:
 *   competitive_rank_percentile =
 *     100 * (rankingDenominator - rankingPosition + 1) / rankingDenominator
 *
 * Examples:
 *   #1 of 100 → 100
 *   #7 of 105 → ~94.3  (near top)
 *   #20 of 34 → ~44.1
 *   #34 of 34 → ~2.9
 *
 * Caveats: denominator geography varies (neighborhood vs city vs market).
 * Never compare percentiles across different rankingString geographies blindly.
 *
 * @param {number|string} rankingPosition
 * @param {number|string} rankingDenominator
 * @returns {number|null}
 */
export function competitiveRankPercentile(rankingPosition, rankingDenominator) {
  const pos = Number(rankingPosition);
  const den = Number(rankingDenominator);
  if (!Number.isFinite(pos) || !Number.isFinite(den) || den <= 0 || pos <= 0) {
    return null;
  }
  if (pos > den) return null;
  return Number((((den - pos + 1) / den) * 100).toFixed(1));
}

/**
 * Share of reviews that are 4–5 bubble (directional polarization/consistency aid).
 * @param {{count1?:number,count2?:number,count3?:number,count4?:number,count5?:number}} hist
 */
export function ratingHistogramShares(hist = {}) {
  const c1 = Number(hist.count1) || 0;
  const c2 = Number(hist.count2) || 0;
  const c3 = Number(hist.count3) || 0;
  const c4 = Number(hist.count4) || 0;
  const c5 = Number(hist.count5) || 0;
  const total = c1 + c2 + c3 + c4 + c5;
  if (!total) return null;
  return {
    total,
    share_1_2: Number((((c1 + c2) / total) * 100).toFixed(1)),
    share_3: Number(((c3 / total) * 100).toFixed(1)),
    share_4_5: Number((((c4 + c5) / total) * 100).toFixed(1)),
  };
}

/**
 * Normalize amenity labels for set comparison (case-fold, trim).
 * @param {string[]} amenities
 * @returns {Set<string>}
 */
export function normalizeAmenitySet(amenities = []) {
  const out = new Set();
  for (const a of amenities || []) {
    const k = String(a || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    if (k) out.add(k);
  }
  return out;
}

/**
 * Amenities present in ≥ threshold of comps but missing on subject.
 * Diagnostic only — does NOT recommend CapEx.
 *
 * @param {string[]} subjectAmenities
 * @param {Array<{amenities?:string[]}>} comps
 * @param {number} [threshold=0.7]
 */
export function amenityGapsVsComps(subjectAmenities, comps = [], threshold = 0.7) {
  const subject = normalizeAmenitySet(subjectAmenities);
  const n = comps.length;
  if (!n) return { gaps: [], differentiators: [], n_comps: 0 };

  const counts = new Map();
  for (const c of comps) {
    for (const a of normalizeAmenitySet(c.amenities || [])) {
      counts.set(a, (counts.get(a) || 0) + 1);
    }
  }

  const gaps = [];
  const differentiators = [];
  for (const [amenity, count] of counts) {
    const share = count / n;
    if (!subject.has(amenity) && share >= threshold) {
      gaps.push({
        amenity,
        comps_with: count,
        comps_total: n,
        comps_share: Number((share * 100).toFixed(1)),
      });
    }
  }
  for (const a of subject) {
    const count = counts.get(a) || 0;
    const share = count / n;
    if (share <= 1 - threshold) {
      differentiators.push({
        amenity: a,
        comps_with: count,
        comps_total: n,
        comps_share: Number((share * 100).toFixed(1)),
      });
    }
  }
  gaps.sort((a, b) => b.comps_share - a.comps_share);
  differentiators.sort((a, b) => a.comps_share - b.comps_share);
  return { gaps, differentiators, n_comps: n };
}

/**
 * Median of numeric values.
 * @param {number[]} values
 */
export function median(values = []) {
  const arr = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (!arr.length) return null;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
}

/**
 * Build a read-only owner vs comps snapshot from Tripadvisor Actor items.
 * @param {object} subject Tripadvisor hotel item
 * @param {object[]} comps
 */
export function buildOwnerCompSnapshot(subject, comps = []) {
  const subRankPct = competitiveRankPercentile(
    subject.rankingPosition,
    subject.rankingDenominator
  );
  const compRankPcts = comps
    .map((c) =>
      competitiveRankPercentile(c.rankingPosition, c.rankingDenominator)
    )
    .filter((v) => v != null);
  const subHist = ratingHistogramShares(subject.ratingHistogram || {});
  const amenity = amenityGapsVsComps(
    subject.amenities || [],
    comps,
    0.7
  );

  const categoryScores = (subject.categoryReviewScores || []).map((c) => ({
    category: c.categoryName,
    score: c.roundedScore ?? c.score,
  }));

  const compCategory = {};
  for (const c of comps) {
    for (const cat of c.categoryReviewScores || []) {
      const name = cat.categoryName;
      if (!compCategory[name]) compCategory[name] = [];
      const v = Number(cat.roundedScore ?? cat.score);
      if (Number.isFinite(v)) compCategory[name].push(v);
    }
  }
  const categoryVsComps = categoryScores.map((c) => {
    const med = median(compCategory[c.category] || []);
    return {
      category: c.category,
      hotel: c.score,
      comp_median: med,
      delta:
        c.score != null && med != null
          ? Number((Number(c.score) - med).toFixed(2))
          : null,
    };
  });

  return {
    version: TRIPADVISOR_PROFILE_INTEL_VERSION,
    production_writes: false,
    subject: {
      tripadvisor_id: subject.id != null ? String(subject.id) : null,
      name: subject.name,
      webUrl: subject.webUrl || null,
      rating: subject.rating ?? null,
      numberOfReviews: subject.numberOfReviews ?? null,
      hotelClass: subject.hotelClass ?? null,
      numberOfRooms: subject.numberOfRooms ?? null,
      rankingPosition: subject.rankingPosition ?? null,
      rankingDenominator:
        subject.rankingDenominator != null
          ? Number(subject.rankingDenominator)
          : null,
      rankingString: subject.rankingString || null,
      competitive_rank_percentile: subRankPct,
      priceLevel: subject.priceLevel || null,
      priceRange: subject.priceRange || null,
      histogram_shares: subHist,
      category_scores: categoryScores,
      amenity_count: Array.isArray(subject.amenities)
        ? subject.amenities.length
        : 0,
    },
    comps: comps.map((c) => ({
      tripadvisor_id: c.id != null ? String(c.id) : null,
      name: c.name,
      rating: c.rating ?? null,
      numberOfReviews: c.numberOfReviews ?? null,
      hotelClass: c.hotelClass ?? null,
      numberOfRooms: c.numberOfRooms ?? null,
      rankingString: c.rankingString || null,
      competitive_rank_percentile: competitiveRankPercentile(
        c.rankingPosition,
        c.rankingDenominator
      ),
      priceLevel: c.priceLevel || null,
      amenity_count: Array.isArray(c.amenities) ? c.amenities.length : 0,
    })),
    vs_comps: {
      rating_hotel: subject.rating ?? null,
      rating_comp_median: median(comps.map((c) => Number(c.rating))),
      reviews_hotel: subject.numberOfReviews ?? null,
      reviews_comp_median: median(
        comps.map((c) => Number(c.numberOfReviews))
      ),
      rank_pct_hotel: subRankPct,
      rank_pct_comp_median: median(compRankPcts),
      rooms_hotel: subject.numberOfRooms ?? null,
      rooms_comp_median: median(comps.map((c) => Number(c.numberOfRooms))),
      category_vs_comps: categoryVsComps,
      amenity_gaps_majority_comps: amenity.gaps.slice(0, 15),
      amenity_differentiators: amenity.differentiators.slice(0, 10),
    },
    caveats: [
      "Tripadvisor ranking geographies differ (city vs neighborhood); compare rankingString carefully.",
      "priceLevel / priceRange are directional OTA/stay quotes — not ADR or RevPAR.",
      "Amenity gaps are diagnostics, not CapEx recommendations.",
      "Room counts remain candidates until independently verified.",
      "Derived metrics are Dealality intelligence, not Tripadvisor-supplied scores.",
    ],
  };
}
