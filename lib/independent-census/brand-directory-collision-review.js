/**
 * Phase 4S — Choice property ID collision review (report-only).
 * Disambiguates multiple Choice URLs linked to one OSM candidate.
 */

import {
  nameSimilarity,
  normalizeKey,
  normalizeText,
  normalizeCountry,
  countriesMatch,
  citiesMatch,
  websiteHost,
} from "./match-current-census.js";

export const COLLISION_RECOMMENDATION = {
  SELECT_SINGLE: "select_single_choice_property_id",
  MANUAL: "needs_manual_property_review",
  REJECT: "reject_collision_group",
  HOLD_METADATA: "hold_for_property_page_metadata",
};

const SCORE_WEIGHTS = {
  country: 20,
  city: 25,
  brand: 15,
  urlText: 20,
  website: 10,
  evidence: 10,
};

const SELECT_MIN_SCORE = 65;
const SELECT_MIN_GAP = 12;
const REJECT_MAX_SCORE = 40;

/** City slugs that often indicate sitemap placeholder / non-geographic paths. */
const SUSPICIOUS_CITY_SLUGS = new Set([
  "resville",
  "hotels",
  "hotel",
  "properties",
  "property",
]);

/**
 * @param {object} extractReport
 */
export function indexPropertyUrlExtract(extractReport) {
  const byPropertyId = new Map();
  for (const r of extractReport.propertyRows || []) {
    if (r.propertyId) byPropertyId.set(r.propertyId, r);
  }
  return byPropertyId;
}

/**
 * @param {object} matchReport
 */
export function indexMatchReportByCandidate(matchReport) {
  const byCandidate = new Map();
  for (const m of matchReport.matches || []) {
    const cid = m.matchedCandidateRecordId;
    if (!cid) continue;
    if (!byCandidate.has(cid)) byCandidate.set(cid, []);
    byCandidate.get(cid).push(m);
  }
  return byCandidate;
}

/**
 * @param {object} promotionReview
 */
export function identifyCollisionGroups(promotionReview) {
  return (promotionReview.reviewRows || []).filter((r) => {
    const ids = r.allChoicePropertyIds || [];
    return ids.length > 1 || (r.evidenceCount || 0) > 1;
  });
}

function slugToLabel(slug) {
  return normalizeText(String(slug || "").replace(/-/g, " "));
}

function propertyIdFromOsmWebsite(url) {
  const m = String(url || "").match(/\/([a-z]{2}\d{3,})\/?$/i);
  return m ? m[1].toLowerCase() : "";
}

function isSuspiciousCitySlug(slug) {
  const k = normalizeKey(slug);
  return !k || SUSPICIOUS_CITY_SLUGS.has(k) || k.length < 3;
}

/**
 * @param {object} osm
 * @param {object} choice
 * @param {object|null} matchRow
 */
export function scoreChoicePropertyCollision(osm, choice, matchRow) {
  const breakdown = {};
  let score = 0;

  const choiceCountry =
    choice.inferredCountry || slugToLabel(choice.countryOrRegionSegment);
  if (countriesMatch(osm.country, choiceCountry)) {
    score += SCORE_WEIGHTS.country;
    breakdown.country = SCORE_WEIGHTS.country;
  } else {
    breakdown.country = 0;
  }

  const cityLabel = slugToLabel(choice.citySlug);
  let citySim = nameSimilarity(cityLabel, osm.city);
  if (citiesMatch(cityLabel, osm.city)) citySim = Math.max(citySim, 0.85);
  const cityPts = Math.round(citySim * SCORE_WEIGHTS.city);
  score += cityPts;
  breakdown.city = cityPts;

  const brandSim = nameSimilarity(osm.brand || osm.name, choice.brand);
  const brandPts = Math.round(brandSim * SCORE_WEIGHTS.brand);
  score += brandPts;
  breakdown.brand = brandPts;

  const urlHaystack = `${choice.propertyUrl || ""} ${cityLabel} ${choice.brand || ""}`;
  const urlSim = nameSimilarity(osm.name, urlHaystack);
  const urlPts = Math.round(urlSim * SCORE_WEIGHTS.urlText);
  score += urlPts;
  breakdown.urlText = urlPts;

  let websitePts = 0;
  const osmHost = websiteHost(osm.website);
  const choiceHost = websiteHost(choice.propertyUrl);
  const osmPid = propertyIdFromOsmWebsite(osm.website);
  if (osmHost && choiceHost && osmHost === choiceHost) {
    websitePts += 5;
  }
  if (osmPid && choice.propertyId && normalizeKey(osmPid) === normalizeKey(choice.propertyId)) {
    websitePts = SCORE_WEIGHTS.website;
  } else if (
    osm.website &&
    choice.propertyId &&
    normalizeKey(osm.website).includes(normalizeKey(choice.propertyId))
  ) {
    websitePts = Math.max(websitePts, 8);
  }
  score += websitePts;
  breakdown.website = websitePts;

  const evidenceRaw = Number(matchRow?.candidateMatchScore) || 0;
  const evidencePts = Math.round((evidenceRaw / 83) * SCORE_WEIGHTS.evidence);
  score += evidencePts;
  breakdown.evidence = evidencePts;

  return {
    collisionScore: Math.min(100, score),
    scoreBreakdown: breakdown,
    citySimilarity: citySim,
    brandSimilarity: brandSim,
    urlSimilarity: urlSim,
    evidenceMatchScore: evidenceRaw,
    osmWebsitePropertyId: osmPid,
  };
}

/**
 * @param {Array<object>} ranked — sorted by collisionScore desc
 * @param {object} osm
 */
export function resolveCollisionGroupRecommendation(ranked, osm) {
  if (!ranked.length) {
    return {
      recommendation: COLLISION_RECOMMENDATION.REJECT,
      selectedPropertyId: "",
      humanNotes: "No Choice property IDs in collision group.",
      readyForEvidenceCleanup: "no",
    };
  }

  const top = ranked[0];
  const second = ranked[1];
  const gap = second ? top.collisionScore - second.collisionScore : 99;
  const allLow = ranked.every((r) => r.collisionScore < REJECT_MAX_SCORE);
  const allSuspiciousCity = ranked.every((r) =>
    isSuspiciousCitySlug(r.choiceCitySlug)
  );
  const anyCityMatch = ranked.some((r) => r.citySimilarity >= 0.55);
  const osmPid = propertyIdFromOsmWebsite(osm.website);
  const osmPidInList =
    osmPid && ranked.some((r) => normalizeKey(r.choicePropertyId) === normalizeKey(osmPid));

  const notes = [];

  if (allLow) {
    return {
      recommendation: COLLISION_RECOMMENDATION.REJECT,
      selectedPropertyId: "",
      humanNotes:
        "All Choice property ID scores below threshold; likely URL-only false positives.",
      readyForEvidenceCleanup: "no",
    };
  }

  if (allSuspiciousCity && !anyCityMatch && !osmPidInList) {
    notes.push(
      `OSM city "${osm.city}" does not align with Choice city slugs (e.g. resville); OSM website property id ${osmPid || "n/a"} not in collision set.`
    );
    return {
      recommendation: COLLISION_RECOMMENDATION.HOLD_METADATA,
      selectedPropertyId: osmPidInList ? top.choicePropertyId : "",
      humanNotes: notes.join(" "),
      readyForEvidenceCleanup: "no",
    };
  }

  if (
    top.collisionScore >= SELECT_MIN_SCORE &&
    gap >= SELECT_MIN_GAP &&
    (top.citySimilarity >= 0.5 || osmPidInList)
  ) {
    if (osmPid && !osmPidInList && top.choicePropertyId !== osmPid) {
      notes.push(
        `OSM website suggests property id ${osmPid}; top ranked is ${top.choicePropertyId} — verify before evidence cleanup.`
      );
    }
    return {
      recommendation: COLLISION_RECOMMENDATION.SELECT_SINGLE,
      selectedPropertyId: top.choicePropertyId,
      humanNotes:
        notes.join(" ") ||
        `Auto-selected highest collision score (${top.collisionScore}); gap ${gap} vs runner-up.`,
      readyForEvidenceCleanup: notes.length ? "after_manual_confirm" : "yes",
    };
  }

  if (osmPidInList) {
    const osmMatch = ranked.find(
      (r) => normalizeKey(r.choicePropertyId) === normalizeKey(osmPid)
    );
    if (osmMatch) {
      return {
        recommendation: COLLISION_RECOMMENDATION.SELECT_SINGLE,
        selectedPropertyId: osmMatch.choicePropertyId,
        humanNotes: `OSM website URL aligns with Choice property id ${osmPid}.`,
        readyForEvidenceCleanup: "yes",
      };
    }
  }

  notes.push(
    `Top score ${top.collisionScore} (gap ${gap}); no clear winner — review city/brand/URL alignment.`
  );
  return {
    recommendation: COLLISION_RECOMMENDATION.MANUAL,
    selectedPropertyId: top.choicePropertyId,
    humanNotes: notes.join(" "),
    readyForEvidenceCleanup: "no",
  };
}

/**
 * @param {object} opts
 */
export function buildCollisionReview(opts) {
  const {
    promotionReview,
    matchReport,
    propertyUrlById,
    batchId,
  } = opts;

  const matchByCandidate = indexMatchReportByCandidate(matchReport);
  const groups = identifyCollisionGroups(promotionReview);
  const detailRows = [];
  const groupSummaries = [];

  for (const group of groups) {
    const cid = group.candidateAirtableRecordId;
    const propertyIds = [
      ...new Set(group.allChoicePropertyIds || [group.choicePropertyId].filter(Boolean)),
    ];
    const matchRows = matchByCandidate.get(cid) || [];

    const osm = {
      recordId: cid,
      name: group.candidateHotelName,
      city: group.candidateCity,
      country: group.candidateCountry,
      website: group.candidateWebsite,
      latitude: group.candidateLatitude,
      longitude: group.candidateLongitude,
      brand: group.candidateBrand || group.choiceBrandSetupBrand,
      sourceUrl: group.osmSourceUrl,
    };

    const scored = [];

    for (const pid of propertyIds) {
      const extract = propertyUrlById.get(pid) || {};
      const matchRow =
        matchRows.find((m) => m.propertyId === pid) ||
        matchRows.find((m) => normalizeKey(m.propertyId) === normalizeKey(pid)) ||
        null;

      const choice = {
        propertyId: pid,
        propertyUrl: extract.propertyUrl || matchRow?.propertyUrl || "",
        brand: extract.matchedBrandSetupBrand || matchRow?.brandSetupBrand || "",
        citySlug: extract.citySlug || matchRow?.citySlug || "",
        countryOrRegionSegment:
          extract.countryOrRegionSegment || matchRow?.inferredCountry || "",
        inferredCountry: extract.inferredCountry || matchRow?.inferredCountry || "",
      };

      const s = scoreChoicePropertyCollision(osm, choice, matchRow);
      scored.push({
        choicePropertyId: pid,
        choicePropertyUrl: choice.propertyUrl,
        choiceBrand: choice.brand,
        choiceCitySlug: choice.citySlug,
        choiceCountrySegment: choice.countryOrRegionSegment,
        ...s,
        matchConfidence: matchRow?.candidateMatchConfidence || "",
        matchReason: matchRow?.candidateMatchReason || "",
      });
    }

    scored.sort((a, b) => b.collisionScore - a.collisionScore);
    scored.forEach((row, idx) => {
      row.collisionRank = idx + 1;
    });

    const resolution = resolveCollisionGroupRecommendation(scored, osm);

    groupSummaries.push({
      osmCandidateRecordId: cid,
      osmCandidateName: osm.name,
      osmCity: osm.city,
      osmCountry: osm.country,
      osmWebsite: osm.website,
      choicePropertyIdCount: propertyIds.length,
      evidenceCount: group.evidenceCount,
      promotionEligibility: group.promotionEligibility,
      promotionRecommendation: group.promotionRecommendation,
      collisionRecommendation: resolution.recommendation,
      recommendedSelectedPropertyId: resolution.selectedPropertyId,
      humanNotes: resolution.humanNotes,
      readyForEvidenceCleanup: resolution.readyForEvidenceCleanup,
      topCollisionScore: scored[0]?.collisionScore ?? 0,
      scoreGapTopTwo:
        scored.length > 1 ? scored[0].collisionScore - scored[1].collisionScore : null,
      rankedPropertyIds: scored.map((r) => r.choicePropertyId),
    });

    for (const row of scored) {
      detailRows.push({
        osmCandidateRecordId: cid,
        osmCandidateName: osm.name,
        osmCity: osm.city,
        osmCountry: osm.country,
        osmWebsite: osm.website,
        choicePropertyId: row.choicePropertyId,
        choicePropertyUrl: row.choicePropertyUrl,
        choiceBrand: row.choiceBrand,
        choiceCitySlug: row.choiceCitySlug,
        choiceCountrySegment: row.choiceCountrySegment,
        evidenceMatchScore: row.evidenceMatchScore,
        collisionScore: row.collisionScore,
        collisionRank: row.collisionRank,
        recommendedSelectedPropertyId: resolution.selectedPropertyId,
        collisionRecommendation: resolution.recommendation,
        humanNotes: resolution.humanNotes,
        readyForEvidenceCleanupPromotion: resolution.readyForEvidenceCleanup,
        scoreBreakdown: row.scoreBreakdown,
        citySimilarity: row.citySimilarity,
        matchConfidence: row.matchConfidence,
        matchReason: row.matchReason,
        batchId,
      });
    }
  }

  return { groups: groupSummaries, detailRows };
}

export function summarizeCollisionReview(groups, detailRows) {
  const summary = {
    collisionGroupsReviewed: groups.length,
    totalChoicePropertyIdsInCollisions: detailRows.length,
    select_single_choice_property_id: 0,
    needs_manual_property_review: 0,
    reject_collision_group: 0,
    hold_for_property_page_metadata: 0,
    readyForEvidenceCleanupYes: 0,
    readyForEvidenceCleanupNo: 0,
    readyForEvidenceCleanupAfterManual: 0,
  };

  for (const g of groups) {
    summary[g.collisionRecommendation] =
      (summary[g.collisionRecommendation] || 0) + 1;
    if (g.readyForEvidenceCleanup === "yes") summary.readyForEvidenceCleanupYes++;
    else if (g.readyForEvidenceCleanup === "after_manual_confirm") {
      summary.readyForEvidenceCleanupAfterManual++;
    } else summary.readyForEvidenceCleanupNo++;
  }

  return summary;
}

export const COLLISION_CSV_COLUMNS = [
  "OSM Candidate Record ID",
  "OSM Candidate Name",
  "OSM City",
  "OSM Country",
  "OSM Website",
  "Choice Property ID",
  "Choice Property URL",
  "Choice Brand",
  "Choice City Slug",
  "Choice Country Segment",
  "Evidence Match Score",
  "Collision Score",
  "Collision Rank",
  "Recommended Selected Property ID",
  "Collision Recommendation",
  "Human Notes",
  "Ready For Evidence Cleanup / Promotion",
];

export function collisionDetailToCsv(r) {
  return {
    "OSM Candidate Record ID": r.osmCandidateRecordId,
    "OSM Candidate Name": r.osmCandidateName,
    "OSM City": r.osmCity,
    "OSM Country": r.osmCountry,
    "OSM Website": r.osmWebsite,
    "Choice Property ID": r.choicePropertyId,
    "Choice Property URL": r.choicePropertyUrl,
    "Choice Brand": r.choiceBrand,
    "Choice City Slug": r.choiceCitySlug,
    "Choice Country Segment": r.choiceCountrySegment,
    "Evidence Match Score": r.evidenceMatchScore,
    "Collision Score": r.collisionScore,
    "Collision Rank": r.collisionRank,
    "Recommended Selected Property ID": r.recommendedSelectedPropertyId,
    "Collision Recommendation": r.collisionRecommendation,
    "Human Notes": r.humanNotes,
    "Ready For Evidence Cleanup / Promotion": r.readyForEvidenceCleanupPromotion,
  };
}
