/**
 * Phase 4B — Human promotion review (report-only, read-only staging).
 */

import {
  CANDIDATE_FIELDS,
  EVIDENCE_FIELDS,
  SOURCE_TYPES,
} from "./fields.js";
import {
  BRAND_DIRECTORY_EVIDENCE_CAPTURED_BY,
  CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY,
  EVIDENCE_CAPTURED_BY,
} from "./evidence-apply.js";
import { normalizePropertyUrl } from "./choice-property-id-reconciliation.js";
import { WIKIDATA_ENTITY_BASE } from "./sources/wikidata.js";
import {
  nameSimilarity,
  distanceMeters,
  parseCoords,
  websiteHost,
  normalizeText,
  normalizeKey,
  citiesMatch,
} from "./match-current-census.js";

export const PROMOTION_ELIGIBILITY = {
  ELIGIBLE_FOR_REVIEW: "eligible_for_review",
  NEEDS_MANUAL_RESEARCH: "needs_manual_research",
  POSSIBLE_DUPLICATE: "possible_duplicate",
  INSUFFICIENT_CORE_FIELDS: "insufficient_core_fields",
};

export const PROMOTION_RECOMMENDATION = {
  PROMOTE_AFTER_REVIEW: "promote_after_review",
  REVIEW_BEFORE_PROMOTE: "review_before_promote",
  DO_NOT_PROMOTE_YET: "do_not_promote_yet",
};

const STRONG_MATCH_SCORE = 65;
const NAME_MISMATCH_THRESHOLD = 0.55;
const NEARBY_DUPLICATE_METERS = 150;

/**
 * Parse Evidence Name: 4A|{batchId}|{qid}|{osmSourceRecordId} or 4Q|{batchId}|{propertyId}|{candidateId}
 */
export function parseEvidenceName(name) {
  const meta = parseEvidenceNameMeta(name);
  if (!meta) return null;
  if (meta.type === "wikidata") {
    return {
      evidenceBatchId: meta.evidenceBatchId,
      wikidataQid: meta.wikidataQid,
      osmSourceRecordId: meta.osmSourceRecordId,
    };
  }
  return null;
}

export function parseEvidenceNameMeta(name) {
  const parts = String(name || "").split("|");
  if (parts.length < 4) return null;
  if (parts[0] === "4A") {
    return {
      type: "wikidata",
      evidenceBatchId: parts[1],
      wikidataQid: parts[2],
      osmSourceRecordId: parts[3],
    };
  }
  if (parts[0] === "4Q") {
    return {
      type: "brand_directory",
      evidenceBatchId: parts[1],
      choicePropertyId: parts[2],
      matchedCandidateRecordId: parts[3],
    };
  }
  if (parts[0] === "4U") {
    return {
      type: "choice_property_id_reconciliation",
      evidenceBatchId: parts[1],
      choicePropertyId: parts[2],
      matchedCandidateRecordId: parts[3],
    };
  }
  return null;
}

/**
 * Parse Phase 4U corrected Choice property ID evidence text.
 */
export function parseCorrectedChoiceEvidenceText(text) {
  const t = String(text || "");
  const pick = (label) => {
    const re = new RegExp(`^${label}:\\s*(.+)$`, "im");
    const m = t.match(re);
    return m ? normalizeText(m[1]) : "";
  };

  return {
    parentCompany: pick("Parent Company") || "Choice Hotels International",
    choicePropertyId: pick("Choice Property ID"),
    choicePropertyUrl: pick("Matched Choice Property URL"),
    matchType: pick("Match Type"),
    matchedChoiceBrand: pick("Matched Choice Brand"),
    matchedChoiceCountry: pick("Matched Choice Country"),
    matchedChoiceCitySlug: pick("Matched Choice City Slug"),
    osmCandidateRecordId: pick("OSM Candidate Record ID"),
    osmCandidateName: pick("OSM Candidate Name"),
    osmWebsite: pick("OSM Website"),
    osmCity: pick("OSM City"),
    osmCountry: pick("OSM Country"),
    reconciliationConfidence: pick("Reconciliation confidence").toLowerCase(),
    reconciliationRecommendedAction: pick("Reconciliation recommended action"),
    reconciliationNotes: pick("Reconciliation notes"),
  };
}

function propertyIdFromOsmWebsite(url) {
  const m = String(url || "").match(/\/([a-z]{2}\d{2,8})\/?$/i);
  return m ? normalizeKey(m[1]) : "";
}

/**
 * Parse Phase 4Q brand-directory evidence text.
 */
export function parseBrandDirectoryEvidenceText(text) {
  const t = String(text || "");
  const pick = (label) => {
    const re = new RegExp(`^${label}:\\s*(.+)$`, "im");
    const m = t.match(re);
    return m ? normalizeText(m[1]) : "";
  };

  return {
    parentCompany: pick("Parent Company"),
    brandSetupBrand: pick("Brand Setup Brand"),
    choicePropertyUrl: pick("Choice Property URL"),
    choicePropertyId: pick("Choice Property ID"),
    inferredCountry: pick("Inferred Country"),
    citySlug: pick("City Slug"),
    candidateMatchConfidence: pick("Candidate Match Confidence").toLowerCase(),
    candidateMatchReason: pick("Candidate Match Reason"),
    matchedOsmCandidateRecordId: pick("Matched OSM Candidate Record ID"),
    matchedOsmCandidateName: pick("Matched OSM Candidate Name"),
  };
}

/**
 * Extract structured lines from Phase 4A evidence text.
 */
export function parseEvidenceText(text) {
  const t = String(text || "");
  const pick = (label) => {
    const re = new RegExp(`^${label}:\\s*(.+)$`, "im");
    const m = t.match(re);
    return m ? normalizeText(m[1]) : "";
  };

  return {
    wikidataQid: pick("QID"),
    wikidataLabel: pick("Label"),
    wikidataCity: pick("Location") || "",
    wikidataCountry: pick("Country"),
    coordinates: pick("Coordinates"),
    wikidataWebsite: pick("Official website"),
    wikidataOperator: pick("Operator"),
    wikidataOwner: pick("Owner"),
    wikipediaUrl: pick("Wikipedia"),
    entityUrl: pick("Entity URL"),
    osmSourceRecordId: pick("Source Record ID"),
    osmName: pick("Name"),
    matchConfidence: pick("Confidence"),
    matchScoreFromText: pick("Score"),
    matchReasonFromText: pick("Reason"),
  };
}

/**
 * @param {object} candidate Airtable-mapped candidate
 * @param {Array<object>} evidenceList
 */
export function mapCandidateRecord(record) {
  const f = record.fields || record;
  let payload = {};
  try {
    const raw = f[CANDIDATE_FIELDS.rawPayloadJson];
    payload = typeof raw === "string" ? JSON.parse(raw) : raw || {};
  } catch {
    payload = {};
  }

  const lat = f[CANDIDATE_FIELDS.rawLatitude];
  const lng = f[CANDIDATE_FIELDS.rawLongitude];

  return {
    airtableRecordId: record.id || record.airtableRecordId,
    sourceRecordId: f[CANDIDATE_FIELDS.sourceRecordId] || "",
    sourceName: f[CANDIDATE_FIELDS.sourceName] || "",
    sourceType: f[CANDIDATE_FIELDS.sourceType] || "",
    sourceUrl: f[CANDIDATE_FIELDS.sourceUrl] || "",
    sourceLicense: f[CANDIDATE_FIELDS.sourceLicense] || "",
    importBatchId: f[CANDIDATE_FIELDS.importBatchId] || "",
    rawHotelName: normalizeText(f[CANDIDATE_FIELDS.rawHotelName]),
    rawCity: normalizeText(f[CANDIDATE_FIELDS.rawCity]),
    rawCountry: normalizeText(f[CANDIDATE_FIELDS.rawCountry]),
    rawWebsite: normalizeText(f[CANDIDATE_FIELDS.rawWebsite]),
    rawPhone: normalizeText(f[CANDIDATE_FIELDS.rawPhone]),
    rawBrand: normalizeText(f[CANDIDATE_FIELDS.rawBrand]),
    rawLatitude: Number.isFinite(lat) ? lat : null,
    rawLongitude: Number.isFinite(lng) ? lng : null,
    coords: parseCoords(lat, lng),
    candidateDedupeKey: f[CANDIDATE_FIELDS.candidateDedupeKey] || "",
    reviewStatus: f[CANDIDATE_FIELDS.reviewStatus] || "",
    possibleMatchConfidence: f[CANDIDATE_FIELDS.possibleMatchConfidence] || "",
    recommendedAction: f[CANDIDATE_FIELDS.recommendedAction] || "",
    osmTourismTag: payload.tourism || payload._osmTourismTag || "",
    payload,
  };
}

export function mapEvidenceRecord(record) {
  const f = record.fields || record;
  const nameMeta = parseEvidenceNameMeta(f.Name);

  if (nameMeta?.type === "choice_property_id_reconciliation") {
    const parsed = parseCorrectedChoiceEvidenceText(f[EVIDENCE_FIELDS.evidenceText]);
    const candidateIds = f[EVIDENCE_FIELDS.candidate] || [];
    const linkIds = Array.isArray(candidateIds)
      ? candidateIds
      : candidateIds
        ? [candidateIds]
        : [];

    return {
      evidenceSource: "choice_property_id_reconciliation",
      airtableRecordId: record.id || record.airtableRecordId,
      name: f.Name || "",
      evidenceBatchId: nameMeta.evidenceBatchId,
      choicePropertyId: nameMeta.choicePropertyId || parsed.choicePropertyId,
      choicePropertyUrl: f[EVIDENCE_FIELDS.evidenceUrl] || parsed.choicePropertyUrl,
      matchedCandidateRecordId:
        nameMeta.matchedCandidateRecordId || parsed.osmCandidateRecordId,
      evidenceUrl: f[EVIDENCE_FIELDS.evidenceUrl] || "",
      evidenceText: f[EVIDENCE_FIELDS.evidenceText] || "",
      matchScore: Number(f[EVIDENCE_FIELDS.matchScore]) || 0,
      matchReason: f[EVIDENCE_FIELDS.matchReason] || "",
      capturedAt: f[EVIDENCE_FIELDS.capturedAt] || "",
      capturedBy: f[EVIDENCE_FIELDS.capturedBy] || "",
      candidateLinkIds: linkIds,
      parsed,
    };
  }

  if (nameMeta?.type === "brand_directory") {
    const parsed = parseBrandDirectoryEvidenceText(f[EVIDENCE_FIELDS.evidenceText]);
    const candidateIds = f[EVIDENCE_FIELDS.candidate] || [];
    const linkIds = Array.isArray(candidateIds)
      ? candidateIds
      : candidateIds
        ? [candidateIds]
        : [];

    return {
      evidenceSource: "brand_directory",
      airtableRecordId: record.id || record.airtableRecordId,
      name: f.Name || "",
      evidenceBatchId: nameMeta.evidenceBatchId,
      choicePropertyId:
        nameMeta.choicePropertyId || parsed.choicePropertyId,
      choicePropertyUrl:
        f[EVIDENCE_FIELDS.evidenceUrl] || parsed.choicePropertyUrl,
      matchedCandidateRecordId:
        nameMeta.matchedCandidateRecordId || parsed.matchedOsmCandidateRecordId,
      evidenceUrl: f[EVIDENCE_FIELDS.evidenceUrl] || "",
      evidenceText: f[EVIDENCE_FIELDS.evidenceText] || "",
      matchScore: Number(f[EVIDENCE_FIELDS.matchScore]) || 0,
      matchReason: f[EVIDENCE_FIELDS.matchReason] || "",
      capturedAt: f[EVIDENCE_FIELDS.capturedAt] || "",
      capturedBy: f[EVIDENCE_FIELDS.capturedBy] || "",
      candidateLinkIds: linkIds,
      parsed,
    };
  }

  const parsed = parseEvidenceText(f[EVIDENCE_FIELDS.evidenceText]);
  const wikidataNameMeta = parseEvidenceName(f.Name);

  const qid =
    wikidataNameMeta?.wikidataQid ||
    parsed.wikidataQid ||
    qidFromUrl(f[EVIDENCE_FIELDS.evidenceUrl]);

  const candidateIds = f[EVIDENCE_FIELDS.candidate] || [];
  const linkIds = Array.isArray(candidateIds)
    ? candidateIds
    : candidateIds
      ? [candidateIds]
      : [];

  return {
    evidenceSource: "wikidata",
    airtableRecordId: record.id || record.airtableRecordId,
    name: f.Name || "",
    evidenceBatchId: wikidataNameMeta?.evidenceBatchId || "",
    wikidataQid: qid,
    osmSourceRecordId: wikidataNameMeta?.osmSourceRecordId || parsed.osmSourceRecordId,
    evidenceUrl: f[EVIDENCE_FIELDS.evidenceUrl] || "",
    evidenceText: f[EVIDENCE_FIELDS.evidenceText] || "",
    matchScore: Number(f[EVIDENCE_FIELDS.matchScore]) || 0,
    matchReason: f[EVIDENCE_FIELDS.matchReason] || "",
    capturedAt: f[EVIDENCE_FIELDS.capturedAt] || "",
    capturedBy: f[EVIDENCE_FIELDS.capturedBy] || "",
    candidateLinkIds: linkIds,
    parsed,
  };
}

function qidFromUrl(url) {
  const m = String(url).match(/\/wiki\/(Q\d+)/i);
  return m ? m[1].toUpperCase() : "";
}

export function generateDealalityHotelIdPlaceholder(candidate, country) {
  const co = normalizeKey(country || candidate.rawCountry || "xx").slice(0, 2);
  const slug = normalizeKey(candidate.rawHotelName || "unknown")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 40);
  const tail = (candidate.sourceRecordId || "id")
    .replace(/[^a-zA-Z0-9]+/g, "")
    .slice(-8);
  return `dh-${co || "xx"}-${slug || "hotel"}-${tail}`;
}

/**
 * @param {object} candidate
 * @param {object} evidence primary evidence row
 * @param {object} context duplicate flags, etc.
 */
export function assessPromotionReview(candidate, evidence, context = {}) {
  const notes = [];
  const parsed = evidence.parsed || parseEvidenceText(evidence.evidenceText);
  const wikidataName = parsed.wikidataLabel || evidence.wikidataQid;
  const wikidataUrl =
    evidence.evidenceUrl || `${WIKIDATA_ENTITY_BASE}${evidence.wikidataQid}`;

  const hasName = !!normalizeKey(candidate.rawHotelName);
  const hasCountry = !!normalizeKey(candidate.rawCountry);
  const hasCoords =
    Number.isFinite(candidate.rawLatitude) && Number.isFinite(candidate.rawLongitude);
  const hasWikidataEvidence =
    !!evidence.wikidataQid && evidence.capturedBy === EVIDENCE_CAPTURED_BY;
  const matchScore = Number(evidence.matchScore) || 0;
  const strongMatch = matchScore >= STRONG_MATCH_SCORE;

  let eligibility = PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW;
  let recommendation = PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW;
  let riskLevel = "low";

  if (!hasName || !hasCountry || !hasCoords) {
    eligibility = PROMOTION_ELIGIBILITY.INSUFFICIENT_CORE_FIELDS;
    recommendation = PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET;
    riskLevel = "high";
    if (!hasName) notes.push("Missing candidate name.");
    if (!hasCountry) notes.push("Missing candidate country.");
    if (!hasCoords) notes.push("Missing candidate coordinates.");
  } else if (context.duplicateQid || context.duplicateNearbyName) {
    eligibility = PROMOTION_ELIGIBILITY.POSSIBLE_DUPLICATE;
    recommendation = PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET;
    riskLevel = "high";
    if (context.duplicateQid) notes.push("Wikidata QID linked to multiple OSM candidates.");
    if (context.duplicateNearbyName) notes.push("Nearby OSM candidate with similar normalized name.");
  } else {
    if (!hasWikidataEvidence) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      notes.push("Missing Phase 4A Wikidata evidence.");
      riskLevel = "medium";
    }
    if (!strongMatch) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(`Match score ${matchScore} below strong threshold (${STRONG_MATCH_SCORE}).`);
      riskLevel = "medium";
    }

    if (!normalizeKey(candidate.rawCity)) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push("Candidate city missing.");
      if (riskLevel === "low") riskLevel = "medium";
    }

    const ns = nameSimilarity(candidate.rawHotelName, wikidataName);
    if (wikidataName && ns < NAME_MISMATCH_THRESHOLD) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(`Wikidata label differs from OSM name (similarity ${ns.toFixed(2)}).`);
      riskLevel = "medium";
    }

    const candHost = websiteHost(candidate.rawWebsite);
    const wdHost = websiteHost(parsed.wikidataWebsite);
    if (candHost && wdHost && candHost !== wdHost) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(`Website host mismatch: OSM=${candHost}, Wikidata=${wdHost}.`);
      riskLevel = "medium";
    }

    if (parsed.wikidataOperator && candidate.rawBrand) {
      const op = normalizeKey(parsed.wikidataOperator);
      const br = normalizeKey(candidate.rawBrand);
      if (op && br && !op.includes(br) && !br.includes(op) && nameSimilarity(op, br) < 0.4) {
        notes.push("Operator vs candidate brand may conflict.");
        if (eligibility === PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW) {
          eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
          recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
          riskLevel = "medium";
        }
      }
    }

    if (
      parsed.wikidataOwner &&
      /private|unknown|yes|no/i.test(parsed.wikidataOwner) === false &&
      parsed.wikidataOwner.length > 80
    ) {
      notes.push("Wikidata owner value looks unusual; verify manually.");
    }
  }

  if (eligibility !== PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW) {
    if (recommendation === PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW) {
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
    }
  }

  if (recommendation === PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET) {
    riskLevel = "high";
  }

  const sourceCount = 2;
  const verifiedWebsite =
    candidate.rawWebsite || parsed.wikidataWebsite || "";
  const verifiedPhone = candidate.rawPhone || "";
  const verifiedCity = candidate.rawCity || parsed.wikidataCity || "";
  const verifiedName = candidate.rawHotelName || wikidataName;

  const proposedVerified = {
    dealalityHotelId: generateDealalityHotelIdPlaceholder(
      candidate,
      candidate.rawCountry
    ),
    verifiedHotelName: verifiedName,
    normalizedHotelName: normalizeKey(verifiedName),
    verifiedCountry: candidate.rawCountry,
    verifiedCity,
    verifiedLatitude: candidate.rawLatitude,
    verifiedLongitude: candidate.rawLongitude,
    verifiedWebsite,
    verifiedPhone,
    primarySourceName: candidate.sourceName || "OpenStreetMap",
    primarySourceType: SOURCE_TYPES.OSM,
    primarySourceUrl: candidate.sourceUrl || "",
    secondarySourceCount: 1,
    sourceConfidence: strongMatch && hasWikidataEvidence ? "medium" : "low",
    verificationStatus: "pending_review",
    approvedBy: "",
    approvedAt: "",
    approvalNotes: "",
    promotedFromCandidate: candidate.airtableRecordId,
    wikidataQid: evidence.wikidataQid,
    wikidataSourceUrl: wikidataUrl,
  };

  return {
    eligibility,
    recommendation,
    reviewRiskLevel: riskLevel,
    humanReviewNotes: notes.join(" ") || "Two-source OSM+Wikidata validation present; human sign-off required.",
    wikidataName,
    wikidataUrl,
    parsed,
    sourceCount,
    proposedVerified,
  };
}

/**
 * Build review rows grouped by candidate (one row per candidate with primary evidence).
 */
export function buildPromotionReviewRows(evidenceRecords, candidateById) {
  const byCandidate = new Map();

  for (const ev of evidenceRecords) {
    const cid = ev.candidateLinkIds[0];
    if (!cid) continue;
    if (!byCandidate.has(cid)) byCandidate.set(cid, []);
    byCandidate.get(cid).push(ev);
  }

  const qidToCandidates = new Map();
  const nameGeoIndex = [];

  for (const [cid, c] of candidateById.entries()) {
    const evs = byCandidate.get(cid) || [];
    for (const ev of evs) {
      if (ev.wikidataQid) {
        if (!qidToCandidates.has(ev.wikidataQid)) {
          qidToCandidates.set(ev.wikidataQid, new Set());
        }
        qidToCandidates.get(ev.wikidataQid).add(cid);
      }
    }
    if (c.coords && normalizeKey(c.rawHotelName)) {
      nameGeoIndex.push(c);
    }
  }

  const rows = [];

  for (const [cid, candidate] of candidateById.entries()) {
    const evidenceList = byCandidate.get(cid) || [];
    if (!evidenceList.length) continue;

    const primary = evidenceList.sort(
      (a, b) => (b.matchScore || 0) - (a.matchScore || 0)
    )[0];

    const duplicateQid =
      primary.wikidataQid &&
      (qidToCandidates.get(primary.wikidataQid)?.size || 0) > 1;

    let duplicateNearbyName = false;
    for (const other of nameGeoIndex) {
      if (other.airtableRecordId === cid) continue;
      const ns = nameSimilarity(candidate.rawHotelName, other.rawHotelName);
      if (ns >= 0.85) {
        const d = distanceMeters(candidate.coords, other.coords);
        if (d != null && d <= NEARBY_DUPLICATE_METERS) {
          duplicateNearbyName = true;
          break;
        }
      }
    }

    const assessment = assessPromotionReview(candidate, primary, {
      duplicateQid,
      duplicateNearbyName,
    });

    const p = assessment.parsed;

    rows.push({
      candidateAirtableRecordId: cid,
      sourceRecordId: candidate.sourceRecordId,
      osmSourceUrl: candidate.sourceUrl,
      wikidataQid: primary.wikidataQid,
      wikidataUrl: assessment.wikidataUrl,
      candidateHotelName: candidate.rawHotelName,
      wikidataHotelName: assessment.wikidataName,
      candidateCountry: candidate.rawCountry,
      candidateCity: candidate.rawCity,
      candidateLatitude: candidate.rawLatitude,
      candidateLongitude: candidate.rawLongitude,
      candidateWebsite: candidate.rawWebsite,
      candidatePhone: candidate.rawPhone,
      candidateBrand: candidate.rawBrand,
      wikidataWebsite: p.wikidataWebsite,
      wikidataOperator: p.wikidataOperator,
      wikidataOwner: p.wikidataOwner,
      wikidataWikipediaUrl: p.wikipediaUrl,
      matchScore: primary.matchScore,
      matchReason: primary.matchReason,
      evidenceCount: evidenceList.length,
      sourceCount: assessment.sourceCount,
      promotionEligibility: assessment.eligibility,
      promotionRecommendation: assessment.recommendation,
      reviewRiskLevel: assessment.reviewRiskLevel,
      humanReviewNotes: assessment.humanReviewNotes,
      evidenceAirtableRecordId: primary.airtableRecordId,
      proposedVerified: assessment.proposedVerified,
      candidateImportBatchId: candidate.importBatchId,
      evidenceBatchId: primary.evidenceBatchId,
    });
  }

  return rows.sort((a, b) => (b.matchScore || 0) - (a.matchScore || 0));
}

export function summarizePromotionReview(rows) {
  const summary = {
    totalReviewRows: rows.length,
    eligible_for_review: 0,
    needs_manual_research: 0,
    possible_duplicate: 0,
    insufficient_core_fields: 0,
    promote_after_review: 0,
    review_before_promote: 0,
    do_not_promote_yet: 0,
    reviewRiskLow: 0,
    reviewRiskMedium: 0,
    reviewRiskHigh: 0,
  };

  for (const r of rows) {
    summary[r.promotionEligibility] = (summary[r.promotionEligibility] || 0) + 1;
    summary[r.promotionRecommendation] = (summary[r.promotionRecommendation] || 0) + 1;
    if (r.reviewRiskLevel === "low") summary.reviewRiskLow++;
    else if (r.reviewRiskLevel === "medium") summary.reviewRiskMedium++;
    else if (r.reviewRiskLevel === "high") summary.reviewRiskHigh++;
  }

  return summary;
}

const MEDIUM_MATCH_SCORE = 50;

function inferCandidateMatchConfidence(evidence) {
  const fromText = evidence.parsed?.candidateMatchConfidence || "";
  if (fromText === "high" || fromText === "medium" || fromText === "low") {
    return fromText;
  }
  const score = Number(evidence.matchScore) || 0;
  if (score >= STRONG_MATCH_SCORE) return "high";
  if (score >= MEDIUM_MATCH_SCORE) return "medium";
  return "low";
}

function isGenericOsmHotelName(name) {
  const n = normalizeKey(name);
  if (!n || n.length < 4) return true;
  const generic = new Set([
    "hotel",
    "hostel",
    "motel",
    "inn",
    "lodging",
    "guesthouse",
    "resort",
    "qualityinn",
    "comfortinn",
    "sleepinn",
    "econolodge",
  ]);
  if (generic.has(n)) return true;
  if (n.split(/\s+/).length === 1 && n.length < 12) return true;
  return false;
}

/**
 * Phase 4R — assess OSM + Choice brand-directory evidence for promotion review.
 */
export function assessBrandDirectoryPromotionReview(candidate, evidence, context = {}) {
  const notes = [];
  const parsed = evidence.parsed || parseBrandDirectoryEvidenceText(evidence.evidenceText);
  const matchConfidence = inferCandidateMatchConfidence(evidence);
  const matchScore = Number(evidence.matchScore) || 0;

  const hasName = !!normalizeKey(candidate.rawHotelName);
  const hasCountry = !!normalizeKey(candidate.rawCountry);
  const hasCoords =
    Number.isFinite(candidate.rawLatitude) && Number.isFinite(candidate.rawLongitude);
  const hasChoiceEvidence =
    evidence.capturedBy === BRAND_DIRECTORY_EVIDENCE_CAPTURED_BY &&
    !!evidence.choicePropertyUrl;
  const hasChoiceBrand = !!normalizeKey(parsed.brandSetupBrand);
  const hasChoicePropertyId = !!normalizeKey(
    evidence.choicePropertyId || parsed.choicePropertyId
  );
  const strongConfidence =
    matchConfidence === "high" || matchScore >= STRONG_MATCH_SCORE;
  const mediumOrBetter =
    matchConfidence === "high" ||
    matchConfidence === "medium" ||
    matchScore >= MEDIUM_MATCH_SCORE;

  let eligibility = PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW;
  let recommendation = PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW;
  let riskLevel = "low";

  if (!hasName || !hasCountry || !hasCoords) {
    eligibility = PROMOTION_ELIGIBILITY.INSUFFICIENT_CORE_FIELDS;
    recommendation = PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET;
    riskLevel = "high";
    if (!hasName) notes.push("Missing candidate name.");
    if (!hasCountry) notes.push("Missing candidate country.");
    if (!hasCoords) notes.push("Missing candidate coordinates.");
  } else if (
    context.duplicateChoicePropertyId ||
    context.multipleChoiceUrlsOnCandidate ||
    context.duplicateNearbyName
  ) {
    eligibility = PROMOTION_ELIGIBILITY.POSSIBLE_DUPLICATE;
    recommendation = PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET;
    riskLevel = "high";
    if (context.duplicateChoicePropertyId) {
      notes.push("Choice property ID linked to multiple OSM candidates.");
    }
    if (context.multipleChoiceUrlsOnCandidate) {
      notes.push("Multiple Choice property URLs linked to same OSM candidate.");
    }
    if (context.duplicateNearbyName) {
      notes.push("Nearby OSM candidate with similar normalized name.");
    }
  } else {
    if (!hasChoiceEvidence) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      notes.push("Missing Phase 4Q Choice brand-directory evidence.");
      riskLevel = "medium";
    }
    if (!hasChoiceBrand || !hasChoicePropertyId) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push("Choice brand or property ID missing from evidence.");
      riskLevel = "medium";
    }
    if (!mediumOrBetter) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(
        `Match confidence ${matchConfidence} / score ${matchScore} below medium threshold.`
      );
      riskLevel = "medium";
    } else if (!strongConfidence) {
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(`Medium-confidence match (score ${matchScore}); verify OSM ↔ Choice alignment.`);
      if (riskLevel === "low") riskLevel = "medium";
    }

    if (!normalizeKey(candidate.rawCity) && !normalizeKey(parsed.citySlug)) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push("Candidate city missing (city slug only in Choice URL).");
      if (riskLevel === "low") riskLevel = "medium";
    }

    if (isGenericOsmHotelName(candidate.rawHotelName)) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push("OSM hotel name is generic or very short.");
      riskLevel = "medium";
    }

    const brandLabel = parsed.brandSetupBrand || "";
    const nsBrand = nameSimilarity(candidate.rawHotelName, brandLabel);
    if (brandLabel && nsBrand < NAME_MISMATCH_THRESHOLD) {
      const nsOsmChoice = nameSimilarity(
        candidate.rawHotelName,
        parsed.matchedOsmCandidateName || ""
      );
      if (nsOsmChoice < NAME_MISMATCH_THRESHOLD) {
        eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
        recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
        notes.push(
          `Choice brand "${brandLabel}" does not align clearly with OSM name (similarity ${nsBrand.toFixed(2)}).`
        );
        riskLevel = "medium";
      }
    }

    const candHost = websiteHost(candidate.rawWebsite);
    if (
      candHost &&
      candHost !== "choicehotels.com" &&
      !candHost.endsWith(".choicehotels.com")
    ) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(`OSM website host (${candHost}) differs from Choice official URL.`);
      riskLevel = "medium";
    }
  }

  if (eligibility !== PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW) {
    if (recommendation === PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW) {
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
    }
  }
  if (recommendation === PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET) {
    riskLevel = "high";
  }

  const sourceCount = 2;
  const verifiedName = candidate.rawHotelName || parsed.matchedOsmCandidateName;
  const verifiedCity =
    candidate.rawCity ||
    (parsed.citySlug ? parsed.citySlug.replace(/-/g, " ") : "");
  const verifiedBrandLabel = parsed.brandSetupBrand || candidate.rawBrand || "";

  const proposedVerified = {
    dealalityHotelId: generateDealalityHotelIdPlaceholder(
      candidate,
      candidate.rawCountry || parsed.inferredCountry
    ),
    verifiedHotelName: verifiedName,
    normalizedHotelName: normalizeKey(verifiedName),
    verifiedCountry: candidate.rawCountry || parsed.inferredCountry,
    verifiedCity,
    verifiedLatitude: candidate.rawLatitude,
    verifiedLongitude: candidate.rawLongitude,
    verifiedWebsite: evidence.choicePropertyUrl || candidate.rawWebsite || "",
    verifiedPhone: candidate.rawPhone || "",
    verifiedBrandLabel,
    parentCompany: parsed.parentCompany || "Choice Hotels International",
    primarySourceName: "Choice Hotels Sitemap",
    primarySourceType: SOURCE_TYPES.BRAND_DIRECTORY,
    primarySourceUrl: evidence.choicePropertyUrl || "",
    secondarySourceName: candidate.sourceName || "OpenStreetMap",
    secondarySourceType: SOURCE_TYPES.OSM,
    secondarySourceUrl: candidate.sourceUrl || "",
    secondarySourceCount: 1,
    sourceConfidence:
      strongConfidence && hasChoiceEvidence ? "medium" : "low",
    verificationStatus: "pending_review",
    approvedBy: "",
    approvedAt: "",
    approvalNotes: "",
    promotedFromCandidate: candidate.airtableRecordId,
    choicePropertyId: evidence.choicePropertyId || parsed.choicePropertyId,
    choicePropertyUrl: evidence.choicePropertyUrl || "",
    candidateMatchConfidence: matchConfidence,
  };

  return {
    eligibility,
    recommendation,
    reviewRiskLevel: riskLevel,
    humanReviewNotes:
      notes.join(" ") ||
      "Two-source OSM+Choice brand-directory validation present; human sign-off required.",
    matchConfidence,
    parsed,
    sourceCount,
    proposedVerified,
  };
}

/**
 * Phase 4R — Build promotion review rows for Choice brand-directory evidence.
 */
export function buildBrandDirectoryPromotionReviewRows(evidenceRecords, candidateById) {
  const byCandidate = new Map();

  for (const ev of evidenceRecords) {
    const cid = ev.candidateLinkIds[0] || ev.matchedCandidateRecordId;
    if (!cid) continue;
    if (!byCandidate.has(cid)) byCandidate.set(cid, []);
    byCandidate.get(cid).push(ev);
  }

  const propertyIdToCandidates = new Map();
  const nameGeoIndex = [];

  for (const [cid, c] of candidateById.entries()) {
    const evs = byCandidate.get(cid) || [];
    for (const ev of evs) {
      const pid = ev.choicePropertyId || ev.parsed?.choicePropertyId;
      if (pid) {
        if (!propertyIdToCandidates.has(pid)) propertyIdToCandidates.set(pid, new Set());
        propertyIdToCandidates.get(pid).add(cid);
      }
    }
    if (c.coords && normalizeKey(c.rawHotelName)) {
      nameGeoIndex.push(c);
    }
  }

  const rows = [];

  for (const [cid, candidate] of candidateById.entries()) {
    const evidenceList = byCandidate.get(cid) || [];
    if (!evidenceList.length) continue;

    const primary = evidenceList.sort(
      (a, b) => (b.matchScore || 0) - (a.matchScore || 0)
    )[0];

    const pid = primary.choicePropertyId || primary.parsed?.choicePropertyId;
    const duplicateChoicePropertyId =
      pid && (propertyIdToCandidates.get(pid)?.size || 0) > 1;
    const multipleChoiceUrlsOnCandidate = evidenceList.length > 1;

    let duplicateNearbyName = false;
    for (const other of nameGeoIndex) {
      if (other.airtableRecordId === cid) continue;
      const ns = nameSimilarity(candidate.rawHotelName, other.rawHotelName);
      if (ns >= 0.85) {
        const d = distanceMeters(candidate.coords, other.coords);
        if (d != null && d <= NEARBY_DUPLICATE_METERS) {
          duplicateNearbyName = true;
          break;
        }
      }
    }

    const assessment = assessBrandDirectoryPromotionReview(candidate, primary, {
      duplicateChoicePropertyId,
      multipleChoiceUrlsOnCandidate,
      duplicateNearbyName,
    });

    const p = assessment.parsed;

    rows.push({
      candidateAirtableRecordId: cid,
      sourceRecordId: candidate.sourceRecordId,
      osmSourceUrl: candidate.sourceUrl,
      choicePropertyUrl: primary.choicePropertyUrl || p.choicePropertyUrl,
      choicePropertyId: primary.choicePropertyId || p.choicePropertyId,
      candidateHotelName: candidate.rawHotelName,
      choiceBrandSetupBrand: p.brandSetupBrand,
      parentCompany: p.parentCompany || "Choice Hotels International",
      candidateCountry: candidate.rawCountry || p.inferredCountry,
      candidateCity: candidate.rawCity,
      candidateLatitude: candidate.rawLatitude,
      candidateLongitude: candidate.rawLongitude,
      candidateWebsite: candidate.rawWebsite,
      candidatePhone: candidate.rawPhone,
      candidateBrand: candidate.rawBrand,
      candidateMatchConfidence: assessment.matchConfidence,
      matchScore: primary.matchScore,
      matchReason: primary.matchReason,
      evidenceCount: evidenceList.length,
      sourceCount: assessment.sourceCount,
      promotionEligibility: assessment.eligibility,
      promotionRecommendation: assessment.recommendation,
      reviewRiskLevel: assessment.reviewRiskLevel,
      humanReviewNotes: assessment.humanReviewNotes,
      evidenceAirtableRecordId: primary.airtableRecordId,
      proposedVerified: assessment.proposedVerified,
      candidateImportBatchId: candidate.importBatchId,
      evidenceBatchId: primary.evidenceBatchId,
      allChoicePropertyIds: evidenceList.map(
        (e) => e.choicePropertyId || e.parsed?.choicePropertyId
      ),
    });
  }

  return rows.sort((a, b) => (b.matchScore || 0) - (a.matchScore || 0));
}

/**
 * Phase 4V — assess OSM + corrected 4U Choice property ID evidence.
 */
export function assessCorrectedChoicePromotionReview(candidate, evidence, context = {}) {
  const notes = [];
  const parsed = evidence.parsed || parseCorrectedChoiceEvidenceText(evidence.evidenceText);
  const choicePropertyId = normalizeKey(
    evidence.choicePropertyId || parsed.choicePropertyId
  );
  const choicePropertyUrl = evidence.choicePropertyUrl || parsed.choicePropertyUrl || "";
  const choiceBrand = parsed.matchedChoiceBrand || candidate.rawBrand || "";

  const hasName = !!normalizeKey(candidate.rawHotelName);
  const hasCountry =
    !!normalizeKey(candidate.rawCountry) || !!normalizeKey(parsed.osmCountry);
  const hasCoords =
    Number.isFinite(candidate.rawLatitude) && Number.isFinite(candidate.rawLongitude);
  const hasCorrectedEvidence =
    evidence.capturedBy === CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY &&
    !!choicePropertyUrl &&
    !!choicePropertyId;

  const osmPid = propertyIdFromOsmWebsite(candidate.rawWebsite);
  const propertyIdMatchesWebsite =
    !!osmPid && !!choicePropertyId && osmPid === choicePropertyId;

  const normOsmUrl = normalizePropertyUrl(candidate.rawWebsite);
  const normChoiceUrl = normalizePropertyUrl(choicePropertyUrl);
  const urlsAlign =
    normOsmUrl &&
    normChoiceUrl &&
    (normOsmUrl === normChoiceUrl ||
      (propertyIdMatchesWebsite &&
        normOsmUrl.includes(choicePropertyId) &&
        normChoiceUrl.includes(choicePropertyId)));

  let eligibility = PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW;
  let recommendation = PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW;
  let riskLevel = "low";

  if (!hasName || !hasCountry || !hasCoords) {
    eligibility = PROMOTION_ELIGIBILITY.INSUFFICIENT_CORE_FIELDS;
    recommendation = PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET;
    riskLevel = "high";
    if (!hasName) notes.push("Missing candidate name.");
    if (!hasCountry) notes.push("Missing candidate country.");
    if (!hasCoords) notes.push("Missing candidate coordinates.");
  } else if (
    context.duplicatePropertyIdInBatch ||
    context.multipleCorrectedEvidenceOnCandidate ||
    context.duplicateNearbyName
  ) {
    eligibility = PROMOTION_ELIGIBILITY.POSSIBLE_DUPLICATE;
    recommendation = PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET;
    riskLevel = "high";
    if (context.duplicatePropertyIdInBatch) {
      notes.push("Multiple OSM candidates linked to same corrected Choice property ID in 4U batch.");
    }
    if (context.multipleCorrectedEvidenceOnCandidate) {
      notes.push("Multiple corrected Choice property IDs on same OSM candidate in 4U batch.");
    }
    if (context.duplicateNearbyName) {
      notes.push("Nearby OSM candidate with similar normalized name.");
    }
  } else {
    if (!hasCorrectedEvidence) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push("Missing Phase 4U corrected Choice evidence.");
      riskLevel = "medium";
    }
    if (!propertyIdMatchesWebsite) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(
        `OSM website property ID (${osmPid || "none"}) does not match corrected Choice property ID (${choicePropertyId}).`
      );
      riskLevel = "medium";
    }
    if (!normalizeKey(choiceBrand)) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push("Choice brand missing from corrected evidence.");
      riskLevel = "medium";
    }

    const cityLabel = parsed.matchedChoiceCitySlug
      ? parsed.matchedChoiceCitySlug.replace(/-/g, " ")
      : "";
    if (!normalizeKey(candidate.rawCity) && !cityLabel) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push("Candidate city missing; infer from Choice city slug before promotion.");
      riskLevel = "medium";
    } else if (
      cityLabel &&
      candidate.rawCity &&
      !citiesMatch(candidate.rawCity, cityLabel) &&
      nameSimilarity(candidate.rawCity, cityLabel) < 0.45
    ) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(`OSM city "${candidate.rawCity}" vs Choice slug "${cityLabel}" may differ.`);
      riskLevel = "medium";
    }

    const nsBrand = nameSimilarity(candidate.rawHotelName, choiceBrand);
    if (choiceBrand && nsBrand < NAME_MISMATCH_THRESHOLD && !propertyIdMatchesWebsite) {
      eligibility = PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH;
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      notes.push(`OSM name vs Choice brand alignment weak (similarity ${nsBrand.toFixed(2)}).`);
      riskLevel = "medium";
    }
  }

  if (eligibility === PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW) {
    if (!propertyIdMatchesWebsite) {
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      riskLevel = "medium";
    } else if ((evidence.matchScore || 0) < STRONG_MATCH_SCORE) {
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
      if (!notes.length) {
        notes.push(
          `Corrected evidence match score ${evidence.matchScore}; confirm before promotion.`
        );
      }
      if (riskLevel === "low") riskLevel = "medium";
    }
  }

  if (eligibility !== PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW) {
    if (recommendation === PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW) {
      recommendation = PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE;
    }
  }
  if (recommendation === PROMOTION_RECOMMENDATION.DO_NOT_PROMOTE_YET) {
    riskLevel = "high";
  }

  const verifiedCity =
    candidate.rawCity ||
    (parsed.matchedChoiceCitySlug
      ? parsed.matchedChoiceCitySlug.replace(/-/g, " ")
      : "");
  const verifiedName = candidate.rawHotelName || parsed.osmCandidateName;
  const verifiedCountry =
    candidate.rawCountry || parsed.osmCountry || parsed.matchedChoiceCountry;

  const proposedVerified = {
    dealalityHotelId: generateDealalityHotelIdPlaceholder(candidate, verifiedCountry),
    verifiedHotelName: verifiedName,
    normalizedHotelName: normalizeKey(verifiedName),
    verifiedCountry,
    verifiedCity,
    verifiedLatitude: candidate.rawLatitude,
    verifiedLongitude: candidate.rawLongitude,
    verifiedWebsite: choicePropertyUrl || candidate.rawWebsite,
    verifiedPhone: candidate.rawPhone || "",
    verifiedBrandLabel: choiceBrand,
    parentCompany: parsed.parentCompany || "Choice Hotels International",
    primarySourceName: "Choice Hotels Sitemap",
    primarySourceType: SOURCE_TYPES.BRAND_DIRECTORY,
    primarySourceUrl: choicePropertyUrl,
    secondarySourceName: candidate.sourceName || "OpenStreetMap",
    secondarySourceType: SOURCE_TYPES.OSM,
    secondarySourceUrl: candidate.sourceUrl || "",
    secondarySourceCount: 1,
    sourceConfidence:
      propertyIdMatchesWebsite && (evidence.matchScore || 0) >= STRONG_MATCH_SCORE
        ? "medium"
        : propertyIdMatchesWebsite
          ? "low"
          : "low",
    verificationStatus: "pending_review",
    approvedBy: "",
    approvedAt: "",
    approvalNotes: "",
    promotedFromCandidate: candidate.airtableRecordId,
    choicePropertyId,
    choicePropertyUrl,
    evidenceMatchType: parsed.matchType,
    osmWebsitePropertyId: osmPid,
    propertyIdMatchesOsmWebsite: propertyIdMatchesWebsite,
  };

  return {
    eligibility,
    recommendation,
    reviewRiskLevel: riskLevel,
    humanReviewNotes:
      notes.join(" ") ||
      "Corrected OSM website property ID matches Choice sitemap (Phase 4U); prior 4Q collision evidence ignored; human sign-off required.",
    propertyIdMatchesWebsite,
    parsed,
    sourceCount: 2,
    proposedVerified,
  };
}

/**
 * Phase 4V — promotion review rows for corrected 4U evidence batch only.
 */
export function buildCorrectedChoicePromotionReviewRows(evidenceRecords, candidateById) {
  const byCandidate = new Map();
  const propertyIdToCandidates = new Map();

  for (const ev of evidenceRecords) {
    if (ev.evidenceSource !== "choice_property_id_reconciliation") continue;
    const cid = ev.candidateLinkIds[0] || ev.matchedCandidateRecordId;
    if (!cid) continue;
    if (!byCandidate.has(cid)) byCandidate.set(cid, []);
    byCandidate.get(cid).push(ev);

    const pid = normalizeKey(ev.choicePropertyId);
    if (pid) {
      if (!propertyIdToCandidates.has(pid)) propertyIdToCandidates.set(pid, new Set());
      propertyIdToCandidates.get(pid).add(cid);
    }
  }

  const nameGeoIndex = [];
  for (const [, c] of candidateById.entries()) {
    if (c.coords && normalizeKey(c.rawHotelName)) nameGeoIndex.push(c);
  }

  const rows = [];

  for (const [cid, candidate] of candidateById.entries()) {
    const evidenceList = byCandidate.get(cid) || [];
    if (!evidenceList.length) continue;

    const primary = evidenceList.sort(
      (a, b) => (b.matchScore || 0) - (a.matchScore || 0)
    )[0];

    const pid = normalizeKey(primary.choicePropertyId);
    const duplicatePropertyIdInBatch =
      pid && (propertyIdToCandidates.get(pid)?.size || 0) > 1;
    const multipleCorrectedEvidenceOnCandidate = evidenceList.length > 1;

    let duplicateNearbyName = false;
    for (const other of nameGeoIndex) {
      if (other.airtableRecordId === cid) continue;
      const ns = nameSimilarity(candidate.rawHotelName, other.rawHotelName);
      if (ns >= 0.85) {
        const d = distanceMeters(candidate.coords, other.coords);
        if (d != null && d <= NEARBY_DUPLICATE_METERS) {
          duplicateNearbyName = true;
          break;
        }
      }
    }

    const assessment = assessCorrectedChoicePromotionReview(candidate, primary, {
      duplicatePropertyIdInBatch,
      multipleCorrectedEvidenceOnCandidate,
      duplicateNearbyName,
    });

    const p = assessment.parsed;

    rows.push({
      candidateAirtableRecordId: cid,
      sourceRecordId: candidate.sourceRecordId,
      osmSourceUrl: candidate.sourceUrl,
      choicePropertyUrl: primary.choicePropertyUrl || p.choicePropertyUrl,
      choicePropertyId: primary.choicePropertyId || p.choicePropertyId,
      candidateHotelName: candidate.rawHotelName,
      choiceBrandSetupBrand: p.matchedChoiceBrand,
      parentCompany: p.parentCompany,
      candidateCountry: candidate.rawCountry || p.osmCountry,
      candidateCity: candidate.rawCity || p.osmCity,
      candidateLatitude: candidate.rawLatitude,
      candidateLongitude: candidate.rawLongitude,
      candidateWebsite: candidate.rawWebsite,
      candidatePhone: candidate.rawPhone,
      candidateBrand: candidate.rawBrand,
      evidenceMatchType: p.matchType,
      propertyIdMatchesOsmWebsite: assessment.propertyIdMatchesWebsite,
      matchScore: primary.matchScore,
      matchReason: primary.matchReason,
      evidenceCount: evidenceList.length,
      sourceCount: assessment.sourceCount,
      promotionEligibility: assessment.eligibility,
      promotionRecommendation: assessment.recommendation,
      reviewRiskLevel: assessment.reviewRiskLevel,
      humanReviewNotes: assessment.humanReviewNotes,
      evidenceAirtableRecordId: primary.airtableRecordId,
      proposedVerified: assessment.proposedVerified,
      candidateImportBatchId: candidate.importBatchId,
      evidenceBatchId: primary.evidenceBatchId,
      priorPhase4QEvidenceIgnored: true,
      allCorrectedChoicePropertyIds: evidenceList.map((e) => e.choicePropertyId),
    });
  }

  return rows.sort((a, b) => (b.matchScore || 0) - (a.matchScore || 0));
}

export const CORRECTED_CHOICE_PROMOTION_CSV_COLUMNS = [
  "candidateAirtableRecordId",
  "sourceRecordId",
  "osmSourceUrl",
  "choicePropertyUrl",
  "choicePropertyId",
  "candidateHotelName",
  "choiceBrandSetupBrand",
  "parentCompany",
  "candidateCountry",
  "candidateCity",
  "candidateLatitude",
  "candidateLongitude",
  "candidateWebsite",
  "candidatePhone",
  "propertyIdMatchesOsmWebsite",
  "evidenceMatchType",
  "matchScore",
  "matchReason",
  "evidenceCount",
  "sourceCount",
  "promotionEligibility",
  "promotionRecommendation",
  "reviewRiskLevel",
  "humanReviewNotes",
  "evidenceAirtableRecordId",
  "proposedDealalityHotelId",
  "proposedVerifiedHotelName",
  "proposedVerifiedBrandLabel",
  "proposedPrimarySourceUrl",
  "proposedVerificationStatus",
];

export function correctedChoicePromotionRowToCsv(r) {
  const p = r.proposedVerified || {};
  return {
    candidateAirtableRecordId: r.candidateAirtableRecordId,
    sourceRecordId: r.sourceRecordId,
    osmSourceUrl: r.osmSourceUrl,
    choicePropertyUrl: r.choicePropertyUrl,
    choicePropertyId: r.choicePropertyId,
    candidateHotelName: r.candidateHotelName,
    choiceBrandSetupBrand: r.choiceBrandSetupBrand,
    parentCompany: r.parentCompany,
    candidateCountry: r.candidateCountry,
    candidateCity: r.candidateCity,
    candidateLatitude: r.candidateLatitude ?? "",
    candidateLongitude: r.candidateLongitude ?? "",
    candidateWebsite: r.candidateWebsite,
    candidatePhone: r.candidatePhone,
    propertyIdMatchesOsmWebsite: r.propertyIdMatchesOsmWebsite ? "yes" : "no",
    evidenceMatchType: r.evidenceMatchType,
    matchScore: r.matchScore,
    matchReason: r.matchReason,
    evidenceCount: r.evidenceCount,
    sourceCount: r.sourceCount,
    promotionEligibility: r.promotionEligibility,
    promotionRecommendation: r.promotionRecommendation,
    reviewRiskLevel: r.reviewRiskLevel,
    humanReviewNotes: r.humanReviewNotes,
    evidenceAirtableRecordId: r.evidenceAirtableRecordId,
    proposedDealalityHotelId: p.dealalityHotelId,
    proposedVerifiedHotelName: p.verifiedHotelName,
    proposedVerifiedBrandLabel: p.verifiedBrandLabel,
    proposedPrimarySourceUrl: p.primarySourceUrl,
    proposedVerificationStatus: p.verificationStatus,
  };
}
