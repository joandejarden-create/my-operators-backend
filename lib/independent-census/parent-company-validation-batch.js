/**
 * Parent-company batch validation — end-to-end read/compute, bucket, optional apply.
 * Consolidates brand-directory extract → reconcile → evidence → promotion review.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { generateBrandDirectorySeedsFromBrandSetup } from "./brand-directory-seeds-from-brand-setup.js";
import {
  MATCH_TYPE,
  loadPropertyUrlExtractReport,
  indexSitemapProperties,
  reconcileCandidatesFromOsmWebsites,
  loadOsmCandidatesForReconciliation,
  summarizeReconciliation,
} from "./choice-property-id-reconciliation.js";
import {
  selectReconciliationEvidenceMatches,
  parseMatchTypesInclude,
  buildCorrectedChoiceEvidenceAirtableFields,
  loadExistingCorrectedChoiceSemanticKeys,
  loadExistingEvidenceDedupeNames,
  partitionCorrectedChoiceEvidenceByDuplicate,
  createEvidenceRecords,
  correctedChoiceSemanticDedupeKey,
  CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY,
} from "./evidence-apply.js";
import {
  assessCorrectedChoicePromotionReview,
  PROMOTION_ELIGIBILITY,
  PROMOTION_RECOMMENDATION,
} from "./promotion-review.js";
import {
  selectPromotableReviewRows,
  filterDuplicates,
  createVerifiedRecords,
  loadExistingVerifiedDedupeIndex,
} from "./promote-verified.js";
import { loadCandidatesByIds } from "./promotion-review-load.js";
import { EVIDENCE_TABLE, VERIFIED_TABLE } from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";
import { normalizeKey, normalizeText } from "./match-current-census.js";
import { reconciliationConfidenceToScore } from "./parent-company-validation-helpers.js";

export const VALIDATION_BUCKET = {
  READY: "ready_for_human_approval",
  ENRICHMENT: "needs_enrichment",
  COLLISION: "duplicate_or_collision_review",
  HOLD: "hold_low_priority",
};

const DIRECT_MATCH_TYPES = new Set([
  MATCH_TYPE.DIRECT_PROPERTY_ID,
  MATCH_TYPE.DIRECT_PROPERTY_URL,
]);

const DEFAULT_COLLISION_REPORT =
  "reports/independent-census-choice-collision-review-2026-05-20.json";

const DEFAULT_RECONCILIATION_REPORT =
  "reports/independent-census-choice-property-id-reconciliation-2026-05-20.json";

export function parentCompanySlug(parentCompany) {
  return normalizeKey(parentCompany).replace(/\s+/g, "-");
}

export function loadJsonReport(filePath) {
  if (!filePath || !existsSync(filePath)) return null;
  return JSON.parse(readFileSync(filePath, "utf8"));
}

/**
 * Phase 4S collision context — prior 4Q weak property IDs; do not use for corrected mx* promotion.
 */
export function loadCollisionHoldContext(collisionReportPath) {
  const weakPropertyIds = new Set();
  const byCandidate = new Map();

  const data = loadJsonReport(collisionReportPath);
  if (!data) {
    return { weakPropertyIds, byCandidate, loaded: false };
  }

  for (const g of data.collisionGroups || []) {
    const cid = g.osmCandidateRecordId;
    if (!cid) continue;
    byCandidate.set(cid, g);
    for (const pid of g.rankedPropertyIds || []) {
      if (pid) weakPropertyIds.add(normalizeKey(pid));
    }
  }

  return { weakPropertyIds, byCandidate, loaded: true, report: data };
}

export function isPrior4QCollisionPropertyId(propertyId, collisionCtx) {
  const pid = normalizeKey(propertyId);
  if (!pid) return false;
  if (collisionCtx.weakPropertyIds.has(pid)) return true;
  if (/^tr\d/i.test(pid)) return true;
  return false;
}

export function isPrior4QCollisionCandidateProperty(
  candidateId,
  propertyId,
  collisionCtx
) {
  const pid = normalizeKey(propertyId);
  const group = collisionCtx.byCandidate.get(candidateId);
  if (!group) return false;
  const ranked = (group.rankedPropertyIds || []).map(normalizeKey);
  return ranked.includes(pid);
}

function propertyIdFromOsmWebsite(url) {
  const m = String(url || "").match(/\/([a-z]{2}\d{2,8})\/?$/i);
  return m ? normalizeKey(m[1]) : "";
}

function reconciliationMatchReason(row) {
  if (row.matchType === MATCH_TYPE.DIRECT_PROPERTY_URL) {
    return "Direct property URL match from OSM website to Choice sitemap (Phase 4T reconciliation).";
  }
  return "Direct property ID match from OSM website to Choice sitemap (Phase 4T reconciliation).";
}

function syntheticEvidenceFromReconciliation(row, evidenceBatchId) {
  const propertyId = row.extractedChoicePropertyId || "";
  return {
    evidenceSource: "choice_property_id_reconciliation",
    airtableRecordId: "",
    evidenceBatchId,
    choicePropertyId: propertyId,
    choicePropertyUrl: row.matchedChoicePropertyUrl || "",
    evidenceUrl: row.matchedChoicePropertyUrl || "",
    evidenceText: "",
    matchScore: reconciliationConfidenceToScore(row.reconciliationConfidence),
    matchReason: reconciliationMatchReason(row),
    capturedBy: CHOICE_RECONCILIATION_EVIDENCE_CAPTURED_BY,
    candidateLinkIds: [row.osmCandidateRecordId],
    parsed: {
      choicePropertyId: propertyId,
      choicePropertyUrl: row.matchedChoicePropertyUrl,
      matchedChoiceBrand: row.matchedChoiceBrand,
      matchedChoiceCountry: row.matchedChoiceCountry,
      matchedChoiceCitySlug: row.matchedChoiceCitySlug,
      osmCandidateRecordId: row.osmCandidateRecordId,
      osmCandidateName: row.osmCandidateName,
      osmCountry: row.osmCountry,
      osmCity: row.osmCity,
      matchType: row.matchType,
      parentCompany: "Choice Hotels International",
    },
  };
}

/**
 * @param {object} reviewRow
 * @param {object} meta
 */
export function assignValidationBucket(reviewRow, meta) {
  if (meta.alreadyPromoted) {
    return {
      bucket: VALIDATION_BUCKET.HOLD,
      bucketReason: "already_promoted_to_verified",
    };
  }

  if (
    meta.prior4QCollisionProperty ||
    isPrior4QCollisionPropertyId(reviewRow.choicePropertyId, meta.collisionCtx)
  ) {
    return {
      bucket: VALIDATION_BUCKET.COLLISION,
      bucketReason: "prior_4q_weak_collision_property_id",
    };
  }

  if (
    reviewRow.promotionEligibility === PROMOTION_ELIGIBILITY.POSSIBLE_DUPLICATE
  ) {
    return {
      bucket: VALIDATION_BUCKET.COLLISION,
      bucketReason: reviewRow.humanReviewNotes || "possible_duplicate",
    };
  }

  if (
    reviewRow.promotionEligibility ===
    PROMOTION_ELIGIBILITY.INSUFFICIENT_CORE_FIELDS
  ) {
    return {
      bucket: VALIDATION_BUCKET.HOLD,
      bucketReason: "insufficient_core_fields",
    };
  }

  if (
    reviewRow.promotionEligibility ===
    PROMOTION_ELIGIBILITY.NEEDS_MANUAL_RESEARCH
  ) {
    return {
      bucket: VALIDATION_BUCKET.ENRICHMENT,
      bucketReason: reviewRow.humanReviewNotes || "needs_manual_research",
    };
  }

  if (
    reviewRow.promotionEligibility === PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW &&
    reviewRow.propertyIdMatchesOsmWebsite &&
    meta.hasCorrectedEvidence
  ) {
    return {
      bucket: VALIDATION_BUCKET.READY,
      bucketReason: "direct_property_match_with_corrected_evidence",
    };
  }

  return {
    bucket: VALIDATION_BUCKET.HOLD,
    bucketReason:
      reviewRow.humanReviewNotes ||
      reviewRow.promotionRecommendation ||
      "hold_default",
  };
}

function buildReviewRowFromReconciliation(
  reconciliationRow,
  candidate,
  evidenceMeta,
  collisionCtx
) {
  const evidence = syntheticEvidenceFromReconciliation(
    reconciliationRow,
    evidenceMeta.evidenceBatchId
  );
  if (evidenceMeta.existingEvidenceRecordId) {
    evidence.airtableRecordId = evidenceMeta.existingEvidenceRecordId;
  }

  const choicePropertyId = normalizeKey(reconciliationRow.extractedChoicePropertyId);
  const osmPid = propertyIdFromOsmWebsite(candidate.rawWebsite);
  const propertyIdMatchesWebsite =
    !!osmPid && !!choicePropertyId && osmPid === choicePropertyId;

  const assessment = assessCorrectedChoicePromotionReview(candidate, evidence, {
    duplicatePropertyIdInBatch: false,
    multipleCorrectedEvidenceOnCandidate: false,
    duplicateNearbyName: false,
  });

  const prior4QCollisionProperty = isPrior4QCollisionCandidateProperty(
    candidate.airtableRecordId,
    choicePropertyId,
    collisionCtx
  );

  const reviewRow = {
    candidateAirtableRecordId: candidate.airtableRecordId,
    sourceRecordId: candidate.sourceRecordId,
    osmSourceUrl: candidate.sourceUrl,
    choicePropertyUrl: reconciliationRow.matchedChoicePropertyUrl,
    choicePropertyId: reconciliationRow.extractedChoicePropertyId,
    candidateHotelName: candidate.rawHotelName,
    choiceBrandSetupBrand: reconciliationRow.matchedChoiceBrand,
    parentCompany: "Choice Hotels International",
    candidateCountry: candidate.rawCountry || reconciliationRow.osmCountry,
    candidateCity: candidate.rawCity || reconciliationRow.osmCity,
    candidateLatitude: candidate.rawLatitude,
    candidateLongitude: candidate.rawLongitude,
    candidateWebsite: candidate.rawWebsite,
    candidatePhone: candidate.rawPhone,
    candidateBrand: candidate.rawBrand,
    evidenceMatchType: reconciliationRow.matchType,
    propertyIdMatchesOsmWebsite: propertyIdMatchesWebsite,
    matchScore: evidence.matchScore,
    matchReason: evidence.matchReason,
    evidenceCount: 1,
    sourceCount: assessment.sourceCount,
    promotionEligibility: assessment.eligibility,
    promotionRecommendation: assessment.recommendation,
    reviewRiskLevel: assessment.reviewRiskLevel,
    humanReviewNotes: assessment.humanReviewNotes,
    evidenceAirtableRecordId: evidenceMeta.existingEvidenceRecordId || "",
    proposedVerified: assessment.proposedVerified,
    candidateImportBatchId: candidate.importBatchId,
    evidenceBatchId: evidenceMeta.evidenceBatchId,
    priorPhase4QEvidenceIgnored: true,
    hasExistingCorrectedEvidence: evidenceMeta.hasExistingEvidence,
    wouldCreateEvidence: evidenceMeta.wouldCreateEvidence,
  };

  const meta = {
    alreadyPromoted: evidenceMeta.alreadyPromoted,
    hasCorrectedEvidence:
      evidenceMeta.hasExistingEvidence || evidenceMeta.wouldCreateEvidence,
    prior4QCollisionProperty,
    collisionCtx,
  };

  const { bucket, bucketReason } = assignValidationBucket(reviewRow, meta);

  return {
    ...reviewRow,
    validationBucket: bucket,
    validationBucketReason: bucketReason,
    alreadyPromoted: evidenceMeta.alreadyPromoted,
  };
}

async function loadOrRunReconciliation(opts) {
  const reportPath =
    opts.reconciliationReportPath ||
    (opts.parentCompanySlug === "choice-hotels-international"
      ? join(opts.projectRoot, DEFAULT_RECONCILIATION_REPORT)
      : null);

  if (reportPath && existsSync(reportPath)) {
    const data = loadJsonReport(reportPath);
    return {
      rows: data.reconciliationRows || [],
      summary: data.summary || summarizeReconciliation(data.reconciliationRows || []),
      source: "report",
      reportPath,
    };
  }

  if (!opts.propertyUrlReportPath) {
    throw new Error(
      "Reconciliation report missing and --property-url-report required to compute reconciliation"
    );
  }

  const extract = loadPropertyUrlExtractReport(opts.propertyUrlReportPath);
  const sitemapIndex = indexSitemapProperties(extract);
  const { rows: candidates } = await loadOsmCandidatesForReconciliation({
    retentionReportPath: opts.candidateRetentionReportPath,
    includeRetention: opts.includeRetention,
    useRetentionFilter: !!opts.candidateRetentionReportPath,
  });

  const { rows, withChoiceUrls } = reconcileCandidatesFromOsmWebsites(
    candidates,
    sitemapIndex
  );

  return {
    rows,
    summary: summarizeReconciliation(rows),
    source: "computed",
    osmCandidatesScanned: candidates.length,
    osmWithChoiceUrls: withChoiceUrls,
  };
}

function countBuckets(rows) {
  const counts = {
    ready_for_human_approval: 0,
    needs_enrichment: 0,
    duplicate_or_collision_review: 0,
    hold_low_priority: 0,
  };
  for (const r of rows) {
    counts[r.validationBucket] = (counts[r.validationBucket] || 0) + 1;
  }
  return counts;
}

function validationRowToCsv(row) {
  return {
    validationBucket: row.validationBucket,
    validationBucketReason: row.validationBucketReason,
    candidateAirtableRecordId: row.candidateAirtableRecordId,
    choicePropertyId: row.choicePropertyId,
    candidateHotelName: row.candidateHotelName,
    candidateCity: row.candidateCity,
    candidateCountry: row.candidateCountry,
    propertyIdMatchesOsmWebsite: row.propertyIdMatchesOsmWebsite ? "yes" : "no",
    evidenceMatchType: row.evidenceMatchType,
    promotionEligibility: row.promotionEligibility,
    promotionRecommendation: row.promotionRecommendation,
    reviewRiskLevel: row.reviewRiskLevel,
    matchScore: row.matchScore,
    hasExistingCorrectedEvidence: row.hasExistingCorrectedEvidence ? "yes" : "no",
    wouldCreateEvidence: row.wouldCreateEvidence ? "yes" : "no",
    alreadyPromoted: row.alreadyPromoted ? "yes" : "no",
    evidenceAirtableRecordId: row.evidenceAirtableRecordId,
    humanReviewNotes: row.humanReviewNotes,
    choicePropertyUrl: row.choicePropertyUrl,
    proposedVerifiedHotelName: row.proposedVerified?.verifiedHotelName,
    proposedPrimarySourceUrl: row.proposedVerified?.primarySourceUrl,
  };
}

export const VALIDATION_CSV_COLUMNS = Object.keys(
  validationRowToCsv({
    validationBucket: "",
    validationBucketReason: "",
    candidateAirtableRecordId: "",
    choicePropertyId: "",
    candidateHotelName: "",
    candidateCity: "",
    candidateCountry: "",
    propertyIdMatchesOsmWebsite: false,
    evidenceMatchType: "",
    promotionEligibility: "",
    promotionRecommendation: "",
    reviewRiskLevel: "",
    matchScore: 0,
    hasExistingCorrectedEvidence: false,
    wouldCreateEvidence: false,
    alreadyPromoted: false,
    evidenceAirtableRecordId: "",
    humanReviewNotes: "",
    choicePropertyUrl: "",
    proposedVerified: {},
  })
);

/**
 * @param {object} opts
 */
export async function runParentCompanyValidationBatch(opts) {
  const projectRoot = opts.projectRoot || process.cwd();
  const parentSlug = parentCompanySlug(opts.parentCompany);
  const evidenceBatchId =
    opts.evidenceBatchId || `${opts.batchId}-corrected-evidence`;
  const apply = !!opts.apply;
  const dryRun = !apply;
  const includeReviewBeforePromote = !!opts.includeReviewBeforePromote;
  const maxPromotions =
    opts.maxPromotions != null ? Number(opts.maxPromotions) : null;

  const collisionReportPath =
    opts.collisionReportPath ||
    (parentSlug === "choice-hotels-international"
      ? join(projectRoot, DEFAULT_COLLISION_REPORT)
      : null);

  const collisionCtx = loadCollisionHoldContext(collisionReportPath);

  let seedsResult = null;
  if (opts.brandSeedsPath && existsSync(opts.brandSeedsPath)) {
    const data = loadJsonReport(opts.brandSeedsPath);
    seedsResult = {
      seeds: data.seeds || [],
      brandsMatched: (data.seeds || []).length,
      source: "file",
      path: opts.brandSeedsPath,
    };
  } else {
    seedsResult = await generateBrandDirectorySeedsFromBrandSetup({
      parentCompany: opts.parentCompany,
      batchId: opts.batchId,
      calaRegionOnly: opts.calaRegionOnly !== false,
    });
    seedsResult.source = "brand_setup";
  }

  const propertyUrlReport = opts.propertyUrlReportPath
    ? loadJsonReport(opts.propertyUrlReportPath)
    : null;

  const propertyLeadCount =
    propertyUrlReport?.summary?.calaIncludedCount ??
    propertyUrlReport?.propertyRows?.length ??
    propertyUrlReport?.summary?.totalPropertyUrlsParsed ??
    0;

  const reconciliation = await loadOrRunReconciliation({
    ...opts,
    projectRoot,
    parentCompanySlug: parentSlug,
    reconciliationReportPath: opts.reconciliationReportPath
      ? join(projectRoot, opts.reconciliationReportPath)
      : join(projectRoot, DEFAULT_RECONCILIATION_REPORT),
    propertyUrlReportPath: opts.propertyUrlReportPath
      ? join(projectRoot, opts.propertyUrlReportPath)
      : null,
    candidateRetentionReportPath: opts.candidateRetentionReportPath
      ? join(projectRoot, opts.candidateRetentionReportPath)
      : null,
  });

  const directRows = reconciliation.rows.filter((r) =>
    DIRECT_MATCH_TYPES.has(normalizeKey(r.matchType))
  );

  const candidateIds = directRows.map((r) => r.osmCandidateRecordId).filter(Boolean);
  const candidateById = await loadCandidatesByIds(candidateIds, {
    candidateSourceType: opts.candidateSourceType || "osm",
  });

  const base = getIndependentCensusBase();
  let existingSemanticKeys = new Set();
  let verifiedCandidateLinks = new Set();
  let evidenceBySemantic = new Map();

  if (base) {
    existingSemanticKeys = await loadExistingCorrectedChoiceSemanticKeys(
      base,
      EVIDENCE_TABLE
    );
    const verifiedIndex = await loadExistingVerifiedDedupeIndex(base, VERIFIED_TABLE);
    verifiedCandidateLinks = verifiedIndex.candidateLinks;
  }

  const evidenceMetaByCandidate = new Map();
  for (const row of directRows) {
    const cid = row.osmCandidateRecordId;
    const pid = row.extractedChoicePropertyId;
    const semantic = correctedChoiceSemanticDedupeKey(pid, cid);
    const hasExisting = existingSemanticKeys.has(semantic);
    const alreadyPromoted = verifiedCandidateLinks.has(cid);

    evidenceMetaByCandidate.set(`${cid}|${normalizeKey(pid)}`, {
      evidenceBatchId,
      hasExistingEvidence: hasExisting,
      wouldCreateEvidence: !hasExisting,
      alreadyPromoted,
      existingEvidenceRecordId: "",
    });
  }

  const validationRows = [];
  for (const row of directRows) {
    const candidate = candidateById.get(row.osmCandidateRecordId);
    if (!candidate) continue;

    const metaKey = `${row.osmCandidateRecordId}|${normalizeKey(row.extractedChoicePropertyId)}`;
    const evidenceMeta = evidenceMetaByCandidate.get(metaKey) || {
      evidenceBatchId,
      hasExistingEvidence: false,
      wouldCreateEvidence: true,
      alreadyPromoted: verifiedCandidateLinks.has(row.osmCandidateRecordId),
    };

    validationRows.push(
      buildReviewRowFromReconciliation(
        row,
        candidate,
        evidenceMeta,
        collisionCtx
      )
    );
  }

  validationRows.sort((a, b) => (b.matchScore || 0) - (a.matchScore || 0));

  const bucketCounts = countBuckets(validationRows);

  let evidenceCreated = [];
  let evidenceWrittenCount = 0;
  let evidenceSkippedDuplicate = 0;
  let promotedRecords = [];
  let promotedCount = 0;
  let promotionSkippedDuplicate = 0;

  const includeMatchTypes = parseMatchTypesInclude(
    "direct_property_id_match,direct_property_url_match"
  );

  if (apply && opts.sourcePolicyApproved) {
    if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

    const { selected } = selectReconciliationEvidenceMatches(
      { reconciliationRows: reconciliation.rows },
      { includeMatchTypes }
    );

    const evidenceRows = selected.map((r) =>
      buildCorrectedChoiceEvidenceAirtableFields(r, evidenceBatchId)
    );

    const batchNames = await loadExistingEvidenceDedupeNames(
      base,
      EVIDENCE_TABLE,
      evidenceBatchId,
      "4U"
    );
    const { toWrite, skippedDuplicate } = partitionCorrectedChoiceEvidenceByDuplicate(
      evidenceRows,
      batchNames,
      existingSemanticKeys
    );
    evidenceSkippedDuplicate = skippedDuplicate.length;

    const evResult = await createEvidenceRecords(
      base,
      EVIDENCE_TABLE,
      toWrite,
      batchNames
    );
    evidenceCreated = evResult.created;
    evidenceWrittenCount = evResult.writtenCount;
  }

  if (apply && opts.approvedBy) {
    const readyRows = validationRows.filter(
      (r) => r.validationBucket === VALIDATION_BUCKET.READY
    );

    const reviewForPromote = readyRows.map((row) => ({
      ...row,
      promotionEligibility: PROMOTION_ELIGIBILITY.ELIGIBLE_FOR_REVIEW,
      promotionRecommendation: includeReviewBeforePromote
        ? PROMOTION_RECOMMENDATION.REVIEW_BEFORE_PROMOTE
        : PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW,
      _approvedBy: opts.approvedBy,
      _batchId: opts.batchId,
      _approvedAt: new Date().toISOString(),
      _approvalNote:
        opts.approvalNote ||
        `Parent-company batch validation promotion (${opts.batchId}).`,
    }));

    const { selected } = selectPromotableReviewRows(reviewForPromote, {
      allowReviewBeforePromote: includeReviewBeforePromote,
      maxRecords: maxPromotions,
      requirePropertyIdMatchOnOsmWebsite: true,
    });

    const verifiedIndex = await loadExistingVerifiedDedupeIndex(base, VERIFIED_TABLE);
    const { toWrite, skippedDuplicate } = filterDuplicates(selected, verifiedIndex);
    promotionSkippedDuplicate = skippedDuplicate.length;

    const promoteResult = await createVerifiedRecords(
      base,
      VERIFIED_TABLE,
      toWrite,
      verifiedIndex.dedupeKeys
    );
    promotedRecords = promoteResult.created;
    promotedCount = promoteResult.writtenCount;
  }

  const reportBase = `independent-census-parent-company-validation-${parentSlug}-${opts.batchId}`;
  const jsonPath = join(projectRoot, "reports", `${reportBase}.json`);
  const csvPath = join(projectRoot, "reports", `${reportBase}.csv`);

  const existingEvidenceCount = validationRows.filter(
    (r) => r.hasExistingCorrectedEvidence
  ).length;
  const wouldCreateEvidenceCount = validationRows.filter(
    (r) => r.wouldCreateEvidence && !r.alreadyPromoted
  ).length;

  return {
    generatedAt: new Date().toISOString(),
    phase: "parent-company-validation-batch",
    parentCompany: opts.parentCompany,
    parentSlug,
    batchId: opts.batchId,
    evidenceBatchId,
    mode: apply ? "apply" : "dry-run",
    dryRun,
    apply,
    sourcePolicyApproved: !!opts.sourcePolicyApproved,
    approvedBy: opts.approvedBy || null,
    includeReviewBeforePromote,
    maxPromotions,
    brandSeeds: {
      source: seedsResult.source,
      brandsMatched: seedsResult.brandsMatched,
      missingSourceUrlCount: seedsResult.missingSourceUrlCount,
    },
    propertyUrlReport: opts.propertyUrlReportPath || null,
    propertyLeadCount,
    candidateRetentionReport: opts.candidateRetentionReportPath || null,
    reconciliationSource: reconciliation.source,
    reconciliationReportPath: reconciliation.reportPath || null,
    collisionReportLoaded: collisionCtx.loaded,
    collisionReportPath: collisionReportPath || null,
    priorPhase4QEvidenceIgnored: true,
    osmCandidatesScanned: reconciliation.osmCandidatesScanned ?? null,
    osmWithChoiceUrls: reconciliation.osmWithChoiceUrls ?? null,
    reconciliationSummary: reconciliation.summary,
    directMatchCount: directRows.length,
    linkedCandidatesReviewed: candidateById.size,
    evidenceRowsExisting: existingEvidenceCount,
    evidenceRowsWouldCreate: wouldCreateEvidenceCount,
    evidenceWrittenCount,
    evidenceSkippedDuplicate,
    evidenceCreated,
    bucketCounts,
    promotedCount,
    promotionSkippedDuplicate,
    promotedRecords,
    promotionReviewRowCount: validationRows.length,
    validationRows,
    reportFiles: { json: jsonPath, csv: csvPath },
    airtableWrites: apply && (!!opts.sourcePolicyApproved || !!opts.approvedBy),
    tablesWritten: [
      ...(apply && opts.sourcePolicyApproved ? [EVIDENCE_TABLE] : []),
      ...(apply && opts.approvedBy && promotedCount ? [VERIFIED_TABLE] : []),
    ],
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: apply && !!opts.sourcePolicyApproved,
    verifiedTableWrites: apply && !!opts.approvedBy && promotedCount > 0,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    humanApprovalRequired: true,
    noAutoPromotion: true,
  };
}

export { validationRowToCsv, countBuckets };
