/**
 * Partner Intelligence package stewardship helpers (dry-run + controlled apply).
 * @see docs/data-intelligence/partner-intelligence-profile-governance-runbook.md
 */
import {
  MAP_PARTNER_SOURCE,
  MAP_PARTNER_FACT,
  VAL_PARTNER_SOURCE_SELECTS,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  assessPackageReadiness,
  assessSourceGate,
  buildPublishPackages,
  detectSourceOriginConflict,
} from "./profile-governance-publish-readiness.js";

export const REPORT_JSON_NAME = "partner-intelligence-stewardship-package.json";
export const REPORT_MD_NAME = "partner-intelligence-stewardship-package.md";

export const RECOMMEND_FACT_MIN = 3;
export const RECOMMEND_FACT_MAX = 8;

/** Human Review Status values never recommended for approval. */
export const EXCLUDED_RECOMMENDATION_STATUSES = new Set([
  "Rejected",
  "Do Not Use",
  "Invalid",
  "Superseded",
  "Needs Re-Extraction",
]);

export const QUARANTINE_NOTE_PATTERN =
  /\b(quarantined|wrong-brand|do not approve|contamination)\b/i;

export const RECOMMENDATION_EXCLUSION_NOTE =
  "Rejected, quarantined, and other non-approval fact statuses are excluded from recommended fact lists and eligibility projections.";

export const NEVER_UPDATE = [
  "Company Validated",
  "Company Validation Date",
  "Brand Setup - Brand Basics profile governance fields",
  "Operator Setup - Master profile governance fields",
  "External Display Status / Show Trust Label on Setup tables",
  "Profile governance trust labels",
  "Published Explorer Fields",
  "Scoring / snapshot fields",
];

export const BRAND_GOVERNANCE_FIELD_KEYS = [
  "be.identity.brandName",
  "be.identity.parentCompany",
  "be.positioning.summary",
  "be.positioning.tagline",
  "be.positioning.guestPromise",
  "be.overview.typicalUseCase",
];

export const OPERATOR_GOVERNANCE_FIELD_KEYS = [
  "op.snapshot.companyName",
  "op.snapshot.parentCompany",
  "op.snapshot.summary",
  "op.snapshot.companyDescription",
  "op.capabilities.managementServices",
  "op.geography.regions",
  "op.brandRelationships",
  "op.ownerValueProposition",
  "op.operatingModel",
];

const BRAND_FIELD_BOOST = [
  { keys: BRAND_GOVERNANCE_FIELD_KEYS, boost: 6, reason: "governance-priority brand field" },
  { pattern: /standard|owner|development|conversion/i, boost: 2, reason: "standards / owner / development relevance" },
];

const OPERATOR_FIELD_BOOST = [
  { keys: OPERATOR_GOVERNANCE_FIELD_KEYS, boost: 6, reason: "governance-priority operator field" },
  { pattern: /region|geograph|market|cala|capabilit/i, boost: 2, reason: "geography / capability relevance" },
];

export function parseIdList(raw) {
  if (!raw) return [];
  return String(raw)
    .split(/[,\s]+/)
    .map((s) => s.trim())
    .filter((s) => /^rec[a-zA-Z0-9]+$/.test(s));
}

export function entityLinkId(entityType, record) {
  if (entityType === "brand") return record.brandId || null;
  if (entityType === "operator") return record.operatorId || null;
  return null;
}

export function isLinkedToTarget(entityType, targetRecId, record) {
  const link = entityLinkId(entityType, record);
  return link === targetRecId;
}

export function sourceSnapshot(source) {
  return {
    id: source.id,
    sourceTitle: source.sourceTitle,
    brandId: source.brandId,
    operatorId: source.operatorId,
    status: source.status,
    approvedForExplorerUse: source.approvedForExplorerUse,
    sourceQuality: source.sourceQuality,
    sourceType: source.sourceType,
    sourceOrigin: source.sourceOrigin,
    region: source.region,
    verifiedSource: source.verifiedSource,
    lastReviewed: source.lastReviewed,
    stale: source.status === "Stale",
  };
}

export function factSnapshot(fact) {
  return {
    id: fact.id,
    fieldName: fact.fieldName,
    explorerSection: fact.explorerSection,
    humanReviewStatus: fact.humanReviewStatus,
    extractionType: fact.extractionType,
    confidenceLevel: fact.confidenceLevel,
    sourceRecordId: fact.sourceRecordId,
    hasEvidence: Boolean(fact.evidenceText),
    dataGap: fact.dataGap || null,
    extractedValuePreview: (fact.extractedValue || "").slice(0, 120),
    approvedValuePreview: (fact.approvedValue || "").slice(0, 120),
  };
}

export function sourceBlockers(source) {
  return assessSourceGate(source).failures;
}

export function summarizeBlockers(pkg, targetProfile) {
  const assessment = assessPackageReadiness(pkg, targetProfile);
  return {
    eligible: assessment.eligible,
    blockReasons: assessment.blockReasons,
    warnings: assessment.warnings,
    needsManualReview: assessment.needsManualReview,
    assessment,
  };
}

export function recommendSourceUpdates(source) {
  const rec = [];
  if (source.approvedForExplorerUse !== "Yes") {
    rec.push({
      field: MAP_PARTNER_SOURCE.approvedForExplorerUse,
      to: "Yes",
      reason: "Required for publish readiness",
    });
  }
  if (source.status === "Found" || source.status === "Captured") {
    rec.push({
      field: MAP_PARTNER_SOURCE.status,
      to: "Approved",
      reason: "Advance status from Found/Captured after steward review",
    });
  }
  const q = source.sourceQuality || "";
  if (!q || q === "Unknown" || q === "Low") {
    rec.push({
      field: MAP_PARTNER_SOURCE.sourceQuality,
      to: "Medium",
      reason: `Raise quality if evidence supports Medium (current: ${q || "blank"})`,
    });
  }
  return rec;
}

/**
 * @param {object} source
 * @param {'brand'|'operator'} entityType
 * @param {string} targetRecId
 * @param {{ approvedSourceIds: Set<string>, allowWrites: boolean, allowQualityBump: boolean, allowStatusAdvance: boolean }} opts
 */
export function buildSafeSourcePatch(source, entityType, targetRecId, opts) {
  const patch = {};
  const skipped = [];
  const applied = [];

  if (!opts.approvedSourceIds.has(source.id)) {
    return { patch: null, skipped: ["source_id_not_in_approve_list"], applied };
  }
  if (!isLinkedToTarget(entityType, targetRecId, source)) {
    return { patch: null, skipped: ["not_linked_to_target"], applied };
  }
  if (source.status === "Stale") {
    return { patch: null, skipped: ["source_status_stale"], applied };
  }
  if (source.status === "Rejected") {
    return { patch: null, skipped: ["source_status_rejected"], applied };
  }

  if (!opts.allowWrites) {
    return { patch: null, skipped: ["dry_run_no_writes"], applied };
  }

  if (source.approvedForExplorerUse !== "Yes") {
    if (!VAL_PARTNER_SOURCE_SELECTS.approvedForExplorerUse.includes("Yes")) {
      return { patch: null, skipped: ["unknown_select_option:approvedForExplorerUse"], applied };
    }
    patch[MAP_PARTNER_SOURCE.approvedForExplorerUse] = "Yes";
    applied.push("Approved for Explorer Use → Yes");
  }

  const q = source.sourceQuality || "";
  if (opts.allowQualityBump && (!q || q === "Unknown" || q === "Low")) {
    if (!VAL_PARTNER_SOURCE_SELECTS.sourceQuality.includes("Medium")) {
      skipped.push("unknown_select_option:sourceQuality");
    } else {
      patch[MAP_PARTNER_SOURCE.sourceQuality] = "Medium";
      applied.push(`Source Quality → Medium (was ${q || "blank"})`);
    }
  } else if (q === "Low") {
    skipped.push("source_quality_low_requires_explicit_review");
  }

  if (
    opts.allowStatusAdvance &&
    (source.status === "Found" || source.status === "Captured")
  ) {
    if (!VAL_PARTNER_SOURCE_SELECTS.status.includes("Approved")) {
      skipped.push("unknown_select_option:status");
    } else {
      patch[MAP_PARTNER_SOURCE.status] = "Approved";
      applied.push(`Status → Approved (was ${source.status})`);
    }
  }

  if (!Object.keys(patch).length) {
    return { patch: null, skipped: skipped.length ? skipped : ["no_changes_needed"], applied };
  }
  return { patch, skipped, applied };
}

export function getFactExclusionReason(fact) {
  const st = String(fact?.humanReviewStatus || "").trim();
  if (EXCLUDED_RECOMMENDATION_STATUSES.has(st)) {
    return `excluded_status:${st}`;
  }
  const notes = [fact?.reviewerNotes, fact?.internalNotes].filter(Boolean).join(" ");
  if (QUARANTINE_NOTE_PATTERN.test(notes)) {
    return "excluded_quarantine_note";
  }
  return null;
}

export function isFactExcludedFromRecommendation(fact) {
  return Boolean(getFactExclusionReason(fact));
}

export function isFactRecommendationCandidate(fact) {
  const st = String(fact?.humanReviewStatus || "");
  if (st === "Approved" || st === "Edited") return false;
  return !isFactExcludedFromRecommendation(fact);
}

export function summarizeFactStatuses(facts) {
  let approved = 0;
  let pendingCandidates = 0;
  let excluded = 0;
  const excludedFacts = [];

  for (const f of facts || []) {
    const st = String(f.humanReviewStatus || "");
    if (st === "Approved" || st === "Edited") {
      approved += 1;
      continue;
    }
    const exclusionReason = getFactExclusionReason(f);
    if (exclusionReason) {
      excluded += 1;
      excludedFacts.push({
        id: f.id,
        fieldName: f.fieldName,
        humanReviewStatus: st,
        exclusionReason,
        extractedValuePreview: (f.extractedValue || "").slice(0, 80),
      });
    } else {
      pendingCandidates += 1;
    }
  }

  return {
    total: (facts || []).length,
    approved,
    pendingCandidates,
    excluded,
    excludedFacts,
    /** @deprecated use pendingCandidates */
    pending: pendingCandidates,
  };
}

export function buildSafeFactPatch(fact, entityType, targetRecId, opts) {
  const skipped = [];
  if (!opts.approvedFactIds.has(fact.id)) {
    return { patch: null, skipped: ["fact_id_not_in_approve_list"] };
  }
  if (!isLinkedToTarget(entityType, targetRecId, fact)) {
    return { patch: null, skipped: ["not_linked_to_target"] };
  }
  const st = String(fact.humanReviewStatus || "");
  if (st === "Approved" || st === "Edited") {
    return { patch: null, skipped: ["already_approved"] };
  }
  if (isFactExcludedFromRecommendation(fact)) {
    return { patch: null, skipped: ["fact_excluded_from_recommendation"] };
  }
  if (!fact.evidenceText && !fact.approvedValue && !fact.extractedValue) {
    return { patch: null, skipped: ["missing_evidence_and_value"] };
  }
  if (!opts.allowWrites) {
    return { patch: null, skipped: ["dry_run_no_writes"] };
  }
  if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Approved")) {
    return { patch: null, skipped: ["unknown_select_option:humanReviewStatus"] };
  }

  const fields = { [MAP_PARTNER_FACT.humanReviewStatus]: "Approved" };
  if (!fact.approvedValue && fact.extractedValue) {
    fields[MAP_PARTNER_FACT.approvedValue] = fact.extractedValue;
  }
  return { patch: fields, skipped };
}

export function simulateSourceAfterPatch(source, patch) {
  if (!patch) return source;
  return {
    ...source,
    approvedForExplorerUse:
      patch[MAP_PARTNER_SOURCE.approvedForExplorerUse] ?? source.approvedForExplorerUse,
    status: patch[MAP_PARTNER_SOURCE.status] ?? source.status,
    sourceQuality: patch[MAP_PARTNER_SOURCE.sourceQuality] ?? source.sourceQuality,
  };
}

export function simulateFactsApproved(facts, factIds) {
  const idSet = new Set(factIds);
  return facts.map((f) => {
    if (!idSet.has(f.id)) return f;
    return {
      ...f,
      humanReviewStatus: "Approved",
      approvedValue: f.approvedValue || f.extractedValue,
    };
  });
}

function fieldKeyBoost(fieldName, entityType) {
  const fn = String(fieldName || "");
  const rules = entityType === "brand" ? BRAND_FIELD_BOOST : OPERATOR_FIELD_BOOST;
  for (const rule of rules) {
    if (rule.keys?.includes(fn)) {
      return { boost: rule.boost, reason: rule.reason };
    }
    if (rule.pattern?.test(fn)) {
      return { boost: rule.boost, reason: rule.reason };
    }
  }
  return { boost: 0, reason: null };
}

export function scoreGovernanceFact(fact, entityType, { stewardSourceIds = [], includeDuplicates = false, approvedFieldNames = new Set() } = {}) {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const fn = String(fact.fieldName || "");

  const keyBoost = fieldKeyBoost(fn, entityType);
  if (keyBoost.boost) {
    score += keyBoost.boost;
    reasons.push(keyBoost.reason);
  }

  if (!includeDuplicates && approvedFieldNames.has(fn)) {
    score -= 5;
    penalties.push("duplicate field key already approved");
  }

  if (fact.evidenceText) {
    score += 2;
    reasons.push("has evidence text");
  } else {
    penalties.push("missing evidence");
    score -= 3;
  }
  if (fact.extractionType === "Directly Stated") {
    score += 2;
    reasons.push("directly stated");
  }
  if (fact.extractionType === "Inferred" || fact.extractionType === "Needs Confirmation") {
    score -= 2;
    penalties.push("inferred / needs confirmation");
  }
  if (fact.confidenceLevel === "High") score += 1;
  if (fact.confidenceLevel === "Low") {
    score -= 2;
    penalties.push("low confidence");
  }
  if (fact.dataGap === "Yes" || fact.dataGap === true) {
    score -= 1;
    penalties.push("data gap flagged");
  }
  if (fact.sourceRecordId && stewardSourceIds.includes(fact.sourceRecordId)) {
    score += 1;
    reasons.push("linked to package source");
  }
  if (!fact.extractedValue && !fact.approvedValue) {
    score -= 4;
    penalties.push("no value");
  }

  return { score, reasons, penalties };
}

export function recommendGovernanceFacts(facts, entityType, options = {}) {
  const {
    stewardSourceIds = [],
    includeDuplicates = false,
    min = RECOMMEND_FACT_MIN,
    max = RECOMMEND_FACT_MAX,
  } = options;

  const approvedFieldNames = new Set(
    facts
      .filter((f) => ["Approved", "Edited"].includes(String(f.humanReviewStatus || "")))
      .map((f) => String(f.fieldName || ""))
      .filter(Boolean)
  );

  const pending = facts.filter((f) => isFactRecommendationCandidate(f));

  const scored = pending.map((f) => {
    const { score, reasons, penalties } = scoreGovernanceFact(f, entityType, {
      stewardSourceIds,
      includeDuplicates,
      approvedFieldNames,
    });
    return { fact: f, score, reasons, penalties };
  });

  scored.sort((a, b) => b.score - a.score);

  const positive = scored.filter((s) => s.score > 0);
  const pick = positive.slice(0, max);
  const governancePicks =
    pick.length >= min ? pick : scored.slice(0, Math.min(max, Math.max(min, scored.length)));

  return governancePicks.map((s) => ({
    ...factSnapshot(s.fact),
    recommendForManualReview: true,
    score: s.score,
    recommendReasons: s.reasons,
    avoidReasons: s.penalties,
  }));
}

export function listAdditionalFacts(facts, recommendedIds, limit, entityType, stewardSourceIds) {
  const recSet = new Set(recommendedIds);
  const pending = facts.filter(
    (f) => isFactRecommendationCandidate(f) && !recSet.has(f.id)
  );
  return pending.slice(0, limit).map((f) => {
    const { score, reasons, penalties } = scoreGovernanceFact(f, entityType, { stewardSourceIds });
    return { ...factSnapshot(f), score, recommendReasons: reasons, avoidReasons: penalties };
  });
}

export function findPackageInReadinessReport(report, entityType, targetRecId) {
  if (!report) return null;
  const key = `${entityType}:${targetRecId}`;
  const pools = [
    ...(report.eligiblePackages || []),
    ...(report.blockedPackages || []),
    ...(report.missingLinkPackages || []),
  ];
  return pools.find((p) => p.entityKey === key || p.recordId === targetRecId) || null;
}

export function buildPackageFromRecords({ sources, facts, published, entityType, targetRecId }) {
  const packages = buildPublishPackages({ sources, facts, published: published || [] });
  return (
    packages.find((p) => p.entityType === entityType && p.recordId === targetRecId) || {
      entityKey: `${entityType}:${targetRecId}`,
      entityType,
      recordId: targetRecId,
      sources: sources.filter((s) => isLinkedToTarget(entityType, targetRecId, s)),
      facts: facts.filter((f) => isLinkedToTarget(entityType, targetRecId, f)),
      published: (published || []).filter((r) => isLinkedToTarget(entityType, targetRecId, r)),
    }
  );
}

export function collectPackageBlockerLabels(pkg, targetProfile) {
  const { blockReasons, warnings, assessment } = summarizeBlockers(pkg, targetProfile);
  const labels = new Set();

  for (const r of blockReasons) {
    if (r === "no_approved_facts") labels.add("no_approved_facts");
    if (r === "missing_entity_link") labels.add("missing_entity_link");
    if (r.includes("approved_for_explorer_use_no")) labels.add("approved_for_explorer_use_no");
    if (r.includes("source_status_not_ready")) labels.add("source_status_not_ready");
    if (r.includes("source_stale")) labels.add("stale source");
    if (r.includes("source_quality_low")) labels.add("low source quality");
    if (r.startsWith("protected:")) labels.add("target protected");
    if (r.startsWith("conflict:")) labels.add("mixed company/public source origins");
  }

  const origin = detectSourceOriginConflict(pkg.sources || []);
  if (origin.conflict) labels.add("mixed company/public source origins");

  for (const s of pkg.sources || []) {
    for (const b of sourceBlockers(s)) {
      if (b === "approved_for_explorer_use_no") labels.add("approved_for_explorer_use_no");
      if (b.startsWith("source_status_not_ready")) labels.add("source_status_not_ready");
      if (b === "source_stale") labels.add("stale source");
      if (b === "source_quality_low") labels.add("low source quality");
    }
  }

  return {
    labels: [...labels],
    blockReasons,
    warnings,
    assessment,
  };
}

export function buildApplyCommandPreview({ entityType, targetRecId, approveSourceIds, approveFactIds }) {
  const parts = [
    "npm run steward-partner-intelligence --",
    "--apply",
    "--approve-stewardship",
    `--entity-type ${entityType}`,
    `--target-rec-id ${targetRecId}`,
  ];
  if (approveSourceIds.length) {
    parts.push(`--approve-source-ids "${approveSourceIds.join(",")}"`);
  }
  if (approveFactIds.length) {
    parts.push(`--approve-fact-ids "${approveFactIds.join(",")}"`);
  }
  return parts.join(" ");
}

export function buildPublishDryRunPreview({ entityType, targetRecId }) {
  return `npm run publish-partner-intelligence-profile-governance -- --entity-type ${entityType} --target-rec-id ${targetRecId} --dry-run`;
}

export function factStatusCounts(facts) {
  return summarizeFactStatuses(facts);
}
