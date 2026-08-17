/**
 * Read-only Partner Intelligence → profile governance publish readiness logic.
 * @see docs/data-intelligence/partner-intelligence-profile-governance-publish-plan.md
 */
import {
  GOVERNANCE_VALIDATION_STATUS,
  GOVERNANCE_USAGE_PERMISSION,
  GOVERNANCE_EXTERNAL_DISPLAY,
} from "../profile-governance/profile-governance-fields.js";
import {
  extractProfileGovernanceRaw,
  normalizeProfileGovernance,
} from "../profile-governance/normalize-profile-governance.js";
import { validatePublishEligibility } from "./publish-overlay.js";

export const SOURCE_READY_STATUSES = new Set([
  "Approved",
  "Extracted",
  "Classified",
]);

export const SOURCE_BLOCKED_STATUSES = new Set(["Rejected", "Stale", "Found", "Captured"]);

export const HOLD_NOTE_PATTERN =
  /\b(HOLD|DO NOT USE|DO NOT PUBLISH|REVIEW REQUIRED|DO NOT OVERWRITE)\b/i;

export const QUALITY_RANK = { Low: 1, Medium: 2, High: 3, Unknown: 0 };

/** PI Source Type → P1 profile Source Type (partial; unmapped → null) */
export const MAP_PI_SOURCE_TYPE_TO_PROFILE = {
  "Development Brochure": "Company PDF / Brochure",
  "Brand Page": "Company Website",
  "Development Page": "Company Website",
  "Operator Capability Deck": "Company PDF / Brochure",
  "Owner Presentation": "Company PDF / Brochure",
  "Portfolio Page": "Company Website",
  "Press Release": "Press Release",
  "Investor Presentation": "Investor Materials",
  "FDD": "Company PDF / Brochure",
  "Website Capture": "Company Website",
  "Case Study": "Hospitality Media",
  "RFP Response": "Company Submission",
  "Internal Note": "Unknown",
  Other: "Unknown",
};

/** Profile Source Type buckets for validation-status inference (brand + operator). */
export const COMPANY_CONTROLLED_PROFILE_SOURCE_TYPES = new Set([
  "Company Website",
  "Company PDF / Brochure",
  "Official Website",
  "Company Materials",
  "Company Published Materials",
  "Company Brochure",
  "Company Fact Sheet",
]);

export const REVIEWED_PUBLIC_PROFILE_SOURCE_TYPES = new Set([
  "Public Sources + AI Extraction",
  "Third-Party Website",
  "News / Press",
  "Press Release",
  "Blog",
  "Industry Database",
  "Mixed Sources",
  "Hospitality Media",
  "Investor Materials",
  "Company Submission",
  "Unknown",
]);

export function mapSourceToProfileSourceType(source) {
  const piType = String(source?.sourceType || "").trim();
  if (!piType) return null;

  if (piType === "Press Release" && isCompanyControlledPressSource(source)) {
    return "Company Materials";
  }

  return MAP_PI_SOURCE_TYPE_TO_PROFILE[piType] || "Unknown";
}

/** Company-hosted press kit / media-center captures — not third-party journalism. */
export function isCompanyControlledPressSource(source) {
  const piType = String(source?.sourceType || "").trim();
  if (piType !== "Press Release") return false;

  const origin = String(source?.sourceOrigin || "").trim();
  if (/^(Brand Provided|Operator Provided|FDD Library|Internal Upload)$/i.test(origin)) {
    return true;
  }

  if (origin !== "Public Web") return false;

  const title = String(source?.sourceTitle || "");
  const url = String(source?.sourceUrl || "");
  const blob = `${title} ${url}`;
  // Capture pipeline titles and company media-center URLs (e.g. media.choicehotels.com/…-press-kit)
  return /press[- ]?kit|media[- ]?center|media kit|press room/i.test(blob);
}

/**
 * Classify a source into company-controlled, reviewed/public, or unknown basis.
 * @returns {"company"|"reviewed"|"unknown"}
 */
export function classifySourceBasisBucket(source) {
  const profileType = mapSourceToProfileSourceType(source);
  if (!profileType) return "unknown";
  if (COMPANY_CONTROLLED_PROFILE_SOURCE_TYPES.has(profileType)) return "company";
  if (REVIEWED_PUBLIC_PROFILE_SOURCE_TYPES.has(profileType)) return "reviewed";
  return "unknown";
}

export function assessPublishScopeSourceBasis(sources) {
  const buckets = (sources || []).map(classifySourceBasisBucket);
  const hasCompany = buckets.includes("company");
  const hasReviewed = buckets.includes("reviewed");
  const hasUnknown = buckets.includes("unknown");
  const allCompany = buckets.length > 0 && buckets.every((b) => b === "company");
  const allReviewed = buckets.length > 0 && buckets.every((b) => b === "reviewed");
  const mixedCompanyAndReviewed = hasCompany && hasReviewed;
  return {
    buckets,
    hasCompany,
    hasReviewed,
    hasUnknown,
    allCompany,
    allReviewed,
    mixedCompanyAndReviewed,
  };
}

/** PI Region → P1 Source Region */
export function mapPiRegionToProfile(region) {
  const r = String(region || "").trim();
  if (!r) return null;
  if (/cala/i.test(r)) return "CALA-Specific";
  if (/global/i.test(r)) return "Global Reference";
  if (/regional/i.test(r)) return "Regional";
  if (/market/i.test(r)) return "Market-Specific";
  return "Unknown";
}

export function mapPiQualityToConfidence(quality, factConfidence) {
  const q = String(quality || factConfidence || "").trim();
  if (q === "High" || q === "Medium" || q === "Low") return q;
  return "Unknown";
}

/** Minimum approved publish-scope facts required for High confidence. */
export const MIN_PUBLISH_SCOPE_FACTS_FOR_HIGH = 3;

const IDENTITY_FIELD_KEYS = new Set([
  "be.identity.brandName",
  "be.identity.parentCompany",
  "op.snapshot.companyName",
  "op.snapshot.parentCompany",
]);

const SUBSTANTIVE_FIELD_PREFIXES = [
  "be.positioning.",
  "be.overview.",
  "be.development.",
  "be.footprint.",
  "be.capabilities.",
  "be.commercial.",
  "be.economics.",
  "be.loyalty.",
  "op.snapshot.summary",
  "op.snapshot.companyDescription",
  "op.capabilities.",
  "op.geography.",
];

const SUBSTANTIVE_FIELD_KEYS = new Set([
  "op.brandRelationships",
  "op.ownerValueProposition",
  "op.operatingModel",
]);

export function isIdentityCoverageFact(fieldName) {
  const fn = String(fieldName || "").trim();
  if (!fn) return false;
  if (IDENTITY_FIELD_KEYS.has(fn)) return true;
  return fn.startsWith("be.identity.");
}

export function isSubstantiveCoverageFact(fieldName) {
  const fn = String(fieldName || "").trim();
  if (!fn) return false;
  if (SUBSTANTIVE_FIELD_KEYS.has(fn)) return true;
  if (SUBSTANTIVE_FIELD_PREFIXES.some((prefix) => fn.startsWith(prefix))) return true;
  if (/positioning|overview|development|capability|geograph|owner.?consider/i.test(fn)) return true;
  return false;
}

function capConfidenceRank(level, maxRank) {
  const rank = QUALITY_RANK[level] ?? 0;
  if (rank <= maxRank) return level;
  if (maxRank >= QUALITY_RANK.High) return "High";
  if (maxRank >= QUALITY_RANK.Medium) return "Medium";
  return "Low";
}

/**
 * Cap proposed confidence from publish-scope fact coverage.
 * High requires ≥3 approved facts, identity + substantive coverage, and ≥1 approved Explorer-use source.
 */
export function assessPublishScopeConfidence({ facts, sources, baseConfidence }) {
  const approvedFacts = (facts || []).filter(isApprovedPublishFact);
  const approvedSources = (sources || []).filter(isApprovedExplorerSource);
  const factCount = approvedFacts.length;
  const hasIdentityFact = approvedFacts.some((f) => isIdentityCoverageFact(f.fieldName));
  const hasSubstantiveFact = approvedFacts.some((f) => isSubstantiveCoverageFact(f.fieldName));
  const identityOnlyCoverage =
    factCount > 0 && approvedFacts.every((f) => isIdentityCoverageFact(f.fieldName));
  const sparseCoverage = factCount < MIN_PUBLISH_SCOPE_FACTS_FOR_HIGH;
  const hasApprovedExplorerSource = approvedSources.length >= 1;

  const evidenceWarnings = [];
  if (sparseCoverage) {
    evidenceWarnings.push("Sparse publish scope: approved facts are limited.");
  }
  if (identityOnlyCoverage) {
    evidenceWarnings.push("identity-only coverage.");
  }

  const meetsHighRequirements =
    factCount >= MIN_PUBLISH_SCOPE_FACTS_FOR_HIGH &&
    hasIdentityFact &&
    hasSubstantiveFact &&
    hasApprovedExplorerSource;

  let confidenceLevel = baseConfidence;
  if (!meetsHighRequirements || sparseCoverage || identityOnlyCoverage) {
    confidenceLevel = capConfidenceRank(confidenceLevel, QUALITY_RANK.Medium);
  }

  return {
    confidenceLevel,
    evidenceWarnings,
    sparseCoverage,
    identityOnlyCoverage,
    meetsHighRequirements,
    factCount,
    hasIdentityFact,
    hasSubstantiveFact,
    hasApprovedExplorerSource,
  };
}

export function parseIsoDate(value) {
  if (!value) return null;
  const s = String(value).trim().split("T")[0];
  const d = new Date(`${s}T12:00:00`);
  return Number.isNaN(d.getTime()) ? null : s;
}

export function maxIsoDate(...dates) {
  const valid = dates.map(parseIsoDate).filter(Boolean);
  if (!valid.length) return null;
  return valid.sort().at(-1);
}

/**
 * @param {{ brandId?: string|null, operatorId?: string|null, profileType?: string, entityName?: string|null }} link
 * @returns {{ entityKey: string, entityType: 'brand'|'operator'|null, recordId: string|null, linkMethod: string }}
 */
export function resolveEntityKey(link) {
  if (link.brandId) {
    return {
      entityKey: `brand:${link.brandId}`,
      entityType: "brand",
      recordId: link.brandId,
      linkMethod: "brand_link",
    };
  }
  if (link.operatorId) {
    return {
      entityKey: `operator:${link.operatorId}`,
      entityType: "operator",
      recordId: link.operatorId,
      linkMethod: "operator_link",
    };
  }
  const name = String(link.entityName || "").trim();
  const profileType = String(link.profileType || "").trim();
  if (name && profileType) {
    return {
      entityKey: `name:${profileType.toLowerCase()}:${name.toLowerCase()}`,
      entityType: profileType === "Brand" ? "brand" : profileType === "Operator" ? "operator" : null,
      recordId: null,
      linkMethod: "name_profile_type",
    };
  }
  return {
    entityKey: `missing:${link.fallbackId || "unknown"}`,
    entityType: null,
    recordId: null,
    linkMethod: "missing_link",
  };
}

export function assessSourceGate(source) {
  const failures = [];
  const warnings = [];
  const status = String(source?.status || "").trim();
  const quality = String(source?.sourceQuality || "").trim();

  if (!source?.id) failures.push("missing_source");
  if (status === "Stale") failures.push("source_stale");
  if (status === "Rejected") failures.push("source_rejected");
  if (!SOURCE_READY_STATUSES.has(status) && status !== "Needs Review") {
    if (SOURCE_BLOCKED_STATUSES.has(status)) {
      failures.push(`source_status_not_ready:${status}`);
    } else if (status === "Needs Review") {
      warnings.push("source_status_needs_review");
    } else if (status) {
      warnings.push(`source_status_unverified:${status}`);
    } else {
      failures.push("source_status_missing");
    }
  }
  if (String(source?.approvedForExplorerUse || "") === "No") {
    failures.push("approved_for_explorer_use_no");
  }
  if ((QUALITY_RANK[quality] || 0) < QUALITY_RANK.Medium) {
    failures.push("source_quality_low");
  }
  if (!source?.lastReviewed && !source?.captureDate && !source?.sourceDate) {
    warnings.push("source_review_date_missing");
  }
  return { ok: failures.length === 0, failures, warnings };
}

export function assessFactGate(fact, source) {
  const failures = [];
  const warnings = [];
  const status = String(fact?.humanReviewStatus || "").trim();
  if (status !== "Approved" && status !== "Edited") {
    failures.push(`fact_review_status:${status || "missing"}`);
  }
  const overlay = validatePublishEligibility(
    {
      humanReviewStatus: fact?.humanReviewStatus,
      approvedValue: fact?.approvedValue,
      extractedValue: fact?.extractedValue,
      dataGap: fact?.dataGap,
      publicVisibility: fact?.publicVisibility,
      sourceQuality: fact?.sourceQuality,
      confidenceLevel: fact?.confidenceLevel,
    },
    source || { sourceQuality: fact?.sourceQuality, status: source?.status, approvedForExplorerUse: source?.approvedForExplorerUse }
  );
  if (!overlay.ok) {
    for (const f of overlay.failures) failures.push(`overlay:${f}`);
  }
  if (!fact?.reviewedAt && !fact?.lastUpdated) warnings.push("fact_review_date_missing");
  return { ok: failures.length === 0, failures, warnings };
}

export function assessPublishedGate(published) {
  const failures = [];
  if (!published?.id) return { ok: true, failures, optional: true };
  const status = String(published.publishStatus || "").trim();
  if (published.stale) failures.push("published_stale");
  if (status && status !== "Published" && status !== "Draft") {
    if (status === "Superseded" || status === "Withdrawn") {
      failures.push(`publish_status:${status}`);
    } else {
      failures.push(`publish_status_unverified:${status}`);
    }
  }
  return { ok: failures.length === 0, failures, optional: false };
}

export function assessTargetProtection(currentFields, entityType, piReviewDate) {
  const raw = extractProfileGovernanceRaw(currentFields, { entityType });
  const reasons = [];

  if (raw.companyValidated === true) reasons.push("company_validated_checkbox");
  if (raw.validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated) {
    reasons.push("validation_status_company_validated");
  }
  if (raw.companyValidationDate) reasons.push("company_validation_date_present");
  if (raw.usagePermission === GOVERNANCE_USAGE_PERMISSION.doNotUse) reasons.push("usage_permission_do_not_use");
  if (raw.externalDisplayStatus === GOVERNANCE_EXTERNAL_DISPLAY.doNotDisplay) {
    reasons.push("external_display_do_not_display");
  }

  const targetReview = parseIsoDate(raw.lastReviewedDate);
  const piDate = parseIsoDate(piReviewDate);
  if (targetReview && piDate && targetReview > piDate) {
    reasons.push(`target_last_reviewed_newer:${targetReview}`);
  }

  const internalNotes = String(raw.internalNotes || currentFields?.["Internal Notes"] || "");
  if (internalNotes && HOLD_NOTE_PATTERN.test(internalNotes)) {
    reasons.push("internal_notes_hold_marker");
  }

  return { blocked: reasons.length > 0, reasons, currentRaw: raw };
}

export function detectSourceOriginConflict(sources) {
  const basis = assessPublishScopeSourceBasis(sources);
  if (basis.mixedCompanyAndReviewed) {
    return {
      conflict: false,
      mixedBasis: true,
      reason: "mixed_company_and_reviewed_source_types",
    };
  }

  const origins = new Set(
    (sources || []).map((s) => String(s.sourceOrigin || "").trim()).filter(Boolean)
  );
  const hasCompanyOrigin = [...origins].some((o) => /provided/i.test(o));
  const hasPublicOrigin = [...origins].some((o) => /public/i.test(o));
  if (hasCompanyOrigin && hasPublicOrigin && !basis.allCompany) {
    return {
      conflict: true,
      reason: "mixed_company_and_public_source_origins",
      origins: [...origins],
    };
  }
  const qualities = (sources || []).map((s) => s.sourceQuality).filter(Boolean);
  const uniqueQ = new Set(qualities);
  if (uniqueQ.has("High") && uniqueQ.has("Low")) {
    return { conflict: true, reason: "mixed_high_and_low_source_quality", qualities };
  }
  return { conflict: false };
}

export function inferValidationStatus(sources, facts) {
  const basis = assessPublishScopeSourceBasis(sources);

  if (basis.mixedCompanyAndReviewed) {
    return GOVERNANCE_VALIDATION_STATUS.sourceInformed;
  }
  if (basis.allCompany) {
    return GOVERNANCE_VALIDATION_STATUS.companyPublished;
  }
  if (basis.allReviewed) {
    return GOVERNANCE_VALIDATION_STATUS.sourceInformed;
  }

  const origins = (sources || []).map((s) => String(s.sourceOrigin || "").trim());
  const hasCompanyOrigin = origins.some((o) => /Brand Provided|Operator Provided/i.test(o));
  const hasPublicOrigin = origins.some((o) => /Public Web|Press Release|FDD Library/i.test(o));

  if (basis.hasUnknown && !basis.hasReviewed && hasCompanyOrigin && !hasPublicOrigin) {
    return GOVERNANCE_VALIDATION_STATUS.companyPublished;
  }

  const extractionTypes = (facts || []).map((f) => String(f.extractionType || "").trim());
  const hasInferred = extractionTypes.some((t) => t === "Inferred" || t === "Needs Confirmation");
  if (hasInferred && !basis.hasCompany) {
    return GOVERNANCE_VALIDATION_STATUS.aiAssisted;
  }

  return GOVERNANCE_VALIDATION_STATUS.sourceInformed;
}

export function proposeProfileGovernance({ entityType, sources, facts, publishedRows, piReviewDate }) {
  const primarySource = (sources || [])[0] || null;
  const validationStatus = inferValidationStatus(sources, facts);
  const neverCompanyValidated =
    validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated
      ? GOVERNANCE_VALIDATION_STATUS.companyPublished
      : validationStatus;

  const sourceType =
    mapSourceToProfileSourceType(primarySource) ||
    (primarySource?.sourceType ? "Unknown" : null);
  const sourceRegion = mapPiRegionToProfile(primarySource?.region);
  const baseConfidence = mapPiQualityToConfidence(
    primarySource?.sourceQuality,
    facts?.[0]?.confidenceLevel || publishedRows?.[0]?.overallSourceConfidence
  );
  const scopeConfidence = assessPublishScopeConfidence({
    facts: facts || [],
    sources: sources || [],
    baseConfidence,
  });
  const confidenceLevel = scopeConfidence.confidenceLevel;

  const lastReviewedDate =
    piReviewDate ||
    maxIsoDate(
      ...(facts || []).map((f) => f.reviewedAt || f.lastUpdated),
      ...(sources || []).map((s) => s.lastReviewed),
      ...(publishedRows || []).map((p) => p.publishedAt)
    );

  const evidenceTitles = (sources || []).map((s) => s.sourceTitle).filter(Boolean);
  let evidenceNotes = evidenceTitles.length
    ? `PI sources (${evidenceTitles.length}): ${evidenceTitles.slice(0, 3).join("; ")}${evidenceTitles.length > 3 ? "…" : ""}. Approved facts: ${(facts || []).length}.`
    : null;
  if (scopeConfidence.evidenceWarnings.length) {
    const warningText = scopeConfidence.evidenceWarnings.join(" ");
    evidenceNotes = evidenceNotes ? `${evidenceNotes} ${warningText}` : warningText;
  }

  const missingFlags = [];
  if (!lastReviewedDate) missingFlags.push("review date missing");
  if ((sources || []).some((s) => s.status === "Stale")) missingFlags.push("stale source linked");
  if (confidenceLevel === "Low") missingFlags.push("low confidence");

  const eligibleForShowLabel =
    neverCompanyValidated !== GOVERNANCE_VALIDATION_STATUS.needsReview &&
    neverCompanyValidated !== GOVERNANCE_VALIDATION_STATUS.doNotUse &&
    confidenceLevel !== "Low";

  const proposed = {
    validationStatus: neverCompanyValidated,
    usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
    sourceType,
    sourceRegion,
    confidenceLevel,
    lastReviewedDate,
    refreshDueDate: null,
    evidenceNotes,
    missingDataFlags: missingFlags.length ? missingFlags.join("; ") : null,
    companyValidated: false,
    companyValidationDate: null,
    externalDisplayStatus: eligibleForShowLabel
      ? GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel
      : GOVERNANCE_EXTERNAL_DISPLAY.needsReview,
    internalNotes: "PI publish readiness audit proposal — not written.",
  };

  const mergedForNormalize = {};
  const col = (k, v) => {
    if (v == null || v === "") return;
    mergedForNormalize[k] = v;
  };
  col("Validation Status", proposed.validationStatus);
  col("Usage Permission", proposed.usagePermission);
  col("Source Type", proposed.sourceType);
  col("Source Region", proposed.sourceRegion);
  col("Confidence Level", proposed.confidenceLevel);
  col("Data Confidence Level", proposed.confidenceLevel);
  col("Last Reviewed Date", proposed.lastReviewedDate);
  col("Evidence Notes", proposed.evidenceNotes);
  col("Missing Data Flags", proposed.missingDataFlags);
  col("Company Validated", false);
  col("External Display Status", proposed.externalDisplayStatus);

  const normalized = normalizeProfileGovernance(mergedForNormalize, {
    entityType,
    sourceTable: entityType === "brand" ? "Brand Setup - Brand Basics" : "Operator Setup - Master",
  });

  return { proposed, expectedGovernance: normalized };
}

export const GOVERNANCE_CHANGE_EQUIVALENT_STABLE = "equivalent_stable_live_governance";

export const GOVERNANCE_CONFIDENCE_RANK = { Low: 1, Medium: 2, High: 3, Unknown: 0 };

export const GOVERNANCE_VALIDATION_RANK = {
  [GOVERNANCE_VALIDATION_STATUS.doNotUse]: 0,
  [GOVERNANCE_VALIDATION_STATUS.needsReview]: 1,
  [GOVERNANCE_VALIDATION_STATUS.aiAssisted]: 2,
  [GOVERNANCE_VALIDATION_STATUS.sourceInformed]: 3,
  [GOVERNANCE_VALIDATION_STATUS.ownerProvided]: 3,
  [GOVERNANCE_VALIDATION_STATUS.companyPublished]: 4,
  [GOVERNANCE_VALIDATION_STATUS.companyValidated]: 5,
  [GOVERNANCE_VALIDATION_STATUS.staleRefreshNeeded]: 1,
};

/** Company-materials profile Source Type values treated as equivalent for audit classification. */
export const COMPANY_MATERIALS_EQUIVALENT_SOURCE_TYPES = new Set([
  ...COMPANY_CONTROLLED_PROFILE_SOURCE_TYPES,
]);

function governanceFieldsForNormalize(raw, entityType) {
  const fields = {};
  const col = (k, v) => {
    if (v == null || v === "") return;
    fields[k] = v;
  };
  col("Validation Status", raw?.validationStatus);
  col("Usage Permission", raw?.usagePermission);
  col("Source Type", raw?.sourceType);
  col("Source Region", raw?.sourceRegion);
  col("Confidence Level", raw?.confidenceLevel);
  col("Data Confidence Level", raw?.confidenceLevel);
  col("Last Reviewed Date", raw?.lastReviewedDate);
  col("External Display Status", raw?.externalDisplayStatus);
  col("Company Validated", raw?.companyValidated);
  col("Company Validation Date", raw?.companyValidationDate);
  return fields;
}

export function isStableGovernanceChangeClass(changeClass) {
  return changeClass === "no_op" || changeClass === GOVERNANCE_CHANGE_EQUIVALENT_STABLE;
}

export function isCompanyMaterialsEquivalentSourceType(sourceType) {
  const value = String(sourceType || "").trim();
  return value ? COMPANY_MATERIALS_EQUIVALENT_SOURCE_TYPES.has(value) : false;
}

export function areCompanyMaterialsSourceTypesEquivalent(liveSourceType, proposedSourceType) {
  const live = String(liveSourceType || "").trim();
  const proposed = String(proposedSourceType || "").trim();
  if (!live && !proposed) return true;
  if (live === proposed) return true;
  return (
    isCompanyMaterialsEquivalentSourceType(live) &&
    isCompanyMaterialsEquivalentSourceType(proposed)
  );
}

export function isConfidenceSameOrStrongerLive(liveConfidence, proposedConfidence) {
  const cur = GOVERNANCE_CONFIDENCE_RANK[liveConfidence] ?? 0;
  const next = GOVERNANCE_CONFIDENCE_RANK[proposedConfidence] ?? 0;
  if (!cur && !next) return true;
  return cur >= next && cur > 0;
}

export function isSourceRegionSameOrMoreSpecificLive(liveRegion, proposedRegion) {
  const live = String(liveRegion || "").trim();
  const proposed = String(proposedRegion || "").trim();
  if (!live && !proposed) return true;
  if (live && !proposed) return true;
  if (!live && proposed) return false;
  return live === proposed;
}

function normalizeGovernanceTrustOutput(raw, entityType) {
  return normalizeProfileGovernance(governanceFieldsForNormalize(raw, entityType), {
    entityType,
    sourceTable: entityType === "brand" ? "Brand Setup - Brand Basics" : "Operator Setup - Master",
  });
}

/**
 * Detect cosmetic PI proposal drift where live governance is same or stronger and trust-chip output is unchanged.
 */
export function hasCriticalGovernanceMismatch(currentRaw, proposed, entityType = "brand") {
  if (!currentRaw?.validationStatus || !proposed?.validationStatus) return true;

  if (proposed.companyValidated === true && currentRaw.companyValidated !== true) return true;
  if (proposed.companyValidationDate && !currentRaw.companyValidationDate) return true;

  const liveNorm = normalizeGovernanceTrustOutput(currentRaw, entityType);
  const propNorm = normalizeGovernanceTrustOutput(proposed, entityType);

  if (liveNorm.displayLabel !== propNorm.displayLabel) return true;
  if (liveNorm.sourceBasis !== propNorm.sourceBasis) return true;

  if (
    currentRaw.usagePermission !== proposed.usagePermission &&
    (currentRaw.usagePermission !== GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed ||
      proposed.usagePermission !== GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed)
  ) {
    return true;
  }

  const blockedExternal = new Set([
    GOVERNANCE_EXTERNAL_DISPLAY.doNotDisplay,
    GOVERNANCE_EXTERNAL_DISPLAY.hideTrustLabel,
    GOVERNANCE_EXTERNAL_DISPLAY.internalOnly,
  ]);
  if (
    blockedExternal.has(proposed.externalDisplayStatus) ||
    (currentRaw.externalDisplayStatus !== proposed.externalDisplayStatus &&
      blockedExternal.has(currentRaw.externalDisplayStatus))
  ) {
    return true;
  }

  if (
    currentRaw.usagePermission === GOVERNANCE_USAGE_PERMISSION.doNotUse ||
    proposed.usagePermission === GOVERNANCE_USAGE_PERMISSION.doNotUse
  ) {
    return true;
  }

  const cur = GOVERNANCE_VALIDATION_RANK[currentRaw.validationStatus] ?? 1;
  const next = GOVERNANCE_VALIDATION_RANK[proposed.validationStatus] ?? 1;
  if (cur < next) return true;

  if (!isConfidenceSameOrStrongerLive(currentRaw.confidenceLevel, proposed.confidenceLevel)) {
    return true;
  }

  if (!areCompanyMaterialsSourceTypesEquivalent(currentRaw.sourceType, proposed.sourceType)) {
    return true;
  }

  if (!isSourceRegionSameOrMoreSpecificLive(currentRaw.sourceRegion, proposed.sourceRegion)) {
    return true;
  }

  return false;
}

export function isEquivalentStableLiveGovernance(currentRaw, proposed, entityType = "brand") {
  if (!currentRaw?.validationStatus || !proposed?.validationStatus) return false;

  const cur = GOVERNANCE_VALIDATION_RANK[currentRaw.validationStatus] ?? 1;
  const next = GOVERNANCE_VALIDATION_RANK[proposed.validationStatus] ?? 1;
  if (cur !== next) return false;

  if (currentRaw.companyValidated === true) return false;
  if (currentRaw.validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated) return false;

  if (currentRaw.usagePermission !== GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed) {
    return false;
  }
  if (currentRaw.externalDisplayStatus !== GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel) {
    return false;
  }

  return !hasCriticalGovernanceMismatch(currentRaw, proposed, entityType);
}

export function classifyGovernanceChange(currentRaw, proposed, options = {}) {
  const entityType = options.entityType || "brand";

  if (!currentRaw?.validationStatus && !currentRaw?.usagePermission) return "new";
  if (
    currentRaw.validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated ||
    currentRaw.companyValidated
  ) {
    return "protected";
  }

  const cur = GOVERNANCE_VALIDATION_RANK[currentRaw.validationStatus] ?? 1;
  const next = GOVERNANCE_VALIDATION_RANK[proposed.validationStatus] ?? 1;

  if (cur === next && currentRaw.confidenceLevel === proposed.confidenceLevel) {
    if (isEquivalentStableLiveGovernance(currentRaw, proposed, entityType)) return "no_op";
  }

  if (next > cur) return "upgrade";
  if (next < cur) return "downgrade";

  if (isEquivalentStableLiveGovernance(currentRaw, proposed, entityType)) {
    return GOVERNANCE_CHANGE_EQUIVALENT_STABLE;
  }

  return "conflict";
}

export function isApprovedExplorerSource(source) {
  return String(source?.approvedForExplorerUse || "") === "Yes";
}

export function isApprovedPublishFact(fact) {
  const st = String(fact?.humanReviewStatus || "");
  return st === "Approved" || st === "Edited";
}

const COMPANY_VALIDATION_FACT_RE =
  /company validated|validated by (?:ihg|marriott|hilton|choice)|company-approved|company approved|official sign-off/i;

export function isBrandExplorerScopedFact(fact) {
  const fieldName = String(fact?.fieldName || "");
  const explorerType = String(fact?.explorerType || "");
  return explorerType === "Brand Explorer" || fieldName.startsWith("be.");
}

function isPublicFacingFact(fact) {
  return String(fact?.publicVisibility || "").trim() !== "Internal Only";
}

/**
 * Brand Explorer active-profile governance — distinct from profile-governance publish eligibility.
 * Rejected/internal facts do not block; pending public facts and unsupported approved public facts do.
 */
export function assessBrandExplorerGovernanceReadiness(liveState) {
  const sources = liveState?.sources || [];
  const facts = (liveState?.facts || []).filter(isBrandExplorerScopedFact);
  const approvedSources = sources.filter(isApprovedExplorerSource);
  const approvedSourceIds = new Set(approvedSources.map((s) => s.id));

  const pendingFacts = facts.filter((f) => String(f.humanReviewStatus || "") === "Pending");
  const pendingPublic = pendingFacts.filter(isPublicFacingFact);
  const holdPublic = facts.filter(
    (f) =>
      isPublicFacingFact(f) &&
      /^(Hold|Founder Review|Needs Review)$/i.test(String(f.humanReviewStatus || ""))
  );
  const approvedPublic = facts.filter((f) => isApprovedPublishFact(f) && isPublicFacingFact(f));
  const approvedInternal = facts.filter(
    (f) => isApprovedPublishFact(f) && !isPublicFacingFact(f)
  );
  const rejectedFacts = facts.filter((f) => String(f.humanReviewStatus || "") === "Rejected");
  const rejectedInternal = rejectedFacts.filter((f) => !isPublicFacingFact(f));
  const rejectedPublic = rejectedFacts.filter(isPublicFacingFact);
  const sourceConfirmationNeeded = facts.filter(
    (f) =>
      isPublicFacingFact(f) &&
      String(f.humanReviewStatus || "") === "Pending" &&
      /source confirmation|confirm source/i.test(
        `${f.reviewerNotes || ""} ${f.followUpQuestion || ""}`
      )
  );

  const approvedPublicWithoutSource = approvedPublic.filter(
    (f) => !f.sourceRecordId || !approvedSourceIds.has(f.sourceRecordId)
  );
  const approvedPublicGateFailures = [];
  for (const f of approvedPublic) {
    const source = sources.find((s) => s.id === f.sourceRecordId);
    const gate = assessFactGate(f, source);
    if (!gate.ok) {
      approvedPublicGateFailures.push({
        factId: f.id,
        fieldName: f.fieldName,
        failures: gate.failures,
      });
    }
  }

  const companyValidationImpliedByFact = approvedPublic.some((f) => {
    const blob = `${f.extractedValue || ""} ${f.approvedValue || ""} ${f.reviewerNotes || ""}`;
    return COMPANY_VALIDATION_FACT_RE.test(blob);
  });

  const blockers = [];
  if (approvedSources.length < 1) blockers.push("no_approved_explorer_sources");
  if (pendingPublic.length > 0) {
    blockers.push(`pending_public_explorer_facts:${pendingPublic.length}`);
  }
  if (holdPublic.length > 0) {
    blockers.push(`hold_public_explorer_facts:${holdPublic.length}`);
  }
  if (approvedPublicWithoutSource.length > 0) {
    blockers.push(
      `approved_public_facts_missing_approved_source:${approvedPublicWithoutSource.length}`
    );
  }
  if (approvedPublicGateFailures.length > 0) {
    blockers.push(`approved_public_facts_gate_failures:${approvedPublicGateFailures.length}`);
  }
  if (rejectedPublic.length > 0) {
    blockers.push(`rejected_still_public:${rejectedPublic.length}`);
  }
  if (companyValidationImpliedByFact) {
    blockers.push("approved_fact_implies_company_validation");
  }

  const ready = blockers.length === 0;

  return {
    ready,
    governedPlatformReady: ready,
    blockers,
    breakdown: {
      totalExplorerFacts: facts.length,
      approvedPublic: approvedPublic.length,
      approvedInternal: approvedInternal.length,
      rejectedInternal: rejectedInternal.length,
      rejectedPublic: rejectedPublic.length,
      pendingReview: pendingFacts.length,
      pendingPublic: pendingPublic.length,
      holdPublic: holdPublic.length,
      sourceConfirmationNeeded: sourceConfirmationNeeded.length,
      approvedExplorerSourceCount: approvedSources.length,
    },
    approvedPublicFacts: approvedPublic.map((f) => ({
      id: f.id,
      fieldName: f.fieldName,
      sourceRecordId: f.sourceRecordId || null,
      publicVisibility: f.publicVisibility || null,
      humanReviewStatus: f.humanReviewStatus || null,
    })),
    approvedPublicGateFailures,
    rejectedInternalExcludedFromGate: true,
    rootCause: ready ? "explorer_governance_ready" : blockers[0] || "explorer_governance_blocked",
  };
}

/**
 * Split a linked PI package into full scope vs approved Explorer-use publish scope.
 */
export function buildPublishScopeSlice(pkg) {
  const allSources = pkg.sources || [];
  const publishSources = allSources.filter(isApprovedExplorerSource);
  const publishSourceIds = new Set(publishSources.map((s) => s.id));
  const excludedSources = allSources.filter((s) => !publishSourceIds.has(s.id));

  const publishFacts = (pkg.facts || []).filter(
    (f) => isApprovedPublishFact(f) && publishSourceIds.has(f.sourceRecordId)
  );

  return {
    publishScopePackage: {
      ...pkg,
      sources: publishSources,
      facts: publishFacts,
    },
    fullPackageSourceCount: allSources.length,
    publishScopeSourceCount: publishSources.length,
    excludedSourceCount: excludedSources.length,
    approvedSourceIds: publishSources.map((s) => s.id),
    excludedSourceIds: excludedSources.map((s) => s.id),
    approvedSources: publishSources.map((s) => ({
      id: s.id,
      sourceTitle: s.sourceTitle,
      sourceOrigin: s.sourceOrigin,
      sourceQuality: s.sourceQuality,
      status: s.status,
    })),
    excludedFromPublishScope: excludedSources.map((s) => ({
      id: s.id,
      sourceTitle: s.sourceTitle,
      approvedForExplorerUse: s.approvedForExplorerUse,
      sourceOrigin: s.sourceOrigin,
      reason: "approved_for_explorer_use_no",
    })),
    nonApprovedSources: excludedSources.map((s) => ({
      id: s.id,
      sourceTitle: s.sourceTitle,
      approvedForExplorerUse: s.approvedForExplorerUse,
      sourceOrigin: s.sourceOrigin,
    })),
    factsUsedForProposal: publishFacts.map((f) => ({
      id: f.id,
      fieldName: f.fieldName,
      humanReviewStatus: f.humanReviewStatus,
      sourceRecordId: f.sourceRecordId,
      extractedValuePreview: String(f.extractedValue || f.approvedValue || "").slice(0, 120),
    })),
    rejectedFactCount: (pkg.facts || []).filter((f) => String(f.humanReviewStatus || "") === "Rejected")
      .length,
  };
}

export function collectFullPackageDiagnostics(pkg) {
  const fullPackageBlockers = [];
  const fullPackageWarnings = [];
  const excludedFromPublishScope = [];

  for (const s of pkg.sources || []) {
    if (!isApprovedExplorerSource(s)) {
      excludedFromPublishScope.push(s.id);
      fullPackageWarnings.push(`excluded_source:${s.id}:approved_for_explorer_use_no`);
    }
  }

  const fullOrigin = detectSourceOriginConflict(pkg.sources);
  if (fullOrigin.conflict) {
    fullPackageWarnings.push(`full_package_conflict:${fullOrigin.reason}`);
  } else if (fullOrigin.mixedBasis) {
    fullPackageWarnings.push(`full_package_mixed_basis:${fullOrigin.reason}`);
  }

  const staleSources = (pkg.sources || []).filter((s) => s.status === "Stale");
  for (const s of staleSources) {
    if (!isApprovedExplorerSource(s)) {
      fullPackageWarnings.push(`excluded_stale_source:${s.id}`);
    } else {
      fullPackageBlockers.push(`full_package_stale_source:${s.id}`);
    }
  }

  return {
    fullPackageBlockers: [...new Set(fullPackageBlockers)],
    fullPackageWarnings: [...new Set(fullPackageWarnings)],
    excludedFromPublishScope,
    fullOriginConflict: fullOrigin,
  };
}

/**
 * Assess a package slice (typically approved publish scope only).
 */
function assessPackageScope(pkg, targetProfile) {
  const blockReasons = [];
  const warnings = [];
  const needsManualReview = [];

  if (!pkg.entityType || !pkg.recordId) {
    blockReasons.push("missing_entity_link");
  }
  if (!targetProfile && pkg.recordId) {
    blockReasons.push("target_profile_not_found");
  }

  if (!pkg.sources?.length) blockReasons.push("no_approved_explorer_sources");

  const sourceResults = (pkg.sources || []).map((s) => ({
    id: s.id,
    ...assessSourceGate(s),
  }));
  const approvedFacts = (pkg.facts || []).filter(isApprovedPublishFact);
  if (!approvedFacts.length) blockReasons.push("no_approved_facts");

  if (approvedFacts.length > 0 && approvedFacts.length < 3) {
    warnings.push("sparse_publish_scope_fact_set");
    needsManualReview.push("sparse_publish_scope_fact_set");
  }

  const factResults = approvedFacts.map((f) => {
    const source = (pkg.sources || []).find((s) => s.id === f.sourceRecordId) || pkg.sources?.[0];
    return { id: f.id, fieldName: f.fieldName, ...assessFactGate(f, source) };
  });

  for (const sr of sourceResults) {
    if (!sr.ok) blockReasons.push(...sr.failures.map((f) => `source:${sr.id}:${f}`));
    warnings.push(...(sr.warnings || []).map((w) => `source:${sr.id}:${w}`));
  }
  for (const fr of factResults) {
    if (!fr.ok) blockReasons.push(...fr.failures.map((f) => `fact:${fr.id}:${f}`));
    warnings.push(...(fr.warnings || []).map((w) => `fact:${fr.id}:${w}`));
  }

  for (const pub of pkg.published || []) {
    const pr = assessPublishedGate(pub);
    if (!pr.ok) warnings.push(...pr.failures.map((f) => `published:${pub.id}:${f}`));
  }

  const originConflict = detectSourceOriginConflict(pkg.sources);
  if (originConflict.conflict) {
    blockReasons.push(`conflict:${originConflict.reason}`);
    needsManualReview.push(originConflict.reason);
  }

  const piReviewDate = maxIsoDate(
    ...(pkg.facts || []).map((f) => f.reviewedAt || f.lastUpdated),
    ...(pkg.sources || []).map((s) => s.lastReviewed)
  );

  let protection = { blocked: false, reasons: [] };
  if (targetProfile?.fields) {
    protection = assessTargetProtection(targetProfile.fields, pkg.entityType, piReviewDate);
    if (protection.blocked) {
      blockReasons.push(...protection.reasons.map((r) => `protected:${r}`));
    }
  }

  if (warnings.length) needsManualReview.push(...warnings);

  const proposal =
    pkg.entityType && blockReasons.length === 0
      ? proposeProfileGovernance({
          entityType: pkg.entityType,
          sources: pkg.sources,
          facts: approvedFacts,
          publishedRows: pkg.published,
          piReviewDate,
        })
      : null;

  const changeClass =
    proposal && protection.currentRaw
      ? classifyGovernanceChange(protection.currentRaw, proposal.proposed, {
          entityType: pkg.entityType || targetProfile?.entityType || "brand",
        })
      : proposal
        ? "new"
        : null;

  if (changeClass === "downgrade") {
    blockReasons.push("would_downgrade_existing_validation");
    needsManualReview.push("downgrade_blocked");
  }

  const eligible = blockReasons.length === 0 && !!proposal;

  return {
    eligible,
    blockReasons: [...new Set(blockReasons)],
    warnings: [...new Set(warnings)],
    needsManualReview: [...new Set(needsManualReview)],
    protection,
    proposal,
    changeClass,
    piReviewDate,
    sourceResults,
    factResults,
  };
}

/**
 * @param {object} pkg
 * @param {object|null} targetProfile — { id, name, fields, entityType }
 */
export function assessPackageReadiness(pkg, targetProfile) {
  const scopeSlice = buildPublishScopeSlice(pkg);
  const fullDiagnostics = collectFullPackageDiagnostics(pkg);

  if (!(pkg.sources || []).length) {
    const missing = assessPackageScope(
      { ...pkg, sources: [], facts: [] },
      targetProfile
    );
    return {
      ...missing,
      eligible: false,
      blockReasons: [...new Set([...(missing.blockReasons || []), "no_linked_sources"])],
      publishScopeBlockers: [...new Set([...(missing.blockReasons || []), "no_linked_sources"])],
      fullPackageBlockers: fullDiagnostics.fullPackageBlockers,
      fullPackageWarnings: fullDiagnostics.fullPackageWarnings,
      publishScope: scopeSlice,
      scopes: {
        full: {
          sourceCount: 0,
          blockers: ["no_linked_sources", ...fullDiagnostics.fullPackageBlockers],
          warnings: fullDiagnostics.fullPackageWarnings,
        },
        publish: {
          sourceCount: 0,
          blockers: ["no_linked_sources", "no_approved_explorer_sources"],
          warnings: [],
        },
      },
    };
  }

  const publishAssessment = assessPackageScope(scopeSlice.publishScopePackage, targetProfile);

  const eligible = publishAssessment.eligible;
  const blockReasons = publishAssessment.blockReasons;
  const warnings = [
    ...publishAssessment.warnings,
    ...fullDiagnostics.fullPackageWarnings,
  ];

  return {
    ...publishAssessment,
    eligible,
    blockReasons,
    publishScopeBlockers: publishAssessment.blockReasons,
    fullPackageBlockers: fullDiagnostics.fullPackageBlockers,
    fullPackageWarnings: fullDiagnostics.fullPackageWarnings,
    publishScope: scopeSlice,
    fullPackageSourceCount: scopeSlice.fullPackageSourceCount,
    publishScopeSourceCount: scopeSlice.publishScopeSourceCount,
    excludedSourceCount: scopeSlice.excludedSourceCount,
    approvedSourceIds: scopeSlice.approvedSourceIds,
    excludedSourceIds: scopeSlice.excludedSourceIds,
    approvedSources: scopeSlice.approvedSources,
    excludedFromPublishScope: scopeSlice.excludedFromPublishScope,
    nonApprovedSources: scopeSlice.nonApprovedSources,
    factsUsedForProposal: scopeSlice.factsUsedForProposal,
    rejectedFactCount: scopeSlice.rejectedFactCount,
    fullPackageOriginConflict: fullDiagnostics.fullOriginConflict,
    scopes: {
      full: {
        sourceCount: scopeSlice.fullPackageSourceCount,
        factCount: (pkg.facts || []).length,
        blockers: fullDiagnostics.fullPackageBlockers,
        warnings: fullDiagnostics.fullPackageWarnings,
        originConflict: fullDiagnostics.fullOriginConflict,
        excludedSourceCount: scopeSlice.excludedSourceCount,
      },
      publish: {
        sourceCount: scopeSlice.publishScopeSourceCount,
        factCount: scopeSlice.factsUsedForProposal.length,
        blockers: publishAssessment.blockReasons,
        warnings: publishAssessment.warnings,
        originConflict: detectSourceOriginConflict(scopeSlice.publishScopePackage.sources),
        approvedSourceIds: scopeSlice.approvedSourceIds,
        factsUsedForProposal: scopeSlice.factsUsedForProposal,
      },
    },
  };
}

/**
 * Build publish packages grouped by entity key from PI records.
 */
export function buildPublishPackages({ sources, facts, published }) {
  /** @type {Map<string, object>} */
  const packages = new Map();

  function ensurePackage(entityKey, entityType, recordId, linkMethod) {
    if (!packages.has(entityKey)) {
      packages.set(entityKey, {
        entityKey,
        entityType,
        recordId,
        linkMethod,
        sources: [],
        facts: [],
        published: [],
        sourceIds: new Set(),
        factIds: new Set(),
        publishedIds: new Set(),
      });
    }
    return packages.get(entityKey);
  }

  for (const source of sources || []) {
    const { entityKey, entityType, recordId, linkMethod } = resolveEntityKey({
      brandId: source.brandId,
      operatorId: source.operatorId,
      profileType: source.profileType,
      entityName: source.sourceTitle,
      fallbackId: source.id,
    });
    const pkg = ensurePackage(entityKey, entityType, recordId, linkMethod);
    if (!pkg.sourceIds.has(source.id)) {
      pkg.sources.push(source);
      pkg.sourceIds.add(source.id);
    }
  }

  for (const fact of facts || []) {
    const { entityKey, entityType, recordId, linkMethod } = resolveEntityKey({
      brandId: fact.brandId,
      operatorId: fact.operatorId,
      profileType: fact.profileType,
      fallbackId: fact.id,
    });
    const pkg = ensurePackage(entityKey, entityType, recordId, linkMethod);
    if (!pkg.factIds.has(fact.id)) {
      pkg.facts.push(fact);
      pkg.factIds.add(fact.id);
    }
  }

  for (const row of published || []) {
    const { entityKey, entityType, recordId, linkMethod } = resolveEntityKey({
      brandId: row.brandId,
      operatorId: row.operatorId,
      profileType: row.profileType,
      fallbackId: row.id,
    });
    const pkg = ensurePackage(entityKey, entityType, recordId, linkMethod);
    if (!pkg.publishedIds.has(row.id)) {
      pkg.published.push(row);
      pkg.publishedIds.add(row.id);
    }
  }

  return [...packages.values()].map((p) => {
    const { sourceIds, factIds, publishedIds, ...rest } = p;
    return rest;
  });
}
