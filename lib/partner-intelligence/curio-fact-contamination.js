/**
 * Curio PI fact contamination detection (read path + quarantine planning).
 * @see docs/data-intelligence/curio-pi-package-integrity-cleanup-plan.md
 */
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";

export const CURIO_BRAND_ID = "receQkxgjlezsc1xg";
export const PRIMARY_CONTAMINATED_SOURCE_ID = "recIH5lyY8MASnfrp";
export const SECONDARY_REPORT_SOURCE_IDS = {
  pointsGuide: "rec6opP76pLDVjDuP",
  usFddBadFootprint: "recy2pyEahF9UUsEk",
};

export const DEFAULT_QUARANTINE_NOTE =
  "Quarantined: wrong-brand extraction values found in Curio package. Do not approve without re-extraction.";

const IDENTITY_FIELD_PREFIX = "be.identity.";

function textBlob(fact) {
  return [
    fact.extractedValue,
    fact.approvedValue,
    fact.evidenceText,
    fact.reviewerNotes,
    fact.normalizedValue,
  ]
    .filter(Boolean)
    .join(" ");
}

function containsKimpton(text) {
  return /kimpton/i.test(String(text || ""));
}

function containsIhg(text) {
  return /\bihg\b/i.test(String(text || ""));
}

function isIdentityField(fieldName) {
  return String(fieldName || "").startsWith(IDENTITY_FIELD_PREFIX);
}

function isParentCompanyField(fieldName) {
  const fn = String(fieldName || "");
  return fn.includes("parentCompany") || fn.endsWith("parentCompany");
}

function isBrandNameField(fieldName) {
  const fn = String(fieldName || "");
  return fn.includes("brandName") || fn.endsWith("brandName");
}

function isCurioOrHilton(text) {
  return /curio|hilton/i.test(String(text || ""));
}

function isHiltonParent(text) {
  return /hilton/i.test(String(text || ""));
}

function isUnsupportedPlaceholder(text) {
  return /^not confirmed in available sources\.?$/i.test(String(text || "").trim());
}

function isBadFootprintDuplicate(fact) {
  return (
    String(fact.fieldName || "") === "be.footprint.globalHotels" &&
    String(fact.extractedValue || "").trim() === "06"
  );
}

/**
 * @param {object} fact — normalized partner fact
 * @returns {{ contaminated: boolean, reasons: string[], severity: 'quarantine'|'report_only' }}
 */
export function assessFactContamination(fact, options = {}) {
  const { includeSecondary = false, secondaryQuarantine = false } = options;
  const reasons = [];
  const extracted = String(fact.extractedValue || "");
  const approved = String(fact.approvedValue || "");
  const blob = textBlob(fact);
  const sourceId = fact.sourceRecordId || "";

  if (containsKimpton(extracted)) reasons.push("extracted_value_contains_kimpton");
  if (containsKimpton(approved)) reasons.push("approved_value_contains_kimpton");
  if (containsIhg(extracted)) reasons.push("extracted_value_contains_ihg");
  if (containsIhg(approved)) reasons.push("approved_value_contains_ihg");
  if (/ihg development brochure/i.test(blob)) reasons.push("metadata_ihg_development_brochure");

  if (isBrandNameField(fact.fieldName) && extracted && !isCurioOrHilton(extracted)) {
    reasons.push("brand_name_not_curio_or_hilton");
  }
  if (isParentCompanyField(fact.fieldName) && extracted && !isHiltonParent(extracted)) {
    reasons.push("parent_company_not_hilton");
  }
  if (isIdentityField(fact.fieldName) && extracted && !isCurioOrHilton(extracted) && !isUnsupportedPlaceholder(extracted)) {
    reasons.push("identity_value_not_curio_or_hilton");
  }

  const isPrimary = sourceId === PRIMARY_CONTAMINATED_SOURCE_ID;
  const isSecondary =
    sourceId === SECONDARY_REPORT_SOURCE_IDS.pointsGuide ||
    sourceId === SECONDARY_REPORT_SOURCE_IDS.usFddBadFootprint;

  if (isSecondary && !includeSecondary) {
    const reportReasons = [];
    if (isUnsupportedPlaceholder(extracted)) reportReasons.push("unsupported_not_confirmed_placeholder");
    if (isBadFootprintDuplicate(fact)) reportReasons.push("suspicious_footprint_duplicate_06");
    if (reasons.length) reportReasons.push(...reasons);
    return {
      contaminated: reportReasons.length > 0,
      reasons: reportReasons,
      severity: "report_only",
    };
  }

  if (isSecondary && includeSecondary && secondaryQuarantine) {
    if (isUnsupportedPlaceholder(extracted)) reasons.push("unsupported_not_confirmed_placeholder");
    if (isBadFootprintDuplicate(fact)) reasons.push("suspicious_footprint_duplicate_06");
  }

  return {
    contaminated: reasons.length > 0,
    reasons: [...new Set(reasons)],
    severity: reasons.length > 0 ? "quarantine" : "none",
  };
}

export function resolveQuarantineReviewStatus() {
  if (VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Rejected")) {
    return "Rejected";
  }
  if (VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Needs More Source")) {
    return "Needs More Source";
  }
  return null;
}

/**
 * @param {object} fact
 * @param {{ reason?: string, clearBadApprovedValue?: boolean }} options
 */
export function buildQuarantinePatch(fact, options = {}) {
  const status = resolveQuarantineReviewStatus();
  if (!status) {
    return { patch: null, skipped: ["unknown_human_review_status_option"] };
  }

  const note = options.reason || DEFAULT_QUARANTINE_NOTE;
  const patch = {
    [MAP_PARTNER_FACT.humanReviewStatus]: status,
  };

  const existingNotes = String(fact.reviewerNotes || "").trim();
  if (existingNotes.includes("Quarantined:")) {
    // idempotent — still update status if needed
  } else {
    patch[MAP_PARTNER_FACT.reviewerNotes] = existingNotes
      ? `${existingNotes}\n\n${note}`
      : note;
  }

  const approved = String(fact.approvedValue || "");
  if (
    options.clearBadApprovedValue !== false &&
    approved &&
    (containsKimpton(approved) || containsIhg(approved))
  ) {
    patch[MAP_PARTNER_FACT.approvedValue] = "";
  }

  return { patch, skipped: [], proposedStatus: status };
}

export function factContaminationSnapshot(fact, assessment) {
  return {
    id: fact.id,
    fieldName: fact.fieldName,
    humanReviewStatus: fact.humanReviewStatus,
    extractionType: fact.extractionType,
    confidenceLevel: fact.confidenceLevel,
    sourceRecordId: fact.sourceRecordId,
    brandId: fact.brandId,
    extractedValue: fact.extractedValue,
    approvedValue: fact.approvedValue,
    contaminationReasons: assessment.reasons,
    severity: assessment.severity,
    proposedAction:
      assessment.severity === "quarantine"
        ? `Human Review Status → ${resolveQuarantineReviewStatus()}; append Reviewer Notes`
        : "Manual review only (report)",
  };
}

export function isEligibleForQuarantineApply(fact, assessment, options = {}) {
  if (fact.brandId !== CURIO_BRAND_ID) {
    return { ok: false, reasons: ["not_linked_to_curio_brand"] };
  }

  const sourceId = fact.sourceRecordId || "";
  const isPrimary = sourceId === PRIMARY_CONTAMINATED_SOURCE_ID;
  const isSecondary =
    sourceId === SECONDARY_REPORT_SOURCE_IDS.pointsGuide ||
    sourceId === SECONDARY_REPORT_SOURCE_IDS.usFddBadFootprint;

  if (!isPrimary && !(options.includeSecondary && isSecondary)) {
    return { ok: false, reasons: ["source_not_in_apply_scope"] };
  }

  if (assessment.severity !== "quarantine" || !assessment.contaminated) {
    return { ok: false, reasons: ["not_contaminated_for_quarantine"] };
  }

  const st = String(fact.humanReviewStatus || "");
  if (st === "Approved" || st === "Edited") {
    const blob = textBlob(fact);
    if (!containsKimpton(blob) && !containsIhg(blob)) {
      return { ok: false, reasons: ["already_approved_without_kimpton_ihg"] };
    }
  }

  if (st === "Rejected" && String(fact.reviewerNotes || "").includes("Quarantined:")) {
    return { ok: false, reasons: ["already_quarantined"] };
  }

  return { ok: true, reasons: [] };
}
