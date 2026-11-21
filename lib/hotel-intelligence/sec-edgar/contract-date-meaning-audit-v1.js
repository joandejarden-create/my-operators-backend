/**
 * Reclassify persisted/extracted contractual dates without trusting prior VERIFIED labels.
 * Preserves original observation; proposes correction / quarantine only.
 */

import { DATE_MEANING } from "./contract-timing-types-v1.js";

export const CONTRACT_DATE_MEANING_AUDIT_V1 = "contract-date-meaning-audit-v1";

export const AUDITED_DATE_MEANING = Object.freeze({
  FRANCHISE_AGREEMENT_EXPIRATION: "FRANCHISE_AGREEMENT_EXPIRATION",
  MANAGEMENT_AGREEMENT_EXPIRATION: "MANAGEMENT_AGREEMENT_EXPIRATION",
  LEASE_OR_LICENSE_EXPIRATION: "LEASE_OR_LICENSE_EXPIRATION",
  HOTEL_OPENING_OR_YEAR_BUILT: "HOTEL_OPENING_OR_YEAR_BUILT",
  LOAN_MATURITY: "LOAN_MATURITY",
  RENEWAL_OR_EXTENSION_EVENT: "RENEWAL_OR_EXTENSION_EVENT",
  OTHER_CONTRACTUAL_EVENT: "OTHER_CONTRACTUAL_EVENT",
  UNRESOLVED_DATE_MEANING: "UNRESOLVED_DATE_MEANING",
});

export const EVIDENCE_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  UNVERIFIED: "UNVERIFIED",
});

export const DECISION_WINDOW_ELIGIBILITY = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  QUARANTINED: "QUARANTINED",
  INELIGIBLE: "INELIGIBLE",
});

const EXPLICIT_FRANCHISE_EXP =
  /\bfranchise\s+(?:agreement\s+)?expir(?:ation|es|y)?\b|\bfranchise\s+expiration\s+date\b/i;
const EXPLICIT_MGMT_EXP =
  /\bmanagement\s+(?:agreement\s+)?expir(?:ation|es|y)?\b|\bhma\s+expir/i;
const EXPLICIT_LEASE_LICENSE =
  /\b(?:ground\s+)?lease\s+expir|\blicense\s+(?:agreement\s+)?expir/i;
const EXPLICIT_LOAN =
  /\bloan\s+matur|\bmortgage\s+matur|\banticipated\s+repayment|\bARD\b/i;
const EXPLICIT_RENEWAL =
  /\brenewal\s+(?:option|executed|extension)|\bextended\s+(?:the\s+)?(?:franchise|management|term)\b/i;
const YEAR_OPENED_CONTEXT =
  /\b(?:year\s+(?:built|opened|open)|opened|built|constructed|vintage)\b/i;

/**
 * CMBS collateral rows often look like:
 *   ... | Hotel | Select Service | 2008 | 2018-2019 | 104 | Rooms | ...
 * The first 4-digit year after property type is Year Built/Opened — NOT franchise expiry.
 */
function looksLikeCmbsYearOpenedRow(snippet, primaryYear) {
  const s = String(snippet || "");
  if (!primaryYear) return false;
  const y = String(primaryYear);
  // Pattern: property-type token then year then rooms/appraisal OR year then N/A/rooms
  const re = new RegExp(
    String.raw`\b(?:Hotel|Extended Stay|Select Service|Limited Service|Full Service|Limited[\s-]?Service)\b[^|]{0,40}\|\s*${y}\s*\|`,
    "i",
  );
  if (re.test(s)) return true;
  // City | County | ST | ZIP | year | rooms
  const re2 = new RegExp(
    String.raw`\|\s*[A-Z]{2}\s*\|\s*\d{5}\b[^|]{0,40}\|\s*${y}\s*\|`,
    "i",
  );
  if (re2.test(s) && !EXPLICIT_FRANCHISE_EXP.test(s)) return true;
  // "... | 2004 | 2018 | 100 | Rooms"
  const re3 = new RegExp(
    String.raw`\|\s*${y}\s*\|\s*(?:\d{4}(?:\s*[-–]\s*\d{2,4})?|N\/A)\s*\|\s*\d{2,4}\s*\|\s*Rooms\b`,
    "i",
  );
  if (re3.test(s) && !EXPLICIT_FRANCHISE_EXP.test(s)) return true;
  // "... | City | ST | ZIP | 2004 | 2018 | 100 | Rooms" without county
  const re4 = new RegExp(
    String.raw`\|\s*[A-Za-z .'-]{2,40}\s*\|\s*[A-Z]{2}\s*\|\s*\d{5}\b[^|]{0,20}\|\s*${y}\s*\|`,
    "i",
  );
  if (re4.test(s) && /Rooms/i.test(s) && !EXPLICIT_FRANCHISE_EXP.test(s)) return true;
  return false;
}

function extractYear(iso) {
  const y = Number(String(iso || "").slice(0, 4));
  return Number.isFinite(y) && y >= 1900 && y <= 2100 ? y : null;
}

function sourceFamilyOf(url, snippet) {
  const u = String(url || "");
  const s = String(snippet || "");
  if (/424b|424h|cmbs|n\d{3,}/i.test(u) || /CREFI|JPMCB|GACC|Loan\s*\|/i.test(s)) {
    return "CMBS_424B";
  }
  if (/ex-?10|dex10|exhibit/i.test(u)) return "EXHIBIT_EX10";
  if (/10-k|10-q|8-k/i.test(u)) return "SEC_PERIODIC";
  return "OTHER_SEC";
}

/**
 * Audit one dated observation.
 * @returns {{auditedMeaning, confidence, decisionWindowEligibility, reasons, originalMeaning, preserveOriginal:true}}
 */
export function auditContractDateObservation(obs = {}) {
  const snippet = String(obs.evidenceSnippet || obs.snippet || (obs.evidenceSnippets || [])[0] || "");
  const originalMeaning = String(obs.dateMeaning || obs.originalMeaning || "").toUpperCase() || null;
  const primaryDate = obs.primaryDate || obs.scheduledExpiration || null;
  const year = extractYear(primaryDate);
  const url = (obs.sourceUrls || [])[0] || obs.sourceUrl || null;
  const reasons = [];
  let auditedMeaning = AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING;
  let confidence = EVIDENCE_CONFIDENCE.UNVERIFIED;

  const franchiseNegated = /\bno\s+franchise\s+expiration\b/i.test(snippet);
  if (EXPLICIT_LOAN.test(snippet) && (!EXPLICIT_FRANCHISE_EXP.test(snippet) || franchiseNegated)) {
    auditedMeaning = AUDITED_DATE_MEANING.LOAN_MATURITY;
    confidence = EVIDENCE_CONFIDENCE.HIGH;
    reasons.push("explicit_loan_maturity_language");
  } else if (EXPLICIT_RENEWAL.test(snippet) && /expir|deadline|option/i.test(snippet)) {
    auditedMeaning = AUDITED_DATE_MEANING.RENEWAL_OR_EXTENSION_EVENT;
    confidence = EVIDENCE_CONFIDENCE.MEDIUM;
    reasons.push("renewal_or_extension_language");
  } else if (EXPLICIT_FRANCHISE_EXP.test(snippet)) {
    auditedMeaning = AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION;
    confidence = EVIDENCE_CONFIDENCE.HIGH;
    reasons.push("explicit_franchise_expiration_language");
  } else if (EXPLICIT_MGMT_EXP.test(snippet)) {
    auditedMeaning = AUDITED_DATE_MEANING.MANAGEMENT_AGREEMENT_EXPIRATION;
    confidence = EVIDENCE_CONFIDENCE.HIGH;
    reasons.push("explicit_management_expiration_language");
  } else if (EXPLICIT_LEASE_LICENSE.test(snippet)) {
    auditedMeaning = AUDITED_DATE_MEANING.LEASE_OR_LICENSE_EXPIRATION;
    confidence = EVIDENCE_CONFIDENCE.HIGH;
    reasons.push("explicit_lease_or_license_expiration_language");
  } else if (YEAR_OPENED_CONTEXT.test(snippet) && year) {
    auditedMeaning = AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT;
    confidence = EVIDENCE_CONFIDENCE.HIGH;
    reasons.push("year_opened_or_built_context");
  } else if (looksLikeCmbsYearOpenedRow(snippet, year)) {
    auditedMeaning = AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT;
    confidence = EVIDENCE_CONFIDENCE.HIGH;
    reasons.push("cmbs_schedule_year_opened_column_pattern");
  } else if (
    originalMeaning === DATE_MEANING.LOAN_MATURITY_DATE ||
    /loan.?matur/i.test(originalMeaning || "")
  ) {
    auditedMeaning = AUDITED_DATE_MEANING.LOAN_MATURITY;
    confidence = EVIDENCE_CONFIDENCE.MEDIUM;
    reasons.push("prior_loan_maturity_label_retained_pending_snippet");
  } else if (
    year &&
    year <= 2018 &&
    originalMeaning === DATE_MEANING.FRANCHISE_EXPIRATION &&
    /Rooms/i.test(snippet) &&
    !EXPLICIT_FRANCHISE_EXP.test(snippet)
  ) {
    // Historical year + rooms schedule without franchise expiration wording → likely year opened
    auditedMeaning = AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT;
    confidence = EVIDENCE_CONFIDENCE.MEDIUM;
    reasons.push("historical_year_with_rooms_schedule_without_franchise_expiration_wording");
  } else if (
    /\b12\/31\/(20(2[0-9]|3[0-5]))\b/.test(snippet) &&
    /franchise|lease|fee\/leasehold|option/i.test(snippet)
  ) {
    // Explicit calendar expiration often lease/franchise in REIT schedules
    if (/lease|fee\/leasehold/i.test(snippet) && !/franchise/i.test(snippet)) {
      auditedMeaning = AUDITED_DATE_MEANING.LEASE_OR_LICENSE_EXPIRATION;
    } else if (/franchise/i.test(snippet)) {
      auditedMeaning = AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION;
    } else {
      auditedMeaning = AUDITED_DATE_MEANING.OTHER_CONTRACTUAL_EVENT;
    }
    confidence = EVIDENCE_CONFIDENCE.MEDIUM;
    reasons.push("explicit_calendar_expiration_in_snippet");
  } else {
    auditedMeaning = AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING;
    confidence = EVIDENCE_CONFIDENCE.LOW;
    reasons.push("insufficient_evidence_for_date_meaning");
  }

  const eligibleMeanings = new Set([
    AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION,
    AUDITED_DATE_MEANING.MANAGEMENT_AGREEMENT_EXPIRATION,
    AUDITED_DATE_MEANING.LEASE_OR_LICENSE_EXPIRATION,
    AUDITED_DATE_MEANING.RENEWAL_OR_EXTENSION_EVENT,
  ]);

  let decisionWindowEligibility = DECISION_WINDOW_ELIGIBILITY.INELIGIBLE;
  if (
    eligibleMeanings.has(auditedMeaning) &&
    (confidence === EVIDENCE_CONFIDENCE.HIGH || confidence === EVIDENCE_CONFIDENCE.MEDIUM)
  ) {
    decisionWindowEligibility = DECISION_WINDOW_ELIGIBILITY.ELIGIBLE;
  } else if (auditedMeaning === AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING) {
    decisionWindowEligibility = DECISION_WINDOW_ELIGIBILITY.QUARANTINED;
  } else {
    decisionWindowEligibility = DECISION_WINDOW_ELIGIBILITY.INELIGIBLE;
  }

  const priorClaimedContractExpiration =
    originalMeaning === DATE_MEANING.FRANCHISE_EXPIRATION ||
    originalMeaning === DATE_MEANING.MANAGEMENT_EXPIRATION ||
    /FRANCHISE_EXPIRATION|MANAGEMENT_EXPIRATION/.test(originalMeaning || "");

  const misclassifiedOpening =
    priorClaimedContractExpiration &&
    auditedMeaning === AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT;

  return {
    version: CONTRACT_DATE_MEANING_AUDIT_V1,
    contractId: obs.contractId || null,
    milestoneId: obs.milestoneId || null,
    hotelName: obs.hotelName || null,
    agreementType: obs.agreementType || null,
    primaryDate,
    originalMeaning,
    auditedMeaning,
    confidence,
    decisionWindowEligibility,
    misclassifiedOpening,
    reasons,
    sourceFamily: sourceFamilyOf(url, snippet),
    sourceUrl: url,
    evidenceSnippet: snippet.slice(0, 400),
    preserveOriginal: true,
    deleteEvidenceForbidden: true,
  };
}

export function summarizeDateMeaningAudit(rows) {
  const byMeaning = {};
  const bySource = {};
  const byAgreementType = {};
  const byConfidence = {};
  const byEligibility = {};
  let misclassifiedOpening = 0;
  let confirmedContractExpirations = 0;
  let loanMaturities = 0;
  let unresolved = 0;

  for (const r of rows) {
    byMeaning[r.auditedMeaning] = (byMeaning[r.auditedMeaning] || 0) + 1;
    bySource[r.sourceFamily] = (bySource[r.sourceFamily] || 0) + 1;
    byAgreementType[r.agreementType || "UNKNOWN"] =
      (byAgreementType[r.agreementType || "UNKNOWN"] || 0) + 1;
    byConfidence[r.confidence] = (byConfidence[r.confidence] || 0) + 1;
    byEligibility[r.decisionWindowEligibility] =
      (byEligibility[r.decisionWindowEligibility] || 0) + 1;
    if (r.misclassifiedOpening) misclassifiedOpening += 1;
    if (
      r.auditedMeaning === AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION ||
      r.auditedMeaning === AUDITED_DATE_MEANING.MANAGEMENT_AGREEMENT_EXPIRATION ||
      r.auditedMeaning === AUDITED_DATE_MEANING.LEASE_OR_LICENSE_EXPIRATION
    ) {
      confirmedContractExpirations += 1;
    }
    if (r.auditedMeaning === AUDITED_DATE_MEANING.LOAN_MATURITY) loanMaturities += 1;
    if (r.auditedMeaning === AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING) unresolved += 1;
  }

  return {
    audited: rows.length,
    byMeaning,
    bySource,
    byAgreementType,
    byConfidence,
    byEligibility,
    confirmedContractExpirations,
    misclassifiedOpeningDates: misclassifiedOpening,
    loanMaturities,
    unresolvedDateMeanings: unresolved,
  };
}

/**
 * Build Research Mode correction manifest (non-destructive).
 */
export function buildCorrectionManifest(auditRows) {
  const corrections = [];
  for (const r of auditRows) {
    if (!r.misclassifiedOpening && r.decisionWindowEligibility === DECISION_WINDOW_ELIGIBILITY.ELIGIBLE) {
      continue;
    }
    if (
      !r.misclassifiedOpening &&
      r.auditedMeaning === r.originalMeaning &&
      r.decisionWindowEligibility !== DECISION_WINDOW_ELIGIBILITY.QUARANTINED
    ) {
      continue;
    }
    corrections.push({
      contractId: r.contractId,
      milestoneId: r.milestoneId,
      hotelName: r.hotelName,
      action:
        r.decisionWindowEligibility === DECISION_WINDOW_ELIGIBILITY.ELIGIBLE
          ? "RECLASSIFY_DATE_MEANING"
          : "QUARANTINE_FROM_DECISION_WINDOWS",
      originalMeaning: r.originalMeaning,
      correctedMeaning: r.auditedMeaning,
      primaryDatePreserved: r.primaryDate,
      decisionWindowEligibility: r.decisionWindowEligibility,
      confidence: r.confidence,
      reasons: r.reasons,
      notesPatch: [
        "DATE_MEANING_AUDIT_V1",
        `audited=${r.auditedMeaning}`,
        `eligibility=${r.decisionWindowEligibility}`,
        `confidence=${r.confidence}`,
        ...(r.reasons || []),
      ].join(" | "),
      deleteHistoricalEvidence: false,
      hpcUpdate: false,
      customerPublication: false,
    });
  }
  return {
    version: CONTRACT_DATE_MEANING_AUDIT_V1,
    status: "PROPOSED_RESEARCH_MODE_CORRECTIONS",
    applyRequires: "HIF_APPLY_RESEARCH=1 + founder authorization",
    correctionCount: corrections.length,
    corrections,
  };
}

export default {
  auditContractDateObservation,
  summarizeDateMeaningAudit,
  buildCorrectionManifest,
  AUDITED_DATE_MEANING,
  DECISION_WINDOW_ELIGIBILITY,
};
