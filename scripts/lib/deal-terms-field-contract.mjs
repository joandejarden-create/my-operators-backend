/**
 * Brand Setup - Deal Terms field contract (central map + form select options).
 * Keep in sync with DEAL_TERMS_FORM_TO_AIRTABLE in api/brand-library.js and
 * public/brand-setup.html Deal Terms selects. Do not invent Airtable options here.
 */

/** Form ID → Airtable column(s). Primary column is [0]. */
export const DEAL_TERMS_FORM_TO_AIRTABLE = Object.freeze({
  minInitialTermQty: ["Quantity - Typical Minimum Initial Term"],
  minInitialTermLength: ["Length - Typical Minimum Initial Term"],
  minInitialTermDuration: ["Duration - Typical Minimum Initial Term"],
  renewalOptionQty: ["Quantity - Typical Renewal Option"],
  renewalOptionLength: ["Length - Typical Renewal Option"],
  renewalOptionDuration: ["Duration - Typical Renewal Option"],
  renewalNoticeQty: ["Length - Typical Renewal Notice Period"],
  renewalNoticeDuration: ["Quantity - Typical Renewal Notice Period"],
  renewalStructure: ["Renewal Structure"],
  renewalNoticeResponsibility: ["Renewal Notice Responsibility"],
  renewalConditions: ["Typical Renewal Conditions"],
  performanceTestRequirement: ["Performance Test Requirement"],
  curePeriodQty: ["Typical Cure Period for Performance Test Failure"],
  curePeriodDuration: ["Duration - Typical Cure Period for Performance Test Failure"],
  qaComplianceRequirement: ["Typical QA"],
  pipAtRenewal: ["Mandatory PIP at Renewal"],
  pipForConversions: ["Mandatory PIP for Conversions"],
  terminationFeeStructure: ["Typical Termination Fee Structure (if any)"],
  terminationFeeStructureNotes: ["Typical Termination Fee Structure (if any) Text"],
  performanceTerminationRights: ["Who Can Exercise Termination Right After Failed Test?"],
  typicalPIPConversionPerRoom: ["Typical Mandatory PIP for Conversions ($/room)"],
  conversionMaxTimeQty: ["Conversion - Typical max time allowed for completion"],
  conversionMaxTimeDuration: ["Conversion - Typical max time allowed for completion -Duration"],
  renewalMaxTimeQty: ["Renewal - Typical max time allowed for completion"],
  renewalMaxTimeDuration: ["Renewal - Typical max time allowed for completion -Duration"],
});

/** All Airtable columns we may write for Deal Terms. */
export const DEAL_TERMS_WRITE_COLUMNS = Object.freeze(
  Object.values(DEAL_TERMS_FORM_TO_AIRTABLE).map((cols) => cols[0])
);

/** Single-select columns (Meta API choices must be used). */
export const DEAL_TERMS_SELECT_COLUMNS = Object.freeze([
  "Duration - Typical Minimum Initial Term",
  "Duration - Typical Renewal Option",
  "Quantity - Typical Renewal Notice Period",
  "Renewal Structure",
  "Renewal Notice Responsibility",
  "Performance Test Requirement",
  "Duration - Typical Cure Period for Performance Test Failure",
  "Typical QA",
  "Mandatory PIP at Renewal",
  "Mandatory PIP for Conversions",
  "Conversion - Typical max time allowed for completion -Duration",
  "Renewal - Typical max time allowed for completion -Duration",
  "Typical Termination Fee Structure (if any)",
  "Who Can Exercise Termination Right After Failed Test?",
]);

/**
 * Options from public/brand-setup.html (authoritative for writers until Meta differs).
 * If Meta has extras, prefer Meta at write time; if form has extras Meta lacks, propose Meta add.
 */
export const DEAL_TERMS_FORM_SELECT_OPTIONS = Object.freeze({
  "Duration - Typical Minimum Initial Term": ["Year(s)", "Month(s)"],
  "Duration - Typical Renewal Option": ["Year(s)", "Month(s)"],
  "Quantity - Typical Renewal Notice Period": ["Year(s)", "Month(s)", "Day(s)"],
  "Renewal Structure": [
    "Renewal by Mutual Agreement Only",
    "Automatic Renewal",
    "Owner Option to Renew",
    "Manager Option to Renew",
  ],
  "Renewal Notice Responsibility": [
    "Owner to Operator",
    "Operator to Owner",
    "Mutual",
    "Automatic",
  ],
  "Performance Test Requirement": ["Yes", "No"],
  "Duration - Typical Cure Period for Performance Test Failure": [
    "Year(s)",
    "Month(s)",
    "Day(s)",
  ],
  "Typical QA": ["Yes", "No"],
  "Mandatory PIP at Renewal": ["Yes", "No"],
  "Mandatory PIP for Conversions": ["Yes", "No"],
  "Conversion - Typical max time allowed for completion -Duration": ["Month(s)", "Year(s)"],
  "Renewal - Typical max time allowed for completion -Duration": ["Month(s)", "Year(s)"],
  "Typical Termination Fee Structure (if any)": [
    "No Early Termination",
    "Allowed With X Months Fees",
    "Allowed With Step-Down Schedule",
    "Case-by-Case",
    "Typically None",
  ],
  "Who Can Exercise Termination Right After Failed Test?": [
    "Owner Only",
    "Mutual",
    "Rarely Exercised / Case-by-Case",
  ],
});

export const TABLE_BASICS = "Brand Setup - Brand Basics";
export const TABLE_DEAL = "Brand Setup - Deal Terms";
export const LINK_FIELD_DEAL = "Brand Setup - Deal Terms";

export const SKIP_DEAL_FIELDS = new Set([
  "Brand",
  "Brand Name",
  "BrandIDLookup",
  "Record_ID",
  "Deal_Terms_ID",
  "User_Record_ID",
]);

export function isEmptyDealValue(v) {
  if (v === undefined || v === null) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  if (Array.isArray(v) && v.length === 0) return true;
  return false;
}

/** True when expected null should clear a non-empty Airtable value. */
export function shouldClearDealValue(expected, actual) {
  return expected === null && !isEmptyDealValue(actual);
}
