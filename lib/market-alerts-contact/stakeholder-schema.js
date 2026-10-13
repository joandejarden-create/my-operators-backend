/**
 * MarketAlertStakeholders — Dealality-owned stakeholder intelligence only.
 * NEVER includes Surfe-derived email / phone / LinkedIn / provider IDs / payloads.
 */

export const MARKET_ALERT_STAKEHOLDERS_TABLE =
  process.env.AIRTABLE_TABLE_MARKET_ALERT_STAKEHOLDERS || "MarketAlertStakeholders";

export const map_marketAlertStakeholderFields = Object.freeze({
  stakeholderId: "Stakeholder ID",
  alert: "Alert",
  personName: "Person Name",
  jobTitle: "Job Title",
  company: "Company",
  stakeholderClass: "Stakeholder Class",
  relationshipToProject: "Relationship to Project",
  whyRelevant: "Why Relevant",
  articleDerived: "Article Derived",
  quoted: "Quoted",
  confidence: "Confidence",
  identificationStatus: "Identification Status",
  selectedPrimary: "Selected Primary",
  selectedSecondary: "Selected Secondary",
  decisionOpen: "Decision Open",
  sourceType: "Source Type",
  identifiedAt: "Identified At",
  updatedAt: "Updated At",
  articleMentionCount: "Article Mention Count",
  sourceArticleUrl: "Source Article URL",
  sourceArticleTitle: "Source Article Title",
  contactLookupStatus: "Contact Lookup Status",
  // Dealality-owned LinkedIn only (HIGH + DEALALITY_PUBLIC_RESEARCH). Never Surfe.
  linkedinUrl: "LinkedIn Profile URL",
  linkedinResolutionConfidence: "LinkedIn Resolution Confidence",
  linkedinResolutionSource: "LinkedIn Resolution Source",
  linkedinResolutionStatus: "LinkedIn Resolution Status",
});

export const IDENTIFICATION_STATUS = Object.freeze({
  IDENTIFIED: "IDENTIFIED",
  MULTIPLE_CANDIDATES: "MULTIPLE_CANDIDATES",
  COMPANY_ONLY: "COMPANY_ONLY",
  BLOCKED_ENTITY_UNKNOWN: "BLOCKED_ENTITY_UNKNOWN",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export const CONTACT_LOOKUP_STATUS = Object.freeze({
  NOT_REQUESTED: "NOT_REQUESTED",
  PENDING: "PENDING",
  MISS: "MISS",
  FAILED_RETRYABLE: "FAILED_RETRYABLE",
});

export const STAKEHOLDER_SOURCE_TYPE = Object.freeze({
  ARTICLE: "ARTICLE",
  DEALALITY_INFER: "DEALALITY_INFER",
  USER_FIND_PERSON: "USER_FIND_PERSON",
});

export const STAKEHOLDER_CLASS_OPTIONS = Object.freeze([
  "OWNER_PRINCIPAL",
  "DEVELOPER_PRINCIPAL",
  "DEVELOPMENT_EXECUTIVE",
  "INVESTMENT_EXECUTIVE",
  "ASSET_MANAGER",
  "BRAND_DEVELOPMENT",
  "OPERATOR_EXECUTIVE",
  "PREOPENING_LEADERSHIP",
  "LENDER",
  "PROJECT_EXECUTIVE",
  "ARCHITECT_CONTRACTOR",
  "GOVERNMENT",
  "ANALYST_CONSULTANT",
  "PR_MEDIA",
  "OTHER",
]);

export const CONFIDENCE_OPTIONS = Object.freeze(["HIGH", "MEDIUM", "LOW"]);

/** Surfe-derived contact PII — must never be written to MarketAlertStakeholders. */
export const FORBIDDEN_SURFE_PII_FIELDS = Object.freeze([
  "Email",
  "Phone",
  "Mobile",
  "Provider Person ID",
  "Surfe Person ID",
  "Provider Payload",
  "Raw Provider Payload",
  // Surfe may return LinkedIn — never copy that response into Airtable.
  // Dealality-resolved LinkedIn uses map fields linkedinUrl + source DEALALITY_PUBLIC_RESEARCH.
]);

/** Accepted LinkedIn Resolution Source values for persistence. */
export const LINKEDIN_PERSIST_SOURCE_ALLOWED = Object.freeze(["DEALALITY_PUBLIC_RESEARCH"]);

/**
 * Strip any Surfe-derived contact detail keys from an object (shallow + nested contacts arrays).
 */
export function stripSurfeContactDetails(input) {
  if (input == null) return input;
  if (Array.isArray(input)) return input.map(stripSurfeContactDetails);
  if (typeof input !== "object") return input;

  const banned = new Set([
    "email",
    "emails",
    "emailStatus",
    "phone",
    "phones",
    "mobile",
    "mobilePhone",
    "mobilePhones",
    "phoneStatus",
    "providerPersonId",
    "externalID",
    "externalId",
    "raw",
    "rawPayload",
    "providerPayload",
    "surfePayload",
    "rawSurfeResponse",
    "surfeResponse",
    "surfePersonId",
  ]);

  // Preserve Dealality-resolved LinkedIn; strip Surfe-sourced LinkedIn keys.
  const dealalityLinkedIn =
    input.linkedinResolutionSource === "DEALALITY_PUBLIC_RESEARCH" &&
    input.linkedinResolutionConfidence === "HIGH" &&
    input.linkedinResolutionStatus === "CONFIRMED" &&
    input.linkedinUrl
      ? {
          linkedinUrl: input.linkedinUrl,
          linkedinResolutionSource: input.linkedinResolutionSource,
          linkedinResolutionConfidence: input.linkedinResolutionConfidence,
          linkedinResolutionStatus: input.linkedinResolutionStatus,
        }
      : null;

  const linkedInKeys = new Set(["linkedinUrl", "linkedInUrl", "linkedin"]);

  const out = {};
  for (const [k, v] of Object.entries(input)) {
    if (banned.has(k) || linkedInKeys.has(k)) continue;
    if (k === "contacts" || k === "people" || k === "cards") {
      out[k] = stripSurfeContactDetails(v);
      continue;
    }
    if (v && typeof v === "object") {
      out[k] = stripSurfeContactDetails(v);
    } else {
      out[k] = v;
    }
  }
  if (dealalityLinkedIn) Object.assign(out, dealalityLinkedIn);
  return out;
}

export function normalizeStakeholderKeyPart(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 80);
}

/**
 * Stable upsert key: alertId + person/company + class/role.
 */
export function buildStakeholderStableKey({
  alertId,
  personName,
  company,
  stakeholderClass,
  jobTitle,
} = {}) {
  const a = normalizeStakeholderKeyPart(alertId);
  const p = normalizeStakeholderKeyPart(personName) || "noperson";
  const c = normalizeStakeholderKeyPart(company) || "nocompany";
  const cls = normalizeStakeholderKeyPart(stakeholderClass || jobTitle) || "other";
  return `mas_${a}__${p}__${c}__${cls}`;
}
