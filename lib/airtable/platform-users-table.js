/**
 * Single Airtable table for platform users (Memberstack auth, deals, company admin, partner directory).
 * Replaces the legacy User Management table (tblQEpYKf2aYNKKjw) in application code.
 */

import { INTAKE_USERS_TABLE } from "../../api/schemas/intake-deal-fields.js";

export const PLATFORM_USERS_TABLE_ID =
  process.env.USERS_TABLE_ID ||
  process.env.AIRTABLE_ME_USERS_TABLE ||
  process.env.AIRTABLE_INTAKE_USERS_TABLE ||
  INTAKE_USERS_TABLE;

/** @deprecated Use PLATFORM_USERS_TABLE_ID — legacy User Management table id for migration only */
export const LEGACY_USER_MANAGEMENT_TABLE_ID =
  process.env.LEGACY_USER_MANAGEMENT_TABLE_ID || "tblQEpYKf2aYNKKjw";

export const PLATFORM_USERS_COMPANY_TABLE_ID =
  process.env.USER_MANAGEMENT_COMPANY_TABLE_ID ||
  process.env.AIRTABLE_COMPANY_PROFILE_TABLE ||
  "tblItyfH6MlOnMKZ9";

/** Writable Airtable field names on the Users table */
export const PUF = {
  firstName: "First Name",
  lastName: "Last Name",
  companyTitle: "Company Title",
  phoneNumber: "Phone Number",
  email: process.env.AIRTABLE_USERS_EMAIL_FIELD_NAME || "Email",
  companyEmail: process.env.AIRTABLE_USERS_COMPANY_EMAIL_FIELD_NAME || "Company Email",
  companyProfile:
    process.env.AIRTABLE_USERS_COMPANY_LINK_FIELD || "Company Profile",
  /** Legacy UM column name — same link as Company Profile when present on Users */
  company: process.env.AIRTABLE_USERS_COMPANY_ALIAS_FIELD || "Company",
  platformRole: "Platform Role",
  contactVisibility: "Contact Visibility",
  dealAccess: "Deal Access",
  documentAccess: "Document Access",
  country: "Based (Country)",
  /** Partner Directory individual card stats (Airtable API field names; grid may show a # prefix in the UI). */
  closedDeals: "Closed Deals",
  uniqueBrandsDeals: "Unique Brands (Deals)",
  submittedBids: "Submitted Bids",
  coverageTerritories: "Coverage Territories",
};

export const REGION_CHECKBOX_FIELDS = [
  "Region - America",
  "Region - Caribbean & Latin America",
  "Region - Europe",
  "Region - Middle East & Africa",
  "Region - Asia Pacific",
];

export const REGION_CODE_TO_CHECKBOX_FIELDS = {
  AMERICAS: ["Region - America"],
  CALA: ["Region - Caribbean & Latin America"],
  EUROPE: ["Region - Europe"],
  MEA: ["Region - Middle East & Africa"],
  AP: ["Region - Asia Pacific"],
};

const PROFILE_FIELD_CANDIDATES = ["Profile", "Profile Picture", "Headshot", "Photo", "Avatar"];

export function profilePhotoUrlFromFields(fields) {
  if (!fields || typeof fields !== "object") return "";
  for (const name of PROFILE_FIELD_CANDIDATES) {
    const v = fields[name];
    if (Array.isArray(v) && v.length > 0 && v[0] && typeof v[0].url === "string") {
      return v[0].url.trim();
    }
    if (typeof v === "string" && v.startsWith("http")) return v.trim();
  }
  return "";
}

export function emailFromUserFields(fields) {
  if (!fields) return "";
  const primary = fields[PUF.email];
  const company = fields[PUF.companyEmail];
  const pick = (v) => {
    if (Array.isArray(v)) return (v[0] && String(v[0]).trim()) || "";
    return v != null ? String(v).trim() : "";
  };
  return pick(primary) || pick(company) || pick(fields.Email) || pick(fields["Company Email"]) || "";
}

export function companyProfileIdFromFields(fields) {
  if (!fields) return null;
  for (const key of [PUF.companyProfile, PUF.company, "Company Profile"]) {
    const v = fields[key];
    if (Array.isArray(v) && v.length > 0) {
      const id = typeof v[0] === "string" ? v[0] : v[0]?.id;
      if (id && String(id).startsWith("rec")) return id;
    }
    if (typeof v === "string" && v.startsWith("rec")) return v;
  }
  return null;
}

/** Fields required on Users (create in Airtable before migration). */
export const USERS_TABLE_FIELDS_TO_ENSURE = [
  PUF.companyTitle,
  PUF.phoneNumber,
  PUF.companyEmail,
  PUF.platformRole,
  PUF.contactVisibility,
  PUF.dealAccess,
  PUF.documentAccess,
  PUF.country,
  PUF.closedDeals,
  PUF.uniqueBrandsDeals,
  PUF.submittedBids,
  PUF.coverageTerritories,
  ...REGION_CHECKBOX_FIELDS,
  ...PROFILE_FIELD_CANDIDATES,
];

export function companyFilterFormula(companyProfileId) {
  const id = String(companyProfileId || "")
    .trim()
    .replace(/'/g, "\\'");
  if (!id) return "";
  return `OR(FIND('${id}', ARRAYJOIN({${PUF.companyProfile}}) & '') > 0, FIND('${id}', ARRAYJOIN({Company}) & '') > 0)`;
}

/** Read Contact Visibility single-select from a Users / UM Airtable fields object. */
export function contactVisibilityFromFields(fields) {
  if (!fields || typeof fields !== "object") return "";
  const cv = fields[PUF.contactVisibility] ?? fields["Contact Visibility"];
  if (typeof cv === "object" && cv && cv.name) return String(cv.name).trim();
  return typeof cv === "string" ? cv.trim() : "";
}

/**
 * Partner Directory Individuals tab: only list users who opted into directory visibility.
 * Hide Contact, Visible on Match, and Admin Controlled are excluded.
 * Empty/unset matches User Management default (Show Contact).
 */
export function isContactVisibleInPartnerDirectory(contactVisibility) {
  const v = String(contactVisibility || "").trim().toLowerCase();
  if (!v) return true;
  return v === "show contact";
}
