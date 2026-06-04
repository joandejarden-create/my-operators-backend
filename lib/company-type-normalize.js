/**
 * Company Profile "Company Type" ↔ Partner Directory User Type filter.
 * (Airtable "User Type" column is often empty; directory uses Company Type.)
 */

export const COMPANY_TYPE_AIRTABLE_FIELD = "Company Type";

/** Canonical Airtable single-select value (Company Profile > Company Type). */
export const COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE = "Hotel Owner - Operator";

/** Legacy / alias Airtable strings → same filter key as canonical value. */
export const COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE_ALIASES = [
  COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  "Owner-Operator",
  "Owner Operator",
  "Hotel Owner Operator",
];

/** Filter / API key → exact Airtable single-select (Company Settings). */
export const COMPANY_TYPE_FILTER_TO_AIRTABLE = {
  "HOTEL MGMT. COMPANY": "Hotel Management Company",
  "HOTEL BRANDS (FRANCHISE)": "Hotel Brands (Franchise)",
  "HOTEL OWNERS": "Hotel Owner",
  "HOSPITALITY CONSULTANTS": "Hospitality Consultants",
  OWNER_OPERATOR: COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE,
  OTHER: "Other",
};

export const COMPANY_TYPE_AIRTABLE_TO_FILTER = {
  ...Object.fromEntries(
    Object.entries(COMPANY_TYPE_FILTER_TO_AIRTABLE).map(([filterKey, airtableVal]) => [
      airtableVal,
      filterKey,
    ])
  ),
  ...Object.fromEntries(
    COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE_ALIASES.map((airtableVal) => [airtableVal, "OWNER_OPERATOR"])
  ),
};

/** User Type dropdown (value = filter key, label = proper case UI). */
export const PARTNER_DIRECTORY_USER_TYPE_FILTERS = [
  { value: "", label: "All Types" },
  { value: "HOTEL MGMT. COMPANY", label: "3rd Party Operator" },
  { value: "HOTEL BRANDS (FRANCHISE)", label: "Hotel Brands (Franchise)" },
  { value: "HOTEL OWNERS", label: "Hotel Owners" },
  { value: "OWNER_OPERATOR", label: "Hotel Owner - Operator" },
  { value: "HOSPITALITY CONSULTANTS", label: "Advisor / Consultant" },
  { value: "OTHER", label: "Other" },
];

export function companyTypeFilterLabel(filterKey) {
  const key = filterKey == null ? "" : String(filterKey).trim();
  const row = PARTNER_DIRECTORY_USER_TYPE_FILTERS.find((o) => o.value === key);
  return row ? row.label : key;
}

function normalizeFieldKey(key) {
  return String(key)
    .toLowerCase()
    .replace(/[\u2018\u2019\u2032]/g, "'");
}

export function getAirtableFieldValue(fields, fieldName) {
  if (!fields || typeof fields !== "object") return undefined;
  const direct = fields[fieldName];
  if (direct != null && String(direct).trim() !== "") return direct;

  const target = normalizeFieldKey(fieldName);
  for (const k of Object.keys(fields)) {
    if (normalizeFieldKey(k) !== target) continue;
    const v = fields[k];
    if (v != null && String(v).trim() !== "") return v;
  }
  return undefined;
}

/**
 * True when a Company Type string is Owner-Operator (any alias).
 * Checked before generic OWNER / OPERATOR substring rules.
 */
export function isOwnerOperatorCompanyTypeString(rawValue) {
  const raw = rawValue == null ? "" : String(rawValue).trim();
  if (!raw) return false;
  if (COMPANY_TYPE_AIRTABLE_TO_FILTER[raw] === "OWNER_OPERATOR") return true;

  const upper = raw
    .toUpperCase()
    .replace(/[\u2013\u2014]/g, "-")
    .replace(/\s+/g, " ")
    .trim();

  if (
    upper === "HOTEL OWNER - OPERATOR" ||
    upper === "HOTEL OWNER OPERATOR" ||
    upper === "OWNER-OPERATOR" ||
    upper === "OWNER OPERATOR" ||
    upper === "OWNER_OPERATOR"
  ) {
    return true;
  }
  if (upper.includes("OWNER-OPERATOR") || upper.includes("OWNER OPERATOR")) {
    return true;
  }
  if (/HOTEL\s+OWNER\s*-\s*OPERATOR/.test(upper) || /HOTEL\s+OWNER\s+OPERATOR/.test(upper)) {
    return true;
  }
  return false;
}

/**
 * Normalize Company Type (or legacy User Type) to Partner Directory filter key.
 */
export function normalizeCompanyTypeToFilterKey(rawValue) {
  const raw = rawValue == null ? "" : String(rawValue).trim();
  if (!raw) return "";

  if (COMPANY_TYPE_AIRTABLE_TO_FILTER[raw]) return COMPANY_TYPE_AIRTABLE_TO_FILTER[raw];

  if (isOwnerOperatorCompanyTypeString(raw)) return "OWNER_OPERATOR";

  const upper = raw.toUpperCase();

  if (
    upper === "HOTEL OWNERS" ||
    upper === "HOTEL OWNER" ||
    upper === "OWNER" ||
    upper === "OWNERS"
  ) {
    return "HOTEL OWNERS";
  }
  if (
    upper === "HOTEL BRANDS (FRANCHISE)" ||
    upper === "HOTEL BRAND" ||
    upper === "HOTEL BRANDS" ||
    upper === "BRAND" ||
    upper === "BRANDS" ||
    upper === "FRANCHISE" ||
    upper.includes("HOTEL BRANDS")
  ) {
    return "HOTEL BRANDS (FRANCHISE)";
  }
  if (
    upper === "HOTEL MGMT. COMPANY" ||
    upper === "HOTEL MGMT COMPANY" ||
    upper === "HOTEL MANAGEMENT COMPANY" ||
    upper === "MGMT" ||
    upper === "MANAGEMENT" ||
    upper === "OPERATOR" ||
    upper === "3RD PARTY OPERATOR"
  ) {
    return "HOTEL MGMT. COMPANY";
  }
  if (
    upper === "HOSPITALITY CONSULTANTS" ||
    upper === "HOSPITALITY CONSULTANT" ||
    upper.includes("CONSULTANT") ||
    upper.includes("ADVISOR") ||
    upper.includes("BROKER")
  ) {
    return "HOSPITALITY CONSULTANTS";
  }
  if (upper === "OTHER") {
    return "OTHER";
  }
  if (upper.includes("LENDER") || upper.includes("LEGAL")) {
    return "HOSPITALITY CONSULTANTS";
  }
  if (upper.includes("BRAND") || upper.includes("FRANCHISE")) {
    return "HOTEL BRANDS (FRANCHISE)";
  }
  if (upper.includes("MGMT") || upper.includes("MANAGEMENT")) {
    return "HOTEL MGMT. COMPANY";
  }
  if (upper === "OPERATOR" || upper === "3RD PARTY OPERATOR") {
    return "HOTEL MGMT. COMPANY";
  }
  if (upper.includes("OPERATOR") && !upper.includes("OWNER")) {
    return "HOTEL MGMT. COMPANY";
  }
  if (upper.includes("OWNER")) {
    return "HOTEL OWNERS";
  }

  return "";
}

export function companyTypeFromProfileFields(fields) {
  const companyType = getAirtableFieldValue(fields, COMPANY_TYPE_AIRTABLE_FIELD);
  const userType = getAirtableFieldValue(fields, "User Type");
  return normalizeCompanyTypeToFilterKey(companyType || userType || "");
}

export function isKnownPartnerDirectoryCompanyType(filterKey) {
  return Boolean(filterKey && COMPANY_TYPE_FILTER_TO_AIRTABLE[filterKey]);
}

export function partnerDirectoryTypeToCardClass(filterKey) {
  if (filterKey === "HOTEL OWNERS") return "owners";
  if (filterKey === "HOTEL BRANDS (FRANCHISE)") return "brands";
  if (filterKey === "HOTEL MGMT. COMPANY") return "mgmt";
  if (filterKey === "OWNER_OPERATOR") return "owner-operator";
  if (filterKey === "HOSPITALITY CONSULTANTS") return "advisor";
  if (filterKey === "OTHER") return "other";
  return "";
}
