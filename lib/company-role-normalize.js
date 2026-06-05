/**
 * Normalize Company Profile "Company's role in the hotel ecosystem" ↔ form filter values.
 * Canonical Airtable options: "Prefix - description" in sentence case.
 */

export const COMPANY_ROLE_AIRTABLE_FIELD = "Company's role in the hotel ecosystem";

export const COMPANY_ROLE_FORM_DISPLAY_LABELS = {
  Brand: "We represent a hotel brand (franchise/licensing platform)",
  Operator: "We operate hotels under third-party brands (operator only)",
  Both: "We both represent a brand and operate hotels",
  Owner: "We are an owner, developer, or investor",
  OwnerOperator:
    "We own, develop, or control hotel assets and we operate hotels",
  Advisor: "We are a broker, consultant, or service provider",
  Lender: "We are a lender or legal/advisory firm",
};

export const COMPANY_ROLE_AIRTABLE_CHOICES = {
  Brand: `Brand - ${COMPANY_ROLE_FORM_DISPLAY_LABELS.Brand}`,
  Operator: `Operator - ${COMPANY_ROLE_FORM_DISPLAY_LABELS.Operator}`,
  Both: `Both - ${COMPANY_ROLE_FORM_DISPLAY_LABELS.Both}`,
  Owner: `Owner - ${COMPANY_ROLE_FORM_DISPLAY_LABELS.Owner}`,
  OwnerOperator: `Owner-Operator - ${COMPANY_ROLE_FORM_DISPLAY_LABELS.OwnerOperator}`,
  Advisor: `Advisor - ${COMPANY_ROLE_FORM_DISPLAY_LABELS.Advisor}`,
  Lender: `Lender - ${COMPANY_ROLE_FORM_DISPLAY_LABELS.Lender}`,
};

export const COMPANY_ROLE_TITLE_CASE_AIRTABLE_CHOICES = {
  Brand: "Brand - We Represent A Hotel Brand (Franchise/Licensing Platform)",
  Operator: "Operator - We Operate Hotels Under Third-Party Brands (Operator Only)",
  Both: "Both - We Both Represent A Brand And Operate Hotels",
  Owner: "Owner - We Are An Owner, Developer, Or Investor",
  OwnerOperator:
    "Owner-Operator - We Own, Develop, Or Control Hotel Assets And We Operate Hotels",
  Advisor: "Advisor - We Are A Broker, Consultant, Or Service Provider",
  Lender: "Lender - We Are A Lender Or Legal/Advisory Firm",
};

export const COMPANY_ROLE_FORM_TO_AIRTABLE = { ...COMPANY_ROLE_AIRTABLE_CHOICES };

const COMPANY_ROLE_PREFIX_TO_FORM = {
  brand: "Brand",
  operator: "Operator",
  both: "Both",
  owner: "Owner",
  advisor: "Advisor",
  lender: "Lender",
};

export const COMPANY_ROLE_AIRTABLE_TO_FORM = Object.fromEntries([
  ...Object.entries(COMPANY_ROLE_AIRTABLE_CHOICES).map(([formVal, airtableVal]) => [
    airtableVal,
    formVal,
  ]),
  ...Object.entries(COMPANY_ROLE_FORM_DISPLAY_LABELS).map(([formVal, airtableVal]) => [
    airtableVal,
    formVal,
  ]),
  ...Object.entries(COMPANY_ROLE_TITLE_CASE_AIRTABLE_CHOICES).map(([formVal, airtableVal]) => [
    airtableVal,
    formVal,
  ]),
]);

export function companyRoleFormDisplayLabel(formKey) {
  const key = formKey == null ? "" : String(formKey).trim();
  return COMPANY_ROLE_FORM_DISPLAY_LABELS[key] || "";
}

export function companyRoleAirtableValueFromForm(formKey) {
  const key = formKey == null ? "" : String(formKey).trim();
  return COMPANY_ROLE_FORM_TO_AIRTABLE[key] || "";
}

export function companyRoleDisplayLabel(rawAirtableValue, formKey) {
  const raw = rawAirtableValue == null ? "" : String(rawAirtableValue).trim();
  if (raw) return raw;
  return companyRoleAirtableValueFromForm(formKey) || companyRoleFormDisplayLabel(formKey);
}

const COMPANY_ROLE_FIELD_NAMES = [
  COMPANY_ROLE_AIRTABLE_FIELD,
  "Company Role",
  "Company role in the hotel ecosystem",
];

export const PARTNER_DIRECTORY_COMPANY_ROLE_FILTERS = [
  { value: "", label: "All Roles" },
  { value: "Brand", label: "Brand (Franchise / Licensing)" },
  { value: "Operator", label: "Operator (Third-Party Brands)" },
  { value: "Both", label: "Brand & Operator (Both)" },
  { value: "Owner", label: "Owner / Developer / Investor" },
  { value: "OwnerOperator", label: "Owner-Operator" },
  { value: "Advisor", label: "Advisor / Consultant" },
  { value: "Lender", label: "Lender / Legal-Advisory" },
];

export function companyRoleFilterLabel(normalizedValue) {
  const key = normalizedValue == null ? "" : String(normalizedValue).trim();
  const row = PARTNER_DIRECTORY_COMPANY_ROLE_FILTERS.find((o) => o.value === key);
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
  for (const key of Object.keys(fields)) {
    if (normalizeFieldKey(key) !== target) continue;
    const value = fields[key];
    if (value != null && String(value).trim() !== "") return value;
  }
  return undefined;
}

export function companyRoleFromEcosystemField(fields) {
  return normalizeCompanyRoleToForm(
    getAirtableFieldValue(fields, COMPANY_ROLE_AIRTABLE_FIELD)
  );
}

function normalizeCompanyRoleFromPrefix(raw) {
  const lower = raw.toLowerCase();
  if (lower.startsWith("owner-operator") || lower.startsWith("owner operator")) {
    return "OwnerOperator";
  }
  const match = raw.match(/^([A-Za-z]+)\s*[-–—]\s*/);
  if (!match) return "";
  const key = match[1].toLowerCase();
  return COMPANY_ROLE_PREFIX_TO_FORM[key] || "";
}

export function normalizeCompanyRoleToForm(rawValue) {
  const raw = rawValue == null ? "" : String(rawValue).trim();
  if (!raw) return "";
  if (COMPANY_ROLE_AIRTABLE_TO_FORM[raw]) return COMPANY_ROLE_AIRTABLE_TO_FORM[raw];

  const fromPrefix = normalizeCompanyRoleFromPrefix(raw);
  if (fromPrefix) return fromPrefix;

  const lower = raw.toLowerCase();
  if (lower.startsWith("brand") || lower.includes("franchise/licensing platform")) return "Brand";
  if (lower.startsWith("operator") || lower.includes("operator only")) return "Operator";
  if (lower.startsWith("both")) return "Both";
  if (
    lower.startsWith("owner-operator") ||
    lower.startsWith("owner operator") ||
    lower.includes("own, develop, or control hotel assets and we operate")
  ) {
    return "OwnerOperator";
  }
  if (lower.startsWith("owner") || lower.includes("developer, or investor")) return "Owner";
  if (lower.startsWith("advisor") || lower.includes("broker, consultant")) return "Advisor";
  if (lower.startsWith("lender") || lower.includes("legal/advisory")) return "Lender";
  return "";
}

export function companyRoleFromAirtableFields(fields) {
  if (!fields || typeof fields !== "object") return "";
  const ecosystem = companyRoleFromEcosystemField(fields);
  if (ecosystem) return ecosystem;

  for (const name of COMPANY_ROLE_FIELD_NAMES) {
    if (name === COMPANY_ROLE_AIRTABLE_FIELD) continue;
    const value = getAirtableFieldValue(fields, name);
    if (value != null) {
      const normalized = normalizeCompanyRoleToForm(value);
      if (normalized) return normalized;
    }
  }
  for (const key of Object.keys(fields)) {
    const lower = key.toLowerCase().trim();
    if (
      lower.includes("role") &&
      lower.includes("ecosystem") &&
      fields[key] != null &&
      String(fields[key]).trim() !== ""
    ) {
      return normalizeCompanyRoleToForm(fields[key]);
    }
  }
  return "";
}
