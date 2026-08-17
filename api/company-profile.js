/**
 * Company Profile API – map Company Settings form fields to Airtable Company Profile table.
 * Table: Company Profile (tblItyfH6MlOnMKZ9)
 *
 * All form field names and Airtable column names are defined here so the mapping
 * stays in one place and matches COMPANY_PROFILE_AIRTABLE_MAPPING.md.
 */

import Airtable from "airtable";

import { stripLeadingWwwFromWebsiteUrl } from "./lib/strip-www-from-website-url.js";
import {
  COMPANY_ROLE_AIRTABLE_FIELD,
  COMPANY_ROLE_FORM_TO_AIRTABLE,
  companyRoleDisplayLabel,
  normalizeCompanyRoleToForm,
} from "../lib/company-role-normalize.js";
import {
  airtableCompanyTypeToFormKey,
  airtableFieldsToOwnerOperatorPrefill,
  companyTypeDisplayLabel,
  finalizeCompanyProfileFieldsForAirtableWrite,
  mergeOwnerOperatorExtensionFields,
  pickFirstCompanyTypeInput,
  toAirtableCompanyType,
} from "../lib/company-profile-owner-operator-fields.js";

const COMPANY_PROFILE_TABLE_ID = "tblItyfH6MlOnMKZ9";
const COMPANY_ADDRESS_AIRTABLE_FIELD = "Company Address";

function getBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    return null;
  }
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
}

const NUMBER_OF_EMPLOYEES_FORM_TO_AIRTABLE = {
  Solo: "Solo / Independent",
  "2-10": "2–10 employees",
  "11-50": "11–50 employees",
  "51-200": "51–200 employees",
  "201-500": "201–500 employees",
  "501-1000": "501–1,000 employees",
  "1001-5000": "1,001–5,000 employees",
  "5001-10000": "5,001–10,000 employees",
  "10000+": "10,000+ employees",
};

const COUNTRY_CODE_TO_NAME = {
  US: "United States",
  CA: "Canada",
  GB: "United Kingdom",
  AU: "Australia",
  DE: "Germany",
  FR: "France",
  ES: "Spain",
  IT: "Italy",
  NL: "Netherlands",
  SG: "Singapore",
  JP: "Japan",
  CN: "China",
  IN: "India",
  AE: "United Arab Emirates",
  MX: "Mexico",
  BR: "Brazil",
  Other: "Other",
};

const PLATFORM_VISIBILITY_FORM_TO_AIRTABLE = {
  Public: "Public",
  "Matched Only": "Visible to Matched Users Only",
  Anonymous: "Anonymous / Hidden Profile",
  Custom: "Custom Group Visibility",
};

const OPEN_TO_CONTACT_FORM_TO_AIRTABLE = {
  Yes: "Yes",
  "Matched Only": "Only matched users",
  No: "No",
};

const NUMBER_OF_EMPLOYEES_AIRTABLE_TO_FORM = Object.fromEntries(
  Object.entries(NUMBER_OF_EMPLOYEES_FORM_TO_AIRTABLE).map(([formVal, airtableVal]) => [
    airtableVal,
    formVal,
  ])
);
const COUNTRY_NAME_TO_CODE = Object.fromEntries(
  Object.entries(COUNTRY_CODE_TO_NAME).map(([code, name]) => [name, code])
);
const PLATFORM_VISIBILITY_AIRTABLE_TO_FORM = Object.fromEntries(
  Object.entries(PLATFORM_VISIBILITY_FORM_TO_AIRTABLE).map(([formVal, airtableVal]) => [
    airtableVal,
    formVal,
  ])
);
const OPEN_TO_CONTACT_AIRTABLE_TO_FORM = Object.fromEntries(
  Object.entries(OPEN_TO_CONTACT_FORM_TO_AIRTABLE).map(([formVal, airtableVal]) => [
    airtableVal,
    formVal,
  ])
);
const LEGACY_PRIMARY_SERVICE_FIELDS = [
  "Primary Services Provided",
  "Primary Services",
  "Primary Service",
];
const LEGACY_ADDITIONAL_SERVICE_FIELDS = [
  "Additional Services Provided",
  "Additional Services",
  "Additional Service",
];
const BRAND_IDS_FIELDS = ["Brands You Operate / Support", "Brands You Operate/Support"];
const BRAND_NAMES_FIELDS = [
  "Brand Name (from Brands You Operate / Support)",
  "Brand Name from Brands You Operate / Support",
  "Brands You Operate / Support (Names)",
];
const COMPANY_LOGO_FIELDS = ["Logo", "Company Logo"];
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const BRAND_BASICS_NAME_FIELD = "Brand Name";

// —— Form primaryServices / additionalServices value → Airtable checkbox column name (suffix after "Primary - " or "Addl - ") ——
const SERVICE_FORM_VALUE_TO_COLUMN_SUFFIX = {
  "Franchise/Licensing": "Franchise / Licensing",
  "Brand Standards": "Brand Standards & Design Guidelines",
  "Brand Marketing": "Brand Marketing & Advertising",
  Loyalty: "Loyalty Program Participation",
  Distribution: "Distribution & CRS",
  "Revenue Mgmt": "Revenue Management Support",
  "Sales Support": "Sales Support (Global/Regional)",
  "Pre-Opening": "Pre-Opening Support",
  "Technical Services": "Technical Services / Plan Review",
  Procurement: "Procurement / FF&E Services",
  "Owner Onboarding": "Owner Onboarding & Orientation",
  "Asset Mgmt": "Asset Management",
  "Hotel Operations": "Hotel Operations (Day-to-Day)",
  "F&B": "Food & Beverage Operations",
  Staffing: "Staffing & Labor Planning",
  Takeover: "Hotel Takeover / Transition Planning",
  "Financial Reporting": "Financial Reporting & Controls",
  Feasibility: "Feasibility Studies",
  "Market Entry": "Market Entry Strategy",
  "Operator Search": "Operator Search / Brand Matching",
  "Deal Structuring": "Deal Structuring & Negotiation",
  "Owner Rep": "Owner Representation",
  "Lender Intro": "Lender/Investor Introductions",
  "Capital Raising": "Capital Raising / Investment Mgmt",
  "Legal Compliance": "Legal & Compliance Services",
  "Project Mgmt": "Project Management (Development)",
  "Design Oversight": "Design Oversight / Brand Compliance",
};
const SERVICE_SUFFIX_TO_FORM_VALUE = Object.fromEntries(
  Object.entries(SERVICE_FORM_VALUE_TO_COLUMN_SUFFIX).map(([formVal, suffix]) => [
    suffix,
    formVal,
  ])
);

const REGION_CHECKBOX_COLUMNS = [
  "Americas",
  "Caribbean & Latin America",
  "Europe",
  "Middle East & Africa",
  "Asia Pacific",
];

function toStr(v) {
  return v == null ? "" : String(v).trim();
}

function toArray(v) {
  if (Array.isArray(v)) return v;
  if (v == null) return [];
  if (typeof v === "string") {
    const raw = v.trim();
    if (!raw) return [];
    if (raw.startsWith("[") && raw.endsWith("]")) {
      try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
      } catch (_) {}
    }
    return raw
      .split(/[\n,;|]+/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [v];
}

function normalizeServiceItemToFormValue(item) {
  const raw = toStr(item);
  if (!raw) return "";
  if (SERVICE_FORM_VALUE_TO_COLUMN_SUFFIX[raw]) return raw;
  if (SERVICE_SUFFIX_TO_FORM_VALUE[raw]) return SERVICE_SUFFIX_TO_FORM_VALUE[raw];

  const lowerRaw = raw.toLowerCase();
  for (const [formVal, suffix] of Object.entries(SERVICE_FORM_VALUE_TO_COLUMN_SUFFIX)) {
    if (suffix.toLowerCase() === lowerRaw) return formVal;
    if (formVal.toLowerCase() === lowerRaw) return formVal;
  }
  return "";
}

function pushUnique(target, values) {
  const existing = new Set(target);
  for (const v of values) {
    if (!v || existing.has(v)) continue;
    target.push(v);
    existing.add(v);
  }
}

function extractAttachmentMeta(fields) {
  for (const key of COMPANY_LOGO_FIELDS) {
    const val = fields[key];
    if (!Array.isArray(val) || val.length === 0) continue;
    const first = val[0];
    if (!first || typeof first !== "object") continue;
    return {
      companyLogoUrl: toStr(first.url),
      companyLogoFilename: toStr(first.filename),
    };
  }
  return { companyLogoUrl: "", companyLogoFilename: "" };
}

function buildEmptyPrefill() {
  return {
    companyName: "",
    companyType: "",
    companyWebsite: "",
    numberOfEmployees: "",
    companyHQCountry: "",
    yearFounded: "",
    companyOverview: "",
    additionalOfficeRegions: "",
    companyAddress: "",
    regions: [],
    brandsOperateSupport: [],
    primaryServices: [],
    additionalServices: [],
    jurisdictionsLicensed: "",
    companyRole: "",
    companyRoleLabel: "",
    platformVisibility: "",
    openToContact: "",
    companyLogoUrl: "",
    companyLogoFilename: "",
    brandsOperateSupportNames: [],
    companyCapabilities: [],
    companyTypeTags: [],
    workspaceAccess: [],
    operatingModel: "",
    thirdPartyManagementAvailability: "",
    coreProfileStatus: "",
    ownerProfileStatus: "",
    operatorProfileStatus: "",
    developerProfileStatus: "",
    potentialConflictFlags: [],
    competitiveSensitivityNotes: "",
    companyTypeDisplay: "",
  };
}

function airtableFieldsToPrefill(fields) {
  const prefill = buildEmptyPrefill();
  const f = fields || {};

  prefill.companyName = toStr(f["Company Name"]);
  const companyTypeRaw = toStr(f["Company Type"]);
  prefill.companyType = airtableCompanyTypeToFormKey(companyTypeRaw) || companyTypeRaw;
  prefill.companyTypeDisplay = companyTypeDisplayLabel(prefill.companyType || companyTypeRaw);
  Object.assign(prefill, airtableFieldsToOwnerOperatorPrefill(f));
  prefill.companyWebsite = stripLeadingWwwFromWebsiteUrl(toStr(f["Company Website"]));
  prefill.numberOfEmployees =
    NUMBER_OF_EMPLOYEES_AIRTABLE_TO_FORM[toStr(f["Number of Employees"])] ||
    toStr(f["Number of Employees"]);
  prefill.companyHQCountry =
    COUNTRY_NAME_TO_CODE[toStr(f["Company HQ Country"])] || toStr(f["Company HQ Country"]);
  prefill.yearFounded = toStr(f["Year Founded"]);
  prefill.companyOverview = toStr(f["Company Overview"]);
  prefill.additionalOfficeRegions = toStr(f["Additional Office Regions"]);
  prefill.companyAddress =
    toStr(f[COMPANY_ADDRESS_AIRTABLE_FIELD]) || toStr(f["Property Address"]);
  prefill.jurisdictionsLicensed = toStr(f["Jurisdictions Licensed"]);
  const companyRoleRaw = toStr(f[COMPANY_ROLE_AIRTABLE_FIELD]);
  prefill.companyRole = normalizeCompanyRoleToForm(companyRoleRaw);
  prefill.companyRoleLabel = companyRoleDisplayLabel(companyRoleRaw, prefill.companyRole);
  prefill.platformVisibility =
    PLATFORM_VISIBILITY_AIRTABLE_TO_FORM[toStr(f["Company Platform Visibility"])] ||
    toStr(f["Company Platform Visibility"]);
  prefill.openToContact =
    OPEN_TO_CONTACT_AIRTABLE_TO_FORM[toStr(f["Open to Contact"])] ||
    toStr(f["Open to Contact"]);

  prefill.regions = REGION_CHECKBOX_COLUMNS.filter((col) => !!f[col]);

  for (const [formVal, suffix] of Object.entries(SERVICE_FORM_VALUE_TO_COLUMN_SUFFIX)) {
    if (f[`Primary - ${suffix}`]) prefill.primaryServices.push(formVal);
    if (f[`Addl - ${suffix}`]) prefill.additionalServices.push(formVal);
  }

  if (prefill.primaryServices.length === 0) {
    for (const key of LEGACY_PRIMARY_SERVICE_FIELDS) {
      const normalized = toArray(f[key])
        .map(normalizeServiceItemToFormValue)
        .filter(Boolean);
      pushUnique(prefill.primaryServices, normalized);
    }
  }
  if (prefill.additionalServices.length === 0) {
    for (const key of LEGACY_ADDITIONAL_SERVICE_FIELDS) {
      const normalized = toArray(f[key])
        .map(normalizeServiceItemToFormValue)
        .filter(Boolean);
      pushUnique(prefill.additionalServices, normalized);
    }
  }

  for (const key of BRAND_IDS_FIELDS) {
    const linked = toArray(f[key]);
    const ids = linked
      .map((item) => (typeof item === "string" ? item : item && item.id))
      .map((id) => toStr(id))
      .filter((id) => id.startsWith("rec"));
    pushUnique(prefill.brandsOperateSupport, ids);
  }
  for (const key of BRAND_NAMES_FIELDS) {
    const rawItems = toArray(f[key]).map(toStr).filter(Boolean);
    const ids = rawItems.filter((v) => v.startsWith("rec"));
    const names = rawItems.filter((v) => !v.startsWith("rec"));
    pushUnique(prefill.brandsOperateSupport, ids);
    pushUnique(prefill.brandsOperateSupportNames, names);
  }

  const logoMeta = extractAttachmentMeta(f);
  prefill.companyLogoUrl = logoMeta.companyLogoUrl;
  prefill.companyLogoFilename = logoMeta.companyLogoFilename;

  return prefill;
}

function escapeAirtableFormulaString(input) {
  return String(input || "")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'");
}

function extractUnknownFieldName(error) {
  const msg = toStr(error && error.message);
  const match = msg.match(/Unknown field name:\s*"([^"]+)"/i);
  return match ? match[1] : "";
}

function prepareCompanyProfileFieldsForAirtableWrite(fields) {
  const working = { ...(fields || {}) };
  finalizeCompanyProfileFieldsForAirtableWrite(working, { loud: true });
  if (process.env.NODE_ENV !== "production" && working["Company Type"]) {
    console.log(
      "[company-profile] Airtable write Company Type:",
      working["Company Type"]
    );
  }
  return working;
}

async function createWithUnknownFieldFallback(base, fields) {
  const working = prepareCompanyProfileFieldsForAirtableWrite(fields);
  const removed = [];
  const maxRetries = Math.max(50, Object.keys(working).length + 10);
  let attempts = 0;

  while (attempts <= maxRetries) {
    try {
      finalizeCompanyProfileFieldsForAirtableWrite(working, { loud: true });
      const record = await base(COMPANY_PROFILE_TABLE_ID).create(working, {
        typecast: false,
      });
      if (removed.length) {
        console.warn(
          "Company profile create: ignored unknown Airtable fields:",
          removed.join(", ")
        );
      }
      return record;
    } catch (err) {
      const unknown = extractUnknownFieldName(err);
      if (!unknown || !Object.prototype.hasOwnProperty.call(working, unknown)) {
        throw err;
      }
      delete working[unknown];
      removed.push(unknown);
      attempts += 1;
      if (Object.keys(working).length === 0) {
        throw err;
      }
    }
  }

  throw new Error(
    "Exceeded retries while removing unknown Airtable fields (create): " +
      removed.join(", ")
  );
}

async function updateWithUnknownFieldFallback(base, recordId, fields) {
  const working = prepareCompanyProfileFieldsForAirtableWrite(fields);
  const removed = [];
  const maxRetries = Math.max(50, Object.keys(working).length + 10);
  let attempts = 0;

  while (attempts <= maxRetries) {
    try {
      finalizeCompanyProfileFieldsForAirtableWrite(working, { loud: true });
      const record = await base(COMPANY_PROFILE_TABLE_ID).update(recordId, working, {
        typecast: false,
      });
      if (removed.length) {
        console.warn(
          "Company profile update: ignored unknown Airtable fields:",
          removed.join(", ")
        );
      }
      return record;
    } catch (err) {
      const unknown = extractUnknownFieldName(err);
      if (!unknown || !Object.prototype.hasOwnProperty.call(working, unknown)) {
        throw err;
      }
      delete working[unknown];
      removed.push(unknown);
      attempts += 1;
      if (Object.keys(working).length === 0) {
        throw err;
      }
    }
  }

  throw new Error(
    "Exceeded retries while removing unknown Airtable fields (update): " +
      removed.join(", ")
  );
}

async function resolveBrandIdsByName(base, names) {
  const out = [];
  const seen = new Set();
  for (const rawName of names || []) {
    const name = toStr(rawName);
    if (!name) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    try {
      const formula = `LOWER({${BRAND_BASICS_NAME_FIELD}})='${escapeAirtableFormulaString(
        key
      )}'`;
      const rows = await base(BRAND_BASICS_TABLE)
        .select({ maxRecords: 1, filterByFormula: formula })
        .firstPage();
      if (rows && rows[0] && rows[0].id) out.push(rows[0].id);
    } catch (_) {
      // ignore lookup errors; keep fallback names in response
    }
  }
  return out;
}

/**
 * Build Airtable fields object from Company Settings form body.
 * @param {Record<string, any>} body - Form data (e.g. from req.body or FormData)
 * @returns {Record<string, any>} - Fields to send to Airtable create/update
 */
export function formToAirtableFields(body) {
  body = body || {};
  const fields = {};

  // —— Simple 1:1 (form name → Airtable column name) ——
  if (body.companyName != null && body.companyName !== "")
    fields["Company Name"] = String(body.companyName).trim();
  if (body.companyWebsite != null && body.companyWebsite !== "")
    fields["Company Website"] = stripLeadingWwwFromWebsiteUrl(String(body.companyWebsite).trim());
  if (body.companyOverview != null && body.companyOverview !== "")
    fields["Company Overview"] = String(body.companyOverview).trim();
  if (body.additionalOfficeRegions != null && body.additionalOfficeRegions !== "")
    fields["Additional Office Regions"] = String(body.additionalOfficeRegions).trim();
  const companyAddress =
    body.companyAddress != null && body.companyAddress !== ""
      ? body.companyAddress
      : body.propertyAddress;
  if (companyAddress != null && companyAddress !== "")
    fields[COMPANY_ADDRESS_AIRTABLE_FIELD] = String(companyAddress).trim();
  if (body.jurisdictionsLicensed != null && body.jurisdictionsLicensed !== "")
    fields["Jurisdictions Licensed"] = String(body.jurisdictionsLicensed).trim();

  // Year Founded – Airtable is singleLineText
  if (body.yearFounded != null && body.yearFounded !== "")
    fields["Year Founded"] = String(body.yearFounded).trim();

  // —— Single select with value mapping (may be overridden by capability derivation) ——
  const companyTypeInput =
    pickFirstCompanyTypeInput(body.derivedCompanyType) ||
    pickFirstCompanyTypeInput(body.companyTypeKey) ||
    pickFirstCompanyTypeInput(body.companyType);
  const companyTypeAirtable = toAirtableCompanyType(companyTypeInput);
  if (companyTypeAirtable) fields["Company Type"] = companyTypeAirtable;
  if (body.numberOfEmployees != null && body.numberOfEmployees !== "") {
    const mapped =
      NUMBER_OF_EMPLOYEES_FORM_TO_AIRTABLE[body.numberOfEmployees] ??
      body.numberOfEmployees;
    fields["Number of Employees"] = mapped;
  }
  if (body.companyHQCountry != null && body.companyHQCountry !== "") {
    const mapped =
      COUNTRY_CODE_TO_NAME[body.companyHQCountry] ?? body.companyHQCountry;
    fields["Company HQ Country"] = mapped;
  }
  if (body.companyRole != null && body.companyRole !== "") {
    const mapped =
      COMPANY_ROLE_FORM_TO_AIRTABLE[body.companyRole] ?? body.companyRole;
    fields[COMPANY_ROLE_AIRTABLE_FIELD] = mapped;
  }
  if (body.platformVisibility != null && body.platformVisibility !== "") {
    const mapped =
      PLATFORM_VISIBILITY_FORM_TO_AIRTABLE[body.platformVisibility] ??
      body.platformVisibility;
    fields["Company Platform Visibility"] = mapped;
  }
  if (body.openToContact != null && body.openToContact !== "") {
    const mapped =
      OPEN_TO_CONTACT_FORM_TO_AIRTABLE[body.openToContact] ?? body.openToContact;
    fields["Open to Contact"] = mapped;
  }

  // —— Regions: form sends regions[] or comma-separated; Airtable has 5 checkboxes ——
  const regionsList = Array.isArray(body.regions)
    ? body.regions
    : typeof body.regions === "string"
      ? body.regions.split(",").map((s) => s.trim()).filter(Boolean)
      : [];
  for (const col of REGION_CHECKBOX_COLUMNS) {
    fields[col] = regionsList.includes(col);
  }

  // —— Primary / additional services: checkbox columns (legacy) + multi-select fields (current) ——
  const primaryList = Array.isArray(body.primaryServices)
    ? body.primaryServices
    : typeof body.primaryServices === "string"
      ? body.primaryServices.split(",").map((s) => s.trim()).filter(Boolean)
      : [];
  const addlList = Array.isArray(body.additionalServices)
    ? body.additionalServices
    : typeof body.additionalServices === "string"
      ? body.additionalServices.split(",").map((s) => s.trim()).filter(Boolean)
      : [];

  const primaryAirtableValues = [];
  const addlAirtableValues = [];
  for (const [formVal, suffix] of Object.entries(SERVICE_FORM_VALUE_TO_COLUMN_SUFFIX)) {
    fields[`Primary - ${suffix}`] = primaryList.includes(formVal);
    fields[`Addl - ${suffix}`] = addlList.includes(formVal);
    if (primaryList.includes(formVal)) primaryAirtableValues.push(suffix);
    if (addlList.includes(formVal)) addlAirtableValues.push(suffix);
  }
  if (primaryAirtableValues.length > 0) {
    fields["Primary Services"] = primaryAirtableValues;
  }
  if (addlAirtableValues.length > 0) {
    fields["Additional Services"] = addlAirtableValues;
  }

  // —— Brands You Operate / Support: linked records (record IDs) ——
  if (body.brandsOperateSupport != null && body.brandsOperateSupport !== "") {
    const ids = String(body.brandsOperateSupport)
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.startsWith("rec"));
    if (ids.length > 0) fields["Brands You Operate / Support"] = ids;
  }

  const prefillCtx = {
    prefill: {
      thirdPartyManagementAvailability: toStr(body.existingThirdPartyManagementAvailability),
      operatingModel: toStr(body.existingOperatingModel),
      coreProfileStatus: toStr(body.existingCoreProfileStatus),
      ownerProfileStatus: toStr(body.existingOwnerProfileStatus),
      operatorProfileStatus: toStr(body.existingOperatorProfileStatus),
      developerProfileStatus: toStr(body.existingDeveloperProfileStatus),
    },
  };
  const { warnings } = mergeOwnerOperatorExtensionFields(body, fields, prefillCtx);
  if (warnings.length && process.env.NODE_ENV !== "production") {
    console.warn("Company profile: owner-operator field warnings:", warnings.join(", "));
  }

  finalizeCompanyProfileFieldsForAirtableWrite(fields, { loud: false });

  // Logo is set in createCompanyProfile from req.file (multipart upload)
  return fields;
}

/**
 * POST /api/company-profile – create a new Company Profile record.
 * Body: form fields (JSON or form-urlencoded).
 */
export async function createCompanyProfile(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(503).json({
        error: "Airtable not configured (AIRTABLE_API_KEY / AIRTABLE_BASE_ID)",
      });
    }

    const bodyKeys = req.body ? Object.keys(req.body) : [];
    if (bodyKeys.length === 0) {
      console.warn("Company profile: req.body is empty (multipart form fields may not have been parsed)");
    } else {
      console.log("Company profile: req.body has", bodyKeys.length, "fields");
    }

    const fields = formToAirtableFields(req.body);

    // Logo: if file was uploaded in same request, add its URL to Airtable (URL must be publicly reachable for Airtable to fetch it)
    if (req.file && req.file.filename) {
      const baseUrl =
        process.env.PUBLIC_URL ||
        (req.protocol && req.get && `${req.protocol}://${req.get("host")}`) ||
        "http://localhost:3000";
      const logoUrl = `${baseUrl.replace(/\/$/, "")}/uploads/${req.file.filename}`;
      fields["Logo"] = [{ url: logoUrl, filename: req.file.originalname || req.file.filename }];
      console.log("Company profile: logo set, url =", logoUrl);
      if (logoUrl.includes("localhost")) {
        console.warn(
          "Company profile: logo URL is localhost — Airtable cannot fetch it from the internet. Set PUBLIC_URL in .env to a public URL (e.g. ngrok or your deployed app) for the logo to appear in Airtable."
        );
      }
    } else {
      console.log("Company profile: no file in request (req.file missing)");
    }

    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ error: "No valid fields to save" });
    }

    const record = await createWithUnknownFieldFallback(base, fields);

    console.log(
      "Company profile created in Airtable:",
      record.id,
      "Base:",
      process.env.AIRTABLE_BASE_ID,
      "fields count:",
      Object.keys(fields).length
    );
    return res.status(201).json({
      id: record.id,
      message: "Company profile created",
      warnings: [],
    });
  } catch (err) {
    console.error("Company profile create error:", err);
    const status = err.statusCode ?? 500;
    return res.status(status).json({
      error: err.message || "Failed to create company profile",
    });
  }
}

/**
 * PATCH /api/company-profile/:recordId – update an existing Company Profile record.
 * Body: form fields (JSON or form-urlencoded).
 */
export async function updateCompanyProfile(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(503).json({
        error: "Airtable not configured (AIRTABLE_API_KEY / AIRTABLE_BASE_ID)",
      });
    }

    const { recordId } = req.params;
    if (!recordId) {
      return res.status(400).json({ error: "Missing recordId" });
    }

    const fields = formToAirtableFields(req.body);

    // Logo update: accept multipart PATCH with optional new companyLogo file.
    if (req.file && req.file.filename) {
      const baseUrl =
        process.env.PUBLIC_URL ||
        (req.protocol && req.get && `${req.protocol}://${req.get("host")}`) ||
        "http://localhost:3000";
      const logoUrl = `${baseUrl.replace(/\/$/, "")}/uploads/${req.file.filename}`;
      fields["Logo"] = [{ url: logoUrl, filename: req.file.originalname || req.file.filename }];
      console.log("Company profile update: logo set, url =", logoUrl);
    }
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ error: "No valid fields to update" });
    }

    const record = await updateWithUnknownFieldFallback(base, recordId, fields);

    return res.json({
      id: record.id,
      message: "Company profile updated",
      warnings: [],
    });
  } catch (err) {
    console.error("Company profile update error:", err);
    const status = err.statusCode ?? 500;
    return res.status(status).json({
      error: err.message || "Failed to update company profile",
    });
  }
}

async function findCompanyProfileRecord(base, { recordId, companyName }) {
  if (recordId) {
    return base(COMPANY_PROFILE_TABLE_ID).find(recordId);
  }
  if (companyName) {
    const formula = `LOWER({Company Name})='${escapeAirtableFormulaString(
      companyName.toLowerCase()
    )}'`;
    const rows = await base(COMPANY_PROFILE_TABLE_ID)
      .select({
        maxRecords: 1,
        filterByFormula: formula,
      })
      .firstPage();
    return rows && rows.length ? rows[0] : null;
  }
  return null;
}

async function buildCompanyProfilePrefillPayload(base, { recordId, companyName }) {
  if (!recordId && !companyName) {
    return {
      success: true,
      recordId: null,
      source: "none",
      prefill: buildEmptyPrefill(),
    };
  }

  const record = await findCompanyProfileRecord(base, { recordId, companyName });
  if (!record) {
    return {
      success: true,
      recordId: null,
      source: "airtable",
      prefill: buildEmptyPrefill(),
    };
  }

  const prefill = airtableFieldsToPrefill(record.fields || {});
  if (prefill.brandsOperateSupport.length === 0 && prefill.brandsOperateSupportNames.length) {
    const resolved = await resolveBrandIdsByName(base, prefill.brandsOperateSupportNames);
    pushUnique(prefill.brandsOperateSupport, resolved);
  }

  return {
    success: true,
    recordId: record.id,
    source: "airtable",
    prefill,
  };
}

/**
 * GET /api/company-profile/prefill – return normalized prefill payload.
 * Query params:
 * - recordId: Airtable record id (preferred)
 * - companyName: fallback lookup by Company Name
 */
export async function getCompanyProfilePrefill(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(503).json({
        success: false,
        error: "Airtable not configured (AIRTABLE_API_KEY / AIRTABLE_BASE_ID)",
      });
    }

    const payload = await buildCompanyProfilePrefillPayload(base, {
      recordId: toStr(req.query.recordId),
      companyName: toStr(req.query.companyName),
    });
    return res.json(payload);
  } catch (err) {
    console.error("Company profile prefill error:", err);
    const status = err.statusCode === 404 ? 404 : err.statusCode ?? 500;
    return res.status(status).json({
      success: false,
      error: err.message || "Failed to load company profile prefill",
    });
  }
}

/**
 * GET /api/company-profile/mine – prefill for the signed-in user's linked Company Profile.
 * Requires Memberstack JWT + Users row (see requireDealalityUser).
 */
export async function getMyCompanyProfilePrefill(req, res) {
  try {
    const base = getBase();
    if (!base) {
      return res.status(503).json({
        success: false,
        error: "Airtable not configured (AIRTABLE_API_KEY / AIRTABLE_BASE_ID)",
      });
    }

    const user = req.dealalityUser || {};
    const recordId = toStr(user.companyId);
    const companyName = recordId ? "" : toStr(user.companyName);

    if (!recordId && !companyName) {
      return res.json({
        success: true,
        recordId: null,
        source: "none",
        prefill: buildEmptyPrefill(),
        hint: "no_company_linked",
      });
    }

    const payload = await buildCompanyProfilePrefillPayload(base, { recordId, companyName });
    return res.json(payload);
  } catch (err) {
    console.error("Company profile mine prefill error:", err);
    const status = err.statusCode === 404 ? 404 : err.statusCode ?? 500;
    return res.status(status).json({
      success: false,
      error: err.message || "Failed to load your company profile",
    });
  }
}
