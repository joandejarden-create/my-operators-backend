import Airtable from "airtable";

import { stripLeadingWwwFromWebsiteUrl } from "./lib/strip-www-from-website-url.js";
import { companyTypeFromProfileFields } from "../lib/company-type-normalize.js";
import {
  COMPANY_ROLE_AIRTABLE_FIELD,
  companyRoleFromEcosystemField,
} from "../lib/company-role-normalize.js";
import {
  PLATFORM_USERS_TABLE_ID,
  PUF,
  contactVisibilityFromFields,
  isContactVisibleInPartnerDirectory,
} from "../lib/airtable/platform-users-table.js";

// Set PARTNER_DIRECTORY_DEBUG=true in .env to enable verbose logs (e.g. for debugging Airtable field mapping).
const DEBUG = process.env.PARTNER_DIRECTORY_DEBUG === 'true';

/** Individuals tab reads platform Users table (consolidated from legacy User Management). */
const PLATFORM_USERS_TABLE = PLATFORM_USERS_TABLE_ID;

// Lazy initialization of Airtable base
function getBase() {
    if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
        return null;
    }
    return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

// Helper function to generate default description
function generateDefaultDescription(companyName, userType) {
  const company = companyName || 'Company';
  let typeText = '';
  
  if (userType === 'HOTEL BRANDS (FRANCHISE)') {
    typeText = 'hotel brand franchise';
  } else if (userType === 'HOTEL MGMT. COMPANY') {
    typeText = 'hotel management company';
  } else if (userType === 'HOTEL OWNERS') {
    typeText = 'hotel owner';
  } else {
    typeText = 'hospitality company';
  }
  
  return `${company} is a ${typeText}.`;
}

/** Normalize to Partner Directory filter buckets (owners / brands / mgmt). */
function normalizePartnerDirectoryType(type) {
  if (!type) return "";
  const upperType = String(type).trim().toUpperCase();

  if (
    upperType === "HOTEL OWNERS" ||
    upperType === "HOTEL OWNER" ||
    upperType === "OWNER" ||
    upperType === "OWNERS"
  ) {
    return "HOTEL OWNERS";
  }
  if (
    upperType === "HOTEL BRANDS (FRANCHISE)" ||
    upperType === "HOTEL BRAND" ||
    upperType === "HOTEL BRANDS" ||
    upperType === "BRAND" ||
    upperType === "BRANDS" ||
    upperType === "FRANCHISE"
  ) {
    return "HOTEL BRANDS (FRANCHISE)";
  }
  if (
    upperType === "HOTEL MGMT. COMPANY" ||
    upperType === "HOTEL MGMT COMPANY" ||
    upperType === "HOTEL MANAGEMENT COMPANY" ||
    upperType === "MGMT" ||
    upperType === "MANAGEMENT" ||
    upperType === "OPERATOR" ||
    upperType === "3RD PARTY OPERATOR"
  ) {
    return "HOTEL MGMT. COMPANY";
  }
  if (upperType.includes("BRAND") || upperType.includes("FRANCHISE")) {
    return "HOTEL BRANDS (FRANCHISE)";
  }
  if (
    upperType.includes("MGMT") ||
    upperType.includes("MANAGEMENT") ||
    upperType.includes("OPERATOR")
  ) {
    return "HOTEL MGMT. COMPANY";
  }
  if (upperType.includes("OWNER")) {
    return "HOTEL OWNERS";
  }
  return "";
}

function isKnownPartnerDirectoryType(normalized) {
  return (
    normalized === "HOTEL OWNERS" ||
    normalized === "HOTEL BRANDS (FRANCHISE)" ||
    normalized === "HOTEL MGMT. COMPANY"
  );
}

/** User row type, else linked company's Company Type / User Type (not Platform Role job title). */
function resolveDirectoryPartnerType(personType, company) {
  const fromPerson = normalizePartnerDirectoryType(personType);
  if (isKnownPartnerDirectoryType(fromPerson)) return fromPerson;
  if (company) {
    const fromCompany = normalizePartnerDirectoryType(
      company.userType || company.companyType || ""
    );
    if (isKnownPartnerDirectoryType(fromCompany)) return fromCompany;
  }
  return "";
}

function isRecordId(value) {
  return typeof value === "string" && /^rec[a-zA-Z0-9]{5,}$/.test(value.trim());
}

function extractRecordIdsFromValue(value, out) {
  if (value == null) return;
  if (Array.isArray(value)) {
    value.forEach((item) => extractRecordIdsFromValue(item, out));
    return;
  }
  if (typeof value === "string") {
    if (isRecordId(value)) out.push(value.trim());
    return;
  }
  if (typeof value === "object") {
    if (isRecordId(value.id)) out.push(String(value.id).trim());
    if (isRecordId(value.recordId)) out.push(String(value.recordId).trim());
  }
}

function uniqueStrings(values) {
  return [...new Set((values || []).filter(Boolean).map((v) => String(v).trim()).filter(Boolean))];
}

function parseAttachmentUrl(value) {
  if (!value) return null;
  if (Array.isArray(value) && value[0]) {
    if (typeof value[0] === "string") return value[0];
    if (value[0] && typeof value[0] === "object" && value[0].url) return String(value[0].url);
  }
  if (typeof value === "string") return value;
  if (typeof value === "object" && value.url) return String(value.url);
  return null;
}

function buildCompanyEnrichment(fields, recordCreatedTime) {
  const safeFields = fields && typeof fields === "object" ? { ...fields } : {};
  if (!safeFields["Created Date"] && recordCreatedTime) {
    safeFields["Created Date"] = recordCreatedTime;
  }

  const brandRecordIds = [];
  const userManagementRecordIds = [];
  const services = [];
  const primaryServices = [];
  const brands = [];

  Object.entries(safeFields).forEach(([key, value]) => {
    const lower = String(key || "").toLowerCase();

    // Linked brand references (record IDs and direct string brand names).
    if (lower.includes("brand")) {
      extractRecordIdsFromValue(value, brandRecordIds);
      if (Array.isArray(value)) {
        value.forEach((item) => {
          if (typeof item === "string" && !isRecordId(item)) brands.push(item);
          else if (item && typeof item === "object") {
            const name = item.name || item.label || item.title || item.fields?.["Brand Name"] || item.fields?.name;
            if (name) brands.push(String(name));
          }
        });
      } else if (typeof value === "string" && !isRecordId(value)) {
        value.split(",").map((x) => x.trim()).filter(Boolean).forEach((x) => brands.push(x));
      }
    }

    // Linked team members (Users table — prefer Team Members over legacy User Management).
    if (
      lower === "team members" ||
      lower.includes("team members") ||
      lower.includes("user management") ||
      lower.includes("user_management") ||
      lower.includes("company users") ||
      (lower.includes("team") && (lower.includes("member") || lower.includes("users")))
    ) {
      extractRecordIdsFromValue(value, userManagementRecordIds);
    }

    // Service checkboxes and service list fields.
    if (lower.includes("service")) {
      if (typeof value === "boolean" && value) {
        const cleaned = String(key)
          .replace(/\s*[\[(]?\s*primary\s*[\])]?/gi, "")
          .replace(/\s*services?\s*/gi, "")
          .trim();
        const serviceName = cleaned || String(key).trim();
        services.push(serviceName);
        if (/primary/i.test(key)) primaryServices.push(serviceName);
      } else if (typeof value === "string" && value.trim()) {
        value.split(",").map((x) => x.trim()).filter(Boolean).forEach((x) => services.push(x));
        if (/primary/i.test(key)) {
          value.split(",").map((x) => x.trim()).filter(Boolean).forEach((x) => primaryServices.push(x));
        }
      } else if (Array.isArray(value)) {
        value
          .map((x) => (typeof x === "string" ? x.trim() : ""))
          .filter(Boolean)
          .forEach((x) => services.push(x));
        if (/primary/i.test(key)) {
          value
            .map((x) => (typeof x === "string" ? x.trim() : ""))
            .filter(Boolean)
            .forEach((x) => primaryServices.push(x));
        }
      }
    }
  });

  return {
    rawFields: safeFields,
    brandRecordIds: uniqueStrings(brandRecordIds),
    userManagementRecordIds: uniqueStrings(userManagementRecordIds),
    services: uniqueStrings(services),
    primaryServices: uniqueStrings(primaryServices),
    brands: uniqueStrings(brands)
  };
}

function parseLinkedCompanyInfo(fields) {
  const candidates = [
    fields["Company Profile"],
    fields["Company Name"],
    fields["Company/Organization"],
    fields["Company"],
    fields["Organization"],
    fields["Company Record ID"],
    fields["Linked Company"]
  ];

  let companyName = "";
  let companyRecordId = "";

  const visit = (value) => {
    if (value == null) return;
    if (Array.isArray(value)) {
      value.forEach(visit);
      return;
    }
    if (typeof value === "string") {
      const v = value.trim();
      if (!v) return;
      if (isRecordId(v)) {
        if (!companyRecordId) companyRecordId = v;
      } else if (!companyName) {
        companyName = v;
      }
      return;
    }
    if (typeof value === "object") {
      if (isRecordId(value.id) && !companyRecordId) companyRecordId = String(value.id).trim();
      const nestedName =
        value.fields?.["Company Name"] ||
        value.fields?.["Name"] ||
        value.name ||
        value.label ||
        value.title ||
        "";
      if (nestedName && !companyName) companyName = String(nestedName).trim();
    }
  };

  candidates.forEach(visit);
  return { companyName, companyRecordId };
}

async function buildBrandNameMap(base) {
  const map = new Map();
  const brandRecords = [];
  await new Promise((resolve, reject) => {
    base(F.brands.table)
      .select({})
      .eachPage(
        (pageRecords, fetchNextPage) => {
          brandRecords.push(...pageRecords);
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });
  brandRecords.forEach((record) => {
    const fields = record.fields || {};
    const name = fields[F.brands.name] || fields["Brand Name"] || fields["Name"] || "";
    if (record.id && name) map.set(record.id, String(name).trim());
  });
  return map;
}

function attachResolvedBrandNames(records, brandNameMap) {
  if (!Array.isArray(records) || !brandNameMap) return records || [];
  return records.map((record) => {
    const existing = Array.isArray(record.brands) ? record.brands.filter(Boolean) : [];
    const resolved = (record.brandRecordIds || [])
      .map((id) => brandNameMap.get(id))
      .filter(Boolean);
    const merged = uniqueStrings([...existing, ...resolved]);
    return { ...record, brands: merged };
  });
}

/** User rows rarely have brand links; inherit from linked Company Profile. */
function mergeLinkedCompanyIntoIndividuals(individuals, companies) {
  if (!Array.isArray(individuals) || !Array.isArray(companies)) return individuals || [];

  const companyById = new Map();
  const companyByName = new Map();
  companies.forEach((company) => {
    if (company?.id) companyById.set(company.id, company);
    const name = (company?.name || "").trim().toLowerCase();
    if (name && !companyByName.has(name)) companyByName.set(name, company);
  });

  return individuals.map((individual) => {
    let company = null;
    const recordId = (individual.companyRecordId || "").trim();
    if (recordId) company = companyById.get(recordId) || null;
    if (!company) {
      const name = (individual.companyName || "").trim().toLowerCase();
      if (name) company = companyByName.get(name) || null;
    }
    if (!company) return individual;

    const personBrands = Array.isArray(individual.brands) ? individual.brands : [];
    const companyBrands = Array.isArray(company.brands) ? company.brands : [];
    const mergedBrands = uniqueStrings([...personBrands, ...companyBrands]);

    const personRegions = Array.isArray(individual.regions) ? individual.regions.filter(Boolean) : [];
    const companyRegions = Array.isArray(company.regions) ? company.regions.filter(Boolean) : [];

    // Stats stay on the person row only — do not roll up company deal/brand counts for cards.

    const companyName =
      (individual.companyName || "").trim() || (company.name || "").trim();

    const personCoverage = Array.isArray(individual.coverageTerritories)
      ? individual.coverageTerritories.filter(Boolean)
      : [];
    const personLanguages = Array.isArray(individual.languages)
      ? individual.languages.filter(Boolean)
      : [];

    const directoryUserType = resolveDirectoryPartnerType(individual.userType, company);
    const personDirectoryType = normalizePartnerDirectoryType(individual.userType);

    return {
      ...individual,
      companyName,
      userType: directoryUserType,
      userTypeViaCompany:
        !isKnownPartnerDirectoryType(personDirectoryType) &&
        isKnownPartnerDirectoryType(directoryUserType),
      brands: mergedBrands,
      brandRecordIds: uniqueStrings([
        ...(individual.brandRecordIds || []),
        ...(company.brandRecordIds || [])
      ]),
      regions: personRegions,
      companyRegions,
      coverageTerritories: personCoverage,
      languages: personLanguages,
      brandsViaCompany: personBrands.length === 0 && companyBrands.length > 0,
      regionsViaCompany: personRegions.length === 0 && companyRegions.length > 0
    };
  });
}

const USER_REGION_CHECKBOX_FIELDS = [
  "Region - America",
  "Region - Caribbean & Latin America",
  "Region - Europe",
  "Region - Middle East & Africa",
  "Region - Asia Pacific",
];

const USER_REGION_CHECKBOX_TO_CODE = {
  "Region - America": "AMERICAS",
  "Region - Caribbean & Latin America": "CALA",
  "Region - Europe": "EUROPE",
  "Region - Middle East & Africa": "MEA",
  "Region - Asia Pacific": "AP",
};

const ALL_USER_REGION_CODES = new Set(["AMERICAS", "CALA", "EUROPE", "MEA", "AP"]);

function normalizeRegionToken(value) {
  const u = (typeof value === "string" ? value : String(value || "")).trim().replace(/\s+/g, " ").toUpperCase();
  if (!u) return null;
  if (u.indexOf("GLOBAL") >= 0) return "GLOBAL";
  if (u.indexOf("CARIBBEAN") >= 0 || u.indexOf("LATIN") >= 0 || u === "CALA") return "CALA";
  if (u.indexOf("EUROPE") >= 0 || u === "EU") return "EUROPE";
  if ((u.indexOf("MIDDLE") >= 0 && u.indexOf("EAST") >= 0) || u.indexOf("MEA") >= 0) return "MEA";
  if ((u.indexOf("ASIA") >= 0 && u.indexOf("PACIFIC") >= 0) || u === "AP") return "AP";
  if (u.indexOf("AMERICAS") >= 0 || (u.indexOf("AMERICA") >= 0 && u.indexOf("LATIN") < 0 && u.indexOf("CARIBBEAN") < 0)) {
    return "AMERICAS";
  }
  return null;
}

function isRegionCheckboxChecked(value) {
  return value === true || value === 1 || value === "true" || value === "1";
}

/** Regions for individuals: User Management checkbox columns only (not company rollup lookups). */
function parseRegionsFromUserFields(fields) {
  const safeFields = fields && typeof fields === "object" ? fields : {};
  const codes = new Set();

  for (const fieldName of USER_REGION_CHECKBOX_FIELDS) {
    if (isRegionCheckboxChecked(safeFields[fieldName])) {
      const code = USER_REGION_CHECKBOX_TO_CODE[fieldName];
      if (code) codes.add(code);
    }
  }

  // Only use Region/Regions multi-select when no checkboxes are set (legacy data entry).
  if (codes.size === 0) {
    const regionField = safeFields["Region"] || safeFields["Regions"] || safeFields["REGION"] || "";
    if (regionField) {
      const items = Array.isArray(regionField)
        ? regionField
        : String(regionField).split(",").map((x) => x.trim()).filter(Boolean);
      items.forEach((item) => {
        const code = normalizeRegionToken(item);
        if (code && code !== "GLOBAL") codes.add(code);
      });
    }
  }

  if (codes.size >= 5 && [...ALL_USER_REGION_CODES].every((r) => codes.has(r))) {
    return ["GLOBAL"];
  }
  return [...codes];
}

function parseCoverageTerritoriesFromFields(fields) {
  const safeFields = fields && typeof fields === "object" ? fields : {};
  const raw =
    safeFields["Coverage Territories"] ??
    safeFields["Coverage territories"] ??
    safeFields["COVERAGE TERRITORIES"] ??
    "";
  if (raw == null || raw === "") return [];

  const territories = [];
  const pushValue = (value) => {
    if (value == null) return;
    if (Array.isArray(value)) {
      value.forEach(pushValue);
      return;
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (!trimmed || isRecordId(trimmed)) return;
      // Keep full text (e.g. "Mexico, Central America, & Caribbean") as one territory.
      territories.push(trimmed);
      return;
    }
    if (typeof value === "object") {
      const name =
        value.name ||
        value.label ||
        value.title ||
        value.fields?.Name ||
        value.fields?.["Coverage Territories"] ||
        "";
      if (name) territories.push(String(name).trim());
    }
  };

  pushValue(raw);
  return uniqueStrings(territories);
}

function extractLanguagesFromFields(fields) {
  const safeFields = fields && typeof fields === "object" ? fields : {};
  const languages = [];
  const pushLanguageValue = (value) => {
    if (value == null) return;
    if (Array.isArray(value)) {
      value.forEach(pushLanguageValue);
      return;
    }
    if (typeof value === "string" && value.trim()) {
      value
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean)
        .forEach((x) => languages.push(x));
      return;
    }
    if (typeof value === "object" && value.name) {
      languages.push(String(value.name).trim());
    }
  };

  const explicit =
    safeFields["Languages"] ??
    safeFields["Language"] ??
    safeFields["languages"] ??
    null;
  if (explicit != null && explicit !== "") {
    pushLanguageValue(explicit);
    return uniqueStrings(languages);
  }

  Object.entries(safeFields).forEach(([key, value]) => {
    if (!/language/i.test(String(key || ""))) return;
    if (Array.isArray(value)) {
      value.forEach((item) => {
        if (typeof item === "string" && item.trim()) languages.push(item.trim());
        else if (item && typeof item === "object" && item.name) languages.push(String(item.name).trim());
      });
    } else if (typeof value === "string" && value.trim()) {
      value.split(",").map((x) => x.trim()).filter(Boolean).forEach((x) => languages.push(x));
    }
  });
  return uniqueStrings(languages);
}

function coerceFiniteNumber(raw) {
  if (raw == null || raw === "") return null;
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  const s = String(raw).trim().replace(/,/g, "");
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

/** Match Airtable column labels even if spacing/casing differs slightly from our literals. */
function getFieldRawInsensitive(fields, preferredNames) {
  const safe = fields && typeof fields === "object" ? fields : {};
  for (const name of preferredNames) {
    if (Object.prototype.hasOwnProperty.call(safe, name)) {
      const v = safe[name];
      if (v !== undefined) return v;
    }
  }
  const lowerByKey = new Map();
  for (const k of Object.keys(safe)) {
    lowerByKey.set(k.trim().toLowerCase(), safe[k]);
  }
  for (const name of preferredNames) {
    const key = String(name).trim().toLowerCase();
    if (lowerByKey.has(key)) return lowerByKey.get(key);
  }
  return undefined;
}

function parseNumericField(fields, names) {
  for (const name of names) {
    const raw = getFieldRawInsensitive(fields, [name]);
    const num = coerceFiniteNumber(raw);
    if (num !== null) return num;
  }
  return 0;
}

function readPersonStatFromFields(fields, airtableColumnName) {
  const raw = getFieldRawInsensitive(fields, [airtableColumnName]);
  const num = coerceFiniteNumber(raw);
  return num !== null ? num : 0;
}

// Field mappings for Airtable tables
const F = {
  // Users table for individuals
  users: {
    table: PLATFORM_USERS_TABLE_ID,
    firstName: "fldG5nbAijQkUVSzr", // First Name
    lastName: "fldV0g50iRB8J46Hh", // Last Name
    email: "fldBl7IXEscwkMhnZ", // Email
    company: "fldCompany", // Company/Organization (field name)
    phone: "fldPhone", // Phone number (field name)
    country: "fld2LWEer7PgkSCe9", // Country
    userType: "User Type", // User Type field (field name)
    profile: "Profile" // Profile image (field name)
  },
  // Company Profile table - main source for companies
  companyProfile: {
    table: "tblItyfH6MlOnMKZ9", // Company Profile table ID
    companyId: "Company ID", // Company ID field (like Term ID in Financial Term Library)
    companyName: "Company Name",
    userType: "User Type", // Should have: HOTEL MGMT. COMPANY, HOTEL BRANDS (FRANCHISE), HOTEL OWNERS
    companyType: "Company Type", // Primary field for company type display
    location: "Location", // or "Headquarters" or "Headquarters Location"
    website: "Website",
    description: "Company Description", // or "Description" or "Company Overview"
    companyOverview: "Company Overview", // Alternative field name
    regions: "Regions", // or "Regions Supported"
    closedDeals: "Closed Deals",
    brandCount: "Brand Count", // or "# of Brand" or "Number of Brands"
    submittedBids: "Submitted Bids",
    logo: "Logo", // or "Company Logo"
    /** Partner Directory Company Role filter ↔ Airtable single-select */
    companyRoleInEcosystem: COMPANY_ROLE_AIRTABLE_FIELD,
  },
  // Brand Setup - Brand Basics for hotel brands (franchise) - as backup
  brands: {
    table: "Brand Setup - Brand Basics", // Table name
    name: "Brand Name",
    parentCompany: "Parent Company",
    chainScale: "Hotel Chain Scale",
    status: "Brand Status",
    tagline: "Brand Tagline",
    positioning: "Brand Positioning",
    valueProposition: "Brand Value Proposition",
    differentiators: "Key Brand Differentiators"
  },
  // Third Party Operators for hotel management companies - as backup
  operators: {
    table: "Third Party Operators", // Table name
    companyName: "Company Name",
    website: "Website",
    headquarters: "Headquarters",
    description: "Company Description",
    regions: "Regions Supported",
    brandsManaged: "Brands Managed",
    numberOfBrands: "Number of Brands Supported"
  }
};

// Get all partners (companies and individuals)
export async function getPartners(req, res) {
  let companies = [];
  let individuals = [];
  
  try {
    const base = getBase();
    if (!base) {
      // Return empty arrays if Airtable is not configured
      return res.json({
        companies: [],
        individuals: []
      });
    }

    // Fetch companies from Company Profile table (primary source)
    let companyProfileRecords = [];
    try {
      // Try table ID first, then table name as fallback
      const tableIdentifier = F.companyProfile.table;
      try {
        await new Promise((resolve, reject) => {
          const table = base(tableIdentifier);
          table
            .select({
              // Fetch complete dataset via pagination (no hard cap)
            })
            .eachPage(
              (pageRecords, fetchNextPage) => {
                try {
                  companyProfileRecords.push(...pageRecords);
                  fetchNextPage();
                } catch (err) {
                  console.error('❌ Error in eachPage callback:', err);
                  console.error('Error details:', err.message, err.stack);
                  reject(err);
                }
              },
              (err) => {
                if (err) {
                  console.error('❌ Error in eachPage completion:', err);
                  console.error('Error type:', err.constructor.name);
                  console.error('Error message:', err.message);
                  if (err.error) {
                    console.error('Airtable error object:', JSON.stringify(err.error, null, 2));
                  }
                  reject(err);
                } else {
                  resolve();
                }
              }
            );
        });
      } catch (selectError) {
        console.error('❌ Error in select/eachPage:', selectError);
        throw selectError; // Re-throw to be caught by outer catch
      }

      // Process company records
      
      const companyProfiles = companyProfileRecords
        .filter(record => {
          const fields = record.fields || {};
          // Try multiple field name variations
          const companyName = fields[F.companyProfile.companyName] 
            || fields["Company Name"] 
            || fields["companyName"]
            || fields["company_name"]
            || fields["Name"]
            || '';
          
          return companyName && companyName.trim();
        })
        .map(record => {
          try {
            const fields = record.fields || {};
            // Try all possible field name variations
            const companyName = fields[F.companyProfile.companyName] 
              || fields["Company Name"] 
              || fields["companyName"]
              || fields["Name"]
              || fields["name"]
              || '';
            // Prioritize "Company Type" field - try multiple variations
            let companyType = '';
            const fieldKeysForType = Object.keys(fields);
            
            // Try exact matches first
            for (const key of fieldKeysForType) {
              const lowerKey = key.toLowerCase().trim();
              if (lowerKey === 'company type' || lowerKey === 'companytype') {
                const value = fields[key];
                companyType = (value != null && value !== undefined) ? String(value).trim() : '';
                if (companyType) break;
              }
            }
            
            // If not found, try partial matches
            if (!companyType) {
              for (const key of fieldKeysForType) {
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey.includes('company') && lowerKey.includes('type')) {
                  const value = fields[key];
                  companyType = (value != null && value !== undefined) ? String(value).trim() : '';
                  if (companyType) break;
                }
              }
            }
            
            // Get User Type separately (don't use companyType as fallback - they're different fields)
            const userType = fields[F.companyProfile.userType] || fields["User Type"] || '';
            
            // Get Company ID field (like Term ID in Financial Term Library)
            const companyId = fields[F.companyProfile.companyId] || fields["Company ID"] || '';
            
            // Get location from "Company HQ Country" column in Airtable
            const location = fields["Company HQ Country"] || '';
            // Get "Company Website" directly from Airtable — normalize www for display/links only
            const website = fields["Company Website"]
              ? stripLeadingWwwFromWebsiteUrl(String(fields["Company Website"]).trim())
              : '';
            // Prioritize "Company Overview" field from Airtable - try multiple variations
            // Check all possible field name variations
            let companyOverview = '';
            const fieldKeysForOverview = Object.keys(fields);
            
            // Try exact matches first
            for (const key of fieldKeysForOverview) {
              const lowerKey = key.toLowerCase().trim();
              if (lowerKey === 'company overview' || lowerKey === 'companyoverview') {
                const value = fields[key];
                companyOverview = (value != null && value !== undefined) ? String(value).trim() : '';
                if (companyOverview) break;
              }
            }
            if (!companyOverview) {
              for (const key of fieldKeysForOverview) {
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey.includes('company') && lowerKey.includes('overview')) {
                  const value = fields[key];
                  companyOverview = (value != null && value !== undefined) ? String(value).trim() : '';
                  if (companyOverview) break;
                }
              }
            }
            // Use Company Overview only - don't fall back to other description fields
            const description = companyOverview;
            
            // SIMPLE region detection: Check ALL boolean fields - if checked (true), extract region from field name
            let regions = [];
            try {
              const allFieldNames = Object.keys(fields);
              for (const fieldName of allFieldNames) {
                const fieldValue = fields[fieldName];
                if (typeof fieldValue !== 'boolean' || fieldValue !== true) continue;
                const fieldNameLower = fieldName.trim().toLowerCase();
                if (fieldNameLower.includes('americas') && !fieldNameLower.includes('latin') && !regions.includes("AMERICAS")) regions.push("AMERICAS");
                else if ((fieldNameLower.includes('caribbean') || fieldNameLower.includes('latin')) && !fieldNameLower.includes('americas') && !regions.includes("CALA")) regions.push("CALA");
                else if (fieldNameLower.includes('europe') && !fieldNameLower.includes('middle') && !fieldNameLower.includes('africa') && !regions.includes("EUROPE")) regions.push("EUROPE");
                else if ((fieldNameLower.includes('middle') && fieldNameLower.includes('east')) || (fieldNameLower.includes('middle') && fieldNameLower.includes('africa')) || fieldNameLower.includes('mea') && !regions.includes("MEA")) regions.push("MEA");
                else if ((fieldNameLower.includes('asia') && fieldNameLower.includes('pacific')) || fieldNameLower.includes('asia pacific') || fieldNameLower === 'ap' && !regions.includes("AP")) regions.push("AP");
              }
            } catch (regionError) {
              // regions stay empty on error
            }
            
            // Get fields directly from Airtable - no fallbacks
            const closedDeals = fields[F.companyProfile.closedDeals] || fields["Closed Deals"];
            const brandCount = fields[F.companyProfile.brandCount] || fields["Brand Count"] || fields["# of Brand"] || fields["Number of Brands"];
            const submittedBids = fields[F.companyProfile.submittedBids] || fields["Submitted Bids"];
            const logo = fields[F.companyProfile.logo] || fields["Logo"] || fields["Company Logo"];

            // Company Type → directory filter key (User Type column is usually empty)
            const normalizedUserType = companyTypeFromProfileFields(fields);

            // Get logo - use first letter of company name as default
            // Only use logo URL if it's a valid image URL, otherwise use initials
            let logoDisplay = companyName && companyName.length > 0 ? companyName.charAt(0).toUpperCase() : '?';
            try {
              if (logo && Array.isArray(logo) && logo.length > 0 && logo[0]) {
                // Check if it's an object with a url property (Airtable attachment)
                if (logo[0].url && typeof logo[0].url === 'string' && logo[0].url.startsWith('http')) {
                  // Store as object to indicate it's an image URL
                  logoDisplay = { type: 'image', url: logo[0].url };
                }
              } else if (logo && typeof logo === 'string' && logo.startsWith('http')) {
                // Direct URL string
                logoDisplay = { type: 'image', url: logo };
              }
            } catch (logoError) {
              // keep logoDisplay as initial
            }

            const enrichment = buildCompanyEnrichment(fields, record.createdTime || null);
            const companyRole = companyRoleFromEcosystemField(fields);
            return {
              id: record.id || '',
              companyId: companyId || '', // Company ID field (like Term ID in Financial Term Library)
              name: companyName || '',
              userType: normalizedUserType,
              companyType: companyType || '', // Include original Company Type from Airtable for reference
              companyRole,
              location: location || '', // No fallback - use exactly what's in Airtable
              website: website || '', // No fallback - use exactly what's in Airtable
              description: description || '', // Only use Company Overview - no fallback
              companyOverview: description || '', // Primary field from Airtable Company Overview column
              regions: regions, // No fallback - use exactly what's in Airtable (empty array if no checkboxes checked)
              closedDeals: closedDeals ? Number(closedDeals) : 0,
              brandCount: brandCount ? Number(brandCount) : 0,
              submittedBids: submittedBids ? Number(submittedBids) : 0,
              logo: logoDisplay,
              _createdTime: record.createdTime || null,
              rawFields: enrichment.rawFields,
              brandRecordIds: enrichment.brandRecordIds,
              userManagementRecordIds: enrichment.userManagementRecordIds,
              services: enrichment.services,
              primaryServices: enrichment.primaryServices,
              brands: enrichment.brands
            };
          } catch (recordError) {
            console.error('Error processing record:', recordError);
            console.error('Record ID:', record.id);
            return null; // Return null for failed records, we'll filter them out
          }
        })
        .filter(record => record !== null); // Remove any failed records

      companies.push(...companyProfiles);

    } catch (companyProfileError) {
      console.error("❌ ERROR fetching Company Profile with table ID:", companyProfileError.message);
      
      // Try with table name as fallback
      try {
        companyProfileRecords = [];
        await new Promise((resolve, reject) => {
          base("Company Profile")
            .select({
              // Fetch complete dataset via pagination (no hard cap)
            })
            .eachPage(
              (pageRecords, fetchNextPage) => {
                try {
                  companyProfileRecords.push(...pageRecords);
                  fetchNextPage();
                } catch (err) {
                  reject(err);
                }
              },
              (err) => {
                if (err) {
                  console.error("❌ Error with table name too:", err);
                  reject(err);
                } else {
                  resolve();
                }
              }
            );
        });
        
        // Process the records we just fetched
        if (companyProfileRecords.length > 0) {
          const companyProfiles = companyProfileRecords
            .filter(record => {
              const fields = record.fields;
              const companyName = fields["Company Name"] || fields["companyName"] || '';
              return companyName && companyName.trim();
            })
            .map(record => {
              const fields = record.fields;
              const companyName = fields["Company Name"] || fields["companyName"] || '';
              // Prioritize "Company Type" field - try multiple variations
              let companyType = '';
              const fieldKeysForTypeFallback = Object.keys(fields);
              
              // Try exact matches first
              for (const key of fieldKeysForTypeFallback) {
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey === 'company type' || lowerKey === 'companytype') {
                  const value = fields[key];
                  companyType = (value != null && value !== undefined) ? String(value).trim() : '';
                  if (companyType) break;
                }
              }
              
              // If not found, try partial matches
              if (!companyType) {
                for (const key of fieldKeysForTypeFallback) {
                  const lowerKey = key.toLowerCase().trim();
                  if (lowerKey.includes('company') && lowerKey.includes('type')) {
                    const value = fields[key];
                    companyType = (value != null && value !== undefined) ? String(value).trim() : '';
                    if (companyType) break;
                  }
                }
              }
              
              // Fallback to User Type
              const userType = companyType || fields["User Type"] || fields["userType"] || '';
              // Get location from "Company HQ Country" column in Airtable
              const location = fields["Company HQ Country"] || '';
              // Get "Company Website" directly from Airtable — normalize www for display/links only
              const website = fields["Company Website"]
                ? stripLeadingWwwFromWebsiteUrl(String(fields["Company Website"]).trim())
                : '';
              // Prioritize "Company Overview" field from Airtable - try multiple variations
              let companyOverview = '';
              const fieldKeysForOverviewFallback = Object.keys(fields);
              
              // Try exact matches first
              for (const key of fieldKeysForOverviewFallback) {
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey === 'company overview' || lowerKey === 'companyoverview') {
                  const value = fields[key];
                  companyOverview = (value != null && value !== undefined) ? String(value).trim() : '';
                  if (companyOverview) break;
                }
              }
              
              // If not found, try partial matches
              if (!companyOverview) {
                for (const key of fieldKeysForOverviewFallback) {
                  const lowerKey = key.toLowerCase().trim();
                  if (lowerKey.includes('company') && lowerKey.includes('overview')) {
                    const value = fields[key];
                    companyOverview = (value != null && value !== undefined) ? String(value).trim() : '';
                    if (companyOverview) break;
                  }
                }
              }
              
              const description = companyOverview; // Only use Company Overview - no fallback
              // Simple region detection: Check ALL boolean fields - if checked (true), check if field name contains region keywords
              let regions = [];
              try {
                const allFieldNames = Object.keys(fields);
                
                // Simple approach: Check every boolean field - if it's true, check if the field name contains region keywords
                for (const fieldName of allFieldNames) {
                  const fieldValue = fields[fieldName];
                  
                  // Only process boolean (checkbox) fields that are checked (true)
                  if (typeof fieldValue !== 'boolean' || fieldValue !== true) continue;
                  
                  const fieldNameLower = fieldName.trim().toLowerCase();
                  
                  // Simple keyword matching - if field name contains region keywords and checkbox is checked, add it
                  if (fieldNameLower.includes('americas') && !fieldNameLower.includes('latin') && !regions.includes("AMERICAS")) regions.push("AMERICAS");
                  else if ((fieldNameLower.includes('caribbean') || fieldNameLower.includes('latin')) && !fieldNameLower.includes('americas') && !regions.includes("CALA")) regions.push("CALA");
                  else if (fieldNameLower.includes('europe') && !fieldNameLower.includes('middle') && !fieldNameLower.includes('africa') && !regions.includes("EUROPE")) regions.push("EUROPE");
                  else if ((fieldNameLower.includes('middle') && fieldNameLower.includes('east')) || (fieldNameLower.includes('middle') && fieldNameLower.includes('africa')) || (fieldNameLower.includes('mea')) && !regions.includes("MEA")) regions.push("MEA");
                  else if ((fieldNameLower.includes('asia') && fieldNameLower.includes('pacific')) || fieldNameLower.includes('asia pacific') || fieldNameLower === 'ap' && !regions.includes("AP")) regions.push("AP");
                }
              } catch (regionError) {
                // regions stay empty
              }
              // Get fields directly from Airtable - no fallbacks
              const closedDeals = fields["Closed Deals"];
              const brandCount = fields["Brand Count"] || fields["# of Brand"];
              const submittedBids = fields["Submitted Bids"];

              const normalizedUserType = companyTypeFromProfileFields(fields);

              // Get logo - use first letter of company name as default only if no logo in Airtable
              let logoDisplay = '';
              const logo = fields["Logo"] || fields["Company Logo"];
              if (logo && Array.isArray(logo) && logo.length > 0 && logo[0] && logo[0].url) {
                logoDisplay = { type: 'image', url: logo[0].url };
              } else if (logo && typeof logo === 'string' && logo.startsWith('http')) {
                logoDisplay = { type: 'image', url: logo };
              } else if (companyName && companyName.length > 0) {
                logoDisplay = companyName.charAt(0).toUpperCase();
              }

              // Get Company ID field (like Term ID in Financial Term Library)
              const companyId = fields[F.companyProfile.companyId] || fields["Company ID"] || '';
              
              const enrichment = buildCompanyEnrichment(fields, record.createdTime || null);
              const companyRole = companyRoleFromEcosystemField(fields);
              return {
                id: record.id || '',
                companyId: companyId || '', // Company ID field (like Term ID in Financial Term Library)
                name: companyName || '',
                userType: normalizedUserType,
                companyType: companyType || '', // Include original Company Type from Airtable for reference
                companyRole,
                location: location || '', // No fallback - use exactly what's in Airtable
                website: website || '', // No fallback - use exactly what's in Airtable
                description: description || '', // Only use Company Overview - no fallback
                companyOverview: description || '', // Primary field from Airtable Company Overview column
                regions: regions, // No fallback - use exactly what's in Airtable (empty array if no checkboxes checked)
                closedDeals: closedDeals ? Number(closedDeals) : 0,
                brandCount: brandCount ? Number(brandCount) : 0,
                submittedBids: submittedBids ? Number(submittedBids) : 0,
                logo: logoDisplay,
                _createdTime: record.createdTime || null,
                rawFields: enrichment.rawFields,
                brandRecordIds: enrichment.brandRecordIds,
                userManagementRecordIds: enrichment.userManagementRecordIds,
                services: enrichment.services,
                primaryServices: enrichment.primaryServices,
                brands: enrichment.brands
              };
            });
          
          companies.push(...companyProfiles);
        }
      } catch (fallbackError) {
        // Company Profile table not accessible; companies stay empty
      }
    }

    // SKIP Users table for companies - all company data should come from Company Profile table only
    // This ensures we only use data from the Company Profile table as requested

    // Fetch individuals from Users table (platform users).
    let userRecords = [];
    try {
      await new Promise((resolve, reject) => {
        base(PLATFORM_USERS_TABLE)
          .select({
            // Fetch complete dataset via pagination (no hard cap)
          })
          .eachPage(
            (pageRecords, fetchNextPage) => {
              try {
                userRecords.push(...pageRecords);
                fetchNextPage();
              } catch (err) {
                reject(err);
              }
            },
            (err) => {
              if (err) {
                console.error('Error fetching users:', err);
                reject(err);
              } else {
                resolve();
              }
            }
          );
      });

      // Filter out users without at least a first name or last name
      // Also try alternative field names in case field IDs don't match
      individuals = userRecords
        .filter(record => {
          const fields = record.fields;
          const firstName = fields[F.users.firstName] || fields["First Name"] || '';
          const lastName = fields[F.users.lastName] || fields["Last Name"] || '';
          if (!firstName && !lastName) return false;
          const visibility = contactVisibilityFromFields(fields);
          return isContactVisibleInPartnerDirectory(visibility);
        })
        .map(record => formatUserRecord(record));
    } catch (userError) {
      console.error('Error fetching user management records:', userError);
      individuals = [];
    }

    // Resolve linked brand record IDs to brand names so modal brand lists can render.
    try {
      const brandNameMap = await buildBrandNameMap(base);
      companies = attachResolvedBrandNames(companies, brandNameMap);
      individuals = attachResolvedBrandNames(individuals, brandNameMap);
      individuals = mergeLinkedCompanyIntoIndividuals(individuals, companies);
    } catch (brandResolveError) {
      console.error("Error resolving linked brand names:", brandResolveError);
    }

    // Always return data, even if empty
    return res.json({
      companies: companies || [],
      individuals: individuals || []
    });
    } catch (error) {
    console.error("❌ CRITICAL ERROR in getPartners:", error);
    console.error("Error message:", error.message);
    console.error("Error name:", error.name);
    console.error("Error stack:", error.stack);
    if (error.error) {
      console.error("Airtable error:", JSON.stringify(error.error, null, 2));
    }
    if (error.statusCode) {
      console.error("Airtable status code:", error.statusCode);
    }
    
    // Try to return at least empty data so the page doesn't completely break
    try {
      return res.status(500).json({ 
        error: "Failed to fetch partners", 
        details: error.message,
        companies: [],
        individuals: [],
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
      });
    } catch (responseError) {
      console.error("❌ Even error response failed:", responseError);
      // Last resort - just send a basic error
      res.status(500).send("Internal Server Error");
    }
  }
}


// Format user record from Airtable
function formatUserRecord(record) {
  const fields = record.fields;
  const companyInfo = parseLinkedCompanyInfo(fields);
  // Try both field IDs and field names for compatibility
  const firstName = fields[F.users.firstName] || fields["First Name"] || "";
  const lastName = fields[F.users.lastName] || fields["Last Name"] || "";
  const company = companyInfo.companyName || fields[F.users.company] || fields["Company/Organization"] || fields["Company Name"] || "";
  const country =
    fields["Based (Country)"] ||
    fields[F.users.country] ||
    fields["Country"] ||
    "";
  const userType =
    fields[F.users.userType] ||
    fields["User Type"] ||
    "";
  const platformRole =
    fields["Platform Role"] ||
    fields[F.users.userType] ||
    "";
  const email =
    fields[F.users.email] ||
    fields["Email"] ||
    fields["Company Email"] ||
    "";
  const phone =
    fields[F.users.phone] ||
    fields["Phone Number"] ||
    fields["Phone"] ||
    "";
  const website = stripLeadingWwwFromWebsiteUrl(
    String(fields["Website"] || fields["Company Website"] || fields["Personal Website"] || "").trim()
  );
  const companyRecordId = companyInfo.companyRecordId;

  const profilePicture = parseAttachmentUrl(
    fields[F.users.profile] ||
      fields["Profile"] ||
      fields["Profile Picture"] ||
      fields["Headshot"] ||
      fields["Photo"]
  );

  const location = country ? String(country).trim() : "";
  const regions = parseRegionsFromUserFields(fields);
  const coverageTerritories = parseCoverageTerritoriesFromFields(fields);
  const languages = extractLanguagesFromFields(fields);
  const enrichment = buildCompanyEnrichment(fields, record.createdTime);
  const explicitBrandsField =
    fields["Brands Supported"] ||
    fields["Brands You Operate / Support"] ||
    fields["Brands"] ||
    fields["Brand Name"] ||
    null;
  if (explicitBrandsField) {
    if (Array.isArray(explicitBrandsField)) {
      explicitBrandsField.forEach((item) => {
        if (typeof item === "string" && !isRecordId(item)) enrichment.brands.push(item);
        else if (item && typeof item === "object") {
          extractRecordIdsFromValue(item, enrichment.brandRecordIds);
          const name =
            item.fields?.["Brand Name"] ||
            item.fields?.Name ||
            item.name ||
            "";
          if (name) enrichment.brands.push(String(name));
        }
      });
    } else if (typeof explicitBrandsField === "string" && !isRecordId(explicitBrandsField)) {
      explicitBrandsField
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean)
        .forEach((x) => enrichment.brands.push(x));
    }
    enrichment.brands = uniqueStrings(enrichment.brands);
  }
  const closedDeals = readPersonStatFromFields(fields, PUF.closedDeals);
  const brandCount = readPersonStatFromFields(fields, PUF.uniqueBrandsDeals);
  const submittedBids = readPersonStatFromFields(fields, PUF.submittedBids);

  const rawFields = { ...fields };
  if (!rawFields["Created Date"] && record.createdTime) {
    rawFields["Created Date"] = record.createdTime;
  }

  return {
    id: record.id,
    firstName: firstName || "",
    lastName: lastName || "",
    companyTitle: fields["Title"] || fields["Company Title"] || "",
    companyName: company || "",
    companyRecordId: isRecordId(companyRecordId) ? companyRecordId : "",
    phoneNumber: phone || "",
    email: email || "",
    companyEmail: email || "",
    userType: userType || "",
    platformRole: platformRole || "",
    regions,
    coverageTerritories,
    languages,
    contactVisibility: contactVisibilityFromFields(fields),
    location: location || "",
    website: website || "",
    closedDeals,
    brandCount,
    submittedBids,
    profilePicture: profilePicture || null,
    responsivenessCombinedBadge:
      fields["responsiveness_combined_badge"] ||
      fields["Responsiveness Combined Badge"] ||
      "",
    responsivenessTimeCategory:
      fields["responsiveness_response_time_category"] ||
      fields["Responsiveness Response Time Category"] ||
      "",
    responsivenessFrequencyCategory:
      fields["responsiveness_frequency_category"] ||
      fields["Responsiveness Frequency Category"] ||
      "",
    _createdTime: record.createdTime || null,
    rawFields,
    brandRecordIds: enrichment.brandRecordIds,
    brands: enrichment.brands
  };
}

// Create a new user
export async function createUser(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const {
      firstName,
      lastName,
      companyTitle,
      phoneNumber,
      companyEmail,
      platformRole,
      regions,
      contactVisibility
    } = req.body;

    // Validate required fields
    if (!firstName || !lastName || !companyTitle || !phoneNumber || !companyEmail || !platformRole || !contactVisibility) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Create user record in Airtable
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    
    const userRecord = await base(F.users.table).create({
      [F.users.firstName]: firstName,
      [F.users.lastName]: lastName,
      [F.users.email]: companyEmail,
      [F.users.phone]: phoneNumber,
      [F.users.company]: companyTitle, // Using company field for company title temporarily
      [F.users.userType]: platformRole
      // Note: regions, contactVisibility would need to be added as fields to Users table
    }, { typecast: true });

    return res.json({
      id: userRecord.id,
      message: "User created successfully"
    });
  } catch (error) {
    console.error("Error creating user:", error);
    return res.status(500).json({ error: "Failed to create user", details: error.message });
  }
}

// Update an existing user
export async function updateUser(req, res) {
  try {
    if (req.method !== "PUT") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const userId = req.params.userId;
    if (!userId) {
      return res.status(400).json({ error: "User ID is required" });
    }

    const {
      firstName,
      lastName,
      companyTitle,
      phoneNumber,
      companyEmail,
      platformRole,
      regions,
      contactVisibility
    } = req.body;

    // Validate required fields
    if (!firstName || !lastName || !companyTitle || !phoneNumber || !companyEmail || !platformRole || !contactVisibility) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Update user record in Airtable
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    
    const userRecord = await base(F.users.table).update(userId, {
      [F.users.firstName]: firstName,
      [F.users.lastName]: lastName,
      [F.users.email]: companyEmail,
      [F.users.phone]: phoneNumber,
      [F.users.company]: companyTitle, // Using company field for company title temporarily
      [F.users.userType]: platformRole
      // Note: regions, contactVisibility would need to be added as fields to Users table
    }, { typecast: true });

    return res.json({
      id: userRecord.id,
      message: "User updated successfully"
    });
  } catch (error) {
    console.error("Error updating user:", error);
    return res.status(500).json({ error: "Failed to update user", details: error.message });
  }
}

// Get single company by Company ID (like getTermById in Financial Term Library)
export async function getCompanyById(req, res) {
  try {
    const { id } = req.query;

    if (!id) {
      return res.status(400).json({ error: 'Company ID is required' });
    }

    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: 'Airtable base not configured' });
    }

    // Find company by Company ID field
    const tableNameOrId = F.companyProfile.table; // Use table ID
    const records = await base(tableNameOrId)
      .select({
        filterByFormula: `{Company ID}='${id.replace(/'/g, "\\'")}'`,
        maxRecords: 1
      })
      .firstPage();

    if (records.length === 0) {
      return res.status(404).json({ error: 'Company not found' });
    }

    // Process the record using the same logic as getPartners
    const record = records[0];
    const fields = record.fields || {};
    const companyName = fields[F.companyProfile.companyName] || fields["Company Name"] || '';
    const companyId = fields[F.companyProfile.companyId] || fields["Company ID"] || '';
    const companyType = fields[F.companyProfile.companyType] || fields["Company Type"] || '';
    const userType = fields[F.companyProfile.userType] || fields["User Type"] || '';
    const location = fields["Company HQ Country"] || '';
    const website = fields["Company Website"]
      ? stripLeadingWwwFromWebsiteUrl(String(fields["Company Website"]).trim())
      : '';
    const companyOverview = fields["Company Overview"] || '';
    
    // Get regions from checkbox fields
    let regions = [];
    const allFieldNames = Object.keys(fields);
    for (const fieldName of allFieldNames) {
      const fieldValue = fields[fieldName];
      if (typeof fieldValue !== 'boolean' || fieldValue !== true) continue;
      
      const fieldNameLower = fieldName.trim().toLowerCase();
      if (fieldNameLower.includes('americas') && !fieldNameLower.includes('latin') && !regions.includes("AMERICAS")) {
        regions.push("AMERICAS");
      } else if ((fieldNameLower.includes('caribbean') || fieldNameLower.includes('latin')) && 
                !fieldNameLower.includes('americas') && !regions.includes("CALA")) {
        regions.push("CALA");
      } else if (fieldNameLower.includes('europe') && 
                !fieldNameLower.includes('middle') && !fieldNameLower.includes('africa') && !regions.includes("EUROPE")) {
        regions.push("EUROPE");
      } else if ((fieldNameLower.includes('middle') && fieldNameLower.includes('east')) || 
                (fieldNameLower.includes('middle') && fieldNameLower.includes('africa')) ||
                fieldNameLower.includes('mea') && !regions.includes("MEA")) {
        regions.push("MEA");
      } else if ((fieldNameLower.includes('asia') && fieldNameLower.includes('pacific')) ||
                fieldNameLower.includes('asia pacific') ||
                fieldNameLower === 'ap' && !regions.includes("AP")) {
        regions.push("AP");
      }
    }
    
    const closedDeals = fields[F.companyProfile.closedDeals] || fields["Closed Deals"] || 0;
    const brandCount = fields[F.companyProfile.brandCount] || fields["Brand Count"] || 0;
    const submittedBids = fields[F.companyProfile.submittedBids] || fields["Submitted Bids"] || 0;
    
    // Get logo
    let logoDisplay = companyName && companyName.length > 0 ? companyName.charAt(0).toUpperCase() : '?';
    const logo = fields[F.companyProfile.logo] || fields["Logo"] || fields["Company Logo"];
    if (logo && Array.isArray(logo) && logo.length > 0 && logo[0] && logo[0].url) {
      logoDisplay = { type: 'image', url: logo[0].url };
    } else if (logo && typeof logo === 'string' && logo.startsWith('http')) {
      logoDisplay = { type: 'image', url: logo };
    }
    
    const normalizedUserType = companyTypeFromProfileFields(fields);

    const enrichment = buildCompanyEnrichment(fields, record.createdTime || null);
    const companyRole = companyRoleFromEcosystemField(fields);
    const company = {
      id: record.id || '',
      companyId: companyId || '',
      name: companyName || '',
      userType: normalizedUserType,
      companyType: companyType || '',
      companyRole,
      location: location || '',
      website: website || '',
      description: companyOverview || '',
      companyOverview: companyOverview || '',
      regions: regions,
      closedDeals: closedDeals ? Number(closedDeals) : 0,
      brandCount: brandCount ? Number(brandCount) : 0,
      submittedBids: submittedBids ? Number(submittedBids) : 0,
      logo: logoDisplay,
      _createdTime: record.createdTime || null,
      rawFields: enrichment.rawFields,
      brandRecordIds: enrichment.brandRecordIds,
      userManagementRecordIds: enrichment.userManagementRecordIds,
      services: enrichment.services,
      primaryServices: enrichment.primaryServices,
      brands: enrichment.brands
    };

    res.json(company);
  } catch (error) {
    console.error('❌ Error fetching company by ID:', error);
    res.status(500).json({ 
      error: 'Failed to fetch company',
      message: error.message 
    });
  }
}

// Default export for route handler
export default async function partnerDirectoryHandler(req, res) {
  if (req.method === "GET") {
    // Check if it's a request for a single company by ID
    if (req.query.id && !req.query.search && !req.query.userType && !req.query.region) {
      return getCompanyById(req, res);
    }
    return getPartners(req, res);
  } else {
    return res.status(405).json({ error: "Method not allowed" });
  }
}
