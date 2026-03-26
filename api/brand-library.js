import Airtable from "airtable";

// Lazy initialization - only connect when needed
function getBase() {
    if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
        throw new Error("Airtable API credentials not configured");
    }
    return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

// Field mappings for Brand Library
//
// Brand Setup tabs → Airtable tables → read vs write:
// | Tab (section)          | Airtable table                        | GET (prefill) | PATCH (save) |
// |------------------------|---------------------------------------|---------------|--------------|
// | 0 Brand Basics         | Brand Setup - Brand Basics            | yes           | yes          |
// | 0 Sustainability/ESG  | Brand Setup - Sustainability & ESG     | yes           | yes          |
// | 1 Brand Footprint      | Brand Setup - Brand Footprint         | yes           | yes          |
// | 2 Project Fit          | Brand Setup - Project Fit             | yes           | yes          |
// | 3 Portfolio & Perf     | Brand Setup - Portfolio & Performance  | yes           | yes          |
// | 4 Brand Standards      | Brand Setup - Brand Standards         | yes           | yes          |
// | 5 Fee Structure        | Brand Setup - Fee Structure           | yes           | yes          |
// | 5 Deal Terms           | Brand Setup - Deal Terms              | yes           | yes          |
// | 6 Brand/OP Support     | Brand Setup - Operational Support     | yes           | yes          |
// | 7 Legal Terms          | Brand Setup - Legal Terms             | yes           | yes          |
//
// Preload: Brand Setup page pulls from these tables; FORM_TO_AIRTABLE_* and F.* define column ↔ form field mapping.
const F = {
  brandBasics: {
    table: "Brand Setup - Brand Basics",
    name: "Brand Name",
    logo: "Logo",
    parentCompany: "Parent Company",
    chainScale: "Hotel Chain Scale",
    brandModel: "Brand Model",
    serviceModel: "Hotel Service Model",
    yearLaunched: "Year Brand Launched",
    developmentStage: "Brand Development Stage",
    positioning: "Brand Positioning",
    tagline: "Brand Tagline",
    customerPromise: "Brand Customer Promise",
    valueProposition: "Brand Value Proposition",
    brandPillars: "Brand Pillars",
    companyHistory: "Brand History",
    targetSegments: "Target Guest Segments",
    guestPsychographics: "Guest Psychographics Description",
    differentiators: "Key Brand Differentiators",
    sustainability: "Sustainability Positioning",
    status: "Brand Status",
    architecture: "Brand Architecture",
    profileAnalysis: "Brand Profile Analysis"
  },
  brandFootprint: {
    table: "Brand Setup - Brand Footprint",
    brandName: "Brand Name"
  },
  feeStructure: {
    table: "Brand Setup - Fee Structure",
    brandName: "Brand Name"
  },
  brandStandards: {
    table: "Brand Setup - Brand Standards",
    brandName: "Brand Name"
  },
  dealTerms: {
    table: "Brand Setup - Deal Terms",
    brandName: "Brand Name"
  },
  portfolioPerformance: {
    table: "Brand Setup - Portfolio & Performance",
    brandName: "Brand Name"
  },
  projectFit: {
    table: "Brand Setup - Project Fit",
    brandName: "Brand Name"
  },
  operationalSupport: { table: "Brand Setup - Operational Support" },
  legalTerms: { table: "Brand Setup - Legal Terms" },
  loyaltyCommercial: {
    table: "Brand Setup - Loyalty & Commercial",
    brandName: "Brand Name"
  },
  sustainabilityEsg: {
    table: "Brand Setup - Sustainability & ESG",
    brandName: "Brand Name"
  }
};

// Brand Setup - Project Fit: form field name → exact Airtable column name (no fallbacks). Used for preload.
const PROJECT_FIT_AIRTABLE_TO_FORM = {
  // idealProjectTypes built from PROJECT_FIT_ACCEPTABLE_PROJECT_TYPES_COLUMNS
  // idealBuildingTypes built from PROJECT_FIT_ACCEPTABLE_BUILDING_TYPES_COLUMNS
  // idealAgreementTypes built from PROJECT_FIT_ACCEPTABLE_AGREEMENT_TYPES_COLUMNS
  // projectStage built from PROJECT_FIT_ACCEPTABLE_PROJECT_STAGES_COLUMNS
  idealRoomCountMin: 'Min - Room Count',
  idealRoomCountMax: 'Max - Room Count',
  idealProjectSizeMin: 'Min - Ideal Project Size',
  idealProjectSizeMax: 'Max - Ideal Project Size',
  minReqOperatorExperienceYears: 'Req Operator Exp',
  minLeadTimeMonths: 'Min Lead Time',
  preferredOwnerType: 'Preferred Owner/Investor Type',
  coBrandingAllowed: 'Co-Branding Allowed',
  brandedResidencesAllowed: 'Branded Residences Allowed',
  mixedUseAllowed: 'Mixed-Use Development Allowed',
  // priorityMarkets built from PROJECT_FIT_PRIORITY_MARKETS_COLUMNS
  // marketsToAvoid built from PROJECT_FIT_MARKETS_TO_AVOID_COLUMNS
  priorityMarketsOther: 'Other - Priority Markets Text',
  marketsToAvoidOther: 'Other - Markets to Avoid Text',
  milestoneOperatorSelectionMinMonths: 'Discussion to Selection - Target Milestones',
  milestoneConstructionStartMinMonths: 'Selection to Construction - Target Milestones',
  milestoneSoftOpeningMinMonths: 'PreOpen to SoftOpen - Target Milestones',
  milestoneGrandOpeningMinMonths: 'SoftOpen to GrandOpen - Target Milestones',
  dateFlexibility: 'Flexibility On Dates',
  // brandStatus built from PROJECT_FIT_BRAND_STATUS_COLUMNS
  pipRepositioningDetails: 'Typical PIP / Repositioning Profile You Will Consider (If Existing Hotel)',
  ownerHotelExperience: 'Owner / Sponsor Hotel Experience',
  // ownerInvolvementLevel built from PROJECT_FIT_OWNER_INVOLVEMENT_COLUMNS
  // ownerNonNegotiableTypes built from PROJECT_FIT_OWNER_NON_NEGOTIABLE_TYPES_COLUMNS
  ownerNonNegotiableOther: 'Other (Text) - Owner Non-Negotiables',
  ownerNonNegotiables: 'Owner Non-Negotiables & Decision Rights',
  capexSupport: 'CapEx and FF&E Support',
  // capitalStatus built from PROJECT_FIT_CAPITAL_STATUS_COLUMNS
  knownRedFlags: 'Red Flag Items That Typically Make You Decline or Proceed With Caution - Risk & Compliance',
  esgExpectations: 'ESG / Sustainability Expectations You Prefer Projects to Meet - Risk & Compliance',
  idealProjectsAdditionalNotes: "Anything else about your commercial 'sweet spot' we should know?",
  typicalPIPRange: 'Typical PIP Range ($/room or %)',
  whoPaysForPIP: 'Who Pays for PIP'
};

// Acceptable Project Types: Airtable columns (exact 1:1 names) → form checkbox value when checked.
const PROJECT_FIT_ACCEPTABLE_PROJECT_TYPES_COLUMNS = [
  { airtableColumn: 'New Build - Acceptable Project Type', formValue: 'New Build' },
  { airtableColumn: 'Conversion - Reflag - Acceptable Project Type', formValue: 'Conversion / Reflag' },
  { airtableColumn: 'Renovation / Repositioning - Acceptable Project Type', formValue: 'Renovation / Repositioning' },
  { airtableColumn: 'Expansion / Add-on - Acceptable Project Type', formValue: 'Expansion / Add-on' }
];

// Acceptable Building Types: same pattern as Project Types – Airtable columns (exact 1:1 names) → form value when checked.
const PROJECT_FIT_ACCEPTABLE_BUILDING_TYPES_COLUMNS = [
  { airtableColumn: 'Low-Rise - Acceptable Building Type', formValue: 'Low-Rise' },
  { airtableColumn: 'Mid-Rise - Acceptable Building Type', formValue: 'Mid-Rise' },
  { airtableColumn: 'High-Rise - Acceptable Building Type', formValue: 'High-Rise' },
  { airtableColumn: 'Mixed-Use - Acceptable Building Type', formValue: 'Mixed-Use' },
  { airtableColumn: 'Podium / Tower - Acceptable Building Type', formValue: 'Podium / Tower' },
  { airtableColumn: 'Historic / Renovated - Acceptable Building Type', formValue: 'Historic / Renovated' },
  { airtableColumn: 'Resort-Style Compound - Acceptable Building Type', formValue: 'Resort-Style Compound' }
];

// Acceptable Agreement Types: exact Airtable column titles → form value (same pattern as Project/Building Types).
const PROJECT_FIT_ACCEPTABLE_AGREEMENT_TYPES_COLUMNS = [
  { airtableColumn: 'Flexible/Open - Acceptable Agreements Type', formValue: 'Flexible/Open' },
  { airtableColumn: 'Franchise Only - Acceptable Agreements Type', formValue: 'Franchise Only' },
  { airtableColumn: 'Brand-Managed - Acceptable Agreements Type', formValue: 'Brand-Managed Only' },
  { airtableColumn: 'Third-Party Management Only - Acceptable Agreements Type', formValue: 'Third-Party Management Only' },
  { airtableColumn: 'Lease - Acceptable Agreements Type', formValue: 'Lease' },
  { airtableColumn: 'Joint Venture - Acceptable Agreements Type', formValue: 'Joint Venture' },
  { airtableColumn: 'Brand + Third-Party - Acceptable Agreements Type', formValue: 'Brand + Third-Party Mgmt. (Combined)' }
];

// Markets Focus – Priority: full exact Airtable column names (use as given).
const PROJECT_FIT_PRIORITY_MARKETS_COLUMNS = [
  { airtableColumn: 'Global - Priority Markets', formValue: 'Global - Priority Markets' },
  { airtableColumn: 'United States - Priority Markets', formValue: 'United States - Priority Markets' },
  { airtableColumn: 'Northeast (US) - Priority Markets', formValue: 'Northeast (US) - Priority Markets' },
  { airtableColumn: 'Southeast (US) - Priority Markets', formValue: 'Southeast (US) - Priority Markets' },
  { airtableColumn: 'Midwest (US) - Priority Markets', formValue: 'Midwest (US) - Priority Markets' },
  { airtableColumn: 'Southwest (US) - Priority Markets', formValue: 'Southwest (US) - Priority Markets' },
  { airtableColumn: 'West (US) - Priority Markets', formValue: 'West (US) - Priority Markets' },
  { airtableColumn: 'Pacific (US) - Priority Markets', formValue: 'Pacific (US) - Priority Markets' },
  { airtableColumn: 'Canada - Priority Markets', formValue: 'Canada - Priority Markets' },
  { airtableColumn: 'Mexico - Priority Markets', formValue: 'Mexico - Priority Markets' },
  { airtableColumn: 'Central America - Priority Markets', formValue: 'Central America - Priority Markets' },
  { airtableColumn: 'Caribbean - Priority Markets', formValue: 'Caribbean - Priority Markets' },
  { airtableColumn: 'South America - Priority Markets', formValue: 'South America - Priority Markets' },
  { airtableColumn: 'Latin America - Priority Markets', formValue: 'Latin America - Priority Markets' },
  { airtableColumn: 'Middle East - Priority Markets', formValue: 'Middle East - Priority Markets' },
  { airtableColumn: 'Western Europe - Priority Markets', formValue: 'Western Europe - Priority Markets' },
  { airtableColumn: 'Eastern Europe - Priority Markets', formValue: 'Eastern Europe - Priority Markets' },
  { airtableColumn: 'Southern Europe - Priority Markets', formValue: 'Southern Europe - Priority Markets' },
  { airtableColumn: 'Northern Europe - Priority Markets', formValue: 'Northern Europe - Priority Markets' },
  { airtableColumn: 'Nordic Countries - Priority Markets', formValue: 'Nordic Countries - Priority Markets' },
  { airtableColumn: 'United Kingdom - Priority Markets', formValue: 'United Kingdom - Priority Markets' },
  { airtableColumn: 'Other - Priority Markets', formValue: 'Other - Priority Markets' }
];

// Markets Focus – To Avoid: full exact Airtable column names (use as given).
const PROJECT_FIT_MARKETS_TO_AVOID_COLUMNS = [
  { airtableColumn: 'Global - Markets to Avoid', formValue: 'Global - Markets to Avoid' },
  { airtableColumn: 'United States (Broad) - Markets to Avoid', formValue: 'United States (Broad) - Markets to Avoid' },
  { airtableColumn: 'Northeast (US) - Markets to Avoid', formValue: 'Northeast (US) - Markets to Avoid' },
  { airtableColumn: 'Southeast (US) - Markets to Avoid', formValue: 'Southeast (US) - Markets to Avoid' },
  { airtableColumn: 'Midwest (US) - Markets to Avoid', formValue: 'Midwest (US) - Markets to Avoid' },
  { airtableColumn: 'Southwest (US) - Markets to Avoid', formValue: 'Southwest (US) - Markets to Avoid' },
  { airtableColumn: 'West (US) - Markets to Avoid', formValue: 'West (US) - Markets to Avoid' },
  { airtableColumn: 'Pacific (US) - Markets to Avoid', formValue: 'Pacific (US) - Markets to Avoid' },
  { airtableColumn: 'Canada - Markets to Avoid', formValue: 'Canada - Markets to Avoid' },
  { airtableColumn: 'Mexico - Markets to Avoid', formValue: 'Mexico - Markets to Avoid' },
  { airtableColumn: 'Central America - Markets to Avoid', formValue: 'Central America - Markets to Avoid' },
  { airtableColumn: 'Caribbean - Markets to Avoid', formValue: 'Caribbean - Markets to Avoid' },
  { airtableColumn: 'South America - Markets to Avoid', formValue: 'South America - Markets to Avoid' },
  { airtableColumn: 'Latin America (Broad) - Markets to Avoid', formValue: 'Latin America (Broad) - Markets to Avoid' },
  { airtableColumn: 'Middle East - Markets to Avoid', formValue: 'Middle East - Markets to Avoid' },
  { airtableColumn: 'Western Europe - Markets to Avoid', formValue: 'Western Europe - Markets to Avoid' },
  { airtableColumn: 'Eastern Europe - Markets to Avoid', formValue: 'Eastern Europe - Markets to Avoid' },
  { airtableColumn: 'Southern Europe - Markets to Avoid', formValue: 'Southern Europe - Markets to Avoid' },
  { airtableColumn: 'Northern Europe - Markets to Avoid', formValue: 'Northern Europe - Markets to Avoid' },
  { airtableColumn: 'Nordic Countries - Markets to Avoid', formValue: 'Nordic Countries - Markets to Avoid' },
  { airtableColumn: 'United Kingdom - Markets to Avoid', formValue: 'United Kingdom - Markets to Avoid' },
  { airtableColumn: 'Other (specify) - Markets to Avoid', formValue: 'Other (specify) - Markets to Avoid' }
];

// Acceptable Project Stages: full exact Airtable column names (use as given).
const PROJECT_FIT_ACCEPTABLE_PROJECT_STAGES_COLUMNS = [
  { airtableColumn: 'Land Under Control Only - Acceptable Project Stages', formValue: 'Land Under Control Only - Acceptable Project Stages' },
  { airtableColumn: 'Entitlements in Process - Acceptable Project Stages', formValue: 'Entitlements in Process - Acceptable Project Stages' },
  { airtableColumn: 'Fully Entitled - Acceptable Project Stages', formValue: 'Fully Entitled - Acceptable Project Stages' },
  { airtableColumn: 'Under Construction - Acceptable Project Stages', formValue: 'Under Construction - Acceptable Project Stages' },
  { airtableColumn: 'Stabilized Operating Asset - Acceptable Project Stages', formValue: 'Stabilized Operating Asset - Acceptable Project Stages' }
];

// Acceptable Owner Involvement Levels: one checkbox column per option (exact Airtable column names → form value).
const PROJECT_FIT_OWNER_INVOLVEMENT_COLUMNS = [
  { airtableColumn: 'Silent Investor - Owner Involvement', formValue: 'Silent Investor' },
  { airtableColumn: 'High-level Oversight Only - Owner Involvement', formValue: 'High-Level Oversight Only' },
  { airtableColumn: 'Hands-on in Operations - Owner Involvement', formValue: 'Hands-On in Operations' },
  { airtableColumn: 'Family in Key Staff Roles - Owner Involvement', formValue: 'Family in Key Staff Roles' }
];

// Brand Status Scenarios: one checkbox column per option (exact Airtable column names → form value).
const PROJECT_FIT_BRAND_STATUS_COLUMNS = [
  { airtableColumn: 'Brand Already Selected - Brand Status Scenarios', formValue: 'Brand Already Selected' },
  { airtableColumn: 'Shortlisted Brands - Brand Status Scenarios', formValue: 'Shortlisted Brands' },
  { airtableColumn: 'Open to Operator Recommendation Only - Brand Status Scenarios', formValue: 'Open to Operator Recommendation Only' },
  { airtableColumn: 'Brand-Agnostic - Brand Status Scenarios', formValue: 'Brand-Agnostic' }
];

// Owner Non-Negotiables (Types): one checkbox column per option (exact Airtable column names → form value). Title case.
const PROJECT_FIT_OWNER_NON_NEGOTIABLE_TYPES_COLUMNS = [
  { airtableColumn: 'Key Vendors / Contracts - Owner Non-Negotiables', formValue: 'Key Vendors / Contracts' },
  { airtableColumn: 'Family Employees in Hotel Roles - Owner Non-Negotiables', formValue: 'Family Employees in Hotel Roles' },
  { airtableColumn: 'Specific Design / Branding Elements - Owner Non-Negotiables', formValue: 'Specific Design / Branding Elements' },
  { airtableColumn: 'ADR / Positioning Philosophy - Owner Non-Negotiables', formValue: 'ADR / Positioning Philosophy' },
  { airtableColumn: 'Minimum Services / Amenities - Owner Non-Negotiables', formValue: 'Minimum Services / Amenities' },
  { airtableColumn: 'Other - Owner Non-Negotiables', formValue: 'Other' }
];

// Acceptable Capital Status at Engagement (Capital & Risk): one checkbox column per option (exact Airtable column names → form value).
const PROJECT_FIT_CAPITAL_STATUS_COLUMNS = [
  { airtableColumn: 'Equity and Debt Fully Committed - Capital & Risk', formValue: 'Equity and Debt Fully Committed' },
  { airtableColumn: 'Equity Committed, Debt in Process - Capital & Risk', formValue: 'Equity Committed, Debt in Process' },
  { airtableColumn: 'Equity in Process, Debt Not Started - Capital & Risk', formValue: 'Equity in Process, Debt Not Started' },
  { airtableColumn: 'Both Equity and Debt Still Being Raised - Capital & Risk', formValue: 'Both Equity and Debt Still Being Raised' }
];

// Canonical form for column name match: trim, nbsp→space, en/em dash→hyphen (Airtable UI may use these).
function canonicalColumnName(s) {
  return String(s).trim().replace(/\u00A0/g, ' ').replace(/\u2013|\u2014/g, '-');
}

// Get value from record.fields by exact column name. If not found, try trimmed match; then canonical match (nbsp + dash).
function getFieldValue(fields, exactColumnName) {
  if (!fields || typeof fields !== 'object') return undefined;
  const v = fields[exactColumnName];
  if (v !== undefined && v !== null && v !== '') return v;
  const want = String(exactColumnName).trim();
  const wantCanon = canonicalColumnName(exactColumnName);
  for (const key of Object.keys(fields)) {
    const k = String(key).trim();
    if (k === want) return fields[key];
    if (canonicalColumnName(key) === wantCanon) return fields[key];
  }
  return undefined;
}

// Find one record in a "Brand Setup - ..." table linked to the given brand (by link field or Brand Name)
async function findLinkedRecordByBrand(base, tableName, brandRecordId, brandName) {
  const escapedName = (brandName || '').replace(/"/g, '\\"');
  const linkFieldNames = ['Brand', 'Brand_Basic_ID', 'Brand Setup - Brand Basics', 'Brand Basics'];
  let records = [];
  for (const linkField of linkFieldNames) {
    try {
      const formula = `FIND("${brandRecordId}", ARRAYJOIN({${linkField}})) > 0`;
      records = await base(tableName).select({ filterByFormula: formula, maxRecords: 1 }).all();
      if (records.length > 0) {
        console.log(`${tableName} found via link field "${linkField}"`);
        return records[0];
      }
    } catch (_) {
      continue;
    }
  }
  if (escapedName) {
    records = await base(tableName).select({ filterByFormula: `{Brand Name} = "${escapedName}"`, maxRecords: 1 }).all();
    if (records.length > 0) {
      console.log(`${tableName} found via Brand Name`);
      return records[0];
    }
  }
  const allRecords = await base(tableName).select({ maxRecords: 100 }).all();
  for (const rec of allRecords) {
    const fields = rec.fields || {};
    for (const key of Object.keys(fields)) {
      const val = fields[key];
      if (Array.isArray(val) && val.length > 0 && typeof val[0] === 'string' && val[0].startsWith('rec') && val.includes(brandRecordId)) {
        console.log(`${tableName} found via fallback (link field "${key}")`);
        return rec;
      }
    }
  }
  return null;
}

// Normalize Airtable single-select / multi-select to display string
function valueToStr(v) {
  if (v == null) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'object' && !Array.isArray(v) && v !== null) {
    if (typeof v.name === 'string') return v.name.trim();
    if (typeof v.value === 'string') return v.value.trim();
  }
  if (Array.isArray(v) && v.length > 0) {
    const parts = v.map((item) => {
      if (typeof item === 'string') return item.trim();
      if (item && typeof item === 'object' && typeof item.name === 'string') return item.name.trim();
      return '';
    }).filter(Boolean);
    return parts.join(', ');
  }
  return '';
}

// Convert a record's fields to display-friendly key-value (skip Brand link, IDs)
function fieldsToDisplayObject(fields) {
  if (!fields || typeof fields !== 'object') return {};
  const skipKeys = new Set(['Brand', 'Brand Name', 'Record_ID', 'Legal_Terms_ID', 'User_Record_ID', 'User_ID']);
  const out = {};
  for (const [key, value] of Object.entries(fields)) {
    if (skipKeys.has(key)) continue;
    if (key.includes('_ID') && key !== 'Brand_Basics_ID') continue;
    if (value == null || value === '') continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (Array.isArray(value) && typeof value[0] === 'string' && value[0].startsWith('rec')) continue; // skip link arrays
    if (typeof value === 'boolean') { out[key] = value ? 'Yes' : 'No'; continue; }
    if (typeof value === 'number' && !Number.isNaN(value)) { out[key] = String(value); continue; }
    const str = valueToStr(value);
    if (str && str.trim() !== '') out[key] = str;
  }
  return out;
}

// Possible Airtable column names for the logo attachment (PNG you upload manually)
const LOGO_FIELD_NAMES = [
  'Logo', 'Brand Logo', 'Logo Image', 'Brand Logo Image',
  'Image', 'Brand Image', 'Icon', 'Logo (PNG)', 'Attachments'
];

function getUrlFromAttachment(att) {
  if (!att || typeof att !== 'object') return '';
  const url =
    att.url ||
    (att.thumbnails && att.thumbnails.large && att.thumbnails.large.url) ||
    (att.thumbnails && att.thumbnails.small && att.thumbnails.small.url);
  if (url && typeof url === 'string' && url.startsWith('http')) return url;
  // Fallback: any string property that looks like an image URL
  for (const v of Object.values(att)) {
    if (typeof v === 'string' && v.startsWith('http')) return v;
    if (v && typeof v === 'object' && v.url && typeof v.url === 'string') return v.url;
  }
  return '';
}

function extractLogoUrl(fields) {
  if (!fields || typeof fields !== 'object') return '';
  // 1) Try known names first (exact "Logo" match for your column)
  for (const name of LOGO_FIELD_NAMES) {
    const logoField = fields[name];
    if (logoField && Array.isArray(logoField)) {
      for (let i = 0; i < logoField.length; i++) {
        const url = getUrlFromAttachment(logoField[i]);
        if (url) return url;
      }
    }
    if (logoField && typeof logoField === 'string' && logoField.startsWith('http')) return logoField;
  }
  // 2) Scan for any field whose name contains "logo" or "image" or "icon" and value is attachment array
  const keyLower = (k) => k.toLowerCase();
  for (const key of Object.keys(fields)) {
    const k = keyLower(key);
    if (!k.includes('logo') && !k.includes('image') && !k.includes('icon')) continue;
    const val = fields[key];
    if (val && Array.isArray(val) && val[0]) {
      const url = getUrlFromAttachment(val[0]);
      if (url) return url;
    }
    if (val && typeof val === 'string' && val.startsWith('http')) return val;
  }
  // 3) Last resort: use the first field that looks like an Airtable attachment (array of { url })
  for (const key of Object.keys(fields)) {
    const val = fields[key];
    if (val && Array.isArray(val) && val[0] && val[0] && typeof val[0] === 'object' && (val[0].url || (val[0].thumbnails && val[0].thumbnails && val[0].thumbnails.large))) {
      const url = getUrlFromAttachment(val[0]);
      if (url) return url;
    }
  }
  return '';
}

// Internal: fetch single-select (or multi-select) choice names for a table field from Airtable schema.
async function getTableFieldChoiceNames(tableName, fieldName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return [];
  try {
    const schemaRes = await fetch(
      `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    if (!schemaRes.ok) return [];
    const schemaData = await schemaRes.json();
    const table = (schemaData.tables || []).find((t) => t.name === tableName);
    if (!table) return [];
    const field = (table.fields || []).find(
      (f) => f.name === fieldName && (f.type === 'singleSelect' || f.type === 'multipleSelects')
    );
    return field?.options?.choices?.map((c) => c.name) || [];
  } catch (_) {
    return [];
  }
}

// Exported for use by my-deals (deal status dropdown options match brand setup status).
export async function getBrandStatusChoiceNames() {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error('Airtable API credentials not configured');
    const schemaRes = await fetch(
      `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    if (!schemaRes.ok) throw new Error('Schema fetch failed');
    const schemaData = await schemaRes.json();
    const table = (schemaData.tables || []).find((t) => t.name === F.brandBasics.table);
    if (!table) throw new Error('Brand Basics table not found in schema');
  const statusField = (table.fields || []).find((f) =>
    f.name === F.brandBasics.status && (f.type === 'singleSelect' || f.type === 'multipleSelects')
  );
  const choices = statusField?.options?.choices?.map((c) => c.name) || [];
  return { choices, isMultiple: statusField?.type === 'multipleSelects' };
}

// Get Brand Status single-select options from Airtable schema (stays in sync with Airtable)
export async function getBrandStatusOptions(req, res) {
  try {
    const { choices } = await getBrandStatusChoiceNames();
    res.json({ success: true, options: choices });
  } catch (error) {
    console.error('Error fetching Brand Status options:', error);
    res.status(500).json({ success: false, error: error.message, options: [] });
  }
}

/** GET /api/brand-library/operational-support?brandId=167 - For BDD match score (uses server credentials) */
export async function getOperationalSupportByBrandId(req, res) {
  try {
    const brandId = (req.query?.brandId ?? req.query?.brand_id ?? '').toString().trim();
    if (!brandId) {
      return res.status(400).json({ success: false, error: 'brandId required' });
    }
    const base = getBase();
    const escaped = String(brandId).replace(/'/g, "\\'").replace(/\\/g, '\\\\');
    const formula = `SEARCH("${escaped}", {Brand})`;
    const records = await base(F.operationalSupport.table)
      .select({ filterByFormula: formula, maxRecords: 1 })
      .firstPage();
    const fields = records && records[0] ? records[0].fields : {};
    res.json({ success: true, fields });
  } catch (err) {
    console.error('[brand-library] operational-support error:', err.message);
    res.status(500).json({ success: false, error: err.message, fields: {} });
  }
}

// Get all brands for the library listing (use REST API so we get exact field names e.g. Brand Architecture)
export async function getBrandLibraryBrands(req, res) {
  try {
    const allStatuses = req.query?.allStatuses === '1' || req.query?.allStatuses === 'true';
    console.log('[Brand Library] API called: fetching brands...', allStatuses ? '(all statuses)' : '(Active only)');
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error('Airtable API credentials not configured');

    const tableName = encodeURIComponent(F.brandBasics.table);
    const useFilter = !allStatuses;
    const formula = encodeURIComponent("FIND('Active', {Brand Status}) > 0");

    // Fetch all records from Airtable REST API (paginated)
    let allRecords = [];
    let offset = null;
    do {
      let url = `https://api.airtable.com/v0/${baseId}/${tableName}?pageSize=100`;
      if (useFilter) url += `&filterByFormula=${formula}`;
      if (offset) url += '&offset=' + encodeURIComponent(offset);
      const pageRes = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
      const pageData = await pageRes.json();
      if (pageData.error) throw new Error(pageData.error.message || 'Airtable API error');
      allRecords = allRecords.concat(pageData.records || []);
      offset = pageData.offset || null;
    } while (offset);

    // Same normalizer for all select-style fields (string or choice object { id, name, color? })
    function valueToStr(v) {
      if (v == null) return '';
      if (typeof v === 'string') return v.trim();
      if (typeof v === 'object' && !Array.isArray(v) && v !== null) {
        if (typeof v.name === 'string') return v.name.trim();
        if (typeof v.value === 'string') return v.value.trim();
      }
      if (Array.isArray(v) && v.length > 0) {
        const first = v[0];
        if (typeof first === 'string') return first.trim();
        if (first && typeof first === 'object' && typeof first.name === 'string') return first.name.trim();
      }
      return '';
    }

    // Airtable omits empty fields from the response — so scan all records to find the architecture column name
    const allFieldNames = new Set();
    allRecords.forEach(rec => { Object.keys(rec.fields || {}).forEach(k => allFieldNames.add(k)); });
    const architectureFieldKey = [...allFieldNames].find(k => k.toLowerCase().includes('architecture')) || null;
    const firstFields = allRecords[0] && allRecords[0].fields ? allRecords[0].fields : {};

    const brandList = allRecords.map(rec => {
      const fields = rec.fields || {};
      const archVal = architectureFieldKey ? valueToStr(fields[architectureFieldKey]) : valueToStr(fields['Brand Architecture'] ?? fields[F.brandBasics.architecture]);
      return {
        id: rec.id,
        name: (fields[F.brandBasics.name] || '').toString().trim() || 'Unknown Brand',
        logo: extractLogoUrl(fields),
        parentCompany: (fields[F.brandBasics.parentCompany] || '').toString().trim(),
        chainScale: valueToStr(fields[F.brandBasics.chainScale]),
        brandModel: valueToStr(fields[F.brandBasics.brandModel]),
        serviceModel: valueToStr(fields[F.brandBasics.serviceModel]),
        architecture: archVal,
        status: (fields[F.brandBasics.status] || '').toString().trim(),
        positioning: (fields[F.brandBasics.positioning] || '').toString().trim(),
        tagline: (fields[F.brandBasics.tagline] || '').toString().trim()
      };
    });

    brandList.sort((a, b) => (a.name || '').localeCompare(b.name || '', undefined, { sensitivity: 'base' }));

    const parentCompanies = [...new Set(brandList.map(b => (b.parentCompany || '').trim()).filter(Boolean))].sort();
    const chainScales = [...new Set(brandList.map(b => (b.chainScale || '').trim()).filter(Boolean))].sort();
    const brandModels = [...new Set(brandList.map(b => (b.brandModel || '').trim()).filter(Boolean))].sort();
    const serviceModels = [...new Set(brandList.map(b => (b.serviceModel || '').trim()).filter(Boolean))].sort();
    const architectures = [...new Set(brandList.map(b => (b.architecture || '').trim()).filter(Boolean))].sort();

    const withArch = brandList.filter(b => (b.architecture || '').trim()).length;
    console.log('[Brand Library] Architecture field key:', architectureFieldKey, '| brands with architecture:', withArch, '/', brandList.length, '| options:', architectures);

    const filterOptions = {
      parentCompanies,
      chainScales,
      brandModels,
      serviceModels,
      architectures
    };

    const payload = {
      success: true,
      brands: brandList,
      totalCount: brandList.length,
      filterOptions
    };
    if (allRecords.length > 0) {
      payload._debug = { airtableColumnLabels: [...allFieldNames].sort() };
    }
    res.json(payload);

  } catch (error) {
    console.error("Error fetching brands:", error);
    res.status(500).json({ 
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Get brands grouped by parent company for operator intake form
export async function getBrandsGroupedByParentCompany(req, res) {
  try {
    const base = getBase();
    const brands = await base(F.brandBasics.table)
      .select({
        fields: [
          F.brandBasics.name,
          F.brandBasics.parentCompany,
          F.brandBasics.status
        ],
        filterByFormula: "FIND('Active', {Brand Status}) > 0",
        maxRecords: 500
      })
      .all();

    // Group brands by parent company
    const brandsByParentCompany = {};
    
    brands.forEach(brand => {
      const brandName = brand.fields[F.brandBasics.name];
      const parentCompany = brand.fields[F.brandBasics.parentCompany] || 'Other';
      
      if (!brandName) return; // Skip if no brand name
      
      if (!brandsByParentCompany[parentCompany]) {
        brandsByParentCompany[parentCompany] = [];
      }
      
      brandsByParentCompany[parentCompany].push({
        id: brand.id,
        name: brandName
      });
    });

    // Convert to array format and sort parent companies alphabetically
    const groupedBrands = Object.keys(brandsByParentCompany)
      .sort()
      .map(parentCompany => ({
        parentCompany: parentCompany,
        brands: brandsByParentCompany[parentCompany].sort((a, b) => 
          a.name.localeCompare(b.name)
        )
      }));

    res.json({
      success: true,
      brandsByParentCompany: groupedBrands,
      totalCount: brands.length
    });

  } catch (error) {
    console.error("Error fetching brands grouped by parent company:", error);
    res.status(500).json({ 
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Get detailed brand information for a specific brand
export async function getBrandLibraryBrandById(req, res) {
  try {
    const { brandId } = req.query;
    
    if (!brandId) {
      return res.status(400).json({ error: "Brand ID is required" });
    }

    console.log(`Fetching brand with identifier: ${brandId}`);

    // Get brand basics
    const base = getBase();
    let brandRecord;
    
    // Decode the identifier first
    const decodedId = decodeURIComponent(brandId);
    console.log(`Decoded identifier: ${decodedId}`);
    
    // Check if it looks like a record ID (starts with 'rec')
    if (decodedId.startsWith('rec')) {
      try {
        brandRecord = await base(F.brandBasics.table).find(decodedId);
        console.log(`Found brand by record ID: ${brandRecord.fields[F.brandBasics.name]}`);
      } catch (error) {
        console.log(`Record ID lookup failed, trying by name: ${error.message}`);
        // Fall through to name lookup
      }
    }
    
    // If not found by ID, try by name
    if (!brandRecord) {
      const brandName = decodedId;
      console.log(`Searching for brand by name: "${brandName}"`);
      const records = await base(F.brandBasics.table)
        .select({
          filterByFormula: `{Brand Name} = "${brandName.replace(/"/g, '\\"')}"`,
          maxRecords: 1
        })
        .all();
      
      if (records.length === 0) {
        console.log(`Brand not found: ${brandName}`);
        return res.status(404).json({ 
          success: false,
          error: `Brand not found: ${brandName}` 
        });
      }
      brandRecord = records[0];
      console.log(`Found brand by name: ${brandRecord.fields[F.brandBasics.name]}`);
    }

    const brandFields = brandRecord.fields;
    const brandName = brandFields[F.brandBasics.name];
    const brandRecordId = brandRecord.id;
    const loadWarnings = [];

    // Get brand footprint data (try link field with ARRAYJOIN, then Brand Name text)
    let footprintData = {};
    try {
      const escapedName = (brandName || '').replace(/"/g, '\\"');
      // Link from Brand Setup - Brand Footprint back to Brand Setup - Brand Basics (field may be "Brand" or "Brand_Basic_ID")
      const linkFieldNames = ['Brand', 'Brand_Basic_ID', 'Brand Setup - Brand Basics', 'Brand Basics'];
      let footprintRecords = [];

      for (const linkField of linkFieldNames) {
        try {
          // Linked record fields need ARRAYJOIN in formula to filter by record ID
          const formula = `FIND("${brandRecordId}", ARRAYJOIN({${linkField}})) > 0`;
          footprintRecords = await base(F.brandFootprint.table)
            .select({ filterByFormula: formula, maxRecords: 1 })
            .all();
          if (footprintRecords.length > 0) {
            console.log(`Footprint found via link field "${linkField}"`);
            break;
          }
        } catch (_) {
          // Field may not exist; try next
          continue;
        }
      }

      if (footprintRecords.length === 0 && escapedName) {
        footprintRecords = await base(F.brandFootprint.table)
          .select({
            filterByFormula: `{Brand Name} = "${escapedName}"`,
            maxRecords: 1
          })
          .all();
        if (footprintRecords.length > 0) console.log('Footprint found via Brand Name');
      }

      // Fallback: fetch records and find by link field containing brand record ID (formula can fail on some bases)
      if (footprintRecords.length === 0) {
        const allFootprint = await base(F.brandFootprint.table).select({ maxRecords: 100 }).all();
        for (const rec of allFootprint) {
          const fields = rec.fields || {};
          for (const key of Object.keys(fields)) {
            const val = fields[key];
            if (Array.isArray(val) && val.length > 0 && typeof val[0] === 'string' && val[0].startsWith('rec')) {
              if (val.includes(brandRecordId)) {
                footprintRecords = [rec];
                console.log('Footprint found via fallback (link field "' + key + '")');
                break;
              }
            }
          }
          if (footprintRecords.length > 0) break;
        }
      }

      if (footprintRecords.length > 0) {
        const footprint = footprintRecords[0].fields;
        // Discover region prefixes from actual Airtable columns (e.g. "AM", "Americas", "CALA")
        const allKeys = Object.keys(footprint);
        const regionPrefixToStandard = { AM: 'AM', CALA: 'CALA', EU: 'EU', MEA: 'MEA', APAC: 'APAC' };
        [['Americas', 'AM'], ['North America', 'AM'], ['NA', 'AM'], ['CALA', 'CALA'], ['EU', 'EU'], ['Europe', 'EU'], ['MEA', 'MEA'], ['Middle East', 'MEA'], ['APAC', 'APAC'], ['Asia Pacific', 'APAC']].forEach(([alt, std]) => {
          if (allKeys.some(k => k.startsWith(alt + ' '))) regionPrefixToStandard[alt] = std;
        });
        const standardRegions = ['AM', 'CALA', 'EU', 'MEA', 'APAC'];

        // Helper: get value from footprint trying standard region then known alternate prefixes
        const getFootprintVal = (standardRegion, suffix) => {
          const tryKeys = [`${standardRegion} ${suffix}`];
          Object.entries(regionPrefixToStandard).forEach(([prefix, std]) => {
            if (std === standardRegion && prefix !== standardRegion) tryKeys.push(`${prefix} ${suffix}`);
          });
          for (const k of tryKeys) {
            const v = footprint[k];
            if (v !== undefined && v !== null && v !== '') return parseNumber(v);
          }
          return 0;
        };

        // Pipeline: Airtable may use "Pipeline Hotel" / "Pipeline Rooms" (not only New Build + Conversion)
        const getPipelineVal = (region) => ({
          hotels: getFootprintVal(region, 'Pipeline Hotel') || getFootprintVal(region, 'Pipeline Hotels'),
          rooms: getFootprintVal(region, 'Pipeline Rooms') || getFootprintVal(region, 'Pipeline Room')
        });

        let totalExistingHotels = 0;
        let totalExistingRooms = 0;
        let totalNewBuildHotels = 0;
        let totalConversionHotels = 0;
        let totalNewBuildRooms = 0;
        let totalConversionRooms = 0;
        let totalManagedHotels = 0;
        let totalFranchisedHotels = 0;
        let totalManagedRooms = 0;
        let totalFranchisedRooms = 0;

        standardRegions.forEach(region => {
          const existingHotels = getFootprintVal(region, 'Existing Hotel') || getFootprintVal(region, 'Existing Hotels');
          const existingRooms = getFootprintVal(region, 'Existing Rooms') || getFootprintVal(region, 'Existing Room');
          const newBuildHotels = getFootprintVal(region, 'New Build Hotel') || getFootprintVal(region, 'New Build Hotels');
          const conversionHotels = getFootprintVal(region, 'Conversion Hotel') || getFootprintVal(region, 'Conversion Hotels');
          const newBuildRooms = getFootprintVal(region, 'New Build Rooms') || getFootprintVal(region, 'New Build Room');
          const conversionRooms = getFootprintVal(region, 'Conversion Rooms') || getFootprintVal(region, 'Conversion Room');
          const managedHotels = getFootprintVal(region, 'Managed Hotel') || getFootprintVal(region, 'Managed Hotels');
          const franchisedHotels = getFootprintVal(region, 'Franchised Hotel') || getFootprintVal(region, 'Franchised Hotels');
          const managedRooms = getFootprintVal(region, 'Managed Rooms') || getFootprintVal(region, 'Managed Room');
          const franchisedRooms = getFootprintVal(region, 'Franchised Rooms') || getFootprintVal(region, 'Franchised Room');

          totalExistingHotels += existingHotels;
          totalExistingRooms += existingRooms;
          totalNewBuildHotels += newBuildHotels;
          totalConversionHotels += conversionHotels;
          totalNewBuildRooms += newBuildRooms;
          totalConversionRooms += conversionRooms;
          totalManagedHotels += managedHotels;
          totalFranchisedHotels += franchisedHotels;
          totalManagedRooms += managedRooms;
          totalFranchisedRooms += franchisedRooms;
        });

        // Calculate percentages
        const totalHotels = totalExistingHotels;
        const totalRooms = totalExistingRooms;
        
        // New Build vs Conversion - Units
        const newBuildPercent = totalHotels > 0 ? (totalNewBuildHotels / totalHotels) * 100 : 0;
        const conversionPercent = totalHotels > 0 ? (totalConversionHotels / totalHotels) * 100 : 0;
        
        // New Build vs Conversion - Rooms
        const newBuildRoomsPercent = totalRooms > 0 ? (totalNewBuildRooms / totalRooms) * 100 : 0;
        const conversionRoomsPercent = totalRooms > 0 ? (totalConversionRooms / totalRooms) * 100 : 0;
        
        // Managed vs Franchised - Units
        const managedPercent = totalHotels > 0 ? (totalManagedHotels / totalHotels) * 100 : 0;
        const franchisedPercent = totalHotels > 0 ? (totalFranchisedHotels / totalHotels) * 100 : 0;
        
        // Managed vs Franchised - Rooms
        const managedRoomsPercent = totalRooms > 0 ? (totalManagedRooms / totalRooms) * 100 : 0;
        const franchisedRoomsPercent = totalRooms > 0 ? (totalFranchisedRooms / totalRooms) * 100 : 0;

        // Get location distribution (Systemwide Existing Unit Distribution by Location Type %)
        const locationFields = [
          'Urban - Existing Systemwide Location',
          'Suburban - Existing Systemwide Location',
          'Resort - Existing Systemwide Location',
          'Airport - Existing Systemwide Location',
          'Small Metro - Existing Systemwide Location',
          'Interstate - Existing Systemwide Location'
        ];

        const locationDistribution = {};
        locationFields.forEach(field => {
          const value = footprint[field];
          if (value !== null && value !== undefined && value !== '') {
            const locationType = field.replace(' - Existing Systemwide Location', '');
            locationDistribution[locationType] = parsePercent(value);
          }
        });

        // Get regional distribution: use Pipeline Hotel/Rooms when present, else New Build + Conversion
        const regionalDistribution = {};
        standardRegions.forEach(region => {
          const existingHotels = getFootprintVal(region, 'Existing Hotel') || getFootprintVal(region, 'Existing Hotels') ||
            parseNumber(footprint[`${region} Total Distribution Hotel`]);
          const existingRooms = getFootprintVal(region, 'Existing Rooms') || getFootprintVal(region, 'Existing Room') ||
            parseNumber(footprint[`${region} Total Distribution Rooms`]);
          const pipelineFromCol = getPipelineVal(region);
          const newBuildHotels = getFootprintVal(region, 'New Build Hotel') || getFootprintVal(region, 'New Build Hotels');
          const conversionHotels = getFootprintVal(region, 'Conversion Hotel') || getFootprintVal(region, 'Conversion Hotels');
          const newBuildRooms = getFootprintVal(region, 'New Build Rooms') || getFootprintVal(region, 'New Build Room');
          const conversionRooms = getFootprintVal(region, 'Conversion Rooms') || getFootprintVal(region, 'Conversion Room');
          const pipelineHotels = pipelineFromCol.hotels || (newBuildHotels + conversionHotels);
          const pipelineRooms = pipelineFromCol.rooms || (newBuildRooms + conversionRooms);
          const managedHotels = getFootprintVal(region, 'Managed Hotel') || getFootprintVal(region, 'Managed Hotels');
          const franchisedHotels = getFootprintVal(region, 'Franchised Hotel') || getFootprintVal(region, 'Franchised Hotels');
          const managedRooms = getFootprintVal(region, 'Managed Rooms') || getFootprintVal(region, 'Managed Room');
          const franchisedRooms = getFootprintVal(region, 'Franchised Rooms') || getFootprintVal(region, 'Franchised Room');

          regionalDistribution[region] = {
            hotels: existingHotels,
            rooms: existingRooms,
            pipelineHotels,
            pipelineRooms,
            percentage: totalHotels > 0 ? (existingHotels / totalHotels) * 100 : 0,
            roomsPercentage: totalRooms > 0 ? (existingRooms / totalRooms) * 100 : 0,
            newBuildHotels,
            conversionHotels,
            newBuildRooms,
            conversionRooms,
            newBuildHotelsPercent: existingHotels > 0 ? (newBuildHotels / existingHotels) * 100 : 0,
            conversionHotelsPercent: existingHotels > 0 ? (conversionHotels / existingHotels) * 100 : 0,
            newBuildRoomsPercent: existingRooms > 0 ? (newBuildRooms / existingRooms) * 100 : 0,
            conversionRoomsPercent: existingRooms > 0 ? (conversionRooms / existingRooms) * 100 : 0,
            managedHotels,
            franchisedHotels,
            managedRooms,
            franchisedRooms,
            managedHotelsPercent: existingHotels > 0 ? (managedHotels / existingHotels) * 100 : 0,
            franchisedHotelsPercent: existingHotels > 0 ? (franchisedHotels / existingHotels) * 100 : 0,
            managedRoomsPercent: existingRooms > 0 ? (managedRooms / existingRooms) * 100 : 0,
            franchisedRoomsPercent: existingRooms > 0 ? (franchisedRooms / existingRooms) * 100 : 0
          };
        });

        // Preload form values using same mapping as write: form field name → Airtable column
        const LOCATION_TYPE_FORM_NAMES = ['locationTypeUrban', 'locationTypeSuburban', 'locationTypeResort', 'locationTypeAirport', 'locationTypeSmallMetro', 'locationTypeInterstate'];
        const EXPERIENCE_PERCENT_FORM_NAMES = ['newBuildExperience', 'conversionExperience', 'turnaroundExperience', 'renovationExperience', 'typicalManagedPercent', 'typicalFranchisedPercent'];
        const FOOTPRINT_TEXT_FIELDS = ['figuresAsOf', 'specificMarkets'];
        const formValues = {};
        for (const { form, airtable } of FOOTPRINT_FORM_TO_AIRTABLE) {
          let val = footprint[airtable];
          let out;
          if (val === undefined || val === null || val === '') {
            out = '';
          } else if (FOOTPRINT_TEXT_FIELDS.includes(form)) {
            out = typeof val === 'string' ? val.trim() : String(val);
            // Figures as of: ensure YYYY-MM-DD for <input type="date"> (Airtable may return ISO datetime)
            if (form === 'figuresAsOf' && out && out.length >= 10) {
              const dateOnly = out.indexOf('T') > 0 ? out.slice(0, out.indexOf('T')) : out.slice(0, 10);
              if (/^\d{4}-\d{2}-\d{2}$/.test(dateOnly)) out = dateOnly;
            }
          } else {
            out = parseNumber(val);
            // Display decimals (0–1) as percentages (e.g. 0.33 → 33)
            if (LOCATION_TYPE_FORM_NAMES.includes(form) && typeof out === 'number' && out >= 0 && out <= 1) {
              out = out * 100;
            } else if (EXPERIENCE_PERCENT_FORM_NAMES.includes(form) && typeof out === 'number' && out >= 0 && out <= 1) {
              out = out * 100;
            }
          }
          formValues[form] = out;
        }

        footprintData = {
          formValues,
          totalExistingHotels,
          totalExistingRooms,
          totalNewBuildHotels,
          totalConversionHotels,
          totalNewBuildRooms,
          totalConversionRooms,
          newBuildPercent: Math.round(newBuildPercent * 10) / 10,
          conversionPercent: Math.round(conversionPercent * 10) / 10,
          newBuildRoomsPercent: Math.round(newBuildRoomsPercent * 10) / 10,
          conversionRoomsPercent: Math.round(conversionRoomsPercent * 10) / 10,
          totalManagedHotels,
          totalFranchisedHotels,
          totalManagedRooms,
          totalFranchisedRooms,
          managedPercent: Math.round(managedPercent * 10) / 10,
          franchisedPercent: Math.round(franchisedPercent * 10) / 10,
          managedRoomsPercent: Math.round(managedRoomsPercent * 10) / 10,
          franchisedRoomsPercent: Math.round(franchisedRoomsPercent * 10) / 10,
          locationDistribution,
          regionalDistribution
        };
      }
    } catch (error) {
      console.error("Error fetching brand footprint:", error);
      loadWarnings.push("Brand Footprint");
    }

    // Get Loyalty & Commercial data — always include every form key so the UI can prefill (use '' when missing)
    const formValuesDefaults = {};
    for (const { form } of LOYALTY_COMMERCIAL_FORM_TO_AIRTABLE) {
      formValuesDefaults[form] = '';
    }
    let loyaltyCommercialData = { formValues: { ...formValuesDefaults } };
    try {
      const lcRec = await findLinkedRecordByBrand(base, F.loyaltyCommercial.table, brandRecordId, brandName);
      if (lcRec) {
        const lc = lcRec.fields;
        const LC_PERCENT = ['typicalLoyaltyRoomsPercent', 'typicalDirectBookingPercent', 'typicalOTAReliancePercent', 'otaCommissionPercent', 'crsUsagePercent', 'websiteAppConvRatesPercent'];
        const notLinked = [];
        for (const { form, airtable } of LOYALTY_COMMERCIAL_FORM_TO_AIRTABLE) {
          let val = getFieldValue(lc, airtable);
          if (val === undefined || val === null || val === '') {
            notLinked.push({ form, airtable });
          }
          // Airtable currency can be number or object; normalize to number for display
          if (val != null && typeof val === 'object' && typeof val.value === 'number') val = val.value;
          let out;
          if (val === undefined || val === null || val === '') {
            out = '';
          } else if (form === 'typicalLoyaltyProgramName') {
            out = typeof val === 'string' ? val.trim() : String(val);
          } else {
            out = parseNumber(val);
            if (LC_PERCENT.includes(form) && typeof out === 'number' && out >= 0 && out <= 1) {
              out = Math.round(out * 100 * 100) / 100; // display as 0–100, max 2 decimals
            } else if (typeof out === 'number' && !Number.isInteger(out)) {
              out = Math.round(out * 100) / 100; // other numbers: 2 decimal places
            }
          }
          loyaltyCommercialData.formValues[form] = out;
        }
        if (notLinked.length) {
          loyaltyCommercialData.unlinkedFields = notLinked;
        }
      }
    } catch (error) {
      console.error("Error fetching Loyalty & Commercial:", error);
      loadWarnings.push("Loyalty & Commercial");
    }

    // Get fee structure using same mapping as form (FEE_FORM_TO_AIRTABLE: form ID → Airtable columns)
    // Try Brand record's "Brand Setup - Fee Structure" link first, then search Fee Structure by Brand link
    let feeStructureData = {};
    let feeRecUsed = null;
    try {
      let feeRec = null;
      const fsLink = brandFields["Brand Setup - Fee Structure"];
      if (Array.isArray(fsLink) && fsLink.length > 0 && typeof fsLink[0] === "string" && fsLink[0].startsWith("rec")) {
        try {
          feeRec = await base(F.feeStructure.table).find(fsLink[0]);
          if (process.env.DEBUG_BRAND_LIBRARY === "1") console.log("[Brand Library] Fee Structure found via Brand Setup - Fee Structure link");
        } catch (_) { /* link may point to deleted record */ }
      }
      if (!feeRec) feeRec = await findLinkedRecordByBrand(base, F.feeStructure.table, brandRecordId, brandName);
      feeRecUsed = feeRec;
      if (feeRec) {
        const fee = feeRec.fields;
        const getFirst = (keys) => {
          for (const k of keys) {
            const v = getFieldValue(fee, k);
            if (v !== undefined && v !== null && v !== '') return v;
          }
          return '';
        };
        for (const [formId, airtableCols] of Object.entries(FEE_FORM_TO_AIRTABLE)) {
          let val = getFirst(airtableCols);
          if (Array.isArray(val) && val.length > 0) val = val[0];
          if (val && typeof val === 'object' && typeof val.name === 'string') val = val.name;
          // Display decimals (0–1) as percentage (e.g. 0.05 → 5)
          if (FEE_PERCENT_FORM_NAMES.includes(formId) && val !== undefined && val !== null && val !== '') {
            const num = typeof val === 'number' ? val : parseFloat(String(val).replace(/,/g, ''));
            if (typeof num === 'number' && !Number.isNaN(num)) {
              val = (num > 0 && num <= 1) ? String(Math.round(num * 100 * 10) / 10) : String(val);
            }
          }
          // Normalize Basis/select values so they match dropdown option values
          if ((formId.endsWith('Basis') || formId === 'typicalApplicationFeeBasis') && val) {
            val = normalizeFeeBasisValue(val);
          }
          feeStructureData[formId] = val !== undefined && val !== null && val !== '' ? (typeof val === 'string' ? val.trim() : val) : '';
        }
      }
    } catch (error) {
      console.error("Error fetching fee structure:", error);
      loadWarnings.push("Fee Structure");
    }
    if (process.env.DEBUG_BRAND_LIBRARY === "1") {
      const res = { min: feeStructureData.typicalReservationFeeMin, max: feeStructureData.typicalReservationFeeMax, basis: feeStructureData.typicalReservationFeeBasis };
      console.log("[Brand Library] Fee Structure reservation fee prefill:", JSON.stringify(res));
      const fsFromBrand = brandFields["Brand Setup - Fee Structure"];
      const keys = (feeRecUsed || {}).fields ? Object.keys(feeRecUsed.fields).filter((k) => /reservation|distribution|booking/i.test(k)) : [];
      console.log("[Brand Library] Fee Structure found:", !!feeRecUsed, "| Brand→Fee link:", Array.isArray(fsFromBrand) && fsFromBrand[0] ? fsFromBrand[0] : "none", "| reservation-related keys:", keys);
    }

    // Get brand standards data (Brand Setup page: amenity checkboxes Lobby, Bar, Fitness, Pool, Meeting/Event, Co-working, Grab & Go)
    let brandStandardsData = {};
    try {
      const stdRec = await findLinkedRecordByBrand(base, F.brandStandards.table, brandRecordId, brandName);
      if (stdRec) {
        const s = stdRec.fields;
        const truthy = (v) => v !== undefined && v !== null && v !== '' && String(v).toLowerCase() !== 'no' && v !== false;
        const getStd = (col) => getFieldValue(s, col) ?? '';
        brandStandardsData = {
          lobby: s['Lobby'] ?? s['Lobby Required'] ?? '',
          lobbyDescription: s['Lobby Description'] ?? '',
          barBeverage: s['Bar / Beverage'] ?? s['Bar or Beverage'] ?? s['Bar/Beverage'] ?? '',
          fitnessCenter: s['Fitness Center'] ?? s['Fitness'] ?? '',
          pool: s['Pool'] ?? '',
          onsiteParking: s['Onsite Parking'] ?? '',
          meetingEventSpace: s['Meeting / Event Space'] ?? s['Meeting/Event Space'] ?? s['Meeting & Event Space'] ?? '',
          coworking: s['Co-Working Space'] ?? s['Co-working'] ?? s['Coworking'] ?? '',
          grabGo: s['Grab & Go'] ?? s['Grab and Go'] ?? s['Grab & Go or Marketplace'] ?? '',
          minimumRoomSize: s['Minimum Room Size (sq ft)'] ?? s['Minimum Room Size'] ?? '',
          minimumRoomSizeMeters: s['Minimum Room Size (sq m)'] ?? '',
          brandStandards: s['Brand Standards'] ?? '',
          brandFbOutletsRequired: getStd('F&B Outlets Required'),
          brandFbOutletsCount: getStd('Typical Number of F&B Outlets'),
          brandFbProgramType: getStd('Typical F&B Program Type'),
          brandFbOutletConcepts: getStd('Typical Outlet Names / Concepts'),
          brandFbOutletSize: (() => {
            const v = getStd('Typical Total F&B Outlet Size') ?? getFieldValue(s, '# Typical Total F&B Outlet Size') ?? '';
            if (v === '' || v === undefined || v === null) return '';
            if (typeof v === 'number') return Number.isNaN(v) ? '' : String(v).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
            return String(v).trim();
          })(),
          brandFbOutletSizeUnit: getStd('F&B Outlet Size Unit'),
          brandMeetingSpaceRequired: getStd('Meeting Space Required') ?? getStd('Meeting Space Required?') ?? '',
          brandMeetingRoomsCount: getStd('Typical Number of Meeting Rooms'),
          brandMeetingSpaceSize: getStd('Typical Meeting Space Size'),
          brandCondoResidencesAllowed: getStd('Condo Residences Allowed'),
          brandHotelRentalProgram: getStd('Hotel Rental Program'),
          // Parking
          brandParkingRequired: getStd('Parking Required'),
          brandParkingSpacesCount: (() => {
            const v = getStd('Typical Total Parking Spaces') ?? getFieldValue(s, '# Typical Total Parking Spaces') ?? '';
            if (v === '' || v === undefined || v === null) return '';
            if (typeof v === 'number') return Number.isNaN(v) ? '' : String(v).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
            return String(v).trim();
          })(),
          // Multi-select columns (Airtable: Parking Program, Sustainability Features, Additional Amenities, Compliance & Safety)
          brandParkingProgramType: (() => {
            const raw = getFieldValue(s, 'Parking Program');
            if (!Array.isArray(raw) || raw.length === 0) return typeof raw === 'string' && raw.trim() ? [raw.trim()] : [];
            return raw.map((item) => (typeof item === 'string' ? item.trim() : (item && item.name) ? String(item.name).trim() : '')).filter(Boolean);
          })(),
          brandSustainability: (() => {
            const raw = getFieldValue(s, 'Sustainability Features');
            if (!Array.isArray(raw) || raw.length === 0) return typeof raw === 'string' && raw.trim() ? [raw.trim()] : [];
            return raw.map((item) => (typeof item === 'string' ? item.trim() : (item && item.name) ? String(item.name).trim() : '')).filter(Boolean);
          })(),
          brandSustainabilityOther: getStd('Other Sustainability Text') ?? '',
          brandRequiredAmenities: (() => {
            const raw = getFieldValue(s, 'Additional Amenities');
            if (!Array.isArray(raw) || raw.length === 0) return typeof raw === 'string' && raw.trim() ? [raw.trim()] : [];
            return raw.map((item) => (typeof item === 'string' ? item.trim() : (item && item.name) ? String(item.name).trim() : '')).filter(Boolean);
          })(),
          brandRequiredAmenitiesOther: getStd('Other Amenities Text - Amenities') ?? getStd('Other Amenities Text') ?? '',
          brandCompliance: (() => {
            const raw = getFieldValue(s, 'Compliance & Safety');
            if (!Array.isArray(raw) || raw.length === 0) return typeof raw === 'string' && raw.trim() ? [raw.trim()] : [];
            return raw.map((item) => (typeof item === 'string' ? item.trim() : (item && item.name) ? String(item.name).trim() : '')).filter(Boolean);
          })(),
          brandComplianceOther: getStd('Other Text - Compliance') ?? getFieldValue(s, 'Other Text - Compliance') ?? '',
          brandQaExpectations: getStd('Typical QA / Brand Standards Expectations') ?? '',
          brandStandardsNotes: getStd('Additional Brand Standards Notes') ?? ''
        };
        // Normalize amenity fields for Brand Setup checkboxes: "Yes"/"Required"/"true" → truthy
        const amenityKeys = ['lobby', 'barBeverage', 'fitnessCenter', 'pool', 'meetingEventSpace', 'coworking', 'grabGo', 'onsiteParking'];
        amenityKeys.forEach((k) => {
          const v = brandStandardsData[k];
          if (typeof v === 'string' && (v.toLowerCase() === 'yes' || v.toLowerCase() === 'required' || v.toLowerCase() === 'true')) brandStandardsData[k] = true;
          else if (v === true || v === 1) brandStandardsData[k] = true;
        });
      }
    } catch (error) {
      console.error("Error fetching brand standards:", error);
      loadWarnings.push("Brand Standards");
    }

    // Get deal terms using same mapping as form (DEAL_TERMS_FORM_TO_AIRTABLE: form ID → Airtable columns)
    let dealTermsData = {};
    try {
      const termsRec = await findLinkedRecordByBrand(base, F.dealTerms.table, brandRecordId, brandName);
      if (termsRec) {
        const t = termsRec.fields;
        const getFirst = (keys) => {
          for (const k of keys) {
            const v = getFieldValue(t, k);
            if (v !== undefined && v !== null && v !== '') return v;
          }
          return '';
        };
        for (const [formId, airtableCols] of Object.entries(DEAL_TERMS_FORM_TO_AIRTABLE)) {
          let val = getFirst(airtableCols);
          if (Array.isArray(val) && val.length > 0) val = val[0];
          if (val && typeof val === 'object' && typeof val.name === 'string') val = val.name;
          dealTermsData[formId] = val;
        }
        if (dealTermsData.minInitialTermLength && !dealTermsData.minInitialTermQty) dealTermsData.minInitialTermQty = '1';
      }
    } catch (error) {
      console.error("Error fetching deal terms:", error);
      loadWarnings.push("Deal Terms");
    }

    // Get Portfolio & Performance data (Brand Setup - Portfolio & Performance)
    let portfolioPerformanceData = {};
    try {
      const ppRec = await findLinkedRecordByBrand(base, F.portfolioPerformance.table, brandRecordId, brandName);
      if (ppRec) {
        const pp = ppRec.fields;
        const getFirst = (keys) => {
          for (const k of keys) {
            const v = getFieldValue(pp, k);
            if (v !== undefined && v !== null && v !== '') return v;
          }
          return '';
        };
        const toDateOnly = (v) => {
          if (v == null || v === '') return '';
          const s = String(v).trim();
          if (!s) return '';
          if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
          const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
          if (m) return `${m[3]}-${m[1].padStart(2, '0')}-${m[2].padStart(2, '0')}`;
          return s;
        };
        const toPercentDisplay = (v) => {
          if (v === undefined || v === null || v === '') return v;
          const n = Number(v);
          if (Number.isNaN(n)) return v;
          if (n > 0 && n < 1) return String(Math.round(n * 10000) / 100);
          if (n > -1 && n < 0) return String(Math.round(n * 10000) / 100);
          return typeof v === 'string' ? v : String(v);
        };
        const PERCENT_FORM_IDS = ['revparImprovement', 'occupancyImprovement', 'noiImprovement', 'ownerRetention', 'renewalRate'];
        for (const [formId, airtableCols] of Object.entries(PORTFOLIO_PERFORMANCE_FORM_TO_AIRTABLE)) {
          if (formId === 'reportTypes') {
            const reportTypesColNew = 'Report Types Required';
            const reportTypesColOld = 'Report Types Required or Provided';
            let raw = getFieldValue(pp, reportTypesColNew) ?? getFieldValue(pp, reportTypesColOld);
            if (!raw && airtableCols && airtableCols[0]) raw = getFieldValue(pp, airtableCols[0]);
            let arr = Array.isArray(raw) ? raw : (raw ? [raw] : []);
            arr = arr.map((s) => (s && typeof s === 'object' && s.name ? s.name : String(s))).filter(Boolean);
            if (arr.length === 0) {
              for (const { airtableColumn, formValue } of REPORT_TYPES_CHECKBOX_COLUMNS) {
                const v = getFieldValue(pp, airtableColumn);
                if (v !== undefined && v !== null && v !== '' && v !== false && String(v).toLowerCase() !== 'no') arr.push(formValue);
              }
            }
            portfolioPerformanceData[formId] = arr;
            continue;
          }
          let val = getFirst(airtableCols);
          if (Array.isArray(val) && val.length > 0) val = val[0];
          if (val && typeof val === 'object' && typeof val.name === 'string') val = val.name;
          val = val !== undefined && val !== null && val !== '' ? (typeof val === 'string' ? val.trim() : val) : '';
          if (formId === 'portfolioMetricsAsOf' && val) val = toDateOnly(val);
          if (PERCENT_FORM_IDS.includes(formId) && val) val = toPercentDisplay(val);
          portfolioPerformanceData[formId] = val;
        }
      }
    } catch (error) {
      console.error("Error fetching portfolio & performance:", error);
      loadWarnings.push("Portfolio & Performance");
    }

    // Backfill Fee Structure with Deal Terms for termination fields when stored in Deal Terms table
    for (const key of FEE_STRUCTURE_FIELDS_ALSO_IN_DEAL_TERMS) {
      const fromFee = feeStructureData[key];
      const fromDeal = dealTermsData[key];
      if ((fromFee === undefined || fromFee === null || fromFee === '') && fromDeal !== undefined && fromDeal !== null && fromDeal !== '') {
        feeStructureData[key] = typeof fromDeal === 'string' ? fromDeal.trim() : String(fromDeal);
      }
    }
    // Get Project Fit data (linked table). Build formValues so Brand Setup can prefill by form field name.
    let projectFitData = {};
    let projectFitRawForDebug = null;
    try {
      const pfRec = await findLinkedRecordByBrand(base, F.projectFit.table, brandRecordId, brandName);
      if (pfRec) {
        const raw = pfRec.fields || {};
        if (req.query && req.query.debug === "projectFit") projectFitRawForDebug = raw;
        projectFitData = fieldsToDisplayObject(raw);
        const formValues = {};
        const isChecked = (v) => {
          if (v === true || v === 1) return true;
          if (v === false || v === 0 || v === null || v === undefined) return false;
          if (typeof v === 'string') {
            const s = v.trim().toLowerCase();
            if (s === '' || s === 'no' || s === 'false' || s === '0') return false;
            if (s === 'yes' || s === 'true' || s === '1' || s === 'x' || s === '✓' || s === '✔') return true;
          }
          return !!v;
        };
        const rawKeys = Object.keys(raw);
        const getRaw = (exactKey) => {
          if (raw[exactKey] !== undefined) return raw[exactKey];
          const norm = (s) => String(s).trim().replace(/\u2013/g, '-').replace(/\u2014/g, '-');
          const exactNorm = norm(exactKey);
          const found = rawKeys.find((k) => norm(k) === exactNorm);
          return found !== undefined ? raw[found] : undefined;
        };
        const toArray = (raw) => {
          if (!Array.isArray(raw) || raw.length === 0) return [];
          return raw.map((item) => (typeof item === "string" ? item.trim() : (item && item.name) ? String(item.name).trim() : "")).filter(Boolean);
        };
        const buildFromCheckboxCols = (cols) => {
          const out = [];
          for (const { airtableColumn, formValue } of cols) {
            const v = raw[airtableColumn];
            if (isChecked(v)) out.push(formValue);
          }
          return out;
        };
        const projectStageToShort = (s) => String(s).replace(/\s*-\s*Acceptable Project Stages\s*$/i, '').trim() || s;
        const acceptableProjectTypeRaw = getRaw("Acceptable Project Type") || getRaw("Acceptable Project Types");
        if (Array.isArray(acceptableProjectTypeRaw) && acceptableProjectTypeRaw.length > 0) formValues.idealProjectTypes = toArray(acceptableProjectTypeRaw);
        else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_ACCEPTABLE_PROJECT_TYPES_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.idealProjectTypes = fromCheckboxes;
        }
        const acceptableBuildingTypesRaw = getRaw("Acceptable Building Types");
        if (Array.isArray(acceptableBuildingTypesRaw) && acceptableBuildingTypesRaw.length > 0) formValues.idealBuildingTypes = toArray(acceptableBuildingTypesRaw);
        else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_ACCEPTABLE_BUILDING_TYPES_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.idealBuildingTypes = fromCheckboxes;
        }
        const acceptableAgreementsTypeRaw = getRaw("Acceptable Agreements Type") || getRaw("Acceptable Agreement Types");
        if (Array.isArray(acceptableAgreementsTypeRaw) && acceptableAgreementsTypeRaw.length > 0) formValues.idealAgreementTypes = toArray(acceptableAgreementsTypeRaw);
        else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_ACCEPTABLE_AGREEMENT_TYPES_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.idealAgreementTypes = fromCheckboxes;
        }
        const acceptableProjectStagesRaw = getRaw("Acceptable Project Stages");
        if (Array.isArray(acceptableProjectStagesRaw) && acceptableProjectStagesRaw.length > 0) {
          const arr = toArray(acceptableProjectStagesRaw);
          formValues.projectStage = arr.map((s) => {
            const match = PROJECT_FIT_ACCEPTABLE_PROJECT_STAGES_COLUMNS.find((c) => s === c.formValue || s === c.airtableColumn || s.endsWith(c.formValue));
            return projectStageToShort(match ? match.formValue : s);
          });
        } else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_ACCEPTABLE_PROJECT_STAGES_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.projectStage = fromCheckboxes.map(projectStageToShort);
        }
        const priorityMarketsRaw = getRaw("Priority Markets");
        if (Array.isArray(priorityMarketsRaw) && priorityMarketsRaw.length > 0) {
          const arr = priorityMarketsRaw.map((item) => (typeof item === "string" ? item.trim() : (item && item.name) ? String(item.name).trim() : "")).filter(Boolean);
          formValues.priorityMarkets = arr.map((s) => {
            const match = PROJECT_FIT_PRIORITY_MARKETS_COLUMNS.find((c) => c.formValue === s || c.airtableColumn === s || s.endsWith(c.formValue));
            return match ? match.formValue : s;
          });
        } else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_PRIORITY_MARKETS_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.priorityMarkets = fromCheckboxes;
        }
        const marketsToAvoidRaw = getRaw("Markets to Avoid");
        if (Array.isArray(marketsToAvoidRaw) && marketsToAvoidRaw.length > 0) {
          const arr = marketsToAvoidRaw.map((item) => (typeof item === "string" ? item.trim() : (item && item.name) ? String(item.name).trim() : "")).filter(Boolean);
          formValues.marketsToAvoid = arr.map((s) => {
            const match = PROJECT_FIT_MARKETS_TO_AVOID_COLUMNS.find((c) => c.formValue === s || c.airtableColumn === s || s.endsWith(c.formValue));
            return match ? match.formValue : s;
          });
        } else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_MARKETS_TO_AVOID_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.marketsToAvoid = fromCheckboxes;
        }
        const ownerInvolvementRaw = getRaw("Acceptable Owner Involvement Levels");
        if (Array.isArray(ownerInvolvementRaw) && ownerInvolvementRaw.length > 0) formValues.ownerInvolvementLevel = toArray(ownerInvolvementRaw);
        else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_OWNER_INVOLVEMENT_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.ownerInvolvementLevel = fromCheckboxes;
        }
        const ownerNonNegotiablesRaw = getRaw("Owner Non-Negotiables");
        if (Array.isArray(ownerNonNegotiablesRaw) && ownerNonNegotiablesRaw.length > 0) formValues.ownerNonNegotiableTypes = toArray(ownerNonNegotiablesRaw);
        else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_OWNER_NON_NEGOTIABLE_TYPES_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.ownerNonNegotiableTypes = fromCheckboxes;
        }
        const capitalStatusRaw = getRaw("Acceptable Capital Status at Engagement");
        if (Array.isArray(capitalStatusRaw) && capitalStatusRaw.length > 0) formValues.capitalStatus = toArray(capitalStatusRaw);
        else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_CAPITAL_STATUS_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.capitalStatus = fromCheckboxes;
        }
        const brandStatusRaw = getRaw("Brand Status Scenarios You Will Consider");
        if (Array.isArray(brandStatusRaw) && brandStatusRaw.length > 0) formValues.brandStatus = toArray(brandStatusRaw);
        else {
          const fromCheckboxes = buildFromCheckboxCols(PROJECT_FIT_BRAND_STATUS_COLUMNS);
          if (fromCheckboxes.length > 0) formValues.brandStatus = fromCheckboxes;
        }
        const feeExpectationsRaw = getRaw("Acceptable Fee Expectations vs Market");
        if (Array.isArray(feeExpectationsRaw) && feeExpectationsRaw.length > 0) formValues.feeExpectationVsMarket = toArray(feeExpectationsRaw);
        const exitHorizonRaw = getRaw("Acceptable Exit Horizon");
        if (Array.isArray(exitHorizonRaw) && exitHorizonRaw.length > 0) formValues.exitHorizon = toArray(exitHorizonRaw);
        for (const [formName, airtableCol] of Object.entries(PROJECT_FIT_AIRTABLE_TO_FORM)) {
          const val = getRaw(airtableCol);
          if (val === undefined || val === null || val === '') continue;
          if (Array.isArray(val) && val.length === 0) continue;
          if (Array.isArray(val) && typeof val[0] === 'string' && val[0].startsWith('rec')) continue;
          if (Array.isArray(val)) {
            const arr = val.map((item) => (typeof item === 'string' ? item.trim() : (item && item.name) || '')).filter(Boolean);
            formValues[formName] = arr;
          } else if (typeof val === 'number' && !Number.isNaN(val)) {
            formValues[formName] = String(val);
          } else {
            formValues[formName] = typeof val === 'string' ? val.trim() : valueToStr(val) || '';
          }
        }
        projectFitData.formValues = formValues;
      }
    } catch (err) {
      console.error("Error fetching project fit:", err.message);
      loadWarnings.push("Project Fit");
    }

    // Get Operational Support data (via linked record from Brand Basics, or by Brand Name)
    let operationalSupportData = {};
    let opFields = null;
    const opSupportLink = brandFields['Brand Setup - Operational Support'];
    if (opSupportLink && Array.isArray(opSupportLink) && opSupportLink.length > 0) {
      try {
        const opRecord = await base(F.operationalSupport.table).find(opSupportLink[0]);
        opFields = opRecord.fields;
      } catch (err) {
        console.error("Error fetching operational support:", err.message);
      }
    }
    if (!opFields || Object.keys(opFields).length === 0) {
      try {
        const opRec = await findLinkedRecordByBrand(base, F.operationalSupport.table, brandRecordId, brandName);
        if (opRec) opFields = opRec.fields;
      } catch (_) {
        loadWarnings.push("Operational Support");
      }
    }
    if (opFields) {
      const opKeys = Object.keys(opFields);
      const getOpRaw = (exactKey) => {
        if (opFields[exactKey] !== undefined) return opFields[exactKey];
        const norm = (s) => String(s).trim().replace(/\u2013/g, '-').replace(/\u2014/g, '-');
        const exactNorm = norm(exactKey);
        const found = opKeys.find((k) => norm(k) === exactNorm || norm(k).toLowerCase() === exactNorm.toLowerCase());
        return found !== undefined ? opFields[found] : undefined;
      };
      const opToArray = (raw) => {
        if (!Array.isArray(raw) || raw.length === 0) return [];
        return raw.map((item) => (typeof item === 'string' ? item.trim() : (item && item.name) ? String(item.name).trim() : '')).filter(Boolean);
      };
      for (const { form, airtable } of OPERATIONAL_SUPPORT_FORM_TO_AIRTABLE) {
        const val = getOpRaw(airtable);
        if (val !== undefined && val !== null && val !== '') {
          operationalSupportData[form] = typeof val === 'string' ? val.trim() : valueToStr(val) || String(val);
        }
      }
      const incentiveTypesRaw = getOpRaw("Incentive Types");
      if (Array.isArray(incentiveTypesRaw) && incentiveTypesRaw.length > 0) operationalSupportData.typesOfIncentives = opToArray(incentiveTypesRaw);
      else if (typeof incentiveTypesRaw === 'string' && incentiveTypesRaw.trim()) operationalSupportData.typesOfIncentives = opToArray([incentiveTypesRaw]);
      for (const { formKey, airtableCol } of OPERATIONAL_SUPPORT_SERVICE_MULTI_SELECT) {
        const raw = getOpRaw(airtableCol);
        if (Array.isArray(raw) && raw.length > 0) operationalSupportData[formKey] = opToArray(raw);
        else if (typeof raw === 'string' && raw.trim()) operationalSupportData[formKey] = opToArray([raw]);
      }
    }

    // Get Legal Terms data (via linked record from Brand Basics, or by Brand Name)
    let legalTermsData = {};
    let legalFields = null;
    const legalTermsLink = brandFields['Brand Setup - Legal Terms'];
    if (legalTermsLink && Array.isArray(legalTermsLink) && legalTermsLink.length > 0) {
      try {
        const legalRecord = await base(F.legalTerms.table).find(legalTermsLink[0]);
        legalFields = legalRecord.fields;
      } catch (err) {
        console.error("Error fetching legal terms:", err.message);
      }
    }
    if (!legalFields || Object.keys(legalFields).length === 0) {
      try {
        const legalRec = await findLinkedRecordByBrand(base, F.legalTerms.table, brandRecordId, brandName);
        if (legalRec) legalFields = legalRec.fields;
      } catch (_) {
        loadWarnings.push("Legal Terms");
      }
    }
    if (legalFields) {
      for (const { form, airtable } of LEGAL_TERMS_FORM_TO_AIRTABLE) {
        const val = legalFields[airtable];
        if (val !== undefined && val !== null && val !== '') {
          legalTermsData[form] = typeof val === 'string' ? val.trim() : valueToStr(val) || String(val);
        }
      }
    }

    // Brand Setup - Sustainability & ESG (linked table; form fields in Brand Basics tab)
    let sustainabilityEsgData = {};
    try {
      const esgRec = await findLinkedRecordByBrand(base, F.sustainabilityEsg.table, brandRecordId, brandName);
      if (esgRec && esgRec.fields) {
        const f = esgRec.fields;
        const getEsg = (col) => (getFieldValue(f, col) ?? '').toString().trim();
        sustainabilityEsgData = {
          sustainabilityPrograms: getEsg('Sustainability Programs'),
          esgReporting: getEsg('ESG Reporting'),
          carbonTracking: getEsg('Carbon Footprint Tracking'),
          energyEfficiency: getEsg('Energy Efficiency Initiatives'),
          wasteReduction: getEsg('Waste Reduction Programs')
        };
      }
    } catch (err) {
      console.error("Error fetching Sustainability & ESG:", err.message);
      loadWarnings.push("Sustainability & ESG");
    }

    const logoUrl = extractLogoUrl(brandFields);

    // Build Basics from same mapping used for PATCH (FORM_TO_AIRTABLE_BASICS) so read and write stay in sync
    const brandDetails = {
      id: brandRecord.id,
      name: brandName,
      logo: logoUrl,
      footprint: footprintData,
      loyaltyCommercial: loyaltyCommercialData,
      feeStructure: feeStructureData,
      brandStandards: brandStandardsData,
      dealTerms: dealTermsData,
      portfolioPerformance: portfolioPerformanceData,
      projectFit: projectFitData,
      operationalSupport: operationalSupportData,
      legalTerms: legalTermsData
    };
    for (const [formKey, airtableKey] of Object.entries(FORM_TO_AIRTABLE_BASICS)) {
      const raw = brandFields[airtableKey];
      if (formKey === 'targetGuestSegments') {
        brandDetails[formKey] = Array.isArray(raw) ? raw.map((s) => valueToStr(s)).filter(Boolean) : [];
      } else if (raw !== undefined && raw !== null && raw !== '') {
        brandDetails[formKey] = typeof raw === 'string' ? raw.trim() : valueToStr(raw) || raw;
      } else {
        brandDetails[formKey] = '';
      }
    }
    for (const [k, v] of Object.entries(sustainabilityEsgData)) {
      brandDetails[k] = v;
    }
    if (loadWarnings.length > 0) brandDetails.loadWarnings = loadWarnings;

    if (req.query && req.query.debug === "projectFit") {
      brandDetails.projectFitDebug = {
        rawAirtableFields: projectFitRawForDebug || {},
        formValues: projectFitData.formValues || {},
        noProjectFitRecord: projectFitRawForDebug == null,
        expectedScalarColumns: Object.entries(PROJECT_FIT_AIRTABLE_TO_FORM).map(([form, col]) => ({ form, airtableColumn: col })),
        expectedCheckboxColumns: {
          idealProjectTypes: "Acceptable Project Type (multi-select)",
          idealBuildingTypes: "Acceptable Building Types (multi-select)",
          idealAgreementTypes: "Acceptable Agreements Type (multi-select)",
          projectStage: "Acceptable Project Stages (multi-select)",
          priorityMarkets: "Priority Markets (multi-select)",
          marketsToAvoid: "Markets to Avoid (multi-select)",
          ownerInvolvementLevel: "Acceptable Owner Involvement Levels (multi-select)",
          brandStatus: "Brand Status Scenarios You Will Consider (multi-select)",
          feeExpectationVsMarket: "Acceptable Fee Expectations vs Market (multi-select)",
          exitHorizon: "Acceptable Exit Horizon (multi-select)",
          ownerNonNegotiableTypes: "Owner Non-Negotiables (multi-select)",
          capitalStatus: "Acceptable Capital Status at Engagement (multi-select)"
        }
      };
    }

    res.json({
      success: true,
      brand: brandDetails
    });

  } catch (error) {
    console.error("Error fetching brand details:", error);
    console.error("Error stack:", error.stack);
    res.status(500).json({ 
      success: false,
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Single source of truth: Brand Setup form field name → Airtable column (Brand Setup - Brand Basics).
// Used for both PATCH (write) and GET (read/preload) so the same mapping drives save and load.
const FORM_TO_AIRTABLE_BASICS = {
  brandName: F.brandBasics.name,
  parentCompany: F.brandBasics.parentCompany,
  hotelChainScale: F.brandBasics.chainScale,
  brandArchitecture: F.brandBasics.architecture,
  brandModelFormat: F.brandBasics.brandModel,
  hotelServiceModel: F.brandBasics.serviceModel,
  yearBrandLaunched: F.brandBasics.yearLaunched,
  brandDevelopmentStage: F.brandBasics.developmentStage,
  brandPositioning: F.brandBasics.positioning,
  brandTaglineMotto: F.brandBasics.tagline,
  brandCustomerPromise: F.brandBasics.customerPromise,
  brandValueProposition: F.brandBasics.valueProposition,
  brandPillars: F.brandBasics.brandPillars,
  companyHistory: F.brandBasics.companyHistory,
  targetGuestSegments: F.brandBasics.targetSegments,
  guestPsychographics: F.brandBasics.guestPsychographics,
  keyBrandDifferentiators: F.brandBasics.differentiators,
  sustainabilityPositioning: F.brandBasics.sustainability,
  brandWebsite: "Brand Website",
  brandStatus: F.brandBasics.status,
  brandProfileAnalysis: F.brandBasics.profileAnalysis
};

// Brand Setup - Brand Footprint: form input name → Airtable column name. Same mapping for read (preload) and future write.
const FOOTPRINT_FORM_TO_AIRTABLE = [
  { form: 'geo_na_existing_hotels', airtable: 'AM Existing Hotel' }, { form: 'geo_na_existing_rooms', airtable: 'AM Existing Rooms' },
  { form: 'geo_na_pipeline_hotels', airtable: 'AM Pipeline Hotel' }, { form: 'geo_na_pipeline_rooms', airtable: 'AM Pipeline Rooms' },
  { form: 'geo_cala_existing_hotels', airtable: 'CALA Existing Hotel' }, { form: 'geo_cala_existing_rooms', airtable: 'CALA Existing Rooms' },
  { form: 'geo_cala_pipeline_hotels', airtable: 'CALA Pipeline Hotel' }, { form: 'geo_cala_pipeline_rooms', airtable: 'CALA Pipeline Rooms' },
  { form: 'geo_eu_existing_hotels', airtable: 'EU Existing Hotel' }, { form: 'geo_eu_existing_rooms', airtable: 'EU Existing Rooms' },
  { form: 'geo_eu_pipeline_hotels', airtable: 'EU Pipeline Hotel' }, { form: 'geo_eu_pipeline_rooms', airtable: 'EU Pipeline Rooms' },
  { form: 'geo_mea_existing_hotels', airtable: 'MEA Existing Hotel' }, { form: 'geo_mea_existing_rooms', airtable: 'MEA Existing Rooms' },
  { form: 'geo_mea_pipeline_hotels', airtable: 'MEA Pipeline Hotel' }, { form: 'geo_mea_pipeline_rooms', airtable: 'MEA Pipeline Rooms' },
  { form: 'geo_apac_existing_hotels', airtable: 'APAC Existing Hotel' }, { form: 'geo_apac_existing_rooms', airtable: 'APAC Existing Rooms' },
  { form: 'geo_apac_pipeline_hotels', airtable: 'APAC Pipeline Hotel' }, { form: 'geo_apac_pipeline_rooms', airtable: 'APAC Pipeline Rooms' },
  // Property Experience Types (%)
  { form: 'newBuildExperience', airtable: 'New Build Experience (New build %)' },
  { form: 'conversionExperience', airtable: 'Conversion Experience (Conversion %)' },
  { form: 'turnaroundExperience', airtable: 'Turnaround Experience (%)' },
  { form: 'renovationExperience', airtable: 'Renovation/Rebrand Experience (%)' },
  { form: 'typicalManagedPercent', airtable: 'Typical Managed %' },
  { form: 'typicalFranchisedPercent', airtable: 'Typical Franchised %' },
  // Systemwide Existing Unit Distribution by Location Type (%)
  { form: 'locationTypeUrban', airtable: 'Urban - Existing Systemwide Location' },
  { form: 'locationTypeSuburban', airtable: 'Suburban - Existing Systemwide Location' },
  { form: 'locationTypeResort', airtable: 'Resort - Existing Systemwide Location' },
  { form: 'locationTypeAirport', airtable: 'Airport - Existing Systemwide Location' },
  { form: 'locationTypeSmallMetro', airtable: 'Small Metro - Existing Systemwide Location' },
  { form: 'locationTypeInterstate', airtable: 'Interstate - Existing Systemwide Location' },
  { form: 'exitsDeflaggings', airtable: 'Number of Exits in Past 24 Months' },
  { form: 'figuresAsOf', airtable: 'Figures as of' },
  { form: 'numberOfMarkets', airtable: 'Number of Markets Operated In' },
  { form: 'specificMarkets', airtable: 'Specific Markets/Cities' }
];

// Brand Setup - Loyalty & Commercial: form field name → Airtable column name.
const LOYALTY_COMMERCIAL_FORM_TO_AIRTABLE = [
  { form: 'typicalLoyaltyProgramName', airtable: 'Typical Loyalty Program Name' },
  { form: 'typicalLoyaltyRoomsPercent', airtable: 'Typical % of Rooms from Loyalty (est.)' },
  { form: 'typicalDirectBookingPercent', airtable: 'Typical Direct Booking % (est.)' },
  { form: 'typicalOTAReliancePercent', airtable: 'Typical OTA Reliance % (est.)' },
  { form: 'totalGlobalMembersMillions', airtable: 'Total Global Members (Approx. Millions)' },
  { form: 'regionalMembersMillions_na', airtable: 'Regional Members - NA (Millions)' },
  { form: 'regionalMembersMillions_cala', airtable: 'Regional Members - CALA (Millions)' },
  { form: 'regionalMembersMillions_eu', airtable: 'Regional Members - EU (Millions)' },
  { form: 'regionalMembersMillions_mea', airtable: 'Regional Members - MEA (Millions)' },
  { form: 'regionalMembersMillions_apac', airtable: 'Regional Members - APAC (Millions)' },
  { form: 'loyaltyCostPerStay', airtable: 'Loyalty Program Cost per Stay (Approximate)' },
  { form: 'otaCommissionPercent', airtable: 'OTA Commission (Typical % of Reservation)' },
  { form: 'crsUsagePercent', airtable: 'CRS Usage (% of bookings flowing through)' },
  { form: 'distributionCostPerReservation', airtable: 'Distribution Cost (Per Reservation)' },
  { form: 'websiteAppConvRatesPercent', airtable: 'Website/App Conv. Rates (%)' },
  { form: 'avgCustomerAcquisitionCost', airtable: 'Avg. Cost of Cust. Acquisition' }
];


// Fee Structure: form fields that store decimal (0–1) in Airtable; display as percentage (× 100).
const FEE_PERCENT_FORM_NAMES = [
  'typicalRoyaltyPercentMin', 'typicalRoyaltyPercentMax',
  'typicalMarketingFeePercentMin', 'typicalMarketingFeePercentMax',
  'typicalLoyaltyFeePercentMin', 'typicalLoyaltyFeePercentMax'
];

// Exact strings from your Airtable column "Basis - Typical Royalty Fee Range". We only ever send one of these three.
const FEE_BASIS_GROSS = '% of Gross Revenue';
const FEE_BASIS_ROOMS = '% of Rooms Revenue';
const FEE_BASIS_TOTAL = '% of Total Revenue';

// For % basis: pick one of the three exact options by keyword. Same code path for all three.
function resolveFeeBasisPercentRevenue(val) {
  if (val === undefined || val === null || val === '') return val;
  const s = String(val).replace(/["\u201C\u201D]/g, '').trim().toLowerCase();
  if (s.includes('total')) return FEE_BASIS_TOTAL;
  if (s.includes('rooms') || s.includes('room')) return FEE_BASIS_ROOMS;
  if (s.includes('gross') || s.includes('revenue')) return FEE_BASIS_GROSS;
  return val;
}

// Normalize form Basis values to Airtable single-select exact options (Fee Structure table).
function normalizeFeeBasisValue(val) {
  if (val === undefined || val === null || val === '') return val;
  let s = String(val)
    .replace(/\\"/g, '')
    .replace(/["\u201C\u201D\u201E\u201F\u2033\u2036]/g, '')
    .replace(/\u00A0/g, ' ')
    .trim();
  const lower = s.toLowerCase();
  if (lower.includes('% of') && lower.includes('revenue')) return resolveFeeBasisPercentRevenue(s);
  const map = {
    'one-time': 'One-Time',
    'one time': 'One-Time',
    'included': 'Included',
    'per application': 'Per Application',
    'per property': 'Per Property',
    'per room / year': 'Per Room / Year',
    'per room/year': 'Per Room / Year',
    'per room': 'Per Room',
    'base + per room over threshold': 'Base + Per Room Over Threshold',
    // Reservation / Distribution Fee Basis options (match Airtable exactly)
    'per reservation / per booking': 'Per Reservation / Per Booking',
    'per reservation': 'Per Reservation / Per Booking',
    'per room / month': 'Per Room / Month',
    'per room/month': 'Per Room / Month',
    'fixed fee': 'Fixed Fee',
    'fixed': 'Fixed Fee'
  };
  return map[lower] !== undefined ? map[lower] : s;
}

// Fee Structure form ID → Airtable column (Brand Setup - Fee Structure). Same mapping for read and future write.
// Typical Franchise Fees: exact column names in Brand Setup - Fee Structure table.
const FEE_FORM_TO_AIRTABLE = {
  // Application Fee
  typicalApplicationFeeMin: ['Min - Typical Application Fee'],
  typicalApplicationFeeMax: ['Max - Typical Application Fee'],
  typicalApplicationFeeBasis: ['Basis - Typical Application Fee'],
  typicalApplicationFeeNotes: ['Additional Notes - Typical Application Fee'],
  applicationFeePerUnitOverThreshold: ['Application Fee Per Unit Over Threshold'],
  applicationFeeThresholdUnits: ['Application Fee Threshold (Units)'],
  // Royalty
  typicalRoyaltyPercentMin: ['Min - Typical Royalty Fee Range'],
  typicalRoyaltyPercentMax: ['Max - Typical Royalty Fee Range'],
  typicalRoyaltyPercentBasis: ['Basis - Typical Royalty Fee Range'],
  typicalRoyaltyNotes: ['Additional Notes - Typical Royalty Fee Range'],
  // Marketing
  typicalMarketingFeePercentMin: ['Min - Typical Marketing Fee Range'],
  typicalMarketingFeePercentMax: ['Max - Typical Marketing Fee Range'],
  typicalMarketingFeePercentBasis: ['Basis - Typical Marketing Fee Range'],
  typicalMarketingFeeNotes: ['Additional Notes - Typical Marketing Fee Range'],
  // Tech
  typicalTechnologyFeeMin: ['Min - Typical Tech'],
  typicalTechnologyFeeMax: ['Max - Typical Tech'],
  typicalTechnologyFeeBasis: ['Basis - Typical Tech'],
  typicalTechnologyFeeNotes: ['Additional Notes - Typical Tech'],
  // Loyalty Program Fee
  typicalLoyaltyFeePercentMin: ['Min - Typical Loyalty Program Fee'],
  typicalLoyaltyFeePercentMax: ['Max - Typical Loyalty Program Fee'],
  typicalLoyaltyFeePercentBasis: ['Basis - Typical Loyalty Program Fee'],
  typicalLoyaltyFeeNotes: ['Additional Notes - Typical Loyalty Program Fee'],
  // Reservation / Distribution Fee (exact column names only)
  typicalReservationFeeMin: ['Min - Typical Reservation / Distribution Fee'],
  typicalReservationFeeMax: ['Max - Typical Reservation / Distribution Fee'],
  typicalReservationFeeBasis: ['Basis - Typical Reservation / Distribution Fee'],
  // Typical Incentives
  typicalIncentivesOffered: ['Typical Incentives Offered'],
  // Termination, Capital & Risk (Fee Structure tab)
  ownerEarlyTerminationRights: ['Typical Owner Early-Termination Rights (without cause)'],
  ownerEarlyTerminationNotes: ['Early-Termination Notes'],
  terminationFeeStructure: ['Typical Termination Fee Structure (if any)'],
  terminationFeeStructureNotes: ['Typical Termination Fee Structure (if any) Text'],
  performanceTerminationRights: ['Who Can Exercise Termination Right After Failed Test?'],
  keyMoneyCoInvestment: ['Key Money / Co-Investment'],
  ownerFundedReserves: ['Typical Expectations for Owner-Funded Reserves'],
  typicalTrainingFeeMin: ['Min - Typical Training Fee', 'Training Fee'],
  typicalTrainingFeeMax: ['Max - Typical Training Fee'],
  typicalTrainingFeeBasis: ['Basis - Typical Training Fee'],
  typicalTrainingFeeNotes: ['Additional Notes - Typical Training Fee'],
  capReimbursableExpenses: ['Do Agreements Typically Cap Operator Reimbursable Expenses?'],
  auditRightsRequired: ['Do You Usually Require Audit Rights for Owner Books / Operator Systems?']
};

// These Fee Structure form fields can also be stored in Deal Terms; we backfill from dealTerms when empty in feeStructure.
const FEE_STRUCTURE_FIELDS_ALSO_IN_DEAL_TERMS = ['terminationFeeStructure', 'terminationFeeStructureNotes', 'performanceTerminationRights'];

// Deal Terms form ID → Airtable column (Brand Setup - Deal Terms). Same mapping for read and future write.
// Use exact column names; fallbacks only where legacy column names may exist.
const DEAL_TERMS_FORM_TO_AIRTABLE = {
  minInitialTermQty: ['Quantity - Typical Minimum Initial Term'],
  minInitialTermLength: ['Length - Typical Minimum Initial Term'],
  minInitialTermDuration: ['Duration - Typical Minimum Initial Term'],
  renewalOptionQty: ['Quantity - Typical Renewal Option'],
  renewalOptionLength: ['Length - Typical Renewal Option'],
  renewalOptionDuration: ['Duration - Typical Renewal Option'],
  renewalNoticeQty: ['Length - Typical Renewal Notice Period'],
  renewalNoticeDuration: ['Quantity - Typical Renewal Notice Period'],
  renewalStructure: ['Renewal Structure'],
  renewalNoticeResponsibility: ['Renewal Notice Responsibility'],
  renewalConditions: ['Typical Renewal Conditions'],
  performanceTestRequirement: ['Performance Test Requirement'],
  curePeriodQty: ['Typical Cure Period for Performance Test Failure'],
  curePeriodDuration: ['Duration - Typical Cure Period for Performance Test Failure'],
  qaComplianceRequirement: ['Typical QA'],
  pipAtRenewal: ['Mandatory PIP at Renewal'],
  pipForConversions: ['Mandatory PIP for Conversions'],
  terminationFeeStructure: ['Typical Termination Fee Structure (if any)'],
  terminationFeeStructureNotes: ['Typical Termination Fee Structure (if any) Text'],
  performanceTerminationRights: ['Who Can Exercise Termination Right After Failed Test?'],
  typicalPIPConversionPerRoom: ['Typical Mandatory PIP for Conversions ($/room)'],
  conversionMaxTimeQty: ['Conversion - Typical max time allowed for completion'],
  conversionMaxTimeDuration: ['Conversion - Typical max time allowed for completion -Duration'],
  renewalMaxTimeQty: ['Renewal - Typical max time allowed for completion'],
  renewalMaxTimeDuration: ['Renewal - Typical max time allowed for completion -Duration']
};

// Report Types: when using separate checkbox columns in Airtable, column name → form checkbox value.
const REPORT_TYPES_CHECKBOX_COLUMNS = [
  { airtableColumn: 'P&L Statement - Report Types', formValue: 'P&L Statement' },
  { airtableColumn: 'Cash Flow - Report Types', formValue: 'Cash Flow' },
  { airtableColumn: 'Budget vs. Actual - Report Types', formValue: 'Budget vs. Actual' },
  { airtableColumn: 'Forecasts - Report Types', formValue: 'Forecasts' },
  { airtableColumn: 'Operational Metrics - Report Types', formValue: 'Operational Metrics' },
  { airtableColumn: 'Capital Expenditure - Report Types', formValue: 'Capital Expenditure' }
];

// Portfolio & Performance form ID → Airtable column (Brand Setup - Portfolio & Performance). Exact column names only.
const PORTFOLIO_PERFORMANCE_FORM_TO_AIRTABLE = {
  portfolioMetricsAsOf: ['Portfolio Metrics As\u00A0of Date'], // non-breaking space (U+00A0) between "As" and "of" to match Airtable
  portfolioValue: ['Total Brand Portfolio Value'],
  annualRevenueManaged: ['Annual Revenue (Brand Wide)'],
  portfolioGrowthRate: ['Brand Portfolio Growth Rate'],
  minPropertySize: ['Minimum Property Size (Rooms)'],
  maxPropertySize: ['Maximum Property Size (Rooms)'],
  avgContractTerm: ['Typical Franchise Agreement Term'],
  feeStructure: ['Franchise Fee Structure'],
  revparImprovement: ['Typical RevPAR Improvement (Brand Benchmark)'],
  occupancyImprovement: ['Typical Occupancy Improvement (Brand Benchmark)'],
  noiImprovement: ['Typical NOI Improvement (Brand Benchmark)'],
  ownerRetention: ['Franchisee Retention Rate'],
  renewalRate: ['Typical Agreement Renewal Rate'],
  turnaroundCount: ['Turnaround Properties in Brand'],
  stabilizationTime: ['Typical Time to Stabilization'],
  reportingFrequency: ['Reporting Frequency Required or Provided'],
  reportTypes: ['Report Types Required'],
  budgetProcess: ['Budget Process (Brand Requirement or Support)'],
  capexPlanning: ['Capital Expenditure Planning (Brand Requirement)'],
  performanceReviews: ['Performance Review Cadence'],
  primaryPMS: ['PMS (Property Management System)'],
  revenueManagementSystem: ['Revenue Management System'],
  accountingSystem: ['Accounting / Reporting System'],
  guestCommunication: ['Guest Communication / Mobile Check-in'],
  mobileCheckin: ['Mobile Check-in / Digital Key Required'],
  ownerPortal: ['Franchisee Reporting Portal / Data Access'],
  analyticsPlatform: ['Data / Analytics Platform Required or Approved'],
  apiIntegrations: ['Required Integrations (OTAs, Payments, etc.)']
};

// All Portfolio & Performance Airtable columns we may write (so upsertLinkedRecord does not strip them when missing from existing.fields)
const PORTFOLIO_PERFORMANCE_ALLOWED_COLS = [
  ...Object.values(PORTFOLIO_PERFORMANCE_FORM_TO_AIRTABLE).flat(),
  "Report Types Required"
];

// Brand Standards: amenity checkbox form ID → Airtable column (Brand Setup - Brand Standards). Same mapping for read and future write.
const BRAND_STANDARDS_FORM_TO_AIRTABLE = {
  amenityLobby: ['Lobby', 'Lobby Required'],
  amenityBar: ['Bar / Beverage', 'Bar or Beverage', 'Bar/Beverage'],
  amenityFitness: ['Fitness Center', 'Fitness'],
  amenityPool: ['Pool'],
  amenityMeeting: ['Meeting / Event Space', 'Meeting/Event Space'],
  amenityCoworking: ['Co-Working Space', 'Co-working', 'Coworking'],
  amenityGrabgo: ['Grab & Go', 'Grab and Go', 'Grab & Go or Marketplace']
};

// Brand Setup - Legal Terms: form input name → Airtable column name. Same mapping for read (preload) and future write.
const LEGAL_TERMS_FORM_TO_AIRTABLE = [
  { form: 'aopRadius', airtable: 'Radius - Typical Area of Protection' },
  { form: 'aopRestrictions', airtable: 'Restrictions - Typical Area of Protection' },
  { form: 'loiTimeline', airtable: 'LOI Timeline to Agreement' },
  { form: 'terminationForCause', airtable: 'For Cause - Termination Rights' },
  { form: 'terminationWithoutCause', airtable: 'Without Cause - Termination Rights' },
  { form: 'terminationOnSale', airtable: 'On Sale - Termination Rights' },
  { form: 'liquidatedDamages', airtable: 'Liquidated Damages' },
  { form: 'exclusivityOptional', airtable: 'Exclusivity' },
  { form: 'loiBinding', airtable: 'Binding - LOI Binding vs NonBinding Terms' },
  { form: 'loiNonBinding', airtable: 'Non-Binding - LOI Binding vs NonBinding Terms' },
  { form: 'legalConfidentiality', airtable: 'Confidentiality' },
  { form: 'conditionsPrecedent', airtable: 'Conditions Precedent' },
  { form: 'buyoutTransferProvisions', airtable: 'Buyout / Transfer Provisions' },
  { form: 'assignmentRestrictions', airtable: 'Assignment Restrictions' }
];

// Brand Setup - Operational Support: form field name → Airtable column name.
// Willing to Negotiate Incentives: single select (Yes/No/Case by case); saved and prefilled from Airtable.
// Types of Incentives: multi-select column "Incentive Types" (handled separately below).
const OPERATIONAL_SUPPORT_FORM_TO_AIRTABLE = [
  { form: 'willingToNegotiateIncentives', airtable: 'Willing to Negotiate Incentives' },
  { form: 'typesOfIncentivesOther', airtable: 'Other (Text) - Types of Incentives' },
  { form: 'keyMoneyPaymentTiming', airtable: 'Key Money Payment Timing' },
  { form: 'keyMoneyPaymentStructure', airtable: 'Key Money Payment Structure' },
  { form: 'keyMoneyClawbackTerms', airtable: 'Key Money Clawback Terms' },
  { form: 'revenueManagementOther', airtable: 'Other Text - Revenue Management Services' },
  { form: 'salesMarketingOther', airtable: 'Other Text - Sales & Marketing Support' },
  { form: 'accountingReportingOther', airtable: 'Other Text - Accounting & Financial Reporting' },
  { form: 'procurementServicesOther', airtable: 'Other Text - Procurement Services' },
  { form: 'hrTrainingServicesOther', airtable: 'Other Text - HR & Training Services' },
  { form: 'technologyServicesOther', airtable: 'Other Text - Technology Services' },
  { form: 'designRenovationSupportOther', airtable: 'Other Text - Design & Renovation Support' },
  { form: 'developmentServicesOther', airtable: 'Other Text - Development Services' },
  { form: 'serviceDifferentiators', airtable: 'Service Offering Summary' },
  // Owner Communication
  { form: 'communicationStyle', airtable: 'Owner Communication Style' },
  { form: 'ownerInvolvement', airtable: 'Owner Involvement Level' },
  { form: 'ownerResponseTime', airtable: 'Typical Response Time for Owner Inquiries' },
  { form: 'decisionMaking', airtable: 'Decision-Making Process' },
  { form: 'disputeResolution', airtable: 'Dispute Resolution Approach' },
  { form: 'concernResolutionTime', airtable: 'Average Time to Resolve Owner Concerns' },
  { form: 'ownerAdvisoryBoard', airtable: 'Owner Advisory Board' },
  { form: 'ownerEducation', airtable: 'Owner Education/Training Provided' },
  // References & Proof Points
  { form: 'ownerReferences', airtable: 'Owner References Available' },
  { form: 'caseStudies', airtable: 'Case Studies Available' },
  { form: 'testimonialLinks', airtable: 'Testimonial Links' },
  { form: 'industryRecognition', airtable: 'Industry Recognition' },
  { form: 'ownerSatisfactionScore', airtable: 'Owner Satisfaction Score (NPS)' },
  { form: 'lenderReferences', airtable: 'Lender References Available' },
  { form: 'majorLenders', airtable: 'Major Lenders Worked With' },
  // Company Specializations & Notes
  { form: 'specializations', airtable: 'Specializations' },
  { form: 'testimonials', airtable: 'Key Owner Success Stories' },
  { form: 'ownerPortalAvailable', airtable: 'Owner Portal Available' },
  { form: 'ownerPortalTier', airtable: 'Owner Portal Tier' },
  { form: 'ownerPortalFeatures', airtable: 'Owner Portal Features' },
  { form: 'ownerPortalNotes', airtable: 'Owner Portal Notes' },
  { form: 'additionalNotes', airtable: 'Additional Notes' },
  { form: 'ongoingSupportIncluded', airtable: 'Ongoing Support Included' },
  { form: 'crsParticipation', airtable: 'CRS / Central Res. Participation' },
  { form: 'gdsParticipation', airtable: 'GDS Participation' }
];

// Service categories: single Airtable multi-select column per category (options must match Airtable exactly).
const OPERATIONAL_SUPPORT_SERVICE_MULTI_SELECT = [
  { formKey: 'revenueManagementServices', airtableCol: 'Revenue Management Services' },
  { formKey: 'salesMarketingSupport', airtableCol: 'Sales & Marketing Support' },
  { formKey: 'accountingReporting', airtableCol: 'Accounting & Financial Reporting' },
  { formKey: 'procurementServices', airtableCol: 'Procurement Services' },
  { formKey: 'hrTrainingServices', airtableCol: 'HR & Training Services' },
  { formKey: 'technologyServices', airtableCol: 'Technology Services' },
  { formKey: 'designRenovationSupport', airtableCol: 'Design & Renovation Support' },
  { formKey: 'developmentServices', airtableCol: 'Development Services' }
];

// Option values per category (for prefill when Airtable returns array; form multi-select options match these).
const OPERATIONAL_SUPPORT_SERVICE_COLUMNS = {
  revenueManagementServices: [
    { formValue: 'In-House Revenue Management Team', airtable: 'In-House Revenue Management Team - Revenue Management Services' },
    { formValue: 'Outsourced Revenue Management', airtable: 'Outsourced Revenue Management - Revenue Management Services' },
    { formValue: 'Dedicated Revenue Manager Per Property', airtable: 'Dedicated Revenue Manager Per Property - Revenue Management Services' },
    { formValue: 'Regional Revenue Management Support', airtable: 'Regional Revenue Management Support - Revenue Management Services' },
    { formValue: 'Advanced Analytics and Forecasting', airtable: 'Advanced Analytics and Forecasting - Revenue Management Services' },
    { formValue: 'Dynamic Pricing Optimization', airtable: 'Dynamic Pricing Optimization - Revenue Management Services' },
    { formValue: 'Market Intelligence and Benchmarking', airtable: 'Market Intelligence and Benchmarking - Revenue Management Services' },
    { formValue: 'Other', airtable: 'Other - Revenue Management Services' }
  ],
  salesMarketingSupport: [
    { formValue: 'Dedicated Sales Team', airtable: 'Dedicated Sales Team - Sales & Marketing Support' },
    { formValue: 'Group Sales Support', airtable: 'Group Sales Support - Sales & Marketing Support' },
    { formValue: 'Corporate Sales Support', airtable: 'Corporate Sales Support - Sales & Marketing Support' },
    { formValue: 'Digital Marketing Services', airtable: 'Digital Marketing Services - Sales & Marketing Support' },
    { formValue: 'Social Media Management', airtable: 'Social Media Management - Sales & Marketing Support' },
    { formValue: 'Brand Marketing Support', airtable: 'Brand Marketing Support - Sales & Marketing Support' },
    { formValue: 'Local Marketing Programs', airtable: 'Local Marketing Programs - Sales & Marketing Support' },
    { formValue: 'SEO and Online Presence', airtable: 'SEO and Online Presence - Sales & Marketing Support' },
    { formValue: 'Other', airtable: 'Other - Sales & Marketing Support' }
  ],
  accountingReporting: [
    { formValue: 'Daily Financial Reporting', airtable: 'Daily Financial Reporting - Accounting & Financial Reporting' },
    { formValue: 'Weekly Financial Reporting', airtable: 'Weekly Financial Reporting - Accounting & Financial Reporting' },
    { formValue: 'Monthly P&L Statements', airtable: 'Monthly P&L Statements - Accounting & Financial Reporting' },
    { formValue: 'Cash Flow Management', airtable: 'Cash Flow Management - Accounting & Financial Reporting' },
    { formValue: 'Budget vs. Actual Analysis', airtable: 'Budget vs. Actual Analysis - Accounting & Financial Reporting' },
    { formValue: 'Forecasting and Projections', airtable: 'Forecasting and Projections - Accounting & Financial Reporting' },
    { formValue: 'Owner Portal Access', airtable: 'Owner Portal Access - Accounting & Financial Reporting' },
    { formValue: 'Real-Time Financial Data', airtable: 'Real-Time Financial Data - Accounting & Financial Reporting' },
    { formValue: 'Other', airtable: 'Other - Accounting & Financial Reporting' }
  ],
  procurementServices: [
    { formValue: 'Centralized Purchasing', airtable: 'Centralized Purchasing - Procurement Services' },
    { formValue: 'Preferred Vendor Network', airtable: 'Preferred Vendor Network - Procurement Services' },
    { formValue: 'Volume Discounts', airtable: 'Volume Discounts - Procurement Services' },
    { formValue: 'Supply Chain Management', airtable: 'Supply Chain Management - Procurement Services' },
    { formValue: 'Vendor Relationship Management', airtable: 'Vendor Relationship Management - Procurement Services' },
    { formValue: 'Cost Savings Programs', airtable: 'Cost Savings Programs - Procurement Services' },
    { formValue: 'Quality Assurance On Purchases', airtable: 'Quality Assurance On Purchases - Procurement Services' },
    { formValue: 'Other', airtable: 'Other - Procurement Services' }
  ],
  hrTrainingServices: [
    { formValue: 'Recruitment and Hiring', airtable: 'Recruitment and Hiring - HR & Training Services' },
    { formValue: 'Onboarding Programs', airtable: 'Onboarding Programs - HR & Training Services' },
    { formValue: 'Ongoing Training Programs', airtable: 'Ongoing Training Programs - HR & Training Services' },
    { formValue: 'Leadership Development', airtable: 'Leadership Development - HR & Training Services' },
    { formValue: 'Certification Support (CHA, CHRM, etc.)', airtable: 'Certification Support (CHA, CHRM, etc.) - HR & Training Services' },
    { formValue: 'Performance Management', airtable: 'Performance Management - HR & Training Services' },
    { formValue: 'Employee Retention Programs', airtable: 'Employee Retention Programs - HR & Training Services' },
    { formValue: 'HR Compliance and Administration', airtable: 'HR Compliance and Administration - HR & Training Services' },
    { formValue: 'Other', airtable: 'Other - HR & Training Services' }
  ],
  technologyServices: [
    { formValue: 'IT Support and Helpdesk', airtable: 'IT Support and Helpdesk - Technology Services' },
    { formValue: 'System Integrations', airtable: 'System Integrations - Technology Services' },
    { formValue: 'Technology Infrastructure Management', airtable: 'Technology Infrastructure Management - Technology Services' },
    { formValue: 'Cybersecurity Services', airtable: 'Cybersecurity Services - Technology Services' },
    { formValue: 'Data Analytics and Reporting', airtable: 'Data Analytics and Reporting - Technology Services' },
    { formValue: 'Cloud Services Management', airtable: 'Cloud Services Management - Technology Services' },
    { formValue: 'Hardware Procurement and Management', airtable: 'Hardware Procurement and Management - Technology Services' },
    { formValue: 'Other', airtable: 'Other - Technology Services' }
  ],
  designRenovationSupport: [
    { formValue: 'In-House Design Team', airtable: 'In-House Design Team - Design & Renovation Support' },
    { formValue: 'Renovation Project Management', airtable: 'Renovation Project Management - Design & Renovation Support' },
    { formValue: 'FF&E Procurement', airtable: 'FF&E Procurement - Design & Renovation Support' },
    { formValue: 'Brand Standard Compliance', airtable: 'Brand Standard Compliance - Design & Renovation Support' },
    { formValue: 'Space Planning and Design', airtable: 'Space Planning and Design - Design & Renovation Support' },
    { formValue: 'Construction Management', airtable: 'Construction Management - Design & Renovation Support' },
    { formValue: 'Vendor Coordination', airtable: 'Vendor Coordination - Design & Renovation Support' },
    { formValue: 'Other', airtable: 'Other - Design & Renovation Support' }
  ],
  developmentServices: [
    { formValue: 'Pre-Opening Services', airtable: 'Pre-Opening Services - Development Services' },
    { formValue: 'New Build Project Management', airtable: 'New Build Project Management - Development Services' },
    { formValue: 'Conversion Project Management', airtable: 'Conversion Project Management - Development Services' },
    { formValue: 'Feasibility Studies', airtable: 'Feasibility Studies - Development Services' },
    { formValue: 'Development Consulting', airtable: 'Development Consulting - Development Services' },
    { formValue: 'Permit and Regulatory Support', airtable: 'Permit and Regulatory Support - Development Services' },
    { formValue: 'Opening Team Deployment', airtable: 'Opening Team Deployment - Development Services' },
    { formValue: 'Other', airtable: 'Other - Development Services' }
  ]
};
// Incentive types: single Airtable multi-select column "Incentive Types" (options must match Airtable).
// Option values for form multi-select and prefill (order can match Airtable config).
const OPERATIONAL_SUPPORT_INCENTIVE_TYPES_OPTIONS = [
  'Key Money / Upfront Incentive',
  'Territorial Exclusivity / Radius',
  'Reduced Royalty Period',
  'Reduced Marketing Fee Period',
  'Reduced / Waived Tech Fee Period',
  'PIP Contribution by Brand',
  'Sign-on / Conversion Incentive',
  'Application Fee Credit',
  'Opening / FF&E Support',
  'Marketing Allowance (one-time or recurring)',
  'Other'
];

export async function updateBrandBasicsById(req, res) {
  let fields = {};
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    fields = {};
    let brandStatusMeta = null;
    for (const [formKey, airtableKey] of Object.entries(FORM_TO_AIRTABLE_BASICS)) {
      const val = body[formKey];
      if (val === undefined) continue;
      if (formKey === "brandStatus") {
        if (brandStatusMeta === null) brandStatusMeta = await getBrandStatusChoiceNames();
        const { choices: brandStatusChoices, isMultiple } = brandStatusMeta;
        const rawList = Array.isArray(val) ? val.filter(Boolean) : (val != null && val !== "" ? [typeof val === "string" ? val.trim() : val] : []);
        if (rawList.length === 0) continue;
        const resolved = rawList.map((raw) => {
          const exact = brandStatusChoices.find((c) => c.trim().toLowerCase() === String(raw).trim().toLowerCase());
          return exact != null ? exact : String(raw).trim();
        }).filter(Boolean);
        if (resolved.length === 0) continue;
        fields[airtableKey] = isMultiple ? resolved : resolved[0];
      } else if (Array.isArray(val)) {
        fields[airtableKey] = val.filter(Boolean);
      } else if (val !== null && val !== "") {
        let out = typeof val === "string" ? val.trim() : val;
        if (formKey === "yearBrandLaunched") out = String(out).trim();
        fields[airtableKey] = out;
      }
    }
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "No fields to update" });
    }
    const updated = await base(F.brandBasics.table).update(recordId, fields);
    res.json({
      success: true,
      brand: {
        id: updated.id,
        name: updated.fields[F.brandBasics.name]
      }
    });
  } catch (error) {
    const msg = error?.message || String(error);
    const code = error?.statusCode;
    const airErr = error?.error;
    console.error("[Brand Basics] 500 -", msg, "| statusCode:", code, "| airtableError:", airErr);
    console.error("[Brand Basics] Fields sent:", JSON.stringify(fields, null, 2));
    res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: msg,
      airtableStatus: code,
      airtableError: airErr
    });
  }
}

// Sustainability & ESG: form field → Airtable column. Matches form options and Airtable schema.
// Single-select options (must exist in Airtable): Sustainability Programs, ESG Reporting, Carbon Footprint Tracking.
// Long text: Energy Efficiency Initiatives, Waste Reduction Programs.
const SUSTAINABILITY_ESG_FORM_TO_AIRTABLE = {
  sustainabilityPrograms: "Sustainability Programs",
  esgReporting: "ESG Reporting",
  carbonTracking: "Carbon Footprint Tracking",
  energyEfficiency: "Energy Efficiency Initiatives",
  wasteReduction: "Waste Reduction Programs"
};

// Valid single-select values (exact match). Do not send values not in this set to avoid "create new option" errors.
const SUSTAINABILITY_ESG_SELECT_OPTIONS = new Set([
  "Yes - Comprehensive", "Yes - Standard", "No", "Planned",
  "Yes - Annual", "Yes - Quarterly",
  "Yes"
]);

export async function updateSustainabilityEsgByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const fields = {};
    const selectCols = ["Sustainability Programs", "ESG Reporting", "Carbon Footprint Tracking"];
    for (const [formKey, airtableCol] of Object.entries(SUSTAINABILITY_ESG_FORM_TO_AIRTABLE)) {
      const val = body[formKey];
      if (val === undefined) continue;
      const str = typeof val === "string" ? val.trim() : (val != null ? String(val).trim() : "");
      if (str === "") {
        continue; // Skip empty; don't send "" to single-select (can cause issues)
      }
      if (selectCols.includes(airtableCol) && !SUSTAINABILITY_ESG_SELECT_OPTIONS.has(str)) {
        continue; // Skip invalid select values to avoid "create new option" error
      }
      fields[airtableCol] = str;
    }
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "No fields to update" });
    }
    await upsertLinkedRecord(base, F.sustainabilityEsg.table, recordId, brandName, fields, "Brand", false, Object.keys(SUSTAINABILITY_ESG_FORM_TO_AIRTABLE).map(k => SUSTAINABILITY_ESG_FORM_TO_AIRTABLE[k]));
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Sustainability & ESG:", error.message, error.statusCode, error.error);
    const msg = String(error.message || "");
    const isSelectOptionError = /insufficient permissions to create new select option/i.test(msg);
    const details = isSelectOptionError
      ? "A Sustainability/ESG single-select value is not in the allowed list. Valid options: Yes - Comprehensive, Yes - Standard, Yes - Annual, Yes - Quarterly, Yes, No, Planned."
      : msg;
    res.status(500).json({
      success: false,
      error: isSelectOptionError ? "Airtable select option mismatch" : "Internal Server Error",
      details
    });
  }
}

// Shared: get brand name from Brand Basics record for linked-table PATCH handlers
async function getBrandNameFromBasics(base, recordId) {
  const brandRecord = await base(F.brandBasics.table).find(recordId);
  return (brandRecord.fields[F.brandBasics.name] || "").toString().trim();
}

// Find or create linked record; update with fields. If create, set Brand Name and optionally link to brand.
// When updating, only send field names that exist on the record to avoid Airtable "unknown field" errors.
// allowedFieldNames: optional Set/array of Airtable column names to always include (for checkbox columns that may not appear in existing.fields).
// When createWithScalarOnly is true, create uses only non-boolean fields (avoids checkbox columns that may not exist).
// updateOptions: optional object passed to Airtable update (e.g. { typecast: true }) for multi-select/text conversion.
async function upsertLinkedRecord(base, tableName, brandRecordId, brandName, fields, linkFieldName = "Brand", createWithScalarOnly = false, allowedFieldNames = null, updateOptions = null) {
  const existing = await findLinkedRecordByBrand(base, tableName, brandRecordId, brandName);
  if (existing) {
    const validKeys = new Set(Object.keys(existing.fields || {}));
    const allowed = allowedFieldNames ? new Set(Array.isArray(allowedFieldNames) ? allowedFieldNames : allowedFieldNames) : null;
    const safeFields = {};
    for (const [k, v] of Object.entries(fields)) {
      if (validKeys.has(k) || (allowed && allowed.has(k))) safeFields[k] = v;
    }
    const reportTypesCol = "Report Types Required";
    if (fields[reportTypesCol] !== undefined && !(reportTypesCol in safeFields)) {
      console.error("[upsertLinkedRecord] Report Types field was DROPPED: validKeys.has?", validKeys.has(reportTypesCol), "allowed.has?", allowed && allowed.has(reportTypesCol), "allowed sample:", allowed ? Array.from(allowed).slice(0, 5) : null);
    } else if (reportTypesCol in safeFields) {
      console.log("[upsertLinkedRecord] Report Types in safeFields, value:", safeFields[reportTypesCol]);
    }
    if (Object.keys(safeFields).length > 0) {
      let toUpdate = { ...safeFields };
      for (let retries = 0; retries < 5; retries++) {
        try {
          const opts = updateOptions && typeof updateOptions === "object" ? updateOptions : undefined;
          await base(tableName).update(existing.id, toUpdate, opts);
          break;
        } catch (updateErr) {
          const msg = updateErr && updateErr.message ? String(updateErr.message) : "";
          const unknownMatch = msg.match(/Unknown field name:\s*['"](.+?)['"]/i) || msg.match(/Unknown field name:\s*(.+?)(?:\s|$)/i);
          const invalidMatch = msg.match(/Field\s+['"](.+?)['"]\s+cannot accept/i) || msg.match(/Field\s+(.+?)\s+cannot accept/i);
          const reportTypesCol = "Report Types Required";
          if ((updateErr.error === "UNKNOWN_FIELD_NAME" || /unknown field/i.test(msg)) && unknownMatch) {
            const badField = unknownMatch[1].trim();
            console.error("[upsertLinkedRecord] Airtable unknown field:", badField, "full error:", msg);
            if (badField === reportTypesCol || badField.includes("Report Types")) {
              throw new Error("Airtable rejected Report Types field: " + msg);
            }
            delete toUpdate[badField];
            if (Object.keys(toUpdate).length === 0) {
              console.error("[upsertLinkedRecord] All fields were stripped; Airtable rejected or unknown:", Object.keys(safeFields));
              throw new Error("Update failed: one or more fields were rejected by Airtable. Check that field names and values (e.g. multi-select options) match your base.");
            }
          } else if ((updateErr.error === "INVALID_VALUE_FOR_COLUMN" || /cannot accept.*value/i.test(msg)) && invalidMatch) {
            const badField = invalidMatch[1].trim();
            console.error("[upsertLinkedRecord] Airtable invalid value for field:", badField, "full error:", msg);
            if (badField === reportTypesCol || badField.includes("Report Types")) {
              throw new Error("Airtable rejected Report Types value: " + msg);
            }
            delete toUpdate[badField];
            if (Object.keys(toUpdate).length === 0) {
              console.error("[upsertLinkedRecord] All fields were stripped; invalid value for:", badField);
              throw new Error("Update failed: one or more fields were rejected by Airtable. Check that field names and values (e.g. multi-select options) match your base.");
            }
          } else {
            throw updateErr;
          }
        }
      }
    }
    return { updated: true, id: existing.id };
  }
  const fieldsForCreate = createWithScalarOnly
    ? Object.fromEntries(Object.entries(fields).filter(([, v]) => typeof v !== "boolean"))
    : fields;
  const createFields = { ...fieldsForCreate, "Brand Name": brandName };
  const linkFieldNames = [linkFieldName, "Brand Setup - Brand Basics", "Brand_Basic_ID"];
  let lastErr;
  for (const linkName of linkFieldNames) {
    const toCreate = { ...createFields };
    if (brandRecordId && linkName) toCreate[linkName] = [brandRecordId];
    try {
      const created = await base(tableName).create(toCreate);
      return { updated: false, id: created.id };
    } catch (createErr) {
      lastErr = createErr;
      if (createErr.message && /unknown field|invalid.*field|could not find field/i.test(createErr.message)) {
        continue;
      }
      throw createErr;
    }
  }
  try {
    const created = await base(tableName).create(createFields);
    return { updated: false, id: created.id };
  } catch (createErr) {
    throw lastErr || createErr;
  }
}

// Brand Footprint PATCH
export async function updateBrandFootprintByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const LOCATION_TYPE_FORM_NAMES = ["locationTypeUrban", "locationTypeSuburban", "locationTypeResort", "locationTypeAirport", "locationTypeSmallMetro", "locationTypeInterstate"];
    const EXPERIENCE_PERCENT_FORM_NAMES = ["newBuildExperience", "conversionExperience", "turnaroundExperience", "renovationExperience", "typicalManagedPercent", "typicalFranchisedPercent"];
    const FOOTPRINT_TEXT_FIELDS = ["figuresAsOf", "specificMarkets"];
    const fields = {};
    for (const { form, airtable } of FOOTPRINT_FORM_TO_AIRTABLE) {
      const val = body[form];
      if (val === undefined) continue;
      if (FOOTPRINT_TEXT_FIELDS.includes(form)) {
        fields[airtable] = val !== null && val !== "" ? (typeof val === "string" ? val.trim() : val) : "";
      } else {
        const num = parseNumber(val);
        if (LOCATION_TYPE_FORM_NAMES.includes(form) || EXPERIENCE_PERCENT_FORM_NAMES.includes(form)) {
          fields[airtable] = typeof num === "number" && num >= 0 && num <= 100 ? num / 100 : num;
        } else {
          fields[airtable] = num;
        }
      }
    }
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "No fields to update" });
    }
    await upsertLinkedRecord(base, F.brandFootprint.table, recordId, brandName, fields);
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Brand Footprint:", error);
    return res.status(500).json({ success: false, error: "Internal Server Error", details: error.message });
  }
}

// Loyalty & Commercial PATCH
export async function updateLoyaltyCommercialByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const LC_PERCENT = ["typicalLoyaltyRoomsPercent", "typicalDirectBookingPercent", "typicalOTAReliancePercent", "otaCommissionPercent", "crsUsagePercent", "websiteAppConvRatesPercent"];
    const LC_TEXT = ["typicalLoyaltyProgramName"];
    const fields = {};
    for (const { form, airtable } of LOYALTY_COMMERCIAL_FORM_TO_AIRTABLE) {
      const val = body[form];
      if (val === undefined) continue;
      if (LC_TEXT.includes(form)) {
        fields[airtable] = val !== null && val !== "" ? (typeof val === "string" ? val.trim() : val) : "";
      } else if (LC_PERCENT.includes(form)) {
        const num = parseNumber(val);
        fields[airtable] = typeof num === "number" && num >= 0 && num <= 100 ? num / 100 : num;
      } else {
        const num = parseNumber(val);
        fields[airtable] = typeof num === "number" && !Number.isNaN(num) ? num : (val !== null && val !== "" ? String(val).trim() : "");
      }
    }
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "No fields to update" });
    }
    await upsertLinkedRecord(base, F.loyaltyCommercial.table, recordId, brandName, fields);
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Loyalty & Commercial:", error);
    return res.status(500).json({ success: false, error: "Internal Server Error", details: error.message });
  }
}

// Project Fit: form field → Airtable (simple fields + checkbox groups)
const PROJECT_FIT_FORM_TO_AIRTABLE = { ...PROJECT_FIT_AIRTABLE_TO_FORM };
const PROJECT_FIT_NUMERIC_FIELDS = new Set([
  "idealRoomCountMin", "idealRoomCountMax", "idealProjectSizeMin", "idealProjectSizeMax",
  "minReqOperatorExperienceYears", "minLeadTimeMonths",
  "milestoneOperatorSelectionMinMonths", "milestoneConstructionStartMinMonths",
  "milestoneSoftOpeningMinMonths", "milestoneGrandOpeningMinMonths"
]);
// Priority Markets and Markets to Avoid are written to single Airtable multi-select columns (not checkbox groups).
// idealProjectTypes, idealBuildingTypes, idealAgreementTypes, projectStage, ownerInvolvementLevel, ownerNonNegotiableTypes, capitalStatus, brandStatus, feeExpectationVsMarket, exitHorizon → single Airtable multi-select columns (handled below)
const PROJECT_FIT_CHECKBOX_GROUPS = [];

export async function updateProjectFitByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const fields = {};
    for (const [formName, airtableCol] of Object.entries(PROJECT_FIT_FORM_TO_AIRTABLE)) {
      const val = body[formName];
      if (val === undefined) continue;
      if (Array.isArray(val)) {
        fields[airtableCol] = val.filter(Boolean).map((s) => (typeof s === "string" ? s.trim() : s));
      } else if (val !== null && val !== "") {
        const str = typeof val === "string" ? val.trim() : String(val);
        if (formName === "coBrandingAllowed" || formName === "brandedResidencesAllowed" || formName === "mixedUseAllowed") {
          // Send "Yes"/"No" for Single select; Airtable Checkbox would need true/false
          fields[airtableCol] = str.toLowerCase() === "yes" || str === "1" || str.toLowerCase() === "true" ? "Yes" : "No";
        } else if (PROJECT_FIT_NUMERIC_FIELDS.has(formName)) {
          const num = parseFloat(String(val).replace(/[^0-9.-]/g, ""));
          if (!Number.isNaN(num)) fields[airtableCol] = num;
        } else {
          fields[airtableCol] = str;
        }
      }
    }
    // Build allowed list from all possible Project Fit columns so upsertLinkedRecord doesn't strip fields missing from existing.fields
    const pfAllowedCols = [
      ...Object.values(PROJECT_FIT_FORM_TO_AIRTABLE),
      "Priority Markets",
      "Markets to Avoid",
      "Other - Priority Markets Text",
      "Other - Markets to Avoid Text",
      "Other (Text) - Owner Non-Negotiables",
      "Flexibility On Dates",
      "Acceptable Project Type",
      "Acceptable Building Types",
      "Acceptable Agreements Type",
      "Acceptable Project Stages",
      "Acceptable Owner Involvement Levels",
      "Owner Non-Negotiables",
      "Acceptable Capital Status at Engagement",
      "Brand Status Scenarios You Will Consider",
      "Acceptable Fee Expectations vs Market",
      "Acceptable Exit Horizon"
    ];
    for (const { formKey, cols } of PROJECT_FIT_CHECKBOX_GROUPS) {
      const selected = body[formKey];
      const arr = Array.isArray(selected) ? selected : selected ? [selected] : [];
      const set = new Set(arr.map((s) => (typeof s === "string" ? s.trim() : String(s))));
      for (const { airtableColumn, formValue } of cols) {
        fields[airtableColumn] = set.has(formValue);
      }
    }
    // Priority Markets, Other - Priority Markets Text, Markets to Avoid, Other - Markets to Avoid Text (form → Airtable)
    if (body.priorityMarkets !== undefined) {
      const priorityArr = Array.isArray(body.priorityMarkets) ? body.priorityMarkets : body.priorityMarkets ? [body.priorityMarkets] : [];
      fields["Priority Markets"] = priorityArr.map((s) => (typeof s === "string" ? s.trim() : String(s))).filter(Boolean);
    }
    if (body.marketsToAvoid !== undefined) {
      const avoidArr = Array.isArray(body.marketsToAvoid) ? body.marketsToAvoid : body.marketsToAvoid ? [body.marketsToAvoid] : [];
      fields["Markets to Avoid"] = avoidArr.map((s) => (typeof s === "string" ? s.trim() : String(s))).filter(Boolean);
    }
    // Single select: Flexibility On Dates (ensure it is always sent when present)
    if (body.dateFlexibility !== undefined && body.dateFlexibility !== null) {
      const dateFlexStr = typeof body.dateFlexibility === "string" ? body.dateFlexibility.trim() : String(body.dateFlexibility).trim();
      if (dateFlexStr !== "") {
        fields["Flexibility On Dates"] = dateFlexStr;
      }
    }
    // Acceptable Project Type, Building Types, Agreements Type, Project Stages, Owner Involvement, Owner Non-Negotiables (multi-select)
    const multiSelectProjectFit = [
      { formKey: "idealProjectTypes", airtableCol: "Acceptable Project Type" },
      { formKey: "idealBuildingTypes", airtableCol: "Acceptable Building Types" },
      { formKey: "idealAgreementTypes", airtableCol: "Acceptable Agreements Type" },
      { formKey: "projectStage", airtableCol: "Acceptable Project Stages" },
      { formKey: "ownerInvolvementLevel", airtableCol: "Acceptable Owner Involvement Levels" },
      { formKey: "ownerNonNegotiableTypes", airtableCol: "Owner Non-Negotiables" },
      { formKey: "capitalStatus", airtableCol: "Acceptable Capital Status at Engagement" },
      { formKey: "brandStatus", airtableCol: "Brand Status Scenarios You Will Consider" },
      { formKey: "feeExpectationVsMarket", airtableCol: "Acceptable Fee Expectations vs Market" },
      { formKey: "exitHorizon", airtableCol: "Acceptable Exit Horizon" }
    ];
    for (const { formKey, airtableCol } of multiSelectProjectFit) {
      const val = body[formKey];
      if (val === undefined) continue;
      const arr = Array.isArray(val) ? val : val ? [val] : [];
      fields[airtableCol] = arr.map((s) => (typeof s === "string" ? s.trim() : String(s))).filter(Boolean);
    }
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "No fields to update" });
    }
    await upsertLinkedRecord(base, F.projectFit.table, recordId, brandName, fields, "Brand", true, pfAllowedCols, { typecast: true });
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Project Fit:", error);
    const message = (error && error.message) ? error.message : "Internal Server Error";
    const airtableError = (error && (error.error || error.statusCode)) ? String(error.error || error.statusCode) : undefined;
    return res.status(500).json({
      success: false,
      error: message,
      details: message,
      ...(airtableError && { airtableError }),
    });
  }
}

// Portfolio & Performance PATCH (percent fields stored as 0–1; reportTypes as checkbox columns)
const PORTFOLIO_PERCENT_FORM_IDS = ["revparImprovement", "occupancyImprovement", "noiImprovement", "ownerRetention", "renewalRate"];

export async function updatePortfolioPerformanceByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const fields = {};
    // Always sync Report Types multi-select to Airtable (column name must match your base exactly)
    const reportTypesCol = "Report Types Required";
    if (body.reportTypes !== undefined) {
      const arr = Array.isArray(body.reportTypes) ? body.reportTypes : body.reportTypes ? [body.reportTypes] : [];
      const cleaned = arr.map((s) => (typeof s === "string" ? s.trim() : String(s))).filter(Boolean);
      // Airtable REST API accepts multi-select as array of strings; use strings (typecast: true helps)
      fields[reportTypesCol] = cleaned;
    }
    for (const [formId, airtableCols] of Object.entries(PORTFOLIO_PERFORMANCE_FORM_TO_AIRTABLE)) {
      const val = body[formId];
      if (val === undefined) continue;
      const col = airtableCols[0];
      if (formId === "reportTypes") {
        continue;
      }
      if (formId === "portfolioMetricsAsOf" && val) {
        const s = String(val).trim();
        fields[col] = /^\d{4}-\d{2}-\d{2}/.test(s) ? s.slice(0, 10) : s;
        continue;
      }
      if (PORTFOLIO_PERCENT_FORM_IDS.includes(formId) && val !== "" && val !== null && val !== undefined) {
        const n = parseFloat(String(val).replace(/[^0-9.-]/g, ""));
        fields[col] = !Number.isNaN(n) ? n / 100 : val;
        continue;
      }
      if (formId === "avgContractTerm") {
        const arr = Array.isArray(val) ? val : val !== null && val !== "" ? [typeof val === "string" ? val.trim() : val] : [];
        fields[col] = arr.filter(Boolean).map((s) => (typeof s === "string" ? s.trim() : String(s)));
        continue;
      }
      fields[col] = val !== null && val !== "" ? (typeof val === "string" ? val.trim() : val) : "";
    }
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "No fields to update" });
    }
    const existing = await findLinkedRecordByBrand(base, F.portfolioPerformance.table, recordId, brandName);
    if (existing && existing.id) {
      const recordKeys = Object.keys(existing.fields || {});
      const toActualKey = (ourKey) => recordKeys.find((ak) => ak === ourKey || ak.trim() === ourKey.trim() || (typeof ak.normalize === "function" && ourKey.normalize("NFKC") === ak.normalize("NFKC"))) || ourKey;
      const toUpdate = {};
      for (const [k, v] of Object.entries(fields)) {
        if (v === undefined) continue;
        if (v === "" || v === null) continue;
        if (Array.isArray(v) && v.length === 0) continue;
        toUpdate[toActualKey(k)] = v;
      }
      if (body.reportTypes !== undefined) toUpdate[toActualKey(reportTypesCol)] = fields[reportTypesCol] || [];
      if (Object.keys(toUpdate).length === 0) {
        return res.status(400).json({ success: false, error: "No fields to update" });
      }
      let lastErr;
      for (let attempt = 0; attempt < 20; attempt++) {
        try {
          await base(F.portfolioPerformance.table).update(existing.id, toUpdate, { typecast: true });
          return res.json({ success: true });
        } catch (updateErr) {
          lastErr = updateErr;
          const msg = (updateErr && updateErr.message) || "";
          const unknownMatch = msg.match(/Unknown field name:\s*["\u201C\u201D]?([^"\u201C\u201D\n]+)["\u201C\u201D]?/i);
          if ((updateErr.error === "UNKNOWN_FIELD_NAME" || unknownMatch) && Object.keys(toUpdate).length > 1) {
            const reportedName = (unknownMatch ? unknownMatch[1].trim() : "").replace(/["\u201C\u201D]$/, "");
            let toDelete = reportedName
              ? Object.keys(toUpdate).find((k) => k === reportedName || k.trim() === reportedName || (typeof k.normalize === "function" && reportedName.normalize("NFKC") === k.normalize("NFKC")))
              : null;
            if (!toDelete && msg.includes("Unknown field")) {
              toDelete = Object.keys(toUpdate).find((k) => msg.includes(k));
            }
            if (toDelete) {
              delete toUpdate[toDelete];
              continue;
            }
          }
          throw updateErr;
        }
      }
      if (lastErr) throw lastErr;
    }
    // No linked record: create one (same create logic as upsertLinkedRecord).
    const linkFieldNames = ["Brand", "Brand Setup - Brand Basics", "Brand_Basic_ID"];
    const createFields = { ...fields, "Brand Name": brandName };
    for (const linkName of linkFieldNames) {
      try {
        const toCreate = { ...createFields };
        if (recordId && linkName) toCreate[linkName] = [recordId];
        await base(F.portfolioPerformance.table).create(toCreate);
        return res.json({ success: true });
      } catch (createErr) {
        if (createErr.message && /unknown field|invalid.*field|could not find field/i.test(createErr.message)) continue;
        throw createErr;
      }
    }
    await base(F.portfolioPerformance.table).create(createFields);
    return res.json({ success: true });
  } catch (error) {
    const message = (error && error.message) ? error.message : "Internal Server Error";
    const desc = error && (error.description || (error.error && error.error.message));
    const airtableError = error && (error.error || error.statusCode);
    return res.status(500).json({
      success: false,
      error: message,
      details: desc || message,
      ...(airtableError && { airtableError: String(airtableError) }),
    });
  }
}

// Brand Standards: form field → Airtable column (multi-select columns + Other text + scalars).
const BRAND_STANDARDS_MULTI_SELECT = [
  { formKey: 'brandParkingProgramType', airtableCol: 'Parking Program' },
  { formKey: 'brandSustainability', airtableCol: 'Sustainability Features' },
  { formKey: 'brandRequiredAmenities', airtableCol: 'Additional Amenities' },
  { formKey: 'brandCompliance', airtableCol: 'Compliance & Safety' }
];
const BRAND_STANDARDS_SCALAR = [
  { form: 'brandSustainabilityOther', airtable: 'Other Sustainability Text' },
  { form: 'brandRequiredAmenitiesOther', airtable: 'Other Amenities Text - Amenities' },
  { form: 'brandComplianceOther', airtable: 'Other Text - Compliance' },
  { form: 'brandQaExpectations', airtable: 'Typical QA / Brand Standards Expectations' },
  { form: 'brandStandardsNotes', airtable: 'Additional Brand Standards Notes' },
  { form: 'brandParkingRequired', airtable: 'Parking Required' },
  { form: 'brandParkingSpacesCount', airtable: 'Typical Total Parking Spaces' }
];

// Brand Standards PATCH (multi-select columns + Other text + scalars)
export async function updateBrandStandardsByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const fields = {};
    for (const { formKey, airtableCol } of BRAND_STANDARDS_MULTI_SELECT) {
      const raw = body[formKey];
      const arr = Array.isArray(raw) ? raw : raw !== undefined && raw !== null && raw !== '' ? [raw] : [];
      const normalized = arr.map((s) => (typeof s === "string" ? s.trim() : String(s))).filter(Boolean);
      fields[airtableCol] = normalized;
    }
    for (const { form, airtable } of BRAND_STANDARDS_SCALAR) {
      const val = body[form];
      // Always include "Other" text fields so they save to Airtable even when empty
      const isOtherText = form === 'brandSustainabilityOther' || form === 'brandRequiredAmenitiesOther' || form === 'brandComplianceOther';
      if (val === undefined && !isOtherText) continue;
      const str = (val === undefined || val === null || val === '') ? '' : (typeof val === 'string' ? val.trim() : String(val));
      if (form === 'brandParkingSpacesCount' && str) {
        const num = parseFloat(String(str).replace(/,/g, ''));
        fields[airtable] = !Number.isNaN(num) ? num : str;
      } else {
        fields[airtable] = str;
      }
    }
    if (Object.keys(fields).length === 0) {
      return res.json({ success: true, message: "No changes to save" });
    }
    const allowedCols = [
      ...BRAND_STANDARDS_MULTI_SELECT.map(({ airtableCol }) => airtableCol),
      ...BRAND_STANDARDS_SCALAR.map(({ airtable }) => airtable)
    ];
    await upsertLinkedRecord(base, F.brandStandards.table, recordId, brandName, fields, "Brand", true, allowedCols, { typecast: true });
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Brand Standards:", error);
    const details = (error && error.message) ? error.message : String(error);
    const airtableCode = (error && error.error) ? error.error : undefined;
    return res.status(500).json({ success: false, error: "Internal Server Error", details, ...(airtableCode && { airtableError: airtableCode }) });
  }
}

// Fee Structure Basis column names (each can have different allowed options in Airtable).
const FEE_BASIS_COLUMNS = [
  'Basis - Typical Application Fee',
  'Basis - Typical Royalty Fee Range',
  'Basis - Typical Marketing Fee Range',
  'Basis - Typical Tech',
  'Basis - Typical Loyalty Program Fee',
  'Basis - Typical Reservation / Distribution Fee',
  'Basis - Typical Training Fee'
];

// Fee Structure PATCH (percent fields 0–100 → 0–1; basis normalized)
export async function updateFeeStructureByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const hasBasis = Object.keys(body).some(
      (k) => (k.endsWith("Basis") || k === "typicalApplicationFeeBasis") && body[k]
    );
    const basisChoicesByCol = {};
    if (hasBasis) {
      await Promise.all(
        FEE_BASIS_COLUMNS.map(async (col) => {
          basisChoicesByCol[col] = await getTableFieldChoiceNames(F.feeStructure.table, col);
        })
      );
    }
    const fields = {};
    for (const [formId, airtableCols] of Object.entries(FEE_FORM_TO_AIRTABLE)) {
      let val = body[formId];
      if (val === undefined) continue;
      const col = airtableCols[0];
      if (FEE_PERCENT_FORM_NAMES.includes(formId) && val !== "" && val !== null && val !== undefined) {
        const n = parseFloat(String(val).replace(/[^0-9.-]/g, ""));
        fields[col] = !Number.isNaN(n) ? n / 100 : val;
        continue;
      }
      if ((formId.endsWith("Basis") || formId === "typicalApplicationFeeBasis") && val) {
        val = normalizeFeeBasisValue(val);
        const allowed = basisChoicesByCol[col];
        if (Array.isArray(allowed) && allowed.length > 0) {
          const exact = allowed.find((c) => String(c).trim().toLowerCase() === String(val).trim().toLowerCase());
          if (exact) {
            fields[col] = exact;
          }
          continue;
        }
      }
      fields[col] = val !== null && val !== "" ? (typeof val === "string" ? val.trim() : val) : "";
    }
    if (Object.keys(fields).length === 0) {
      console.warn("[Fee Structure] 400: No matching fields. Body keys:", Object.keys(body));
      return res.status(400).json({
        success: false,
        error: "No fields to update",
        detail: "Request body had no matching Fee Structure fields. Expected keys like typicalRoyaltyPercentBasis, typicalRoyaltyPercentMin, typicalRoyaltyPercentMax, typicalApplicationFeeBasis, etc.",
        receivedKeys: Object.keys(body)
      });
    }
    await upsertLinkedRecord(base, F.feeStructure.table, recordId, brandName, fields);
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Fee Structure:", error);
    const message = (error && error.message) ? error.message : "Internal Server Error";
    const airtableError = (error && (error.error || error.statusCode)) ? String(error.error || error.statusCode) : undefined;
    return res.status(500).json({
      success: false,
      error: message,
      details: message,
      ...(airtableError && { airtableError }),
    });
  }
}

// Deal Terms PATCH
export async function updateDealTermsByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const fields = {};
    for (const [formId, airtableCols] of Object.entries(DEAL_TERMS_FORM_TO_AIRTABLE)) {
      const val = body[formId];
      if (val === undefined) continue;
      const col = airtableCols[0];
      fields[col] = val !== null && val !== "" ? (typeof val === "string" ? val.trim() : val) : "";
    }
    if (Object.keys(fields).length === 0) {
      return res.json({ success: true, message: "No Deal Terms fields to update" });
    }
    await upsertLinkedRecord(base, F.dealTerms.table, recordId, brandName, fields);
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Deal Terms:", error);
    return res.status(500).json({ success: false, error: "Internal Server Error", details: error.message });
  }
}

// Operational Support PATCH (scalar fields + typesOfIncentives checkboxes + service checkbox groups).
// Prefer the Operational Support record linked from Brand Basics so we update the same record the UI prefill uses.
export async function updateOperationalSupportByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const bodyKeys = Object.keys(body);
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const fields = {};
    for (const { form, airtable } of OPERATIONAL_SUPPORT_FORM_TO_AIRTABLE) {
      const val = body[form];
      if (val === undefined) continue;
      fields[airtable] = val !== null && val !== "" ? (typeof val === "string" ? val.trim() : val) : "";
    }
    if (body.typesOfIncentives !== undefined) {
    const incentivesArr = Array.isArray(body.typesOfIncentives) ? body.typesOfIncentives : body.typesOfIncentives ? [body.typesOfIncentives] : [];
      fields["Incentive Types"] = incentivesArr.map((s) => (typeof s === "string" ? s.trim() : String(s))).filter(Boolean);
    }
    for (const { formKey, airtableCol } of OPERATIONAL_SUPPORT_SERVICE_MULTI_SELECT) {
      const val = body[formKey];
      if (val === undefined) continue;
      const arr = Array.isArray(val) ? val : val ? [val] : [];
      fields[airtableCol] = arr.map((s) => (typeof s === "string" ? s.trim() : String(s))).filter(Boolean);
    }
    if (Object.keys(fields).length === 0) {
      console.warn("[operational-support] No fields to save. Body keys received:", bodyKeys.slice(0, 40).join(", "), "total:", bodyKeys.length);
      return res.json({ success: true, message: "No changes to save" });
    }
    const opAllowedCols = [
      ...OPERATIONAL_SUPPORT_FORM_TO_AIRTABLE.map(({ airtable }) => airtable),
      "Incentive Types",
      ...OPERATIONAL_SUPPORT_SERVICE_MULTI_SELECT.map(({ airtableCol }) => airtableCol)
    ];

    // Prefer the Operational Support record linked from Brand Basics (same record the prefill uses).
    let opRecordId = null;
    try {
      const brandRecord = await base(F.brandBasics.table).find(recordId);
      const opSupportLink = (brandRecord.fields || {})["Brand Setup - Operational Support"];
      if (opSupportLink && Array.isArray(opSupportLink) && opSupportLink.length > 0 && typeof opSupportLink[0] === "string") {
        opRecordId = opSupportLink[0];
      }
    } catch (e) {
      console.warn("[operational-support] No linked op record from Brand Basics:", (e && e.message) || e);
    }

    if (opRecordId) {
      const existing = await base(F.operationalSupport.table).find(opRecordId);
      const validKeys = new Set(Object.keys(existing.fields || {}));
      const allowed = new Set(opAllowedCols);
      const safeFields = {};
      for (const [k, v] of Object.entries(fields)) {
        if (validKeys.has(k) || allowed.has(k)) safeFields[k] = v;
      }
      const safeCount = Object.keys(safeFields).length;
      console.log("[operational-support] brand", recordId, "opRecordId", opRecordId, "mappedFields", Object.keys(fields).length, "safeFields", safeCount);
      if (safeCount === 0) {
        console.warn("[operational-support] No safe fields to write; Airtable update skipped.");
        return res.json({ success: true, message: "No fields to update" });
      }
      let updateSucceeded = false;
      if (safeCount > 0) {
        let toUpdate = { ...safeFields };
        for (let retries = 0; retries < 5; retries++) {
          try {
            await base(F.operationalSupport.table).update(opRecordId, toUpdate, { typecast: true });
            updateSucceeded = true;
            console.log("[operational-support] Updated record", opRecordId, "fields:", Object.keys(toUpdate).length);
            break;
          } catch (updateErr) {
            const msg = updateErr && updateErr.message ? String(updateErr.message) : "";
            const unknownMatch = msg.match(/Unknown field name:\s*['"](.+?)['"]/i) || msg.match(/Unknown field name:\s*(.+?)(?:\s|$)/i);
            const invalidMatch = msg.match(/Field\s+['"](.+?)['"]\s+cannot accept/i) || msg.match(/Field\s+(.+?)\s+cannot accept/i);
            if ((updateErr.error === "UNKNOWN_FIELD_NAME" || /unknown field/i.test(msg)) && unknownMatch) {
              const badField = unknownMatch[1].trim();
              console.warn("[operational-support] Airtable unknown field, stripping:", badField, "|", msg.slice(0, 120));
              delete toUpdate[badField];
              if (Object.keys(toUpdate).length === 0) break;
            } else if ((updateErr.error === "INVALID_VALUE_FOR_COLUMN" || /cannot accept.*value/i.test(msg)) && invalidMatch) {
              const badField = invalidMatch[1].trim();
              console.warn("[operational-support] Airtable invalid value, stripping:", badField, "|", msg.slice(0, 120));
              delete toUpdate[badField];
              if (Object.keys(toUpdate).length === 0) break;
            } else {
              throw updateErr;
            }
          }
        }
      }
      if (!updateSucceeded && safeCount > 0) {
        console.error("[operational-support] Update failed or all fields stripped for", opRecordId);
        return res.status(500).json({ success: false, error: "Airtable rejected the update (check server logs for stripped fields).", details: "One or more fields may have invalid values or unknown column names." });
      }
      return res.json({ success: true });
    }

    console.log("[operational-support] No linked op record; using find/create by brand. recordId=", recordId, "brandName=", brandName);
    await upsertLinkedRecord(base, F.operationalSupport.table, recordId, brandName, fields, "Brand", true, opAllowedCols);
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Operational Support:", error);
    const message = (error && error.message) ? error.message : "Internal Server Error";
    const airtableError = (error && (error.error || error.statusCode)) ? String(error.error || error.statusCode) : undefined;
    return res.status(500).json({
      success: false,
      error: message,
      details: message,
      ...(airtableError && { airtableError }),
    });
  }
}

// Legal Terms PATCH
export async function updateLegalTermsByBrandId(req, res) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid brand record ID is required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const base = getBase();
    const brandName = await getBrandNameFromBasics(base, recordId);
    const fields = {};
    for (const { form, airtable } of LEGAL_TERMS_FORM_TO_AIRTABLE) {
      const val = body[form];
      if (val === undefined) continue;
      fields[airtable] = val !== null && val !== "" ? (typeof val === "string" ? val.trim() : val) : "";
    }
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "No fields to update" });
    }
    await upsertLinkedRecord(base, F.legalTerms.table, recordId, brandName, fields);
    return res.json({ success: true });
  } catch (error) {
    console.error("Error updating Legal Terms:", error);
    const message = (error && error.message) ? error.message : "Internal Server Error";
    const airtableError = (error && (error.error || error.statusCode)) ? String(error.error || error.statusCode) : undefined;
    return res.status(500).json({
      success: false,
      error: message,
      details: message,
      ...(airtableError && { airtableError }),
    });
  }
}

// Helper functions
function parseNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number') return value;
  const parsed = parseFloat(String(value).replace(/[^0-9.-]/g, ''));
  return isNaN(parsed) ? 0 : parsed;
}

function parsePercent(value) {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number') return value;
  const parsed = parseFloat(String(value).replace(/[^0-9.-]/g, ''));
  return isNaN(parsed) ? 0 : parsed;
}
