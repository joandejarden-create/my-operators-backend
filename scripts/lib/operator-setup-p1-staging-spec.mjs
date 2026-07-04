/**
 * P1 field spec for staging proof (values from public/fixtures/operator-alignment-field-options.json).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.resolve(__dirname, "../../public/fixtures/operator-alignment-field-options.json");

export const P1_SANDBOX_COMPANY_NAME =
  "P1 Staging Proof Sandbox (Do Not Use Production)";

export const P1_TEST_NAME_BLOCKLIST = [
  "e2e shadow",
  "e2e validation",
  "shadowval",
  "shadow test",
  "gold test",
  "example-operator-e2e",
];

export function isBlockedTestOperatorName(name) {
  const n = String(name || "").toLowerCase();
  return P1_TEST_NAME_BLOCKLIST.some((h) => n.includes(h));
}

function opt(fixtureKey, index = 0) {
  const j = JSON.parse(fs.readFileSync(FIXTURES, "utf8"));
  const arr = j[fixtureKey];
  if (!Array.isArray(arr) || !arr.length) throw new Error("Missing fixture options: " + fixtureKey);
  return arr[index];
}

/** Expected values for write + readback (exact live options). */
export function buildP1StagingPayload(overrides = {}) {
  const today = new Date().toISOString().slice(0, 10);
  return {
    companyName: P1_SANDBOX_COMPANY_NAME,
    contactEmail: "staging-proof@dealality.invalid",
    contactName: "Staging Proof",
    website: "https://dealality-staging-proof.example",
    headquarters: "Miami, FL, United States",
    companyDescription:
      "Sandbox operator for P1 staging proof only. Not for production or investor publication.",
    primaryServiceModel: "Third-Party Management",
    yearEstablished: "2010",
    yearsInBusiness: "16",
    dataConfidenceLevel: opt("dataConfidence", 1),
    sourceType: [opt("sourceType", 0)],
    lastUpdatedDate: today,
    activeCountries: [opt("activeCountries", 0), opt("activeCountries", 14)],
    activeMarkets: [opt("activeMarkets", 0), opt("activeMarkets", 4)],
    marketPresenceType: [opt("marketPresenceType", 0)],
    serviceModelsSupported: [opt("serviceModelsSupported", 1), opt("serviceModelsSupported", 2)],
    chainScalesSupported: [opt("chainScalesSupported", 1), opt("chainScalesSupported", 2)],
    managementStructuresSupported: [opt("managementStructures", 0)],
    offeredServices: [
      opt("operatorServices", 0),
      opt("operatorServices", 2),
      opt("operatorServices", 5),
      opt("operatorServices", 12),
    ],
    newBuildOpeningExperience: opt("experienceLevel", 0),
    preOpeningSupportCapability: opt("experienceLevel", 0),
    ownerReportingLevel: opt("ownerReportingLevel", 2),
    revenueManagementCapability: opt("revenueMgmt", 1),
    salesPlatform: [opt("salesPlatform", 1), opt("salesPlatform", 2)],
    fbCapabilityLevel: opt("fbCapability", 2),
    brands: [],
    ...overrides,
  };
}

/**
 * P1 audit field definitions for readback / pipeline checks.
 */
export const P1_PIPELINE_FIELDS = [
  {
    id: "company_name",
    formKeys: ["companyName", "company_name"],
    airtableFields: ["company_name", "Company Name"],
    tables: ["Operator Setup - Master", "Operator Setup - Profile & Positioning"],
    explorerSection: "A. Profile Snapshot",
    oas: false,
    strategy: true,
  },
  {
    id: "operator_id",
    formKeys: ["operatorId", "operator_id"],
    airtableFields: ["operator_id"],
    tables: ["Operator Setup - Master"],
    system: true,
    explorerSection: "—",
    oas: false,
    strategy: false,
  },
  {
    id: "data_confidence",
    formKeys: ["dataConfidenceLevel"],
    airtableFields: ["Data Confidence Level", "dataConfidenceLevel"],
    tables: ["Operator Setup - Master"],
    explorerSection: "A. Profile Snapshot",
    oas: true,
    strategy: true,
  },
  {
    id: "source_type",
    formKeys: ["sourceType"],
    airtableFields: ["Source Type", "sourceType"],
    tables: ["Operator Setup - Master"],
    explorerSection: "F. Owner Reporting & Governance",
    oas: true,
    strategy: false,
  },
  {
    id: "last_updated",
    formKeys: ["lastUpdatedDate"],
    airtableFields: ["Last Updated Date", "lastUpdatedDate"],
    tables: ["Operator Setup - Master"],
    explorerSection: "A. Profile Snapshot",
    oas: true,
    strategy: false,
  },
  {
    id: "active_countries",
    formKeys: ["activeCountries"],
    airtableFields: ["Active Countries", "activeCountries"],
    tables: ["Operator Setup - Platform & Markets"],
    explorerSection: "B. Market Presence",
    oas: true,
    strategy: false,
  },
  {
    id: "active_markets",
    formKeys: ["activeMarkets"],
    airtableFields: ["Active Markets / Cities", "activeMarkets"],
    tables: ["Operator Setup - Platform & Markets"],
    explorerSection: "B. Market Presence",
    oas: true,
    strategy: false,
  },
  {
    id: "market_presence_type",
    formKeys: ["marketPresenceType"],
    airtableFields: ["Market Presence Type", "marketPresenceType"],
    tables: ["Operator Setup - Platform & Markets"],
    explorerSection: "B. Market Presence",
    oas: true,
    strategy: false,
  },
  {
    id: "service_models",
    formKeys: ["serviceModelsSupported"],
    airtableFields: ["Service Models Supported", "serviceModelsSupported"],
    tables: ["Operator Setup - Profile & Positioning", "Operator Setup - Platform & Markets"],
    explorerSection: "C. Operating Profile",
    oas: true,
    strategy: false,
  },
  {
    id: "chain_scales",
    formKeys: ["chainScalesSupported", "chainScale"],
    airtableFields: ["chainScalesSupported", "chainScale", "Chain Scales Supported"],
    tables: ["Operator Setup - Profile & Positioning"],
    explorerSection: "C. Operating Profile",
    oas: true,
    strategy: false,
  },
  {
    id: "management_structures",
    formKeys: ["managementStructuresSupported"],
    airtableFields: ["Management Structures Supported", "managementStructuresSupported"],
    tables: ["Operator Setup - Commercial Fit & Terms"],
    explorerSection: "C. Operating Profile",
    oas: true,
    strategy: false,
  },
  {
    id: "offered_services",
    formKeys: ["offeredServices"],
    airtableFields: ["Offered Services", "offeredServices"],
    tables: ["Operator Setup - Governance, Delivery & Diligence"],
    explorerSection: "D. Services & Platform",
    oas: true,
    strategy: false,
  },
  {
    id: "new_build",
    formKeys: ["newBuildOpeningExperience"],
    airtableFields: ["New-Build Opening Experience", "newBuildOpeningExperience"],
    tables: ["Operator Setup - Commercial Fit & Terms"],
    explorerSection: "E. Opening / Transition Support",
    oas: true,
    strategy: false,
  },
  {
    id: "pre_opening",
    formKeys: ["preOpeningSupportCapability"],
    airtableFields: ["Pre-Opening Support Capability", "preOpeningSupportCapability"],
    tables: ["Operator Setup - Commercial Fit & Terms"],
    explorerSection: "E. Opening / Transition Support",
    oas: true,
    strategy: false,
  },
  {
    id: "owner_reporting",
    formKeys: ["ownerReportingLevel"],
    airtableFields: ["Owner Reporting Level", "ownerReportingLevel"],
    tables: ["Operator Setup - Governance, Delivery & Diligence"],
    explorerSection: "F. Owner Reporting & Governance",
    oas: true,
    strategy: false,
  },
  {
    id: "revenue_mgmt",
    formKeys: ["revenueManagementCapability"],
    airtableFields: ["Revenue Management Capability", "revenueManagementCapability"],
    tables: ["Operator Setup - Governance, Delivery & Diligence"],
    explorerSection: "D. Services & Platform",
    oas: true,
    strategy: false,
  },
  {
    id: "sales_platform",
    formKeys: ["salesPlatform"],
    airtableFields: ["Sales Platform", "salesPlatform"],
    tables: ["Operator Setup - Governance, Delivery & Diligence"],
    explorerSection: "D. Services & Platform",
    oas: true,
    strategy: false,
  },
  {
    id: "fb_capability",
    formKeys: ["fbCapabilityLevel", "fBCapabilityLevel"],
    airtableFields: ["F&B Capability Level", "fbCapabilityLevel", "fBCapabilityLevel"],
    tables: ["Operator Setup - Governance, Delivery & Diligence"],
    explorerSection: "D. Services & Platform",
    oas: true,
    strategy: false,
  },
];

export function normalizeList(val) {
  if (val == null || val === "") return [];
  if (Array.isArray(val)) return val.map(String).map((s) => s.trim()).filter(Boolean);
  return String(val)
    .split(/[,;\n]/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export function valuesMatch(expected, actual) {
  const e = normalizeList(expected);
  const a = normalizeList(actual);
  if (!e.length) return { ok: false, reason: "no expected value" };
  if (!a.length) return { ok: false, reason: "empty actual" };
  if (e.length === 1 && a.length === 1) {
    return {
      ok: String(e[0]).toLowerCase() === String(a[0]).toLowerCase(),
      reason: e[0] !== a[0] ? `expected "${e[0]}", got "${a[0]}"` : "",
    };
  }
  const missing = e.filter(
    (x) => !a.some((y) => y.toLowerCase() === x.toLowerCase() || y.toLowerCase().includes(x.toLowerCase()))
  );
  return { ok: missing.length === 0, reason: missing.length ? `missing: ${missing.join(", ")}` : "" };
}

export function pickFromMergedFields(merged, keys) {
  for (const k of keys) {
    if (merged[k] != null && merged[k] !== "") return merged[k];
  }
  return null;
}

export function pickFromPrefill(prefill, keys) {
  for (const k of keys) {
    if (prefill[k] != null && prefill[k] !== "") return prefill[k];
  }
  return null;
}

export function explorerWouldShow(prefill, spec, payload) {
  if (spec.system) return { show: "N/A", note: "system field" };
  for (const k of spec.formKeys) {
    const v = prefill[k];
    if (v != null && v !== "" && !(Array.isArray(v) && !v.length)) {
      return { show: "Yes", note: "prefill key " + k };
    }
  }
  const exp = spec.formKeys.map((k) => payload[k]).find((x) => x != null && x !== "");
  if (exp) return { show: "Partial", note: "expected in payload; verify Explorer rail after save" };
  return { show: "No", note: "missing from prefill" };
}
