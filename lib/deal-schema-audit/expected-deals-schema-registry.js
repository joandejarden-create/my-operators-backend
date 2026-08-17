/**
 * Expected Airtable field specs for Deals workflow tables (from code maps + audit refinements).
 * Read-only audit input — does not mutate schema.
 *
 * Field spec shape:
 * { name, aliases?, classification?, notes?, deprecated? }
 */
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEALS_TABLE,
  LOCATION_PROPERTY_TABLE,
  MARKET_PERFORMANCE_TABLE,
  STRATEGIC_INTENT_TABLE,
  CONTACT_UPLOADS_TABLE,
  LEASE_STRUCTURE_TABLE,
  DEALS_STATUS_FIELD,
  DEALS_ONLY_FORM_FIELDS,
  DEALS_FORM_TO_AIRTABLE,
  LOCATION_FORM_TO_AIRTABLE,
  LOCATION_LINK_FIELD,
  LOCATION_LINK_ALIAS,
  MARKET_PERFORMANCE_LINK_FIELD,
  STRATEGIC_INTENT_LINK_FIELD,
  CONTACT_UPLOADS_LINK_FIELD,
  LEASE_STRUCTURE_LINK_FIELD,
  MP_DEAL_LINK_FIELD,
  CU_DEAL_LINK_FIELD,
  LS_DEAL_LINK_FIELD,
  LOCATION_PROPERTY_ID_FIELD,
  MARKET_PERFORMANCE_FIELD_NAMES,
  MP_FORM_TO_TABLE,
  STRATEGIC_INTENT_FORM_FIELDS,
  SI_FORM_TO_AIRTABLE,
  CONTACT_UPLOADS_FORM_FIELDS,
  CU_FORM_TO_AIRTABLE,
  CU_ATTACHMENT_AIRTABLE_FIELDS,
  LEASE_STRUCTURE_FORM_FIELDS,
  LS_FORM_TO_AIRTABLE,
  DEAL_READINESS_SCORE_AIRTABLE_FIELD,
  DEAL_READINESS_STAGE_AIRTABLE_FIELD,
  DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD,
  DEAL_READINESS_SUMMARY_AIRTABLE_FIELD,
} from "../../api/schemas/deal-setup-fields.js";
import { INTAKE_DEALS_USER_LINK_NAME } from "../../api/schemas/intake-deal-fields.js";
import { MAP_ODR_AIRTABLE } from "../../api/operator-deal-requests-fields.js";
import { BDR_TABLE } from "../../api/schemas/brand-deal-request-fields.js";
import { DEALS_COMPANY_LINK_FIELD } from "../pilot-provisioning/pilot-field-registry.js";
import { OAS_DEAL_DEALS_FIELD_NAMES, OAS_DEAL_SI_FIELD_NAMES, OAS_DEAL_MP_FIELD_NAMES } from "../operator-alignment-field-options.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..", "..");

/** @param {string} name @param {{ aliases?: string[], classification?: string, notes?: string, deprecated?: boolean }} [opts] */
export function fieldSpec(name, opts = {}) {
  return {
    name,
    aliases: opts.aliases || [],
    classification: opts.classification || "",
    notes: opts.notes || "",
    deprecated: Boolean(opts.deprecated),
  };
}

function mapFormFields(formFields, formToAirtable = {}) {
  return [...new Set(formFields.map((f) => formToAirtable[f] || f))];
}

function collectUnique(...arrays) {
  return [...new Set(arrays.flat().filter(Boolean))];
}

function dealsAirtableColumns(formToAirtable) {
  return mapFormFields([...DEALS_ONLY_FORM_FIELDS], formToAirtable);
}

function readBdrProposalFieldNames() {
  const src = readFileSync(join(ROOT, "api", "brand-deal-requests.js"), "utf8");
  const m = src.match(/const PROPOSAL_FIELD_MAP = \[([\s\S]*?)\];/);
  if (!m) return [];
  const names = [];
  const re = /"([^"]+)"/g;
  let hit;
  while ((hit = re.exec(m[1]))) names.push(hit[1]);
  return names;
}

/** Merge auto-generated names with explicit alias/classification overlays. */
function buildSpecsFromNames(names, overlays = {}) {
  const specs = [];
  const seen = new Set();
  for (const raw of names) {
    if (!raw || seen.has(raw)) continue;
    seen.add(raw);
    const o = overlays[raw] || {};
    if (o.deprecated) continue;
    specs.push(
      fieldSpec(raw, {
        aliases: o.aliases || [],
        classification: o.classification || "",
        notes: o.notes || "",
        deprecated: o.deprecated,
      })
    );
  }
  return specs;
}

const BDR_CORE_FIELDS = [
  "Deal",
  "Brand Name",
  "Status",
  "Request Sent At",
  "Response Date",
  "Response Notes",
  "Match Score",
  "Created At",
  "Last Updated",
  "Owner Notes",
  "Next Follow-up Date",
  "Next Follow-up Header",
  "Next Follow-up Notes (Internal)",
  "Next Follow-up Notes (External)",
  "Next Follow-up Notes",
  "NDA Required?",
  "NDA Status",
  "NDA Sent At",
  "NDA Signed At",
  "Deal Room Access",
  "Access Granted At",
  "Access Revoked At",
  "NDA Sent File",
  "NDA Signed File",
  "Proposal Status",
  "Proposal Updated At",
  "Proposal Submitted At",
];

const DEAL_BRAND_CACHE_FIELDS = [
  "Name",
  "Deal",
  "Preferred Brands",
  "Preferred Scores",
  "Top Alternatives",
  "Preferred Score",
  "Best Match Brand",
  "Best Match Score",
  "Last Computed At",
  "Breakdown Details By Brand",
];

/** Live schema audit 2026-07 — activity log columns (documented; not yet in write-path maps). */
const DEAL_ACTIVITY_LOG_FIELDS = [
  "Brand Name",
  "Deal",
  "Action",
  "Details",
  "Created At",
  "Subject",
  "Message_Summary",
  "Seed Batch ID",
  "Operating Company Name",
  "Stakeholder",
];

/** Live schema audit 2026-07 — deal room document columns. */
const DEAL_ROOM_DOCUMENT_FIELDS = [
  "Document Name",
  "Deal",
  "Category",
  "Confidentiality",
  "File",
  "Uploaded By",
  "Party Folder",
  "Uploaded At",
];

/** Phase 5B markdown name → live Airtable column aliases. */
export const PHASE_5B_FIELD_ALIASES = {
  "Brand Operator Responsibility Split": ["Brand / Operator Responsibility Split"],
  "Local Labor HR Support Needed": ["Local Labor / HR Support Needed"],
};

/** Founder-approved: do not treat as blocking gaps (live substitute or deferred). */
export const PHASE_5B_SKIP_FIELDS = new Set(["F&B Complexity Level"]);

export function parsePhase5bDealFieldsFromMarkdown() {
  const mdPath = join(ROOT, "docs", "operator-alignment-recommended-airtable-fields.md");
  const md = readFileSync(mdPath, "utf8");
  const out = new Map();
  const rowRe = /^\|\s*\*\*([^*|]+)\*\*(?:\s*\([^)]*\))?\s*\|\s*`([^`]+)`\s*\|/;
  let priority = "P1";
  for (const line of md.split("\n")) {
    if (line.startsWith("## Priority 2")) priority = "P2";
    if (line.startsWith("## Priority 3")) priority = "P3";
    const m = line.match(rowRe);
    if (!m) continue;
    const rawTable = m[1].trim();
    const field = m[2].trim();
    if (PHASE_5B_SKIP_FIELDS.has(field)) continue;
    let tableKey = null;
    if (/market\s*-\s*performance/i.test(rawTable)) tableKey = MARKET_PERFORMANCE_TABLE;
    else if (/strategic intent/i.test(rawTable)) tableKey = STRATEGIC_INTENT_TABLE;
    else if (/^deals$/i.test(rawTable) || /deals or location/i.test(rawTable)) tableKey = DEALS_TABLE;
    else continue;
    if (!out.has(tableKey)) out.set(tableKey, []);
    out.get(tableKey).push({
      field,
      aliases: PHASE_5B_FIELD_ALIASES[field] || [],
      priority,
      source: "operator-alignment-recommended-airtable-fields.md",
    });
  }
  return out;
}

export function buildExpectedDealsSchemaRegistry() {
  const dealsRaw = collectUnique(
    dealsAirtableColumns(DEALS_FORM_TO_AIRTABLE),
    [
      DEALS_STATUS_FIELD,
      "Deal Status",
      "Property Name",
      "Project Name",
      INTAKE_DEALS_USER_LINK_NAME,
      DEALS_COMPANY_LINK_FIELD,
      "NDA Template File",
      process.env.AIRTABLE_DEALS_LINKED_MARKET_RECORD_ID_FIELD || "Linked Market Record ID",
      DEAL_READINESS_SCORE_AIRTABLE_FIELD,
      DEAL_READINESS_STAGE_AIRTABLE_FIELD,
      DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD,
      DEAL_READINESS_SUMMARY_AIRTABLE_FIELD,
      process.env.DEAL_READINESS_MISSING_COUNT_FIELD || "Deal Readiness Missing Count",
      process.env.DEAL_READINESS_BLOCKING_COUNT_FIELD || "Deal Readiness Blocking Count",
      LOCATION_LINK_FIELD,
      MARKET_PERFORMANCE_LINK_FIELD,
      STRATEGIC_INTENT_LINK_FIELD,
      CONTACT_UPLOADS_LINK_FIELD,
      ...Object.values(OAS_DEAL_DEALS_FIELD_NAMES),
    ]
  );

  const dealsOverlays = {
    "Deal Status": {
      aliases: ["Status"],
      notes: "DEALS_STATUS_FIELD; my-deals also reads legacy Status.",
    },
    "Property Name": {
      aliases: ["Project Name", "Name"],
      notes: "List UI falls back to Project Name / Name when Property Name empty.",
    },
    [LOCATION_LINK_FIELD]: {
      aliases: [LOCATION_LINK_ALIAS],
      notes: "Linked child table for Location & Property.",
    },
    "Lease Structure": {
      classification: "Expected In Code But Not Live",
      notes:
        "deal-setup-fields.js expects Deals→Lease Structure link; live base links Lease Structure child rows via Deal_ID only.",
      deprecated: true,
    },
    Name: { deprecated: true, notes: "Use Property Name (alias)." },
    Status: { deprecated: true, notes: "Use Deal Status (alias)." },
    [LOCATION_LINK_ALIAS]: { deprecated: true, notes: "Alias of Location & Property." },
  };

  const locationRaw = collectUnique(Object.values(LOCATION_FORM_TO_AIRTABLE), [
    LOCATION_PROPERTY_ID_FIELD,
    MP_DEAL_LINK_FIELD,
  ]);
  const locationOverlays = {
    "Site/Development Restrictions Description": {
      aliases: ["Site Restrictions Describe"],
      notes: "Live column name differs from form map.",
    },
    "Ownership Type Other Text": {
      classification: "Confirmed Missing",
      notes: "Not present in audited live base; may be optional free-text.",
    },
    "Zoning Status Other Text": {
      classification: "Confirmed Missing",
      notes: "Not present in audited live base.",
    },
  };

  const mpRaw = collectUnique(
    [...MARKET_PERFORMANCE_FIELD_NAMES].map((f) => MP_FORM_TO_TABLE[f] || f),
    [MP_DEAL_LINK_FIELD, "Deal_ID", ...Object.values(OAS_DEAL_MP_FIELD_NAMES)]
  );

  const siRaw = collectUnique(
    mapFormFields(STRATEGIC_INTENT_FORM_FIELDS, SI_FORM_TO_AIRTABLE),
    Object.values(OAS_DEAL_SI_FIELD_NAMES)
  );
  const siOverlays = {
    "Top Priorities for Project Other": {
      aliases: ["Top Priorities for Project Other Text"],
      notes: "Form key maps to *Other Text column in Airtable.",
    },
    "Top Concerns for this Project Other": {
      aliases: ["Top Concerns for this Project Other Text"],
    },
    "Top 3 Deal Breakers Other": {
      aliases: ["Top 3 Deal Breakers Other Text"],
    },
    "Brand / Operator Responsibility Split": {
      aliases: ["Brand Operator Responsibility Split"],
      notes: "OAS code uses slash form; Phase 5B doc omits slashes.",
    },
    "Local Labor / HR Support Needed": {
      aliases: ["Local Labor HR Support Needed"],
    },
  };

  const cuRaw = collectUnique(
    mapFormFields(
      CONTACT_UPLOADS_FORM_FIELDS.filter((f) => f !== "Upload Supporting Docs"),
      CU_FORM_TO_AIRTABLE
    ),
    CU_ATTACHMENT_AIRTABLE_FIELDS,
    [CU_DEAL_LINK_FIELD, "Deal_ID", "Broker/Firm Name"]
  );
  const cuOverlays = {
    "Secondary Contact": {
      aliases: ["Secondary Contact (Name & Email)"],
    },
    "Contact Source": {
      aliases: ["Contact Info Type"],
      classification: "Needs Manual Verification",
    },
  };

  const lsRaw = collectUnique(
    mapFormFields(LEASE_STRUCTURE_FORM_FIELDS, LS_FORM_TO_AIRTABLE),
    [LS_DEAL_LINK_FIELD, "Deal_ID"]
  );
  const lsOverlays = {
    Deal: {
      aliases: ["Deals", "Deal_ID"],
      notes: "Child table links to Deals via Deal_ID.",
    },
    Deals: { deprecated: true, notes: "Use Deal_ID on Lease Structure child table." },
  };

  const bdrRaw = collectUnique(BDR_CORE_FIELDS, readBdrProposalFieldNames());
  const bdrOverlays = {
    "Next Follow-up Notes (External)": {
      aliases: ["Next Follow-up Notes"],
      notes: "brand-deal-requests.js maps external notes to legacy column when needed.",
    },
    "Brand Internal Notes": {
      deprecated: true,
      classification: "Deprecated / Should Remove From Registry",
      notes: "Legacy read fallback in brand-deal-requests.js; use Next Follow-up Notes (Internal).",
    },
    "External CRM ID": { classification: "Proposed / Not Yet Implemented" },
    "CRM Sync Status": { classification: "Proposed / Not Yet Implemented" },
    "Last CRM Sync At": { classification: "Proposed / Not Yet Implemented" },
    "CRM Owner": { classification: "Proposed / Not Yet Implemented" },
    "CRM Stage": { classification: "Proposed / Not Yet Implemented" },
    "CRM Notes": { classification: "Proposed / Not Yet Implemented" },
  };

  const odrRaw = collectUnique(
    Object.values(MAP_ODR_AIRTABLE).filter((v) => v !== MAP_ODR_AIRTABLE.table)
  );

  const tables = {
    deals: {
      tableName: process.env.AIRTABLE_TABLE_DEALS || DEALS_TABLE,
      fieldSpecs: buildSpecsFromNames(dealsRaw, dealsOverlays),
      sources: [
        "api/schemas/deal-setup-fields.js",
        "api/schemas/intake-deal-fields.js",
        "lib/operator-alignment-field-options.js",
      ],
    },
    location: {
      tableName: process.env.AIRTABLE_TABLE_LOCATION_PROPERTY || LOCATION_PROPERTY_TABLE,
      fieldSpecs: buildSpecsFromNames(locationRaw, locationOverlays),
      sources: ["api/schemas/deal-setup-fields.js"],
    },
    marketPerformance: {
      tableName: MARKET_PERFORMANCE_TABLE,
      fieldSpecs: buildSpecsFromNames(mpRaw),
      sources: ["api/schemas/deal-setup-fields.js"],
    },
    strategicIntent: {
      tableName: process.env.AIRTABLE_TABLE_STRATEGIC_INTENT || STRATEGIC_INTENT_TABLE,
      fieldSpecs: buildSpecsFromNames(siRaw, siOverlays),
      sources: [
        "api/schemas/deal-setup-fields.js",
        "lib/operator-alignment-field-options.js",
      ],
    },
    contactUploads: {
      tableName: process.env.AIRTABLE_TABLE_CONTACT_UPLOADS || CONTACT_UPLOADS_TABLE,
      fieldSpecs: buildSpecsFromNames(cuRaw, cuOverlays),
      sources: ["api/schemas/deal-setup-fields.js"],
    },
    leaseStructure: {
      tableName: process.env.AIRTABLE_TABLE_LEASE_STRUCTURE || LEASE_STRUCTURE_TABLE,
      fieldSpecs: buildSpecsFromNames(lsRaw, lsOverlays),
      sources: ["api/schemas/deal-setup-fields.js"],
    },
    brandDealRequests: {
      tableName: BDR_TABLE,
      fieldSpecs: buildSpecsFromNames(bdrRaw, bdrOverlays),
      sources: ["api/brand-deal-requests.js"],
    },
    operatorDealRequests: {
      tableName: MAP_ODR_AIRTABLE.table,
      fieldSpecs: buildSpecsFromNames(odrRaw),
      sources: ["api/operator-deal-requests-fields.js"],
    },
    dealBrandCache: {
      tableName: process.env.AIRTABLE_TABLE_DEAL_BRAND_CACHE || "Deal Brand Cache",
      fieldSpecs: buildSpecsFromNames(DEAL_BRAND_CACHE_FIELDS),
      sources: ["api/my-deals.js"],
    },
    dealActivityLog: {
      tableName: process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log",
      fieldSpecs: buildSpecsFromNames(DEAL_ACTIVITY_LOG_FIELDS, {
        Message_Summary: {
          notes: "Underscore column name from live base.",
        },
      }),
      sources: ["reports/airtable-deals-schema-live.json (2026-07 audit)"],
    },
    proposalSubmissions: {
      tableName: process.env.AIRTABLE_TABLE_PROPOSAL_SUBMISSIONS || "Proposal Submissions",
      fieldSpecs: [],
      optionalTable: true,
      notes:
        "Referenced in .env.example and brand-deal-requests.js; not present in audited live base (appvtnDurnMSjINP6).",
      sources: [".env.example", "api/brand-deal-requests.js"],
    },
    dealRoomDocuments: {
      tableName: process.env.AIRTABLE_TABLE_DEAL_ROOM_DOCUMENTS || "Deal Room Documents",
      fieldSpecs: buildSpecsFromNames(DEAL_ROOM_DOCUMENT_FIELDS),
      sources: ["reports/airtable-deals-schema-live.json (2026-07 audit)"],
    },
  };

  return { tables, phase5b: parsePhase5bDealFieldsFromMarkdown() };
}

export const DEAL_SCHEMA_AUDIT_TABLE_KEYS = [
  "deals",
  "location",
  "marketPerformance",
  "strategicIntent",
  "contactUploads",
  "leaseStructure",
  "brandDealRequests",
  "operatorDealRequests",
  "dealBrandCache",
  "dealActivityLog",
  "proposalSubmissions",
  "dealRoomDocuments",
];

/** Flat field names for backward compatibility. */
export function fieldNamesFromSpecs(fieldSpecs) {
  return (fieldSpecs || []).filter((s) => !s.deprecated).map((s) => s.name);
}

/** All names that satisfy a spec (canonical + aliases). */
export function coveredNamesForSpec(spec) {
  return [spec.name, ...(spec.aliases || [])];
}

/**
 * @param {object} spec
 * @param {string[]} liveNames
 * @returns {{ kind: 'exact'|'alias'|'missing', live: string|null, matchedAlias?: string }}
 */
export function matchSpecToLive(spec, liveNames) {
  const liveSet = new Set(liveNames);
  if (liveSet.has(spec.name)) return { kind: "exact", live: spec.name };
  for (const alias of spec.aliases || []) {
    if (liveSet.has(alias)) return { kind: "alias", live: alias, matchedAlias: alias };
  }
  const norm = (s) => String(s).trim().toLowerCase();
  const byNorm = liveNames.find((n) => norm(n) === norm(spec.name));
  if (byNorm) return { kind: "exact", live: byNorm, notes: "case-insensitive match" };
  for (const alias of spec.aliases || []) {
    const hit = liveNames.find((n) => norm(n) === norm(alias));
    if (hit) return { kind: "alias", live: hit, matchedAlias: alias };
  }
  return { kind: "missing", live: null };
}
