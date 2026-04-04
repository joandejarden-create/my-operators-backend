/**
 * Operator Setup explicit write routing (canonical map: docs/operator-setup-canonical-map.json).
 * Splits compact/expanded field maps into per-store payloads so Basics schema filtering
 * does not silently lose values that belong to split tables.
 */

import { getUsedThirdPartyOperatorFieldNamesByTable } from "./third-party-operator-airtable-fields-used.js";

const PERFORMANCE_TABLE = "3rd Party Operator - Performance & Operations";
const SERVICES_TABLE = "3rd Party Operator - Service Offerings";
const IDEAL_TABLE = "3rd Party Operator - Ideal Projects & Deal Fit";
const OWNER_REL_TABLE = "3rd Party Operator - Owner Relations & Communication";
const DEAL_TERMS_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATOR_DEAL_TERMS_TABLE || "3rd Party Operator - Deal Terms & Fees";

/**
 * Fields that must stay Basics-primary (identity, validation drivers, Explorer long text, legacy JSON mirrors).
 * Split-table intersection does not delegate these away.
 * Aligned to docs/operator-setup-canonical-map.json `meta.basics_primary_field_names` (run `node scripts/check-basics-primary-drift.mjs` after edits).
 */
export const BASICS_PRIMARY_FIELD_NAMES = new Set([
  "Company Name",
  "Website",
  "Headquarters Location",
  "Headquarters",
  "Year Established",
  "Primary Contact Email",
  "Contact Email",
  "Primary Contact Phone",
  "Contact Phone",
  "Contact Name",
  "Preferred Contact Method",
  "Company Description",
  "Company Tagline",
  "Mission Statement",
  "Company Size",
  "Years in Business",
  "Primary Service Model",
  "Portfolio Metrics As of Date",
  "Number of Brands Supported",
  "Brands Managed",
  "Chain Scales You Support",
  "Additional Brands",
  "Regions Supported",
  "Specific Markets",
  "Chain Scale",
  "Property Types",
  "Property Types Managed",
  "Company Logo",
  "Submitted At",
  "Company History",
  "Key Differentiators",
  "Notable Achievements",
  "Management Philosophy",
  "Key Leadership",
  "Total Employees",
  "Avg On-Site Staff",
  "Regional Teams",
  "Avg Experience Years",
  "Average Years of Industry Experience",
  "Certifications",
  "Certifications Held",
  "Specializations",
  "Technology & Systems",
  "Additional Notes",
  "Owner Testimonials",
  "Testimonial Links",
  "Industry Recognition",
  "Lender References",
  "Major Lenders",
  "Owner References",
  "Case Studies Detail",
  "Owner Diligence Q&A",
  "Owner Diligence Document Links",
  // Explorer normalization / Basics-only fields referenced in prefill
  "Best Fit Asset Types",
  "Best Fit Geographies",
  "Best Fit Owner Types",
  "Best Fit Deal Structures",
  "Typical Assignment Types",
  "Less Ideal Situations",
  "Owner Value Proposition",
  "Owner Reporting Cadence",
  "KPI Dashboard Provided",
  "Budget / Forecast Reporting Discipline",
  "Capital Planning Support",
  "Franchise-Compatible Experience",
  "Soft Brand Experience",
  "Independent Collection Experience",
  "Brand Standards Flexibility",
  "Operator Status",
  "Deal Status",
  "Status",
]);

/** Intake / compact field names that map to Ideal Projects & Deal Fit (see withTableSpecificAliases in intake). */
const IDEAL_INTAKE_FIELD_NAMES = new Set([
  "Ideal Project Types",
  "Ideal Building Types",
  "Ideal Agreement Types",
  "Owner Hotel Experience",
  "Owner Non-Negotiable Types",
  "PIP / Repositioning Details",
  "Known Red Flag Items",
  "ESG / Sustainability Expectations",
  "Ideal Projects Additional Notes",
]);

/** Intake / compact names that map to Performance & Operations. */
const PERFORMANCE_INTAKE_FIELD_NAMES = new Set([
  "RevPAR Improvement",
  "Occupancy Improvement",
  "NOI Improvement",
  "Reporting Frequency",
  "Report Types",
  "Capex Planning",
  "Performance Reviews",
  "Primary PMS",
  "Guest Communication",
  "Mobile Check-in",
  "Analytics Platform",
  "Portfolio Value",
  "Annual Revenue Managed",
  "Average Contract Term",
  "Fee Structure",
]);

/**
 * Resolve primary Airtable store for a field name using split-table precedence from the canonical map:
 * Deal Terms → Ideal → Owner Relations → Service Offerings → Performance → Basics.
 * @param {string} fieldName
 * @param {Map<string, Set<string>>} usedByTable
 * @returns {string} store id
 */
export function resolvePrimaryStoreForField(fieldName, usedByTable) {
  if (BASICS_PRIMARY_FIELD_NAMES.has(fieldName)) return "basics";

  if (IDEAL_INTAKE_FIELD_NAMES.has(fieldName)) return "ideal";
  if (PERFORMANCE_INTAKE_FIELD_NAMES.has(fieldName)) return "performance";

  const deal = usedByTable.get(DEAL_TERMS_TABLE);
  const ideal = usedByTable.get(IDEAL_TABLE);
  const ownerRel = usedByTable.get(OWNER_REL_TABLE);
  const services = usedByTable.get(SERVICES_TABLE);
  const perf = usedByTable.get(PERFORMANCE_TABLE);

  if (deal && deal.has(fieldName)) return "deal_terms";
  if (ideal && ideal.has(fieldName)) return "ideal";
  if (ownerRel && ownerRel.has(fieldName)) return "owner_relations";
  if (services && services.has(fieldName)) return "service_offerings";
  if (perf && perf.has(fieldName)) return "performance";

  return "basics";
}

/**
 * @param {object} params
 * @param {Record<string, unknown>} params.expandedFields — withTableSpecificAliases(compactFields)
 * @param {string} params.explorerProfileFieldName
 * @param {boolean} [params.mirrorSplitsToBasics] — default true (backward compatible)
 */
export function buildOperatorSetupWritePlan({
  expandedFields,
  explorerProfileFieldName = "Explorer Profile JSON",
  mirrorSplitsToBasics = true,
}) {
  const usedByTable = getUsedThirdPartyOperatorFieldNamesByTable();

  const writePlan = {
    basics: {},
    basicsMirrorFromSplits: {},
    /** Intentionally unused for Footprint today — Footprint payload is built in `buildFootprintRowPayloadFromIntake` (see third-party-operator-intake.js). */
    footprint: {},
    performance: {},
    services: {},
    ideal: {},
    owner_rel: {},
    deal_terms: {},
    case_studies: [],
    owner_diligence: [],
    explorer_json: null,
  };

  /** @type {Record<string, string>} */
  const primaryByKey = {};

  const explorerField = explorerProfileFieldName || "Explorer Profile JSON";

  for (const [k, v] of Object.entries(expandedFields || {})) {
    if (v == null) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    if (Array.isArray(v) && v.length === 0) continue;

    if (k === explorerField) {
      writePlan.explorer_json = v;
      primaryByKey[k] = "explorer_profile_json";
      // Physical persistence is still the Basics long-text column; do not map to other tables.
      writePlan.basics[k] = v;
      continue;
    }

    // Child tables are canonical for case studies / diligence; Basics long text is legacy mirror (see canonical map).
    if (k === "Case Studies Detail" || k === "Owner Diligence Q&A") {
      primaryByKey[k] = "child_table_with_basics_json_mirror";
      writePlan.basics[k] = v;
      continue;
    }

    const primary = resolvePrimaryStoreForField(k, usedByTable);
    primaryByKey[k] = primary;

    if (primary === "basics") {
      writePlan.basics[k] = v;
      continue;
    }

    const bucket =
      primary === "deal_terms"
        ? writePlan.deal_terms
        : primary === "ideal"
          ? writePlan.ideal
          : primary === "owner_relations"
            ? writePlan.owner_rel
            : primary === "service_offerings"
              ? writePlan.services
              : primary === "performance"
                ? writePlan.performance
                : writePlan.basics;

    bucket[k] = v;

    if (mirrorSplitsToBasics) {
      writePlan.basicsMirrorFromSplits[k] = v;
    }
  }

  return { writePlan, primaryByKey, usedByTable };
}

/**
 * Owner Relations columns that should receive copies of Basics-primary contact / proof fields
 * (canonical map: operator_identity_contact + owner_engagement_relations mirrors).
 * Source keys are alias-aware (see withTableSpecificAliases in intake).
 */
export const OWNER_REL_MIRROR_FROM_EXPANDED = [
  { ownerRelColumn: "Primary Contact Email", sourceKeys: ["Primary Contact Email", "Contact Email"] },
  { ownerRelColumn: "Primary Contact Phone", sourceKeys: ["Primary Contact Phone", "Contact Phone"] },
  { ownerRelColumn: "Contact Name", sourceKeys: ["Contact Name"] },
  { ownerRelColumn: "Preferred Contact Method", sourceKeys: ["Preferred Contact Method"] },
  { ownerRelColumn: "Key Owner Success Stories", sourceKeys: ["Key Owner Success Stories", "Owner Testimonials"] },
  { ownerRelColumn: "Testimonial Links", sourceKeys: ["Testimonial Links"] },
  { ownerRelColumn: "Industry Recognition", sourceKeys: ["Industry Recognition"] },
  { ownerRelColumn: "Lender References Available", sourceKeys: ["Lender References Available", "Lender References"] },
  { ownerRelColumn: "Major Lenders Worked With", sourceKeys: ["Major Lenders Worked With", "Major Lenders"] },
  { ownerRelColumn: "Owner References Available", sourceKeys: ["Owner References Available", "Owner References"] },
];

function pickFirstExpandedValue(expandedFields, sourceKeys) {
  for (const k of sourceKeys) {
    const v = expandedFields[k];
    if (v == null) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    if (Array.isArray(v) && v.length === 0) continue;
    return v;
  }
  return undefined;
}

/**
 * Copies Basics-primary contact / proof values into `writePlan.owner_rel` when the Owner Relations table has that column.
 * Does not modify Basics payloads — safe compatibility mirror toward canonical “relational home” on Owner Relations.
 */
export function mirrorBasicsPrimaryToOwnerRelations(writePlan, expandedFields, ownerRelSchema) {
  if (!writePlan || !ownerRelSchema || ownerRelSchema.size === 0) return;
  const ef = expandedFields || {};
  for (const { ownerRelColumn, sourceKeys } of OWNER_REL_MIRROR_FROM_EXPANDED) {
    if (!ownerRelSchema.has(ownerRelColumn)) continue;
    const v = pickFirstExpandedValue(ef, sourceKeys);
    if (v === undefined) continue;
    writePlan.owner_rel[ownerRelColumn] = v;
  }
}

/**
 * Merge Basics-primary fields and optional split mirrors for the Basics Airtable write.
 * Later keys win (Basics-primary overwrites mirror for same key).
 */
export function mergeBasicsPayloadForWrite(writePlan) {
  return {
    ...writePlan.basicsMirrorFromSplits,
    ...writePlan.basics,
  };
}

/**
 * Log when Basics schema filter removes keys — distinguishes intentional routing vs unknown columns.
 * @param {string[]} droppedKeys
 * @param {Record<string, string>} primaryByKey
 * @param {Set<string> | null} basicsSchema
 * @param {string} [correlationId] request correlation id (Operator Setup intake)
 */
export function logBasicsDropRouting(droppedKeys, primaryByKey, basicsSchema, correlationId) {
  const log = process.env.LOG_AIRTABLE_FIELD_DROPS === "1" || process.env.LOG_OPERATOR_SETUP_WRITE_ROUTING === "1";
  if (!log || !droppedKeys.length) return;

  for (const key of droppedKeys) {
    const primary = primaryByKey[key] || "unknown";
    const inSchema = basicsSchema && basicsSchema.has(key);
    if (primary !== "basics" && primary !== "unknown" && primary !== "explorer_profile_json") {
      console.warn(
        JSON.stringify({
          scope: "operator_setup_intake",
          event: "basics_schema_omit",
          ...(correlationId ? { cid: correlationId } : {}),
          key,
          primary,
          reason: "split_or_mirror",
        })
      );
    } else if (!inSchema) {
      console.warn(
        JSON.stringify({
          scope: "operator_setup_intake",
          event: "basics_schema_omit",
          ...(correlationId ? { cid: correlationId } : {}),
          key,
          primary,
          reason: "no_column",
        })
      );
    }
  }
}

export function debugLogWritePlan(writePlan, opts = {}) {
  if (process.env.DEBUG_OPERATOR_SETUP_WRITE !== "1") return;
  const footprint = writePlan.footprint || {};
  const footprintKeys = Array.isArray(opts.footprintKeys)
    ? opts.footprintKeys
    : typeof footprint === "object" && !Array.isArray(footprint)
      ? Object.keys(footprint)
      : [];
  console.log("[WRITE PLAN]", {
    ...(opts.correlationId ? { cid: opts.correlationId } : {}),
    basics: Object.keys(writePlan.basics || {}),
    basicsMirrorFromSplits: Object.keys(writePlan.basicsMirrorFromSplits || {}),
    footprint: footprintKeys,
    performance: Object.keys(writePlan.performance || {}),
    services: Object.keys(writePlan.services || {}),
    ideal: Object.keys(writePlan.ideal || {}),
    owner_rel: Object.keys(writePlan.owner_rel || {}),
    deal_terms: Object.keys(writePlan.deal_terms || {}),
    case_studies_count: opts.caseStudiesCount ?? (Array.isArray(writePlan.case_studies) ? writePlan.case_studies.length : 0),
    diligence_count: opts.diligenceCount ?? (Array.isArray(writePlan.owner_diligence) ? writePlan.owner_diligence.length : 0),
    has_explorer_json: writePlan.explorer_json != null && String(writePlan.explorer_json).trim() !== "",
  });
}
