#!/usr/bin/env node
/**
 * Phase 5B — Export schema backup + create Operator Alignment fields in Airtable (non-destructive).
 *
 *   node scripts/operator-alignment-phase-5b-airtable-schema.mjs --export-only
 *   node scripts/operator-alignment-phase-5b-airtable-schema.mjs --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OAS_OPERATOR_SERVICE_OPTIONS,
  OAS_ACTIVE_COUNTRIES_OPTIONS,
  OAS_ACTIVE_MARKETS_OPTIONS,
  OAS_MARKET_PRESENCE_TYPE_OPTIONS,
  OAS_SERVICE_MODELS_SUPPORTED_OPTIONS,
  OAS_MANAGEMENT_STRUCTURES_OPTIONS,
  OAS_EXPERIENCE_LEVEL_OPTIONS,
  OAS_OWNER_REPORTING_LEVEL_OPTIONS,
  OAS_DATA_CONFIDENCE_OPTIONS,
  OAS_SOURCE_TYPE_OPTIONS,
  OAS_BRAND_FAMILIES_OPTIONS,
  OAS_FB_CAPABILITY_OPTIONS,
  OAS_REVENUE_MGMT_CAPABILITY_OPTIONS,
  OAS_SALES_PLATFORM_OPTIONS,
  OAS_GOVERNANCE_CADENCE_OPTIONS,
  OAS_OPERATOR_REVIEW_STATUS_OPTIONS,
  OAS_PREFERRED_MANAGEMENT_STRUCTURE_OPTIONS,
  OAS_MARKET_PRESENCE_REQUIREMENT_OPTIONS,
  OAS_PREOPENING_SUPPORT_NEEDED_OPTIONS,
  OAS_OWNER_REPORTING_EXPECTATIONS_OPTIONS,
  OAS_BRAND_OPERATOR_SPLIT_OPTIONS,
  OAS_OWNER_CONTROL_PREFERENCE_OPTIONS,
  OAS_COMMERCIAL_PRIORITY_OPTIONS,
  OAS_YES_NO_NA_OPTIONS,
  OAS_OWNER_INTERNAL_OPS_OPTIONS,
  OAS_OPENING_TIMELINE_OPTIONS,
  OAS_BRAND_AGREEMENT_STRUCTURE_OPTIONS,
  OAS_DEAL_OPERATING_MODEL_OPTIONS,
  OAS_OPERATOR_SCOPE_OPTIONS,
  OAS_DEAL_SI_FIELD_NAMES,
  OAS_DEAL_DEALS_FIELD_NAMES,
  OAS_YES_NO_CASE_BY_CASE_OPTIONS,
  OAS_BRANDED_RESIDENCE_PROGRAM_MODEL_OPTIONS,
  OAS_CONDO_RENTAL_PROGRAM_MODEL_OPTIONS,
  OAS_HOA_CONDO_INTERFACE_OPTIONS,
  OAS_RESIDENCE_SALES_SUPPORT_OPTIONS,
  OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES,
} from "../lib/operator-alignment-field-options.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const EXPORT_ONLY = process.argv.includes("--export-only") || !APPLY;

const TABLE_NAMES = {
  deals: "Deals",
  si: "Strategic Intent - Operational - Key Challenges",
  mp: "Market - Performance - Deal & Capital Structure",
  master: "Operator Setup - Master",
  profile: "Operator Setup - Profile & Positioning",
  platform: "Operator Setup - Platform & Markets",
  commercial: "Operator Setup - Commercial Fit & Terms",
  governance: "Operator Setup - Governance, Delivery & Diligence",
};

function choices(names) {
  return names.map((name) => ({ name }));
}

function multi(name, options) {
  return { name, type: "multipleSelects", options: { choices: choices(options) } };
}

function single(name, options) {
  return { name, type: "singleSelect", options: { choices: choices(options) } };
}

function dateField(name) {
  return { name, type: "date", options: { dateFormat: { name: "iso" } } };
}

function numberField(name) {
  return { name, type: "number", options: { precision: 0 } };
}

function longText(name) {
  return { name, type: "multilineText" };
}

/** Fields to create if missing (does not touch existing columns). */
const FIELD_SPECS = [
  // Operator Platform P1
  { table: "platform", ...multi("Active Countries", OAS_ACTIVE_COUNTRIES_OPTIONS) },
  { table: "platform", ...multi("Active Markets / Cities", OAS_ACTIVE_MARKETS_OPTIONS) },
  { table: "platform", ...multi("Market Presence Type", OAS_MARKET_PRESENCE_TYPE_OPTIONS) },
  // Operator Profile P1
  { table: "profile", ...multi("Service Models Supported", OAS_SERVICE_MODELS_SUPPORTED_OPTIONS) },
  // chainScalesSupported — REUSE existing; not created
  { table: "commercial", ...multi("Management Structures Supported", OAS_MANAGEMENT_STRUCTURES_OPTIONS) },
  { table: "governance", ...multi("Offered Services", OAS_OPERATOR_SERVICE_OPTIONS) },
  { table: "commercial", ...single("New-Build Opening Experience", OAS_EXPERIENCE_LEVEL_OPTIONS) },
  { table: "commercial", ...single("Pre-Opening Support Capability", [
    "Advanced",
    "Standard",
    "Limited",
    "None documented",
    "Unknown",
  ]) },
  { table: "governance", ...single("Owner Reporting Level", OAS_OWNER_REPORTING_LEVEL_OPTIONS) },
  { table: "master", ...single("Data Confidence Level", OAS_DATA_CONFIDENCE_OPTIONS) },
  { table: "master", ...multi("Source Type", OAS_SOURCE_TYPE_OPTIONS) },
  { table: "master", ...dateField("Last Updated Date") },
  // Operator P2
  { table: "profile", ...multi("Brand Families Operated", OAS_BRAND_FAMILIES_OPTIONS) },
  { table: "commercial", ...single("Conversion / Reflag Experience", OAS_EXPERIENCE_LEVEL_OPTIONS) },
  { table: "profile", ...single("Soft Brand / Lifestyle Experience", OAS_EXPERIENCE_LEVEL_OPTIONS) },
  { table: "governance", ...single("F&B Capability Level", OAS_FB_CAPABILITY_OPTIONS) },
  { table: "governance", ...single("Revenue Management Capability", OAS_REVENUE_MGMT_CAPABILITY_OPTIONS) },
  { table: "governance", ...multi("Sales Platform", OAS_SALES_PLATFORM_OPTIONS) },
  { table: "governance", ...single("Governance Cadence", OAS_GOVERNANCE_CADENCE_OPTIONS) },
  { table: "commercial", ...numberField("Minimum Key Count") },
  { table: "commercial", ...longText("Similar Project Case Studies") },
  // Deal SI P1
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.operatorReviewStatus, OAS_OPERATOR_REVIEW_STATUS_OPTIONS) },
  { table: "si", ...multi(OAS_DEAL_SI_FIELD_NAMES.preferredManagementStructure, OAS_PREFERRED_MANAGEMENT_STRUCTURE_OPTIONS) },
  { table: "si", ...multi(OAS_DEAL_SI_FIELD_NAMES.requiredOperatorServices, OAS_OPERATOR_SERVICE_OPTIONS) },
  { table: "si", ...multi(OAS_DEAL_SI_FIELD_NAMES.mustHaveOperatorServices, OAS_OPERATOR_SERVICE_OPTIONS) },
  { table: "si", ...multi(OAS_DEAL_SI_FIELD_NAMES.niceToHaveOperatorServices, OAS_OPERATOR_SERVICE_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.marketPresenceRequirement, OAS_MARKET_PRESENCE_REQUIREMENT_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.preOpeningSupportNeeded, OAS_PREOPENING_SUPPORT_NEEDED_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.ownerReportingExpectations, OAS_OWNER_REPORTING_EXPECTATIONS_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.brandOperatorResponsibilitySplit, OAS_BRAND_OPERATOR_SPLIT_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.ownerControlPreference, OAS_OWNER_CONTROL_PREFERENCE_OPTIONS) },
  // Deal SI P2
  { table: "si", ...multi(OAS_DEAL_SI_FIELD_NAMES.commercialPriority, OAS_COMMERCIAL_PRIORITY_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.localLaborHrSupportNeeded, OAS_YES_NO_NA_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.procurementSupportNeeded, OAS_YES_NO_NA_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.ownerInternalOpsCapability, OAS_OWNER_INTERNAL_OPS_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.brandAgreementStructure, OAS_BRAND_AGREEMENT_STRUCTURE_OPTIONS) },
  { table: "si", ...single(OAS_DEAL_SI_FIELD_NAMES.dealOperatingModel, OAS_DEAL_OPERATING_MODEL_OPTIONS) },
  { table: "si", ...multi(OAS_DEAL_SI_FIELD_NAMES.operatorScope, OAS_OPERATOR_SCOPE_OPTIONS) },
  // Deals P1/P2
  { table: "deals", ...single(OAS_DEAL_DEALS_FIELD_NAMES.fbComplexity, OAS_FB_CAPABILITY_OPTIONS) },
  { table: "deals", ...single(OAS_DEAL_DEALS_FIELD_NAMES.openingTimeline, OAS_OPENING_TIMELINE_OPTIONS) },
  // Operator branded residence / mixed-use (Commercial Fit + Platform + Governance)
  {
    table: "commercial",
    ...single(
      OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.brandedResidencesAllowed,
      OAS_YES_NO_CASE_BY_CASE_OPTIONS
    ),
  },
  {
    table: "commercial",
    ...single(
      OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.mixedUseAllowed,
      OAS_YES_NO_CASE_BY_CASE_OPTIONS
    ),
  },
  {
    table: "commercial",
    ...single("Co-Branding Allowed", OAS_YES_NO_CASE_BY_CASE_OPTIONS),
  },
  {
    table: "commercial",
    ...single(
      OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.brandedResidenceExperienceLevel,
      OAS_EXPERIENCE_LEVEL_OPTIONS
    ),
  },
  {
    table: "commercial",
    ...multi(
      OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.brandedResidenceProgramModelsSupported,
      OAS_BRANDED_RESIDENCE_PROGRAM_MODEL_OPTIONS
    ),
  },
  {
    table: "commercial",
    ...multi(
      OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.condoRentalProgramModelsSupported,
      OAS_CONDO_RENTAL_PROGRAM_MODEL_OPTIONS
    ),
  },
  {
    table: "commercial",
    ...longText(OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.brandedResidenceFitSignal),
  },
  {
    table: "platform",
    ...numberField(OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.brandedResidencePropertiesManaged),
  },
  {
    table: "platform",
    ...numberField(OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.mixedUseHospitalityExperience),
  },
  {
    table: "governance",
    ...single(
      OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.hoaCondoAssociationInterface,
      OAS_HOA_CONDO_INTERFACE_OPTIONS
    ),
  },
  {
    table: "governance",
    ...single(
      OAS_OPERATOR_BRANDED_RESIDENCE_FIELD_NAMES.residenceSalesClosingSupport,
      OAS_RESIDENCE_SALES_SUPPORT_OPTIONS
    ),
  },
];

const REUSED_FIELDS = [
  { table: "profile", name: "chainScalesSupported", note: "Chain Scales Supported — reuse; form options aligned in UI" },
  { table: "si", name: "Services Required From Operator", note: "Legacy services field — keep; parallel Required Operator Services added" },
  { table: "si", name: "Operator Strategy Status", note: "Keep; parallel Operator Review Status added" },
  { table: "si", name: "Must-Haves From Brand/Operator", note: "Keep; parallel Must-Have Operator Services added" },
  { table: "mp", name: "Preferred Deal Structure", note: "Keep; Brand Agreement Structure + Operating Model clarify franchise vs operator path" },
  { table: "commercial", name: "bf_selected_deal_structures", note: "Legacy structures — keep; Management Structures Supported is canonical for OAS" },
  { table: "platform", name: "specificMarkets", note: "Keep long text; Active Markets / Cities is structured parallel" },
];

async function fetchTables(baseId, apiKey) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || `meta ${res.status}`);
  return json.tables || [];
}

async function createField(baseId, apiKey, tableId, spec) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables/${encodeURIComponent(tableId)}/fields`;
  const body = {
    name: spec.name,
    type: spec.type,
    ...(spec.options ? { options: spec.options } : {}),
  };
  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await res.json();
  return { ok: res.ok, status: res.status, json };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const tables = await fetchTables(baseId, apiKey);
  const tableByKey = {};
  for (const [key, name] of Object.entries(TABLE_NAMES)) {
    const t = tables.find((x) => x.name === name);
    if (!t) throw new Error(`Table not found: ${name}`);
    tableByKey[key] = t;
  }

  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });
  const stamp = new Date().toISOString().slice(0, 10);
  const backupPath = path.join(REPORTS, `operator-alignment-5b-schema-backup-${stamp}.json`);
  const backup = {
    exportedAt: new Date().toISOString(),
    baseId,
    tables: Object.fromEntries(
      Object.entries(TABLE_NAMES).map(([key, name]) => {
        const t = tableByKey[key];
        return [
          key,
          {
            name,
            id: t.id,
            fields: (t.fields || []).map((f) => ({
              name: f.name,
              id: f.id,
              type: f.type,
            })),
          },
        ];
      })
    ),
    reusedFields: REUSED_FIELDS,
    plannedCreates: FIELD_SPECS.map((s) => ({ table: s.table, name: s.name, type: s.type })),
  };
  fs.writeFileSync(backupPath, JSON.stringify(backup, null, 2));
  console.log("Backup:", backupPath);

  if (EXPORT_ONLY) {
    console.log("Export-only mode. Re-run with --apply to create missing fields.");
    return;
  }

  const results = { created: [], skipped: [], failed: [] };
  for (const spec of FIELD_SPECS) {
    const t = tableByKey[spec.table];
    const exists = (t.fields || []).some((f) => f.name === spec.name);
    if (exists) {
      results.skipped.push({ table: t.name, name: spec.name, reason: "already exists" });
      continue;
    }
    const { ok, status, json } = await createField(baseId, apiKey, t.id, spec);
    if (ok) {
      results.created.push({ table: t.name, name: spec.name, id: json.id });
      console.log("CREATED", t.name, "→", spec.name);
      // refresh field list for subsequent checks in same table
      t.fields = [...(t.fields || []), { name: spec.name, id: json.id, type: spec.type }];
    } else {
      results.failed.push({ table: t.name, name: spec.name, status, error: json });
      console.error("FAILED", t.name, spec.name, status, JSON.stringify(json));
    }
    await new Promise((r) => setTimeout(r, 220));
  }

  const outPath = path.join(REPORTS, `operator-alignment-5b-schema-apply-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log("\nApply results:", outPath);
  console.log("Created:", results.created.length, "Skipped:", results.skipped.length, "Failed:", results.failed.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
