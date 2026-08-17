/**
 * Read-only Production Census schema v1.1.1 cleanup plan.
 * Naming alignment + amenities simplification. No Airtable mutations.
 */

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildV11FieldSpecs } from "./production-census-schema-v11.js";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS, EXPECTED_FREEZE, PRODUCTION_USE_STATUS } from "./production-census-write.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const PLAN_VERSION = "production-census-schema-v111-cleanup-plan-v1";

export const STATUS = Object.freeze({
  READY: "production_census_schema_v111_cleanup_plan_ready_for_founder_approval",
  REQUIRES_DECISIONS: "production_census_schema_v111_requires_founder_decisions",
  NO_CLEANUP: "production_census_schema_v111_no_cleanup_needed",
});

const VIC_DIR = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family"
);
const FROZEN_62 = [
  "reports/brand-explorer-62-active-public-full-baseline.json",
  "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js",
];

const OVERMODELED_AMENITIES = [
  "Fitness Flag",
  "Pool Flag",
  "Parking Flag",
  "Airport Shuttle Flag",
  "Spa Flag",
  "Beach / Waterfront Flag",
];

const STRATEGIC_AMENITIES = [
  "F&B Flag",
  "Meeting Space Flag",
  "Resort Amenities Flag",
  "Extended Stay Amenity Flag",
  "Mixed-Use Flag",
  "Branded Residences Flag",
];

function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function hashVic() {
  if (!existsSync(VIC_DIR)) return null;
  const files = readdirSync(VIC_DIR)
    .filter((f) => f.endsWith(".json") || f.endsWith(".md"))
    .sort();
  const h = createHash("sha256");
  for (const f of files) {
    const p = join(VIC_DIR, f);
    if (!statSync(p).isFile()) continue;
    h.update(f);
    h.update(readFileSync(p));
  }
  return { file_count: files.length, aggregate_sha256: h.digest("hex") };
}

async function metaTables(baseId, token) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`meta ${res.status}`);
  return json.tables || [];
}

async function listAll(baseId, token, tableId, fields) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(100);
  } while (offset);
  return out;
}

function extractConventions(mvpTables, platformTables) {
  const pick = (tables, table, names) => {
    const t = tables.find((x) => x.name === table);
    if (!t) return {};
    const set = new Set((t.fields || []).map((f) => f.name));
    const found = {};
    for (const n of names) if (set.has(n)) found[n] = true;
    return { table, fields: [...set].filter((n) => names.some((q) => n.toLowerCase().includes(q.toLowerCase()) || set.has(q))) };
  };

  const interest = [
    "Name",
    "Brand",
    "Slug",
    "Company",
    "Owner",
    "Operator",
    "Management",
    "Developer",
    "Source",
    "Review",
    "Validation",
    "Country",
    "Region",
    "State",
    "Market",
    "City",
    "Property",
    "Room",
    "Key",
    "Status",
    "Note",
    "Amenit",
    "Verified",
  ];

  function interestingFields(tables, tableName) {
    const t = tables.find((x) => x.name === tableName);
    if (!t) return { table: tableName, exists: false, fields: [] };
    return {
      table: tableName,
      exists: true,
      fields: (t.fields || [])
        .filter((f) => interest.some((k) => f.name.toLowerCase().includes(k.toLowerCase())))
        .map((f) => ({ name: f.name, type: f.type })),
    };
  }

  return {
    brand_basics: interestingFields(mvpTables, "Brand Setup - Brand Basics"),
    brand_presentation: interestingFields(mvpTables, "Brand Setup - Brand Explorer Presentation"),
    company_profile: interestingFields(mvpTables, "Company Profile"),
    hotel_ownership: interestingFields(mvpTables, "Hotel Ownership"),
    operator_master: interestingFields(mvpTables, "Operator Setup - Master"),
    operator_profile: interestingFields(mvpTables, "Operator Setup - Profile & Positioning"),
    legacy_hotel_census: interestingFields(platformTables, "Hotel Census"),
    verified_independent: interestingFields(platformTables, "Verified Independent Hotel Census"),
    canonical: {
      brand_name: "Brand Name",
      last_reviewed_date: "Last Reviewed Date",
      confidence_level: "Confidence Level",
      validation_status: "Validation Status",
      source_type: "Source Type",
      company_name: "Company Name",
      company_type: "Company Type",
      management_company_legacy: "Management Company",
      owner_company_legacy: "Owner Company",
      rooms_legacy: "rooms",
      amenities_legacy: "Amenities",
      market_legacy: "Market",
      submarket_legacy: "Submarket",
    },
  };
}

function classifyCensusField(name, conventions) {
  const decisions = {
    "Property Name": {
      classification: "keep_as_is",
      recommended_final_name: "Property Name",
      similar: "Property Name / Brand Name / Verified Hotel Name",
      table: "Hotel Ownership / Brand Basics / VIC stub",
      reason: "Correct property-master label; Brand Name is brand-level.",
      risk: "none",
    },
    "Current Brand": {
      classification: "census_specific_name_ok",
      recommended_final_name: "Current Brand",
      similar: "Brand Name",
      table: "Brand Setup - Brand Basics",
      reason: "Property affiliation snapshot; not Brand Basics Brand Name.",
      risk: "low",
    },
    "Brand Explorer Slug if mapped": {
      classification: "rename_recommended",
      recommended_final_name: "Brand Explorer Slug",
      similar: "(code-derived slug)",
      table: "Brand Explorer runtime",
      reason: "Drop 'if mapped' suffix for clarity; optional v1.1.1 rename.",
      risk: "low",
      bucket: "B",
    },
    "Owner Name": {
      classification: "census_specific_name_ok",
      recommended_final_name: "Owner Name",
      similar: "Owner Company / Owner/Operator Name / Company Name",
      table: "Hotel Census / Hotel Ownership / Company Profile",
      reason:
        "Keep Census-specific. Company Name stays on Company Profile; link later via enrichment, do not rename Owner Name to Company Name.",
      risk: "medium_if_renamed_to_company_name",
      founder_decision: true,
      decision_id: "D_owner_name",
    },
    "Operator / Management Company": {
      classification: "founder_decision_needed",
      recommended_final_name: "Operator / Management Company (keep) OR Management Company",
      similar: "Management Company / company_name",
      table: "Hotel Census / Operator Setup - Master",
      reason:
        "Slash form is explicit for property-level ops. Aligning to Management Company matches legacy Hotel Census; company_name is Operator Setup entity naming — different layer.",
      risk: "medium",
      founder_decision: true,
      decision_id: "C_operator_management",
    },
    "Source URL": {
      classification: "keep_as_is",
      recommended_final_name: "Source URL",
      similar: "Brand Website / Branded Residences Source URL",
      table: "Brand Setup - Brand Basics",
      reason: "Source URL is the standard; do not add Source Link.",
      risk: "none",
    },
    "Source Confidence": {
      classification: "possible_duplicate",
      recommended_final_name: "Source Confidence",
      similar: "Confidence Level",
      table: "Brand Setup - Brand Basics",
      reason: "Keep; document vs Relationship Confidence and Data Confidence Tier.",
      risk: "low",
    },
    "Last Verified Date": {
      classification: "rename_recommended",
      recommended_final_name: "Last Reviewed Date",
      similar: "Last Reviewed Date",
      table: "Brand Setup - Brand Basics / Operator Setup - Master",
      reason: "Align with Brand/Operator Setup common name before enrichment starts.",
      risk: "low",
      founder_decision: true,
      decision_id: "A_last_verified_date",
      bucket: "B",
    },
    "Rooms / Keys": {
      classification: "founder_decision_needed",
      recommended_final_name: "Rooms / Keys (preferred) OR Rooms",
      similar: "rooms",
      table: "Hotel Census",
      reason: "Rooms / Keys is clearer than legacy rooms. No Key Count field in Brand/Company Setup. Prefer keep Rooms / Keys.",
      risk: "low",
      founder_decision: true,
      decision_id: "B_rooms_keys",
    },
    "State / Region": {
      classification: "census_specific_name_ok",
      recommended_final_name: "State / Region",
      similar: "Region Offered / Region / Verified State / State",
      table: "Brand Basics / Hotel Census / VIC stub",
      reason: "Geo admin for property; not Brand Region Offered. Keep.",
      risk: "none",
      decision_id: "F_state_region",
    },
    "Market / Submarket": {
      classification: "possible_duplicate",
      recommended_final_name: "Market / Submarket (short-term) → later Dealality Market + Submarket",
      similar: "Market / Submarket / Dealality Market",
      table: "Hotel Census",
      reason: "Combined field OK for now; long-term split to Dealality geography.",
      risk: "low",
      bucket: "C",
    },
    "Production Use Status": {
      classification: "do_not_change",
      recommended_final_name: "Production Use Status",
      similar: "Brand Status / External Display Status",
      table: "Brand Basics",
      reason: "Must stay distinct from Brand Status.",
      risk: "high_if_renamed",
    },
    "Enrichment Status": {
      classification: "do_not_change",
      recommended_final_name: "Enrichment Status",
      similar: "Validation Status",
      table: "Brand Basics",
      reason: "Enrichment lane progress ≠ validation.",
      risk: "medium_if_renamed",
    },
    "Steward Review Status": {
      classification: "do_not_change",
      recommended_final_name: "Steward Review Status",
      similar: "Validation Status",
      table: "Brand Basics",
      reason: "Census steward lane specific.",
      risk: "medium_if_renamed",
    },
  };

  if (decisions[name]) {
    const d = decisions[name];
    return {
      current_field: name,
      similar_existing_field: d.similar,
      existing_table: d.table,
      classification: d.classification,
      recommended_final_name: d.recommended_final_name,
      reason: d.reason,
      risk: d.risk,
      founder_decision_needed: Boolean(d.founder_decision),
      decision_id: d.decision_id || null,
      bucket: d.bucket || null,
    };
  }

  return {
    current_field: name,
    similar_existing_field: null,
    existing_table: null,
    classification: "keep_as_is",
    recommended_final_name: name,
    reason: "No conflicting Brand/Company Setup convention requiring change.",
    risk: "none",
    founder_decision_needed: false,
  };
}

function classifyAmenity(name, filledCount, scriptRefs) {
  if (name === "Amenities - Source Text" || name === "Amenities - Structured Tags") {
    return {
      amenity_field: name,
      classification: "keep_as_primary_amenity_field",
      recommendation: "keep",
      filled_count: filledCount,
      script_references: scriptRefs,
      reason: "Primary amenity model.",
      delete_safe_if_blank: false,
    };
  }
  if (STRATEGIC_AMENITIES.includes(name)) {
    const rename =
      name === "Resort Amenities Flag"
        ? "Resort / Leisure Flag"
        : name === "Extended Stay Amenity Flag"
          ? "Extended Stay Flag"
          : null;
    return {
      amenity_field: name,
      classification: rename ? "rename_recommended" : "keep_as_strategic_flag",
      recommendation: rename ? "rename_later" : "keep",
      recommended_final_name: rename || name,
      filled_count: filledCount,
      script_references: scriptRefs,
      reason: rename
        ? `Strategic flag; align name to preferred model (${rename}).`
        : "Strategic flag in preferred final amenity model.",
      founder_decision_needed: Boolean(rename),
      delete_safe_if_blank: false,
    };
  }
  if (OVERMODELED_AMENITIES.includes(name)) {
    return {
      amenity_field: name,
      classification: "move_to_structured_tags_later",
      recommendation: filledCount === 0 ? "hide_from_default_view_later_or_delete_candidate_if_blank" : "hide_from_default_view_later",
      filled_count: filledCount,
      blank_across_all_666: filledCount === 0,
      script_references: scriptRefs,
      would_deleting_break_brand_explorer: false,
      would_deleting_require_script_updates: scriptRefs.length > 0,
      reason:
        "Over-modeled vs preferred Structured Tags model. Blank today. Prefer hide from default views; founder decides delete vs keep deferred.",
      founder_decision_needed: true,
      delete_candidate_if_blank: filledCount === 0,
      delete_safe_if_blank: filledCount === 0 && scriptRefs.every((r) => /research-engine-v2|production-census|docs\/|reports\//.test(r)),
    };
  }
  return {
    amenity_field: name,
    classification: "founder_decision_needed",
    recommendation: "review",
    filled_count: filledCount,
    script_references: scriptRefs,
  };
}

function findScriptRefs(fieldName) {
  // Static known refs from research-engine-v2 census lane (no full-repo scan required for plan)
  const known = {
    "Fitness Flag": [
      "lib/research-engine-v2/production-census-schema-v11.js",
      "docs/data-intelligence/production-census-v11-post-apply-review.md",
    ],
    "Pool Flag": [
      "lib/research-engine-v2/production-census-schema-v11.js",
      "docs/data-intelligence/production-census-v11-post-apply-review.md",
    ],
    "Parking Flag": ["lib/research-engine-v2/production-census-schema-v11.js"],
    "Airport Shuttle Flag": ["lib/research-engine-v2/production-census-schema-v11.js"],
    "Spa Flag": ["lib/research-engine-v2/production-census-schema-v11.js"],
    "Beach / Waterfront Flag": ["lib/research-engine-v2/production-census-schema-v11.js"],
    "Last Verified Date": ["lib/research-engine-v2/production-census-schema-v11.js"],
    "Rooms / Keys": [
      "lib/research-engine-v2/production-census-schema-v11.js",
      "lib/research-engine-v2/production-census-write.js",
    ],
    "Operator / Management Company": [
      "lib/research-engine-v2/production-census-schema-v11.js",
      "lib/research-engine-v2/production-census-v11-post-apply-review.js",
    ],
    "Resort Amenities Flag": ["lib/research-engine-v2/production-census-schema-v11.js"],
    "Extended Stay Amenity Flag": ["lib/research-engine-v2/production-census-schema-v11.js"],
  };
  return known[fieldName] || ["lib/research-engine-v2/production-census-schema-v11.js"];
}

/**
 * @param {{ beSafety?: object }} [opts]
 */
export async function runV111CleanupPlan(opts = {}) {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const platformId = bases.target_base_id;
  const mvpId = process.env.AIRTABLE_BASE_ID;

  const platformTables = await metaTables(platformId, token);
  const mvpTables = mvpId ? await metaTables(mvpId, token) : [];
  const conventions = extractConventions(mvpTables, platformTables);

  const census = platformTables.find((t) => t.name === "Hotel Property Census");
  const censusFields = (census?.fields || []).map((f) => ({ name: f.name, type: f.type, id: f.id }));

  const amenityFieldNames = censusFields
    .map((f) => f.name)
    .filter((n) =>
      /amenit|f&b|meeting space|fitness|pool|parking|shuttle|spa|beach|resort|extended stay|mixed-use|branded residences/i.test(
        n
      )
    );

  const sampleFields = [
    "Property Identity Key",
    "Enrichment Status",
    "Human Review Required",
    "Production Use Status",
    ...amenityFieldNames,
    "Last Verified Date",
    "Rooms / Keys",
    "Owner Name",
    "Operator / Management Company",
  ];
  const rows = await listAll(platformId, token, TABLE_IDS["Hotel Property Census"], [
    ...new Set(sampleFields),
  ]);

  const filled = {};
  for (const name of amenityFieldNames) filled[name] = 0;
  for (const r of rows) {
    for (const name of amenityFieldNames) {
      const v = r.fields?.[name];
      if (v === true || (typeof v === "string" && v.trim()) || typeof v === "number") filled[name] += 1;
    }
  }

  const naming_alignment = censusFields.map((f) => classifyCensusField(f.name, conventions));

  const amenities_review = amenityFieldNames.map((name) =>
    classifyAmenity(name, filled[name] || 0, findScriptRefs(name))
  );

  const overmodeled = amenities_review.filter((a) => OVERMODELED_AMENITIES.includes(a.amenity_field));

  const founder_decisions = [
    {
      id: "A_last_verified_date",
      question: "Rename Last Verified Date → Last Reviewed Date to match Brand/Operator Setup?",
      options: ["rename_to_Last_Reviewed_Date", "keep_Last_Verified_Date", "document_alias_only"],
      recommendation: "rename_to_Last_Reviewed_Date",
      timing: "before_enrichment",
    },
    {
      id: "B_rooms_keys",
      question: "Keep Rooms / Keys or rename to Rooms (legacy Hotel Census)?",
      options: ["keep_Rooms_/_Keys", "rename_to_Rooms"],
      recommendation: "keep_Rooms_/_Keys",
      timing: "before_enrichment",
    },
    {
      id: "C_operator_management",
      question: "Keep Operator / Management Company or rename to Management Company?",
      options: ["keep_Operator_/_Management_Company", "rename_to_Management_Company"],
      recommendation: "keep_Operator_/_Management_Company",
      timing: "before_enrichment",
    },
    {
      id: "D_owner_name",
      question: "Keep Owner Name (Census-specific) vs confusing with Company Name?",
      options: ["keep_Owner_Name", "document_relationship_to_Company_Profile"],
      recommendation: "keep_Owner_Name",
      timing: "before_enrichment",
    },
    {
      id: "E_source_url",
      question: "Confirm Source URL (not Source Link) as standard?",
      options: ["keep_Source_URL"],
      recommendation: "keep_Source_URL",
      timing: "immediate_docs_only",
    },
    {
      id: "F_state_region",
      question: "Keep State / Region as property geo (vs Brand Region Offered)?",
      options: ["keep_State_/_Region"],
      recommendation: "keep_State_/_Region",
      timing: "immediate_docs_only",
    },
    {
      id: "G_amenity_overmodel",
      question:
        "For blank over-modeled amenity flags (Fitness/Pool/Parking/Shuttle/Spa/Beach): hide from views, delete, or keep deferred?",
      options: ["hide_from_default_views", "delete_blank_fields", "keep_deferred"],
      recommendation: "hide_from_default_views",
      timing: "before_enrichment",
      note: "All 6 are blank across 666 records. Deleting is low data risk but needs schema write + script map updates. Hiding is safest.",
    },
    {
      id: "H_resort_extended_naming",
      question: "Rename Resort Amenities Flag → Resort / Leisure Flag and Extended Stay Amenity Flag → Extended Stay Flag?",
      options: ["rename_both", "keep_current", "rename_later_after_enrichment"],
      recommendation: "rename_both",
      timing: "before_enrichment",
    },
  ];

  const actions = [
    {
      action: "document_field_roles",
      field: "(docs)",
      reason: "Document Source Confidence vs Relationship Confidence vs Data Confidence Tier; Production Use Status vs Brand Status",
      risk: "none",
      requires_schema_change: false,
      requires_record_migration: false,
      recommended_timing: "now",
      bucket: "A",
    },
    {
      action: "confirm_source_url_standard",
      field: "Source URL",
      reason: "Do not introduce Source Link",
      risk: "none",
      requires_schema_change: false,
      requires_record_migration: false,
      recommended_timing: "now",
      bucket: "A",
    },
    {
      action: "rename_field",
      field: "Last Verified Date",
      reason: "Align to Brand/Operator Last Reviewed Date",
      risk: "low",
      requires_schema_change: true,
      requires_record_migration: false,
      recommended_timing: "after_founder_approval",
      bucket: "B",
    },
    {
      action: "rename_field",
      field: "Brand Explorer Slug if mapped",
      reason: "Simplify to Brand Explorer Slug",
      risk: "low",
      requires_schema_change: true,
      requires_record_migration: false,
      recommended_timing: "after_founder_approval",
      bucket: "B",
    },
    {
      action: "rename_field",
      field: "Resort Amenities Flag",
      reason: "Align to Resort / Leisure Flag",
      risk: "low",
      requires_schema_change: true,
      requires_record_migration: false,
      recommended_timing: "after_founder_approval",
      bucket: "B",
    },
    {
      action: "rename_field",
      field: "Extended Stay Amenity Flag",
      reason: "Align to Extended Stay Flag",
      risk: "low",
      requires_schema_change: true,
      requires_record_migration: false,
      recommended_timing: "after_founder_approval",
      bucket: "B",
    },
    {
      action: "hide_or_delete_blank_overmodeled_flags",
      field: OVERMODELED_AMENITIES.join(", "),
      reason: "Blank across 666; prefer Structured Tags; hide safest, delete only with founder OK",
      risk: "low_if_hide / medium_if_delete",
      requires_schema_change: true,
      requires_record_migration: false,
      recommended_timing: "after_founder_approval",
      bucket: "B",
    },
    {
      action: "keep_owner_operator_names",
      field: "Owner Name; Operator / Management Company",
      reason: "Census-specific property relationships; do not conflate with Company Profile Company Name",
      risk: "high_if_wrongly_renamed",
      requires_schema_change: false,
      requires_record_migration: false,
      recommended_timing: "confirm_then_keep",
      bucket: "B",
    },
    {
      action: "split_market_submarket_later",
      field: "Market / Submarket",
      reason: "Long-term Dealality Market + corridor Submarket",
      risk: "medium",
      requires_schema_change: true,
      requires_record_migration: true,
      recommended_timing: "after_enrichment",
      bucket: "C",
    },
    {
      action: "do_not_change",
      field: "Production Use Status; Enrichment Status; Steward Review Status; Property Name; Source URL",
      reason: "Correct Census-specific or already aligned",
      risk: "high_if_changed",
      requires_schema_change: false,
      requires_record_migration: false,
      recommended_timing: "never_or_docs_only",
      bucket: "D",
    },
  ];

  const suggested_views = [
    {
      name: "Census - Core Identity",
      fields: [
        "Property Name",
        "Current Brand",
        "City",
        "State / Region",
        "Country",
        "Affiliation Status",
        "Production Use Status",
        "Data Confidence Tier",
        "Enrichment Status",
        "Human Review Required",
      ],
    },
    {
      name: "Census - Enrichment",
      fields: [
        "Property Name",
        "Hotel Description - Source Text",
        "Hotel Description - AI Summary",
        "Amenities - Structured Tags",
        "Property Type",
        "Asset Context",
        "Market / Submarket",
        "Enrichment Status",
        "Enrichment Priority",
      ],
    },
    {
      name: "Census - Owner Operator",
      fields: [
        "Property Name",
        "Owner Name",
        "Owner Confidence",
        "Operator / Management Company",
        "Operator Confidence",
        "Ownership Review Status",
        "Operator Review Status",
      ],
    },
    {
      name: "Census - Steward Review",
      fields: [
        "Property Name",
        "Human Review Required",
        "Notes for Steward",
        "Brand-Unassigned Reason",
        "Enrichment Priority",
      ],
    },
  ];

  const schema_status = {
    base: "Deal Capture Platform",
    base_id_masked: mask(platformId),
    table: "Hotel Property Census",
    table_id: census?.id,
    record_count: rows.length,
    field_count: censusFields.length,
    production_use_status: PRODUCTION_USE_STATUS,
    enrichment_not_started: rows.filter((r) => r.fields?.["Enrichment Status"] === "Not Started").length,
    human_review_true: rows.filter((r) => r.fields?.["Human Review Required"] === true).length,
    freeze_hash: EXPECTED_FREEZE,
    amenity_fields_all_blank: Object.values(filled).every((n) => n === 0),
  };

  const status = STATUS.REQUIRES_DECISIONS;

  return {
    version: PLAN_VERSION,
    generated_at: new Date().toISOString(),
    mode: "read_only",
    airtable_writes: false,
    schema_mutations: false,
    status,
    executive_summary: {
      verdict: status,
      headline:
        "Cleanup plan ready: founder decisions required on Last Reviewed Date rename, amenity hide-vs-delete, and Resort/Extended Stay naming before enrichment.",
      amenities:
        "6 over-modeled amenity flags exist and are blank across all 666 records — recommend hide from default views (or delete with approval).",
      naming: "Align Last Verified Date → Last Reviewed Date; keep Owner Name and Operator / Management Company as Census-specific.",
      enrichment: "Do not start enrichment until founder approves v1.1.1 structure decisions.",
    },
    schema_status,
    naming_conventions: conventions,
    naming_alignment,
    amenities_review,
    overmodeled_amenity_fields: overmodeled,
    founder_decisions,
    action_plan: {
      A_safe_to_apply_now: actions.filter((a) => a.bucket === "A"),
      B_founder_decision_required: actions.filter((a) => a.bucket === "B"),
      C_defer_until_after_enrichment: actions.filter((a) => a.bucket === "C"),
      D_do_not_change: actions.filter((a) => a.bucket === "D"),
      all: actions,
    },
    suggested_views,
    brand_explorer_safety: opts.beSafety || { pending: true },
    frozen_artifacts: {
      vic: hashVic(),
      frozen_62: FROZEN_62.map((rel) => {
        const p = join(ROOT, rel);
        if (!existsSync(p)) return { path: rel, exists: false };
        return {
          path: rel,
          exists: true,
          sha256: createHash("sha256").update(readFileSync(p)).digest("hex"),
        };
      }),
    },
    recommended_next_step:
      "Founder decides A–H (especially Last Reviewed Date rename + amenity hide/delete + Resort/Extended Stay renames). Then run a separate apply command for approved v1.1.1 changes only. Enrichment stays blocked until then.",
  };
}

export function renderV111PlanMarkdown(r) {
  const lines = [
    `# Production Census Schema v1.1.1 Cleanup Plan`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Mode:** read-only (no schema/record/BE writes)`,
    `**Generated:** ${r.generated_at}`,
    ``,
    `## 1. Executive summary`,
    ``,
    `- ${r.executive_summary.headline}`,
    `- Amenities: ${r.executive_summary.amenities}`,
    `- Naming: ${r.executive_summary.naming}`,
    `- Enrichment: ${r.executive_summary.enrichment}`,
    ``,
    `## 2. Current Census schema status`,
    ``,
    "```json",
    JSON.stringify(r.schema_status, null, 2),
    "```",
    ``,
    `## 3. Naming alignment findings`,
    ``,
    `| Current Field | Similar Existing Field | Existing Table | Classification | Recommended Final Name | Reason | Risk |`,
    `| --- | --- | --- | --- | --- | --- | --- |`,
  ];
  for (const row of r.naming_alignment.filter(
    (n) =>
      n.founder_decision_needed ||
      ["rename_recommended", "founder_decision_needed", "possible_duplicate", "do_not_change", "census_specific_name_ok"].includes(
        n.classification
      )
  )) {
    // include decision-critical + key classifications; also include keep_as_is for key fields already listed
  }
  // Full table for decision-relevant rows first, then summarize rest
  const priority = r.naming_alignment.filter(
    (n) =>
      n.founder_decision_needed ||
      n.classification !== "keep_as_is" ||
      [
        "Property Name",
        "Source URL",
        "Owner Name",
        "Operator / Management Company",
        "Last Verified Date",
        "Rooms / Keys",
        "State / Region",
        "Market / Submarket",
        "Production Use Status",
        "Enrichment Status",
        "Brand Explorer Slug if mapped",
      ].includes(n.current_field)
  );
  for (const row of priority) {
    lines.push(
      `| ${row.current_field} | ${row.similar_existing_field || "—"} | ${row.existing_table || "—"} | ${row.classification} | ${row.recommended_final_name} | ${String(row.reason).replace(/\|/g, "/")} | ${row.risk} |`
    );
  }
  lines.push(
    ``,
    `Other Census fields: **keep_as_is** (see JSON for full list).`,
    ``,
    `## 4. Amenities simplification findings`,
    ``,
    `Preferred model: **Amenities - Source Text** + **Amenities - Structured Tags** + strategic flags only.`,
    ``,
    `| Amenity Field | Classification | Recommendation | Filled /666 | Delete candidate if blank? |`,
    `| --- | --- | --- | ---: | --- |`
  );
  for (const a of r.amenities_review) {
    lines.push(
      `| ${a.amenity_field} | ${a.classification} | ${a.recommendation} | ${a.filled_count} | ${a.delete_candidate_if_blank === true} |`
    );
  }
  lines.push(
    ``,
    `## 5. Over-modeled amenity fields`,
    ``,
    "```json",
    JSON.stringify(r.overmodeled_amenity_fields, null, 2),
    "```",
    ``,
    `## 6. Founder decisions needed`,
    ``
  );
  for (const d of r.founder_decisions) {
    lines.push(
      `### ${d.id}`,
      ``,
      `- **Question:** ${d.question}`,
      `- **Options:** ${d.options.join(" | ")}`,
      `- **Recommendation:** ${d.recommendation}`,
      `- **Timing:** ${d.timing}`,
      d.note ? `- **Note:** ${d.note}` : "",
      ``
    );
  }
  lines.push(
    `## 7. Safe actions (bucket A)`,
    ``,
    "```json",
    JSON.stringify(r.action_plan.A_safe_to_apply_now, null, 2),
    "```",
    ``,
    `## 8. Deferred actions (bucket C)`,
    ``,
    "```json",
    JSON.stringify(r.action_plan.C_defer_until_after_enrichment, null, 2),
    "```",
    ``,
    `## 9. Do-not-change fields (bucket D)`,
    ``,
    "```json",
    JSON.stringify(r.action_plan.D_do_not_change, null, 2),
    "```",
    ``,
    `## 10. Suggested Airtable views (do not create yet)`,
    ``
  );
  for (const v of r.suggested_views) {
    lines.push(`### ${v.name}`, ``, ...v.fields.map((f) => `- ${f}`), ``);
  }
  lines.push(
    `## 11. Brand Explorer safety result`,
    ``,
    "```json",
    JSON.stringify(r.brand_explorer_safety, null, 2),
    "```",
    ``,
    `## 12. Recommended next step`,
    ``,
    r.recommended_next_step,
    ``,
    `### Action table`,
    ``,
    `| Action | Field | Reason | Risk | Schema change? | Record migration? | Timing |`,
    `| --- | --- | --- | --- | --- | --- | --- |`
  );
  for (const a of r.action_plan.all) {
    lines.push(
      `| ${a.action} | ${a.field} | ${String(a.reason).replace(/\|/g, "/")} | ${a.risk} | ${a.requires_schema_change} | ${a.requires_record_migration} | ${a.recommended_timing} |`
    );
  }
  return lines.filter((x) => x !== "").join("\n");
}
