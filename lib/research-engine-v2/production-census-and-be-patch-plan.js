/**
 * Production Census schema plan + Mexico VIC dry-run + Brand Explorer patch path.
 * Read-only: no Airtable writes, no VIC/62 freeze mutation.
 */

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const PLAN_VERSION = "production-census-and-be-patch-plan-v1";

export const EXPECTED_VIC_FREEZE =
  "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";

export const VIC_DIR = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family"
);

export const STATUS = Object.freeze({
  PLAN_READY: "production_census_plan_ready_for_schema_setup",
  DRY_RUN_READY: "production_census_dry_run_ready_for_founder_approval",
  BLOCKED_SCHEMA: "production_census_blocked_schema_missing",
});

export const AFFILIATION_STATUSES = Object.freeze([
  "Branded",
  "Soft-Branded / Collection",
  "Brand-Unconfirmed",
  "Independent",
  "Formerly Branded",
  "Future / Pipeline",
  "Unknown",
]);

export const PRODUCTION_USE_STATUS = "Census Only / Not Owner-Facing";

/** Soft-brand / collection brands (Mexico VIC-aware). */
const SOFT_BRAND_EXACT = new Set(
  [
    "Ascend Hotel Collection",
    "Curio Collection by Hilton",
    "Autograph Collection",
    "Tribute Portfolio",
    "Design Hotels",
    "The Luxury Collection",
    "Tapestry by Hilton",
    "Tapestry Collection by Hilton",
    "Small Luxury Hotels of the World",
    "Apartment Collection by Hilton",
    "JOIA Iberostar",
    "voco",
  ].map((s) => s.toLowerCase())
);

const SOFT_BRAND_RE =
  /\b(ascend|curio|autograph|tribute|design hotels|luxury collection|tapestry|small luxury|apartment collection|joia|voco)\b/i;

const PROPOSED_TABLES = Object.freeze([
  {
    key: "census",
    name: "Hotel Property Census",
    role: "Primary hotel/property identity",
    write_allowed_stage: "census_production_after_founder_approval",
  },
  {
    key: "affiliations",
    name: "Hotel Property Brand Affiliations",
    role: "Current / historical / future / soft-brand / brand-unconfirmed / independent states",
    write_allowed_stage: "census_production_after_founder_approval",
  },
  {
    key: "evidence",
    name: "Hotel Property Source Evidence",
    role: "Source lineage, URL, type, discovery date, confidence, freeze hash",
    write_allowed_stage: "census_production_after_founder_approval",
  },
  {
    key: "steward",
    name: "Hotel Property Steward Review",
    role: "Holds, ambiguity, duplicate risk, brand-unconfirmed, manual decisions",
    write_allowed_stage: "census_production_after_founder_approval",
  },
]);

const EXISTING_PLATFORM_RELATED = Object.freeze([
  {
    name: "Hotel Census",
    id: "tblgj2qEwxjTcg6q0",
    role: "Legacy STR-backed reference — READ ONLY for VIC; never write from this lane",
  },
  {
    name: "Verified Independent Hotel Census",
    id: "tbljBvId1z4cEyE1J",
    role: "Existing verified master stub (18 fields) — incomplete vs Hotel Property Census model; do not overload as BE write target",
  },
  {
    name: "Independent Hotel Source Candidates",
    id: "tblKAUKiku7eMlV0d",
    role: "Staging candidates — keep separate from production Hotel Property Census",
  },
  {
    name: "Independent Hotel Source Evidence",
    id: "tbl4FrVompVBs9mTw",
    role: "Staging evidence — keep separate from Hotel Property Source Evidence",
  },
]);

const CENSUS_FIELDS = Object.freeze({
  identity: [
    "Property Name",
    "Canonical Property Name",
    "Property Identity Key",
    "Family / Source Family",
    "Country",
    "State / Region",
    "City",
    "Address",
    "Latitude",
    "Longitude",
    "Phone",
    "Official Property URL",
    "Source URL",
    "Source Type",
    "Source Confidence",
    "Discovery Date",
    "VIC Freeze Hash",
    "Data Eligible",
    "Identity Confidence",
    "Production Use Status",
  ],
  affiliation: [
    "Current Brand",
    "Brand Family",
    "Brand Explorer Slug if mapped",
    "Affiliation Status",
    "Affiliation As-Of Date",
    "Affiliation Start Date",
    "Prior Brand",
    "Future Opening Flag",
    "Brand Confidence",
    "Steward Review Status",
  ],
});

const SAFE_WRITE_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Family / Source Family",
  "Country",
  "State / Region",
  "City",
  "Address",
  "Official Property URL",
  "Source URL",
  "Source Type",
  "Source Confidence",
  "Discovery Date",
  "VIC Freeze Hash",
  "Data Eligible",
  "Identity Confidence",
  "Production Use Status",
  "Current Brand",
  "Brand Family",
  "Brand Explorer Slug if mapped",
  "Affiliation Status",
  "Affiliation As-Of Date",
  "Future Opening Flag",
  "Brand Confidence",
  "Steward Review Status",
  "Latitude",
  "Longitude",
  "Phone",
]);

const UNSAFE_HELD_FIELDS = Object.freeze([
  "Rooms",
  "Owner",
  "Operator",
  "Opening Date",
  "Affiliation Start Date (unless source-supported — VIC policy: never fabricate)",
  "Brand Explorer Presentation Title/Body/Slot Key",
  "Brand Basics",
  "Brand Status",
  "release fields",
  "Company Validated",
  "Brand Verified",
  "Recent Momentum",
  "any public-rendered Brand Explorer field",
  "coordinates when missing (never 0,0)",
]);

const FORBIDDEN_PRODUCTION_TARGETS = Object.freeze([
  "Brand Setup - Brand Explorer Presentation",
  "Brand Setup - Brand Basics",
  "Brand Status / release fields",
  "Company Validated",
  "Brand Verified",
  "Recent Momentum",
  "Hotel Census (legacy STR)",
]);

function hashDirectoryFiles(dir) {
  if (!existsSync(dir)) return null;
  const files = readdirSync(dir)
    .filter((f) => f.endsWith(".json") || f.endsWith(".md"))
    .sort();
  const h = createHash("sha256");
  for (const f of files) {
    const p = join(dir, f);
    if (!statSync(p).isFile()) continue;
    h.update(f);
    h.update("\0");
    h.update(readFileSync(p));
    h.update("\0");
  }
  return { fileCount: files.length, aggregate_sha256: h.digest("hex"), files };
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

export function classifyAffiliationStatus(record, overlayById) {
  const brand = String(record.brand || "").trim();
  const status = record.status;
  const overlay = overlayById.get(record.independent_record_id);

  if (/brand\s*unconfirmed/i.test(brand) || brand.toLowerCase().includes("unconfirmed")) {
    if (overlay?.action === "confirm_brand") {
      return {
        affiliation_status: "Branded",
        brand_for_census: overlay.brand || brand,
        steward_review_status: "overlay_confirm_brand",
        hold: false,
      };
    }
    if (overlay?.action === "exclude_from_brand_completion") {
      return {
        affiliation_status: "Brand-Unconfirmed",
        brand_for_census: brand,
        steward_review_status: "exclude_from_brand_completion",
        hold: true,
        hold_reason: "marriott_steward_exclude_from_brand_completion",
      };
    }
    if (overlay?.action === "steward_manual_review_required") {
      return {
        affiliation_status: "Brand-Unconfirmed",
        brand_for_census: brand,
        steward_review_status: "steward_manual_review_required",
        hold: true,
        hold_reason: "marriott_steward_manual_review_required",
      };
    }
    return {
      affiliation_status: "Brand-Unconfirmed",
      brand_for_census: brand,
      steward_review_status: "brand_unconfirmed_held",
      hold: true,
      hold_reason: "brand_unconfirmed",
    };
  }

  if (status === "Pipeline" || /pipeline|future|coming soon/i.test(String(status || ""))) {
    return {
      affiliation_status: "Future / Pipeline",
      brand_for_census: brand || "Unknown",
      steward_review_status: "pipeline",
      hold: false,
    };
  }

  if (!brand) {
    return {
      affiliation_status: "Unknown",
      brand_for_census: "Unknown",
      steward_review_status: "missing_brand",
      hold: true,
      hold_reason: "missing_brand",
    };
  }

  if (/^independent$/i.test(brand) || /independent\s*hotel/i.test(brand)) {
    return {
      affiliation_status: "Independent",
      brand_for_census: brand,
      steward_review_status: "independent",
      hold: false,
    };
  }

  if (SOFT_BRAND_EXACT.has(brand.toLowerCase()) || SOFT_BRAND_RE.test(brand)) {
    return {
      affiliation_status: "Soft-Branded / Collection",
      brand_for_census: brand,
      steward_review_status: "none",
      hold: false,
    };
  }

  return {
    affiliation_status: "Branded",
    brand_for_census: brand,
    steward_review_status: "none",
    hold: false,
  };
}

function missingRequiredForCensus(record) {
  const missing = [];
  if (!record.name) missing.push("Property Name");
  if (!record.independent_record_id) missing.push("Property Identity Key");
  if (!record.country) missing.push("Country");
  if (!record.website && !record.discovery_source) missing.push("Official Property URL / Source URL");
  return missing;
}

async function probeAirtableSchema() {
  const key =
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    "";
  const prodMvp = process.env.AIRTABLE_BASE_ID || "";
  const platform = process.env.AIRTABLE_BASE_ID_ALT || "";

  const result = {
    probed: Boolean(key),
    prod_mvp: { base_id_masked: prodMvp ? `${prodMvp.slice(0, 6)}…` : null, tables: {} },
    platform: { base_id_masked: platform ? `${platform.slice(0, 6)}…` : null, tables: {} },
    proposed_table_existence: {},
    schema_must_be_created_manually: true,
    recommended_base: "platform (AIRTABLE_BASE_ID_ALT) — isolate from MVP Brand Explorer rendering",
  };

  if (!key) {
    result.error = "No Airtable token in env; schema existence inferred from docs + prior knowledge only";
    for (const t of PROPOSED_TABLES) {
      result.proposed_table_existence[t.name] = {
        exists: false,
        inferred: true,
        note: "Token missing; treat as absent pending Meta probe",
      };
    }
    return result;
  }

  async function listTables(baseId, bucket) {
    if (!baseId) return;
    const r = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${key}` },
    });
    const d = await r.json().catch(() => ({}));
    bucket.http_status = r.status;
    bucket.table_count = (d.tables || []).length;
    if (!r.ok) {
      bucket.error = d.error || d;
      return;
    }
    const byName = Object.fromEntries((d.tables || []).map((t) => [t.name, { id: t.id, field_count: t.fields?.length }]));
    bucket.tables = byName;
  }

  await listTables(prodMvp, result.prod_mvp);
  await listTables(platform, result.platform);

  for (const t of PROPOSED_TABLES) {
    const onPlatform = result.platform.tables?.[t.name];
    const onMvp = result.prod_mvp.tables?.[t.name];
    result.proposed_table_existence[t.name] = {
      exists: Boolean(onPlatform || onMvp),
      platform: onPlatform || null,
      prod_mvp: onMvp || null,
    };
  }

  const allExist = PROPOSED_TABLES.every(
    (t) => result.proposed_table_existence[t.name]?.exists
  );
  const anyExist = PROPOSED_TABLES.some(
    (t) => result.proposed_table_existence[t.name]?.exists
  );
  result.schema_must_be_created_manually = !allExist;
  result.all_four_proposed_absent = !anyExist;
  result.all_four_proposed_exist = allExist;
  result.tables_ready_on_platform = PROPOSED_TABLES.every(
    (t) => Boolean(result.platform.tables?.[t.name])
  );

  return result;
}

function buildSchemaPlan(schemaProbe) {
  const gaps = [];
  for (const t of PROPOSED_TABLES) {
    if (!schemaProbe.proposed_table_existence[t.name]?.exists) {
      gaps.push({
        table: t.name,
        gap: "table_missing",
        action: "Create manually in Platform base (or dedicated census base) before any write",
      });
    }
  }

  gaps.push({
    table: "Verified Independent Hotel Census",
    gap: "field_model_incomplete_vs_hotel_property_census",
    existing_field_count: 18,
    note: "Do not silently remap VIC freeze into this stub; prefer new Hotel Property Census tables",
  });

  gaps.push({
    table: "Hotel Census",
    gap: "legacy_str_not_write_target",
    action: "Keep read-only; never receive VIC production writes",
  });

  const allExist = schemaProbe.all_four_proposed_exist === true;

  return {
    plan_version: PLAN_VERSION,
    operator_explorer_lane: "paused",
    founder_priority: {
      census_first: true,
      brand_explorer_controlled: true,
      operator_explorer: "paused_do_not_start",
    },
    recommended_tables: PROPOSED_TABLES,
    required_fields: CENSUS_FIELDS,
    affiliation_statuses: AFFILIATION_STATUSES,
    production_use_status_default: PRODUCTION_USE_STATUS,
    existing_related_tables_platform: EXISTING_PLATFORM_RELATED,
    schema_probe: schemaProbe,
    existing_production_schema_gaps: gaps,
    tables_already_exist: allExist,
    schema_must_be_created_manually: schemaProbe.schema_must_be_created_manually !== false && !allExist,
    write_rules: {
      allowed: "create/update records in dedicated Census tables only (after schema + founder approval)",
      forbidden_now: FORBIDDEN_PRODUCTION_TARGETS,
      mark_records: `Production Use Status = ${PRODUCTION_USE_STATUS}`,
    },
    safe_fields_to_write: SAFE_WRITE_FIELDS,
    unsafe_fields_held: UNSAFE_HELD_FIELDS,
    no_fabricate: [
      "rooms",
      "owner",
      "operator",
      "opening date",
      "affiliation start date",
      "missing coordinates as 0,0",
    ],
    brand_explorer_consumption:
      "Later: Brand Explorer consumes only approved Branded / Soft-Branded / Collection subset — not this lane",
  };
}

function buildCensusDryRun(schemaPlan) {
  const indexPath = join(VIC_DIR, "01_combined_4family_index.json");
  const eligiblePath = join(VIC_DIR, "07_data_eligible_index.json");
  const overlayPath = join(VIC_DIR, "11_marriott_steward_overlay.json");
  const readinessPath = join(VIC_DIR, "13_staging_migration_readiness.json");
  const lockPath = join(VIC_DIR, "00_baseline_lock.json");

  if (!existsSync(indexPath)) {
    throw new Error(`Missing VIC index: ${indexPath}`);
  }

  const index = readJson(indexPath);
  const eligible = existsSync(eligiblePath) ? readJson(eligiblePath) : { records: [] };
  const overlay = existsSync(overlayPath) ? readJson(overlayPath) : { brand_unconfirmed_overlay: [] };
  const readiness = existsSync(readinessPath) ? readJson(readinessPath) : {};
  const lock = existsSync(lockPath) ? readJson(lockPath) : {};

  const freezeHash =
    lock.freeze_hash_sha256 ||
    lock.sha256 ||
    index.freeze_hash_sha256 ||
    EXPECTED_VIC_FREEZE;

  const dirHash = hashDirectoryFiles(VIC_DIR);
  const eligibleIds = new Set((eligible.records || []).map((r) => r.independent_record_id));
  const overlayById = new Map(
    (overlay.brand_unconfirmed_overlay || []).map((o) => [o.independent_record_id, o])
  );

  const byAffiliation = Object.fromEntries(AFFILIATION_STATUSES.map((s) => [s, 0]));
  const toCreate = [];
  const toUpdate = [];
  const skipped = [];
  const held = [];
  const independentOrNonBrandReady = [];
  const duplicateRisks = [];
  const missingRequired = [];

  const seenKeys = new Map();

  for (const rec of index.records || []) {
    const classification = classifyAffiliationStatus(rec, overlayById);
    byAffiliation[classification.affiliation_status] =
      (byAffiliation[classification.affiliation_status] || 0) + 1;

    const missing = missingRequiredForCensus(rec);
    const dataEligible = eligibleIds.has(rec.independent_record_id);
    const identityKey = rec.independent_record_id;

    if (seenKeys.has(identityKey)) {
      duplicateRisks.push({
        independent_record_id: identityKey,
        other: seenKeys.get(identityKey),
        risk: "duplicate_identity_key_in_vic_index",
      });
    } else {
      seenKeys.set(identityKey, rec.name);
    }

    const planned = {
      independent_record_id: identityKey,
      property_name: rec.name,
      family: rec.family,
      brand: classification.brand_for_census,
      city: rec.city || "Unknown",
      country: rec.country || "Unknown",
      affiliation_status: classification.affiliation_status,
      data_eligible: dataEligible,
      production_use_status: PRODUCTION_USE_STATUS,
      steward_review_status: classification.steward_review_status,
      fields_to_write_preview: {
        "Property Name": rec.name,
        "Canonical Property Name": rec.name,
        "Property Identity Key": identityKey,
        "Family / Source Family": rec.family,
        Country: rec.country || "Unknown",
        City: rec.city || "Unknown",
        "Official Property URL": rec.website || null,
        "Source URL": rec.discovery_source || rec.website || null,
        "Source Type": "brand_directory",
        "VIC Freeze Hash": freezeHash,
        "Data Eligible": dataEligible,
        "Production Use Status": PRODUCTION_USE_STATUS,
        "Current Brand": classification.brand_for_census,
        "Brand Family": rec.parent || rec.family,
        "Affiliation Status": classification.affiliation_status,
        "Future Opening Flag": classification.affiliation_status === "Future / Pipeline",
      },
      fields_explicitly_not_written: [
        "Rooms",
        "Owner",
        "Operator",
        "Opening Date",
        "Affiliation Start Date",
        "Latitude/Longitude when absent",
      ],
    };

    if (missing.length) {
      missingRequired.push({ independent_record_id: identityKey, missing });
      skipped.push({
        independent_record_id: identityKey,
        reason: "missing_required_fields",
        missing,
      });
      continue;
    }

    if (classification.hold) {
      held.push({
        independent_record_id: identityKey,
        reason: classification.hold_reason,
        affiliation_status: classification.affiliation_status,
        steward_review_status: classification.steward_review_status,
      });
      // Held rows still planned as creates into Steward Review + Census with hold flag —
      // but execute blocked until founder + steward clearance for those rows.
      toCreate.push({ ...planned, create_with_hold: true });
      continue;
    }

    if (
      classification.affiliation_status === "Independent" ||
      classification.affiliation_status === "Brand-Unconfirmed" ||
      classification.affiliation_status === "Unknown"
    ) {
      independentOrNonBrandReady.push(planned);
    }

    // Tables do not exist yet → all are creates (0 updates against Hotel Property Census)
    toCreate.push(planned);
  }

  // Website collision duplicate risks
  const byWebsite = new Map();
  for (const rec of index.records || []) {
    const w = String(rec.website || "")
      .trim()
      .toLowerCase();
    if (!w) continue;
    if (!byWebsite.has(w)) byWebsite.set(w, []);
    byWebsite.get(w).push(rec.independent_record_id);
  }
  for (const [w, ids] of byWebsite) {
    if (ids.length > 1) {
      duplicateRisks.push({
        risk: "shared_official_url",
        website: w,
        independent_record_ids: ids,
      });
    }
  }

  const schemaReady =
    schemaPlan.tables_already_exist === true &&
    (schemaPlan.schema_must_be_created_manually === false ||
      schemaPlan.schema_probe?.all_four_proposed_exist === true);

  const productionSafety = {
    production_writes_executed: false,
    execute: false,
    freeze_artifacts_modified: false,
    brand_explorer_modified: false,
    schema_ready_for_write: schemaReady,
    founder_approval_required: true,
    production_census_write_may_proceed: false,
    reason_blocked: schemaReady
      ? "Schema present — awaiting founder approval of census record dry-run (no execute yet)"
      : "Proposed Hotel Property * tables missing — create schema first",
  };

  return {
    plan_version: PLAN_VERSION,
    dry_run: true,
    execute: false,
    source: {
      path: VIC_DIR,
      baseline_status: index.baseline_status || lock.baseline_status,
      expected_freeze_hash: EXPECTED_VIC_FREEZE,
      freeze_hash: freezeHash,
      freeze_hash_match: freezeHash === EXPECTED_VIC_FREEZE,
      total_records: (index.records || []).length,
      data_eligible: eligibleIds.size,
      directory_aggregate: dirHash,
      staging_readiness: readiness,
    },
    counts: {
      records_to_create: toCreate.length,
      records_to_update: toUpdate.length,
      skipped_records: skipped.length,
      held_records: held.length,
      missing_required_fields: missingRequired.length,
      duplicate_risks: duplicateRisks.length,
      brand_unconfirmed_records: byAffiliation["Brand-Unconfirmed"] || 0,
      independent_or_non_brand_ready: independentOrNonBrandReady.length,
      by_affiliation_status: byAffiliation,
      data_eligible: eligibleIds.size,
      not_data_eligible: (index.records || []).length - eligibleIds.size,
    },
    brand_unconfirmed_ids: [
      ...new Set(held.filter((h) => h.affiliation_status === "Brand-Unconfirmed").map((h) => h.independent_record_id)),
    ],
    samples: {
      to_create_first_5: toCreate.slice(0, 5),
      held_first_10: held.slice(0, 10),
      skipped_first_5: skipped.slice(0, 5),
      duplicate_risks_first_5: duplicateRisks.slice(0, 5),
    },
    fields_to_be_written: SAFE_WRITE_FIELDS,
    fields_explicitly_not_written: UNSAFE_HELD_FIELDS,
    production_safety_status: productionSafety,
    note: "Dry-run only. No Airtable census create/update until schema setup + founder approval.",
  };
}

function buildBePatchPath() {
  const mediumReviewPath = join(
    ROOT,
    "reports/research-engine-v2/mexico-vic-be-medium-sandbox-founder-review.json"
  );
  const laneClosurePath = join(ROOT, "reports/research-engine-v2/vic-be-sandbox-lane-closure.json");
  const mediumReview = existsSync(mediumReviewPath) ? readJson(mediumReviewPath) : null;
  const laneClosure = existsSync(laneClosurePath) ? readJson(laneClosurePath) : null;

  return {
    plan_version: PLAN_VERSION,
    execute: false,
    brand_explorer_production_patch_run_now: false,
    brand_explorer_production_patch_remains_blocked: true,
    operator_explorer: "paused",
    sandbox_source: {
      slot_namespace: "vic.pilot.medium.*",
      rows: 28,
      brands: 7,
      brand_slugs: [
        "hotel-indigo",
        "ascend",
        "curio-collection",
        "holiday-inn-express",
        "voco-hotels",
        "kimpton",
        "avid-hotels",
      ],
      founder_review: mediumReview?.status || "medium_sandbox_founder_review_approved_pause_vic_lane",
      lane_closure_status: laneClosure?.status || null,
    },
    options: {
      A: {
        name: "Non-rendering production rows",
        slot_namespace: "vic.production.pilot.medium.*",
        description:
          "Create non-rendering production Presentation rows under vic.production.pilot.medium.* so approved evidence is stored without changing owner-facing UI",
        risk: "low",
        recommended: true,
      },
      B: {
        name: "Patch real rendered Brand Explorer slots",
        description: "Write into live rendered slots — higher risk; requires founder approval + visual review",
        risk: "high",
        recommended: false,
      },
    },
    default_recommendation: "Option A",
    must_not_touch: [
      "Brand Status",
      "release fields",
      "Company Validated",
      "Brand Verified",
      "Recent Momentum",
      "owner / operator / rooms / open-date / affiliation-start-date fields",
      "frozen_62 baseline artifacts",
    ],
    production_dry_run_only: true,
    next_step:
      "After Census schema + VIC census write path is approved, run a separate BE Option A dry-run (no execute) for founder approval",
    frozen_62: {
      decision: "frozen_62_active_public_full_baseline_quality_clean_flex_held",
      mutated: false,
      bulk_public_patch: false,
    },
  };
}

export function renderSchemaPlanMarkdown(plan) {
  const lines = [
    `# Production Census Airtable Schema Plan`,
    ``,
    `**Status:** \`${plan._acceptance_status || STATUS.PLAN_READY}\``,
    `**Version:** ${PLAN_VERSION}`,
    `**Operator Explorer:** paused`,
    ``,
    `## Decision`,
    ``,
    `- Production Airtable Census foundation first`,
    `- Brand Explorer production updates controlled (no bulk public patch)`,
    `- Operator Explorer remains paused`,
    ``,
    `## Recommended tables`,
    ``,
  ];
  for (const t of plan.recommended_tables) {
    lines.push(`1. **${t.name}** — ${t.role}`);
  }
  lines.push(
    ``,
    `## Required fields`,
    ``,
    `### Identity`,
    ``,
    ...plan.required_fields.identity.map((f) => `- ${f}`),
    ``,
    `### Affiliation`,
    ``,
    ...plan.required_fields.affiliation.map((f) => `- ${f}`),
    ``,
    `### Affiliation statuses`,
    ``,
    ...plan.affiliation_statuses.map((s) => `- ${s}`),
    ``,
    `## Existing production schema gaps`,
    ``
  );
  for (const g of plan.existing_production_schema_gaps) {
    lines.push(`- **${g.table}:** ${g.gap}${g.action ? ` — ${g.action}` : ""}${g.note ? ` (${g.note})` : ""}`);
  }
  lines.push(
    ``,
    `## Do tables already exist?`,
    ``,
    `- Proposed four tables exist: **${plan.tables_already_exist ? "yes" : "no"}**`,
    `- Schema must be created manually: **${plan.schema_must_be_created_manually ? "yes" : "no"}**`,
    `- Recommended base: ${plan.schema_probe?.recommended_base || "platform"}`,
    ``,
    `## Write rules`,
    ``,
    `- Allowed: ${plan.write_rules.allowed}`,
    `- Mark: \`${plan.write_rules.mark_records}\``,
    `- Forbidden now: ${plan.write_rules.forbidden_now.join("; ")}`,
    ``,
    `## Safe vs unsafe fields`,
    ``,
    `**Safe (when schema + approval):**`,
    ``,
    ...plan.safe_fields_to_write.map((f) => `- ${f}`),
    ``,
    `**Unsafe / held:**`,
    ``,
    ...plan.unsafe_fields_held.map((f) => `- ${f}`),
    ``,
    `## Probe summary`,
    ``,
    "```json",
    JSON.stringify(
      {
        platform: plan.schema_probe?.platform?.base_id_masked,
        prod_mvp: plan.schema_probe?.prod_mvp?.base_id_masked,
        proposed: plan.schema_probe?.proposed_table_existence,
      },
      null,
      2
    ),
    "```",
    ``
  );
  return lines.join("\n");
}

export function renderCensusDryRunMarkdown(dry) {
  const c = dry.counts;
  const lines = [
    `# Mexico VIC → Production Census Dry-Run`,
    ``,
    `**Dry-run only — execute: false**`,
    `**Freeze hash:** \`${dry.source.freeze_hash}\``,
    `**Freeze match:** ${dry.source.freeze_hash_match}`,
    `**Total VIC records:** ${dry.source.total_records}`,
    `**Data eligible:** ${dry.source.data_eligible}`,
    ``,
    `## Counts`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Records to create | ${c.records_to_create} |`,
    `| Records to update | ${c.records_to_update} |`,
    `| Skipped | ${c.skipped_records} |`,
    `| Held | ${c.held_records} |`,
    `| Missing required fields | ${c.missing_required_fields} |`,
    `| Duplicate risks | ${c.duplicate_risks} |`,
    `| Brand-unconfirmed (ids) | ${dry.brand_unconfirmed_ids?.length ?? c.brand_unconfirmed_records} |`,
    `| Independent / non-brand-ready samples | ${c.independent_or_non_brand_ready} |`,
    ``,
    `### By affiliation status`,
    ``,
    `| Status | Count |`,
    `| --- | ---: |`,
    ...Object.entries(c.by_affiliation_status).map(([k, v]) => `| ${k} | ${v} |`),
    ``,
    `## Fields to be written`,
    ``,
    ...dry.fields_to_be_written.map((f) => `- ${f}`),
    ``,
    `## Fields explicitly not written`,
    ``,
    ...dry.fields_explicitly_not_written.map((f) => `- ${f}`),
    ``,
    `## Production safety status`,
    ``,
    "```json",
    JSON.stringify(dry.production_safety_status, null, 2),
    "```",
    ``,
    `## Held sample (first 10)`,
    ``,
    "```json",
    JSON.stringify(dry.samples.held_first_10, null, 2),
    "```",
    ``,
    `## Decision`,
    ``,
    `- Production Census write may proceed: **${dry.production_safety_status.production_census_write_may_proceed}**`,
    `- Reason: ${dry.production_safety_status.reason_blocked}`,
    ``,
  ];
  return lines.join("\n");
}

export function renderBePatchPathMarkdown(be) {
  return [
    `# Brand Explorer Production Patch Path`,
    ``,
    `**Execute now:** false`,
    `**Production patch remains blocked:** ${be.brand_explorer_production_patch_remains_blocked}`,
    `**Default recommendation:** ${be.default_recommendation}`,
    ``,
    `## Sandbox source (approved medium pilot)`,
    ``,
    `- Slot namespace: \`${be.sandbox_source.slot_namespace}\``,
    `- Rows: ${be.sandbox_source.rows}`,
    `- Brands: ${be.sandbox_source.brands}`,
    `- Founder review: \`${be.sandbox_source.founder_review}\``,
    ``,
    `## Options`,
    ``,
    `### Option A (recommended)`,
    ``,
    `- Namespace: \`${be.options.A.slot_namespace}\``,
    `- ${be.options.A.description}`,
    `- Risk: ${be.options.A.risk}`,
    ``,
    `### Option B (higher risk)`,
    ``,
    `- ${be.options.B.description}`,
    `- Risk: ${be.options.B.risk}`,
    ``,
    `## Must not touch`,
    ``,
    ...be.must_not_touch.map((x) => `- ${x}`),
    ``,
    `## Next step`,
    ``,
    be.next_step,
    ``,
  ].join("\n");
}

/**
 * @returns {Promise<object>}
 */
export async function runProductionCensusAndBePatchPlan() {
  const schemaProbe = await probeAirtableSchema();
  const schemaPlan = buildSchemaPlan(schemaProbe);
  const dryRun = buildCensusDryRun(schemaPlan);
  const bePatch = buildBePatchPath();

  let acceptance = STATUS.PLAN_READY;
  if (!existsSync(join(VIC_DIR, "01_combined_4family_index.json")) || !dryRun.source.freeze_hash_match) {
    acceptance = STATUS.BLOCKED_SCHEMA;
  } else if (schemaProbe.all_four_proposed_exist === true) {
    acceptance = STATUS.DRY_RUN_READY;
  } else if (schemaPlan.schema_must_be_created_manually || schemaProbe.all_four_proposed_absent) {
    acceptance = STATUS.PLAN_READY;
  } else if (dryRun.production_safety_status.founder_approval_required) {
    acceptance = STATUS.DRY_RUN_READY;
  }

  schemaPlan._acceptance_status = acceptance;

  return {
    generated_at: new Date().toISOString(),
    plan_version: PLAN_VERSION,
    acceptance_status: acceptance,
    operator_explorer_paused: true,
    production_writes_occurred: false,
    brand_explorer_production_modified: false,
    frozen_vic_modified: false,
    frozen_62_modified: false,
    production_census_write_may_proceed: false,
    brand_explorer_production_patch_remains_blocked: true,
    recommended_be_option: "A",
    schema_plan: schemaPlan,
    census_dry_run: dryRun,
    brand_explorer_patch_path: bePatch,
    summary: {
      recommended_airtable_census_tables: PROPOSED_TABLES.map((t) => t.name),
      required_fields: CENSUS_FIELDS,
      existing_production_schema_gaps: schemaPlan.existing_production_schema_gaps,
      tables_already_exist: schemaPlan.tables_already_exist,
      schema_must_be_created_manually: schemaPlan.schema_must_be_created_manually,
      dry_run_count_666: dryRun.source.total_records,
      records_to_create: dryRun.counts.records_to_create,
      records_to_update: dryRun.counts.records_to_update,
      held_records: dryRun.counts.held_records,
      safe_fields_to_write: SAFE_WRITE_FIELDS,
      unsafe_fields_held: UNSAFE_HELD_FIELDS,
      brand_explorer_production_patch_recommendation: "Option A — vic.production.pilot.medium.* (non-rendering)",
      production_census_write_may_proceed: false,
      brand_explorer_production_patch_remains_blocked: true,
      acceptance_status: acceptance,
    },
  };
}
