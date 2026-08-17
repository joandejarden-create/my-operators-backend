/**
 * Read-only post-apply review of Production Census schema v1.1.
 * No Airtable schema/record writes. No Brand Explorer writes.
 */

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import {
  TABLE_IDS,
  EXPECTED_FREEZE,
  PRODUCTION_USE_STATUS,
} from "./production-census-write.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const REVIEW_VERSION = "production-census-v11-post-apply-review-v1";

export const STATUS = Object.freeze({
  CLEAN: "production_census_v11_post_apply_review_clean_ready_for_enrichment",
  MINOR_CLEANUP: "production_census_v11_post_apply_review_minor_cleanup_recommended",
  HOLD: "production_census_v11_post_apply_review_hold_before_enrichment",
});

const VIC_DIR = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family"
);
const FROZEN_62 = [
  "reports/brand-explorer-62-active-public-full-baseline.json",
  "reports/brand-explorer-62-active-public-full-baseline.md",
  "docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md",
  "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js",
];

const SOFT_BRAND_RE =
  /\b(ascend|curio|autograph|tribute|design hotels|luxury collection|tapestry|small luxury|apartment collection|joia|voco)\b/i;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function maskId(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function maskToken(token) {
  if (!token) return null;
  if (token.length < 12) return "***";
  return `${token.slice(0, 6)}…${token.slice(-4)}`;
}
function hashVicDir() {
  if (!existsSync(VIC_DIR)) return null;
  const files = readdirSync(VIC_DIR)
    .filter((f) => f.endsWith(".json") || f.endsWith(".md"))
    .sort();
  const h = createHash("sha256");
  for (const f of files) {
    const p = join(VIC_DIR, f);
    if (!statSync(p).isFile()) continue;
    h.update(f);
    h.update("\0");
    h.update(readFileSync(p));
    h.update("\0");
  }
  return { file_count: files.length, aggregate_sha256: h.digest("hex") };
}
function fingerprintArtifacts(paths) {
  return paths.map((rel) => {
    const p = join(ROOT, rel);
    if (!existsSync(p)) return { path: rel, exists: false };
    const st = statSync(p);
    return {
      path: rel,
      exists: true,
      size: st.size,
      mtime_ms: st.mtimeMs,
      sha256: createHash("sha256").update(readFileSync(p)).digest("hex"),
    };
  });
}

async function metaTables(baseId, token) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`meta ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json.tables || [];
}

async function listAll(baseId, token, tableId, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${tableId} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

function fieldIndex(tables, tableNames) {
  const out = [];
  for (const name of tableNames) {
    const t = tables.find((x) => x.name === name);
    if (!t) continue;
    for (const f of t.fields || []) {
      out.push({ table: name, table_id: t.id, field: f.name, type: f.type });
    }
  }
  return out;
}

function findSimilar(existingIndex, censusField) {
  const norm = (s) =>
    String(s || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  const target = norm(censusField);
  const tokens = new Set(target.split(/\s+/).filter((t) => t.length > 2));
  const hits = [];
  for (const row of existingIndex) {
    const n = norm(row.field);
    if (!n) continue;
    if (n === target) {
      hits.push({ ...row, score: 100 });
      continue;
    }
    const overlap = n.split(/\s+/).filter((t) => tokens.has(t)).length;
    if (overlap >= 2 || (overlap === 1 && tokens.size <= 2 && n.includes([...tokens][0]))) {
      hits.push({ ...row, score: overlap * 20 + (n.includes(target) || target.includes(n) ? 30 : 0) });
    }
  }
  hits.sort((a, b) => b.score - a.score);
  return hits[0] || null;
}

/** Explicit naming alignment decisions for key Census fields. */
function classifyNaming(censusField, similar) {
  const map = {
    "Property Name": {
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason:
        "Brand Basics uses Brand Name; Hotel Ownership uses Property Name; Verified Independent uses Verified Hotel Name. Property Name is correct for property master.",
      similar_hint: { field: "Property Name / Brand Name / Verified Hotel Name", table: "Hotel Ownership / Brand Basics / VIC stub" },
    },
    "Brand Explorer Slug if mapped": {
      classification: "census_specific_name_ok",
      recommendation: "add_alias/documentation_only",
      reason:
        "No literal Brand Slug field on Brand Basics (slug is derived in code). Name is verbose but clear; optional later rename to Brand Explorer Slug.",
      similar_hint: { field: "(code slug / Brand Name)", table: "Brand Setup - Brand Basics" },
    },
    "Owner Name": {
      classification: "aligned_with_existing_name",
      recommendation: "keep",
      reason: "Aligns with Owner Company (legacy Hotel Census) and Owner/Operator Name (Hotel Ownership); distinct from Company Name on Company Profile.",
      similar_hint: { field: "Owner Company / Owner/Operator Name / Company Name", table: "Hotel Census / Hotel Ownership / Company Profile" },
    },
    "Operator / Management Company": {
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason:
        "Bridges Management Company (legacy Hotel Census) and Operator company_name (Operator Setup). Slash form is explicit for property-level ops.",
      similar_hint: { field: "Management Company / company_name", table: "Hotel Census / Operator Setup - Master" },
    },
    "Source URL": {
      classification: "aligned_with_existing_name",
      recommendation: "keep",
      reason: "Matches Source URL usage pattern; Brand Basics uses Brand Website / branded-residences Source URL variants.",
      similar_hint: { field: "Brand Website / Branded Residences Source URL", table: "Brand Setup - Brand Basics" },
    },
    "Source Confidence": {
      classification: "possible_duplicate_concept",
      recommendation: "add_alias/documentation_only",
      reason:
        "Brand Setup uses Confidence Level; Census also has Relationship Confidence + Data Confidence Tier. Document roles: Source=evidence quality, Relationship=link certainty, Tier=overall.",
      similar_hint: { field: "Confidence Level", table: "Brand Setup - Brand Basics" },
    },
    "Relationship Confidence": {
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason: "Census-specific for ownership/operator relationship certainty; not a Brand Setup duplicate.",
    },
    "Steward Review Status": {
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason: "Distinct from Validation Status / Brand Status / Ownership Review Status. Steward lane is Census-ops specific.",
      similar_hint: { field: "Validation Status / Brand Status", table: "Brand Setup - Brand Basics" },
    },
    "Ownership Review Status": {
      classification: "aligned_with_existing_name",
      recommendation: "keep",
      reason: "Parallel to Brand Review Status patterns and Company Profile Owner Profile Status.",
      similar_hint: { field: "Owner Profile Status / Branded Residences Review Status", table: "Company Profile / Brand Basics" },
    },
    "Operator Review Status": {
      classification: "aligned_with_existing_name",
      recommendation: "keep",
      reason: "Parallel to Operator Profile Status on Company Profile.",
      similar_hint: { field: "Operator Profile Status", table: "Company Profile" },
    },
    "Last Verified Date": {
      classification: "naming_conflict",
      recommendation: "rename_later",
      reason:
        "Brand/Operator Setup standard is Last Reviewed Date. Prefer aligning to Last Reviewed Date in v1.1.1 (documentation + optional rename later). Does not block enrichment.",
      similar_hint: { field: "Last Reviewed Date", table: "Brand Setup - Brand Basics / Operator Setup - Master" },
      founder_decision: true,
      cleanup_bucket: "C",
    },
    "Rooms / Keys": {
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason: "Legacy Hotel Census uses rooms; Rooms / Keys is clearer for hospitality. Do not invent values until sourced.",
      similar_hint: { field: "rooms", table: "Hotel Census" },
    },
    "State / Region": {
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason: "Distinct from Brand Basics Region Offered (commercial regions). State/Region is geo admin for the property.",
      similar_hint: { field: "Region Offered / Region / Verified State", table: "Brand Basics / Hotel Census / VIC stub" },
    },
    "Market / Submarket": {
      classification: "possible_duplicate_concept",
      recommendation: "add_alias/documentation_only",
      reason:
        "Legacy Hotel Census splits Market + Submarket + Dealality Market. Combined field is OK short-term; long-term prefer Dealality Market + corridor Submarket as separate fields.",
      similar_hint: { field: "Market / Submarket / Dealality Market", table: "Hotel Census" },
      cleanup_bucket: "B",
    },
    "Production Use Status": {
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason: "Must not collide with Brand Status or External Display Status. Census-only rendering gate.",
      similar_hint: { field: "Brand Status / External Display Status / status", table: "Brand Basics / Hotel Census" },
    },
    "Enrichment Status": {
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason: "Distinct from Validation Status / submission_status. Tracks enrichment lane progress.",
    },
    "Company Name": {
      classification: "founder_decision_needed",
      recommendation: "do_not_add",
      reason: "Not on Census; Owner Name / Operator / Management Company are property-level. Company Profile keeps Company Name.",
    },
  };

  if (map[censusField]) {
    const m = map[censusField];
    return {
      census_field: censusField,
      similar_existing_field: m.similar_hint?.field || similar?.field || null,
      existing_table: m.similar_hint?.table || similar?.table || null,
      classification: m.classification,
      recommendation: m.recommendation,
      reason: m.reason,
      founder_decision_needed: Boolean(m.founder_decision),
      cleanup_bucket: m.cleanup_bucket || null,
    };
  }

  if (!similar) {
    return {
      census_field: censusField,
      similar_existing_field: null,
      existing_table: null,
      classification: "census_specific_name_ok",
      recommendation: "keep",
      reason: "No close Brand/Company Setup match; Census-specific field OK.",
      founder_decision_needed: false,
    };
  }

  return {
    census_field: censusField,
    similar_existing_field: similar.field,
    existing_table: similar.table,
    classification: similar.score >= 80 ? "aligned_with_existing_name" : "possible_duplicate_concept",
    recommendation: similar.score >= 80 ? "keep" : "add_alias/documentation_only",
    reason:
      similar.score >= 80
        ? "Closely matches existing naming; keep Census field as-is."
        : "Related concept exists on another table; document difference rather than rename now.",
    founder_decision_needed: false,
  };
}

function classifyAmenity(fieldName) {
  const strategic = new Set([
    "F&B Flag",
    "Meeting Space Flag",
    "Mixed-Use Flag",
    "Branded Residences Flag",
  ]);
  const preferredResort = "Resort Amenities Flag"; // recommend consolidate to Resort / Leisure Flag later
  const preferredExtended = "Extended Stay Amenity Flag"; // recommend Extended Stay Flag naming later
  const tagCandidates = new Set([
    "Fitness Flag",
    "Pool Flag",
    "Parking Flag",
    "Airport Shuttle Flag",
    "Spa Flag",
    "Beach / Waterfront Flag",
  ]);
  const sourceText = new Set(["Amenities - Source Text", "Amenities - Structured Tags"]);

  if (sourceText.has(fieldName)) {
    return {
      amenity_field: fieldName,
      classification: "keep_as_source_text",
      recommendation: "keep",
      reason: "Primary amenity model — source text + structured tags.",
    };
  }
  if (strategic.has(fieldName)) {
    return {
      amenity_field: fieldName,
      classification: "keep_as_strategic_flag",
      recommendation: "keep",
      reason: "High-signal strategic flag useful across BE / OE / Owner lanes.",
    };
  }
  if (fieldName === preferredResort) {
    return {
      amenity_field: fieldName,
      classification: "founder_decision_needed",
      recommendation: "rename_later",
      reason: "Prefer strategic name Resort / Leisure Flag in v1.1.1; keep current flag until then.",
      cleanup_bucket: "B",
    };
  }
  if (fieldName === preferredExtended) {
    return {
      amenity_field: fieldName,
      classification: "founder_decision_needed",
      recommendation: "rename_later",
      reason: "Prefer Extended Stay Flag naming alignment; Amenity suffix is redundant with Structured Tags model.",
      cleanup_bucket: "B",
    };
  }
  if (tagCandidates.has(fieldName)) {
    return {
      amenity_field: fieldName,
      classification: "possible_overmodeling",
      recommendation: "move_to_structured_tags_later",
      reason: "Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated.",
      cleanup_bucket: "B",
    };
  }
  return {
    amenity_field: fieldName,
    classification: "founder_decision_needed",
    recommendation: "keep",
    reason: "Review manually.",
  };
}

function scoreEnrichmentLane(id, scores) {
  const avg =
    (scores.business_value +
      scores.source_availability +
      (5 - scores.risk) +
      scores.usefulness_be +
      scores.usefulness_oe +
      scores.usefulness_owner) /
    6;
  return { id, ...scores, composite: Math.round(avg * 100) / 100 };
}

/**
 * @param {{ beSafety?: object }} [opts]
 */
export async function runPostApplyReview(opts = {}) {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const mvpId = process.env.AIRTABLE_BASE_ID;
  const platformId = bases.target_base_id;

  const report = {
    version: REVIEW_VERSION,
    generated_at: new Date().toISOString(),
    mode: "read_only",
    airtable_writes: false,
    schema_mutations: false,
    brand_explorer_writes: false,
    webhound_used: false,
    token_masked: maskToken(token),
    base_platform_masked: maskId(platformId),
    base_mvp_masked: maskId(mvpId),
    freeze_hash: EXPECTED_FREEZE,
    frozen_artifacts: { vic: hashVicDir(), frozen_62: fingerprintArtifacts(FROZEN_62) },
  };

  const mvpTables = mvpId ? await metaTables(mvpId, token) : [];
  const platformTables = await metaTables(platformId, token);

  const brandCompanyTables = [
    "Brand Setup - Brand Basics",
    "Brand Setup - Brand Explorer Presentation",
    "Company Profile",
    "Hotel Ownership",
    "Operator Setup - Master",
    "Operator Setup - Profile & Positioning",
  ];
  const censusTables = [
    "Hotel Property Census",
    "Hotel Property Brand Affiliations",
    "Hotel Property Source Evidence",
    "Hotel Property Steward Review",
    "Hotel Census",
    "Verified Independent Hotel Census",
  ];

  const existingIndex = [
    ...fieldIndex(mvpTables, brandCompanyTables),
    ...fieldIndex(platformTables, ["Hotel Census", "Verified Independent Hotel Census"]),
  ];

  const naming_conventions_found = {
    brand_setup: {
      brand_name: "Brand Name",
      brand_status: "Brand Status",
      parent_company: "Parent Company",
      validation_status: "Validation Status",
      source_type: "Source Type",
      confidence_level: "Confidence Level",
      company_validated: "Company Validated",
      last_reviewed_date: "Last Reviewed Date",
      external_display_status: "External Display Status",
    },
    company_setup: {
      company_name: "Company Name",
      company_type: "Company Type",
      owner_profile_status: "Owner Profile Status",
      operator_profile_status: "Operator Profile Status",
      developer_profile_status: "Developer Profile Status",
      operator_master_company_name: "company_name",
      data_confidence_level: "Data Confidence Level",
    },
    legacy_hotel_census: {
      name: "name",
      rooms: "rooms",
      management_company: "Management Company",
      owner_company: "Owner Company",
      market: "Market",
      submarket: "Submarket",
      amenities: "Amenities",
    },
  };

  const censusTable = platformTables.find((t) => t.name === "Hotel Property Census");
  const censusFields = (censusTable?.fields || []).map((f) => ({ name: f.name, type: f.type }));
  report.census_field_count = censusFields.length;

  const naming_alignment = censusFields.map((f) => {
    const similar = findSimilar(existingIndex, f.name);
    return { ...classifyNaming(f.name, similar), census_type: f.type };
  });

  const amenityFields = censusFields.filter(
    (f) =>
      /amenit|f&b|meeting space|fitness|pool|parking|shuttle|spa|beach|resort|extended stay|mixed-use|branded residences/i.test(
        f.name
      )
  );
  const amenities_review = amenityFields.map((f) => ({
    ...classifyAmenity(f.name),
    current_type: f.type,
  }));

  // Counts + foundation
  const censusRows = await listAll(platformId, token, TABLE_IDS["Hotel Property Census"], [
    "Property Identity Key",
    "Property Name",
    "Family / Source Family",
    "Current Brand",
    "City",
    "State / Region",
    "Country",
    "Official Property URL",
    "Source URL",
    "Affiliation Status",
    "Data Eligible",
    "Data Confidence Tier",
    "Enrichment Status",
    "Enrichment Priority",
    "Production Use Status",
    "Human Review Required",
    "Steward Review Status",
    "Latitude",
    "Longitude",
    "Owner Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Hotel Description - Source Text",
    "Amenities - Source Text",
    "F&B Flag",
    "Fitness Flag",
    "Pool Flag",
  ]);
  const affRows = await listAll(platformId, token, TABLE_IDS["Hotel Property Brand Affiliations"], [
    "Property Identity Key",
    "VIC Freeze Hash",
  ]);
  const evRows = await listAll(platformId, token, TABLE_IDS["Hotel Property Source Evidence"], [
    "Property Identity Key",
    "VIC Freeze Hash",
  ]);
  const stRows = await listAll(platformId, token, TABLE_IDS["Hotel Property Steward Review"], [
    "Property Identity Key",
    "Steward Review Name",
    "Steward Review Status",
    "Affiliation Status",
    "Hold Reason",
    "Brand-Unconfirmed Flag",
    "VIC Freeze Hash",
    "Manual Decision",
  ]);

  // Steward doesn't have Property Name — join via census
  const censusByKey = new Map(
    censusRows.map((r) => [r.fields?.["Property Identity Key"], r])
  );

  const freezeCensus = censusRows.filter((r) => true); // all should be freeze; verify Production Use
  const keyCounts = new Map();
  for (const r of censusRows) {
    const k = r.fields?.["Property Identity Key"];
    keyCounts.set(k, (keyCounts.get(k) || 0) + 1);
  }
  const duplicateKeys = [...keyCounts.entries()].filter(([, n]) => n > 1);

  const foundation = {
    hotel_property_census: censusRows.length,
    brand_affiliations: affRows.length,
    source_evidence: evRows.length,
    steward_review: stRows.length,
    expected: { census: 666, affiliations: 666, evidence: 666, steward: 4 },
    duplicate_property_identity_keys: duplicateKeys.length,
    duplicate_keys_sample: duplicateKeys.slice(0, 5).map(([k, n]) => ({ key: k, count: n })),
    production_use_status_census_only: censusRows.filter(
      (r) => r.fields?.["Production Use Status"] === PRODUCTION_USE_STATUS
    ).length,
    enrichment_not_started: censusRows.filter((r) => r.fields?.["Enrichment Status"] === "Not Started")
      .length,
    human_review_true: censusRows.filter((r) => r.fields?.["Human Review Required"] === true).length,
    descriptions_filled: censusRows.filter((r) => Boolean(r.fields?.["Hotel Description - Source Text"]))
      .length,
    amenities_source_filled: censusRows.filter((r) => Boolean(r.fields?.["Amenities - Source Text"]))
      .length,
    owner_filled: censusRows.filter((r) => Boolean(r.fields?.["Owner Name"])).length,
    operator_filled: censusRows.filter((r) => Boolean(r.fields?.["Operator / Management Company"]))
      .length,
    rooms_filled: censusRows.filter((r) => r.fields?.["Rooms / Keys"] != null).length,
    opening_filled: censusRows.filter((r) => Boolean(r.fields?.["Opening Date"])).length,
    zero_zero: censusRows.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0).length,
    checks: {},
  };
  foundation.checks = {
    census_666: foundation.hotel_property_census === 666,
    affiliations_666: foundation.brand_affiliations === 666,
    evidence_666: foundation.source_evidence === 666,
    steward_4: foundation.steward_review === 4,
    no_duplicates: foundation.duplicate_property_identity_keys === 0,
    production_use_all: foundation.production_use_status_census_only === 666,
    enrichment_not_started_all: foundation.enrichment_not_started === 666,
    human_review_only_held: foundation.human_review_true === 4,
    enrichment_fields_blank:
      foundation.descriptions_filled === 0 &&
      foundation.amenities_source_filled === 0 &&
      foundation.owner_filled === 0 &&
      foundation.operator_filled === 0 &&
      foundation.rooms_filled === 0 &&
      foundation.opening_filled === 0,
    no_zero_zero: foundation.zero_zero === 0,
  };

  // Sample by family
  function sampleFamily(family) {
    const rows = censusRows.filter((r) => r.fields?.["Family / Source Family"] === family);
    const branded = rows
      .filter((r) => r.fields?.["Affiliation Status"] === "Branded")
      .slice(0, 5);
    const soft = rows
      .filter((r) => r.fields?.["Affiliation Status"] === "Soft-Branded / Collection")
      .slice(0, 3);
    const unconfirmed = rows.filter((r) => r.fields?.["Affiliation Status"] === "Brand-Unconfirmed");
    const held = rows.filter((r) => r.fields?.["Human Review Required"] === true);
    const issues = [];
    for (const r of [...branded, ...soft, ...unconfirmed, ...held]) {
      const f = r.fields || {};
      if (!f["Property Name"]) issues.push({ id: f["Property Identity Key"], issue: "missing_property_name" });
      if (!f.Country) issues.push({ id: f["Property Identity Key"], issue: "missing_country" });
      if (!f["Source URL"] && !f["Official Property URL"]) {
        issues.push({ id: f["Property Identity Key"], issue: "missing_source_url" });
      }
      if (f["Production Use Status"] !== PRODUCTION_USE_STATUS) {
        issues.push({ id: f["Property Identity Key"], issue: "bad_production_use_status" });
      }
      if (f["Enrichment Status"] !== "Not Started") {
        issues.push({ id: f["Property Identity Key"], issue: "unexpected_enrichment_status" });
      }
      const url = f["Official Property URL"] || f["Source URL"];
      if (url && !/^https?:\/\//i.test(String(url))) {
        issues.push({ id: f["Property Identity Key"], issue: "bad_source_url_format" });
      }
    }
    const slim = (arr) =>
      arr.map((r) => ({
        property_name: r.fields?.["Property Name"],
        brand: r.fields?.["Current Brand"],
        city: r.fields?.City,
        state_region: r.fields?.["State / Region"] || null,
        country: r.fields?.Country,
        source_url: r.fields?.["Source URL"] || r.fields?.["Official Property URL"],
        affiliation_status: r.fields?.["Affiliation Status"],
        data_eligible: r.fields?.["Data Eligible"] === true,
        confidence_tier: r.fields?.["Data Confidence Tier"],
        enrichment_priority: r.fields?.["Enrichment Priority"],
        production_use_status: r.fields?.["Production Use Status"],
        human_review_required: r.fields?.["Human Review Required"] === true,
      }));
    return {
      family,
      totals: {
        rows: rows.length,
        branded: rows.filter((r) => r.fields?.["Affiliation Status"] === "Branded").length,
        soft: rows.filter((r) => r.fields?.["Affiliation Status"] === "Soft-Branded / Collection")
          .length,
        unconfirmed: unconfirmed.length,
        held: held.length,
      },
      samples: {
        branded: slim(branded),
        soft_branded: slim(soft),
        brand_unconfirmed: slim(unconfirmed),
        held: slim(held),
      },
      issues,
    };
  }

  const sample_review = {
    IHG: sampleFamily("IHG"),
    Hilton: sampleFamily("Hilton"),
    Choice: sampleFamily("Choice"),
    Marriott: sampleFamily("Marriott"),
  };

  // Held records
  const held_records = stRows.map((st) => {
    const key = st.fields?.["Property Identity Key"];
    const census = censusByKey.get(key);
    const status = st.fields?.["Steward Review Status"];
    let next = "keep held";
    if (status === "steward_manual_review_required") next = "needs brand steward review";
    else if (status === "exclude_from_brand_completion") next = "exclude from Brand Explorer";
    else if (status === "brand_unconfirmed_held") next = "needs source enrichment";
    return {
      property_name: census?.fields?.["Property Name"] || st.fields?.["Steward Review Name"],
      property_identity_key: key,
      current_classification: st.fields?.["Affiliation Status"],
      reason_held: st.fields?.["Hold Reason"] || status,
      steward_review_status: status,
      source_family: census?.fields?.["Family / Source Family"],
      source_url: census?.fields?.["Official Property URL"] || census?.fields?.["Source URL"],
      recommended_next_action: next,
    };
  });

  const enrichment_readiness = [
    scoreEnrichmentLane("A_hotel_descriptions", {
      business_value: 5,
      source_availability: 4,
      risk: 2,
      usefulness_be: 5,
      usefulness_oe: 3,
      usefulness_owner: 4,
    }),
    scoreEnrichmentLane("B_amenities", {
      business_value: 5,
      source_availability: 4,
      risk: 2,
      usefulness_be: 5,
      usefulness_oe: 4,
      usefulness_owner: 4,
    }),
    scoreEnrichmentLane("C_property_type_asset_context", {
      business_value: 5,
      source_availability: 4,
      risk: 1,
      usefulness_be: 4,
      usefulness_oe: 5,
      usefulness_owner: 4,
    }),
    scoreEnrichmentLane("D_independent_hotel_classification", {
      business_value: 4,
      source_availability: 3,
      risk: 3,
      usefulness_be: 2,
      usefulness_oe: 3,
      usefulness_owner: 4,
    }),
    scoreEnrichmentLane("E_owner_developer", {
      business_value: 5,
      source_availability: 2,
      risk: 5,
      usefulness_be: 2,
      usefulness_oe: 3,
      usefulness_owner: 5,
    }),
    scoreEnrichmentLane("F_operator_management", {
      business_value: 5,
      source_availability: 2,
      risk: 5,
      usefulness_be: 2,
      usefulness_oe: 5,
      usefulness_owner: 4,
    }),
    scoreEnrichmentLane("G_rooms_keys", {
      business_value: 4,
      source_availability: 3,
      risk: 4,
      usefulness_be: 3,
      usefulness_oe: 4,
      usefulness_owner: 4,
    }),
    scoreEnrichmentLane("H_opening_renovation_dates", {
      business_value: 3,
      source_availability: 2,
      risk: 4,
      usefulness_be: 3,
      usefulness_oe: 3,
      usefulness_owner: 3,
    }),
  ].sort((a, b) => b.composite - a.composite);

  const recommended_first_lane =
    "Start with descriptions + amenities + property type / asset context. Do not start with owner/operator until sourcing process is defined.";

  const fields_safe_to_enrich_next = [
    "Hotel Description - Source Text",
    "Hotel Description - AI Summary",
    "Short Property Summary",
    "Property Positioning",
    "Amenities - Source Text",
    "Amenities - Structured Tags",
    "F&B Flag",
    "Meeting Space Flag",
    "Resort Amenities Flag",
    "Extended Stay Amenity Flag",
    "Mixed-Use Flag",
    "Branded Residences Flag",
    "Property Type",
    "Asset Context",
    "Hotel Class / Segment",
    "Market / Submarket",
  ];

  const fields_must_remain_blank = [
    "Owner Name",
    "Owner Type",
    "Owner Source URL",
    "Developer Name",
    "Operator / Management Company",
    "Management Model",
    "Rooms / Keys",
    "Opening Date",
    "Renovation / Conversion Date",
    "Latitude/Longitude when unknown (never 0,0)",
  ];

  // v1.1.1 cleanup plan
  const cleanup_plan = [];
  for (const row of naming_alignment) {
    if (row.cleanup_bucket || row.founder_decision_needed || row.recommendation === "rename_later") {
      cleanup_plan.push({
        current_field: row.census_field,
        issue: row.reason,
        proposed_action: row.recommendation,
        risk: row.recommendation === "rename_later" ? "medium" : "low",
        requires_data_migration: row.recommendation === "rename_later",
        founder_decision: Boolean(row.founder_decision_needed),
        bucket: row.cleanup_bucket || (row.founder_decision_needed ? "C" : "B"),
      });
    }
  }
  for (const a of amenities_review) {
    if (a.classification === "possible_overmodeling" || a.recommendation === "move_to_structured_tags_later") {
      cleanup_plan.push({
        current_field: a.amenity_field,
        issue: a.reason,
        proposed_action: "move_to_structured_tags_later",
        risk: "low",
        requires_data_migration: false,
        founder_decision: false,
        bucket: "B",
        note: "Can live with for now; hide from default views before enrichment UI work",
      });
    }
    if (a.recommendation === "rename_later") {
      cleanup_plan.push({
        current_field: a.amenity_field,
        issue: a.reason,
        proposed_action: "rename_later",
        risk: "low",
        requires_data_migration: true,
        founder_decision: true,
        bucket: "C",
      });
    }
  }

  const foundationFail = Object.values(foundation.checks).some((v) => v === false);
  const overmodelCount = amenities_review.filter((a) => a.classification === "possible_overmodeling")
    .length;
  const sampleIssues = Object.values(sample_review).reduce((n, f) => n + (f.issues?.length || 0), 0);

  let status = STATUS.CLEAN;
  if (foundationFail || sampleIssues > 10) status = STATUS.HOLD;
  else if (overmodelCount > 0 || cleanup_plan.some((c) => c.bucket === "A" || c.bucket === "C")) {
    status = STATUS.MINOR_CLEANUP;
  }

  report.status = status;
  report.executive_summary = {
    verdict: status,
    census_master_ok: !foundationFail,
    naming: "Mostly aligned; Last Verified Date vs Last Reviewed Date is the main naming tension",
    amenities: `${overmodelCount} amenity flags recommended for later Structured Tags consolidation`,
    enrichment: recommended_first_lane,
    brand_explorer: "Must remain untouched; re-check via BE gates",
  };
  report.naming_conventions_found = naming_conventions_found;
  report.naming_alignment = naming_alignment;
  report.amenities_review = amenities_review;
  report.foundation = foundation;
  report.sample_review = sample_review;
  report.held_records = held_records;
  report.enrichment_readiness = enrichment_readiness;
  report.recommended_first_enrichment_lane = recommended_first_lane;
  report.fields_safe_to_enrich_next = fields_safe_to_enrich_next;
  report.fields_must_remain_blank_until_sourced = fields_must_remain_blank;
  report.v11_1_cleanup_plan = {
    note: "Do not apply in this task",
    buckets: {
      A_should_fix_before_enrichment: cleanup_plan.filter((c) => c.bucket === "A"),
      B_can_live_with_for_now: cleanup_plan.filter((c) => c.bucket === "B"),
      C_founder_decision_needed: cleanup_plan.filter((c) => c.bucket === "C"),
      D_do_not_change: naming_alignment
        .filter((n) => n.recommendation === "keep")
        .slice(0, 20)
        .map((n) => ({ current_field: n.census_field, proposed_action: "keep" })),
    },
    items: cleanup_plan,
  };
  report.brand_explorer_safety = opts.beSafety || {
    status: "pending_external_gate_run",
    note: "Filled by CLI after BE gate commands",
  };
  report.final_recommendation =
    status === STATUS.HOLD
      ? "Hold enrichment until foundation failures are resolved."
      : status === STATUS.MINOR_CLEANUP
        ? "Proceed to descriptions + amenities + property type enrichment; schedule non-blocking v1.1.1 amenity/naming cleanup (hide tag-level amenity flags; decide Last Verified vs Last Reviewed)."
        : "Census v1.1 is clean enough to begin the first enrichment lane.";

  return report;
}

export function renderPostApplyMarkdown(r) {
  const lines = [
    `# Production Census v1.1 Post-Apply Review`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Mode:** read-only (no schema/record/BE writes)`,
    `**Generated:** ${r.generated_at}`,
    ``,
    `## 1. Executive summary`,
    ``,
    `- Verdict: \`${r.status}\``,
    `- ${r.executive_summary?.naming}`,
    `- Amenities: ${r.executive_summary?.amenities}`,
    `- Enrichment: ${r.recommended_first_enrichment_lane}`,
    `- Final: ${r.final_recommendation}`,
    ``,
    `## 2. Census count validation`,
    ``,
    `| Table | Expected | Actual | Pass |`,
    `| --- | ---: | ---: | --- |`,
    `| Hotel Property Census | 666 | ${r.foundation.hotel_property_census} | ${r.foundation.checks.census_666} |`,
    `| Brand Affiliations | 666 | ${r.foundation.brand_affiliations} | ${r.foundation.checks.affiliations_666} |`,
    `| Source Evidence | 666 | ${r.foundation.source_evidence} | ${r.foundation.checks.evidence_666} |`,
    `| Steward Review | 4 | ${r.foundation.steward_review} | ${r.foundation.checks.steward_4} |`,
    ``,
    `Duplicates: ${r.foundation.duplicate_property_identity_keys} · 0,0 coords: ${r.foundation.zero_zero} · Human Review true: ${r.foundation.human_review_true}`,
    ``,
    `## 3. Existing Brand Setup / Company Setup naming conventions`,
    ``,
    "```json",
    JSON.stringify(r.naming_conventions_found, null, 2),
    "```",
    ``,
    `## 4. Census field naming alignment`,
    ``,
    `| Census Field | Similar Existing Field | Existing Table | Classification | Recommendation | Reason |`,
    `| --- | --- | --- | --- | --- | --- |`,
  ];
  for (const row of r.naming_alignment || []) {
    lines.push(
      `| ${row.census_field} | ${row.similar_existing_field || "—"} | ${row.existing_table || "—"} | ${row.classification} | ${row.recommendation} | ${String(row.reason || "").replace(/\|/g, "/")} |`
    );
  }
  lines.push(
    ``,
    `## 5. Amenities simplification review`,
    ``,
    `| Amenity Field | Current Type | Classification | Recommendation | Reason |`,
    `| --- | --- | --- | --- | --- |`
  );
  for (const a of r.amenities_review || []) {
    lines.push(
      `| ${a.amenity_field} | ${a.current_type} | ${a.classification} | ${a.recommendation} | ${String(a.reason || "").replace(/\|/g, "/")} |`
    );
  }
  lines.push(
    ``,
    `## 6. Master Census foundation review`,
    ``,
    "```json",
    JSON.stringify(r.foundation.checks, null, 2),
    "```",
    ``,
    `## 7. Record sample review by family`,
    ``
  );
  for (const [fam, block] of Object.entries(r.sample_review || {})) {
    lines.push(`### ${fam}`, ``, `- Totals: ${JSON.stringify(block.totals)}`, `- Issues: ${block.issues?.length || 0}`, ``);
  }
  lines.push(
    `## 8. Held record review`,
    ``,
    `| Property | Family | Classification | Reason | Next action |`,
    `| --- | --- | --- | --- | --- |`
  );
  for (const h of r.held_records || []) {
    lines.push(
      `| ${h.property_name} | ${h.source_family} | ${h.current_classification} | ${h.reason_held} | ${h.recommended_next_action} |`
    );
  }
  lines.push(
    ``,
    `## 9. Enrichment readiness scoring`,
    ``,
    `| Lane | Composite | BE | OE | Owner | Risk |`,
    `| --- | ---: | ---: | ---: | ---: | ---: |`
  );
  for (const e of r.enrichment_readiness || []) {
    lines.push(
      `| ${e.id} | ${e.composite} | ${e.usefulness_be} | ${e.usefulness_oe} | ${e.usefulness_owner} | ${e.risk} |`
    );
  }
  lines.push(
    ``,
    `## 10. Recommended first enrichment lane`,
    ``,
    r.recommended_first_enrichment_lane,
    ``,
    `## 11. Fields safe to enrich next`,
    ``,
    ...(r.fields_safe_to_enrich_next || []).map((f) => `- ${f}`),
    ``,
    `## 12. Fields that must remain blank until sourced`,
    ``,
    ...(r.fields_must_remain_blank_until_sourced || []).map((f) => `- ${f}`),
    ``,
    `## 13. Possible v1.1.1 cleanup plan (do not apply)`,
    ``,
    "```json",
    JSON.stringify(r.v11_1_cleanup_plan?.buckets, null, 2),
    "```",
    ``,
    `## 14. Brand Explorer safety result`,
    ``,
    "```json",
    JSON.stringify(r.brand_explorer_safety, null, 2),
    "```",
    ``,
    `## 15. Final recommendation`,
    ``,
    r.final_recommendation,
    ``
  );
  return lines.join("\n");
}
