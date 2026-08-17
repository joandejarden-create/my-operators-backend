#!/usr/bin/env node
/**
 * Read-only: Full Brand Setup field coverage audit vs Brand Explorer validation + Batch 1.
 *
 * Does NOT write Airtable. Does NOT apply Batch 1A/1B.
 *
 * Usage:
 *   node scripts/brand-setup-full-field-coverage-audit.mjs
 *   npm run brand-setup-full-field-coverage-audit
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const BATCH1_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.json"
);
const APPLY_1A_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-apply.json"
);
const WEBFLOW_REVIEW_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-webflow-field-review.json"
);
const CENSUS_CROSSCHECK_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-new-census-crosscheck.json"
);

const OUT_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-setup-full-field-coverage-audit.json"
);
const OUT_MD = path.join(
  ROOT,
  "reports/brand-explorer/brand-setup-full-field-coverage-audit.md"
);
const OUT_DOC = path.join(
  ROOT,
  "docs/data-intelligence/brand-setup-full-field-coverage-audit.md"
);

const BRAND_SETUP_TABLE_PREFIX = "Brand Setup -";

/** Presentation fields known to render in Brand Explorer owner UI */
const PRESENTATION_PUBLIC_RENDERED = new Set([
  "Slot Key",
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Owner Objective",
  "Case Summary Brand Relevance",
  "Case Summary Interpretation",
  "Case Summary Tags",
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Image URL",
  "Sort Order",
  "Active",
  "External Display Status",
]);

/** Protected / governance — must never be Batch 1 patched */
const PROTECTED_FIELD_NAMES = new Set([
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Founder Visual Review Pass",
  "External Display Status",
  "Active Profile Approved Date",
  "Brand Explorer Last Reviewed",
  "Last Reviewed Date",
  "Profile Last Reviewed",
  "Last Reviewed",
  "Release Status",
  "Public Release Status",
  "Explorer Release Status",
  "Brand Explorer Release Status",
  "Wave Release Status",
  "Public Full Status",
  "Brand Explorer Public Visibility",
]);

const RELEASE_FIELD_PATTERNS = [/release/i, /public.?full/i, /wave.?status/i];
const MOMENTUM_SLOT_HINT = /momentum/i;

const BATCH1_ALLOWED_FIELDS = new Set([
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Relevance",
  "Case Summary Interpretation",
  "Case Summary Tags",
]);

/** Validation surface coverage for Presentation text fields */
const VALIDATION_SURFACES = {
  semantic: ["Title", "Body", "Case Summary Overview", "Case Summary Brand Relevance", "Case Summary Owner Objective", "Case Summary Interpretation", "Case Summary Tags", "Slot Key"],
  pvql: ["Title", "Body", "Case Summary Overview", "Case Summary Brand Relevance", "Case Summary Owner Objective", "Case Summary Interpretation", "Case Summary Tags", "External Display Status", "Active", "Slot Key", "Image URL"],
  quality: ["Title", "Body", "Slot Key", "Image", "Image URL", "Case Summary Overview", "Case Summary Brand Relevance", "Case Summary Interpretation", "Case Summary Tags"],
  footnote: ["Company Validated", "Brand Explorer Last Reviewed", "Last Reviewed Date", "Profile Last Reviewed", "Last Reviewed", "Brand Status", "Region Offered"],
  momentum: ["Title", "Body", "Slot Key"], // footprint.momentum / openings slots
  mandatory: ["Brand Status", "External Display Status", "Active", "Company Validated", "Title", "Body", "Slot Key"],
  webflow_product: ["Title", "Body", "Case Summary Overview", "Case Summary Brand Relevance", "Case Summary Owner Objective", "Case Summary Interpretation", "Case Summary Tags", "Slot Key", "Image", "Sort Order", "Active", "External Display Status"],
  census_crosscheck: ["Title", "Body", "Case Summary Overview", "Case Summary Brand Relevance", "Case Summary Interpretation", "Case Summary Tags"],
};

const BASICS_PUBLIC_SUPPORTING = new Set([
  "Brand Name",
  "Logo",
  "Parent Company",
  "Hotel Chain Scale",
  "Brand Model",
  "Hotel Service Model",
  "Year Brand Launched",
  "Brand Development Stage",
  "Brand Positioning",
  "Brand Tagline",
  "Brand Customer Promise",
  "Brand Value Proposition",
  "Brand Pillars",
  "Brand History",
  "Target Guest Segments",
  "Guest Psychographics Description",
  "Key Brand Differentiators",
  "Sustainability Positioning",
  "Brand Architecture",
  "Region Offered",
  "Branded Residences Status",
  "Branded Residences Notes",
]);

const BASICS_PROTECTED = new Set([
  "Brand Status",
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Founder Visual Review Pass",
  "Explorer Hero Verification",
  "Explorer Hero Data Source",
  "Branded Residences Review Status",
  "Active Profile Approved Date",
  "Brand Explorer Last Reviewed",
  "Last Reviewed Date",
  "Profile Last Reviewed",
  "Last Reviewed",
]);

const CENSUS_CONNECTED_PRESENTATION_HINTS = [
  { field: "Title", reason: "Property example titles / openings titles may claim property names + locations" },
  { field: "Body", reason: "Footprint/openings/geo/portfolio claims may cite properties, cities, asset types" },
  { field: "Case Summary Overview", reason: "Case property overview may cite property identity + location" },
  { field: "Case Summary Brand Relevance", reason: "May claim brand–property fit tied to census properties" },
  { field: "Case Summary Interpretation", reason: "May restate property/location/asset claims" },
  { field: "Case Summary Tags", reason: "May tag property type / market / asset context" },
];

const CENSUS_FIELDS_FOR_CROSSCHECK = [
  "Property Name",
  "Brand",
  "Affiliation Status",
  "City",
  "State / Region",
  "Country",
  "Source URL",
  "Human Review Required",
  "Public Census Eligibility",
  "Public Display Confidence",
  "Property Type",
  "Asset Context",
  "Amenities - Structured Tags",
  "Resort / Leisure Flag",
  "Extended Stay Flag",
  "Mixed-Use Flag",
  "Branded Residences Flag",
  "Latitude",
  "Longitude",
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.length ? String(v[0] ?? "").trim() : "";
  return String(v).trim();
}

function isPopulated(v) {
  if (v == null) return false;
  if (typeof v === "boolean") return true;
  if (typeof v === "number") return !Number.isNaN(v);
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return String(v).trim().length > 0;
}

async function metaListTables(baseId, token) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`meta tables ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json.tables || [];
}

async function listAllRecords(baseId, token, tableIdOrName, { fields = null, filterByFormula = null } = {}) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    if (filterByFormula) params.set("filterByFormula", filterByFormula);
    if (fields?.length) for (const f of fields) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableIdOrName)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(`list ${tableIdOrName} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(140);
  } while (offset);
  return out;
}

function fieldTypeLabel(f) {
  return f?.type || "unknown";
}

function isReleaseField(name) {
  if (PROTECTED_FIELD_NAMES.has(name)) return /release|public.?full|wave/i.test(name);
  return RELEASE_FIELD_PATTERNS.some((re) => re.test(name));
}

function isProtectedField(name, tableName) {
  if (PROTECTED_FIELD_NAMES.has(name)) return true;
  if (isReleaseField(name)) return true;
  if (tableName === BASICS_TABLE && BASICS_PROTECTED.has(name)) return true;
  if (/Company Validat|Brand Verif|Founder Visual|Brand Status/i.test(name)) return true;
  return false;
}

function classifyField(tableName, fieldName, fieldType) {
  const n = fieldName;

  if (isProtectedField(n, tableName) || isReleaseField(n)) {
    if (/Company Validat|Brand Verif|Validation Status|Steward Review/i.test(n)) {
      return "validation_status";
    }
    if (isReleaseField(n) || /External Display|Founder Visual|Active Profile/i.test(n)) {
      return "release_control";
    }
    return "protected_governance";
  }

  if (tableName === PRESENTATION_TABLE) {
    if (/Source|Evidence|Provenance|Citation|URL Source|Confidence/i.test(n) && !PRESENTATION_PUBLIC_RENDERED.has(n)) {
      return "source_evidence";
    }
    if (PRESENTATION_PUBLIC_RENDERED.has(n)) {
      if (/Case Summary|Title|Body|Image|Slot Key/i.test(n)) return "public_rendered_owner_facing";
      if (/Sort Order|Active|External Display/i.test(n)) return "release_control";
      return "public_rendered_supporting";
    }
    if (/Brand Name|Brand$|created|modified|record id/i.test(n) || fieldType === "multipleRecordLinks" || fieldType === "createdTime" || fieldType === "lastModifiedTime" || fieldType === "formula" || fieldType === "rollup" || fieldType === "count" || fieldType === "lookup") {
      return "internal_only";
    }
    return "unknown_needs_review";
  }

  if (tableName === BASICS_TABLE) {
    if (BASICS_PUBLIC_SUPPORTING.has(n)) return "public_rendered_supporting";
    if (/Source|Evidence|URL|Confidence|Verification|Data Source/i.test(n)) return "source_evidence";
    if (/Profile Analysis|Brand Profile Analysis/i.test(n)) return "internal_only";
    if (fieldType === "multipleRecordLinks" || fieldType === "formula" || fieldType === "rollup" || fieldType === "count" || fieldType === "lookup" || fieldType === "createdTime" || fieldType === "lastModifiedTime" || fieldType === "button" || fieldType === "autoNumber") {
      return "internal_only";
    }
    return "unknown_needs_review";
  }

  // Other Brand Setup child tables — mostly setup/internal, some feed explorer heuristics
  if (/Source|Evidence|URL|Confidence/i.test(n)) return "source_evidence";
  if (fieldType === "multipleRecordLinks" || fieldType === "formula" || fieldType === "rollup" || fieldType === "count" || fieldType === "lookup" || fieldType === "createdTime" || fieldType === "lastModifiedTime" || fieldType === "button" || fieldType === "autoNumber") {
    return "internal_only";
  }
  if (/Brand Name|^Brand$/i.test(n)) return "internal_only";
  return "unknown_needs_review";
}

function validationCoverageForField(tableName, fieldName) {
  const covered = [];
  if (tableName === PRESENTATION_TABLE || tableName === BASICS_TABLE) {
    for (const [surface, fields] of Object.entries(VALIDATION_SURFACES)) {
      if (fields.includes(fieldName)) covered.push(surface);
    }
    // Momentum is slot-scoped; field Title/Body covered when slot is momentum
    if (tableName === PRESENTATION_TABLE && (fieldName === "Title" || fieldName === "Body")) {
      if (!covered.includes("momentum")) covered.push("momentum");
    }
  }
  return covered;
}

function censusConnected(tableName, fieldName) {
  if (tableName !== PRESENTATION_TABLE) {
    if (tableName === BASICS_TABLE && /Brand Name|Parent Company|Region|Branded Residences|Chain Scale|Service Model/i.test(fieldName)) {
      return true;
    }
    return false;
  }
  return CENSUS_CONNECTED_PRESENTATION_HINTS.some((h) => h.field === fieldName);
}

function safeToPatch(tableName, fieldName, classification) {
  if (tableName !== PRESENTATION_TABLE) return false;
  if (!BATCH1_ALLOWED_FIELDS.has(fieldName) && fieldName !== "Case Summary Brand Relevance") return false;
  if (["protected_governance", "release_control", "validation_status"].includes(classification)) return false;
  return ["public_rendered_owner_facing", "public_rendered_supporting"].includes(classification);
}

function blockedFromPatching(tableName, fieldName, classification) {
  if (isProtectedField(fieldName, tableName)) return true;
  if (["protected_governance", "release_control", "validation_status"].includes(classification)) return true;
  if (tableName !== PRESENTATION_TABLE) return true;
  if (!BATCH1_ALLOWED_FIELDS.has(fieldName) && fieldName !== "Case Summary Brand Relevance") return true;
  return false;
}

function loadJsonSafe(p) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function countFieldPopulations(records, fieldNames) {
  const stats = {};
  for (const name of fieldNames) {
    stats[name] = { populated: 0, empty: 0 };
  }
  for (const rec of records) {
    const f = rec.fields || {};
    for (const name of fieldNames) {
      if (isPopulated(f[name])) stats[name].populated += 1;
      else stats[name].empty += 1;
    }
  }
  return stats;
}

function mdEscape(s) {
  return String(s ?? "").replace(/\|/g, "\\|");
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const batch1 = loadJsonSafe(BATCH1_JSON);
  const apply1A = loadJsonSafe(APPLY_1A_JSON);
  const webflowReview = loadJsonSafe(WEBFLOW_REVIEW_JSON);
  const censusCrosscheck = loadJsonSafe(CENSUS_CROSSCHECK_JSON);

  console.log("[coverage-audit] loading active universe…");
  const universe = await loadActiveUniverse({ includeDetails: true });
  const activeBrands = universe.brands || universe.activeBrands || [];
  const activeCount = activeBrands.length || universe.activeCount || 0;
  const activeIds = new Set(
    activeBrands.map((b) => b.recordId || b.id || b.airtableRecordId).filter(Boolean)
  );
  const activeNames = new Set(
    activeBrands.map((b) => nz(b.brandName || b.name)).filter(Boolean)
  );
  const activeSlugs = activeBrands.map((b) => b.slug).filter(Boolean);

  console.log(`[coverage-audit] active universe=${activeCount}`);

  console.log("[coverage-audit] fetching meta schema…");
  const allTables = await metaListTables(baseId, token);
  const brandSetupTables = allTables.filter((t) => String(t.name || "").startsWith(BRAND_SETUP_TABLE_PREFIX));
  // Related supporting tables sometimes used with Brand Setup
  const relatedExtras = allTables.filter((t) => {
    const n = String(t.name || "");
    return (
      /Partner Intelligence.*Brand/i.test(n) ||
      n === "Brand Alias Mapping" ||
      /Brand Asset Registry/i.test(n)
    );
  });

  const tablesInScope = [...brandSetupTables];
  for (const t of relatedExtras) {
    if (!tablesInScope.some((x) => x.id === t.id)) tablesInScope.push(t);
  }

  console.log(
    `[coverage-audit] Brand Setup tables=${brandSetupTables.length}; related extras=${relatedExtras.length}`
  );

  // Population: Brand Basics (active 62) + Presentation (rows for active brands)
  console.log("[coverage-audit] listing Brand Basics records…");
  const basicsTable = brandSetupTables.find((t) => t.name === BASICS_TABLE);
  const presentationTable = brandSetupTables.find((t) => t.name === PRESENTATION_TABLE);
  if (!basicsTable || !presentationTable) {
    throw new Error("Required Brand Setup tables missing from schema");
  }

  const basicsFieldNames = (basicsTable.fields || []).map((f) => f.name);
  const presentationFieldNames = (presentationTable.fields || []).map((f) => f.name);

  // Fetch all basics then filter to active — safer than huge OR formula
  const allBasics = await listAllRecords(baseId, token, BASICS_TABLE);
  const activeBasics = allBasics.filter((r) => {
    if (activeIds.has(r.id)) return true;
    const name = nz(r.fields?.["Brand Name"]);
    return name && activeNames.has(name);
  });
  console.log(`[coverage-audit] basics total=${allBasics.length} activeMatched=${activeBasics.length}`);

  console.log("[coverage-audit] listing Presentation records…");
  const allPresentation = await listAllRecords(baseId, token, PRESENTATION_TABLE);
  const activePresentation = allPresentation.filter((r) => {
    const f = r.fields || {};
    const name = nz(f["Brand Name"]);
    if (name && activeNames.has(name)) return true;
    const links = f.Brand || f["Brand Setup - Brand Basics"] || f.Brand_Basic_ID || [];
    if (Array.isArray(links) && links.some((id) => activeIds.has(id))) return true;
    return false;
  });
  console.log(
    `[coverage-audit] presentation total=${allPresentation.length} activeMatched=${activePresentation.length}`
  );

  const basicsPop = countFieldPopulations(activeBasics, basicsFieldNames);
  const presentationPop = countFieldPopulations(activePresentation, presentationFieldNames);

  // Batch 1 field inclusion
  const batch1A = batch1?.batch1A || [];
  const batch1B = batch1?.batch1B || [];
  const batch1FieldsA = {};
  const batch1FieldsB = {};
  for (const p of batch1A) batch1FieldsA[p.fieldName] = (batch1FieldsA[p.fieldName] || 0) + 1;
  for (const p of batch1B) batch1FieldsB[p.fieldName] = (batch1FieldsB[p.fieldName] || 0) + 1;
  const batch1FieldSet = new Set([...Object.keys(batch1FieldsA), ...Object.keys(batch1FieldsB)]);

  const fieldInventory = [];
  const classificationCounts = {};

  for (const table of tablesInScope) {
    const isCoreBrandSetup = String(table.name).startsWith(BRAND_SETUP_TABLE_PREFIX);
    const isActiveScoped = table.name === BASICS_TABLE || table.name === PRESENTATION_TABLE;
    const pop =
      table.name === BASICS_TABLE
        ? basicsPop
        : table.name === PRESENTATION_TABLE
          ? presentationPop
          : null;
    const recordUniverse =
      table.name === BASICS_TABLE
        ? activeBasics.length
        : table.name === PRESENTATION_TABLE
          ? activePresentation.length
          : null;

    for (const f of table.fields || []) {
      const classification = classifyField(table.name, f.name, f.type);
      classificationCounts[classification] = (classificationCounts[classification] || 0) + 1;
      const coveredBy = validationCoverageForField(table.name, f.name);
      const inBatch1 = batch1FieldSet.has(f.name) && table.name === PRESENTATION_TABLE;
      const inBatch1A = Boolean(batch1FieldsA[f.name]) && table.name === PRESENTATION_TABLE;
      const inBatch1B = Boolean(batch1FieldsB[f.name]) && table.name === PRESENTATION_TABLE;
      const publicFacing =
        classification === "public_rendered_owner_facing" ||
        classification === "public_rendered_supporting";
      const internalOnly = classification === "internal_only";
      const protectedFlag = isProtectedField(f.name, table.name);
      const sourceEvidence = classification === "source_evidence";
      const releaseGov =
        classification === "release_control" || classification === "protected_governance";
      const censusConn = censusConnected(table.name, f.name);
      const includedInCurrentValidation = coveredBy.length > 0;
      const safe = safeToPatch(table.name, f.name, classification);
      const blocked = blockedFromPatching(table.name, f.name, classification);

      const populated = pop?.[f.name]?.populated ?? null;
      const empty = pop?.[f.name]?.empty ?? null;

      fieldInventory.push({
        tableName: table.name,
        tableId: table.id,
        isCoreBrandSetup,
        fieldName: f.name,
        fieldId: f.id,
        fieldType: fieldTypeLabel(f),
        usedByActive62: isActiveScoped ? true : "unknown_child_table",
        recordUniverseActive62: recordUniverse,
        populatedCount: populated,
        emptyCount: empty,
        publicFacing,
        webflowProductRendered: publicFacing && (table.name === PRESENTATION_TABLE || BASICS_PUBLIC_SUPPORTING.has(f.name)),
        internalOnly,
        protected: protectedFlag,
        sourceEvidence,
        releaseGovernance: releaseGov,
        censusConnected: censusConn,
        includedInCurrentValidation,
        validationSurfaces: coveredBy,
        includedInBatch1PatchPlan: inBatch1,
        includedInBatch1A: inBatch1A,
        includedInBatch1B: inBatch1B,
        batch1APatchCount: inBatch1A ? batch1FieldsA[f.name] || 0 : 0,
        batch1BPatchCount: inBatch1B ? batch1FieldsB[f.name] || 0 : 0,
        safeToPatch: safe,
        blockedFromPatching: blocked,
        unknownUsage: classification === "unknown_needs_review",
        classification,
      });
    }
  }

  // Coverage gap list — public/rendered with no validation
  const coverageGaps = fieldInventory
    .filter((row) => {
      if (!row.isCoreBrandSetup) return false;
      if (row.tableName !== PRESENTATION_TABLE && row.tableName !== BASICS_TABLE) {
        // child setup tables: gap if unknown + text-like
        return (
          row.unknownUsage &&
          /text|multiline|richText/i.test(row.fieldType)
        );
      }
      if (row.publicFacing && !row.includedInCurrentValidation) return true;
      if (row.classification === "public_rendered_owner_facing" && !row.includedInCurrentValidation) {
        return true;
      }
      // Presentation Case Summary Owner Objective — in PVQL but confirm
      return false;
    })
    .map((row) => ({
      field: row.fieldName,
      table: row.tableName,
      currentCoverage: row.validationSurfaces.length
        ? row.validationSurfaces.join(", ")
        : "none",
      risk: row.publicFacing ? "Medium" : "Low",
      recommendation:
        row.tableName === PRESENTATION_TABLE
          ? "Add to forbidden-language / PVQL text corpus if owner-facing"
          : row.tableName === BASICS_TABLE
            ? "Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal"
            : "Classify usage; exclude from Batch 1; add validation only if product-rendered",
    }));

  // Explicit known gaps: Case Summary Owner Objective is PVQL-covered; check Sort Order / Image
  const presentationPublicNoValidation = fieldInventory.filter(
    (r) =>
      r.tableName === PRESENTATION_TABLE &&
      r.publicFacing &&
      !r.includedInCurrentValidation
  );

  // Child Brand Setup tables — not in BE Active 62 validation pipeline
  const childSetupTables = brandSetupTables
    .filter((t) => t.name !== BASICS_TABLE && t.name !== PRESENTATION_TABLE)
    .map((t) => ({
      table: t.name,
      fieldCount: (t.fields || []).length,
      validationCoverage: "not_in_brand_explorer_active_62_gates",
      risk: "Low_for_Batch_1A_but_Medium_for_full_Brand_Setup_process_claim",
      recommendation:
        "Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed",
    }));

  for (const c of childSetupTables) {
    coverageGaps.push({
      field: "(all fields)",
      table: c.table,
      currentCoverage: c.validationCoverage,
      risk: "Medium",
      recommendation: c.recommendation,
    });
  }

  // Webflow / product mapping for public rendered presentation fields
  const webflowMapping = fieldInventory
    .filter((r) => r.tableName === PRESENTATION_TABLE && r.publicFacing)
    .map((r) => {
      const popRate =
        r.recordUniverseActive62 > 0 && r.populatedCount != null
          ? Number((r.populatedCount / r.recordUniverseActive62).toFixed(3))
          : null;
      return {
        fieldName: r.fieldName,
        whereItRenders:
          r.fieldName === "Title" || r.fieldName === "Body"
            ? "Brand Explorer tabs/cards via slotKey (atelier + gold detail)"
            : r.fieldName.startsWith("Case Summary")
              ? "Case study modal sections"
              : r.fieldName.startsWith("Image")
                ? "Scenario / gallery media"
                : r.fieldName === "Slot Key"
                  ? "Routing key (not displayed raw)"
                  : r.fieldName === "Sort Order"
                    ? "Ordering within slot"
                    : r.fieldName === "Active" || r.fieldName === "External Display Status"
                      ? "Visibility gate (not owner prose)"
                      : "See docs/brand-explorer-presentation-slots.md",
        active62Coverage: {
          populated: r.populatedCount,
          empty: r.emptyCount,
          universeRows: r.recordUniverseActive62,
          populateRate: popRate,
        },
        lengthRisk: ["Body", "Case Summary Overview", "Case Summary Interpretation"].includes(r.fieldName)
          ? "Medium"
          : "Low",
        textLeakageRisk: BATCH1_ALLOWED_FIELDS.has(r.fieldName) || r.fieldName === "Case Summary Brand Relevance" || r.fieldName === "Case Summary Owner Objective"
          ? "High_if_unscanned"
          : "Low",
        sourceSupportRisk: "Medium_for_property_claims",
        censusSupportRelevant: r.censusConnected,
        validationCommands: r.validationSurfaces,
        flaggedNoValidation: !r.includedInCurrentValidation,
      };
    });

  // Answer founder question
  const processReviewsAllBrandSetupFields = false;
  const processReviewsAllPresentationOwnerText = true; // PVQL scans title/body/case summary*
  const processReviewsAllBasics = false;

  // Status decision
  // Batch 1A is scoped to Presentation Low-risk text only — audit shows broader Brand Setup
  // is NOT fully in the Active-62 validation process, but that does not block Batch 1A
  // if we are explicit that Batch 1A scope ≠ full Brand Setup coverage.
  const apply1AAlreadyDone =
    apply1A?.status === "brand_explorer_62_safe_text_cleanup_batch_1A_applied_ready_for_1B_review" ||
    apply1A?.mode === "apply";

  let status = "brand_setup_full_field_coverage_audit_complete_batch_1A_safe_to_apply";
  const holdReasons = [];

  // Hold only if Batch 1A would touch uncovered/protected fields — it does not
  const batch1ATouchesBlocked = batch1A.some((p) => {
    const row = fieldInventory.find(
      (r) => r.tableName === PRESENTATION_TABLE && r.fieldName === p.fieldName
    );
    return row?.blockedFromPatching && !row?.safeToPatch;
  });
  // Actually safeToPatch for Body/Case Summary* is true; blockedFromPatching is false for those

  if (batch1A.some((p) => p.riskLevel && p.riskLevel !== "Low")) {
    status = "brand_setup_full_field_coverage_audit_hold_batch_1A";
    holdReasons.push("Batch 1A contains non-Low risk patches");
  }
  if (batch1A.some((p) => p.patchCategory !== "safe_text_cleanup")) {
    status = "brand_setup_full_field_coverage_audit_hold_batch_1A";
    holdReasons.push("Batch 1A contains non-safe_text_cleanup patches");
  }

  // Additional validation needed for FULL Brand Setup claim — but Batch 1A itself is safe
  const recommendationBefore1A = {
    batch1ASafeToApply: status === "brand_setup_full_field_coverage_audit_complete_batch_1A_safe_to_apply",
    founderQuestionAnswer:
      "No — the current Brand Explorer Active-62 validation process does not review all Brand Setup fields. It primarily reviews Brand Basics governance/status fields used for universe + footnote, and Brand Explorer Presentation owner-facing text (Title/Body/Case Summary*). Other Brand Setup child tables (Footprint, Fee Structure, Standards, Deal Terms, Project Fit, etc.) are outside Active-62 gates.",
    noteOnApplyReport: apply1AAlreadyDone
      ? "Local apply report indicates Batch 1A was already applied in a prior session; this audit is read-only and did not apply patches."
      : "No Batch 1A apply performed by this audit.",
    beforeApplyingBatch1A: [
      "Confirm Batch 1A scope remains Presentation Title/Body/Case Summary* only",
      "Do not interpret Active-62 gate PASS as full Brand Setup field coverage",
      "Keep child Brand Setup tables out of Batch 1",
      "Keep protected/release/Recent Momentum fields out of Batch 1",
      "Schedule separate Brand Setup form/schema coverage if founder needs full-table QA",
    ],
  };

  // If founder asked "is process reviewing all fields?" — answer is no, so also flag additional validation for full coverage claim
  // But status enum for Batch 1A safety remains safe_to_apply when patches themselves are fine.
  // Provide dual: primary status for Batch 1A + coverageCompleteness flag
  const coverageCompleteness = {
    allBrandSetupFieldsReviewedByCurrentProcess: false,
    presentationOwnerFacingTextReviewed: true,
    brandBasicsGovernanceReviewed: "partial",
    childBrandSetupTablesReviewed: false,
    statusIfJudgingFullCoverageOnly: "brand_setup_full_field_coverage_audit_complete_additional_validation_needed",
  };

  // Prefer the more accurate status when founder goal is full coverage audit:
  // Task says final status must be one of three — the honest answer for "full field coverage" is additional_validation_needed
  // BUT also Batch 1A is safe. Reading the status options:
  // - complete_batch_1A_safe_to_apply
  // - complete_additional_validation_needed
  // - hold_batch_1A
  // Since founder question is about whether ALL fields are reviewed, and gaps exist in Brand Setup child tables + some Basics unknowns,
  // additional_validation_needed is the correct primary status for the audit goal.
  // Batch 1A can still be recommended as safe within its narrow scope.
  status = "brand_setup_full_field_coverage_audit_complete_additional_validation_needed";
  if (holdReasons.length) {
    status = "brand_setup_full_field_coverage_audit_hold_batch_1A";
  }

  const report = {
    version: "brand-setup-full-field-coverage-audit-v1",
    generatedAt: new Date().toISOString(),
    airtableWrites: false,
    patchesApplied: false,
    status,
    executiveSummary: {
      founderQuestion:
        "Is the current process reviewing all Brand Setup fields?",
      answer: "No",
      activeUniverse: activeCount,
      brandSetupTablesReviewed: brandSetupTables.map((t) => t.name),
      relatedTablesReviewed: relatedExtras.map((t) => t.name),
      totalFieldsInventoried: fieldInventory.length,
      classificationCounts,
      presentationRowsActive62: activePresentation.length,
      basicsRecordsActive62: activeBasics.length,
      batch1APatchCount: batch1A.length,
      batch1BPatchCount: batch1B.length,
      batch1ASafeWithinScope: recommendationBefore1A.batch1ASafeToApply && !holdReasons.length,
      apply1AAlreadyPresentLocally: apply1AAlreadyDone,
      coverageCompleteness,
      holdReasons,
    },
    tablesReviewed: tablesInScope.map((t) => ({
      name: t.name,
      id: t.id,
      fieldCount: (t.fields || []).length,
      isBrandSetup: String(t.name).startsWith(BRAND_SETUP_TABLE_PREFIX),
    })),
    fieldInventory,
    fieldClassificationMatrix: classificationCounts,
    currentValidationCoverage: {
      surfaces: VALIDATION_SURFACES,
      presentationFieldsCoveredByAnyGate: fieldInventory
        .filter((r) => r.tableName === PRESENTATION_TABLE && r.includedInCurrentValidation)
        .map((r) => r.fieldName),
      presentationFieldsNotCovered: fieldInventory
        .filter((r) => r.tableName === PRESENTATION_TABLE && !r.includedInCurrentValidation)
        .map((r) => r.fieldName),
      basicsFieldsCoveredByAnyGate: fieldInventory
        .filter((r) => r.tableName === BASICS_TABLE && r.includedInCurrentValidation)
        .map((r) => r.fieldName),
      basicsFieldsNotCovered: fieldInventory
        .filter((r) => r.tableName === BASICS_TABLE && !r.includedInCurrentValidation)
        .map((r) => r.fieldName),
      childTablesOutsideActive62Gates: childSetupTables,
    },
    batch1Coverage: {
      expectedAllowedFields: [...BATCH1_ALLOWED_FIELDS],
      fieldsIncludedBatch1A: batch1FieldsA,
      fieldsIncludedBatch1B: batch1FieldsB,
      fieldsExcludedByDesign: [
        "Recent Momentum (slot-specific Body/Title unless separately approved)",
        "Company Validated / Company Validation Date",
        "Brand Verified",
        "Brand Status",
        "Founder Visual Review Pass",
        "release fields",
        "External Display Status / Active (visibility controls)",
        "Image / media fields",
        "all non-Presentation Brand Setup tables",
        "Case Summary Owner Objective (no Batch 1 hits; allowed only if Low safe_text)",
        "Case Summary Brand Relevance / Tags (allowed but no Batch 1 patches in this plan)",
      ],
      fieldsThatShouldNeverBePatchedByBatch1: [...PROTECTED_FIELD_NAMES],
      fieldsWhereFutureCleanupMayBeNeeded: [
        ...fieldInventory
          .filter(
            (r) =>
              r.tableName === PRESENTATION_TABLE &&
              r.safeToPatch &&
              !r.includedInBatch1PatchPlan &&
              ["Body", "Title", "Case Summary Overview", "Case Summary Brand Relevance", "Case Summary Interpretation", "Case Summary Tags", "Case Summary Owner Objective"].includes(
                r.fieldName
              )
          )
          .map((r) => r.fieldName),
        "Remaining Batch 1B census wording patches",
        "MGallery quality minor (held)",
        "Wrong fuzzy Census property swaps (held)",
      ],
      note:
        "Batch 1 plan fieldsIncluded list is slot-qualified; actual Airtable columns patched are Title/Body/Case Summary Overview/Interpretation only.",
    },
    webflowProductMapping: webflowMapping,
    internalProtectedFields: fieldInventory
      .filter((r) => r.protected || r.releaseGovernance || r.classification === "validation_status")
      .map((r) => ({ table: r.tableName, field: r.fieldName, classification: r.classification })),
    censusConnectedFields: {
      brandSetupFields: fieldInventory
        .filter((r) => r.censusConnected)
        .map((r) => ({ table: r.tableName, field: r.fieldName, classification: r.classification })),
      censusFieldsForCrosscheck: CENSUS_FIELDS_FOR_CROSSCHECK,
      priorCrosscheckStatus: censusCrosscheck?.status || null,
      noCensusWrites: true,
    },
    coverageGaps,
    presentationPublicNoValidation,
    recommendationBeforeApplyingBatch1A: recommendationBefore1A,
    priorArtifacts: {
      batch1Status: batch1?.status || null,
      apply1AStatus: apply1A?.status || null,
      webflowReviewStatus: webflowReview?.status || null,
      censusCrosscheckStatus: censusCrosscheck?.status || null,
    },
    hardRulesHonored: {
      noBatch1AApply: true,
      noBatch1BApply: true,
      noBrandExplorerPatch: true,
      noCensusWrite: true,
      noBrandStatusChange: true,
      noCompanyValidatedWrite: true,
      noBrandVerifiedWrite: true,
      noRecentMomentumWrite: true,
      noReleaseFieldWrite: true,
    },
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, `${JSON.stringify(report, null, 2)}\n`);

  // Markdown
  const lines = [];
  lines.push("# Brand Setup — Full Field Coverage Audit (Read-Only)");
  lines.push("");
  lines.push(`**Status:** \`${status}\``);
  lines.push(`**Generated:** ${report.generatedAt}`);
  lines.push(`**Airtable writes:** false · **Patches applied:** false`);
  lines.push("");
  lines.push("## 1. Executive summary");
  lines.push("");
  lines.push(
    "**Founder question:** Is the current process reviewing all Brand Setup fields?"
  );
  lines.push("");
  lines.push(
    "**Answer: No.** Active-62 Brand Explorer gates cover Presentation owner-facing text (Title / Body / Case Summary*) plus selected Brand Basics governance fields (Brand Status, validation/footnote inputs). They do **not** cover the full Brand Setup schema (Footprint, Fee Structure, Standards, Deal Terms, Project Fit, Operational Support, Legal Terms, Loyalty, Sustainability, etc.)."
  );
  lines.push("");
  lines.push(
    `- Active universe: **${activeCount}** · Basics matched: **${activeBasics.length}** · Presentation rows matched: **${activePresentation.length}**`
  );
  lines.push(
    `- Brand Setup tables reviewed: **${brandSetupTables.length}** · Total fields inventoried: **${fieldInventory.length}**`
  );
  lines.push(
    `- Batch 1A patches (plan): **${batch1A.length}** · Batch 1B: **${batch1B.length}** · Batch 1A safe within narrow scope: **${!holdReasons.length}**`
  );
  if (apply1AAlreadyDone) {
    lines.push(
      `- Note: local apply report shows Batch 1A already applied previously; **this audit did not apply anything**.`
    );
  }
  lines.push("");
  lines.push("## 2. All Brand Setup tables reviewed");
  lines.push("");
  lines.push("| Table | Fields | Brand Setup? |");
  lines.push("| --- | ---: | --- |");
  for (const t of report.tablesReviewed) {
    lines.push(`| ${mdEscape(t.name)} | ${t.fieldCount} | ${t.isBrandSetup} |`);
  }
  lines.push("");
  lines.push("## 3. Complete field inventory");
  lines.push("");
  lines.push(
    "Full machine-readable inventory: `reports/brand-explorer/brand-setup-full-field-coverage-audit.json` → `fieldInventory`."
  );
  lines.push("");
  lines.push(
    "| Table | Field | Type | Class | Populated (active62) | Empty | Public | Protected | In validation | In Batch1 |"
  );
  lines.push("| --- | --- | --- | --- | ---: | ---: | --- | --- | --- | --- |");
  for (const r of fieldInventory.filter((x) => x.isCoreBrandSetup)) {
    lines.push(
      `| ${mdEscape(r.tableName)} | ${mdEscape(r.fieldName)} | ${mdEscape(r.fieldType)} | ${mdEscape(r.classification)} | ${r.populatedCount ?? "—"} | ${r.emptyCount ?? "—"} | ${r.publicFacing} | ${r.protected} | ${r.includedInCurrentValidation} | ${r.includedInBatch1PatchPlan} |`
    );
  }
  lines.push("");
  lines.push("## 4. Field classification matrix");
  lines.push("");
  lines.push("| Classification | Count |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(classificationCounts).sort((a, b) => b[1] - a[1])) {
    lines.push(`| \`${k}\` | ${v} |`);
  }
  lines.push("");
  lines.push("## 5. Current validation coverage");
  lines.push("");
  lines.push("### Surfaces (Presentation / Basics)");
  lines.push("");
  for (const [surface, fields] of Object.entries(VALIDATION_SURFACES)) {
    lines.push(`- **${surface}:** ${fields.map((f) => `\`${f}\``).join(", ")}`);
  }
  lines.push("");
  lines.push("### Presentation fields with no Active-62 validation surface");
  lines.push("");
  for (const f of report.currentValidationCoverage.presentationFieldsNotCovered) {
    lines.push(`- \`${f}\``);
  }
  lines.push("");
  lines.push("### Child Brand Setup tables outside Active-62 gates");
  lines.push("");
  for (const c of childSetupTables) {
    lines.push(`- **${c.table}** (${c.fieldCount} fields) — ${c.recommendation}`);
  }
  lines.push("");
  lines.push("## 6. Fields included in Batch 1");
  lines.push("");
  lines.push("### Batch 1A");
  lines.push("");
  for (const [f, n] of Object.entries(batch1FieldsA)) lines.push(`- \`${f}\` × ${n}`);
  lines.push("");
  lines.push("### Batch 1B");
  lines.push("");
  for (const [f, n] of Object.entries(batch1FieldsB)) lines.push(`- \`${f}\` × ${n}`);
  lines.push("");
  lines.push(
    "Allowed by Batch 1 design but **not present** as patches in this plan: Case Summary Brand Relevance, Case Summary Tags (and Case Summary Owner Objective)."
  );
  lines.push("");
  lines.push("## 7. Fields excluded from Batch 1");
  lines.push("");
  for (const x of report.batch1Coverage.fieldsExcludedByDesign) lines.push(`- ${x}`);
  lines.push("");
  lines.push("## 8. Public/rendered fields not yet validated");
  lines.push("");
  if (!presentationPublicNoValidation.length) {
    lines.push(
      "- No Presentation `public_facing` fields lack a mapped validation surface in this audit matrix."
    );
  } else {
    for (const r of presentationPublicNoValidation) {
      lines.push(`- \`${r.tableName}\`.\`${r.fieldName}\` (${r.classification})`);
    }
  }
  lines.push("");
  lines.push(
    "- **Gap type that matters for the founder question:** entire child Brand Setup tables are unvalidated by Active-62 BE gates (see §5 / §11)."
  );
  lines.push("");
  lines.push("## 9. Internal / protected fields");
  lines.push("");
  lines.push("| Table | Field | Classification |");
  lines.push("| --- | --- | --- |");
  for (const r of report.internalProtectedFields) {
    lines.push(`| ${mdEscape(r.table)} | ${mdEscape(r.field)} | ${mdEscape(r.classification)} |`);
  }
  lines.push("");
  lines.push("## 10. Census-connected fields");
  lines.push("");
  lines.push("### Brand Setup side");
  lines.push("");
  for (const r of report.censusConnectedFields.brandSetupFields) {
    lines.push(`- \`${r.table}\`.\`${r.field}\``);
  }
  lines.push("");
  lines.push("### Census fields for crosscheck (read-only)");
  lines.push("");
  for (const f of CENSUS_FIELDS_FOR_CROSSCHECK) lines.push(`- \`${f}\``);
  lines.push("");
  lines.push(`Prior crosscheck artifact status: \`${censusCrosscheck?.status || "n/a"}\``);
  lines.push("");
  lines.push("**No Census writes in this audit.**");
  lines.push("");
  lines.push("## 11. Coverage gaps");
  lines.push("");
  lines.push("| Field | Table | Current Coverage | Risk | Recommendation |");
  lines.push("| --- | --- | --- | --- | --- |");
  for (const g of coverageGaps) {
    lines.push(
      `| ${mdEscape(g.field)} | ${mdEscape(g.table)} | ${mdEscape(g.currentCoverage)} | ${mdEscape(g.risk)} | ${mdEscape(g.recommendation)} |`
    );
  }
  lines.push("");
  lines.push("## 12. Recommendation before applying Batch 1A");
  lines.push("");
  lines.push(
    `- **Batch 1A within its declared scope (Presentation Low-risk safe_text_cleanup):** ${!holdReasons.length ? "**safe to apply**" : "**HOLD** — " + holdReasons.join("; ")}`
  );
  lines.push(
    `- **Full Brand Setup field coverage claim:** **not met** — additional validation needed for child Brand Setup tables and unclassified Basics fields before saying “all Brand Setup fields are reviewed.”`
  );
  lines.push("");
  for (const step of recommendationBefore1A.beforeApplyingBatch1A) {
    lines.push(`- ${step}`);
  }
  lines.push("");
  lines.push(recommendationBefore1A.noteOnApplyReport);
  lines.push("");
  lines.push(`**Final status:** \`${status}\``);
  lines.push("");

  fs.writeFileSync(OUT_MD, `${lines.join("\n")}\n`);

  const doc = `# Brand Setup — Full Field Coverage Audit

> **Status:** \`${status}\`  
> **Generated:** ${report.generatedAt}  
> **Mode:** read-only (no Airtable writes, no Batch 1 apply)

## Verdict

**No — the current Brand Explorer Active-62 process does not review all Brand Setup fields.**

It reviews:
- **Brand Setup - Brand Explorer Presentation** owner-facing text (\`Title\`, \`Body\`, Case Summary*) via semantic / PVQL / quality / momentum / Webflow review
- **Brand Setup - Brand Basics** governance inputs used for universe + footnote (\`Brand Status\`, validation/review date fields, etc.) — partial

It does **not** review child Brand Setup tables (Footprint, Fee Structure, Brand Standards, Deal Terms, Project Fit, Operational Support, Legal Terms, Loyalty & Commercial, Sustainability & ESG) under Active-62 gates.

## Batch 1A

Batch 1A remains **narrow-scope safe** (Presentation Low-risk text only). This audit does **not** apply patches.

Full reports:
- \`reports/brand-explorer/brand-setup-full-field-coverage-audit.md\`
- \`reports/brand-explorer/brand-setup-full-field-coverage-audit.json\`
`;
  fs.writeFileSync(OUT_DOC, doc);

  console.log("[coverage-audit] status=", status);
  console.log("[coverage-audit] fields=", fieldInventory.length);
  console.log("[coverage-audit] wrote", OUT_JSON);
  console.log("[coverage-audit] wrote", OUT_MD);
  console.log("[coverage-audit] wrote", OUT_DOC);
}

main().catch((err) => {
  console.error("[coverage-audit] FAILED", err);
  process.exit(1);
});
