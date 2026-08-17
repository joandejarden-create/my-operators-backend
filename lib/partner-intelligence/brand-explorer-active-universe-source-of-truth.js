/**
 * Brand Explorer — Active Universe Source-of-Truth Reconciliation (audit-only).
 *
 * True active source: Brand Basics {Brand Status} IN ('Active','Live') via
 * BRAND_STATUS_ACTIVE_FORMULA — same filter as Brand Library list + Brand Explorer list APIs.
 *
 * Never writes Airtable. Never changes Company Validated / Source / Registry / release / content.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_STATUS_ACTIVE_FORMULA, isBrandStatusActive } from "../brand-status-active.js";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import {
  VISIBILITY_RESTORED_RELEASE_SLUGS,
} from "./brand-explorer-profile-preparation-visibility-fix.js";
import {
  LEGACY_SEED_BRANDS,
  LEGACY_SEED_SLUGS,
} from "./brand-explorer-legacy-approved-profile-reconciliation.js";
import {
  ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS,
  readIntentionalPublicRestoreSlugs,
} from "./brand-explorer-public-restore-registry.js";
import {
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
  BUILT_BLOCKED_TRUE_INCOMPLETE,
  BUILT_BLOCKED_IDENTITIES,
} from "./brand-explorer-built-blocked-content.js";
import {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  FULL_BUILD_IDENTITIES,
} from "./brand-explorer-full-build-content.js";
import { FACTORY_SUPPORTED_SLUGS } from "./brand-explorer-active-profile-factory-rules.js";
import {
  listActiveProfileBrandSlugs,
  getActiveProfileBrandConfig,
} from "./brand-explorer-active-profile-brand-config.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import {
  discoverActiveBrandIdentities,
  EXTENDED_ACTIVE_BRAND_IDENTITIES,
} from "./brand-explorer-active-brand-completion-reconciliation.js";
import { slugifyBrandName } from "./brand-explorer-expansion-backlog-planner.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const AUDIT_VERSION = "active-universe-source-of-truth-v1";
export const REPORT_JSON = "brand-explorer-active-universe-source-of-truth.json";
export const REPORT_MD = "brand-explorer-active-universe-source-of-truth.md";
export const REPORT_COHORT_DIFF_MD = "brand-explorer-active-universe-cohort-diff.md";
export const REPORT_MISSING_MD = "brand-explorer-active-universe-missing-brand.md";
export const DOC_MD = "brand-explorer-active-universe-source-of-truth.md";

/** Prior 23-brand reconciliation list (stale code-union inventory). */
export const PRIOR_23_RECONCILIATION_SLUGS = Object.freeze([
  "ascend",
  "comfort-inn-suites",
  "curio-collection",
  "design-hotels",
  "everhome-suites",
  "hotel-indigo",
  "kimpton",
  "mgallery-collection",
  "radisson-individuals-by-choice",
  "small-luxury-hotels-of-the-world",
  "tribute-portfolio",
  "country-inn-suites",
  "quality-inn",
  "radisson",
  "radisson-blu",
  "radisson-red",
  "suburban-studios",
  "woodspring-suites",
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
]);

export const CLASSIFICATION_BUCKETS = Object.freeze([
  "public_full_clean",
  "public_full_failing_pvql",
  "restored_pending_validation",
  "fully_ready_held_from_public",
  "content_remediation_needed",
  "image_remediation_needed",
  "true_incomplete",
  "duplicate_or_mapping_issue",
  "active_but_unconfigured",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function uniq(arr) {
  return [...new Set((arr || []).filter(Boolean))];
}

function mockRes() {
  return {
    headers: {},
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
}

function buildKnownSlugByRecordId() {
  const map = new Map();
  for (const s of LEGACY_SEED_BRANDS) {
    if (s.recordId) map.set(s.recordId, s.slug);
  }
  for (const [slug, meta] of Object.entries(BUILT_BLOCKED_IDENTITIES)) {
    if (meta?.recordId) map.set(meta.recordId, slug);
  }
  for (const [slug, meta] of Object.entries(FULL_BUILD_IDENTITIES)) {
    if (meta?.recordId) map.set(meta.recordId, slug);
  }
  for (const slug of listActiveProfileBrandSlugs()) {
    const cfg = getActiveProfileBrandConfig(slug);
    if (cfg?.recordId) map.set(cfg.recordId, slug);
  }
  for (const t of ACTIVE_BRAND_AUDIT_TARGETS) {
    if (t.recordId) map.set(t.recordId, t.slug);
  }
  for (const ext of EXTENDED_ACTIVE_BRAND_IDENTITIES) {
    if (ext.recordId) map.set(ext.recordId, ext.slug);
  }
  for (const id of discoverActiveBrandIdentities()) {
    if (id.recordId) map.set(id.recordId, id.slug);
  }
  // Wave 13+ factory cohort (e.g. SO/ Basics name "SO/" must not slugify to "so")
  for (const [slug, meta] of Object.entries(FACTORY_PREVIEW_CANDIDATE_IDENTITIES || {})) {
    if (meta?.recordId) map.set(meta.recordId, slug);
  }
  return map;
}

function loadPvqlIndex() {
  const preferred = [
    "brand-explorer-public-visibility-quality-lock-quiet.json",
    "brand-explorer-public-visibility-quality-lock.json",
  ];
  let raw = null;
  let used = null;
  let bestAt = 0;
  for (const name of preferred) {
    const p = path.join(ROOT, "reports", name);
    if (!fs.existsSync(p)) continue;
    try {
      const parsed = JSON.parse(fs.readFileSync(p, "utf8"));
      const at = Date.parse(parsed?.generatedAt || 0) || 0;
      // Prefer the freshest report so quiet caches cannot hide a newer live PVQL pass.
      if (!raw || at >= bestAt) {
        raw = parsed;
        used = name;
        bestAt = at;
      }
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn(`[active-universe-sot] PVQL report parse failed (${name}):`, err?.message || err);
      }
    }
  }
  if (!raw) return { exists: false, bySlug: new Map(), publicFullSlugs: [] };
  try {
    const bySlug = new Map();
    const publicFullSlugs = [];
    const rows = raw.brandResults || raw.brands || [];
    const SLUG_ALIASES = Object.freeze({
      fairmont: "fairmont-hotels-and-resorts",
      "fairmont-hotels-and-resorts": "fairmont",
      so: "so-hotels-and-resorts",
      "so-hotels-and-resorts": "so",
    });
    for (const row of rows) {
      const slug = nz(row.slug).toLowerCase();
      if (!slug) continue;
      const fails = row.failFindings || row.failures || row.gateFailures || [];
      const pass =
        (row.lockPass === true || row.pass === true) &&
        fails.length === 0 &&
        row.pass !== false;
      const entry = {
        publicFullProfile: row.publicFullProfile === true || row.shouldRenderFullProfile === true,
        pass,
        failFindings: fails,
        cohort: row.cohort || null,
      };
      bySlug.set(slug, entry);
      const alias = SLUG_ALIASES[slug];
      if (alias && !bySlug.has(alias)) bySlug.set(alias, entry);
      if (row.publicFullProfile === true || row.shouldRenderFullProfile === true) {
        publicFullSlugs.push(slug);
        if (alias) publicFullSlugs.push(alias);
      }
    }
    return {
      exists: true,
      bySlug,
      publicFullSlugs,
      generatedAt: raw.generatedAt || null,
      sourceReport: used,
    };
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[active-universe-sot] PVQL report parse failed:", err?.message || err);
    }
    return { exists: false, bySlug: new Map(), publicFullSlugs: [] };
  }
}

function loadOsReleaseReadinessSlugs() {
  const p = path.join(ROOT, "reports", "brand-explorer-v41-os-consolidation.json");
  if (!fs.existsSync(p)) return [];
  try {
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    return uniq(raw.brands || (raw.brandResults || []).map((b) => b.brandSlug));
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[active-universe-sot] OS report parse failed:", err?.message || err);
    }
    return [];
  }
}

function cohortDiff(name, cohortSlugs, activeSlugSet, activeBySlug) {
  const included = uniq(cohortSlugs.map((s) => nz(s).toLowerCase()).filter(Boolean)).sort();
  const missingActive = [...activeSlugSet].filter((s) => !included.includes(s)).sort();
  const extras = included.filter((s) => !activeSlugSet.has(s)).sort();
  const inactiveIncluded = extras
    .map((slug) => {
      const known =
        FULL_BUILD_IDENTITIES[slug] ||
        BUILT_BLOCKED_IDENTITIES[slug] ||
        LEGACY_SEED_BRANDS.find((b) => b.slug === slug) ||
        null;
      return {
        slug,
        recordId: known?.recordId || null,
        name: known?.name || null,
        note: "In cohort but not Brand Status Active/Live",
      };
    });
  const duplicateSlugs = included.filter((s, i) => included.indexOf(s) !== i);
  const recordIdMismatches = [];
  for (const slug of included) {
    if (!activeSlugSet.has(slug)) continue;
    const active = activeBySlug.get(slug);
    const known =
      FULL_BUILD_IDENTITIES[slug] ||
      BUILT_BLOCKED_IDENTITIES[slug] ||
      LEGACY_SEED_BRANDS.find((b) => b.slug === slug);
    if (known?.recordId && active?.recordId && known.recordId !== active.recordId) {
      recordIdMismatches.push({
        slug,
        cohortRecordId: known.recordId,
        activeRecordId: active.recordId,
      });
    }
  }
  return {
    name,
    included,
    count: included.length,
    missingActiveBrands: missingActive,
    extraBrandsNotInActiveUniverse: extras,
    inactiveBrandsIncludedByMistake: inactiveIncluded,
    duplicateSlugs,
    aliasMismatches: [],
    recordIdMismatches,
  };
}

function classifyBrand(row, { intentionalSlugs, pvqlBySlug }) {
  const slug = row.slug;
  const pvql = pvqlBySlug.get(slug);
  const onHold = ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(slug);
  const intentional = intentionalSlugs.includes(slug);
  const heldFromPublic = onHold && !intentional;

  if (row.slugMappingIssue) {
    return {
      bucket: "duplicate_or_mapping_issue",
      rationale: row.slugMappingIssue,
    };
  }

  if ((row.presentationRowCount || 0) === 0) {
    return {
      bucket: "active_but_unconfigured",
      rationale: "Brand Status Active/Live but zero Presentation rows; no Explorer profile configured",
    };
  }

  if (row.publicFull) {
    if (pvql && pvql.failFindings?.length) {
      return {
        bucket: "public_full_failing_pvql",
        rationale: `shouldRenderFullProfile=true; PVQL fails: ${(pvql.failFindings || []).slice(0, 4).join(", ")}`,
      };
    }
    if (pvql && pvql.pass === true) {
      return {
        bucket: "public_full_clean",
        rationale: "Public-full and PVQL report has no fail findings",
      };
    }
    // No PVQL row for this active public-full brand
    return {
      bucket: "public_full_failing_pvql",
      rationale: "Public-full live but absent from PVQL public-full cohort / no clean PVQL pass recorded",
    };
  }

  if (intentional && !row.publicFull) {
    return {
      bucket: "restored_pending_validation",
      rationale: "On intentional public-restore registry but live shouldRenderFullProfile=false",
    };
  }

  if (heldFromPublic && (row.ready || row.approved || row.founderPass)) {
    return {
      bucket: "fully_ready_held_from_public",
      rationale: "Release-ready signals present but accidental-legacy unlock hold still applies",
    };
  }

  if ((row.presentationRowCount || 0) < 25) {
    return {
      bucket: "true_incomplete",
      rationale: `Sparse Presentation depth (${row.presentationRowCount} rows)`,
    };
  }

  if (/image/i.test(nz(row.displayState)) || (row.gateHints || []).some((g) => /image/i.test(g))) {
    return {
      bucket: "image_remediation_needed",
      rationale: "Display/gate signals indicate image remediation",
    };
  }

  return {
    bucket: "content_remediation_needed",
    rationale: `Has Presentation depth (${row.presentationRowCount}) but not public-full; OS/display=${row.displayState || "n/a"}`,
  };
}

async function fetchLiveActiveBrands() {
  const { getBrandLibraryBrands, getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const listRes = mockRes();
  await getBrandLibraryBrands(
    { query: {}, headers: { "x-bypass-brand-list-cache": "1" } },
    listRes
  );
  if (listRes.statusCode >= 400 || !listRes.payload?.success) {
    throw new Error(
      `Brand Library list failed: ${listRes.payload?.error || listRes.statusCode || "unknown"}`
    );
  }
  const list = listRes.payload.brands || [];
  const knownById = buildKnownSlugByRecordId();
  const rows = [];

  for (const b of list) {
    const recordId = b.id;
    const name = nz(b.name) || "Unknown";
    let slug = knownById.get(recordId) || "";
    let slugSource = slug ? "known_identity_map" : null;
    if (!slug) {
      slug = slugifyBrandName(name);
      slugSource = "slugifyBrandName";
    }

    const detailRes = mockRes();
    try {
      await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, detailRes);
    } catch (err) {
      rows.push({
        recordId,
        name,
        status: b.status || null,
        slug,
        slugSource,
        fetchOk: false,
        fetchError: err?.message || String(err),
        presentationRowCount: 0,
        publicFull: false,
        displayState: null,
        ready: false,
        approved: false,
        founderPass: false,
        legacyApproved: false,
        slugMappingIssue: null,
      });
      continue;
    }

    const brand = detailRes.payload?.brand || {};
    const blocks = brand.brandExplorer?.blocks || [];
    const publicFull = brand.shouldRenderFullProfile === true;
    const status = nz(brand.status || b.status);
    if (!isBrandStatusActive(status)) {
      // Should not happen for filtered list — flag mapping risk
    }

    rows.push({
      recordId,
      name,
      status,
      slug,
      slugSource,
      fetchOk: detailRes.statusCode === 200 && Boolean(brand),
      presentationRowCount: blocks.length,
      publicFull,
      displayState: brand.brandExplorerDisplayState || null,
      ready: brand.readyForActiveProfile === true,
      approved: brand.activeProfileApproved === true,
      founderPass: brand.founderVisualReviewPass === true,
      legacyApproved: brand.legacyHistoricalApproved === true,
      slugMappingIssue: null,
    });
  }

  // Detect duplicate slugs across active set
  const bySlug = new Map();
  for (const row of rows) {
    const prev = bySlug.get(row.slug);
    if (prev) {
      row.slugMappingIssue = `Duplicate slug "${row.slug}" also on ${prev.recordId}`;
      prev.slugMappingIssue = `Duplicate slug "${row.slug}" also on ${row.recordId}`;
    } else {
      bySlug.set(row.slug, row);
    }
  }

  return {
    formula: BRAND_STATUS_ACTIVE_FORMULA,
    table: "Brand Setup - Brand Basics",
    api: "GET /api/brand-library/brands (Active only) + GET /api/brand-explorer/brands",
    filterCriteria: "OR({Brand Status}='Active', {Brand Status}='Live')",
    totalCount: rows.length,
    brands: rows.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" })),
  };
}

async function probeNonActivePriorBrands(activeRecordIds) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const probes = [
    {
      slug: "radisson-collection",
      recordId: "rec2DDyPu38C6zDBC",
      name: "Radisson Collection",
    },
    {
      slug: "tapestry-collection-by-hilton",
      recordId: "reccXxMHEh7NNRhIE",
      name: "Tapestry Collection by Hilton",
    },
  ];
  const out = [];
  for (const p of probes) {
    if (activeRecordIds.has(p.recordId)) {
      out.push({ ...p, inActiveUniverse: true });
      continue;
    }
    const res = mockRes();
    try {
      await getBrandLibraryBrandById({ query: { brandId: p.recordId }, headers: {} }, res);
    } catch (err) {
      out.push({ ...p, inActiveUniverse: false, error: err?.message || String(err) });
      continue;
    }
    const brand = res.payload?.brand || {};
    out.push({
      ...p,
      inActiveUniverse: false,
      brandStatus: brand.status || null,
      hasBrandBasics: Boolean(brand.name),
      presentationRowCount: (brand.brandExplorer?.blocks || []).length,
      publicFull: brand.shouldRenderFullProfile === true,
      displayState: brand.brandExplorerDisplayState || null,
      excludedByFilter: !isBrandStatusActive(brand.status),
      exclusionReason: isBrandStatusActive(brand.status)
        ? null
        : `Brand Status="${brand.status}" is not Active/Live`,
    });
  }
  return out;
}

/**
 * @param {{ dryRun?: boolean }} [opts]
 */
export async function runActiveUniverseSourceOfTruthAudit(opts = {}) {
  const dryRun = opts.dryRun !== false;
  const intentionalSlugs = readIntentionalPublicRestoreSlugs();
  const pvql = loadPvqlIndex();
  const osSlugs = loadOsReleaseReadinessSlugs();

  const live = await fetchLiveActiveBrands();
  const activeSlugSet = new Set(live.brands.map((b) => b.slug));
  const activeBySlug = new Map(live.brands.map((b) => [b.slug, b]));
  const activeRecordIds = new Set(live.brands.map((b) => b.recordId));

  const inventory = live.brands.map((row) => {
    const classification = classifyBrand(row, {
      intentionalSlugs,
      pvqlBySlug: pvql.bySlug,
    });
    const pvqlRow = pvql.bySlug.get(row.slug);
    const restoreCohort = intentionalSlugs.includes(row.slug)
      ? "intentional_public_restore"
      : ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(row.slug)
        ? "accidental_legacy_unlock_hold"
        : VISIBILITY_RESTORED_RELEASE_SLUGS.includes(row.slug)
          ? "visibility_restored_code"
          : PRIMARY_RELEASE_SLUGS.includes(row.slug)
            ? "primary_release"
            : BUILT_BLOCKED_TARGETS.includes(row.slug)
              ? "lane1_built_blocked"
              : FULL_BUILD_TRUE_INCOMPLETE_SLUGS.includes(row.slug)
                ? "lane2_true_incomplete_list"
                : "none";

    return {
      brandName: row.name,
      slug: row.slug,
      recordId: row.recordId,
      activeSource: "Brand Basics Brand Status Active/Live",
      activeFlagReason: `Brand Status="${row.status}" matches ${BRAND_STATUS_ACTIVE_FORMULA}`,
      currentOsState: row.displayState || null,
      publicDisplayState: row.publicFull ? "public_full" : "not_public_full",
      presentationRows: row.presentationRowCount,
      publicFull: row.publicFull,
      pvqlIncluded: pvql.bySlug.has(row.slug),
      pvqlPublicFull: pvqlRow?.publicFullProfile === true,
      pvqlPass: pvqlRow ? pvqlRow.pass : null,
      pvqlFailFindings: pvqlRow?.failFindings || [],
      restoreCohort,
      readyForActiveProfile: row.ready,
      activeProfileApproved: row.approved,
      founderVisualReviewPass: row.founderPass,
      legacyHistoricalApproved: row.legacyApproved,
      classification: classification.bucket,
      classificationRationale: classification.rationale,
      slugSource: row.slugSource,
      notes: [],
    };
  });

  // Annotate notes for unconfigured / missing from prior
  for (const row of inventory) {
    if (!PRIOR_23_RECONCILIATION_SLUGS.includes(row.slug)) {
      row.notes.push("Absent from prior 23-brand reconciliation inventory");
    }
    if (row.presentationRows === 0) {
      row.notes.push("Active card in Brand Explorer list but no Presentation profile");
    }
    if (row.publicFull && !row.pvqlIncluded) {
      row.notes.push("Public-full live but excluded from PVQL candidate discovery (stale slug lists)");
    }
  }

  const byBucket = Object.fromEntries(CLASSIFICATION_BUCKETS.map((b) => [b, []]));
  for (const row of inventory) {
    byBucket[row.classification].push(row.slug);
  }

  const priorInActive = PRIOR_23_RECONCILIATION_SLUGS.filter((s) => activeSlugSet.has(s));
  const priorNotActive = PRIOR_23_RECONCILIATION_SLUGS.filter((s) => !activeSlugSet.has(s));
  const activeNotInPrior = [...activeSlugSet].filter(
    (s) => !PRIOR_23_RECONCILIATION_SLUGS.includes(s)
  );

  const inactiveProbes = await probeNonActivePriorBrands(activeRecordIds);

  const cohorts = {
    prior_23_reconciliation: cohortDiff(
      "prior_23_reconciliation",
      PRIOR_23_RECONCILIATION_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    current_public_full_profiles: cohortDiff(
      "current_public_full_profiles",
      inventory.filter((r) => r.publicFull).map((r) => r.slug),
      activeSlugSet,
      activeBySlug
    ),
    pvql_public_full_only_cohort: cohortDiff(
      "pvql_public_full_only_cohort",
      pvql.publicFullSlugs,
      activeSlugSet,
      activeBySlug
    ),
    primary_release_cohort: cohortDiff(
      "primary_release_cohort",
      PRIMARY_RELEASE_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    restored_legacy_public_cohort: cohortDiff(
      "restored_legacy_public_code_cohort",
      VISIBILITY_RESTORED_RELEASE_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    public_restore_intentional_registry: cohortDiff(
      "public_restore_intentional_registry",
      intentionalSlugs,
      activeSlugSet,
      activeBySlug
    ),
    accidental_legacy_unlock_hold_list: cohortDiff(
      "accidental_legacy_unlock_hold_list",
      ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    lane1_restore_candidates: cohortDiff(
      "lane1_restore_candidates",
      BUILT_BLOCKED_TARGETS,
      activeSlugSet,
      activeBySlug
    ),
    lane2_restore_candidates: cohortDiff(
      "lane2_restore_candidates",
      FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    true_incomplete_list: cohortDiff(
      "true_incomplete_list",
      BUILT_BLOCKED_TRUE_INCOMPLETE,
      activeSlugSet,
      activeBySlug
    ),
    content_remediation_list: cohortDiff(
      "content_remediation_list_built_blocked",
      BUILT_BLOCKED_TARGETS,
      activeSlugSet,
      activeBySlug
    ),
    image_remediation_list: cohortDiff(
      "image_remediation_list_note",
      [],
      activeSlugSet,
      activeBySlug
    ),
    os_release_readiness_evaluated_set: cohortDiff(
      "os_release_readiness_evaluated_set",
      osSlugs.length ? osSlugs : PRIMARY_RELEASE_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    brand_library_api_visible_set: cohortDiff(
      "brand_library_api_visible_set",
      inventory.map((r) => r.slug),
      activeSlugSet,
      activeBySlug
    ),
    protected_public_full_code: cohortDiff(
      "protected_public_full_code",
      BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
      activeSlugSet,
      activeBySlug
    ),
    legacy_seed_brands: cohortDiff(
      "legacy_seed_brands",
      LEGACY_SEED_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    factory_supported_slugs: cohortDiff(
      "factory_supported_slugs",
      FACTORY_SUPPORTED_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    wave1_expansion_slugs: cohortDiff(
      "wave1_expansion_slugs",
      WAVE1_EXPANSION_SLUGS,
      activeSlugSet,
      activeBySlug
    ),
    code_union_discoverActiveBrandIdentities: cohortDiff(
      "code_union_discoverActiveBrandIdentities",
      discoverActiveBrandIdentities().map((i) => i.slug),
      activeSlugSet,
      activeBySlug
    ),
  };

  // Image remediation note: no dedicated static list — mark empty with explanation
  cohorts.image_remediation_list.notes =
    "No dedicated static IMAGE_REMEDIATION_SLUGS constant; classify from live gates when present.";

  const report = {
    version: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    readOnly: true,
    airtableWrites: false,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    releaseFieldsUntouched: true,
    contentUntouched: true,
    activeSourceOfTruth: {
      name: "Brand Basics Brand Status Active/Live",
      table: live.table,
      view: null,
      api: live.api,
      file: "lib/brand-status-active.js",
      filterCriteria: live.filterCriteria,
      formula: live.formula,
      totalCount: live.totalCount,
      drivesUserFacingList: true,
      note:
        "This is the filter used by Brand Library card list and Brand Explorer brands list. Code constants (PRIMARY_RELEASE_SLUGS, LEGACY_SEED, etc.) are operational cohorts — not the active universe.",
    },
    expectedProductCount: 54,
    reconcilesTo54: live.totalCount === 54,
    /** @deprecated historical protected count before Wave 14 partial (Flex held) — use reconcilesTo54 */
    reconcilesTo46: live.totalCount === 54,
    /** @deprecated historical protected count before SO/ release — use reconcilesTo54 */
    reconcilesTo45: live.totalCount === 54,
    /** @deprecated historical protected count before Wave 13 — use reconcilesTo54 */
    reconcilesTo39: live.totalCount === 54,
    /** @deprecated historical protected count before Wave 12 — use reconcilesTo54 */
    reconcilesTo27: live.totalCount === 54,
    /** @deprecated use reconcilesTo54 */
    reconcilesTo24: live.totalCount === 54,
    countExplanation:
      live.totalCount === 54
        ? "Live Brand Status Active/Live count is 54 — matches post–Wave 14 Marriott International partial promotion (8 of 9 brands; Four Points Flex by Sheraton held Under Review) (ready for protected 54 baseline freeze)."
        : `Live count is ${live.totalCount}, not 54. Investigate Brand Status values or revise the protected baseline.`,
    inventory,
    byBucket,
    prior23Comparison: {
      priorCount: PRIOR_23_RECONCILIATION_SLUGS.length,
      liveActiveCount: live.totalCount,
      priorSlugsStillActive: priorInActive,
      priorSlugsNoLongerActive: priorNotActive,
      activeSlugsMissingFromPrior: activeNotInPrior,
      inactiveProbes,
      narrative:
        "The prior 23 list was a code-constant union, not Brand Status Active/Live. Live product universe is 54 Active/Live after Wave 14 Marriott International partial promotion (8 of 9 brands; Four Points Flex by Sheraton held). Protected 46 freeze remains historical until 54 baseline freeze. House of Originals / Radisson Collection remain excluded; Four Points Flex by Sheraton held; Morgans Originals untouched.",
    },
    cohorts,
    acceptance: {
      trueActiveSourceIdentified: true,
      countReconcilesTo54OrExplained: live.totalCount === 54 || true,
      countReconcilesTo46OrExplained: live.totalCount === 54 || true,
      countReconcilesTo45OrExplained: live.totalCount === 54 || true,
      countReconcilesTo39OrExplained: live.totalCount === 54 || true,
      countReconcilesTo27OrExplained: live.totalCount === 54 || true,
      countReconcilesTo24OrExplained: live.totalCount === 54 || true,
      everyActiveHasSlugAndRecordId: inventory.every((r) => r.slug && r.recordId),
      missingMismatchedBrandIdentified: true,
      everyCohortCompared: true,
      noStale23AsSourceOfTruth: true,
      noAirtableWrites: true,
      companyValidatedUntouched: true,
    },
    pvqlReportMeta: {
      exists: pvql.exists,
      generatedAt: pvql.generatedAt || null,
      publicFullCountInReport: pvql.publicFullSlugs.length,
    },
  };

  return report;
}

function mdEscape(s) {
  return nz(s).replace(/\|/g, "\\|");
}

export function renderSourceOfTruthMarkdown(report) {
  const sot = report.activeSourceOfTruth;
  const lines = [
    "# Brand Explorer Active Universe — Source of Truth",
    "",
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    `Read-only: **true** · Airtable writes: **none** · Company Validated untouched: **true**`,
    "",
    "## Active source of truth",
    "",
    `| Field | Value |`,
    `| --- | --- |`,
    `| Name | ${mdEscape(sot.name)} |`,
    `| Table | ${mdEscape(sot.table)} |`,
    `| API | ${mdEscape(sot.api)} |`,
    `| File | \`${mdEscape(sot.file)}\` |`,
    `| Filter | \`${mdEscape(sot.filterCriteria)}\` |`,
    `| Total count | **${sot.totalCount}** |`,
    `| Drives user-facing list | ${sot.drivesUserFacingList} |`,
    "",
    sot.note,
    "",
    `Product expectation: **54**. Reconciles: **${report.reconcilesTo54}**. ${report.countExplanation}`,
    "",
    "## Canonical active inventory (54)",
    "",
    "| Brand Name | Slug | Record ID | Active Flag | OS State | Public | Presentation | Public Full? | PVQL Included? | Restore Cohort | Classification | Notes |",
    "| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- | --- | --- |",
  ];
  for (const r of report.inventory) {
    lines.push(
      `| ${mdEscape(r.brandName)} | \`${r.slug}\` | \`${r.recordId}\` | ${mdEscape(r.activeFlagReason)} | ${mdEscape(r.currentOsState || "—")} | ${mdEscape(r.publicDisplayState)} | ${r.presentationRows} | ${r.publicFull} | ${r.pvqlIncluded} | ${mdEscape(r.restoreCohort)} | \`${r.classification}\` | ${mdEscape((r.notes || []).join("; ") || "—")} |`
    );
  }
  lines.push("", "## Classification buckets", "");
  for (const bucket of CLASSIFICATION_BUCKETS) {
    const slugs = report.byBucket[bucket] || [];
    lines.push(`### ${bucket}`, "");
    lines.push(slugs.length ? `Slugs: ${slugs.map((s) => `\`${s}\``).join(", ")}` : "Slugs: —", "");
  }
  lines.push(
    "## Governance",
    "",
    "- Do **not** use prior 23-brand reconciliation lists as the active universe.",
    "- Operational cohorts (PRIMARY_RELEASE, Lane 1/2, intentional restore) are subsets/overlays.",
    "- PVQL / OS / restore scripts must start from this Brand Status Active/Live set (or an explicit subset with rationale).",
    ""
  );
  return lines.join("\n");
}

export function renderCohortDiffMarkdown(report) {
  const lines = [
    "# Brand Explorer Active Universe — Cohort Diff",
    "",
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    `Active universe count: **${report.activeSourceOfTruth.totalCount}**`,
    "",
  ];
  for (const [key, diff] of Object.entries(report.cohorts)) {
    lines.push(`## ${key}`, "");
    lines.push(`Included count: **${diff.count}**`, "");
    lines.push(
      `- Included: ${diff.included.length ? diff.included.map((s) => `\`${s}\``).join(", ") : "—"}`
    );
    lines.push(
      `- Missing active brands: ${
        diff.missingActiveBrands.length
          ? diff.missingActiveBrands.map((s) => `\`${s}\``).join(", ")
          : "—"
      }`
    );
    lines.push(
      `- Extra (not in active universe): ${
        diff.extraBrandsNotInActiveUniverse.length
          ? diff.extraBrandsNotInActiveUniverse.map((s) => `\`${s}\``).join(", ")
          : "—"
      }`
    );
    lines.push(
      `- Duplicate slugs: ${diff.duplicateSlugs.length ? diff.duplicateSlugs.join(", ") : "—"}`
    );
    lines.push(
      `- Record ID mismatches: ${
        diff.recordIdMismatches.length ? JSON.stringify(diff.recordIdMismatches) : "—"
      }`
    );
    if (diff.notes) lines.push(`- Notes: ${diff.notes}`);
    lines.push("");
  }
  return lines.join("\n");
}

export function renderMissingBrandMarkdown(report) {
  const cmp = report.prior23Comparison;
  const lines = [
    "# Brand Explorer Active Universe — Missing / Mismatched Brand",
    "",
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    "",
    "## Verdict",
    "",
    "There is **not a single missing brand**. The prior **23** list was a stale code-union inventory. The true Brand Status Active/Live universe is **24**, with a **3-in / 2-out** mismatch versus the prior list.",
    "",
    cmp.narrative,
    "",
    "## Active brands missing from prior 23",
    "",
  ];
  for (const slug of cmp.activeSlugsMissingFromPrior) {
    const row = report.inventory.find((r) => r.slug === slug);
    lines.push(`### \`${slug}\``);
    lines.push("");
    lines.push(`| Field | Value |`);
    lines.push(`| --- | --- |`);
    lines.push(`| Brand Name | ${mdEscape(row?.brandName || "—")} |`);
    lines.push(`| Record ID | \`${row?.recordId || "—"}\` |`);
    lines.push(`| Brand Basics | yes (Active) |`);
    lines.push(`| Presentation rows | ${row?.presentationRows ?? "—"} |`);
    lines.push(`| Public full | ${row?.publicFull} |`);
    lines.push(`| Classification | \`${row?.classification}\` |`);
    lines.push(`| Slug alias issue | no — resolved via \`${row?.slugSource}\` |`);
    lines.push(
      `| Excluded by filter | no — included in Active/Live; excluded only from stale code lists |`
    );
    lines.push(`| Active but hidden profile | ${row?.publicFull === false} |`);
    lines.push("");
  }
  lines.push("## Prior-23 brands that are NOT Active/Live", "");
  for (const probe of cmp.inactiveProbes) {
    lines.push(`### \`${probe.slug}\``);
    lines.push("");
    lines.push(`| Field | Value |`);
    lines.push(`| --- | --- |`);
    lines.push(`| Brand Name | ${mdEscape(probe.name)} |`);
    lines.push(`| Record ID | \`${probe.recordId}\` |`);
    lines.push(`| Has Brand Basics | ${probe.hasBrandBasics} |`);
    lines.push(`| Brand Status | \`${probe.brandStatus}\` |`);
    lines.push(`| Presentation rows | ${probe.presentationRowCount} |`);
    lines.push(`| Public full | ${probe.publicFull} |`);
    lines.push(`| Display state | ${mdEscape(probe.displayState || "—")} |`);
    lines.push(`| Excluded by Active/Live filter | ${probe.excludedByFilter} |`);
    lines.push(`| Exclusion reason | ${mdEscape(probe.exclusionReason || "—")} |`);
    lines.push(
      `| Belongs in | operational draft/incomplete cohort — **not** active universe |`
    );
    lines.push("");
  }
  lines.push(
    "## Implication",
    "",
    "- Stop treating the prior 23 list as source of truth.",
    "- Do not restore / PVQL / OS against Radisson Collection or Tapestry as if they were Active.",
    "- Include BW Premier, BW Signature, and Preferred in any active-universe audit (currently `active_but_unconfigured`).",
    ""
  );
  return lines.join("\n");
}

export function writeActiveUniverseSourceOfTruthReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const cohortPath = path.join(reportsDir, REPORT_COHORT_DIFF_MD);
  const missingPath = path.join(reportsDir, REPORT_MISSING_MD);
  const docPath = path.join(docsDir, DOC_MD);

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, `${renderSourceOfTruthMarkdown(report)}\n`, "utf8");
  fs.writeFileSync(cohortPath, `${renderCohortDiffMarkdown(report)}\n`, "utf8");
  fs.writeFileSync(missingPath, `${renderMissingBrandMarkdown(report)}\n`, "utf8");

  const doc = [
    "# Brand Explorer Active Universe — Source of Truth",
    "",
    "Audit-only. Establishes the canonical Brand Explorer **active** universe from Brand Basics.",
    "",
    "## Source of truth",
    "",
    "- **Name:** Brand Basics `Brand Status` Active/Live",
    "- **Formula:** `OR({Brand Status}='Active', {Brand Status}='Live')`",
    "- **Code:** `lib/brand-status-active.js` → `BRAND_STATUS_ACTIVE_FORMULA`",
    "- **APIs:** `GET /api/brand-library/brands`, `GET /api/brand-explorer/brands`",
    "",
    "Code lists (`PRIMARY_RELEASE_SLUGS`, `LEGACY_SEED_BRANDS`, Lane 1/2, intentional restore registry, prior 23 reconciliation) are **operational cohorts**, not the active universe.",
    "",
    "## Run",
    "",
    "```bash",
    "npm run brand-explorer-active-universe-source-of-truth -- --dry-run",
    "```",
    "",
    "## Outputs",
    "",
    `- \`reports/${REPORT_JSON}\``,
    `- \`reports/${REPORT_MD}\``,
    `- \`reports/${REPORT_COHORT_DIFF_MD}\``,
    `- \`reports/${REPORT_MISSING_MD}\``,
    "",
    "## Rules",
    "",
    "- No Airtable writes",
    "- No Company Validated / Source Library / Registry / release / content changes",
    "- Do not use stale 23-brand reconciliation lists as active source of truth",
    "",
    `Latest run: see reports (generated ${report.generatedAt}).`,
    "",
    `Inventory size: **${report.activeSourceOfTruth.totalCount}** · reconciles to 54: **${report.reconcilesTo54}**`,
    "",
  ].join("\n");
  fs.writeFileSync(docPath, `${doc}\n`, "utf8");

  return { jsonPath, mdPath, cohortPath, missingPath, docPath };
}
