/**
 * Brand Explorer — protected 24 Active/Live public-full baseline freeze.
 *
 * Read-only. No Airtable writes. No Presentation / image / CV / Source /
 * Registry / Brand Status / release changes.
 *
 * Universe SoT: Brand Basics Brand Status ∈ {Active, Live}
 * via lib/partner-intelligence/brand-explorer-active-universe.js
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACTIVE_UNIVERSE_SOURCE,
  ACTIVE_UNIVERSE_VERSION,
  NON_ACTIVE_STATUS_CONFLICT_PROBES,
  loadActiveUniverse,
} from "./brand-explorer-active-universe.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { TAB_SECTION_FAMILIES } from "./brand-explorer-24-tab-section-quality-audit.js";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import { runPublicVisibilityQualityLock } from "./brand-explorer-public-visibility-quality-lock.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const BASELINE_VERSION = "24-active-public-full-baseline-v1";
export const EXPECTED_ACTIVE_COUNT = 24;
export const EXPECTED_QUALITY_RECOMMENDATION = "approve_for_baseline_freeze";

export const REPORT_JSON = "brand-explorer-24-active-public-full-baseline.json";
export const REPORT_MD = "brand-explorer-24-active-public-full-baseline.md";
export const DOCS_MD = "brand-explorer-24-active-public-full-baseline.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const QUALITY_AUDIT_JSON = "brand-explorer-24-tab-section-quality-audit.json";
const IMAGE_AUDIT_JSON = "brand-explorer-24-image-repetition-audit.json";
const PVQL_JSON = "brand-explorer-public-visibility-quality-lock.json";
const OS_JSON = "brand-explorer-v41-os-consolidation.json";

/** Known stale operational lists that must never be treated as the active universe. */
export const STALE_OPERATIONAL_LIST_NAMES = Object.freeze([
  "PRIMARY_RELEASE_SLUGS",
  "prior_23_reconciliation",
  "legacy_23_active_list",
  "FACTORY_SUPPORTED_SLUGS_as_universe",
]);

export const PROTECTED_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library status",
  "Registry approval/status",
  "Brand Status",
  "release fields",
  "public restore registry",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readJsonReport(name) {
  const p = path.join(REPORTS_DIR, name);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

async function fetchBrandById(brandId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    return { ok: false, brand: null, error: `HTTP ${res.statusCode}` };
  }
  return { ok: true, brand: res.payload.brand, error: null };
}

function countVisibleTabsSections() {
  return {
    visibleTabCount: TAB_SECTION_FAMILIES.length,
    visibleSectionCount: TAB_SECTION_FAMILIES.reduce((n, t) => n + (t.sections?.length || 0), 0),
  };
}

function osStateBySlug(osReport) {
  const map = new Map();
  for (const row of osReport?.brands || osReport?.results || []) {
    const slug = nz(row.brandSlug || row.slug);
    if (!slug) continue;
    map.set(slug, {
      canonicalState: row.canonicalState || row.displayState || null,
      nextAction: row.allowedNextAction || row.nextAction || row.action || null,
      founderVisualReviewPass: row.founderVisualReviewPass ?? row.metrics?.founderVisualReviewPassed ?? null,
      activeProfileApproved: row.activeProfileApproved ?? row.metrics?.activeReleaseApproved ?? null,
    });
  }
  // consolidation summary rows
  for (const row of osReport?.summaryRows || []) {
    const slug = nz(row.brandSlug || row.slug);
    if (!slug || map.has(slug)) continue;
    map.set(slug, {
      canonicalState: row.canonicalState || null,
      nextAction: row.nextAction || null,
      founderVisualReviewPass: null,
      activeProfileApproved: null,
    });
  }
  return map;
}

/**
 * Build protected baseline snapshot (read-only).
 * Composes live Active/Live universe + latest quality/PVQL/image/OS reports.
 */
export async function build24ActivePublicFullBaseline({
  requireReports = true,
} = {}) {
  const universe = await loadActiveUniverse({ includeDetails: true });
  const quality = readJsonReport(QUALITY_AUDIT_JSON);
  const imageAudit = readJsonReport(IMAGE_AUDIT_JSON);
  const pvql = readJsonReport(PVQL_JSON);
  const osReport = readJsonReport(OS_JSON);

  if (requireReports) {
    if (!quality?.brandResults?.length) {
      throw new Error(`Missing ${QUALITY_AUDIT_JSON} — run quality audit before freeze`);
    }
    if (!pvql?.brands?.length) {
      throw new Error(`Missing ${PVQL_JSON} — run PVQL before freeze`);
    }
  }

  const qualityBySlug = new Map((quality?.brandResults || []).map((b) => [b.slug, b]));
  const pvqlBySlug = new Map((pvql?.brands || []).map((b) => [b.slug, b]));
  const imageBySlug = new Map((imageAudit?.brandResults || []).map((b) => [b.slug, b]));
  const osBySlug = osStateBySlug(osReport);
  const { visibleTabCount, visibleSectionCount } = countVisibleTabsSections();
  const validatedAt = new Date().toISOString();

  const brands = [];
  for (const u of universe.brands) {
    const q = qualityBySlug.get(u.slug) || {};
    const p = pvqlBySlug.get(u.slug) || {};
    const img = imageBySlug.get(u.slug) || {};
    const os = osBySlug.get(u.slug) || null;
    const brandApi = u.brandApi || {};

    const companyValidated =
      p.companyValidated === true ||
      brandApi.companyValidated === true ||
      brandApi.governance?.companyValidated === true
        ? true
        : false;
    const companyValidationDate =
      brandApi.companyValidationDate ||
      brandApi.governance?.companyValidationDate ||
      null;

    const gateResults = p.gateResults || {};
    const scenarioDistinct = q.gates?.scenarioDistinct ?? p.evidence?.scenarioDistinct ?? 0;
    const galleryDistinct = q.gates?.galleryDistinct ?? p.evidence?.galleryDistinct ?? 0;
    const propertyDistinct = q.gates?.propertyDistinct ?? p.evidence?.propertyDistinct ?? 0;

    const repeatedScenario =
      (img.imageFindings || []).some((f) => /repeated_visual_role/i.test(f.finding || f.issueType || "")) ||
      scenarioDistinct < 3;

    brands.push({
      brandName: u.name,
      slug: u.slug,
      recordId: u.recordId,
      brandStatus: u.status,
      shouldRenderFullProfile: u.publicFull === true || brandApi.shouldRenderFullProfile === true,
      publicDisplayState: u.displayState || p.publicDisplayState || q.publicDisplayState || null,
      publicFullProfile: u.publicFull === true || p.publicFullProfile === true,
      pvqlStatus: p.lockPass === true || q.pvqlStatus === "pass" ? "pass" : "fail",
      pvqlFailures: p.failures || q.pvqlFailures || [],
      qualityRecommendation: q.overallRecommendation || null,
      qualityComposite: q.scores?.composite ?? null,
      blockerCount: q.scores?.blockerCount ?? null,
      osState: os?.canonicalState || null,
      osNextAction: os?.nextAction || null,
      releaseRestoreStatus: {
        cohort: p.cohort || null,
        primaryReleaseCohort: PRIMARY_RELEASE_SLUGS.includes(u.slug),
        restoredLegacyPublic: p.restoredLegacyPublic === true,
        readyForActiveProfile: u.readyForActiveProfile === true,
        activeProfileApproved: u.activeProfileApproved === true,
        founderVisualReviewPass: u.founderVisualReviewPass === true,
        legacyHistoricalApproved: u.legacyHistoricalApproved === true,
      },
      presentationRowCount: u.presentationRowCount ?? p.allPresentationRowCount ?? null,
      ownerFacingRowCount: p.ownerFacingRowCount ?? null,
      visibleTabCount,
      visibleSectionCount,
      galleryImageCount: galleryDistinct,
      scenarioImageCount: scenarioDistinct,
      propertyExampleImageCount: propertyDistinct,
      imageRepetitionStatus: repeatedScenario ? "scenario_repetition_flagged" : "ok",
      crossBrandImageReuseStatus: "none",
      companyValidated,
      companyValidationDate,
      sourceLibraryStatus: "untouched_at_freeze",
      registryStatus: "untouched_at_freeze",
      lastValidationTimestamp: validatedAt,
      notes: [],
      gateHits: {
        rawUrlScanHits: gateResults.raw_url_scan?.hits?.length ?? 0,
        forbiddenOwnerFacingHits: gateResults.forbidden_owner_facing_language?.hits?.length ?? 0,
        genericCopyMechanicalHits: gateResults.generic_copy_scan?.mechanicalHits?.length ?? 0,
        genericCopyGoldenFailures: gateResults.generic_copy_scan?.goldenFailures?.length ?? 0,
      },
    });
  }

  // Excluded non-active probes
  const excluded = [];
  for (const probe of NON_ACTIVE_STATUS_CONFLICT_PROBES) {
    const fetched = await fetchBrandById(probe.recordId);
    const status = fetched.brand?.status || fetched.brand?.brandStatus || null;
    excluded.push({
      brandName: probe.name,
      slug: probe.slug,
      recordId: probe.recordId,
      brandStatus: status,
      excludedBecause: "Not Active/Live — Brand Status is not Active or Live",
      includedInBaseline: false,
      isActiveLive: isBrandStatusActive(status),
    });
  }

  const crossBrandCount = (imageAudit?.crossBrandImageIssues || quality?.crossBrandImageIssues || []).length;
  for (const b of brands) {
    b.crossBrandImageReuseStatus = crossBrandCount > 0 ? "present" : "none";
  }

  const freezeDecision =
    universe.totalCount === EXPECTED_ACTIVE_COUNT &&
    brands.length === EXPECTED_ACTIVE_COUNT &&
    brands.every((b) => b.shouldRenderFullProfile && b.publicFullProfile) &&
    brands.every((b) => b.pvqlStatus === "pass") &&
    brands.every((b) => b.qualityRecommendation === EXPECTED_QUALITY_RECOMMENDATION) &&
    brands.every((b) => (b.blockerCount ?? 0) === 0) &&
    brands.every((b) => b.scenarioImageCount >= 3) &&
    crossBrandCount === 0 &&
    excluded.every((e) => e.includedInBaseline === false && e.isActiveLive !== true)
      ? "frozen_24_active_public_full_baseline"
      : "freeze_incomplete";

  return {
    version: BASELINE_VERSION,
    generatedAt: validatedAt,
    dryRun: true,
    writePerformed: false,
    airtableWrites: false,
    presentationWrites: false,
    imageWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    brandStatusWrites: false,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    activeUniverseVersion: ACTIVE_UNIVERSE_VERSION,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT,
    activeCount: universe.totalCount,
    frozen: freezeDecision === "frozen_24_active_public_full_baseline",
    freezeDecision,
    protectedFields: [...PROTECTED_FIELDS],
    staleOperationalListsRejectedAsUniverse: [...STALE_OPERATIONAL_LIST_NAMES],
    primaryReleaseOverlayCount: PRIMARY_RELEASE_SLUGS.length,
    primaryReleaseNote:
      "PRIMARY_RELEASE_SLUGS is an operational overlay (7), not the Active/Live universe (24).",
    brands,
    excludedNonActive: excluded,
    validationSources: {
      qualityAudit: quality
        ? { file: QUALITY_AUDIT_JSON, generatedAt: quality.generatedAt, decision: quality.baselineFreezeDecision }
        : null,
      pvql: pvql
        ? { file: PVQL_JSON, generatedAt: pvql.generatedAt, publicFull: pvql.summary?.publicFullProfileCount }
        : null,
      imageAudit: imageAudit
        ? { file: IMAGE_AUDIT_JSON, generatedAt: imageAudit.generatedAt, crossBrand: crossBrandCount }
        : null,
      os: osReport ? { file: OS_JSON, generatedAt: osReport.generatedAt || null } : null,
    },
    summary: {
      activeCount: universe.totalCount,
      publicFullCount: brands.filter((b) => b.publicFullProfile).length,
      shouldRenderFullProfileCount: brands.filter((b) => b.shouldRenderFullProfile).length,
      pvqlPassCount: brands.filter((b) => b.pvqlStatus === "pass").length,
      freezeRecommendationCount: brands.filter(
        (b) => b.qualityRecommendation === EXPECTED_QUALITY_RECOMMENDATION
      ).length,
      remediationCount: brands.filter((b) => b.qualityRecommendation === "remediation_required").length,
      blockerBrandCount: brands.filter((b) => (b.blockerCount ?? 0) > 0).length,
      crossBrandImageReuse: crossBrandCount,
      scenarioRepetitionBrandCount: brands.filter((b) => b.imageRepetitionStatus !== "ok").length,
      excludedNonActiveCount: excluded.length,
      companyValidatedTrueCount: brands.filter((b) => b.companyValidated === true).length,
    },
    regressionRules: [
      "Active/Live universe count must remain 24 unless freeze is explicitly revised",
      "Every Active/Live brand must remain public-full with shouldRenderFullProfile=true",
      "Every Active/Live brand must pass PVQL",
      "Every Active/Live brand must remain approve_for_baseline_freeze",
      "No blocker or remediation_required on Active/Live brands",
      "No cross-brand image reuse",
      "Value scenario images must remain distinct (scenarioDistinct ≥ 3)",
      "raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0",
      "Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly",
      "Radisson Collection and Tapestry must remain excluded unless Brand Status promoted to Active/Live",
      "Stale 23-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT",
    ],
    rollbackNotes: [
      "This freeze is report-only — no Airtable writes occurred.",
      "To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT contract after an explicit founder decision.",
      "Do not revert Brand Status / CV / Source / Registry to 'undo' this freeze — those fields were never written.",
    ],
    futureWorkRules: [
      "New Active/Live brands require a new baseline revision (count will leave 24).",
      "Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.",
      "Promoting Radisson Collection or Tapestry requires Brand Status → Active/Live first, then full public-full + PVQL + quality freeze path.",
      "Operational cohorts (PRIMARY_RELEASE, restore lanes) remain overlays, not universe SoT.",
    ],
  };
}

/**
 * Evaluate live state against a frozen baseline snapshot.
 */
export function evaluate24ActivePublicFullBaselineRegression({
  frozen,
  liveUniverse,
  livePvql,
  liveQuality,
  liveExcluded = [],
} = {}) {
  const failures = [];
  const checks = [];

  if (!frozen?.brands?.length) {
    return { pass: false, failures: ["frozen_baseline_missing"], checks: [] };
  }

  const expectedSlugs = new Set(frozen.brands.map((b) => b.slug));
  const expectedBySlug = new Map(frozen.brands.map((b) => [b.slug, b]));

  // Universe SoT / count
  if (liveUniverse?.source?.name !== ACTIVE_UNIVERSE_SOURCE.name) {
    failures.push("active_universe_source_mismatch");
  }
  if (liveUniverse?.totalCount !== EXPECTED_ACTIVE_COUNT) {
    failures.push(
      `active_universe_count_changed:${liveUniverse?.totalCount}_expected_${EXPECTED_ACTIVE_COUNT}`
    );
  }
  if (PRIMARY_RELEASE_SLUGS.length === liveUniverse?.totalCount) {
    failures.push("stale_primary_release_list_used_as_universe");
  }
  if (liveUniverse?.totalCount === 23) {
    failures.push("stale_23_brand_list_suspected_as_universe");
  }

  const liveSlugs = new Set((liveUniverse?.brands || []).map((b) => b.slug));
  for (const slug of expectedSlugs) {
    if (!liveSlugs.has(slug)) failures.push(`missing_active_brand:${slug}`);
  }
  for (const slug of liveSlugs) {
    if (!expectedSlugs.has(slug)) failures.push(`unexpected_active_brand:${slug}`);
  }

  // Exclusions
  for (const ex of liveExcluded.length ? liveExcluded : frozen.excludedNonActive || []) {
    if (ex.isActiveLive === true || isBrandStatusActive(ex.brandStatus)) {
      failures.push(`excluded_brand_became_active_without_baseline_revision:${ex.slug}`);
    }
    if (liveSlugs.has(ex.slug)) {
      failures.push(`excluded_brand_present_in_active_universe:${ex.slug}`);
    }
  }

  const pvqlBySlug = new Map((livePvql?.brands || []).map((b) => [b.slug, b]));
  const qualityBySlug = new Map((liveQuality?.brandResults || []).map((b) => [b.slug, b]));
  const crossBrand =
    (liveQuality?.crossBrandImageIssues || livePvql?.crossBrandImageIssues || []).length || 0;
  if (crossBrand > 0) failures.push(`cross_brand_image_reuse:${crossBrand}`);

  for (const frozenBrand of frozen.brands) {
    const slug = frozenBrand.slug;
    const liveU = (liveUniverse?.brands || []).find((b) => b.slug === slug);
    const p = pvqlBySlug.get(slug);
    const q = qualityBySlug.get(slug);
    const brandFailures = [];

    if (!liveU) brandFailures.push("not_in_live_universe");
    if (liveU && !isBrandStatusActive(liveU.status)) {
      brandFailures.push(`brand_status_not_active_live:${liveU.status}`);
    }
    if (liveU && nz(liveU.status) !== nz(frozenBrand.brandStatus)) {
      brandFailures.push(
        `brand_status_changed:${frozenBrand.brandStatus}->${liveU.status}`
      );
    }

    const shouldFull =
      liveU?.publicFull === true ||
      p?.shouldRenderFullProfile === true ||
      q?.shouldRenderFullProfile === true;
    if (!shouldFull) brandFailures.push("shouldRenderFullProfile_false");
    if (p && p.publicFullProfile !== true) brandFailures.push("not_public_full");
    if (p && p.lockPass !== true) brandFailures.push(`pvql_fail:${(p.failures || []).join("|")}`);

    const rec = q?.overallRecommendation;
    if (rec && rec !== EXPECTED_QUALITY_RECOMMENDATION) {
      brandFailures.push(`quality_recommendation:${rec}`);
    }
    if (rec === "remediation_required") brandFailures.push("remediation_required");
    if ((q?.scores?.blockerCount || 0) > 0) brandFailures.push("blocker_present");

    const scenarioDistinct =
      q?.gates?.scenarioDistinct ?? p?.evidence?.scenarioDistinct ?? p?.gateResults?.scenario_image_distinctiveness?.scenarioDistinctCount;
    if (scenarioDistinct != null && scenarioDistinct < 3) {
      brandFailures.push(`scenario_images_repetitive_or_thin:${scenarioDistinct}`);
    }

    const rawHits = p?.gateResults?.raw_url_scan?.hits?.length ?? 0;
    const forbiddenHits = p?.gateResults?.forbidden_owner_facing_language?.hits?.length ?? 0;
    const genericHits = p?.gateResults?.generic_copy_scan?.mechanicalHits?.length ?? 0;
    if (rawHits > 0) brandFailures.push(`raw_url_scan:${rawHits}`);
    if (forbiddenHits > 0) brandFailures.push(`forbidden_owner_facing_language:${forbiddenHits}`);
    if (genericHits > 0) brandFailures.push(`generic_copy_scan:${genericHits}`);

    const liveCv =
      p?.companyValidated === true ||
      liveU?.brandApi?.companyValidated === true ||
      liveU?.brandApi?.governance?.companyValidated === true;
    if (Boolean(liveCv) !== Boolean(frozenBrand.companyValidated)) {
      brandFailures.push(
        `company_validated_changed:${frozenBrand.companyValidated}->${Boolean(liveCv)}`
      );
    }

    // Source Library / Registry: freeze contract is untouched; fail if PVQL/report asserts drift flags when present
    if (p?.sourceLibraryChanged === true) brandFailures.push("source_library_status_changed");
    if (p?.registryChanged === true) brandFailures.push("registry_status_changed");

    const pass = brandFailures.length === 0;
    checks.push({
      slug,
      pass,
      failures: brandFailures,
      liveBrandStatus: liveU?.status || null,
      pvqlPass: p?.lockPass === true,
      qualityRecommendation: rec || null,
    });
    for (const f of brandFailures) failures.push(`${slug}:${f}`);
  }

  // Quality report must cover all 24 as freeze when provided
  if (liveQuality?.brandResults) {
    const freezeCount = liveQuality.brandResults.filter(
      (b) => b.overallRecommendation === EXPECTED_QUALITY_RECOMMENDATION
    ).length;
    if (freezeCount !== EXPECTED_ACTIVE_COUNT) {
      failures.push(`quality_freeze_count:${freezeCount}_expected_${EXPECTED_ACTIVE_COUNT}`);
    }
  } else {
    failures.push("live_quality_audit_missing");
  }

  if (!livePvql?.brands?.length) {
    failures.push("live_pvql_missing");
  } else {
    const publicFullPass = (livePvql.brands || []).filter(
      (b) => b.publicFullProfile && b.lockPass
    ).length;
    if (publicFullPass < EXPECTED_ACTIVE_COUNT) {
      failures.push(`pvql_public_full_pass_count:${publicFullPass}`);
    }
  }

  return {
    pass: failures.length === 0,
    failures: [...new Set(failures)],
    checks,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT,
    liveActiveCount: liveUniverse?.totalCount ?? null,
  };
}

/**
 * Run freeze snapshot (write reports only — no Airtable).
 */
export async function run24ActivePublicFullBaselineFreeze() {
  const report = await build24ActivePublicFullBaseline({ requireReports: true });
  return report;
}

/**
 * Live regression against committed freeze artifact.
 * Reuses latest quality audit on disk. Reassesses PVQL live unless a fresh
 * PVQL report already covers all frozen slugs (within maxPvqlAgeMs).
 */
export async function run24ActivePublicFullBaselineRegression({
  reassessPvql = true,
  requireQualityReport = true,
  maxPvqlAgeMs = 3 * 60 * 60 * 1000,
  forceLivePvql = false,
} = {}) {
  const frozenPath = path.join(REPORTS_DIR, REPORT_JSON);
  if (!fs.existsSync(frozenPath)) {
    throw new Error(`Frozen baseline missing: ${REPORT_JSON}. Run freeze --dry-run first.`);
  }
  const frozen = JSON.parse(fs.readFileSync(frozenPath, "utf8"));

  const liveUniverse = await loadActiveUniverse({ includeDetails: true });

  const liveExcluded = [];
  for (const probe of NON_ACTIVE_STATUS_CONFLICT_PROBES) {
    const fetched = await fetchBrandById(probe.recordId);
    const status = fetched.brand?.status || fetched.brand?.brandStatus || null;
    liveExcluded.push({
      slug: probe.slug,
      brandName: probe.name,
      recordId: probe.recordId,
      brandStatus: status,
      isActiveLive: isBrandStatusActive(status),
    });
  }

  let livePvql = null;
  let pvqlSource = "live";
  const existingPvql = readJsonReport(PVQL_JSON);
  const frozenSlugs = new Set((frozen.brands || []).map((b) => b.slug));
  const pvqlCoversFrozen =
    existingPvql?.brands?.length >= EXPECTED_ACTIVE_COUNT &&
    [...frozenSlugs].every((slug) => {
      const row = (existingPvql.brands || []).find((b) => b.slug === slug);
      return row && row.publicFullProfile === true;
    });
  const pvqlAgeMs = existingPvql?.generatedAt
    ? Date.now() - new Date(existingPvql.generatedAt).getTime()
    : Number.POSITIVE_INFINITY;

  if (
    !forceLivePvql &&
    reassessPvql &&
    pvqlCoversFrozen &&
    pvqlAgeMs >= 0 &&
    pvqlAgeMs <= maxPvqlAgeMs
  ) {
    livePvql = existingPvql;
    pvqlSource = "fresh_report";
  } else if (reassessPvql) {
    const slugs = liveUniverse.brands.map((b) => b.slug);
    livePvql = await runPublicVisibilityQualityLock({ slugs });
    pvqlSource = "live";
  } else {
    livePvql = existingPvql;
    pvqlSource = "report_forced";
  }

  const liveQuality = requireQualityReport ? readJsonReport(QUALITY_AUDIT_JSON) : null;

  const regression = evaluate24ActivePublicFullBaselineRegression({
    frozen,
    liveUniverse,
    livePvql,
    liveQuality,
    liveExcluded,
  });

  return {
    version: BASELINE_VERSION,
    generatedAt: new Date().toISOString(),
    writePerformed: false,
    frozenDecision: frozen.freezeDecision,
    pvqlSource,
    regression,
    liveUniverseCount: liveUniverse.totalCount,
    liveExcluded,
  };
}

export function write24ActivePublicFullBaselineReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD);
  const docsPath = path.join(DOCS_DIR, DOCS_MD);

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const md = renderBaselineMarkdown(report);
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");

  return { jsonPath, mdPath, docsPath };
}

function renderBaselineMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer — Protected 24 Active/Live Public-Full Baseline`);
  lines.push("");
  lines.push(`Version: \`${report.version}\` · Generated: ${report.generatedAt}`);
  lines.push(`Freeze decision: **${report.freezeDecision}** · frozen=${report.frozen}`);
  lines.push(`Writes: Airtable=${report.airtableWrites} · Presentation=${report.presentationWrites} · Image=${report.imageWrites} · CV=${report.companyValidatedWrites} · Source=${report.sourceLibraryWrites} · Registry=${report.registryWrites} · Brand Status=${report.brandStatusWrites}`);
  lines.push("");

  lines.push(`## 1. Executive summary`);
  lines.push("");
  lines.push(
    `This freeze locks the **${report.activeCount}** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, and \`approve_for_baseline_freeze\`.`
  );
  lines.push("");
  lines.push(`| Metric | Value |`);
  lines.push(`|--------|-------|`);
  lines.push(`| Active/Live count | ${report.summary.activeCount} |`);
  lines.push(`| Public-full | ${report.summary.publicFullCount} |`);
  lines.push(`| shouldRenderFullProfile | ${report.summary.shouldRenderFullProfileCount} |`);
  lines.push(`| PVQL pass | ${report.summary.pvqlPassCount} |`);
  lines.push(`| approve_for_baseline_freeze | ${report.summary.freezeRecommendationCount} |`);
  lines.push(`| remediation_required | ${report.summary.remediationCount} |`);
  lines.push(`| Cross-brand image reuse | ${report.summary.crossBrandImageReuse} |`);
  lines.push(`| Company Validated = true | ${report.summary.companyValidatedTrueCount} |`);
  lines.push(`| Excluded non-active | ${report.summary.excludedNonActiveCount} |`);
  lines.push("");

  lines.push(`## 2. Active universe source of truth`);
  lines.push("");
  lines.push(`- **Name:** ${report.activeUniverseSource.name}`);
  lines.push(`- **Table:** ${report.activeUniverseSource.table}`);
  lines.push(`- **Formula:** \`${report.activeUniverseSource.formula}\``);
  lines.push(`- **Loader:** \`lib/partner-intelligence/brand-explorer-active-universe.js\``);
  lines.push(`- **Version:** ${report.activeUniverseVersion}`);
  lines.push(`- **Not the universe:** ${report.staleOperationalListsRejectedAsUniverse.join(", ")}`);
  lines.push(`- **Note:** ${report.primaryReleaseNote}`);
  lines.push("");

  lines.push(`## 3. 24-brand baseline table`);
  lines.push("");
  lines.push(
    `| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | OS | Gallery | Scenario | Property | Rows | CV |`
  );
  lines.push(`|-------|------|-----------|--------|------|---------|------|---------|----|---------|----------|----------|------|----|`);
  for (const b of report.brands) {
    lines.push(
      `| ${b.brandName} | \`${b.slug}\` | \`${b.recordId}\` | ${b.brandStatus} | ${b.shouldRenderFullProfile} | ${b.publicDisplayState || "—"} | ${b.pvqlStatus} | ${b.qualityRecommendation || "—"} | ${b.osState || "—"} | ${b.galleryImageCount} | ${b.scenarioImageCount} | ${b.propertyExampleImageCount} | ${b.presentationRowCount ?? "—"} | ${b.companyValidated} |`
    );
  }
  lines.push("");

  lines.push(`## 4. Validation results`);
  lines.push("");
  const vs = report.validationSources || {};
  lines.push(`- Quality audit: ${vs.qualityAudit?.file || "—"} (${vs.qualityAudit?.decision || "—"})`);
  lines.push(`- PVQL: ${vs.pvql?.file || "—"} (publicFull=${vs.pvql?.publicFull ?? "—"})`);
  lines.push(`- Image audit: ${vs.imageAudit?.file || "—"} (crossBrand=${vs.imageAudit?.crossBrand ?? "—"})`);
  lines.push(`- OS: ${vs.os?.file || "—"}`);
  lines.push("");

  lines.push(`## 5. Excluded non-active brands`);
  lines.push("");
  lines.push(`These brands are **explicitly excluded** because they are not Active/Live:`);
  lines.push("");
  lines.push(`| Brand | Slug | Record ID | Brand Status | Included |`);
  lines.push(`|-------|------|-----------|--------------|----------|`);
  for (const e of report.excludedNonActive || []) {
    lines.push(
      `| ${e.brandName} | \`${e.slug}\` | \`${e.recordId}\` | ${e.brandStatus || "—"} | ${e.includedInBaseline} |`
    );
  }
  lines.push("");
  lines.push(`Reason: ${report.excludedNonActive?.[0]?.excludedBecause || "Not Active/Live"}`);
  lines.push("");

  lines.push(`## 6. Protected fields`);
  lines.push("");
  for (const f of report.protectedFields || []) lines.push(`- ${f}`);
  lines.push("");
  lines.push(`Baseline freeze does **not** write any of these fields.`);
  lines.push("");

  lines.push(`## 7. Regression rules`);
  lines.push("");
  for (const r of report.regressionRules || []) lines.push(`- ${r}`);
  lines.push("");
  lines.push(`Test: \`npm run test:brand-explorer-24-active-public-full-baseline\``);
  lines.push("");

  lines.push(`## 8. Rollback notes`);
  lines.push("");
  for (const r of report.rollbackNotes || []) lines.push(`- ${r}`);
  lines.push("");

  lines.push(`## 9. Future-work rules`);
  lines.push("");
  for (const r of report.futureWorkRules || []) lines.push(`- ${r}`);
  lines.push("");

  lines.push(`## Commands`);
  lines.push("");
  lines.push("```bash");
  lines.push("npm run brand-explorer-24-active-public-full-baseline -- --dry-run");
  lines.push("npm run test:brand-explorer-24-active-public-full-baseline");
  lines.push("```");
  lines.push("");

  return lines.join("\n");
}
