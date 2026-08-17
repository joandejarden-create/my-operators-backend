/**
 * Brand Explorer — protected 27 Active/Live public-full baseline freeze.
 *
 * Supersedes interim Active/Live-only freeze (v1 / interim artifact).
 * Read-only. No Airtable / Presentation / image / CV / Source / Registry /
 * Brand Status / release writes.
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
import { resolveLane2BrandIdentity } from "./brand-explorer-lane2-common.js";
import { LANE2_PROPERTY_CATALOG_BY_SLUG } from "./brand-explorer-lane2-property-catalog.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import {
  evaluateRecentMomentumEvidenceQuality,
  MOMENTUM_EVIDENCE_TARGET_SLUGS,
} from "./brand-explorer-recent-momentum-evidence-quality.js";
import { CALA_AVAILABLE_BY_SLUG } from "./brand-explorer-27-recent-momentum-evidence-fix-content.js";
import {
  BASELINE_VERSION as BASELINE_VERSION_24,
  EXPECTED_ACTIVE_COUNT as EXPECTED_ACTIVE_COUNT_24,
  PROTECTED_FIELDS,
  REPORT_JSON as REPORT_JSON_24,
  ROOT,
  STALE_OPERATIONAL_LIST_NAMES,
} from "./brand-explorer-24-active-public-full-baseline.js";
import {
  BASELINE_VERSION_25,
  EXPECTED_ACTIVE_COUNT_25,
  REPORT_JSON_25,
} from "./brand-explorer-25-active-public-full-baseline.js";

export const BASELINE_VERSION_27 = "27-active-public-full-baseline-v2";
export const EXPECTED_ACTIVE_COUNT_27 = 27;
export const EXPECTED_QUALITY_RECOMMENDATION = "approve_for_baseline_freeze";

export const REPORT_JSON_27 = "brand-explorer-27-active-public-full-baseline.json";
export const REPORT_MD_27 = "brand-explorer-27-active-public-full-baseline.md";
export const DOCS_MD_27 = "brand-explorer-27-active-public-full-baseline.md";
export const INTERIM_REPORT_JSON_27 = "brand-explorer-27-active-universe-interim-baseline.json";

export const BASELINE_27_NEW_WAVE = Object.freeze([
  Object.freeze({
    slug: "tapestry-collection-by-hilton",
    recordId: "reccXxMHEh7NNRhIE",
    name: "Tapestry Collection by Hilton",
    wave: 25,
  }),
  Object.freeze({
    slug: "dazzler-by-wyndham",
    recordId: "rec5CNMM4ZUD7ZHlM",
    name: "Dazzler by Wyndham",
    wave: 27,
  }),
  Object.freeze({
    slug: "trademark-collection-by-wyndham",
    recordId: "recob7tgHRryRSbeO",
    name: "Trademark Collection by Wyndham",
    wave: 27,
  }),
]);

const NEW_WAVE_SLUGS = new Set(BASELINE_27_NEW_WAVE.map((b) => b.slug));

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const QUALITY_AUDIT_JSON = "brand-explorer-24-tab-section-quality-audit.json";
const IMAGE_AUDIT_JSON = "brand-explorer-24-image-repetition-audit.json";
const PVQL_JSON = "brand-explorer-public-visibility-quality-lock.json";
const OS_JSON = "brand-explorer-v41-os-consolidation.json";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

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
    });
  }
  for (const row of osReport?.summaryRows || []) {
    const slug = nz(row.brandSlug || row.slug);
    if (!slug || map.has(slug)) continue;
    map.set(slug, {
      canonicalState: row.canonicalState || null,
      nextAction: row.nextAction || null,
    });
  }
  return map;
}

async function listPresentationRows(brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return [];
  const formula = `{Brand Name}='${String(brandName).replace(/'/g, "\\'")}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `list failed ${res.status}`);
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      rows.push({
        recordId: rec.id,
        slotKey: String(f["Slot Key"] || "").trim(),
        title: String(f.Title || "").trim(),
        body: String(f.Body || "").trim(),
        caseSummaryOverview: String(f["Case Summary Overview"] || "").trim(),
        caseSummaryTags: String(f["Case Summary Tags"] || "").trim(),
        caseSummaryBrandRelevance: String(f["Case Summary Brand Relevance"] || "").trim(),
        externalDisplayStatus: String(f["External Display Status"] || "").trim(),
        active: f.Active !== false,
      });
    }
    offset = json.offset || "";
  } while (offset);
  return rows;
}

async function evaluateEvidenceForSlug(slug) {
  const identity = resolveLane2BrandIdentity(slug);
  const rows = await listPresentationRows(identity.name);
  const fetched = await fetchBrandById(identity.recordId || slug);
  const brand = fetched.brand;
  if (!brand) {
    return {
      pass: false,
      status: "fail",
      failCount: 1,
      failures: [{ id: "brand_api_missing" }],
      openingsRegionalPriorityStatus: "unknown",
    };
  }
  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });
  const result = evaluateRecentMomentumEvidenceQuality({
    brandSlug: slug,
    brandName: identity.name,
    presentationRows: rows,
    html,
    propertyCatalog: LANE2_PROPERTY_CATALOG_BY_SLUG[slug] || [],
    calaAvailableOverride: CALA_AVAILABLE_BY_SLUG[slug],
  });
  const openingsIssues = (result.openingsCards || []).flatMap((c) => c.issues || []);
  const openingsOk = openingsIssues.length === 0;
  return {
    pass: result.pass === true,
    status: result.pass ? "pass" : "fail",
    failCount: (result.failures || []).length,
    failures: (result.failures || []).slice(0, 12),
    calaAvailable: result.calaAvailable === true,
    momentumCards: result.summary?.momentumCards ?? null,
    openingsCards: result.summary?.openingsCards ?? null,
    openingsRegionalPriorityStatus: openingsOk
      ? result.calaAvailable
        ? "cala_first_ok"
        : "international_reference_labeled"
      : "fail",
  };
}

/**
 * Build protected 27 public-full baseline snapshot (read-only).
 */
export async function build27ActivePublicFullBaseline({
  requireReports = true,
  evaluateEvidence = true,
} = {}) {
  const universe = await loadActiveUniverse({ includeDetails: true });
  const quality = readJsonReport(QUALITY_AUDIT_JSON);
  const imageAudit = readJsonReport(IMAGE_AUDIT_JSON);
  const pvql = readJsonReport(PVQL_JSON);
  const osReport = readJsonReport(OS_JSON);
  const interim = readJsonReport(INTERIM_REPORT_JSON_27);
  const frozen24 = readJsonReport(REPORT_JSON_24);
  const frozen25 = readJsonReport(REPORT_JSON_25);

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

  const evidenceBySlug = new Map();
  if (evaluateEvidence) {
    for (const slug of MOMENTUM_EVIDENCE_TARGET_SLUGS) {
      evidenceBySlug.set(slug, await evaluateEvidenceForSlug(slug));
    }
  }

  const brands = [];
  for (const u of universe.brands) {
    const q = qualityBySlug.get(u.slug) || {};
    const p = pvqlBySlug.get(u.slug) || {};
    const img = imageBySlug.get(u.slug) || {};
    const os = osBySlug.get(u.slug) || null;
    const brandApi = u.brandApi || {};
    const evidence = evidenceBySlug.get(u.slug) || null;

    const companyValidated =
      p.companyValidated === true ||
      brandApi.companyValidated === true ||
      brandApi.governance?.companyValidated === true
        ? true
        : false;
    const companyValidationDate =
      brandApi.companyValidationDate || brandApi.governance?.companyValidationDate || null;

    const gateResults = p.gateResults || {};
    const scenarioDistinct = q.gates?.scenarioDistinct ?? p.evidence?.scenarioDistinct ?? 0;
    const galleryDistinct = q.gates?.galleryDistinct ?? p.evidence?.galleryDistinct ?? 0;
    const propertyDistinct = q.gates?.propertyDistinct ?? p.evidence?.propertyDistinct ?? 0;
    const repeatedScenario =
      (img.imageFindings || []).some((f) =>
        /repeated_visual_role/i.test(f.finding || f.issueType || "")
      ) || scenarioDistinct < 3;

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
      imageRoleMatchStatus: q.gates?.imageRoleMatch || "reported_in_quality_audit",
      crossBrandImageReuseStatus: "none",
      recentMomentumEvidenceQualityStatus: evidence
        ? evidence.status
        : NEW_WAVE_SLUGS.has(u.slug)
          ? "not_evaluated"
          : "n_a_protected_prior",
      openingsRegionalPriorityStatus: evidence
        ? evidence.openingsRegionalPriorityStatus
        : NEW_WAVE_SLUGS.has(u.slug)
          ? "not_evaluated"
          : "n_a_protected_prior",
      evidenceQualityDetail: evidence
        ? {
            pass: evidence.pass,
            failCount: evidence.failCount,
            calaAvailable: evidence.calaAvailable,
            momentumCards: evidence.momentumCards,
            openingsCards: evidence.openingsCards,
          }
        : null,
      companyValidated,
      companyValidationDate,
      sourceLibraryStatus: "untouched_at_freeze",
      registryStatus: "untouched_at_freeze",
      lastValidationTimestamp: validatedAt,
      addedInNewWave: NEW_WAVE_SLUGS.has(u.slug),
      notes: NEW_WAVE_SLUGS.has(u.slug)
        ? ["new_wave_public_full_baseline_27"]
        : ["carried_from_protected_prior_baseline"],
      gateHits: {
        rawUrlScanHits: gateResults.raw_url_scan?.hits?.length ?? 0,
        forbiddenOwnerFacingHits: gateResults.forbidden_owner_facing_language?.hits?.length ?? 0,
        genericCopyMechanicalHits: gateResults.generic_copy_scan?.mechanicalHits?.length ?? 0,
        genericCopyGoldenFailures: gateResults.generic_copy_scan?.goldenFailures?.length ?? 0,
      },
    });
  }

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

  const evidenceWavePass = MOMENTUM_EVIDENCE_TARGET_SLUGS.every((slug) => {
    const e = evidenceBySlug.get(slug);
    return e && e.pass === true;
  });

  const freezeDecision =
    universe.totalCount === EXPECTED_ACTIVE_COUNT_27 &&
    brands.length === EXPECTED_ACTIVE_COUNT_27 &&
    brands.every((b) => b.shouldRenderFullProfile && b.publicFullProfile) &&
    brands.every((b) => b.pvqlStatus === "pass") &&
    brands.every((b) => b.qualityRecommendation === EXPECTED_QUALITY_RECOMMENDATION) &&
    brands.every((b) => (b.blockerCount ?? 0) === 0) &&
    brands.every((b) => b.scenarioImageCount >= 3) &&
    brands.every((b) => (b.gateHits?.rawUrlScanHits || 0) === 0) &&
    brands.every((b) => (b.gateHits?.forbiddenOwnerFacingHits || 0) === 0) &&
    brands.every((b) => (b.gateHits?.genericCopyMechanicalHits || 0) === 0) &&
    crossBrandCount === 0 &&
    evidenceWavePass &&
    excluded.every((e) => e.includedInBaseline === false && e.isActiveLive !== true)
      ? "frozen_27_active_public_full_baseline"
      : "freeze_incomplete";

  const newWave = BASELINE_27_NEW_WAVE.map((meta) => {
    const row = brands.find((b) => b.slug === meta.slug);
    return {
      ...meta,
      brandStatus: row?.brandStatus || null,
      included: Boolean(row),
      shouldRenderFullProfile: row?.shouldRenderFullProfile === true,
      publicFullProfile: row?.publicFullProfile === true,
      pvqlStatus: row?.pvqlStatus || null,
      qualityRecommendation: row?.qualityRecommendation || null,
      recentMomentumEvidenceQualityStatus: row?.recentMomentumEvidenceQualityStatus || null,
      openingsRegionalPriorityStatus: row?.openingsRegionalPriorityStatus || null,
      presentationRowCount: row?.presentationRowCount ?? 0,
    };
  });

  return {
    version: BASELINE_VERSION_27,
    baselineType: "active_live_public_full",
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
    predecessorBaselines: {
      baseline24: {
        version: BASELINE_VERSION_24,
        expectedCount: EXPECTED_ACTIVE_COUNT_24,
        report: REPORT_JSON_24,
        present: Boolean(frozen24),
        role: "historical_public_full",
      },
      baseline25: {
        version: BASELINE_VERSION_25,
        expectedCount: EXPECTED_ACTIVE_COUNT_25,
        report: REPORT_JSON_25,
        present: Boolean(frozen25),
        role: "historical_tapestry_wave",
      },
      interim27ActiveLiveOnly: {
        version: interim?.version || "27-active-public-full-baseline-v1",
        report: INTERIM_REPORT_JSON_27,
        present: Boolean(interim),
        role: "historical_active_live_universe_not_yet_public_full",
        note: "Preserved as history. Not current enforcement.",
      },
    },
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    activeUniverseVersion: ACTIVE_UNIVERSE_VERSION,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_27,
    activeCount: universe.totalCount,
    frozen: freezeDecision === "frozen_27_active_public_full_baseline",
    freezeDecision,
    protectedFields: [...PROTECTED_FIELDS],
    staleOperationalListsRejectedAsUniverse: [...STALE_OPERATIONAL_LIST_NAMES],
    primaryReleaseOverlayCount: PRIMARY_RELEASE_SLUGS.length,
    primaryReleaseNote:
      "PRIMARY_RELEASE_SLUGS is an operational overlay, not the Active/Live universe (27).",
    brands,
    newWaveBrands: newWave,
    excludedNonActive: excluded,
    evidenceQuality: {
      gate: "test:brand-explorer-recent-momentum-evidence-quality",
      targetSlugs: [...MOMENTUM_EVIDENCE_TARGET_SLUGS],
      pass: evidenceWavePass,
      bySlug: Object.fromEntries([...evidenceBySlug.entries()]),
    },
    validationSources: {
      qualityAudit: quality
        ? {
            file: QUALITY_AUDIT_JSON,
            generatedAt: quality.generatedAt,
            decision: quality.baselineFreezeDecision,
          }
        : null,
      pvql: pvql
        ? {
            file: PVQL_JSON,
            generatedAt: pvql.generatedAt,
            publicFull: pvql.summary?.publicFullProfileCount,
          }
        : null,
      imageAudit: imageAudit
        ? { file: IMAGE_AUDIT_JSON, generatedAt: imageAudit.generatedAt, crossBrand: crossBrandCount }
        : null,
      os: osReport ? { file: OS_JSON, generatedAt: osReport.generatedAt || null } : null,
      recentMomentumEvidenceQuality: {
        evaluated: evaluateEvidence,
        pass: evidenceWavePass,
      },
    },
    summary: {
      activeCount: universe.totalCount,
      publicFullCount: brands.filter((b) => b.publicFullProfile).length,
      shouldRenderFullProfileCount: brands.filter((b) => b.shouldRenderFullProfile).length,
      pvqlPassCount: brands.filter((b) => b.pvqlStatus === "pass").length,
      freezeRecommendationCount: brands.filter(
        (b) => b.qualityRecommendation === EXPECTED_QUALITY_RECOMMENDATION
      ).length,
      remediationCount: brands.filter((b) => b.qualityRecommendation === "remediation_required")
        .length,
      blockerBrandCount: brands.filter((b) => (b.blockerCount ?? 0) > 0).length,
      crossBrandImageReuse: crossBrandCount,
      scenarioRepetitionBrandCount: brands.filter((b) => b.imageRepetitionStatus !== "ok").length,
      excludedNonActiveCount: excluded.length,
      companyValidatedTrueCount: brands.filter((b) => b.companyValidated === true).length,
      evidenceQualityPass: evidenceWavePass,
      newWaveCount: newWave.length,
    },
    regressionRules: [
      "Active/Live universe count must remain 27 unless freeze is explicitly revised",
      "Every Active/Live brand must remain public-full with shouldRenderFullProfile=true",
      "Every Active/Live brand must pass PVQL",
      "Every Active/Live brand must remain approve_for_baseline_freeze",
      "No blocker or remediation_required on Active/Live brands",
      "No cross-brand image reuse",
      "Value scenario images must remain distinct (scenarioDistinct ≥ 3)",
      "raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0",
      "Recent Momentum / Openings Evidence Quality must pass for Tapestry, Dazzler, Trademark",
      "Openings property URLs must match property-distinctive title tokens; CALA-first or International Reference labels required",
      "Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly",
      "Radisson Collection must remain excluded unless Brand Status promoted to Active/Live",
      "Stale 24/23-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT",
    ],
    rollbackNotes: [
      "This freeze is report-only — no Airtable writes occurred.",
      "Interim Active/Live-only freeze preserved at reports/brand-explorer-27-active-universe-interim-baseline.json",
      "Historical 24/25 public-full freezes remain as predecessor artifacts.",
      "To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT_27 after an explicit founder decision.",
      "Do not revert Brand Status / CV / Source / Registry to undo this freeze — those fields were never written.",
    ],
    futureWorkRules: [
      "New Active/Live brands require a new baseline revision (count will leave 27).",
      "Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.",
      "Required gates: test:brand-explorer-27-active-public-full-baseline · test:brand-explorer-public-visibility-quality-lock --public-full-only · test:brand-explorer-recent-momentum-evidence-quality",
      "Promoting Radisson Collection requires Brand Status → Active/Live first, then Tab Factory + public-full + PVQL + evidence + quality freeze path.",
      "Operational cohorts (PRIMARY_RELEASE, restore lanes, factory preview) remain overlays, not universe SoT.",
    ],
  };
}

export function evaluate27ActivePublicFullBaselineRegression({
  frozen,
  liveUniverse,
  livePvql,
  liveQuality,
  liveExcluded = [],
  liveEvidenceBySlug = new Map(),
} = {}) {
  const failures = [];
  const checks = [];

  if (!frozen?.brands?.length) {
    return { pass: false, failures: ["frozen_baseline_missing"], checks: [] };
  }
  if (frozen.baselineType && frozen.baselineType !== "active_live_public_full") {
    failures.push(`frozen_baseline_type_not_public_full:${frozen.baselineType}`);
  }

  const expectedSlugs = new Set(frozen.brands.map((b) => b.slug));

  if (liveUniverse?.source?.name !== ACTIVE_UNIVERSE_SOURCE.name) {
    failures.push("active_universe_source_mismatch");
  }
  if (liveUniverse?.totalCount !== EXPECTED_ACTIVE_COUNT_27) {
    failures.push(
      `active_universe_count_changed:${liveUniverse?.totalCount}_expected_${EXPECTED_ACTIVE_COUNT_27}`
    );
  }
  if (PRIMARY_RELEASE_SLUGS.length === liveUniverse?.totalCount) {
    failures.push("stale_primary_release_list_used_as_universe");
  }
  if (liveUniverse?.totalCount === 23 || liveUniverse?.totalCount === 24) {
    failures.push(`stale_${liveUniverse.totalCount}_brand_list_suspected_as_universe`);
  }

  const liveSlugs = new Set((liveUniverse?.brands || []).map((b) => b.slug));
  for (const slug of expectedSlugs) {
    if (!liveSlugs.has(slug)) failures.push(`missing_active_brand:${slug}`);
  }
  for (const slug of liveSlugs) {
    if (!expectedSlugs.has(slug)) failures.push(`unexpected_active_brand:${slug}`);
  }

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
      brandFailures.push(`brand_status_changed:${frozenBrand.brandStatus}->${liveU.status}`);
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
      q?.gates?.scenarioDistinct ??
      p?.evidence?.scenarioDistinct ??
      p?.gateResults?.scenario_image_distinctiveness?.scenarioDistinctCount;
    if (scenarioDistinct != null && scenarioDistinct < 3) {
      brandFailures.push(`scenario_images_repetitive_or_thin:${scenarioDistinct}`);
    }

    const rawHits = p?.gateResults?.raw_url_scan?.hits?.length ?? 0;
    const forbiddenHits = p?.gateResults?.forbidden_owner_facing_language?.hits?.length ?? 0;
    const genericGate = p?.gateResults?.generic_copy_scan;
    const genericHits = genericGate?.mechanicalHits?.length ?? 0;
    const tabFactoryGate = p?.gateResults?.tab_factory_audit;
    if (rawHits > 0) brandFailures.push(`raw_url_scan:${rawHits}`);
    if (forbiddenHits > 0) brandFailures.push(`forbidden_owner_facing_language:${forbiddenHits}`);
    if (genericGate && genericGate.pass !== true) {
      const goldenBits = (genericGate.goldenFailures || []).join("|");
      brandFailures.push(
        `generic_copy_scan:${goldenBits || `mechanical=${genericHits}` || "fail"}`
      );
    } else if (genericHits > 0) {
      brandFailures.push(`generic_copy_scan:${genericHits}`);
    }
    if (tabFactoryGate && tabFactoryGate.pass !== true) {
      brandFailures.push(`tab_factory_audit:failFindings=${tabFactoryGate.failFindings ?? "?"}`);
    }

    const liveCv =
      p?.companyValidated === true ||
      liveU?.brandApi?.companyValidated === true ||
      liveU?.brandApi?.governance?.companyValidated === true;
    if (Boolean(liveCv) !== Boolean(frozenBrand.companyValidated)) {
      brandFailures.push(
        `company_validated_changed:${frozenBrand.companyValidated}->${Boolean(liveCv)}`
      );
    }
    if (p?.sourceLibraryChanged === true) brandFailures.push("source_library_status_changed");
    if (p?.registryChanged === true) brandFailures.push("registry_status_changed");

    if (MOMENTUM_EVIDENCE_TARGET_SLUGS.includes(slug)) {
      const ev = liveEvidenceBySlug.get(slug);
      if (!ev) brandFailures.push("recent_momentum_evidence_quality_not_evaluated");
      else if (ev.pass !== true) {
        brandFailures.push(
          `recent_momentum_evidence_quality_fail:${(ev.failures || [])
            .map((f) => f.id)
            .slice(0, 5)
            .join("|")}`
        );
      }
    }

    const pass = brandFailures.length === 0;
    checks.push({
      slug,
      pass,
      failures: brandFailures,
      liveBrandStatus: liveU?.status || null,
      pvqlPass: p?.lockPass === true,
      qualityRecommendation: rec || null,
      evidencePass: liveEvidenceBySlug.get(slug)?.pass ?? null,
    });
    for (const f of brandFailures) failures.push(`${slug}:${f}`);
  }

  if (liveQuality?.brandResults) {
    const freezeCount = liveQuality.brandResults.filter(
      (b) => b.overallRecommendation === EXPECTED_QUALITY_RECOMMENDATION
    ).length;
    if (freezeCount !== EXPECTED_ACTIVE_COUNT_27) {
      failures.push(`quality_freeze_count:${freezeCount}_expected_${EXPECTED_ACTIVE_COUNT_27}`);
    }
  } else {
    failures.push("live_quality_audit_missing");
  }

  if (!livePvql?.brands?.length) {
    failures.push("live_pvql_missing");
  } else {
    if (livePvql.summary?.overallPass !== true) {
      failures.push(
        `pvql_overall_pass_false:${(livePvql.summary?.hardFails || []).join("|") || "overallPass_false"}`
      );
    }
    const publicFullCount =
      livePvql.summary?.publicFullProfileCount ??
      (livePvql.brands || []).filter((b) => b.publicFullProfile).length;
    if (publicFullCount !== EXPECTED_ACTIVE_COUNT_27) {
      failures.push(`pvql_public_full_count:${publicFullCount}_expected_${EXPECTED_ACTIVE_COUNT_27}`);
    }
    const publicFullPass = (livePvql.brands || []).filter(
      (b) => b.publicFullProfile && b.lockPass
    ).length;
    if (publicFullPass !== EXPECTED_ACTIVE_COUNT_27) {
      failures.push(`pvql_public_full_pass_count:${publicFullPass}_expected_${EXPECTED_ACTIVE_COUNT_27}`);
    }
    const tabFactoryFails = (livePvql.brands || []).filter(
      (b) => b.publicFullProfile && b.gateResults?.tab_factory_audit?.pass !== true
    );
    if (tabFactoryFails.length) {
      failures.push(
        `pvql_tab_factory_audit_fail:${tabFactoryFails.map((b) => b.slug).join("|")}`
      );
    }
    const genericFails = (livePvql.brands || []).filter(
      (b) => b.publicFullProfile && b.gateResults?.generic_copy_scan?.pass !== true
    );
    if (genericFails.length) {
      failures.push(`pvql_generic_copy_scan_fail:${genericFails.map((b) => b.slug).join("|")}`);
    }
  }

  return {
    pass: failures.length === 0,
    failures: [...new Set(failures)],
    checks,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_27,
    liveActiveCount: liveUniverse?.totalCount ?? null,
  };
}

export async function run27ActivePublicFullBaselineRegression({
  reassessPvql = true,
  requireQualityReport = true,
  maxPvqlAgeMs = 6 * 60 * 60 * 1000,
  forceLivePvql = false,
  allowCachedPvqlIfPass = false,
  evaluateEvidence = true,
} = {}) {
  const frozenPath = path.join(REPORTS_DIR, REPORT_JSON_27);
  if (!fs.existsSync(frozenPath)) {
    throw new Error(`Frozen baseline missing: ${REPORT_JSON_27}. Run freeze --dry-run first.`);
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
    existingPvql?.summary?.overallPass === true &&
    existingPvql?.summary?.publicFullProfileCount === EXPECTED_ACTIVE_COUNT_27 &&
    existingPvql?.brands?.length >= EXPECTED_ACTIVE_COUNT_27 &&
    [...frozenSlugs].every((slug) => {
      const row = (existingPvql.brands || []).find((b) => b.slug === slug);
      return (
        row &&
        row.publicFullProfile === true &&
        row.lockPass === true &&
        row.gateResults?.tab_factory_audit?.pass === true &&
        row.gateResults?.generic_copy_scan?.pass === true &&
        (row.gateResults?.raw_url_scan?.hits?.length || 0) === 0 &&
        (row.gateResults?.forbidden_owner_facing_language?.hits?.length || 0) === 0
      );
    });
  const pvqlAgeMs = existingPvql?.generatedAt
    ? Date.now() - new Date(existingPvql.generatedAt).getTime()
    : Number.POSITIVE_INFINITY;

  // Default: always run a fresh public-full PVQL lock. Cached reuse is opt-in and
  // only allowed when the on-disk report is overallPass + gate-clean for all 27.
  if (!reassessPvql) {
    livePvql = existingPvql;
    pvqlSource = "report_forced";
  } else if (
    !forceLivePvql &&
    allowCachedPvqlIfPass &&
    pvqlCoversFrozen &&
    pvqlAgeMs >= 0 &&
    pvqlAgeMs <= maxPvqlAgeMs
  ) {
    livePvql = existingPvql;
    pvqlSource = "fresh_report";
  } else {
    const slugs = liveUniverse.brands.map((b) => b.slug);
    livePvql = await runPublicVisibilityQualityLock({ slugs });
    pvqlSource = "live";
  }

  const liveQuality = requireQualityReport ? readJsonReport(QUALITY_AUDIT_JSON) : null;

  const liveEvidenceBySlug = new Map();
  if (evaluateEvidence) {
    for (const slug of MOMENTUM_EVIDENCE_TARGET_SLUGS) {
      liveEvidenceBySlug.set(slug, await evaluateEvidenceForSlug(slug));
    }
  }

  const regression = evaluate27ActivePublicFullBaselineRegression({
    frozen,
    liveUniverse,
    livePvql,
    liveQuality,
    liveExcluded,
    liveEvidenceBySlug,
  });

  return {
    version: BASELINE_VERSION_27,
    generatedAt: new Date().toISOString(),
    writePerformed: false,
    frozenDecision: frozen.freezeDecision,
    pvqlSource,
    regression,
    liveUniverseCount: liveUniverse.totalCount,
    liveExcluded,
    evidenceBySlug: Object.fromEntries([...liveEvidenceBySlug.entries()]),
  };
}

export function write27ActivePublicFullBaselineReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON_27);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD_27);
  const docsPath = path.join(DOCS_DIR, DOCS_MD_27);

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  const md = render27BaselineMarkdown(report);
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  return { jsonPath, mdPath, docsPath };
}

function render27BaselineMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer — Protected 27 Active/Live Public-Full Baseline`);
  lines.push("");
  lines.push(`Version: \`${report.version}\` · Generated: ${report.generatedAt}`);
  lines.push(`Baseline type: **${report.baselineType}**`);
  lines.push(`Freeze decision: **${report.freezeDecision}** · frozen=${report.frozen}`);
  lines.push(
    `Writes: Airtable=${report.airtableWrites} · Presentation=${report.presentationWrites} · Image=${report.imageWrites} · CV=${report.companyValidatedWrites} · Source=${report.sourceLibraryWrites} · Registry=${report.registryWrites} · Brand Status=${report.brandStatusWrites}`
  );
  lines.push("");

  lines.push(`## 1. Executive summary`);
  lines.push("");
  lines.push(
    `This freeze locks the **${report.activeCount}** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, quality \`approve_for_baseline_freeze\`, and evidence-quality clean for the new wave.`
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
  lines.push(`| Evidence quality (wave) | ${report.summary.evidenceQualityPass} |`);
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
  lines.push(`### Predecessor freezes (history, not current enforcement)`);
  lines.push("");
  lines.push(
    `- 24 public-full: \`${report.predecessorBaselines?.baseline24?.report}\` (${report.predecessorBaselines?.baseline24?.version})`
  );
  lines.push(
    `- 25 Tapestry wave: \`${report.predecessorBaselines?.baseline25?.report}\` (${report.predecessorBaselines?.baseline25?.version})`
  );
  lines.push(
    `- Interim 27 Active/Live-only (pre public-full): \`${report.predecessorBaselines?.interim27ActiveLiveOnly?.report}\``
  );
  lines.push("");

  lines.push(`## 3. 27-brand baseline table`);
  lines.push("");
  lines.push(
    `| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | Evidence | OS | Gallery | Scenario | Property | Rows | CV |`
  );
  lines.push(
    `|-------|------|-----------|--------|------|---------|------|---------|----------|----|---------|----------|----------|------|----|`
  );
  for (const b of report.brands) {
    lines.push(
      `| ${b.brandName} | \`${b.slug}\` | \`${b.recordId}\` | ${b.brandStatus} | ${b.shouldRenderFullProfile} | ${b.publicDisplayState || "—"} | ${b.pvqlStatus} | ${b.qualityRecommendation || "—"} | ${b.recentMomentumEvidenceQualityStatus || "—"} | ${b.osState || "—"} | ${b.galleryImageCount} | ${b.scenarioImageCount} | ${b.propertyExampleImageCount} | ${b.presentationRowCount ?? "—"} | ${b.companyValidated} |`
    );
  }
  lines.push("");

  lines.push(`## 4. New wave summary`);
  lines.push("");
  lines.push(`| Brand | Slug | Wave | Status | Full | PVQL | Quality | Evidence | Openings region |`);
  lines.push(`|-------|------|------|--------|------|------|---------|----------|-----------------|`);
  for (const b of report.newWaveBrands || []) {
    lines.push(
      `| ${b.name} | \`${b.slug}\` | ${b.wave} | ${b.brandStatus || "—"} | ${b.shouldRenderFullProfile} | ${b.pvqlStatus || "—"} | ${b.qualityRecommendation || "—"} | ${b.recentMomentumEvidenceQualityStatus || "—"} | ${b.openingsRegionalPriorityStatus || "—"} |`
    );
  }
  lines.push("");

  lines.push(`## 5. Validation results`);
  lines.push("");
  const vs = report.validationSources || {};
  lines.push(`- Quality audit: ${vs.qualityAudit?.file || "—"} (${vs.qualityAudit?.decision || "—"})`);
  lines.push(`- PVQL: ${vs.pvql?.file || "—"} (publicFull=${vs.pvql?.publicFull ?? "—"})`);
  lines.push(`- Image audit: ${vs.imageAudit?.file || "—"} (crossBrand=${vs.imageAudit?.crossBrand ?? "—"})`);
  lines.push(`- OS: ${vs.os?.file || "—"}`);
  lines.push(
    `- Recent Momentum / Openings Evidence Quality: pass=${vs.recentMomentumEvidenceQuality?.pass}`
  );
  lines.push("");

  lines.push(`## 6. Recent Momentum / Openings Evidence Quality gate`);
  lines.push("");
  lines.push(`Gate: \`npm run test:brand-explorer-recent-momentum-evidence-quality\``);
  lines.push(`Wave pass: **${report.evidenceQuality?.pass}**`);
  lines.push("");
  for (const slug of report.evidenceQuality?.targetSlugs || []) {
    const row = report.evidenceQuality?.bySlug?.[slug];
    lines.push(
      `- \`${slug}\`: ${row?.status || "—"} (fails=${row?.failCount ?? "—"}; openings=${row?.openingsRegionalPriorityStatus || "—"})`
    );
  }
  lines.push("");

  lines.push(`## 7. Excluded non-active brands`);
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
  lines.push(
    `**Radisson Collection** remains excluded because Brand Status is not Active/Live (Draft at freeze).`
  );
  lines.push("");

  lines.push(`## 8. Protected fields`);
  lines.push("");
  for (const f of report.protectedFields || []) lines.push(`- ${f}`);
  lines.push("");
  lines.push(`Baseline freeze does **not** write any of these fields.`);
  lines.push("");

  lines.push(`## 9. Regression rules`);
  lines.push("");
  for (const r of report.regressionRules || []) lines.push(`- ${r}`);
  lines.push("");
  lines.push(`Test: \`npm run test:brand-explorer-27-active-public-full-baseline\``);
  lines.push("");

  lines.push(`## 10. Rollback notes`);
  lines.push("");
  for (const r of report.rollbackNotes || []) lines.push(`- ${r}`);
  lines.push("");

  lines.push(`## 11. Future factory rules`);
  lines.push("");
  for (const r of report.futureWorkRules || []) lines.push(`- ${r}`);
  lines.push("");

  lines.push(`## Commands`);
  lines.push("");
  lines.push("```bash");
  lines.push("npm run brand-explorer-27-active-public-full-baseline -- --dry-run");
  lines.push("npm run test:brand-explorer-27-active-public-full-baseline");
  lines.push("npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only");
  lines.push("npm run test:brand-explorer-recent-momentum-evidence-quality");
  lines.push("```");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

export {
  BASELINE_VERSION_27 as BASELINE_VERSION,
  EXPECTED_ACTIVE_COUNT_27 as EXPECTED_ACTIVE_COUNT,
  PROTECTED_FIELDS,
  ROOT,
};
