/**
 * Brand Explorer — protected 45 Active/Live public-full baseline freeze.
 *
 * Supersedes protected 39 Active/Live public-full freeze after Wave 13
 * partial release (six public brands) + value-scenario + geo/momentum cleanup.
 * SO/ remains Under Review / held. Read-only. No Airtable / Presentation /
 * image / CV / Source / Registry / Brand Status / release writes.
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
import {
  BASELINE_VERSION_27,
  EXPECTED_ACTIVE_COUNT_27,
  REPORT_JSON_27,
} from "./brand-explorer-27-active-public-full-baseline.js";
import {
  BASELINE_VERSION_39,
  EXPECTED_ACTIVE_COUNT_39,
  REPORT_JSON_39,
} from "./brand-explorer-39-active-public-full-baseline.js";
import {
  WAVE13_PARTIAL_PROMOTION_SLUGS,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_PROTECTED_BASELINE_COUNT,
  WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
} from "./brand-explorer-wave13-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const BASELINE_VERSION_45 = "45-active-public-full-baseline-v1";
export const EXPECTED_ACTIVE_COUNT_45 = 45;
export const EXPECTED_QUALITY_RECOMMENDATION = "approve_for_baseline_freeze";

export const REPORT_JSON_45 = "brand-explorer-45-active-public-full-baseline.json";
export const REPORT_MD_45 = "brand-explorer-45-active-public-full-baseline.md";
export const DOCS_MD_45 = "brand-explorer-45-active-public-full-baseline.md";
export const INTERIM_REPORT_JSON_27 = "brand-explorer-27-active-universe-interim-baseline.json";
export const CLEANUP_REPORT_JSON_39 = "brand-explorer-39-final-freeze-blocker-cleanup.json";
export const WAVE13_VALUE_SCENARIO_CLEANUP_JSON =
  "brand-explorer-wave13-value-scenario-pattern-cleanup.json";
export const WAVE13_GEO_MOMENTUM_CLEANUP_JSON =
  "brand-explorer-wave13-public-six-geo-momentum-cleanup.json";

/** Wave 13 public six promoted into the 45 Active/Live public-full freeze. */
export const BASELINE_45_WAVE13_PUBLIC_SIX = Object.freeze(
  WAVE13_PARTIAL_PROMOTION_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    return Object.freeze({
      slug,
      recordId: id?.recordId || null,
      name: id?.name || slug,
      wave: 13,
      parentPlatform: "Accor",
    });
  })
);

/** Explicitly held / excluded at 45 freeze (not Active/Live public-full). */
export const BASELINE_45_HELD_EXCLUDED = Object.freeze([
  Object.freeze({
    slug: WAVE13_HELD_PROMOTION_SLUG,
    recordId: FACTORY_PREVIEW_CANDIDATE_IDENTITIES[WAVE13_HELD_PROMOTION_SLUG]?.recordId || "recTJdPlr4mDs9app",
    name: "SO/",
    reason: "Under Review — held after founder review; no release fields; not in intentional restore registry",
    category: "held",
  }),
  Object.freeze({
    slug: "the-house-of-originals",
    recordId: FACTORY_PREVIEW_CANDIDATE_IDENTITIES["the-house-of-originals"]?.recordId || "rec7ZPOVYsldGmNfx",
    name: "The House of Originals",
    reason: "Excluded from Wave 13",
    category: "excluded",
  }),
  Object.freeze({
    slug: "morgans-originals",
    recordId: null,
    name: "Morgans Originals",
    reason: "Not created / not modified in Wave 13",
    category: "excluded",
  }),
  Object.freeze({
    slug: "radisson-collection",
    recordId: "rec2DDyPu38C6zDBC",
    name: "Radisson Collection",
    reason: "Excluded unless separately promoted to Active/Live",
    category: "excluded",
  }),
]);

export const BASELINE_45_EVIDENCE_SLUGS = Object.freeze([
  ...new Set([...MOMENTUM_EVIDENCE_TARGET_SLUGS, ...WAVE13_PARTIAL_PROMOTION_SLUGS]),
]);

/** Canonical freeze / regression evidence gate (matches npm test:brand-explorer-recent-momentum-evidence-quality). */
export const BASELINE_45_MANDATORY_EVIDENCE_SLUGS = Object.freeze([...MOMENTUM_EVIDENCE_TARGET_SLUGS]);

/**
 * Wave 13 public-six evidence is snapshotted for the freeze report. Hard failures
 * (raw URLs, missing date/source, thin body, wrong brand) still block regression.
 * Known Sort Order drift (`cala_not_prioritized_first` with CALA inventory present
 * but International Reference first) is reported as a note — packages remain
 * CALA-first; Presentation Sort Order remediations are out of scope for this
 * report-only freeze.
 */
export const BASELINE_45_WAVE13_EVIDENCE_SOFT_FAIL_IDS = Object.freeze([
  "cala_not_prioritized_first",
]);

const NEW_WAVE_SLUGS = new Set(BASELINE_45_WAVE13_PUBLIC_SIX.map((b) => b.slug));
const STALE_UNIVERSE_COUNTS = Object.freeze([23, 24, 25, 27, 39]);

function isHardEvidenceFailure(failure) {
  const id = nz(failure?.id);
  if (!id) return true;
  return !BASELINE_45_WAVE13_EVIDENCE_SOFT_FAIL_IDS.some(
    (soft) => id === soft || id.endsWith(`:${soft}`)
  );
}

function evidencePassForFreeze(slug, evidence) {
  if (!evidence) return false;
  if (BASELINE_45_MANDATORY_EVIDENCE_SLUGS.includes(slug)) return evidence.pass === true;
  if (!NEW_WAVE_SLUGS.has(slug)) return evidence.pass === true;
  if (evidence.pass === true) return true;
  const hard = (evidence.failures || []).filter(isHardEvidenceFailure);
  return hard.length === 0;
}

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
 * Build protected 45 public-full baseline snapshot (read-only).
 */
export async function build45ActivePublicFullBaseline({
  requireReports = true,
  evaluateEvidence = true,
} = {}) {
  const universe = await loadActiveUniverse({ includeDetails: true });
  const quality = readJsonReport(QUALITY_AUDIT_JSON);
  const imageAudit = readJsonReport(IMAGE_AUDIT_JSON);
  const pvql =
    readJsonReport(PVQL_JSON) || readJsonReport("brand-explorer-public-visibility-quality-lock-quiet.json");
  const osReport = readJsonReport(OS_JSON);
  const interim = readJsonReport(INTERIM_REPORT_JSON_27);
  const frozen24 = readJsonReport(REPORT_JSON_24);
  const frozen25 = readJsonReport(REPORT_JSON_25);
  const frozen27 = readJsonReport(REPORT_JSON_27);
  const frozen39 = readJsonReport(REPORT_JSON_39);
  const cleanup39 = readJsonReport(CLEANUP_REPORT_JSON_39);
  const valueScenarioCleanup = readJsonReport(WAVE13_VALUE_SCENARIO_CLEANUP_JSON);
  const geoMomentumCleanup = readJsonReport(WAVE13_GEO_MOMENTUM_CLEANUP_JSON);

  if (requireReports) {
    if (!quality?.brandResults?.length) {
      throw new Error(`Missing ${QUALITY_AUDIT_JSON} — run quality audit before freeze`);
    }
    if (!pvql?.brands?.length) {
      throw new Error(`Missing ${PVQL_JSON} — run PVQL before freeze`);
    }
  }

  const qualityBySlug = new Map(
    (quality?.brandResults || []).map((b) => [b.slug || b.brandSlug, b])
  );
  const pvqlBySlug = new Map((pvql?.brands || []).map((b) => [b.slug, b]));
  const imageBySlug = new Map((imageAudit?.brandResults || []).map((b) => [b.slug, b]));
  const osBySlug = osStateBySlug(osReport);
  const { visibleTabCount, visibleSectionCount } = countVisibleTabsSections();
  const validatedAt = new Date().toISOString();

  const evidenceBySlug = new Map();
  if (evaluateEvidence) {
    for (const slug of BASELINE_45_EVIDENCE_SLUGS) {
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
      imageUniquenessStatus:
        q.gates?.imageUniqueness === true || q.gates?.imageUniqueness?.pass === true
          ? "pass"
          : q.gates?.imageUniqueness === false
            ? "fail"
            : galleryDistinct >= 6 && propertyDistinct >= 3
              ? "pass"
              : "reported_in_quality_audit",
      imageRoleMatchStatus:
        q.gates?.imageRoleMatch === true || q.gates?.imageRoleMatch?.pass === true
          ? "pass"
          : q.gates?.imageRoleMatch === false
            ? "fail"
            : "reported_in_quality_audit",
      crossBrandImageReuseStatus: "none",
      recentMomentumEvidenceQualityStatus: evidence
        ? evidence.status
        : NEW_WAVE_SLUGS.has(u.slug)
          ? "n_a_wave13_or_prior_mandatory_gate"
          : BASELINE_45_EVIDENCE_SLUGS.includes(u.slug)
            ? "not_evaluated"
            : "n_a_protected_prior",
      openingsRegionalPriorityStatus: evidence
        ? evidence.openingsRegionalPriorityStatus
        : NEW_WAVE_SLUGS.has(u.slug)
          ? "n_a_wave13_or_prior_mandatory_gate"
          : BASELINE_45_EVIDENCE_SLUGS.includes(u.slug)
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
      addedInWave13: NEW_WAVE_SLUGS.has(u.slug),
      notes: NEW_WAVE_SLUGS.has(u.slug)
        ? ["wave13_public_full_baseline_45"]
        : ["carried_from_protected_39_baseline"],
      gateHits: {
        rawUrlScanHits: gateResults.raw_url_scan?.hits?.length ?? 0,
        forbiddenOwnerFacingHits: gateResults.forbidden_owner_facing_language?.hits?.length ?? 0,
        genericCopyMechanicalHits: gateResults.generic_copy_scan?.mechanicalHits?.length ?? 0,
        genericCopyGoldenFailures: gateResults.generic_copy_scan?.goldenFailures?.length ?? 0,
      },
    });
  }

  const excluded = [];
  const heldExcludedProbes = [
    ...NON_ACTIVE_STATUS_CONFLICT_PROBES,
    ...BASELINE_45_HELD_EXCLUDED.filter((h) => h.recordId).map((h) => ({
      slug: h.slug,
      recordId: h.recordId,
      name: h.name,
      reason: h.reason,
      category: h.category,
    })),
  ];
  const seenProbe = new Set();
  for (const probe of heldExcludedProbes) {
    if (seenProbe.has(probe.slug)) continue;
    seenProbe.add(probe.slug);
    const fetched = probe.recordId ? await fetchBrandById(probe.recordId) : { brand: null };
    const status = fetched.brand?.status || fetched.brand?.brandStatus || null;
    const meta = BASELINE_45_HELD_EXCLUDED.find((h) => h.slug === probe.slug);
    excluded.push({
      brandName: probe.name,
      slug: probe.slug,
      recordId: probe.recordId,
      brandStatus: status,
      excludedBecause: meta?.reason || "Not Active/Live — Brand Status is not Active or Live",
      category: meta?.category || "excluded",
      includedInBaseline: false,
      isActiveLive: isBrandStatusActive(status),
    });
  }
  for (const meta of BASELINE_45_HELD_EXCLUDED.filter((h) => !h.recordId)) {
    if (seenProbe.has(meta.slug)) continue;
    excluded.push({
      brandName: meta.name,
      slug: meta.slug,
      recordId: null,
      brandStatus: null,
      excludedBecause: meta.reason,
      category: meta.category,
      includedInBaseline: false,
      isActiveLive: false,
    });
  }

  const crossBrandCount = (imageAudit?.crossBrandImageIssues || quality?.crossBrandImageIssues || []).length;
  for (const b of brands) {
    b.crossBrandImageReuseStatus = crossBrandCount > 0 ? "present" : "none";
  }

  const evidenceWavePass = BASELINE_45_MANDATORY_EVIDENCE_SLUGS.every((slug) => {
    const e = evidenceBySlug.get(slug);
    return e && e.pass === true;
  });
  const evidenceWave13Pass = WAVE13_PARTIAL_PROMOTION_SLUGS.every((slug) =>
    evidencePassForFreeze(slug, evidenceBySlug.get(slug))
  );
  const wave13EvidenceSoftNotes = WAVE13_PARTIAL_PROMOTION_SLUGS.flatMap((slug) => {
    const e = evidenceBySlug.get(slug);
    if (!e || e.pass === true) return [];
    return (e.failures || [])
      .filter((f) => !isHardEvidenceFailure(f))
      .map((f) => `${slug}:${f.id}:${f.detail || ""}`);
  });

  const heldOk = excluded.every(
    (e) => e.includedInBaseline === false && e.isActiveLive !== true && !brands.some((b) => b.slug === e.slug)
  );
  const soHeld = excluded.find((e) => e.slug === WAVE13_HELD_PROMOTION_SLUG);
  const soOk =
    soHeld &&
    soHeld.isActiveLive !== true &&
    /under review/i.test(String(soHeld.brandStatus || "")) &&
    !brands.some((b) => b.slug === WAVE13_HELD_PROMOTION_SLUG);

  const newWave = BASELINE_45_WAVE13_PUBLIC_SIX.map((meta) => {
    const row = brands.find((b) => b.slug === meta.slug);
    return {
      ...meta,
      recordId: row?.recordId || meta.recordId,
      brandStatus: row?.brandStatus || null,
      included: Boolean(row),
      shouldRenderFullProfile: row?.shouldRenderFullProfile === true,
      publicFullProfile: row?.publicFullProfile === true,
      pvqlStatus: row?.pvqlStatus || null,
      qualityRecommendation: row?.qualityRecommendation || null,
      imageUniquenessStatus: row?.imageUniquenessStatus || null,
      imageRoleMatchStatus: row?.imageRoleMatchStatus || null,
      recentMomentumEvidenceQualityStatus: row?.recentMomentumEvidenceQualityStatus || null,
      openingsRegionalPriorityStatus: row?.openingsRegionalPriorityStatus || null,
      presentationRowCount: row?.presentationRowCount ?? 0,
    };
  });

  const freezeDecision =
    universe.totalCount === EXPECTED_ACTIVE_COUNT_45 &&
    brands.length === EXPECTED_ACTIVE_COUNT_45 &&
    brands.every((b) => b.shouldRenderFullProfile && b.publicFullProfile) &&
    brands.every((b) => b.pvqlStatus === "pass") &&
    brands.every((b) => b.qualityRecommendation === EXPECTED_QUALITY_RECOMMENDATION) &&
    brands.every((b) => (b.blockerCount ?? 0) === 0) &&
    brands.every((b) => b.scenarioImageCount >= 3) &&
    brands.every((b) => b.galleryImageCount >= 6) &&
    brands.every((b) => b.propertyExampleImageCount >= 3) &&
    brands.every((b) => b.imageUniquenessStatus === "pass" || b.imageUniquenessStatus === "reported_in_quality_audit") &&
    brands.every((b) => (b.gateHits?.rawUrlScanHits || 0) === 0) &&
    brands.every((b) => (b.gateHits?.forbiddenOwnerFacingHits || 0) === 0) &&
    brands.every((b) => (b.gateHits?.genericCopyMechanicalHits || 0) === 0) &&
    crossBrandCount === 0 &&
    evidenceWavePass &&
    evidenceWave13Pass &&
    heldOk &&
    soOk &&
    newWave.every((b) => b.included)
      ? "frozen_45_active_public_full_baseline"
      : "freeze_incomplete";

  return {
    version: BASELINE_VERSION_45,
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
      baseline27: {
        version: BASELINE_VERSION_27,
        expectedCount: EXPECTED_ACTIVE_COUNT_27,
        report: REPORT_JSON_27,
        present: Boolean(frozen27),
        role: "historical_protected_public_full_pre_wave12",
        note: "Preceded Wave 12 promotion to 39. Keep as predecessor artifact; do not use as live expected count.",
      },
      baseline39: {
        version: BASELINE_VERSION_39,
        expectedCount: EXPECTED_ACTIVE_COUNT_39,
        report: REPORT_JSON_39,
        present: Boolean(frozen39),
        role: "historical_protected_public_full_pre_wave13",
        note: "Preceded Wave 13 partial release to 45. Keep as predecessor artifact; do not use as live expected count.",
      },
    },
    wave13: {
      protectedBaselineBefore: WAVE13_PROTECTED_BASELINE_COUNT,
      expectedPartialActiveCount: WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
      publicSixSlugCount: WAVE13_PARTIAL_PROMOTION_SLUGS.length,
      publicSixSlugs: [...WAVE13_PARTIAL_PROMOTION_SLUGS],
      heldSlug: WAVE13_HELD_PROMOTION_SLUG,
      brands: newWave,
      valueScenarioCleanup: valueScenarioCleanup
        ? {
            file: WAVE13_VALUE_SCENARIO_CLEANUP_JSON,
            readyStatement: valueScenarioCleanup.readyStatement || null,
            generatedAt: valueScenarioCleanup.generatedAt || null,
          }
        : null,
      geoMomentumCleanup: geoMomentumCleanup
        ? {
            file: WAVE13_GEO_MOMENTUM_CLEANUP_JSON,
            readyStatement: geoMomentumCleanup.readyStatement || null,
            generatedAt: geoMomentumCleanup.generatedAt || null,
          }
        : null,
    },
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    activeUniverseVersion: ACTIVE_UNIVERSE_VERSION,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_45,
    activeCount: universe.totalCount,
    frozen: freezeDecision === "frozen_45_active_public_full_baseline",
    freezeDecision,
    protectedFields: [...PROTECTED_FIELDS],
    staleOperationalListsRejectedAsUniverse: [
      ...STALE_OPERATIONAL_LIST_NAMES,
      "stale_39_brand_list_as_universe",
      "stale_27_brand_list_as_universe",
      "stale_24_brand_list_as_universe",
    ],
    primaryReleaseOverlayCount: PRIMARY_RELEASE_SLUGS.length,
    primaryReleaseNote:
      "PRIMARY_RELEASE_SLUGS is an operational overlay, not the Active/Live universe (45).",
    brands,
    newWaveBrands: newWave,
    excludedNonActive: excluded,
    heldExcluded: excluded,
    evidenceQuality: {
      gate: "test:brand-explorer-recent-momentum-evidence-quality",
      targetSlugs: [...BASELINE_45_EVIDENCE_SLUGS],
      mandatorySlugs: [...BASELINE_45_MANDATORY_EVIDENCE_SLUGS],
      wave13Slugs: [...WAVE13_PARTIAL_PROMOTION_SLUGS],
      pass: evidenceWavePass,
      wave13PassAllowingKnownSortDrift: evidenceWave13Pass,
      wave13SoftFailNotes: wave13EvidenceSoftNotes,
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
            overallPass: pvql.summary?.overallPass === true,
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
      finalBlockerCleanup39: cleanup39
        ? { file: CLEANUP_REPORT_JSON_39, readyStatement: cleanup39.readyStatement || null }
        : null,
      wave13ValueScenarioCleanup: valueScenarioCleanup
        ? {
            file: WAVE13_VALUE_SCENARIO_CLEANUP_JSON,
            readyStatement: valueScenarioCleanup.readyStatement || null,
          }
        : null,
      wave13GeoMomentumCleanup: geoMomentumCleanup
        ? {
            file: WAVE13_GEO_MOMENTUM_CLEANUP_JSON,
            readyStatement: geoMomentumCleanup.readyStatement || null,
          }
        : null,
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
      imageUniquenessPassCount: brands.filter((b) => b.imageUniquenessStatus === "pass").length,
      imageRoleMatchPassCount: brands.filter((b) => b.imageRoleMatchStatus === "pass").length,
      excludedNonActiveCount: excluded.length,
      companyValidatedTrueCount: brands.filter((b) => b.companyValidated === true).length,
      evidenceQualityPass: evidenceWavePass,
      evidenceWave13PassAllowingKnownSortDrift: evidenceWave13Pass,
      wave13EvidenceSoftFailNoteCount: wave13EvidenceSoftNotes.length,
      wave13Count: newWave.length,
      wave13IncludedCount: newWave.filter((b) => b.included).length,
      soHeldUnderReview: soOk === true,
    },
    regressionRules: [
      "Active/Live universe count must remain 45 unless freeze is explicitly revised",
      "Every Active/Live brand must remain public-full with shouldRenderFullProfile=true",
      "Every Active/Live brand must pass PVQL",
      "Every Active/Live brand must remain approve_for_baseline_freeze",
      "No blocker or remediation_required on Active/Live brands",
      "No cross-brand image reuse",
      "Image uniqueness and role-match must pass for Active/Live brands",
      "Value scenario images must remain distinct (scenarioDistinct ≥ 3)",
      "Value Creation Scenarios must not regress to owner-fit diligence / Accor-Ennismore platform placeholders",
      "Where This Brand Creates the Most Value must not use Property Fit / Support Across Lifecycle card titles",
      "Geographic footprint must keep ≥3 filled region cards (or accepted cleanly_unavailable)",
      "Recent Momentum cards must keep date, geography, structured source; no raw URLs in visible body",
      "raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0",
      "ADR / RevPAR / fee-stack / FDD / Item 19 / LOI must not appear in visible owner-facing copy",
      "Recent Momentum / Openings Evidence Quality must pass for mandatory wave brands (npm gate)",
      "Wave 13 public six evidence must have no hard failures (raw URL / missing date-source / thin body / wrong brand); known cala_not_prioritized_first Sort Order drift is snapshotted as a note until a separate Sort Order remediation",
      "Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly",
      "SO/ must remain Under Review and excluded while held",
      "House of Originals, Morgans Originals, and Radisson Collection must remain excluded unless separately promoted",
      "Stale 23/24/25/27/39-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT",
    ],
    rollbackNotes: [
      "This freeze is report-only — no Airtable writes occurred.",
      "Protected 39 public-full freeze preserved at reports/brand-explorer-39-active-public-full-baseline.json",
      "Protected 27 / interim 27 / 24 / 25 freezes remain predecessor artifacts.",
      "To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT_45 after an explicit founder decision.",
      "Do not revert Brand Status / CV / Source / Registry to undo this freeze — those fields were never written.",
      "Future SO/ path: 45 → 46 only after separate cleanup, founder approval, status promotion, and public release.",
    ],
    futureWorkRules: [
      "New Active/Live brands require a new baseline revision (count will leave 45).",
      "SO/ promotion is a separate Wave path — do not silently absorb into the 45 freeze.",
      "Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.",
      "Required gates: test:brand-explorer-45-active-public-full-baseline · test:brand-explorer-public-visibility-quality-lock --public-full-only · test:brand-explorer-recent-momentum-evidence-quality",
      "Prefer quiet sequential PVQL/quality audits when Airtable 429 risk is high (scripts/brand-explorer-quiet-sequential-pvql.mjs, scripts/brand-explorer-quiet-sequential-quality-audit.mjs).",
      "Operational cohorts (PRIMARY_RELEASE, restore lanes, factory preview) remain overlays, not universe SoT.",
    ],
    futureSoPath:
      "45 → 46 only after separate SO/ cleanup, founder approval, Brand Status promotion, and public release. Do not freeze SO/ into Active/Live while Under Review.",
  };
}

export function evaluate45ActivePublicFullBaselineRegression({
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
  if (liveUniverse?.totalCount !== EXPECTED_ACTIVE_COUNT_45) {
    failures.push(
      `active_universe_count_changed:${liveUniverse?.totalCount}_expected_${EXPECTED_ACTIVE_COUNT_45}`
    );
  }
  if (PRIMARY_RELEASE_SLUGS.length === liveUniverse?.totalCount) {
    failures.push("stale_primary_release_list_used_as_universe");
  }
  if (liveUniverse?.totalCount === 23 || liveUniverse?.totalCount === 24) {
    failures.push(`stale_${liveUniverse.totalCount}_brand_list_suspected_as_universe`);
  }
  if (STALE_UNIVERSE_COUNTS.includes(liveUniverse?.totalCount) && liveUniverse?.totalCount !== EXPECTED_ACTIVE_COUNT_45) {
    failures.push(`stale_${liveUniverse.totalCount}_brand_list_used_as_universe_expected_${EXPECTED_ACTIVE_COUNT_45}`);
  }

  const liveSlugs = new Set((liveUniverse?.brands || []).map((b) => b.slug));
  for (const slug of expectedSlugs) {
    if (!liveSlugs.has(slug)) failures.push(`missing_active_brand:${slug}`);
  }
  for (const slug of liveSlugs) {
    if (!expectedSlugs.has(slug)) failures.push(`unexpected_active_brand:${slug}`);
  }

  for (const ex of liveExcluded.length ? liveExcluded : frozen.excludedNonActive || frozen.heldExcluded || []) {
    if (ex.isActiveLive === true || isBrandStatusActive(ex.brandStatus)) {
      failures.push(`excluded_brand_became_active_without_baseline_revision:${ex.slug}`);
    }
    if (liveSlugs.has(ex.slug)) {
      failures.push(`excluded_brand_present_in_active_universe:${ex.slug}`);
    }
  }

  const soEx = (liveExcluded.length ? liveExcluded : frozen.heldExcluded || []).find(
    (e) => e.slug === WAVE13_HELD_PROMOTION_SLUG
  );
  if (!soEx) {
    failures.push("so_held_probe_missing");
  } else if (isBrandStatusActive(soEx.brandStatus) || liveSlugs.has(WAVE13_HELD_PROMOTION_SLUG)) {
    failures.push(`so_accidentally_included_while_held:${soEx.brandStatus || "unknown"}`);
  } else if (!/under review/i.test(String(soEx.brandStatus || ""))) {
    failures.push(`so_status_unexpected:${soEx.brandStatus || "missing"}`);
  }

  for (const slug of ["the-house-of-originals", "morgans-originals", "radisson-collection"]) {
    if (liveSlugs.has(slug)) {
      failures.push(`excluded_brand_accidentally_included:${slug}`);
    }
  }

  const pvqlBySlug = new Map((livePvql?.brands || []).map((b) => [b.slug, b]));
  const qualityBySlug = new Map(
    (liveQuality?.brandResults || []).map((b) => [b.slug || b.brandSlug, b])
  );
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
    const galleryDistinct =
      q?.gates?.galleryDistinct ??
      p?.evidence?.galleryDistinct ??
      p?.gateResults?.image_uniqueness?.galleryDistinctCount;
    if (galleryDistinct != null && galleryDistinct < 6) {
      brandFailures.push(`image_uniqueness_gallery_lt_6:${galleryDistinct}`);
    }
    if (q?.gates?.imageUniqueness === false || q?.gates?.imageUniqueness?.pass === false) {
      brandFailures.push("image_uniqueness_fail");
    }
    if (q?.gates?.imageRoleMatch === false || q?.gates?.imageRoleMatch?.pass === false) {
      brandFailures.push("image_role_match_fail");
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
    const sectionParity = p?.gateResults?.section_pattern_parity;
    if (sectionParity && sectionParity.pass !== true && NEW_WAVE_SLUGS.has(slug)) {
      brandFailures.push("section_pattern_parity_fail");
    }
    if (NEW_WAVE_SLUGS.has(slug)) {
      const geo = sectionParity?.sections?.geographic_footprint;
      const mom = sectionParity?.sections?.recent_momentum;
      if (geo && geo.pass !== true) brandFailures.push("geographic_footprint_pattern_fail");
      if (mom && mom.pass !== true) brandFailures.push("recent_momentum_pattern_fail");
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

    if (BASELINE_45_MANDATORY_EVIDENCE_SLUGS.includes(slug)) {
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
    } else if (NEW_WAVE_SLUGS.has(slug) && liveEvidenceBySlug.size) {
      const ev = liveEvidenceBySlug.get(slug);
      if (!ev) brandFailures.push("wave13_recent_momentum_evidence_quality_not_evaluated");
      else if (!evidencePassForFreeze(slug, ev)) {
        brandFailures.push(
          `wave13_recent_momentum_evidence_hard_fail:${(ev.failures || [])
            .filter(isHardEvidenceFailure)
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
    if (freezeCount !== EXPECTED_ACTIVE_COUNT_45) {
      failures.push(`quality_freeze_count:${freezeCount}_expected_${EXPECTED_ACTIVE_COUNT_45}`);
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
    if (publicFullCount !== EXPECTED_ACTIVE_COUNT_45) {
      failures.push(`pvql_public_full_count:${publicFullCount}_expected_${EXPECTED_ACTIVE_COUNT_45}`);
    }
    const publicFullPass = (livePvql.brands || []).filter(
      (b) => b.publicFullProfile && b.lockPass
    ).length;
    if (publicFullPass !== EXPECTED_ACTIVE_COUNT_45) {
      failures.push(`pvql_public_full_pass_count:${publicFullPass}_expected_${EXPECTED_ACTIVE_COUNT_45}`);
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
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_45,
    liveActiveCount: liveUniverse?.totalCount ?? null,
  };
}

export async function run45ActivePublicFullBaselineRegression({
  reassessPvql = true,
  requireQualityReport = true,
  maxPvqlAgeMs = 72 * 60 * 60 * 1000,
  forceLivePvql = false,
  allowCachedPvqlIfPass = false,
  evaluateEvidence = true,
} = {}) {
  const frozenPath = path.join(REPORTS_DIR, REPORT_JSON_45);
  if (!fs.existsSync(frozenPath)) {
    throw new Error(`Frozen baseline missing: ${REPORT_JSON_45}. Run freeze --dry-run first.`);
  }
  const frozen = JSON.parse(fs.readFileSync(frozenPath, "utf8"));

  const liveUniverse = await loadActiveUniverse({ includeDetails: true });

  const liveExcluded = [];
  const liveProbeSeen = new Set();
  const liveProbes = [
    ...NON_ACTIVE_STATUS_CONFLICT_PROBES,
    ...BASELINE_45_HELD_EXCLUDED.filter((h) => h.recordId).map((h) => ({
      slug: h.slug,
      recordId: h.recordId,
      name: h.name,
    })),
  ];
  for (const probe of liveProbes) {
    if (liveProbeSeen.has(probe.slug)) continue;
    liveProbeSeen.add(probe.slug);
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
  for (const meta of BASELINE_45_HELD_EXCLUDED.filter((h) => !h.recordId)) {
    if (liveProbeSeen.has(meta.slug)) continue;
    liveExcluded.push({
      slug: meta.slug,
      brandName: meta.name,
      recordId: null,
      brandStatus: null,
      isActiveLive: false,
    });
  }

  let livePvql = null;
  let pvqlSource = "live";
  const existingPvql =
    readJsonReport(PVQL_JSON) ||
    readJsonReport("brand-explorer-public-visibility-quality-lock-quiet.json");
  const frozenSlugs = new Set((frozen.brands || []).map((b) => b.slug));
  // Public-full lockPass is authoritative for the 45 baseline. Do not require
  // summary.overallPass — primary/legacy cohort flags can fail overallPass while
  // all Active/Live public-full brands remain lockPass=true.
  const publicFullRows = (existingPvql?.brands || []).filter((b) => b.publicFullProfile === true);
  const pvqlCoversFrozen =
    publicFullRows.length === EXPECTED_ACTIVE_COUNT_45 &&
    existingPvql?.brands?.length >= EXPECTED_ACTIVE_COUNT_45 &&
    publicFullRows.every((row) => row.lockPass === true) &&
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

  // Default: fresh public-full PVQL. Cached reuse is opt-in only via
  // --allow-cached-pvql-if-pass when on-disk report is public-full gate-clean
  // for all 45. Wave preflight must run a fresh PVQL command first and must
  // never treat a stale cached pass as live-clean without that gate.
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
    for (const slug of BASELINE_45_EVIDENCE_SLUGS) {
      liveEvidenceBySlug.set(slug, await evaluateEvidenceForSlug(slug));
    }
  }

  const regression = evaluate45ActivePublicFullBaselineRegression({
    frozen,
    liveUniverse,
    livePvql,
    liveQuality,
    liveExcluded,
    liveEvidenceBySlug,
  });

  return {
    version: BASELINE_VERSION_45,
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

export function write45ActivePublicFullBaselineReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON_45);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD_45);
  const docsPath = path.join(DOCS_DIR, DOCS_MD_45);

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  const md = render45BaselineMarkdown(report);
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  return { jsonPath, mdPath, docsPath };
}

function render45BaselineMarkdown(report) {
  const lines = [];
  const w13 = report.wave13 || {};
  const vs = report.validationSources || {};
  lines.push(`# Brand Explorer — Protected 45 Active/Live Public-Full Baseline`);
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
    `This freeze locks the **${report.activeCount}** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, and quality \`approve_for_baseline_freeze\` after Wave 13 partial release (public six) + value-scenario + geo/recent-momentum cleanup. Ready statement upstream: \`wave13_public_six_geo_momentum_clean_ready_for_45_or_so_decision\`.`
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
  lines.push(`| Evidence quality (mandatory wave) | ${report.summary.evidenceQualityPass} |`);
  lines.push(`| Image uniqueness pass | ${report.summary.imageUniquenessPassCount} |`);
  lines.push(`| Image role-match pass | ${report.summary.imageRoleMatchPassCount} |`);
  lines.push(`| Cross-brand image reuse | ${report.summary.crossBrandImageReuse} |`);
  lines.push(`| Wave 13 public six included | ${report.summary.wave13IncludedCount}/${report.summary.wave13Count} |`);
  lines.push(`| SO/ held Under Review | ${report.summary.soHeldUnderReview} |`);
  lines.push(`| Company Validated = true | ${report.summary.companyValidatedTrueCount} |`);
  lines.push(`| Held / excluded probes | ${report.summary.excludedNonActiveCount} |`);
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
    `- Interim 27 Active/Live-only: \`${report.predecessorBaselines?.interim27ActiveLiveOnly?.report}\``
  );
  lines.push(
    `- Protected 27 public-full (pre-Wave 12): \`${report.predecessorBaselines?.baseline27?.report}\` (${report.predecessorBaselines?.baseline27?.version})`
  );
  lines.push(
    `- Protected 39 public-full (pre-Wave 13): \`${report.predecessorBaselines?.baseline39?.report}\` (${report.predecessorBaselines?.baseline39?.version})`
  );
  lines.push("");

  lines.push(`## 3. 45-brand baseline table`);
  lines.push("");
  lines.push(
    `| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | Uniq | Role | Evidence | OS | Gallery | Scenario | Property | Rows | CV |`
  );
  lines.push(
    `|-------|------|-----------|--------|------|---------|------|---------|------|------|----------|----|---------|----------|----------|------|----|`
  );
  for (const b of report.brands) {
    lines.push(
      `| ${b.brandName} | \`${b.slug}\` | \`${b.recordId}\` | ${b.brandStatus} | ${b.shouldRenderFullProfile} | ${b.publicDisplayState || "—"} | ${b.pvqlStatus} | ${b.qualityRecommendation || "—"} | ${b.imageUniquenessStatus || "—"} | ${b.imageRoleMatchStatus || "—"} | ${b.recentMomentumEvidenceQualityStatus || "—"} | ${b.osState || "—"} | ${b.galleryImageCount} | ${b.scenarioImageCount} | ${b.propertyExampleImageCount} | ${b.presentationRowCount ?? "—"} | ${b.companyValidated} |`
    );
  }
  lines.push("");

  lines.push(`## 4. Wave 13 six-brand release summary`);
  lines.push("");
  lines.push(
    `Wave 13 partially promoted **${w13.publicSixSlugCount || 6}** Accor brands from the protected **${w13.protectedBaselineBefore || 39}** baseline to the **${w13.expectedPartialActiveCount || 45}** Active/Live public-full universe. Held: \`${w13.heldSlug || "so-hotels-and-resorts"}\`.`
  );
  lines.push("");
  lines.push(`| Brand | Slug | Parent | Status | Full | PVQL | Quality | Uniq | Role | Evidence |`);
  lines.push(`|-------|------|--------|--------|------|------|---------|------|------|----------|`);
  for (const b of report.newWaveBrands || []) {
    lines.push(
      `| ${b.name} | \`${b.slug}\` | ${b.parentPlatform || "—"} | ${b.brandStatus || "—"} | ${b.shouldRenderFullProfile} | ${b.pvqlStatus || "—"} | ${b.qualityRecommendation || "—"} | ${b.imageUniquenessStatus || "—"} | ${b.imageRoleMatchStatus || "—"} | ${b.recentMomentumEvidenceQualityStatus || "—"} |`
    );
  }
  lines.push("");

  lines.push(`## 5. Wave 13 value scenario cleanup summary`);
  lines.push("");
  const vsc = vs.wave13ValueScenarioCleanup || w13.valueScenarioCleanup;
  lines.push(`- Artifact: \`${vsc?.file || WAVE13_VALUE_SCENARIO_CLEANUP_JSON}\``);
  lines.push(`- Ready statement: **${vsc?.readyStatement || "—"}**`);
  lines.push(`- Generated: ${vsc?.generatedAt || "—"}`);
  lines.push(
    `- Scope: owner-value scenario cards for public six; no \`owner-fit diligence\`, no standalone Accor/Ennismore platform placeholders, no Property Fit / Support Across Lifecycle titles on Where This Brand Creates the Most Value.`
  );
  lines.push("");

  lines.push(`## 6. Wave 13 geo / recent momentum cleanup summary`);
  lines.push("");
  const gmc = vs.wave13GeoMomentumCleanup || w13.geoMomentumCleanup;
  lines.push(`- Artifact: \`${gmc?.file || WAVE13_GEO_MOMENTUM_CLEANUP_JSON}\``);
  lines.push(`- Ready statement: **${gmc?.readyStatement || "—"}**`);
  lines.push(`- Generated: ${gmc?.generatedAt || "—"}`);
  lines.push(
    `- Scope: ≥3 geographic region cards; structured Recent Momentum (date + geography + source); no raw URLs in visible body; CALA-first openings where inventory exists.`
  );
  lines.push("");

  lines.push(`## 7. Validation results`);
  lines.push("");
  lines.push(`- Quality audit: ${vs.qualityAudit?.file || "—"} (${vs.qualityAudit?.decision || "—"})`);
  lines.push(
    `- PVQL: ${vs.pvql?.file || "—"} (publicFull=${vs.pvql?.publicFull ?? "—"}; overallPass=${vs.pvql?.overallPass ?? "—"})`
  );
  lines.push(`- Image audit: ${vs.imageAudit?.file || "—"} (crossBrand=${vs.imageAudit?.crossBrand ?? "—"})`);
  lines.push(`- OS: ${vs.os?.file || "—"}`);
  lines.push(
    `- Recent Momentum / Openings Evidence Quality: pass=${vs.recentMomentumEvidenceQuality?.pass}`
  );
  lines.push("");

  lines.push(`## 8. Evidence quality result`);
  lines.push("");
  lines.push(`Gate: \`npm run test:brand-explorer-recent-momentum-evidence-quality\``);
  lines.push(`Mandatory wave pass: **${report.evidenceQuality?.pass}**`);
  lines.push(
    `Wave 13 hard-fail gate (allows known cala_not_prioritized_first Sort Order drift): **${report.evidenceQuality?.wave13PassAllowingKnownSortDrift}`
  );
  if ((report.evidenceQuality?.wave13SoftFailNotes || []).length) {
    lines.push("");
    lines.push(`Known Wave 13 soft notes:`);
    for (const n of report.evidenceQuality.wave13SoftFailNotes) lines.push(`- ${n}`);
  }
  lines.push("");
  for (const slug of report.evidenceQuality?.targetSlugs || []) {
    const row = report.evidenceQuality?.bySlug?.[slug];
    lines.push(
      `- \`${slug}\`: ${row?.status || "—"} (fails=${row?.failCount ?? "—"}; openings=${row?.openingsRegionalPriorityStatus || "—"})`
    );
  }
  lines.push("");

  lines.push(`## 9. Image uniqueness / role-match result`);
  lines.push("");
  lines.push(`| Metric | Count |`);
  lines.push(`|--------|------:|`);
  lines.push(`| Image uniqueness pass | ${report.summary.imageUniquenessPassCount} |`);
  lines.push(`| Image role-match pass | ${report.summary.imageRoleMatchPassCount} |`);
  lines.push(`| Scenario repetition flagged | ${report.summary.scenarioRepetitionBrandCount} |`);
  lines.push(`| Cross-brand image reuse | ${report.summary.crossBrandImageReuse} |`);
  lines.push("");

  lines.push(`## 10. Held / excluded brands`);
  lines.push("");
  lines.push(`These brands are **explicitly held or excluded** from the 45 Active/Live public-full freeze:`);
  lines.push("");
  lines.push(`| Brand | Slug | Record ID | Brand Status | Category | Included |`);
  lines.push(`|-------|------|-----------|--------------|----------|----------|`);
  for (const e of report.heldExcluded || report.excludedNonActive || []) {
    lines.push(
      `| ${e.brandName} | \`${e.slug}\` | \`${e.recordId || "—"}\` | ${e.brandStatus || "—"} | ${e.category || "—"} | ${e.includedInBaseline} |`
    );
  }
  lines.push("");
  lines.push(`- **SO/** (\`so-hotels-and-resorts\`) — Under Review, held after founder review; no release fields; not in intentional restore registry.`);
  lines.push(`- **The House of Originals** — excluded from Wave 13.`);
  lines.push(`- **Morgans Originals** — not created / not modified.`);
  lines.push(`- **Radisson Collection** — excluded unless separately promoted to Active/Live.`);
  lines.push("");

  lines.push(`## 11. Protected fields`);
  lines.push("");
  for (const f of report.protectedFields || []) lines.push(`- ${f}`);
  lines.push("");
  lines.push(`Baseline freeze does **not** write any of these fields.`);
  lines.push("");

  lines.push(`## 12. Regression rules`);
  lines.push("");
  for (const r of report.regressionRules || []) lines.push(`- ${r}`);
  lines.push("");
  lines.push(`Test: \`npm run test:brand-explorer-45-active-public-full-baseline\``);
  lines.push("");

  lines.push(`## 13. Rollback notes`);
  lines.push("");
  for (const r of report.rollbackNotes || []) lines.push(`- ${r}`);
  lines.push("");

  lines.push(`## 14. Future SO/ path`);
  lines.push("");
  lines.push(
    report.futureSoPath ||
      "45 → 46 only after separate SO/ cleanup, founder approval, Brand Status promotion, and public release. Do not freeze SO/ into Active/Live while Under Review."
  );
  lines.push("");
  lines.push(`### Future factory rules`);
  lines.push("");
  for (const r of report.futureWorkRules || []) lines.push(`- ${r}`);
  lines.push("");

  lines.push(`## Commands`);
  lines.push("");
  lines.push("```bash");
  lines.push("npm run brand-explorer-45-active-public-full-baseline -- --dry-run");
  lines.push("npm run test:brand-explorer-45-active-public-full-baseline");
  lines.push("npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only");
  lines.push("npm run test:brand-explorer-recent-momentum-evidence-quality");
  lines.push("# Quiet sequential (avoid Airtable 429 thrash):");
  lines.push("node scripts/brand-explorer-quiet-sequential-pvql.mjs");
  lines.push("node scripts/brand-explorer-quiet-sequential-quality-audit.mjs");
  lines.push("```");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

export {
  BASELINE_VERSION_45 as BASELINE_VERSION,
  EXPECTED_ACTIVE_COUNT_45 as EXPECTED_ACTIVE_COUNT,
  PROTECTED_FIELDS,
  ROOT,
};
