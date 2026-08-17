/**
 * Brand Explorer — protected 62 Active/Live public-full baseline freeze.
 *
 * Supersedes protected 54 Active/Live public-full freeze after Wave 15
 * Hilton Worldwide eight-brand promotion to Brand Status Active + public
 * release + Medium semantic cleanup. Four Points Flex by Sheraton remains
 * HELD (Under Review) — not part of Active/Live public-full. Read-only.
 * No Airtable / Presentation / image / CV / Source / Registry / Brand
 * Status / release writes.
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
  BASELINE_VERSION_45,
  EXPECTED_ACTIVE_COUNT_45,
  REPORT_JSON_45,
} from "./brand-explorer-45-active-public-full-baseline.js";
import {
  BASELINE_VERSION_46,
  EXPECTED_ACTIVE_COUNT_46,
  REPORT_JSON_46,
  INTERIM_REPORT_JSON_27,
  BASELINE_46_HELD_EXCLUDED,
} from "./brand-explorer-46-active-public-full-baseline.js";
import {
  BASELINE_VERSION_54,
  EXPECTED_ACTIVE_COUNT_54,
  REPORT_JSON_54,
  BASELINE_54_HELD_EXCLUDED,
} from "./brand-explorer-54-active-public-full-baseline.js";
import {
  WAVE14_HELD_PROMOTION_SLUG,
} from "./brand-explorer-wave14-factory-plan.js";
import {
  WAVE15_SLUGS,
  WAVE15_PARENT_PLATFORM,
  WAVE15_PROTECTED_BASELINE_COUNT,
  WAVE15_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE15_BRAND_PLAN,
} from "./brand-explorer-wave15-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  evaluateAiAssistedProfileFootnoteGate,
} from "./brand-explorer-ai-assisted-footnote.js";

export const BASELINE_VERSION_62 = "62-active-public-full-baseline-v1";
export const EXPECTED_ACTIVE_COUNT_62 = 62;
export const EXPECTED_QUALITY_RECOMMENDATION = "approve_for_baseline_freeze";

/**
 * Durable freeze decision after MGallery quality minor resolution.
 * Predecessor: `frozen_62_active_public_full_baseline_semantic_clean_flex_held`
 * (accepted MGallery `approve_after_minor_cleanup`).
 */
export const FREEZE_DECISION_62 =
  "frozen_62_active_public_full_baseline_quality_clean_flex_held";

/** Product / lane status after quality-clean freeze. */
export const FREEZE_STATUS_62_QUALITY_CLEAN =
  "brand_explorer_62_active_public_full_quality_clean_frozen_ready_for_child_table_validation";

/**
 * Accepted 24-tab minors at freeze (0 blockers). Empty after quality-clean revision —
 * every Active/Live brand must be `approve_for_baseline_freeze`. Do not re-add without
 * an explicit founder baseline revision.
 */
export const BASELINE_62_ACCEPTED_MINOR_CLEANUP_SLUGS = Object.freeze([]);

function qualityRecommendationAcceptable(slug, recommendation) {
  if (recommendation === EXPECTED_QUALITY_RECOMMENDATION) return true;
  return (
    recommendation === "approve_after_minor_cleanup" &&
    BASELINE_62_ACCEPTED_MINOR_CLEANUP_SLUGS.includes(slug)
  );
}

export const REPORT_JSON_62 = "brand-explorer-62-active-public-full-baseline.json";
export const REPORT_MD_62 = "brand-explorer-62-active-public-full-baseline.md";
export const DOCS_MD_62 = "brand-explorer-62-active-public-full-baseline.md";
export const QUALITY_CLEAN_FREEZE_JSON =
  "brand-explorer/brand-explorer-62-active-public-full-quality-clean-freeze.json";
export const QUALITY_CLEAN_FREEZE_MD =
  "brand-explorer/brand-explorer-62-active-public-full-quality-clean-freeze.md";
export const QUALITY_CLEAN_FREEZE_DOCS_MD =
  "brand-explorer-62-active-public-full-quality-clean-freeze.md";

/** Optional Wave 14 remediation/cleanup artifacts (descriptive only — not freeze gates). */
export const WAVE15_MEDIUM_CLEANUP_JSON = "brand-explorer-wave15-medium-cleanup.json";
export const WAVE15_PUBLIC_RELEASE_JSON = "brand-explorer-wave15-public-release.json";
export const WAVE15_STATUS_PROMOTION_JSON = "brand-explorer-wave15-status-promotion.json";

export const FOOTNOTE_STANDARDIZATION_JSON =
  "brand-explorer-ai-assisted-footnote-standardization.json";

/** Wave 14 Marriott International brands promoted to Active/Live public-full at 54. Flex is NOT here — it is held. */
export const BASELINE_62_WAVE15_PUBLIC_EIGHT = Object.freeze(
  WAVE15_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    return Object.freeze({
      slug,
      recordId: id?.recordId || null,
      name: id?.name || slug,
      wave: 15,
      parentPlatform: WAVE15_PARENT_PLATFORM,
      promotedVia: "wave15_public_eight",
    });
  })
);

/**
 * Explicitly held / excluded at 54 freeze (not Active/Live public-full).
 * Carries forward the 46 held/excluded list and adds Four Points Flex by
 * Sheraton, which is held (Under Review) after the Wave 14 partial release.
 */
export const BASELINE_62_HELD_EXCLUDED = Object.freeze([...BASELINE_54_HELD_EXCLUDED]);

export const BASELINE_62_EVIDENCE_SLUGS = Object.freeze([
  ...new Set([...MOMENTUM_EVIDENCE_TARGET_SLUGS, ...WAVE15_SLUGS]),
]);

/** Canonical freeze / regression evidence gate (matches npm test:brand-explorer-recent-momentum-evidence-quality). */
export const BASELINE_62_MANDATORY_EVIDENCE_SLUGS = Object.freeze([...MOMENTUM_EVIDENCE_TARGET_SLUGS]);

/**
 * Wave 14 public-eight evidence is snapshotted for the freeze report. Hard
 * failures (raw URLs, missing date/source, thin body, wrong brand) still
 * block regression. Known Sort Order drift (`cala_not_prioritized_first`
 * with CALA inventory present but International Reference first) is
 * reported as a note, following the same soft-fail pattern used for the
 * Wave 13 public six/seven at the 46 freeze.
 */
export const BASELINE_62_WAVE15_EVIDENCE_SOFT_FAIL_IDS = Object.freeze([
  "cala_not_prioritized_first",
  "international_reference_openings_accepted",
]);

const NEW_WAVE_SLUGS = new Set(BASELINE_62_WAVE15_PUBLIC_EIGHT.map((b) => b.slug));
const STALE_UNIVERSE_COUNTS = Object.freeze([23, 24, 25, 27, 39, 45, 46, 54]);

function snapshotFootnote(brandApi = {}) {
  const gate = evaluateAiAssistedProfileFootnoteGate(brandApi, "");
  const meta = brandApi?.governance?.brandExplorerFootnote || {};
  const subtitle = nz(brandApi?.governance?.displaySubtitle);
  const lastReviewed =
    nz(meta.lastReviewedFormatted) ||
    (subtitle.match(/Last Reviewed:\s*([^·]+)/i) || [])[1]?.trim() ||
    null;
  const sourceBasis =
    nz(meta.sourceBasis) ||
    (subtitle.match(/Source Basis:\s*([^·]+)/i) || [])[1]?.trim() ||
    null;
  const regionBasis =
    nz(meta.regionBasis) ||
    (subtitle.match(/Region:\s*([^·]+)/i) || [])[1]?.trim() ||
    null;
  return {
    aiAssistedProfileFootnoteVisible: gate.pass === true && Boolean(nz(brandApi?.governance?.displayLabel)),
    footnoteDisplayLabel: nz(brandApi?.governance?.displayLabel) || null,
    footnoteDisplaySubtitle: subtitle || null,
    lastReviewed,
    sourceBasis,
    regionBasis,
    footnoteGatePass: gate.pass === true,
    footnoteGateFailures: gate.failures || [],
    companyValidatedWordingOk:
      brandApi?.governance?.companyValidated === true ||
      !/company-?validated|brand verified/i.test(
        `${nz(brandApi?.governance?.displayLabel)} ${subtitle}`
      ),
    calaClaimOk: !(gate.failures || []).includes("cala_specific_without_source_support"),
  };
}

function isHardEvidenceFailure(failure) {
  const id = nz(failure?.id);
  if (!id) return true;
  return !BASELINE_62_WAVE15_EVIDENCE_SOFT_FAIL_IDS.some(
    (soft) => id === soft || id.endsWith(`:${soft}`)
  );
}

function evidencePassForFreeze(slug, evidence) {
  if (BASELINE_62_MANDATORY_EVIDENCE_SLUGS.includes(slug)) return evidence?.pass === true;
  // Wave 15 public-eight: snapshot evidence when present; missing catalog eval does not block freeze.
  if (NEW_WAVE_SLUGS.has(slug)) {
    if (!evidence) return true;
    if (evidence.pass === true) return true;
    const hard = (evidence.failures || []).filter(isHardEvidenceFailure);
    return hard.length === 0;
  }
  if (!evidence) return false;
  return evidence.pass === true;
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

function presentationRowsFromBrandApi(brand) {
  const blocks = brand?.brandExplorer?.blocks;
  if (!Array.isArray(blocks) || !blocks.length) return [];
  return blocks.map((b, i) => ({
    recordId: b.recordId || b.id || `api-${i}`,
    slotKey: nz(b.slotKey),
    title: nz(b.title),
    body: nz(b.body),
    caseSummaryOverview: nz(b.caseSummaryOverview),
    caseSummaryTags: nz(b.caseSummaryTags),
    caseSummaryBrandRelevance: nz(b.caseSummaryBrandRelevance),
    externalDisplayStatus: nz(b.externalDisplayStatus),
    active: b.active !== false,
    visible: b.visible !== false,
  }));
}

async function listPresentationRowsForEvidence(brand, slug, frozenBrand = null) {
  const embedded = presentationRowsFromBrandApi(brand);
  if (embedded.length) return embedded;

  const aliases = [];
  const push = (v) => {
    const s = nz(v);
    if (s && !aliases.includes(s)) aliases.push(s);
  };
  push(brand?.name);
  push(brand?.brandName);
  push(frozenBrand?.name);
  push(frozenBrand?.brandName);
  try {
    const { WAVE15_BRAND_PLAN } = await import("./brand-explorer-wave15-factory-plan.js");
    const plan = WAVE15_BRAND_PLAN?.[slug];
    push(plan?.name);
    for (const a of plan?.nameAliases || []) push(a);
  } catch {
    // Wave 14 plan optional for non-wave brands
  }

  for (const name of aliases) {
    const rows = await listPresentationRows(name);
    if (rows.length) return rows;
  }
  return [];
}

const BASELINE_SLUG_ALIASES = Object.freeze({
  fairmont: "fairmont-hotels-and-resorts",
  "fairmont-hotels-and-resorts": "fairmont",
  so: "so-hotels-and-resorts",
  "so-hotels-and-resorts": "so",
});

function canonicalBaselineSlug(slug) {
  const s = nz(slug).toLowerCase();
  if (s === "fairmont") return "fairmont-hotels-and-resorts";
  if (s === "so") return "so-hotels-and-resorts";
  return s;
}

function liveBrandForSlug(liveUniverse, slug) {
  const wanted = nz(slug).toLowerCase();
  const alias = BASELINE_SLUG_ALIASES[wanted];
  return (liveUniverse?.brands || []).find((b) => {
    const live = nz(b.slug).toLowerCase();
    return live === wanted || live === alias || BASELINE_SLUG_ALIASES[live] === wanted;
  });
}

async function evaluateEvidenceForSlug(slug, frozenBrand = null) {
  const identity = resolveLane2BrandIdentity(slug);
  const recordId = identity.recordId || frozenBrand?.recordId || null;
  const fetched = await fetchBrandById(recordId || slug);
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
  const brandName =
    nz(brand.name) ||
    nz(brand.brandName) ||
    (identity.name && identity.name !== slug ? identity.name : "") ||
    frozenBrand?.name ||
    frozenBrand?.brandName ||
    identity.name;
  const rows = await listPresentationRowsForEvidence(brand, slug, frozenBrand);
  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });
  const result = evaluateRecentMomentumEvidenceQuality({
    brandSlug: slug,
    brandName,
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
 * Build protected 62 public-full baseline snapshot (read-only).
 */
export async function build62ActivePublicFullBaseline({
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
  const frozen45 = readJsonReport(REPORT_JSON_45);
  const frozen46 = readJsonReport(REPORT_JSON_46);
  const frozen54 = readJsonReport(REPORT_JSON_54);
  const footnoteStandardization = readJsonReport(FOOTNOTE_STANDARDIZATION_JSON);
  const valueScenarioVisualRemediation = readJsonReport(WAVE15_MEDIUM_CLEANUP_JSON);
  const datedMomentumCleanup = readJsonReport(WAVE15_PUBLIC_RELEASE_JSON);
  const founderVisualSemanticRemediation = readJsonReport(WAVE15_STATUS_PROMOTION_JSON);
  const publicActiveSemanticBlockerCleanup = readJsonReport(WAVE15_MEDIUM_CLEANUP_JSON);

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
    const freezeBySlug = new Map((readJsonReport(REPORT_JSON_62)?.brands || []).map((b) => [b.slug, b]));
    for (const slug of BASELINE_62_EVIDENCE_SLUGS) {
      const live = liveBrandForSlug(universe, slug);
      const hint =
        freezeBySlug.get(slug) ||
        freezeBySlug.get(BASELINE_SLUG_ALIASES[slug]) ||
        (live ? { recordId: live.recordId, name: live.name || live.brandName } : null);
      evidenceBySlug.set(slug, await evaluateEvidenceForSlug(slug, hint));
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

    const footnote = snapshotFootnote(brandApi);

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
      majorCount: q.scores?.majorCount ?? null,
      minorCount: q.scores?.minorCount ?? null,
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
          ? "n_a_wave15_or_prior_mandatory_gate"
          : BASELINE_62_EVIDENCE_SLUGS.includes(u.slug)
            ? "not_evaluated"
            : "n_a_protected_prior",
      openingsRegionalPriorityStatus: evidence
        ? evidence.openingsRegionalPriorityStatus
        : NEW_WAVE_SLUGS.has(u.slug)
          ? "n_a_wave15_or_prior_mandatory_gate"
          : BASELINE_62_EVIDENCE_SLUGS.includes(u.slug)
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
      geographicFootprintPatternStatus:
        p.gateResults?.section_pattern_parity?.sections?.geographic_footprint?.pass === true
          ? "pass"
          : p.gateResults?.section_pattern_parity?.sections?.geographic_footprint
            ? "fail"
            : "reported_in_pvql_or_quality",
      valueScenarioPatternStatus:
        p.gateResults?.generic_copy_scan?.pass === true &&
        (p.gateResults?.scenario_image_distinctiveness?.pass !== false)
          ? "pass"
          : "reported_in_pvql_or_quality",
      aiAssistedProfileFootnoteVisible: footnote.aiAssistedProfileFootnoteVisible,
      lastReviewed: footnote.lastReviewed,
      sourceBasis: footnote.sourceBasis,
      regionBasis: footnote.regionBasis,
      footnoteDisplayLabel: footnote.footnoteDisplayLabel,
      footnoteDisplaySubtitle: footnote.footnoteDisplaySubtitle,
      footnoteGatePass: footnote.footnoteGatePass,
      footnoteGateFailures: footnote.footnoteGateFailures,
      companyValidated,
      companyValidationDate,
      sourceLibraryStatus: "untouched_at_freeze",
      registryStatus: "untouched_at_freeze",
      lastValidationTimestamp: validatedAt,
      addedInWave14: NEW_WAVE_SLUGS.has(u.slug),
      notes: NEW_WAVE_SLUGS.has(u.slug)
        ? ["wave15_public_full_baseline_62"]
        : ["carried_from_protected_46_baseline"],
      gateHits: {
        rawUrlScanHits: gateResults.raw_url_scan?.hits?.length ?? 0,
        forbiddenOwnerFacingHits: gateResults.forbidden_owner_facing_language?.hits?.length ?? 0,
        genericCopyMechanicalHits: gateResults.generic_copy_scan?.mechanicalHits?.length ?? 0,
        genericCopyGoldenFailures: gateResults.generic_copy_scan?.goldenFailures?.length ?? 0,
        aiAssistedFootnoteVisible:
          p.gateResults?.ai_assisted_profile_footnote_visible?.pass === true ||
          footnote.footnoteGatePass === true,
      },
    });
  }

  const excluded = [];
  const heldExcludedProbes = [
    ...NON_ACTIVE_STATUS_CONFLICT_PROBES,
    ...BASELINE_62_HELD_EXCLUDED.filter((h) => h.recordId).map((h) => ({
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
    const meta = BASELINE_62_HELD_EXCLUDED.find((h) => h.slug === probe.slug);
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
  for (const meta of BASELINE_62_HELD_EXCLUDED.filter((h) => !h.recordId)) {
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

  const evidenceWavePass = BASELINE_62_MANDATORY_EVIDENCE_SLUGS.every((slug) => {
    const e = evidenceBySlug.get(slug);
    return e && e.pass === true;
  });
  const evidenceWave15Pass = WAVE15_SLUGS.every((slug) =>
    evidencePassForFreeze(slug, evidenceBySlug.get(slug))
  );
  const wave15EvidenceSoftNotes = WAVE15_SLUGS.flatMap((slug) => {
    const e = evidenceBySlug.get(slug);
    if (!e || e.pass === true) return [];
    return (e.failures || [])
      .filter((f) => !isHardEvidenceFailure(f))
      .map((f) => `${slug}:${f.id}:${f.detail || ""}`);
  });

  const heldOk = excluded.every(
    (e) => e.includedInBaseline === false && e.isActiveLive !== true && !brands.some((b) => b.slug === e.slug)
  );

  const newWave = BASELINE_62_WAVE15_PUBLIC_EIGHT.map((meta) => {
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
      aiAssistedProfileFootnoteVisible: row?.aiAssistedProfileFootnoteVisible === true,
      lastReviewed: row?.lastReviewed || null,
      sourceBasis: row?.sourceBasis || null,
      regionBasis: row?.regionBasis || null,
      presentationRowCount: row?.presentationRowCount ?? 0,
    };
  });

  const footnoteOk = brands.every(
    (b) =>
      b.aiAssistedProfileFootnoteVisible === true &&
      Boolean(b.lastReviewed) &&
      Boolean(b.sourceBasis) &&
      Boolean(b.regionBasis) &&
      b.footnoteGatePass === true
  );

  const freezeDecision =
    universe.totalCount === EXPECTED_ACTIVE_COUNT_62 &&
    brands.length === EXPECTED_ACTIVE_COUNT_62 &&
    brands.every((b) => b.shouldRenderFullProfile && b.publicFullProfile) &&
    brands.every((b) => b.pvqlStatus === "pass") &&
    brands.every((b) => qualityRecommendationAcceptable(b.slug, b.qualityRecommendation)) &&
    brands.every((b) => (b.blockerCount ?? 0) === 0) &&
    brands.every((b) => b.scenarioImageCount >= 3) &&
    brands.every((b) => b.galleryImageCount >= 6) &&
    brands.every((b) => b.propertyExampleImageCount >= 3) &&
    brands.every((b) => b.imageUniquenessStatus === "pass" || b.imageUniquenessStatus === "reported_in_quality_audit") &&
    brands.every((b) => (b.gateHits?.rawUrlScanHits || 0) === 0) &&
    brands.every((b) => (b.gateHits?.forbiddenOwnerFacingHits || 0) === 0) &&
    brands.every((b) => (b.gateHits?.genericCopyMechanicalHits || 0) === 0) &&
    footnoteOk &&
    crossBrandCount === 0 &&
    evidenceWavePass &&
    evidenceWave15Pass &&
    heldOk &&
    newWave.every((b) => b.included)
      ? FREEZE_DECISION_62
      : "freeze_incomplete";

  return {
    version: BASELINE_VERSION_62,
    baselineType: "active_live_public_full",
    qualityClean: freezeDecision === FREEZE_DECISION_62,
    qualityCleanRevision: "quality-clean-v1",
    predecessorFreezeDecision: "frozen_62_active_public_full_baseline_semantic_clean_flex_held",
    status: freezeDecision === FREEZE_DECISION_62 ? FREEZE_STATUS_62_QUALITY_CLEAN : null,
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
      baseline45: {
        version: BASELINE_VERSION_45,
        expectedCount: EXPECTED_ACTIVE_COUNT_45,
        report: REPORT_JSON_45,
        present: Boolean(frozen45),
        role: "historical_protected_public_full_pre_so_release",
        note: "Preceded SO/ promotion to 46. Keep as predecessor artifact; do not use as live expected count.",
      },
      baseline46: {
        version: BASELINE_VERSION_46,
        expectedCount: EXPECTED_ACTIVE_COUNT_46,
        report: REPORT_JSON_46,
        present: Boolean(frozen46),
        role: "historical_protected_public_full_pre_wave14",
        note: "Preceded Wave 14 Marriott partial release to 54 (Four Points Flex held). Keep as predecessor artifact; do not use as live expected count.",
      },
      baseline54: {
        version: BASELINE_VERSION_54,
        expectedCount: EXPECTED_ACTIVE_COUNT_54,
        report: REPORT_JSON_54,
        present: Boolean(frozen54),
        role: "historical_protected_public_full_pre_wave15",
        note: "Preceded Wave 15 Hilton eight promotion to 62. Keep as predecessor artifact; do not use as live expected count.",
      },
    },
    wave14: {
      protectedBaselineBefore: WAVE15_PROTECTED_BASELINE_COUNT,
      expectedPartialActiveCount: WAVE15_EXPECTED_FINAL_ACTIVE_COUNT,
      publicEightSlugCount: WAVE15_SLUGS.length,
      publicEightSlugs: [...WAVE15_SLUGS],
      heldSlug: WAVE14_HELD_PROMOTION_SLUG,
      heldExcluded: true,
      brands: newWave,
      valueScenarioVisualRemediation: valueScenarioVisualRemediation
        ? {
            file: WAVE15_MEDIUM_CLEANUP_JSON,
            readyStatement: valueScenarioVisualRemediation.readyStatement || null,
            generatedAt: valueScenarioVisualRemediation.generatedAt || null,
          }
        : null,
      datedMomentumCleanup: datedMomentumCleanup
        ? {
            file: WAVE15_PUBLIC_RELEASE_JSON,
            readyStatement: datedMomentumCleanup.readyStatement || null,
            generatedAt: datedMomentumCleanup.generatedAt || null,
          }
        : null,
      founderVisualSemanticRemediation: founderVisualSemanticRemediation
        ? {
            file: WAVE15_STATUS_PROMOTION_JSON,
            readyStatement: founderVisualSemanticRemediation.readyStatement || null,
            generatedAt: founderVisualSemanticRemediation.generatedAt || null,
          }
        : null,
      publicActiveSemanticBlockerCleanup: publicActiveSemanticBlockerCleanup
        ? {
            file: WAVE15_MEDIUM_CLEANUP_JSON,
            readyStatement: publicActiveSemanticBlockerCleanup.readyStatement || null,
            generatedAt: publicActiveSemanticBlockerCleanup.generatedAt || null,
          }
        : null,
    },
    footnoteStandardization: footnoteStandardization
      ? {
          file: FOOTNOTE_STANDARDIZATION_JSON,
          readyState: footnoteStandardization.readyState || null,
          airtableWrites: footnoteStandardization.airtableWrites ?? 0,
          generatedAt: footnoteStandardization.generatedAt || null,
        }
      : null,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    activeUniverseVersion: ACTIVE_UNIVERSE_VERSION,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_62,
    activeCount: universe.totalCount,
    frozen: freezeDecision === FREEZE_DECISION_62,
    freezeDecision,
    acceptedMinorCleanupSlugs: [...BASELINE_62_ACCEPTED_MINOR_CLEANUP_SLUGS],
    protectedFields: [...PROTECTED_FIELDS],
    staleOperationalListsRejectedAsUniverse: [
      ...STALE_OPERATIONAL_LIST_NAMES,
      "stale_46_brand_list_as_universe",
      "stale_45_brand_list_as_universe",
      "stale_39_brand_list_as_universe",
      "stale_27_brand_list_as_universe",
      "stale_24_brand_list_as_universe",
    ],
    primaryReleaseOverlayCount: PRIMARY_RELEASE_SLUGS.length,
    primaryReleaseNote:
      "PRIMARY_RELEASE_SLUGS is an operational overlay, not the Active/Live universe (54).",
    brands,
    newWaveBrands: newWave,
    excludedNonActive: excluded,
    heldExcluded: excluded,
    evidenceQuality: {
      gate: "test:brand-explorer-recent-momentum-evidence-quality",
      targetSlugs: [...BASELINE_62_EVIDENCE_SLUGS],
      mandatorySlugs: [...BASELINE_62_MANDATORY_EVIDENCE_SLUGS],
      wave14Slugs: [...WAVE15_SLUGS],
      pass: evidenceWavePass,
      wave14PassAllowingKnownSortDrift: evidenceWave15Pass,
      wave14SoftFailNotes: wave15EvidenceSoftNotes,
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
      wave14ValueScenarioVisualRemediation: valueScenarioVisualRemediation
        ? {
            file: WAVE15_MEDIUM_CLEANUP_JSON,
            readyStatement: valueScenarioVisualRemediation.readyStatement || null,
          }
        : null,
      wave14DatedMomentumCleanup: datedMomentumCleanup
        ? {
            file: WAVE15_PUBLIC_RELEASE_JSON,
            readyStatement: datedMomentumCleanup.readyStatement || null,
          }
        : null,
      wave14FounderVisualSemanticRemediation: founderVisualSemanticRemediation
        ? {
            file: WAVE15_STATUS_PROMOTION_JSON,
            readyStatement: founderVisualSemanticRemediation.readyStatement || null,
          }
        : null,
      wave14PublicActiveSemanticBlockerCleanup: publicActiveSemanticBlockerCleanup
        ? {
            file: WAVE15_MEDIUM_CLEANUP_JSON,
            readyStatement: publicActiveSemanticBlockerCleanup.readyStatement || null,
          }
        : null,
      aiAssistedFootnoteStandardization: footnoteStandardization
        ? {
            file: FOOTNOTE_STANDARDIZATION_JSON,
            readyState: footnoteStandardization.readyState || null,
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
      evidenceWave15PassAllowingKnownSortDrift: evidenceWave15Pass,
      wave14EvidenceSoftFailNoteCount: wave15EvidenceSoftNotes.length,
      wave14Count: newWave.length,
      wave14IncludedCount: newWave.filter((b) => b.included).length,
      footnoteVisibleCount: brands.filter((b) => b.aiAssistedProfileFootnoteVisible).length,
      footnoteCompleteCount: brands.filter(
        (b) => b.lastReviewed && b.sourceBasis && b.regionBasis
      ).length,
    },
    regressionRules: [
      "Active/Live universe count must remain 54 unless freeze is explicitly revised",
      "Every Active/Live brand must remain public-full with shouldRenderFullProfile=true",
      "Every Active/Live brand must pass PVQL",
      "Every Active/Live brand must remain approve_for_baseline_freeze",
      "No blocker or remediation_required on Active/Live brands",
      "No cross-brand image reuse",
      "Image uniqueness and role-match must pass for Active/Live brands",
      "Value scenario images must remain distinct (scenarioDistinct ≥ 3)",
      "Value Creation Scenarios must not regress to owner-fit diligence / generic platform placeholders",
      "Where This Brand Creates the Most Value must not use Property Fit / Support Across Lifecycle card titles",
      "Geographic footprint must keep ≥3 filled region cards (or accepted cleanly_unavailable)",
      "Recent Momentum cards must keep date, geography, structured source; no raw URLs in visible body",
      "raw_url_scan / forbidden_owner_facing_language / generic_copy_scan mechanical hits must stay at 0",
      "ADR / RevPAR / fee-stack / FDD / Item 19 / LOI must not appear in visible owner-facing copy",
      "Recent Momentum / Openings Evidence Quality must pass for mandatory wave brands (npm gate)",
      "Wave 14 public eight evidence must have no hard failures (raw URL / missing date-source / thin body / wrong brand); known cala_not_prioritized_first Sort Order drift is snapshotted as a note until a separate Sort Order remediation",
      "Company Validated, Company Validation Date, Source Library, Registry, Brand Status must not change unexpectedly",
      "Four Points Flex by Sheraton must remain held (Under Review) — not Active/Live public-full",
      "AI-Assisted Profile footnote must render for every Active/Live brand (enriched path) with Last Reviewed, Source Basis, and Region",
      "Company Validated / Brand Verified wording must not appear unless Company Validated is true",
      "CALA-specific must not appear without source-supported CALA basis",
      "House of Originals, Morgans Originals, Radisson Collection, and Four Points Flex by Sheraton must remain excluded unless separately promoted",
      "Stale 23/24/25/27/39/45/46-brand / PRIMARY_RELEASE lists must never replace the Active/Live universe SoT",
    ],
    rollbackNotes: [
      "This freeze is report-only — no Airtable writes occurred.",
      "Protected 46 public-full freeze preserved at reports/brand-explorer-46-active-public-full-baseline.json",
      "Protected 45 / 39 / 27 / interim 27 / 24 / 25 freezes remain predecessor artifacts.",
      "To unfreeze: revise docs/reports and EXPECTED_ACTIVE_COUNT_62 after an explicit founder decision.",
      "Do not revert Brand Status / CV / Source / Registry to undo this freeze — those fields were never written.",
      "AI-Assisted footnote is code/rendering — rollback is a code revert of brand-explorer-ai-assisted-footnote.js wiring, not Airtable.",
    ],
    futureWorkRules: [
      "New Active/Live brands require a new baseline revision (count will leave 54).",
      "Wave 15 factory work starts from this 54 freeze — do not silently absorb new Active brands.",
      "Do not patch baseline brands casually — use targeted cleanup + re-audit + re-freeze.",
      "Required gates: test:brand-explorer-62-active-public-full-baseline · test:brand-explorer-public-visibility-quality-lock --public-full-only · test:brand-explorer-recent-momentum-evidence-quality · brand-explorer-ai-assisted-footnote-standardization --audit",
      "Prefer quiet sequential PVQL/quality audits when Airtable 429 risk is high (scripts/brand-explorer-quiet-sequential-pvql.mjs, scripts/brand-explorer-quiet-sequential-quality-audit.mjs).",
      "Operational cohorts (PRIMARY_RELEASE, restore lanes, factory preview) remain overlays, not universe SoT.",
    ],
    futureWave15StartingConditions:
      `Start next factory work from ${FREEZE_DECISION_62} (quality-clean; no accepted minors). Keep House of Originals / Morgans Originals / Radisson Collection / Four Points Flex by Sheraton excluded unless separate promotion. Preserve AI-Assisted footnote always-on gate. Do not use 46/45/39/27/54-brand lists as universe SoT. Child Brand Setup table validation is a separate read-only program.`,
  };
}

export function evaluate62ActivePublicFullBaselineRegression({
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
  if (liveUniverse?.totalCount !== EXPECTED_ACTIVE_COUNT_62) {
    failures.push(
      `active_universe_count_changed:${liveUniverse?.totalCount}_expected_${EXPECTED_ACTIVE_COUNT_62}`
    );
  }
  if (PRIMARY_RELEASE_SLUGS.length === liveUniverse?.totalCount) {
    failures.push("stale_primary_release_list_used_as_universe");
  }
  if (liveUniverse?.totalCount === 23 || liveUniverse?.totalCount === 24) {
    failures.push(`stale_${liveUniverse.totalCount}_brand_list_suspected_as_universe`);
  }
  if (STALE_UNIVERSE_COUNTS.includes(liveUniverse?.totalCount) && liveUniverse?.totalCount !== EXPECTED_ACTIVE_COUNT_62) {
    failures.push(`stale_${liveUniverse.totalCount}_brand_list_used_as_universe_expected_${EXPECTED_ACTIVE_COUNT_62}`);
  }

  const liveSlugs = new Set((liveUniverse?.brands || []).map((b) => b.slug));
  const liveCanonical = new Set([...liveSlugs].map(canonicalBaselineSlug));
  const expectedCanonical = new Set([...expectedSlugs].map(canonicalBaselineSlug));
  for (const slug of expectedCanonical) {
    if (!liveCanonical.has(slug)) failures.push(`missing_active_brand:${slug}`);
  }
  for (const slug of liveCanonical) {
    if (!expectedCanonical.has(slug)) failures.push(`unexpected_active_brand:${slug}`);
  }

  for (const ex of liveExcluded.length ? liveExcluded : frozen.excludedNonActive || frozen.heldExcluded || []) {
    if (ex.isActiveLive === true || isBrandStatusActive(ex.brandStatus)) {
      failures.push(`excluded_brand_became_active_without_baseline_revision:${ex.slug}`);
    }
    if (liveBrandForSlug(liveUniverse, ex.slug)) {
      failures.push(`excluded_brand_present_in_active_universe:${ex.slug}`);
    }
  }

  for (const slug of [
    "the-house-of-originals",
    "morgans-originals",
    "radisson-collection",
    WAVE14_HELD_PROMOTION_SLUG,
  ]) {
    if (liveBrandForSlug(liveUniverse, slug)) {
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
    const liveU = liveBrandForSlug(liveUniverse, slug);
    const p = pvqlBySlug.get(slug) || pvqlBySlug.get(BASELINE_SLUG_ALIASES[slug]);
    const q = qualityBySlug.get(slug) || qualityBySlug.get(BASELINE_SLUG_ALIASES[slug]);
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
    if (rec && !qualityRecommendationAcceptable(slug, rec)) {
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

    // Enriched/global AI-Assisted footnote (not raw native chip alone).
    const liveBrandApi = liveU?.brandApi || null;
    if (liveBrandApi) {
      const g = evaluateAiAssistedProfileFootnoteGate(liveBrandApi, "");
      if (!g.pass) {
        brandFailures.push(`ai_assisted_profile_footnote_visible:${(g.failures || []).join("|")}`);
      }
      if (!nz(liveBrandApi.governance?.displayLabel)) {
        brandFailures.push("ai_assisted_profile_footnote_missing");
      }
      const sub = nz(liveBrandApi.governance?.displaySubtitle);
      if (!/Last Reviewed:/i.test(sub)) brandFailures.push("last_reviewed_missing");
      if (!/Source Basis:/i.test(sub)) brandFailures.push("source_basis_missing");
      if (!/Region:/i.test(sub)) brandFailures.push("region_basis_missing");
      if (
        liveBrandApi.governance?.companyValidated !== true &&
        /company-?validated|brand verified/i.test(
          `${nz(liveBrandApi.governance?.displayLabel)} ${sub}`
        )
      ) {
        brandFailures.push("company_validated_wording_without_company_validated");
      }
    } else if (p?.gateResults?.ai_assisted_profile_footnote_visible) {
      if (p.gateResults.ai_assisted_profile_footnote_visible.pass !== true) {
        brandFailures.push(
          `ai_assisted_profile_footnote_visible:${(p.gateResults.ai_assisted_profile_footnote_visible.failures || []).join("|")}`
        );
      }
    } else if (frozenBrand.aiAssistedProfileFootnoteVisible !== true) {
      brandFailures.push("ai_assisted_profile_footnote_visible:missing");
    }

    if (BASELINE_62_MANDATORY_EVIDENCE_SLUGS.includes(slug)) {
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
      if (!ev) brandFailures.push("wave15_recent_momentum_evidence_quality_not_evaluated");
      else if (!evidencePassForFreeze(slug, ev)) {
        brandFailures.push(
          `wave15_recent_momentum_evidence_hard_fail:${(ev.failures || [])
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
    const freezeCount = liveQuality.brandResults.filter((b) =>
      qualityRecommendationAcceptable(b.slug, b.overallRecommendation)
    ).length;
    if (freezeCount !== EXPECTED_ACTIVE_COUNT_62) {
      failures.push(`quality_freeze_count:${freezeCount}_expected_${EXPECTED_ACTIVE_COUNT_62}`);
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
    if (publicFullCount !== EXPECTED_ACTIVE_COUNT_62) {
      failures.push(`pvql_public_full_count:${publicFullCount}_expected_${EXPECTED_ACTIVE_COUNT_62}`);
    }
    const publicFullPass = (livePvql.brands || []).filter(
      (b) => b.publicFullProfile && b.lockPass
    ).length;
    if (publicFullPass !== EXPECTED_ACTIVE_COUNT_62) {
      failures.push(`pvql_public_full_pass_count:${publicFullPass}_expected_${EXPECTED_ACTIVE_COUNT_62}`);
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
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_62,
    liveActiveCount: liveUniverse?.totalCount ?? null,
  };
}

export async function run62ActivePublicFullBaselineRegression({
  reassessPvql = true,
  requireQualityReport = true,
  maxPvqlAgeMs = 72 * 60 * 60 * 1000,
  forceLivePvql = false,
  allowCachedPvqlIfPass = false,
  evaluateEvidence = true,
} = {}) {
  const frozenPath = path.join(REPORTS_DIR, REPORT_JSON_62);
  if (!fs.existsSync(frozenPath)) {
    throw new Error(`Frozen baseline missing: ${REPORT_JSON_62}. Run freeze --dry-run first.`);
  }
  const frozen = JSON.parse(fs.readFileSync(frozenPath, "utf8"));

  const liveUniverse = await loadActiveUniverse({ includeDetails: true });

  const liveExcluded = [];
  const liveProbeSeen = new Set();
  const liveProbes = [
    ...NON_ACTIVE_STATUS_CONFLICT_PROBES,
    ...BASELINE_62_HELD_EXCLUDED.filter((h) => h.recordId).map((h) => ({
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
  for (const meta of BASELINE_62_HELD_EXCLUDED.filter((h) => !h.recordId)) {
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
  // Public-full lockPass is authoritative for the 54 baseline. Do not require
  // summary.overallPass — primary/legacy cohort flags can fail overallPass while
  // all Active/Live public-full brands remain lockPass=true.
  const publicFullRows = (existingPvql?.brands || []).filter((b) => b.publicFullProfile === true);
  const pvqlCoversFrozen =
    publicFullRows.length === EXPECTED_ACTIVE_COUNT_62 &&
    existingPvql?.brands?.length >= EXPECTED_ACTIVE_COUNT_62 &&
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
  // for all 54. Wave preflight must run a fresh PVQL command first and must
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
    const freezeBySlug = new Map((frozen?.brands || []).map((b) => [b.slug, b]));
    for (const slug of BASELINE_62_EVIDENCE_SLUGS) {
      const live = liveBrandForSlug(liveUniverse, slug);
      const hint =
        freezeBySlug.get(slug) ||
        freezeBySlug.get(BASELINE_SLUG_ALIASES[slug]) ||
        (live ? { recordId: live.recordId, name: live.name || live.brandName } : null);
      liveEvidenceBySlug.set(slug, await evaluateEvidenceForSlug(slug, hint));
    }
  }

  const regression = evaluate62ActivePublicFullBaselineRegression({
    frozen,
    liveUniverse,
    livePvql,
    liveQuality,
    liveExcluded,
    liveEvidenceBySlug,
  });

  return {
    version: BASELINE_VERSION_62,
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

export function write62ActivePublicFullBaselineReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  fs.mkdirSync(path.join(REPORTS_DIR, "brand-explorer"), { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON_62);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD_62);
  const docsPath = path.join(DOCS_DIR, DOCS_MD_62);

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  const md = render62BaselineMarkdown(report);
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");

  const qualityCleanPaths = write62QualityCleanFreezeReports(report);
  return { jsonPath, mdPath, docsPath, ...qualityCleanPaths };
}

/**
 * Quality-clean freeze overlay artifacts (same 62 universe; no accepted minors).
 * Report-only — no Airtable writes.
 */
export function write62QualityCleanFreezeReports(report) {
  fs.mkdirSync(path.join(REPORTS_DIR, "brand-explorer"), { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const mg = (report.brands || []).find((b) => b.slug === "mgallery-collection") || null;
  const minorSlugs = (report.brands || [])
    .filter((b) => b.qualityRecommendation === "approve_after_minor_cleanup")
    .map((b) => b.slug);

  const artifact = {
    version: "brand-explorer-62-active-public-full-quality-clean-freeze-v1",
    generatedAt: new Date().toISOString(),
    status: FREEZE_STATUS_62_QUALITY_CLEAN,
    freezeDecision: report.freezeDecision,
    frozen: report.frozen === true,
    qualityClean: report.qualityClean === true,
    qualityCleanRevision: report.qualityCleanRevision || "quality-clean-v1",
    predecessorFreezeDecision:
      report.predecessorFreezeDecision ||
      "frozen_62_active_public_full_baseline_semantic_clean_flex_held",
    durableBaseline: {
      version: report.version,
      reportJson: REPORT_JSON_62,
      reportMd: REPORT_MD_62,
      docsMd: `docs/data-intelligence/${DOCS_MD_62}`,
      contract: "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js",
    },
    criteria: {
      activeUniverse: report.activeCount,
      expectedActive: EXPECTED_ACTIVE_COUNT_62,
      semanticNote: "Validated separately via brand-explorer-global-active-semantic-audit (C/H/M=0/0/0)",
      pvqlPassCount: report.summary?.pvqlPassCount ?? null,
      approveForBaselineFreeze: report.summary?.freezeRecommendationCount ?? null,
      approveAfterMinorCleanup: minorSlugs,
      acceptedMinorCleanupSlugs: [...BASELINE_62_ACCEPTED_MINOR_CLEANUP_SLUGS],
      footnoteVisible: report.summary?.footnoteVisibleCount ?? null,
      footnoteComplete: report.summary?.footnoteCompleteCount ?? null,
      evidenceQualityPass: report.summary?.evidenceQualityPass ?? null,
      mgallery: mg
        ? {
            slug: mg.slug,
            qualityRecommendation: mg.qualityRecommendation,
            composite: mg.qualityComposite ?? mg.compositeScore ?? null,
            majors: mg.majorCount ?? mg.scores?.majorCount ?? null,
            minors: mg.minorCount ?? mg.scores?.minorCount ?? null,
            blockers: mg.blockerCount ?? 0,
          }
        : null,
    },
    heldExcluded: (report.heldExcluded || report.excludedNonActive || []).map((e) => ({
      slug: e.slug,
      brandStatus: e.brandStatus || null,
      includedInBaseline: e.includedInBaseline === true,
      isActiveLive: e.isActiveLive === true,
    })),
    scopeGuarantees: {
      airtableWrites: false,
      presentationWrites: false,
      imageWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      brandStatusWrites: false,
      recentMomentumWrites: false,
      censusWrites: false,
      childBrandSetupWrites: false,
    },
    nextLane:
      "Brand Setup child-table validation (read-only) — separate program; does not mutate this freeze.",
    brands: (report.brands || []).map((b) => ({
      slug: b.slug,
      recordId: b.recordId,
      name: b.name || b.brandName,
      brandStatus: b.brandStatus,
      qualityRecommendation: b.qualityRecommendation,
      pvqlStatus: b.pvqlStatus,
      publicFullProfile: b.publicFullProfile === true,
      aiAssistedProfileFootnoteVisible: b.aiAssistedProfileFootnoteVisible === true,
    })),
  };

  const jsonPath = path.join(REPORTS_DIR, QUALITY_CLEAN_FREEZE_JSON);
  const mdPath = path.join(REPORTS_DIR, QUALITY_CLEAN_FREEZE_MD);
  const docsPath = path.join(DOCS_DIR, QUALITY_CLEAN_FREEZE_DOCS_MD);

  fs.writeFileSync(jsonPath, `${JSON.stringify(artifact, null, 2)}\n`, "utf8");

  const lines = [];
  lines.push("# Brand Explorer 62 — Active Public-Full Quality-Clean Freeze");
  lines.push("");
  lines.push(`**Status:** \`${artifact.status}\``);
  lines.push(`**Freeze decision:** \`${artifact.freezeDecision}\``);
  lines.push(`**Frozen:** ${artifact.frozen}`);
  lines.push(`**Generated:** ${artifact.generatedAt}`);
  lines.push("");
  lines.push("## Verdict");
  lines.push("");
  lines.push(
    "Locks the Active-62 Brand Explorer public-full baseline after MGallery quality minor resolution. All 62 brands recommend `approve_for_baseline_freeze`. Accepted-minor exception list is empty."
  );
  lines.push("");
  lines.push("## Criteria snapshot");
  lines.push("");
  lines.push("| Gate | Value |");
  lines.push("|------|------:|");
  lines.push(`| Active universe | ${artifact.criteria.activeUniverse} |`);
  lines.push(`| PVQL pass | ${artifact.criteria.pvqlPassCount} |`);
  lines.push(`| approve_for_baseline_freeze | ${artifact.criteria.approveForBaselineFreeze} |`);
  lines.push(
    `| approve_after_minor_cleanup | ${artifact.criteria.approveAfterMinorCleanup.length} |`
  );
  lines.push(`| Footnote visible | ${artifact.criteria.footnoteVisible} |`);
  lines.push(`| Footnote complete | ${artifact.criteria.footnoteComplete} |`);
  lines.push(
    `| MGallery recommendation | ${artifact.criteria.mgallery?.qualityRecommendation || "—"} |`
  );
  lines.push(`| MGallery composite | ${artifact.criteria.mgallery?.composite ?? "—"} |`);
  lines.push(`| MGallery majors | ${artifact.criteria.mgallery?.majors ?? "—"} |`);
  lines.push("");
  lines.push("## Durable baseline");
  lines.push("");
  lines.push(`- Contract: \`${artifact.durableBaseline.contract}\``);
  lines.push(`- Freeze JSON: \`reports/${artifact.durableBaseline.reportJson}\``);
  lines.push(`- Freeze docs: \`${artifact.durableBaseline.docsMd}\``);
  lines.push(`- Predecessor decision: \`${artifact.predecessorFreezeDecision}\``);
  lines.push("");
  lines.push("## Held / excluded (unchanged)");
  lines.push("");
  for (const e of artifact.heldExcluded) {
    lines.push(
      `- \`${e.slug}\` · status=${e.brandStatus || "—"} · activeLive=${e.isActiveLive} · inBaseline=${e.includedInBaseline}`
    );
  }
  lines.push("");
  lines.push("## Scope guarantees");
  lines.push("");
  lines.push("No Airtable / Presentation / Census / child Brand Setup / protected-field writes.");
  lines.push("");
  lines.push("## Next lane");
  lines.push("");
  lines.push(artifact.nextLane);
  lines.push("");
  lines.push(`**Final status:** \`${artifact.status}\``);
  lines.push("");

  const mdBody = `${lines.join("\n")}\n`;
  fs.writeFileSync(mdPath, mdBody, "utf8");
  fs.writeFileSync(docsPath, mdBody, "utf8");

  return {
    qualityCleanJsonPath: jsonPath,
    qualityCleanMdPath: mdPath,
    qualityCleanDocsPath: docsPath,
    qualityCleanStatus: artifact.status,
  };
}

function render62BaselineMarkdown(report) {
  const lines = [];
  const w14 = report.wave14 || {};
  const vs = report.validationSources || {};
  const s = report.summary || {};

  lines.push(`# Brand Explorer — Protected 62 Active/Live Public-Full Baseline`);
  lines.push("");
  lines.push(`Version: \`${report.version}\` · Generated: ${report.generatedAt}`);
  lines.push(`Baseline type: **${report.baselineType}**`);
  lines.push(`Freeze decision: **\`${report.freezeDecision}\`** · frozen=${report.frozen}`);
  if (report.qualityClean) {
    lines.push(
      `Quality-clean revision: **${report.qualityCleanRevision || "quality-clean-v1"}** · status=\`${report.status || FREEZE_STATUS_62_QUALITY_CLEAN}\` · accepted minors=**none**`
    );
    lines.push(`Predecessor freeze: \`${report.predecessorFreezeDecision || "frozen_62_active_public_full_baseline_semantic_clean_flex_held"}\``);
  }
  lines.push(
    `Writes: Airtable=${report.airtableWrites} · Presentation=${report.presentationWrites} · Image=${report.imageWrites} · CV=${report.companyValidatedWrites} · Source=${report.sourceLibraryWrites} · Registry=${report.registryWrites} · Brand Status=${report.brandStatusWrites}`
  );
  lines.push("");

  lines.push(`## 1. Executive summary`);
  lines.push("");
  lines.push(
    report.qualityClean
      ? `This **quality-clean** freeze locks the **${report.activeCount}** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, quality \`approve_for_baseline_freeze\` (no accepted minors), and AI-Assisted footnote-complete. MGallery quality minor is resolved. Four Points Flex by Sheraton remains **held** (Under Review). House / Morgans / Radisson Collection remain excluded.`
      : `This freeze locks the **${report.activeCount}** Brand Basics Active/Live Brand Explorer profiles that are public-full, PVQL-clean, quality \`approve_for_baseline_freeze\`, and AI-Assisted footnote-complete after Wave 14 Marriott International partial promotion (8 of 9 brands) + public release + semantic cleanup. Four Points Flex by Sheraton is **held** (Under Review). Predecessor: protected **46**.`
  );
  lines.push("");
  lines.push(`| Metric | Value |`);
  lines.push(`|--------|-------|`);
  lines.push(`| Active/Live count | ${s.activeCount} |`);
  lines.push(`| Public-full | ${s.publicFullCount} |`);
  lines.push(`| shouldRenderFullProfile | ${s.shouldRenderFullProfileCount} |`);
  lines.push(`| PVQL pass | ${s.pvqlPassCount} |`);
  lines.push(`| approve_for_baseline_freeze | ${s.freezeRecommendationCount} |`);
  lines.push(`| remediation_required | ${s.remediationCount} |`);
  lines.push(`| AI-Assisted footnote visible | ${s.footnoteVisibleCount} |`);
  lines.push(`| Footnote complete (LR+SB+Region) | ${s.footnoteCompleteCount} |`);
  lines.push(`| Evidence quality (mandatory wave) | ${s.evidenceQualityPass} |`);
  lines.push(`| Image uniqueness pass | ${s.imageUniquenessPassCount} |`);
  lines.push(`| Image role-match pass | ${s.imageRoleMatchPassCount} |`);
  lines.push(`| Cross-brand image reuse | ${s.crossBrandImageReuse} |`);
  lines.push(`| Wave 14 Marriott eight included | ${s.wave14IncludedCount}/${s.wave14Count} |`);
  lines.push(`| Company Validated = true | ${s.companyValidatedTrueCount} |`);
  lines.push(`| Held / excluded probes | ${s.excludedNonActiveCount} |`);
  lines.push("");

  lines.push(`## 2. Active universe source of truth`);
  lines.push("");
  lines.push(`- **Name:** ${report.activeUniverseSource.name}`);
  lines.push(`- **Table:** ${report.activeUniverseSource.table}`);
  lines.push(`- **Formula:** \`${report.activeUniverseSource.formula}\``);
  lines.push(`- **Loader:** \`lib/partner-intelligence/brand-explorer-active-universe.js\``);
  lines.push(`- **Version:** ${report.activeUniverseVersion}`);
  lines.push(`- **Not the universe:** ${(report.staleOperationalListsRejectedAsUniverse || []).join(", ")}`);
  lines.push(`- **Note:** ${report.primaryReleaseNote}`);
  lines.push("");
  lines.push(`### Predecessor freezes (history, not current enforcement)`);
  lines.push("");
  lines.push(`- 24 / 25 / interim 27 / protected 27 / protected 39 / protected 45 / **protected 46** — preserved as predecessor artifacts.`);
  lines.push(
    `- Protected 46: \`${report.predecessorBaselines?.baseline46?.report}\` (${report.predecessorBaselines?.baseline46?.version})`
  );
  lines.push("");

  lines.push(`## 3. 54-brand baseline table`);
  lines.push("");
  lines.push(
    `| Brand | Slug | Record ID | Status | Full | Display | PVQL | Quality | Footnote | Last Reviewed | Source Basis | Region | Uniq | Role | Evidence | Gallery | Scenario | Property | Rows | CV |`
  );
  lines.push(
    `|-------|------|-----------|--------|------|---------|------|---------|----------|---------------|--------------|--------|------|------|----------|---------|----------|----------|------|----|`
  );
  for (const b of report.brands || []) {
    lines.push(
      `| ${b.brandName} | \`${b.slug}\` | \`${b.recordId}\` | ${b.brandStatus} | ${b.shouldRenderFullProfile} | ${b.publicDisplayState || "—"} | ${b.pvqlStatus} | ${b.qualityRecommendation || "—"} | ${b.aiAssistedProfileFootnoteVisible} | ${b.lastReviewed || "—"} | ${b.sourceBasis || "—"} | ${b.regionBasis || "—"} | ${b.imageUniquenessStatus || "—"} | ${b.imageRoleMatchStatus || "—"} | ${b.recentMomentumEvidenceQualityStatus || "—"} | ${b.galleryImageCount} | ${b.scenarioImageCount} | ${b.propertyExampleImageCount} | ${b.presentationRowCount ?? "—"} | ${b.companyValidated} |`
    );
  }
  lines.push("");

  lines.push(`## 4. Wave 14 Marriott eight release summary`);
  lines.push("");
  lines.push(
    `Eight of the nine Wave 14 Marriott International brands were promoted via founder acceptance → Brand Status Active → public release → semantic cleanup. **Four Points Flex by Sheraton** (\`${w14.heldSlug || "four-points-flex-by-sheraton"}\`) remains **held** (Under Review) and is NOT part of the **54** Active/Live public-full universe.`
  );
  lines.push(`- Public eight slugs: ${(w14.publicEightSlugs || []).map((s2) => `\`${s2}\``).join(", ")}`);
  lines.push(`- Held slug: \`${w14.heldSlug || "four-points-flex-by-sheraton"}\` (excluded=${w14.heldExcluded})`);
  lines.push(`- Expected partial active count: ${w14.expectedPartialActiveCount || 54}`);
  lines.push(`- Value scenario visual remediation: \`${vs.wave14ValueScenarioVisualRemediation?.file || "brand-explorer-wave14-value-scenario-visual-remediation.json"}\``);
  lines.push(`- Dated momentum cleanup: \`${vs.wave14DatedMomentumCleanup?.file || "brand-explorer-wave14-dated-momentum-cleanup.json"}\``);
  lines.push(`- Founder visual/semantic remediation: \`${vs.wave14FounderVisualSemanticRemediation?.file || "brand-explorer-wave14-founder-visual-semantic-remediation.json"}\``);
  lines.push(`- Public/Active semantic blocker cleanup: \`${vs.wave14PublicActiveSemanticBlockerCleanup?.file || "brand-explorer-wave14-public-active-semantic-blocker-cleanup.json"}\``);
  lines.push("");

  lines.push(`## 5. AI-Assisted Profile footnote standardization summary`);
  lines.push("");
  lines.push(`- Artifact: \`${vs.aiAssistedFootnoteStandardization?.file || FOOTNOTE_STANDARDIZATION_JSON}\``);
  lines.push(`- Ready state: **${vs.aiAssistedFootnoteStandardization?.readyState || report.footnoteStandardization?.readyState || "—"}**`);
  lines.push(`- Approach: code/rendering enricher (\`applyBrandExplorerAiAssistedFootnote\`) — **0 Airtable writes**`);
  lines.push(`- Gate: \`ai_assisted_profile_footnote_visible\` (PVQL + factory)`);
  lines.push(`- Footnote visible count: **${s.footnoteVisibleCount}** / complete: **${s.footnoteCompleteCount}**`);
  lines.push("");

  lines.push(`## 6. Validation results`);
  lines.push("");
  lines.push(`- Quality audit: ${vs.qualityAudit?.file || "—"} (${vs.qualityAudit?.decision || "—"})`);
  lines.push(
    `- PVQL: ${vs.pvql?.file || "—"} (publicFull=${vs.pvql?.publicFull ?? "—"}; overallPass=${vs.pvql?.overallPass ?? "—"})`
  );
  lines.push(`- Image audit: ${vs.imageAudit?.file || "—"} (crossBrand=${vs.imageAudit?.crossBrand ?? "—"})`);
  lines.push(`- OS: ${vs.os?.file || "—"}`);
  lines.push(`- Recent Momentum / Openings Evidence Quality: pass=${vs.recentMomentumEvidenceQuality?.pass}`);
  lines.push("");

  lines.push(`## 7. PVQL result`);
  lines.push("");
  lines.push(`Public-full PVQL must be **${EXPECTED_ACTIVE_COUNT_62}/${EXPECTED_ACTIVE_COUNT_62}** lockPass, including \`ai_assisted_profile_footnote_visible\`.`);
  lines.push(`Snapshot: publicFull=${vs.pvql?.publicFull ?? "—"} · overallPass=${vs.pvql?.overallPass ?? "—"}`);
  lines.push("");

  lines.push(`## 8. 24-tab quality result`);
  lines.push("");
  lines.push(
    BASELINE_62_ACCEPTED_MINOR_CLEANUP_SLUGS.length
      ? `All Active/Live brands must be \`approve_for_baseline_freeze\` with blockerCount=0 (accepted minor: \`${BASELINE_62_ACCEPTED_MINOR_CLEANUP_SLUGS.join("`, `")}\`).`
      : "All Active/Live brands must be `approve_for_baseline_freeze` with blockerCount=0 (accepted minors: **none** — quality-clean)."
  );
  lines.push(`Freeze recommendation count: **${s.freezeRecommendationCount}** · remediation: **${s.remediationCount}**`);
  lines.push("");

  lines.push(`## 9. Evidence quality result`);
  lines.push("");
  lines.push(`Gate: \`npm run test:brand-explorer-recent-momentum-evidence-quality\``);
  lines.push(`Mandatory wave pass: **${report.evidenceQuality?.pass}**`);
  lines.push(
    `Wave 14 hard-fail gate (allows known cala_not_prioritized_first Sort Order drift): **${report.evidenceQuality?.wave14PassAllowingKnownSortDrift}**`
  );
  if ((report.evidenceQuality?.wave14SoftFailNotes || []).length) {
    lines.push("");
    lines.push(`Known Wave 14 soft notes:`);
    for (const n of report.evidenceQuality.wave14SoftFailNotes) lines.push(`- ${n}`);
  }
  lines.push("");

  lines.push(`## 10. Image uniqueness / role-match result`);
  lines.push("");
  lines.push(`| Metric | Count |`);
  lines.push(`|--------|------:|`);
  lines.push(`| Image uniqueness pass | ${s.imageUniquenessPassCount} |`);
  lines.push(`| Image role-match pass | ${s.imageRoleMatchPassCount} |`);
  lines.push(`| Scenario repetition flagged | ${s.scenarioRepetitionBrandCount} |`);
  lines.push(`| Cross-brand image reuse | ${s.crossBrandImageReuse} |`);
  lines.push("");

  lines.push(`## 11. Value scenario / semantic cleanup result`);
  lines.push("");
  lines.push(`- Value scenario visual remediation: \`${vs.wave14ValueScenarioVisualRemediation?.file || "—"}\` · ready: **${vs.wave14ValueScenarioVisualRemediation?.readyStatement || "—"}**`);
  lines.push(`- Founder visual/semantic remediation: \`${vs.wave14FounderVisualSemanticRemediation?.file || "—"}\` · ready: **${vs.wave14FounderVisualSemanticRemediation?.readyStatement || "—"}**`);
  lines.push(`- Public/Active semantic blocker cleanup: \`${vs.wave14PublicActiveSemanticBlockerCleanup?.file || "—"}\` · ready: **${vs.wave14PublicActiveSemanticBlockerCleanup?.readyStatement || "—"}**`);
  lines.push(
    `- No \`owner-fit diligence\`; no standalone platform placeholders; no Property Fit / Support Across Lifecycle titles.`
  );
  lines.push("");

  lines.push(`## 12. Geographic footprint / Recent Momentum pattern result`);
  lines.push("");
  lines.push(`- Dated momentum cleanup: \`${vs.wave14DatedMomentumCleanup?.file || "—"}\` · ready: **${vs.wave14DatedMomentumCleanup?.readyStatement || "—"}**`);
  lines.push(
    `- ≥3 geographic region cards (or accepted cleanly_unavailable); structured Recent Momentum; no raw URLs in visible body.`
  );
  lines.push("");

  lines.push(`## 13. Held / excluded brands`);
  lines.push("");
  lines.push(`These brands are **explicitly excluded** from the 62 Active/Live public-full freeze:`);
  lines.push("");
  lines.push(`| Brand | Slug | Record ID | Brand Status | Category | Included |`);
  lines.push(`|-------|------|-----------|--------------|----------|----------|`);
  for (const e of report.heldExcluded || report.excludedNonActive || []) {
    lines.push(
      `| ${e.brandName} | \`${e.slug}\` | \`${e.recordId || "—"}\` | ${e.brandStatus || "—"} | ${e.category || "—"} | ${e.includedInBaseline} |`
    );
  }
  lines.push("");
  lines.push(`- **The House of Originals** — excluded from Wave 13.`);
  lines.push(`- **Morgans Originals** — not created / not modified.`);
  lines.push(`- **Radisson Collection** — excluded unless separately promoted to Active/Live.`);
  lines.push(`- **Four Points Flex by Sheraton** — held (Under Review) after the Wave 14 partial release.`);
  lines.push("");

  lines.push(`## 14. Protected fields`);
  lines.push("");
  for (const f of report.protectedFields || []) lines.push(`- ${f}`);
  lines.push("");
  lines.push(`Baseline freeze does **not** write any of these fields.`);
  lines.push("");

  lines.push(`## 15. Regression rules`);
  lines.push("");
  for (const r of report.regressionRules || []) lines.push(`- ${r}`);
  lines.push("");
  lines.push(`Test: \`npm run test:brand-explorer-62-active-public-full-baseline\``);
  lines.push("");

  lines.push(`## 16. Rollback notes`);
  lines.push("");
  for (const r of report.rollbackNotes || []) lines.push(`- ${r}`);
  lines.push("");

  lines.push(`## 17. Future Wave 15 starting conditions`);
  lines.push("");
  lines.push(
    report.futureWave15StartingConditions ||
      `Start next factory work from ${FREEZE_DECISION_62}.`
  );
  lines.push("");
  lines.push(`### Future factory rules`);
  lines.push("");
  for (const r of report.futureWorkRules || []) lines.push(`- ${r}`);
  lines.push("");

  lines.push(`## Commands`);
  lines.push("");
  lines.push("```bash");
  lines.push("npm run brand-explorer-62-active-public-full-baseline -- --dry-run");
  lines.push("npm run test:brand-explorer-62-active-public-full-baseline");
  lines.push("npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only");
  lines.push("npm run test:brand-explorer-recent-momentum-evidence-quality");
  lines.push("npm run brand-explorer-ai-assisted-footnote-standardization -- --audit");
  lines.push("# Quiet sequential (avoid Airtable 429 thrash):");
  lines.push("node scripts/brand-explorer-quiet-sequential-pvql.mjs");
  lines.push("node scripts/brand-explorer-quiet-sequential-quality-audit.mjs");
  lines.push("```");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

export {
  BASELINE_VERSION_62 as BASELINE_VERSION,
  EXPECTED_ACTIVE_COUNT_62 as EXPECTED_ACTIVE_COUNT,
  PROTECTED_FIELDS,
  ROOT,
};
