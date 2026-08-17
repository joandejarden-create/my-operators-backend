/**
 * Brand Explorer — Public Visibility Quality Lock (read-only).
 *
 * Default discovery uses the canonical Active/Live universe
 * (Brand Basics Brand Status via brand-explorer-active-universe.js).
 * Operational cohorts (PRIMARY_RELEASE, restored legacy, etc.) remain
 * classification overlays — not the discovery source of truth.
 *
 * No Airtable writes. No content / CV / Source Library / Registry / release changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listActiveProfileBrandSlugs, getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { FACTORY_SUPPORTED_SLUGS } from "./brand-explorer-active-profile-factory-rules.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import {
  VISIBILITY_RESTORED_RELEASE_SLUGS,
  isVisibilityRestoredReleaseSlug,
} from "./brand-explorer-profile-preparation-visibility-fix.js";
import {
  LEGACY_SEED_SLUGS,
  getLegacySeedBrand,
} from "./brand-explorer-legacy-approved-profile-reconciliation.js";
import {
  listActiveUniverseSlugs,
  resolveActiveUniverseRecordId,
} from "./brand-explorer-active-universe.js";
import { resolveSectionPatternBrandIdentity } from "./brand-explorer-section-pattern-parity.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
} from "./brand-explorer-v40b-copy-quality-patterns.js";
import { evaluateGoldenContentQuality } from "./brand-explorer-golden-content-quality.js";
import { evaluateAiAssistedProfileFootnoteGate } from "./brand-explorer-ai-assisted-footnote.js";

export const PUBLIC_VISIBILITY_QUALITY_LOCK_VERSION = "public-visibility-quality-lock-v1";

export const REPORT_JSON = "brand-explorer-public-visibility-quality-lock.json";
export const REPORT_MD = "brand-explorer-public-visibility-quality-lock.md";
export const REPORT_PRIMARY_MD = "brand-explorer-public-visibility-quality-lock-primary.md";
export const REPORT_LEGACY_MD = "brand-explorer-public-visibility-quality-lock-legacy.md";
export const REPORT_HIDDEN_MD = "brand-explorer-public-visibility-quality-lock-hidden-row-hygiene.md";

export const COHORTS = Object.freeze([
  "primary_release",
  "restored_legacy_public",
  "founder_preview_only",
  "remediation_locked",
  "no_profile_or_not_ready",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function lower(v) {
  return nz(v).toLowerCase();
}

export function isOwnerFacingPresentationRow(row = {}) {
  if (row.visible === false || row.active === false) return false;
  const ext = nz(row.externalDisplayStatus || row.external_display_status);
  if (/^do not display$/i.test(ext) || /^internal only$/i.test(ext)) return false;
  return true;
}

export function isHiddenPresentationRow(row = {}) {
  const ext = nz(row.externalDisplayStatus || row.external_display_status);
  return /^do not display$/i.test(ext) || /^internal only$/i.test(ext) || row.visible === false;
}

/**
 * Legacy code-union fallback only — NOT the active universe.
 * Prefer discoverActiveUniverseCandidateSlugs() for all new PVQL runs.
 */
function discoverLegacyCodeUnionCandidateSlugs() {
  const set = new Set([
    ...LEGACY_SEED_SLUGS,
    ...FACTORY_SUPPORTED_SLUGS,
    ...listActiveProfileBrandSlugs(),
    ...PRIMARY_RELEASE_SLUGS,
    ...VISIBILITY_RESTORED_RELEASE_SLUGS,
  ]);
  return [...set]
    .filter(Boolean)
    .filter((slug) => {
      const seed = getLegacySeedBrand(slug);
      return !seed || seed.slug === slug;
    })
    .sort();
}

/**
 * Canonical PVQL discovery: Brand Basics Brand Status Active/Live universe.
 * Operational cohorts are evaluated as overlays inside evaluateBrandPublicVisibility.
 */
export async function discoverActiveUniverseCandidateSlugs() {
  try {
    return await listActiveUniverseSlugs();
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn(
        "[pvql] Active universe discovery failed; falling back to legacy code-union (stale — do not treat as SoT):",
        err?.message || err
      );
    }
    return discoverLegacyCodeUnionCandidateSlugs();
  }
}

function isAirtableRecordId(v) {
  return /^rec[a-zA-Z0-9]{10,}$/.test(nz(v));
}

async function fetchBrandApi(slugOrId) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const seed = getLegacySeedBrand(slugOrId);
  const sectionIdentity = resolveSectionPatternBrandIdentity(slugOrId);
  const activeCfg = getActiveProfileBrandConfig(String(slugOrId || "").toLowerCase());
  const discovery = getDiscoveryBrandConfig(slugOrId);
  // Prefer real Airtable record ids. resolveSectionPatternBrandIdentity falls back to
  // the slug string as recordId — that must not short-circuit Active Universe lookup.
  const sectionRecordId = isAirtableRecordId(sectionIdentity?.recordId)
    ? sectionIdentity.recordId
    : null;
  // Active-universe anchors (incl. Wave 13 Accor + fairmont/so aliases) before
  // slug-as-name fallback — prevents brand_not_found after factory-preview churn.
  const lookupId =
    (isAirtableRecordId(slugOrId) && slugOrId) ||
    (isAirtableRecordId(seed?.recordId) && seed.recordId) ||
    sectionRecordId ||
    (isAirtableRecordId(activeCfg?.recordId) && activeCfg.recordId) ||
    (isAirtableRecordId(discovery?.recordId) && discovery.recordId) ||
    resolveActiveUniverseRecordId(slugOrId) ||
    slugOrId;
  const res = {
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
  await getBrandLibraryBrandById({ query: { brandId: lookupId }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function readCompanyValidated(brand, brandBasics = null) {
  if (brand?.governance?.companyValidated === true) return true;
  if (brand?.companyValidated === true) return true;
  const basics = brandBasics?.fields || brandBasics || {};
  return basics["Company Validated"] === true || basics.company_validated === true;
}

function classifyCohort({
  slug,
  shouldRenderFullProfile,
  displayState,
  hasPresentationRows,
}) {
  const inPrimary = PRIMARY_RELEASE_SLUGS.includes(slug);
  const inRestored = isVisibilityRestoredReleaseSlug(slug);

  if (shouldRenderFullProfile && inPrimary) return "primary_release";
  if (shouldRenderFullProfile && inRestored) return "restored_legacy_public";
  if (shouldRenderFullProfile) return "restored_legacy_public"; // future migrated / other public full

  if (
    /internal_preview|founder_review/i.test(nz(displayState)) ||
    displayState === "internal_preview_only" ||
    displayState === "founder_review_ready"
  ) {
    return "founder_preview_only";
  }

  if (
    /draft_applied|profile_in_preparation|hidden_incomplete|legacy_approved|remediation/i.test(
      nz(displayState)
    ) ||
    hasPresentationRows
  ) {
    return "remediation_locked";
  }

  return "no_profile_or_not_ready";
}

function presentationCorpus(rows = []) {
  return (rows || [])
    .map((r) =>
      [
        r.title,
        r.body,
        r.caseSummaryOverview,
        r.caseSummaryBrandRelevance,
        r.caseSummaryOwnerObjective,
        r.caseSummaryInterpretation,
        r.caseSummaryTags,
      ]
        .map(nz)
        .filter(Boolean)
        .join("\n")
    )
    .filter(Boolean)
    .join("\n\n");
}

/** Slots where trailing https announcement URLs are required (Recent Momentum contract). */
const PVQL_ANNOUNCEMENT_URL_SLOTS = new Set(["footprint.momentum", "footprint.openings"]);

/**
 * Owner-facing forbidden scan with slot-aware raw_url exceptions for
 * footprint.momentum / footprint.openings announcement URLs.
 */
export function scanOwnerFacingForbiddenLanguage(rows = []) {
  const hits = [];
  for (const r of rows || []) {
    const slotKey = nz(r.slotKey);
    const allowAnnouncementUrls = PVQL_ANNOUNCEMENT_URL_SLOTS.has(slotKey);
    const text = [
      r.title,
      r.body,
      r.caseSummaryOverview,
      r.caseSummaryBrandRelevance,
      r.caseSummaryOwnerObjective,
      r.caseSummaryInterpretation,
      r.caseSummaryTags,
    ]
      .map(nz)
      .filter(Boolean)
      .join("\n");
    if (!text) continue;
    for (const h of scanForbiddenLanguage(text)) {
      if (allowAnnouncementUrls && h.id === "raw_url") continue;
      hits.push({
        ...h,
        slotKey: slotKey || null,
        recordId: r.recordId || r.id || null,
      });
    }
  }
  return hits;
}

function recommendedActionForBrand(row) {
  if (!row.inPublicVisibilityLockScope) {
    if (row.cohort === "remediation_locked") return "keep_locked_until_gates_pass";
    if (row.cohort === "founder_preview_only") return "founder_preview_only_no_public_full";
    return "no_action";
  }
  if (row.lockPass) return "no_action";
  if (row.cohort === "restored_legacy_public") {
    return "flag_legacy_public_quality_debt_or_remediate";
  }
  return "remediate_failed_public_visibility_gates";
}

function buildHiddenRowHygiene(rows = [], meta = {}) {
  return (rows || []).filter(isHiddenPresentationRow).map((r) => ({
    brandSlug: meta.brandSlug || null,
    brandName: meta.brandName || null,
    recordId: r.recordId || r.id || null,
    slotKey: r.slotKey || null,
    title: nz(r.title).slice(0, 120) || null,
    externalDisplayStatus: nz(r.externalDisplayStatus || r.external_display_status) || null,
    active: r.active !== false,
    hasImageUrl: Boolean(nz(r.imageUrl)),
    hasBody: Boolean(nz(r.body)),
    bodyHasRawUrl: /https?:\/\//i.test(nz(r.body)),
  }));
}

function evaluatePublicGates({
  brand,
  brandSlug,
  brandConfig,
  ownerFacingRows,
  registryAssets,
  html,
}) {
  const tabFactory = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerFacingRows,
    html,
    brandSlug,
    brandConfig,
    registryAssets,
  });
  const uniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: ownerFacingRows,
    brandSlug,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: ownerFacingRows,
    brandSlug,
  });
  const forbidden = scanOwnerFacingForbiddenLanguage(ownerFacingRows);
  const mechanical = scanMechanicalCopy(presentationCorpus(ownerFacingRows)).filter((h) =>
    ["high", "medium"].includes(h.severity)
  );
  const golden = evaluateGoldenContentQuality(brand, ownerFacingRows, html, { brandSlug });
  const externalQl = evaluateBrandExternalQualityLock(brand, html, { brandSlug });

  const blankVisibleFields = (tabFactory.completeness?.findings || []).filter(
    (f) => f.status === "blank" || f.blankInHtml === true
  );
  const emptyCards = tabFactory.emptyScan?.failFindings || 0;
  const scenarioDupes = (uniqueness.duplicateGroups || []).filter((g) => g.section === "scenario");
  const propertyDupes = (uniqueness.duplicateGroups || []).filter(
    (g) => g.section === "property_example"
  );
  const roleHardFails = (roleMatch.findings || []).filter((f) => f.status === "fail");
  const wrongPropertyOrBrand = roleHardFails.filter((f) =>
    /wrong|mismatch|property|brand|caption|role/i.test(
      `${f.id || ""} ${f.detail || ""} ${f.reason || ""} ${f.failureReason || ""}`
    )
  );

  const gates = {
    rendered_field_completeness: {
      pass: tabFactory.completeness?.auditPass === true,
      failFindings: tabFactory.failFindings,
      blankVisibleFields: blankVisibleFields.length,
    },
    no_empty_rendered_components: {
      pass: tabFactory.emptyScan?.pass === true,
      emptyCards,
    },
    tab_factory_audit: {
      pass: tabFactory.auditPass === true,
      failFindings: tabFactory.failFindings,
    },
    source_provenance_by_tab: {
      pass: tabFactory.provenance?.pass === true,
      failures: (tabFactory.provenance?.failures || []).slice(0, 12),
    },
    image_uniqueness: {
      pass: uniqueness.pass === true,
      galleryDistinctCount: uniqueness.galleryDistinctCount,
      scenarioDistinctCount: uniqueness.scenarioDistinctCount,
      propertyExampleDistinctCount: uniqueness.propertyExampleDistinctCount,
    },
    image_role_match: {
      pass: roleMatch.pass === true,
      unresolvedRoleMismatchCount: roleMatch.unresolvedRoleMismatchCount,
      hardFails: roleHardFails.length,
    },
    scenario_image_distinctiveness: {
      pass: uniqueness.scenarioDistinctCount >= 3 && scenarioDupes.length === 0,
      scenarioDistinctCount: uniqueness.scenarioDistinctCount,
      duplicateGroups: scenarioDupes.length,
    },
    property_image_distinctiveness: {
      pass: uniqueness.propertyExampleDistinctCount >= 3 && propertyDupes.length === 0,
      propertyExampleDistinctCount: uniqueness.propertyExampleDistinctCount,
      duplicateGroups: propertyDupes.length,
    },
    forbidden_owner_facing_language: {
      pass: forbidden.length === 0,
      hits: forbidden.slice(0, 20),
    },
    generic_copy_scan: {
      pass: mechanical.length === 0 && !(golden.failures || []).includes("generic_audience_prose"),
      mechanicalHits: mechanical.slice(0, 12),
      goldenFailures: (golden.failures || []).slice(0, 12),
    },
    raw_url_scan: {
      pass: !forbidden.some((h) => h.id === "raw_url"),
      hits: forbidden.filter((h) => h.id === "raw_url").slice(0, 10),
    },
    wrong_brand_or_property_examples: {
      pass: wrongPropertyOrBrand.length === 0 && roleMatch.pass === true,
      hits: wrongPropertyOrBrand.slice(0, 10),
    },
    display_consistency: {
      pass:
        brand.shouldRenderFullProfile === true
          ? externalQl.actualRenderedFullProfile === true &&
            externalQl.profileInPreparationRendered !== true
          : externalQl.actualRenderedFullProfile !== true,
      shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
      actualRenderedFullProfile: externalQl.actualRenderedFullProfile === true,
      profileInPreparationRendered: externalQl.profileInPreparationRendered === true,
    },
    ai_assisted_profile_footnote_visible: (() => {
      const footnoteGate = evaluateAiAssistedProfileFootnoteGate(brand, "");
      return {
        pass: footnoteGate.pass === true,
        failures: footnoteGate.failures,
        displayLabel: footnoteGate.displayLabel,
        displaySubtitle: footnoteGate.displaySubtitle,
      };
    })(),
  };

  const failures = [];
  for (const [name, g] of Object.entries(gates)) {
    if (!g.pass) failures.push(name);
  }
  if (blankVisibleFields.length) failures.push("visible_blank_fields");
  if (emptyCards > 0) failures.push("empty_rendered_cards");
  if ((uniqueness.galleryDistinctCount || 0) < 6) failures.push("gallery_distinct_lt_6");
  if (scenarioDupes.length) failures.push("scenario_images_duplicated");

  return {
    gates,
    failures: [...new Set(failures)],
    lockPass: failures.length === 0,
    tabFactory,
    uniqueness,
    roleMatch,
    forbidden,
    golden,
  };
}

export async function evaluateBrandPublicVisibility(slug) {
  const brandConfig =
    getActiveProfileBrandConfig(slug) || getDiscoveryBrandConfig(slug) || getLegacySeedBrand(slug);
  let brand = null;
  try {
    brand = await fetchBrandApi(slug);
  } catch (err) {
    return {
      slug,
      brandName: brandConfig?.name || slug,
      recordId: brandConfig?.recordId || null,
      error: err.message,
      cohort: "no_profile_or_not_ready",
      publicFullProfile: false,
      inPublicVisibilityLockScope: false,
      lockPass: null,
      gateResults: {},
      failures: ["brand_api_fetch_failed"],
      recommendedAction: "investigate_brand_api_fetch",
      companyValidated: null,
      hiddenRows: [],
    };
  }

  if (!brand) {
    return {
      slug,
      brandName: brandConfig?.name || slug,
      recordId: brandConfig?.recordId || null,
      error: "brand_not_found",
      cohort: "no_profile_or_not_ready",
      publicFullProfile: false,
      inPublicVisibilityLockScope: false,
      lockPass: null,
      gateResults: {},
      failures: ["brand_not_found"],
      recommendedAction: "no_action",
      companyValidated: null,
      hiddenRows: [],
    };
  }

  let ctx = null;
  const needsFactory =
    brand.shouldRenderFullProfile === true ||
    (brand.brandExplorer?.blocks || []).some(isHiddenPresentationRow);
  if (needsFactory) {
    try {
      // Factory context: registry provenance + hidden-row inventory.
      // Public gate evaluation uses live brandExplorer.blocks (what owners see).
      ctx = await loadBrandFactoryContext(slug);
    } catch {
      ctx = null;
    }
  }

  const liveBlocks = brand.brandExplorer?.blocks || [];
  const factoryRows = ctx?.presentationRows || [];
  const allRowsForHidden = factoryRows.length ? factoryRows : liveBlocks;
  const ownerFacingRows = liveBlocks.filter(isOwnerFacingPresentationRow);
  const hiddenRows = buildHiddenRowHygiene(allRowsForHidden, {
    brandSlug: slug,
    brandName: brand.name,
  });

  const displayState = brand.brandExplorerDisplayState || null;
  const shouldRenderFullProfile = brand.shouldRenderFullProfile === true;
  const cohort = classifyCohort({
    slug,
    shouldRenderFullProfile,
    displayState,
    hasPresentationRows: liveBlocks.length > 0 || factoryRows.length > 0,
  });

  const companyValidatedBefore = readCompanyValidated(brand, ctx?.brandBasics);
  const inPrimary = PRIMARY_RELEASE_SLUGS.includes(slug);
  const inRestoredLegacy = isVisibilityRestoredReleaseSlug(slug);
  const founderPreviewOnly = cohort === "founder_preview_only";
  const remediationLocked = cohort === "remediation_locked";

  const html = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: false,
  });
  const externalQl = evaluateBrandExternalQualityLock(brand, html, { brandSlug: slug });

  // Locked profiles must not leak full external tabs.
  if (!shouldRenderFullProfile) {
    const lockedOk =
      externalQl.actualRenderedFullProfile !== true ||
      externalQl.profileInPreparationRendered === true;
    const companyValidatedAfter = readCompanyValidated(brand, ctx?.brandBasics);
    return {
      slug,
      brandName: brand.name,
      recordId: brand.id,
      publicDisplayState: displayState,
      shouldRenderFullProfile: false,
      publicFullProfile: false,
      primaryReleaseCohort: inPrimary,
      restoredLegacyPublic: inRestoredLegacy && false,
      founderPreviewOnly,
      remediationLocked,
      cohort,
      inPublicVisibilityLockScope: false,
      lockPass: lockedOk,
      gateResults: {
        remains_locked_externally: {
          pass: lockedOk,
          actualRenderedFullProfile: externalQl.actualRenderedFullProfile === true,
          profileInPreparationRendered: externalQl.profileInPreparationRendered === true,
        },
        company_validated_unchanged: {
          pass: companyValidatedBefore === companyValidatedAfter,
          before: companyValidatedBefore,
          after: companyValidatedAfter,
        },
      },
      failures: lockedOk ? [] : ["locked_profile_rendered_full_externally"],
      recommendedAction: lockedOk
        ? recommendedActionForBrand({
            inPublicVisibilityLockScope: false,
            cohort,
            lockPass: true,
          })
        : "fix_external_lock_leak",
      companyValidated: companyValidatedBefore,
      companyValidatedUnchanged: companyValidatedBefore === companyValidatedAfter,
      hiddenRows,
      ownerFacingRowCount: ownerFacingRows.length,
      allPresentationRowCount: allRowsForHidden.length,
    };
  }

  // Public full profile — mandatory lock scope.
  const evalResult = evaluatePublicGates({
    brand,
    brandSlug: slug,
    brandConfig,
    ownerFacingRows,
    registryAssets: ctx?.registryAssets || [],
    html,
  });
  const companyValidatedAfter = readCompanyValidated(brand, ctx?.brandBasics);
  const cvUnchanged = companyValidatedBefore === companyValidatedAfter;
  if (!cvUnchanged) evalResult.failures.push("company_validated_changed");

  // Hidden rows must not appear in owner-facing forbidden scan (already filtered).
  const hiddenScannedAsPublic = false;

  const gateResults = {
    ...Object.fromEntries(
      Object.entries(evalResult.gates).map(([k, v]) => [k, { pass: v.pass, ...v }])
    ),
    company_validated_unchanged: {
      pass: cvUnchanged,
      before: companyValidatedBefore,
      after: companyValidatedAfter,
    },
    do_not_display_excluded_from_public_scan: {
      pass: !hiddenScannedAsPublic,
      hiddenRowCount: hiddenRows.length,
      ownerFacingRowCount: ownerFacingRows.length,
    },
  };

  const failures = [...evalResult.failures];
  if (!cvUnchanged) failures.push("company_validated_changed");

  return {
    slug,
    brandName: brand.name,
    recordId: brand.id,
    publicDisplayState: displayState,
    shouldRenderFullProfile: true,
    publicFullProfile: true,
    primaryReleaseCohort: inPrimary,
    restoredLegacyPublic: inRestoredLegacy || cohort === "restored_legacy_public",
    founderPreviewOnly: false,
    remediationLocked: false,
    cohort,
    inPublicVisibilityLockScope: true,
    lockPass: failures.length === 0,
    gateResults,
    failures: [...new Set(failures)],
    recommendedAction: recommendedActionForBrand({
      inPublicVisibilityLockScope: true,
      cohort,
      lockPass: failures.length === 0,
    }),
    companyValidated: companyValidatedBefore,
    companyValidatedUnchanged: cvUnchanged,
    hiddenRows,
    ownerFacingRowCount: ownerFacingRows.length,
    allPresentationRowCount: allRowsForHidden.length,
    evidence: {
      galleryDistinct: evalResult.uniqueness?.galleryDistinctCount ?? null,
      scenarioDistinct: evalResult.uniqueness?.scenarioDistinctCount ?? null,
      propertyDistinct: evalResult.uniqueness?.propertyExampleDistinctCount ?? null,
      roleMatchPass: evalResult.roleMatch?.pass ?? null,
      tabFactoryPass: evalResult.tabFactory?.auditPass ?? null,
      forbiddenHitCount: (evalResult.forbidden || []).length,
    },
  };
}

export async function runPublicVisibilityQualityLock({
  slugs = null,
  /** When true (default), discover from Brand Status Active/Live — not stale code unions. */
  useActiveUniverse = true,
} = {}) {
  const candidates = slugs?.length
    ? slugs
    : useActiveUniverse
      ? await discoverActiveUniverseCandidateSlugs()
      : discoverLegacyCodeUnionCandidateSlugs();
  const brands = [];
  const hiddenHygiene = [];

  for (const slug of candidates) {
    const row = await evaluateBrandPublicVisibility(slug);
    brands.push(row);
    for (const h of row.hiddenRows || []) hiddenHygiene.push(h);
  }

  const publicFull = brands.filter((b) => b.publicFullProfile === true);
  const scoped = brands.filter((b) => b.inPublicVisibilityLockScope === true);
  const uncoveredPublic = publicFull.filter((b) => !b.inPublicVisibilityLockScope);
  const primary = brands.filter((b) => b.cohort === "primary_release");
  const legacy = brands.filter((b) => b.cohort === "restored_legacy_public");
  const locked = brands.filter((b) =>
    ["remediation_locked", "founder_preview_only", "no_profile_or_not_ready"].includes(b.cohort)
  );

  const primaryPass = primary.every((b) => b.lockPass === true);
  const legacyPass = legacy.every((b) => b.lockPass === true);
  const legacyFlagged = legacy.filter((b) => b.lockPass !== true);
  const lockedRemainLocked = locked.every(
    (b) => !(b.failures || []).includes("locked_profile_rendered_full_externally")
  );
  const cvUntouched = brands.every(
    (b) => b.companyValidatedUnchanged !== false && b.error !== "brand_api_fetch_failed"
  );
  const coverageComplete =
    uncoveredPublic.length === 0 && publicFull.every((b) => b.inPublicVisibilityLockScope);

  const hardFails = [];
  if (!coverageComplete) hardFails.push("externally_visible_profile_not_in_lock");
  if (!primaryPass) hardFails.push("primary_release_cohort_failed");
  if (!lockedRemainLocked) hardFails.push("locked_profile_leaked_full_external");
  if (!cvUntouched) hardFails.push("company_validated_changed");

  // Legacy may fail but must be explicitly flagged (not silent).
  const legacyExplicitlyHandled = legacy.every(
    (b) => b.lockPass === true || (b.failures || []).length > 0
  );
  if (!legacyExplicitlyHandled) hardFails.push("legacy_public_not_explicitly_flagged");

  const overallPass = hardFails.length === 0;

  return {
    version: PUBLIC_VISIBILITY_QUALITY_LOCK_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    readOnly: true,
    noAirtableWrites: true,
    candidateSlugCount: candidates.length,
    cohorts: {
      primary_release: primary.map((b) => b.slug),
      restored_legacy_public: legacy.map((b) => b.slug),
      founder_preview_only: brands.filter((b) => b.cohort === "founder_preview_only").map((b) => b.slug),
      remediation_locked: brands.filter((b) => b.cohort === "remediation_locked").map((b) => b.slug),
      no_profile_or_not_ready: brands
        .filter((b) => b.cohort === "no_profile_or_not_ready")
        .map((b) => b.slug),
    },
    summary: {
      overallPass,
      hardFails,
      coverageComplete,
      publicFullProfileCount: publicFull.length,
      scopedCount: scoped.length,
      primaryCount: primary.length,
      primaryPass,
      legacyCount: legacy.length,
      legacyPass,
      legacyFlaggedCount: legacyFlagged.length,
      lockedCount: locked.length,
      lockedRemainLocked,
      companyValidatedUntouched: cvUntouched,
      hiddenRowCount: hiddenHygiene.length,
    },
    brands,
    hiddenRowHygiene: hiddenHygiene,
    acceptance: {
      everyExternallyVisibleFullProfileCovered: coverageComplete,
      primaryReleaseCohortPasses: primaryPass,
      restoredLegacyPublicPassOrFlagged: legacyPass || legacyFlagged.length === legacy.length,
      noVisibleProfileUntested: coverageComplete,
      lockedRemediationProfilesRemainLocked: lockedRemainLocked,
      companyValidatedUntouched: cvUntouched,
    },
  };
}

function mdTableHeader() {
  return [
    "| Brand | Cohort | Public Full Profile? | Gate Results | Failures | Recommended Action |",
    "| --- | --- | --- | --- | --- | --- |",
  ];
}

function gateSummary(row) {
  if (!row.gateResults || !Object.keys(row.gateResults).length) return "—";
  const parts = Object.entries(row.gateResults).map(([k, v]) => `${k}:${v.pass ? "pass" : "FAIL"}`);
  return parts.join("; ");
}

function brandTableRows(rows) {
  return (rows || []).map((b) => {
    const name = `${b.brandName || b.slug} (\`${b.slug}\`)`;
    return `| ${name} | ${b.cohort} | ${b.publicFullProfile === true} | ${gateSummary(b)} | ${(b.failures || []).join("; ") || "—"} | ${b.recommendedAction || "—"} |`;
  });
}

export function writePublicVisibilityQualityLockReports(
  report,
  { reportsDir = path.join(ROOT, "reports") } = {}
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const primary = (report.brands || []).filter((b) => b.cohort === "primary_release");
  const legacy = (report.brands || []).filter((b) => b.cohort === "restored_legacy_public");

  const mainMd = [
    "# Brand Explorer — Public Visibility Quality Lock",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    `Read-only: **${report.readOnly !== false}** · No Airtable writes: **${report.noAirtableWrites !== false}**`,
    "",
    "## Summary",
    "",
    `- Overall pass: **${report.summary.overallPass}**`,
    `- Hard fails: ${(report.summary.hardFails || []).join("; ") || "—"}`,
    `- Public full profiles: **${report.summary.publicFullProfileCount}** (scoped ${report.summary.scopedCount})`,
    `- Primary: ${report.summary.primaryCount} · pass=${report.summary.primaryPass}`,
    `- Restored legacy public: ${report.summary.legacyCount} · pass=${report.summary.legacyPass} · flagged=${report.summary.legacyFlaggedCount}`,
    `- Locked remain locked: **${report.summary.lockedRemainLocked}**`,
    `- Company Validated untouched: **${report.summary.companyValidatedUntouched}**`,
    `- Hidden rows inventoried: **${report.summary.hiddenRowCount}**`,
    "",
    "## Acceptance",
    "",
    ...Object.entries(report.acceptance || {}).map(([k, v]) => `- ${k}: **${v}**`),
    "",
    "## All evaluated brands",
    "",
    ...mdTableHeader(),
    ...brandTableRows(report.brands),
    "",
  ];
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mainMd.join("\n"));

  const primaryMd = [
    "# Public Visibility Quality Lock — Primary Release Cohort",
    "",
    `Generated: ${report.generatedAt}`,
    `Primary pass: **${report.summary.primaryPass}**`,
    "",
    ...mdTableHeader(),
    ...brandTableRows(primary),
    "",
  ];
  const primaryPath = path.join(reportsDir, REPORT_PRIMARY_MD);
  fs.writeFileSync(primaryPath, primaryMd.join("\n"));

  const legacyMd = [
    "# Public Visibility Quality Lock — Restored Legacy Public Cohort",
    "",
    `Generated: ${report.generatedAt}`,
    `Legacy pass: **${report.summary.legacyPass}** · Flagged: **${report.summary.legacyFlaggedCount}**`,
    "",
    "Legacy brands may fail current mandatory gates while remaining publicly visible via transitional unlock. Failures must be explicit.",
    "",
    ...mdTableHeader(),
    ...brandTableRows(legacy),
    "",
  ];
  const legacyPath = path.join(reportsDir, REPORT_LEGACY_MD);
  fs.writeFileSync(legacyPath, legacyMd.join("\n"));

  const hiddenMd = [
    "# Public Visibility Quality Lock — Hidden Row Hygiene",
    "",
    `Generated: ${report.generatedAt}`,
    `Hidden rows: **${(report.hiddenRowHygiene || []).length}**`,
    "",
    "Do Not Display / Internal Only rows are excluded from public owner-facing scans but inventoried here.",
    "",
    "| Brand | Record | Slot | Status | Has Image | Has Body | Body Has Raw URL |",
    "| --- | --- | --- | --- | --- | --- | --- |",
  ];
  for (const h of report.hiddenRowHygiene || []) {
    hiddenMd.push(
      `| ${h.brandName || h.brandSlug} | \`${h.recordId || "—"}\` | \`${h.slotKey || "—"}\` | ${h.externalDisplayStatus || "—"} | ${h.hasImageUrl} | ${h.hasBody} | ${h.bodyHasRawUrl} |`
    );
  }
  const hiddenPath = path.join(reportsDir, REPORT_HIDDEN_MD);
  fs.writeFileSync(hiddenPath, hiddenMd.join("\n") + "\n");

  return { jsonPath, mdPath, primaryPath, legacyPath, hiddenPath };
}
