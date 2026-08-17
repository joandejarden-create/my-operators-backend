/**
 * Brand Explorer — Post-visibility reconciliation sanity audit (read-only).
 *
 * Confirms true live state of every Brand Explorer profile after Profile in
 * Preparation visibility restore. No Airtable writes. No content/image/release
 * field changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { listActiveProfileBrandSlugs, getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { FACTORY_SUPPORTED_SLUGS } from "./brand-explorer-active-profile-factory-rules.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import {
  PRIMARY_RELEASE_SLUGS,
  ORIGINAL_GOLDEN_RELEASE_SLUGS,
} from "./brand-explorer-os-state-machine.js";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import { resolveBrandExplorerDisplayState } from "./brand-explorer-display-state.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { scanInternalPreviewOwnerCopy } from "./brand-explorer-economics-chrome-remediation.js";
import { buildResidualOwnerCopyPatchPlan } from "./brand-explorer-residual-owner-copy-remediation.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import {
  LEGACY_SEED_BRANDS,
  LEGACY_SEED_SLUGS,
  getLegacySeedBrand,
  resolveLegacyApprovedSeed,
} from "./brand-explorer-legacy-approved-profile-reconciliation.js";
import {
  VISIBILITY_RESTORED_RELEASE_SLUGS,
  isVisibilityRestoredReleaseSlug,
} from "./brand-explorer-profile-preparation-visibility-fix.js";
import {
  DEFAULT_COMPLETE_BRANDS,
  DEFAULT_INCOMPLETE_BRANDS,
  DEFAULT_BRANDS as EXTERNAL_QUALITY_LOCK_COHORT,
} from "./brand-explorer-v38-display-quality-lock-audit.js";

export const SANITY_AUDIT_VERSION = "post-visibility-reconciliation-sanity-audit-v1";
export const REPORT_JSON = "brand-explorer-post-visibility-reconciliation-sanity-audit.json";
export const REPORT_MD = "brand-explorer-post-visibility-reconciliation-sanity-audit.md";

const INTERNAL_PREVIEW_COPY_FOCUS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
]);

const RADISSON_PROBE_NAMES = Object.freeze([
  "Radisson Individuals by Choice",
  "Radisson Individuals",
  "Radisson by Choice",
  "Radisson Blu by Choice",
  "Radisson Blu",
  "Radisson RED",
  "Radisson Collection",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function stripHtmlForCopyScan(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function discoverCandidateSlugs() {
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

async function fetchBrandApi(slugOrId) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const seed = getLegacySeedBrand(slugOrId);
  const lookupId = seed?.recordId || slugOrId;
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

function readReleaseFlags(brand) {
  const c = brand?.brandExplorerDisplayCompleteness || {};
  return {
    legacyHistoricalApproved:
      brand?.legacyHistoricalApproved === true || c.historicalApproved === true,
    activeProfileApproved:
      brand?.activeProfileApproved === true || c.activeProfileApproved === true,
    founderVisualReviewPass:
      brand?.founderVisualReviewPass === true || c.founderVisualReviewPass === true,
    readyForActiveProfile: brand?.readyForActiveProfile === true,
    companyValidated: brand?.governance?.companyValidated === true || c.companyValidated === true,
  };
}

function classifyExpectedStatus(row) {
  if (row.inPrimaryReleaseSlugs && row.shouldRenderFullProfile && row.osState === "active_profile_ready") {
    return "primary_active_release_clean";
  }
  if (row.inPrimaryReleaseSlugs && row.shouldRenderFullProfile) {
    return "primary_release_visible_with_gate_debt";
  }
  if (row.inPrimaryReleaseSlugs && !row.shouldRenderFullProfile) {
    return "primary_release_locked_needs_remediation";
  }
  if (row.inVisibilityRestoredCohort && row.shouldRenderFullProfile) {
    return "restored_legacy_approved_public_visible_transitional";
  }
  if (row.inLegacySeedCohort && !row.shouldRenderFullProfile) {
    if ((row.gateFailures || []).some((g) => /image|visual|uniqueness|role_match/i.test(g))) {
      return "legacy_seed_image_remediation";
    }
    if ((row.gateFailures || []).some((g) => /presentation|content|scenario/i.test(g))) {
      return "legacy_seed_content_remediation";
    }
    return "legacy_seed_locked_other";
  }
  if (row.shouldRenderFullProfile) {
    return "public_visible_outside_primary";
  }
  if (row.hasPresentationRows) {
    return "founder_preview_only";
  }
  return "incomplete_or_empty";
}

function detectMismatches(row) {
  const issues = [];
  if (row.inPrimaryReleaseSlugs && row.osState === "active_profile_ready" && !row.shouldRenderFullProfile) {
    issues.push("primary_active_profile_ready_but_public_locked");
  }
  if (row.inPrimaryReleaseSlugs && row.displayState === "draft_applied_with_defects") {
    issues.push("primary_cohort_member_not_active_profile_ready_live");
  }
  if (row.inVisibilityRestoredCohort && !row.shouldRenderFullProfile) {
    issues.push("visibility_restored_cohort_but_still_locked");
  }
  if (row.inVisibilityRestoredCohort && !row.inExternalQualityLockCohort) {
    issues.push(
      "restored_legacy_publicly_visible_but_not_in_external_quality_lock_cohort_transitional"
    );
  }
  if (row.inExternalQualityLockCohort && !row.inPrimaryReleaseSlugs && !row.inIncompleteControlCohort) {
    issues.push("quality_lock_cohort_membership_unexpected");
  }
  if (
    row.slug === "radisson-individuals-by-choice" &&
    row.osState !== "active_profile_ready" &&
    row.inPrimaryReleaseSlugs
  ) {
    issues.push(
      "radisson_individuals_in_primary_release_but_not_active_profile_ready_live_image_or_copy_debt"
    );
  }
  if (row.expectedStatus !== row.liveBucket) {
    // soft — expected vs live bucket naming only when we set both
  }
  return issues;
}

function assignLiveBucket(row) {
  if (row.inPrimaryReleaseSlugs && row.shouldRenderFullProfile && row.osState === "active_profile_ready") {
    return "primary_active_release_cohort";
  }
  if (row.inVisibilityRestoredCohort && row.shouldRenderFullProfile) {
    return "restored_legacy_approved_cohort";
  }
  if (row.inPrimaryReleaseSlugs && !row.shouldRenderFullProfile) {
    return "remediation_cohort_primary";
  }
  if (!row.shouldRenderFullProfile && row.hasPresentationRows) {
    return "founder_preview_only_cohort";
  }
  if (!row.shouldRenderFullProfile) {
    return "remediation_cohort";
  }
  return "other_visible";
}

async function scanOwnerCopyForBrand(slug, brand) {
  const liveHtml = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: true,
  });
  const liveHits = scanInternalPreviewOwnerCopy(stripHtmlForCopyScan(liveHtml));

  let projectedHits = null;
  let residualPatchCount = null;
  try {
    const ctx = await loadBrandFactoryContext(slug).catch(() => null);
    const rows = ctx?.presentationRows || brand?.brandExplorer?.blocks || [];
    const plan = buildResidualOwnerCopyPatchPlan({ brandSlug: slug, presentationRows: rows });
    residualPatchCount = plan.summary?.patchCount ?? plan.patches?.length ?? 0;
    // Projected path is what test:brand-explorer-internal-preview-owner-copy uses by default
    const FIELD_TO_API = {
      Title: "title",
      Body: "body",
      "Case Summary Overview": "caseSummaryOverview",
      "Case Summary Brand Relevance": "caseSummaryBrandRelevance",
      "Case Summary Owner Objective": "caseSummaryOwnerObjective",
      "Case Summary Interpretation": "caseSummaryInterpretation",
      "Case Summary Tags": "caseSummaryTags",
    };
    const byRecord = new Map();
    for (const p of plan.patches || []) {
      if (!p.recordId) continue;
      if (!byRecord.has(p.recordId)) byRecord.set(p.recordId, {});
      const apiKey = FIELD_TO_API[p.field];
      if (apiKey) byRecord.get(p.recordId)[apiKey] = p.after;
    }
    const blocks = (brand?.brandExplorer?.blocks || []).map((b) => {
      const overlay = byRecord.get(b.recordId);
      return overlay ? { ...b, ...overlay } : b;
    });
    const projectedBrand = {
      ...brand,
      brandExplorer: { ...(brand.brandExplorer || {}), blocks },
    };
    const projectedHtml = renderBrandExplorerHtmlForTest(projectedBrand, {
      allPanels: true,
      internalPreview: true,
    });
    projectedHits = scanInternalPreviewOwnerCopy(stripHtmlForCopyScan(projectedHtml));
  } catch (err) {
    projectedHits = [{ id: "scan_error", label: "scan_error", snippet: err.message }];
  }

  const publicHtml = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: false,
  });
  const publicHits = scanInternalPreviewOwnerCopy(stripHtmlForCopyScan(publicHtml));

  return {
    liveInternalPreviewHits: liveHits,
    projectedInternalPreviewHits: projectedHits,
    publicExternalHits: publicHits,
    residualPatchCount,
    publicAffected: publicHits.length > 0 && brand?.shouldRenderFullProfile === true,
    blocksReleaseBaseline:
      liveHits.length > 0 &&
      PRIMARY_RELEASE_SLUGS.includes(slug) &&
      brand?.shouldRenderFullProfile === true,
    founderBannerFalsePositive:
      liveHits.length > 0 &&
      liveHits.every((h) => h.id === "internal_review") &&
      publicHits.length === 0 &&
      brand?.shouldRenderFullProfile !== true,
    note:
      liveHits.length === 0
        ? "clean_on_live_internal_preview"
        : liveHits.every((h) => h.id === "internal_review") &&
            publicHits.length === 0 &&
            brand?.shouldRenderFullProfile !== true
          ? "founder_preview_banner_phrase_internal_review_false_positive_on_locked_brand_public_unaffected"
          : publicHits.length === 0 && brand?.shouldRenderFullProfile !== true
            ? "hits_only_visible_under_founder_preview_or_locked_shell_path"
            : publicHits.length > 0
              ? "hits_present_in_public_render_path"
              : "hits_on_internal_preview_full_tabs_public_may_still_pass_external_lock",
  };
}

async function probeRadissonRecords() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const findings = {
    configuredCanonical: {
      slug: "radisson-individuals-by-choice",
      recordId: getDiscoveryBrandConfig("radisson-individuals-by-choice")?.recordId || null,
      name: getDiscoveryBrandConfig("radisson-individuals-by-choice")?.name || null,
      inLegacySeed: Boolean(getLegacySeedBrand("radisson-individuals-by-choice")),
      inPrimaryRelease: PRIMARY_RELEASE_SLUGS.includes("radisson-individuals-by-choice"),
      inActiveProfileConfig: Boolean(getActiveProfileBrandConfig("radisson-individuals-by-choice")),
    },
    nameLookups: [],
    formulaSearchHits: [],
    duplicateRisk: false,
    conclusion: "",
  };

  // Name lookups via brand API (safe, no merge)
  for (const name of RADISSON_PROBE_NAMES) {
    try {
      const brand = await fetchBrandApi(name);
      findings.nameLookups.push({
        probedName: name,
        found: Boolean(brand),
        brandName: brand?.name || null,
        recordId: brand?.id || null,
        slug: brand?.slug || null,
        displayState: brand?.brandExplorerDisplayState || null,
        shouldRenderFullProfile: brand?.shouldRenderFullProfile ?? null,
        legacyHistoricalApproved: brand?.legacyHistoricalApproved === true,
      });
    } catch (err) {
      findings.nameLookups.push({ probedName: name, found: false, error: err.message });
    }
  }

  if (apiKey && baseId) {
    try {
      const base = new Airtable({ apiKey }).base(baseId);
      const records = await base("Brand Setup - Brand Basics")
        .select({
          filterByFormula: `FIND("Radisson", {Brand Name})`,
          fields: ["Brand Name"],
          maxRecords: 50,
        })
        .all();
      findings.formulaSearchHits = records.map((r) => ({
        recordId: r.id,
        brandName: r.fields["Brand Name"] || null,
      }));
    } catch (err) {
      findings.formulaSearchError = err.message;
    }
  }

  const ids = new Set(
    [
      ...findings.nameLookups.map((x) => x.recordId).filter(Boolean),
      ...findings.formulaSearchHits.map((x) => x.recordId).filter(Boolean),
    ]
  );
  findings.uniqueRecordIds = [...ids];
  findings.individualsByChoiceMatch = findings.formulaSearchHits.find((h) =>
    /individuals by choice/i.test(nz(h.brandName))
  );
  findings.individualsNameVariants = findings.formulaSearchHits.filter((h) =>
    /\bindividuals\b/i.test(nz(h.brandName))
  );
  findings.otherRadissonBrands = findings.formulaSearchHits.filter(
    (h) => !/individuals by choice/i.test(nz(h.brandName))
  );
  // Duplicate risk only if multiple Brand Basics rows look like "Individuals" profiles.
  findings.duplicateIndividualsRisk = (findings.individualsNameVariants || []).length > 1;
  findings.duplicateRisk = findings.duplicateIndividualsRisk;
  findings.siblingRadissonBrandCount = findings.otherRadissonBrands.length;

  const canonicalId = findings.configuredCanonical.recordId;
  const liveIndividuals = findings.nameLookups.find(
    (x) => x.probedName === "Radisson Individuals by Choice" && x.found
  );
  if (
    liveIndividuals?.recordId &&
    canonicalId &&
    liveIndividuals.recordId === canonicalId &&
    !findings.duplicateIndividualsRisk
  ) {
    findings.conclusion =
      "Single canonical Brand Basics record for Radisson Individuals by Choice " +
      `(${canonicalId}). ${findings.siblingRadissonBrandCount} other Radisson* Brand Basics rows are sibling Choice brands, not duplicate Individuals profiles. ` +
      "No separate “Radisson Individuals” (without by Choice) record exists. " +
      "image_remediation / internal_preview_blocked on radisson-individuals-by-choice refers to this same record’s live image-uniqueness / OS debt — not a wrong legacy seed.";
  } else if (findings.duplicateIndividualsRisk) {
    findings.conclusion =
      "Multiple Brand Basics rows match an Individuals-like name. Do not merge automatically. " +
      "Verify which recordId the slug radisson-individuals-by-choice resolves to before remediation.";
  } else {
    findings.conclusion =
      "Could not fully reconcile Radisson Individuals mapping; see nameLookups and formulaSearchHits.";
  }

  return findings;
}

function buildCohortDefinitions() {
  return {
    primaryActiveReleaseCohort: {
      definition:
        "PRIMARY_RELEASE_SLUGS — OS release-readiness + default external quality lock complete cohort",
      slugs: [...PRIMARY_RELEASE_SLUGS],
      testedByExternalQualityLock: true,
      testedByGoldenReleaseSuiteDefault: true,
    },
    restoredLegacyApprovedCohort: {
      definition:
        "VISIBILITY_RESTORED_RELEASE_SLUGS — historically approved profiles restored to public full-profile visibility after visibility fix. Transitional: publicly visible but NOT in default external quality lock (7/7).",
      slugs: [...VISIBILITY_RESTORED_RELEASE_SLUGS],
      testedByExternalQualityLock: false,
      recommendedNext:
        "Add dedicated legacy-restored quality lock OR expand EXTERNAL_QUALITY_LOCK_COHORT once image role-match / uniqueness gates are accepted for these brands.",
    },
    remediationCohort: {
      definition:
        "Profiles with presentation content (or historical approval) that remain externally locked due to image/content gates",
      slugsDynamic: true,
    },
    founderPreviewOnlyCohort: {
      definition:
        "Externally locked but full tabs available with ?beInternalPreview=1 when presentation rows exist",
      slugsDynamic: true,
    },
    externalQualityLockCohort: {
      definition:
        "DEFAULT_BRANDS in brand-explorer-v38-display-quality-lock-audit = INCOMPLETE_CONTROL + PRIMARY_RELEASE (currently 7 primary; incomplete empty)",
      slugs: [...EXTERNAL_QUALITY_LOCK_COHORT],
      incomplete: [...DEFAULT_INCOMPLETE_BRANDS],
      complete: [...DEFAULT_COMPLETE_BRANDS],
      note:
        "7/7 after legacy restore is expected: restored Ascend/Comfort/Curio/Tribute are outside this cohort by design until explicitly added.",
    },
    goldenReleaseSuiteCohort: {
      definition:
        "test:brand-explorer-golden-release-suite defaults to PRIMARY_RELEASE_SLUGS; OS regression uses everhome, kimpton, radisson-individuals-by-choice",
      defaultSlugs: [...PRIMARY_RELEASE_SLUGS],
      osRegressionFocus: [...INTERNAL_PREVIEW_COPY_FOCUS],
      originalGoldenFour: [...ORIGINAL_GOLDEN_RELEASE_SLUGS],
    },
    legacyMigrationSeedCohort: {
      definition: "LEGACY_SEED_BRANDS — founder-named historically finished profiles",
      slugs: [...LEGACY_SEED_SLUGS],
    },
  };
}

export async function runPostVisibilityReconciliationSanityAudit({
  slugs = null,
  includeOsEval = true,
} = {}) {
  const candidates = slugs?.length ? slugs : discoverCandidateSlugs();
  const cohortDefs = buildCohortDefinitions();
  const brandResults = [];
  const ownerCopyFocus = {};

  for (const slug of candidates) {
    let brand = null;
    let os = null;
    try {
      brand = await fetchBrandApi(slug);
    } catch (err) {
      brandResults.push({
        brandName: slug,
        slug,
        brandBasicsRecordId: getLegacySeedBrand(slug)?.recordId || null,
        error: err.message,
        mismatchIssues: ["fetch_failed"],
      });
      continue;
    }
    if (!brand) {
      brandResults.push({
        brandName: slug,
        slug,
        brandBasicsRecordId: getLegacySeedBrand(slug)?.recordId || null,
        error: "not_found",
        mismatchIssues: ["not_found"],
      });
      continue;
    }

    if (includeOsEval && PRIMARY_RELEASE_SLUGS.includes(slug)) {
      try {
        os = await evaluateBrandExplorerOsBrand(slug);
      } catch (err) {
        os = { error: err.message };
      }
    }

    const blocks = brand.brandExplorer?.blocks || [];
    const seed = resolveLegacyApprovedSeed({
      slug,
      recordId: brand.id,
      brandName: brand.name,
    });
    const display = resolveBrandExplorerDisplayState(brand, {
      legacyHistoricalApproved:
        Boolean(seed) || brand.legacyHistoricalApproved === true,
    });
    const imageUniqueness = evaluateImageUniqueness({
      brand,
      presentationRows: blocks,
      brandSlug: slug,
    });
    const imageRoleMatch = evaluateBrandImageRoleMatch({
      presentationRows: blocks,
      brandSlug: slug,
    });
    const flags = readReleaseFlags(brand);

    const gateFailures = [];
    if (!display.completeness?.hasPresentationRows) gateFailures.push("missing_presentation_rows");
    if (!display.completeness?.hasScenarioRows) gateFailures.push("missing_scenario_rows");
    if (!display.completeness?.visualsCountReady) gateFailures.push("visual_asset_counts");
    if (imageUniqueness.pass !== true) gateFailures.push("image_uniqueness");
    if (imageRoleMatch.pass !== true) gateFailures.push("image_role_match");
    if (!flags.activeProfileApproved) gateFailures.push("active_profile_not_approved");
    if (!flags.founderVisualReviewPass) gateFailures.push("founder_visual_review_not_passed");
    for (const g of os?.gateEval?.failedGates || []) {
      if (!gateFailures.includes(g)) gateFailures.push(g);
    }

    const row = {
      brandName: brand.name || seed?.name || slug,
      slug,
      brandBasicsRecordId: brand.id,
      currentOsState: os?.canonicalState || `display:${display.brandExplorerDisplayState}`,
      currentOsAction:
        os?.routing?.allowedNextAction ||
        (PRIMARY_RELEASE_SLUGS.includes(slug) ? null : "not_in_os_default_cohort"),
      currentPublicDisplayState: display.brandExplorerDisplayState,
      founderPreviewDisplayState:
        blocks.length > 0
          ? "full_tabs_with_banner_if_externally_locked"
          : "profile_in_preparation_or_empty",
      shouldRenderFullProfile: display.shouldRenderFullProfile === true,
      liveApiShouldRenderFullProfile: brand.shouldRenderFullProfile === true,
      legacyHistoricalApproved: flags.legacyHistoricalApproved,
      activeProfileApproved: flags.activeProfileApproved,
      founderVisualReviewPass: flags.founderVisualReviewPass,
      readyForActiveProfile: flags.readyForActiveProfile,
      inPrimaryReleaseSlugs: PRIMARY_RELEASE_SLUGS.includes(slug),
      inLegacySeedCohort: Boolean(seed) || LEGACY_SEED_SLUGS.includes(slug),
      inVisibilityRestoredCohort: isVisibilityRestoredReleaseSlug(slug),
      inExternalQualityLockCohort: EXTERNAL_QUALITY_LOCK_COHORT.includes(slug),
      inIncompleteControlCohort: DEFAULT_INCOMPLETE_BRANDS.includes(slug),
      inGoldenReleaseSuiteCohort: PRIMARY_RELEASE_SLUGS.includes(slug),
      inOriginalGoldenFour: ORIGINAL_GOLDEN_RELEASE_SLUGS.includes(slug),
      hasPresentationRows: blocks.length > 0,
      imageUniquenessPass: imageUniqueness.pass === true,
      imageRoleMatchPass: imageRoleMatch.pass === true,
      galleryDistinctCount: imageUniqueness.galleryDistinctCount,
      currentGateFailures: gateFailures,
      osFailedGates: os?.gateEval?.failedGates || [],
      osTrueBlockers: os?.gateEval?.trueBlockers || [],
    };

    row.liveBucket = assignLiveBucket(row);
    row.expectedStatus = classifyExpectedStatus(row);
    row.mismatchIssues = detectMismatches(row);
    brandResults.push(row);

    if (INTERNAL_PREVIEW_COPY_FOCUS.includes(slug)) {
      ownerCopyFocus[slug] = await scanOwnerCopyForBrand(slug, brand);
    }
  }

  const radissonProbe = await probeRadissonRecords();

  const byBucket = {};
  for (const b of brandResults) {
    const k = b.liveBucket || "unknown";
    byBucket[k] = (byBucket[k] || 0) + 1;
  }

  const withMismatches = brandResults.filter((b) => (b.mismatchIssues || []).length > 0);

  return {
    version: SANITY_AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    auditOnly: true,
    guardrails: {
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      ownerFacingContentWrites: false,
      imageWrites: false,
      releaseFieldWrites: false,
    },
    cohortDefinitions: cohortDefs,
    brandResults,
    summary: {
      brandCount: brandResults.length,
      byLiveBucket: byBucket,
      mismatchCount: withMismatches.length,
      primaryVisible: brandResults.filter(
        (b) => b.inPrimaryReleaseSlugs && b.shouldRenderFullProfile
      ).length,
      primaryLocked: brandResults.filter(
        (b) => b.inPrimaryReleaseSlugs && !b.shouldRenderFullProfile
      ).length,
      restoredLegacyVisible: brandResults.filter(
        (b) => b.inVisibilityRestoredCohort && b.shouldRenderFullProfile
      ).length,
      externalQualityLockCohortSize: EXTERNAL_QUALITY_LOCK_COHORT.length,
      whyExternalQualityLockStillSevenOfSeven:
        "External quality lock defaults to PRIMARY_RELEASE_SLUGS only (7 brands). Restored legacy Ascend/Comfort/Curio/Tribute are publicly visible via transitional legacy unlock but are not members of that test cohort yet.",
    },
    radissonIndividualsConflict: radissonProbe,
    internalPreviewCopyInvestigation: {
      focusBrands: [...INTERNAL_PREVIEW_COPY_FOCUS],
      results: ownerCopyFocus,
      osRegressionCommand:
        "npm run test:brand-explorer-internal-preview-owner-copy -- --brands everhome-suites,kimpton,radisson-individuals-by-choice",
      note:
        "Do not patch in this audit. Classify whether hits are public-visible defects or founder-preview hygiene.",
    },
    recommendations: [
      "Treat VISIBILITY_RESTORED_RELEASE_SLUGS as a documented transitional public cohort until a legacy quality lock (or cohort expansion) is approved.",
      "Do not interpret image_remediation on radisson-individuals-by-choice as a duplicate-record problem unless radissonIndividualsConflict.duplicateRisk shows conflicting Individuals record IDs.",
      "Resolve internal-preview-owner-copy hits on Everhome/Kimpton/Radisson before treating OS release-readiness as fully green; external quality lock PASS alone is insufficient for founder path.",
    ],
  };
}

export function writePostVisibilityReconciliationSanityAuditReports(
  report,
  { reportsDir = path.join(ROOT, "reports") } = {}
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    "# Brand Explorer — Post-Visibility Reconciliation Sanity Audit",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    "",
    "**Audit-only.** No Company Validated / Source Library / Registry / content / image / release-field writes.",
    "",
    "## Summary",
    "",
    `- Brands evaluated: **${report.summary.brandCount}**`,
    `- External quality lock cohort size: **${report.summary.externalQualityLockCohortSize}** (still 7/7 by design)`,
    `- Restored legacy publicly visible: **${report.summary.restoredLegacyVisible}**`,
    `- Primary locked: **${report.summary.primaryLocked}**`,
    `- Rows with mismatch flags: **${report.summary.mismatchCount}**`,
    "",
    report.summary.whyExternalQualityLockStillSevenOfSeven,
    "",
    "### Live buckets",
    "",
  ];
  for (const [k, v] of Object.entries(report.summary.byLiveBucket || {})) {
    md.push(`- ${k}: **${v}**`);
  }

  md.push(
    "",
    "## Cohort definitions",
    "",
    "### Primary active release",
    "",
    `\`${(report.cohortDefinitions.primaryActiveReleaseCohort.slugs || []).join(", ")}\``,
    "",
    "### Restored legacy approved (transitional public)",
    "",
    `\`${(report.cohortDefinitions.restoredLegacyApprovedCohort.slugs || []).join(", ")}\``,
    "",
    report.cohortDefinitions.restoredLegacyApprovedCohort.recommendedNext,
    "",
    "### External quality lock cohort",
    "",
    `\`${(report.cohortDefinitions.externalQualityLockCohort.slugs || []).join(", ")}\``,
    "",
    report.cohortDefinitions.externalQualityLockCohort.note,
    "",
    "## Radisson Individuals conflict",
    "",
    report.radissonIndividualsConflict.conclusion,
    "",
    `- Configured canonical record: \`${report.radissonIndividualsConflict.configuredCanonical.recordId}\``,
    `- Unique Radisson* Brand Basics IDs found: **${(report.radissonIndividualsConflict.uniqueRecordIds || []).length}**`,
    `- Duplicate Individuals risk: **${report.radissonIndividualsConflict.duplicateIndividualsRisk ?? report.radissonIndividualsConflict.duplicateRisk}**`,
    `- Sibling Radisson* brands (not Individuals duplicates): **${report.radissonIndividualsConflict.siblingRadissonBrandCount ?? "—"}**`,
    "",
    "### Formula search (Brand Name contains Radisson)",
    ""
  );
  for (const h of report.radissonIndividualsConflict.formulaSearchHits || []) {
    md.push(`- \`${h.recordId}\` — ${h.brandName}`);
  }
  md.push("", "### Name probe results", "");
  for (const h of report.radissonIndividualsConflict.nameLookups || []) {
    md.push(
      `- **${h.probedName}**: found=${h.found} id=\`${h.recordId || "—"}\` state=${h.displayState || "—"} full=${h.shouldRenderFullProfile}`
    );
  }

  md.push(
    "",
    "## Internal-preview-copy investigation (Everhome / Kimpton / Radisson)",
    ""
  );
  for (const slug of report.internalPreviewCopyInvestigation.focusBrands || []) {
    const r = report.internalPreviewCopyInvestigation.results?.[slug];
    if (!r) {
      md.push(`### ${slug}`, "", "_No scan result._", "");
      continue;
    }
    md.push(`### ${slug}`, "");
    md.push(`- Live internal hits: **${r.liveInternalPreviewHits.length}**`);
    md.push(
      `- Projected residual hits: **${(r.projectedInternalPreviewHits || []).length}** (residual patches=${r.residualPatchCount})`
    );
    md.push(`- Public external path hits: **${r.publicExternalHits.length}**`);
    md.push(`- Public affected while full profile: **${r.publicAffected}**`);
    md.push(`- Blocks release baseline: **${r.blocksReleaseBaseline}**`);
    md.push(`- Founder banner false positive: **${r.founderBannerFalsePositive === true}**`);
    md.push(`- Note: ${r.note}`);
    if (r.liveInternalPreviewHits.length) {
      md.push("", "Live hits:");
      for (const h of r.liveInternalPreviewHits) {
        md.push(`- \`${h.id}\` ${h.label}: ${h.snippet}`);
      }
    }
    md.push("");
  }

  md.push(
    "",
    "## Full profile matrix",
    "",
    "| Brand | Slug | Record ID | OS state | OS action | Public display | shouldRenderFull | Legacy hist | Active approved | Founder pass | PRIMARY | Legacy seed | Restored | EQL cohort | Golden | Gate failures | Expected | Mismatches |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
  );
  for (const b of report.brandResults || []) {
    md.push(
      `| ${b.brandName} | ${b.slug} | ${b.brandBasicsRecordId || "—"} | ${b.currentOsState || "—"} | ${b.currentOsAction || "—"} | ${b.currentPublicDisplayState || "—"} | ${b.shouldRenderFullProfile} | ${b.legacyHistoricalApproved} | ${b.activeProfileApproved} | ${b.founderVisualReviewPass} | ${b.inPrimaryReleaseSlugs} | ${b.inLegacySeedCohort} | ${b.inVisibilityRestoredCohort} | ${b.inExternalQualityLockCohort} | ${b.inGoldenReleaseSuiteCohort} | ${(b.currentGateFailures || []).join("; ") || "—"} | ${b.expectedStatus || "—"} | ${(b.mismatchIssues || []).join("; ") || "—"} |`
    );
  }

  md.push("", "## Recommendations", "");
  for (const r of report.recommendations || []) md.push(`- ${r}`);
  md.push("");

  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}
