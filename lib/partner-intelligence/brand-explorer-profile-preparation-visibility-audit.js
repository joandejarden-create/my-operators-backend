/**
 * Brand Explorer — Profile in Preparation visibility audit.
 *
 * Discovers every brand currently rendering (or would render) the external
 * Profile in Preparation shell, classifies each, and recommends a visibility action.
 * Read-only / dry-run by default.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listActiveProfileBrandSlugs, getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { FACTORY_SUPPORTED_SLUGS } from "./brand-explorer-active-profile-factory-rules.js";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { resolveBrandExplorerDisplayState } from "./brand-explorer-display-state.js";
import {
  LEGACY_SEED_BRANDS,
  LEGACY_SEED_SLUGS,
  getLegacySeedBrand,
  isLegacyApprovedSeedSlug,
  resolveLegacyApprovedSeed,
} from "./brand-explorer-legacy-approved-profile-reconciliation.js";

export const VISIBILITY_AUDIT_VERSION = "profile-preparation-visibility-audit-v1";
export const REPORT_JSON = "brand-explorer-profile-preparation-visibility-audit.json";
export const REPORT_MD = "brand-explorer-profile-preparation-visibility-audit.md";

export const VISIBILITY_CLASSIFICATIONS = Object.freeze([
  "legacy_approved_ready_to_restore",
  "legacy_approved_pending_migration",
  "founder_preview_required",
  "image_remediation_required",
  "content_remediation_required",
  "no_legacy_evidence",
  "already_full_profile",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function discoverCandidateSlugs() {
  const set = new Set([
    ...LEGACY_SEED_SLUGS,
    ...FACTORY_SUPPORTED_SLUGS,
    ...listActiveProfileBrandSlugs(),
    ...PRIMARY_RELEASE_SLUGS,
  ]);
  return [...set]
    .filter(Boolean)
    .filter((slug) => {
      const seed = getLegacySeedBrand(slug);
      return !seed || seed.slug === slug;
    })
    .sort();
}

function loadHistoricalReportSignals(reportsDir) {
  const signals = new Map();
  if (!fs.existsSync(reportsDir)) return signals;
  const files = fs.readdirSync(reportsDir).filter((f) => /brand-explorer.*\.(json)$/i.test(f));
  for (const file of files.slice(0, 140)) {
    try {
      const raw = JSON.parse(fs.readFileSync(path.join(reportsDir, file), "utf8"));
      const rows = raw.brandResults || raw.brands || raw.results || [];
      for (const row of Array.isArray(rows) ? rows : []) {
        const slug = nz(row.brandSlug || row.slug || row.brand?.slug).toLowerCase();
        if (!slug) continue;
        const ready =
          row.readyForActiveProfile === true ||
          row.summary?.readyForActiveProfile === true ||
          row.overallStatus === "ready" ||
          row.eligibility?.status === "active_profile_ready" ||
          row.canonicalState === "active_profile_ready" ||
          row.displayState === "active_profile_ready" ||
          row.liveState?.displayState === "active_profile_ready";
        if (!signals.has(slug)) signals.set(slug, { sources: [], ready: false });
        const entry = signals.get(slug);
        if (ready) {
          entry.ready = true;
          entry.sources.push(file);
        }
      }
      const completeSlug = nz(raw.brand?.slug || raw.resolution?.resolvedSlug).toLowerCase();
      if (completeSlug) {
        const ready =
          raw.readyForActiveProfile === true ||
          (raw.halted === false && /complete-build/i.test(file));
        if (!signals.has(completeSlug)) signals.set(completeSlug, { sources: [], ready: false });
        if (ready) {
          signals.get(completeSlug).ready = true;
          signals.get(completeSlug).sources.push(file);
        }
      }
    } catch {
      // skip unreadable reports
    }
  }
  return signals;
}

async function fetchBrandApi(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const seed = getLegacySeedBrand(slug);
  const lookupId = seed?.recordId || slug;
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

function readHistoricalStatusFields(brand) {
  const g = brand?.governance || {};
  const c = brand?.brandExplorerDisplayCompleteness || {};
  return {
    readyForActiveProfile: brand?.readyForActiveProfile === true,
    activeProfileApproved: brand?.activeProfileApproved === true || c.activeProfileApproved === true,
    founderVisualReviewPass:
      brand?.founderVisualReviewPass === true || c.founderVisualReviewPass === true,
    companyValidated: g.companyValidated === true || c.companyValidated === true,
    historicalApproved: c.historicalApproved === true || brand?.legacyHistoricalApproved === true,
    legacyHistoricalApproved: brand?.legacyHistoricalApproved === true,
  };
}

function classifyVisibilityRow({
  brand,
  slug,
  display,
  imageUniqueness,
  imageRoleMatch,
  historicalReportReady,
  seedNamed,
}) {
  const hist = readHistoricalStatusFields(brand);
  const historicalEvidence =
    hist.historicalApproved || historicalReportReady || seedNamed || hist.legacyHistoricalApproved;
  const full = display.shouldRenderFullProfile === true;
  const locked = !full;
  const hasRows = display.completeness?.hasPresentationRows === true;
  const visualsCountReady = display.completeness?.visualsCountReady === true;
  const uniquenessPass = imageUniqueness.pass === true;
  const roleMatchPass = imageRoleMatch.pass === true;
  const activeApproved = hist.activeProfileApproved;
  const inPrimary = PRIMARY_RELEASE_SLUGS.includes(slug);

  const missingGates = [];
  if (!hasRows) missingGates.push("presentation_rows");
  if (!display.completeness?.hasScenarioRows) missingGates.push("scenario_rows");
  if (!visualsCountReady) missingGates.push("visual_asset_counts");
  if (!uniquenessPass) missingGates.push("image_uniqueness");
  if (!roleMatchPass) missingGates.push("image_role_match");
  if (!activeApproved) missingGates.push("active_profile_approved");
  if (!hist.founderVisualReviewPass) missingGates.push("founder_visual_review_pass");
  if (!inPrimary) missingGates.push("primary_release_cohort");

  let classification = "no_legacy_evidence";
  let recommendedAction = "keep_locked_shell_until_content_exists";
  let migrationNote = null;

  if (full && activeApproved) {
    classification = "already_full_profile";
    recommendedAction = "no_action";
  } else if (full && historicalEvidence && !activeApproved) {
    classification = "legacy_approved_pending_migration";
    recommendedAction = "migrate_display_fields_when_confirmed";
    migrationNote =
      "Legacy approved with full-profile visibility via reconciliation unlock; pending Active Profile field migration.";
  } else if (historicalEvidence && uniquenessPass && visualsCountReady && hasRows && locked === false) {
    classification = "legacy_approved_ready_to_restore";
    recommendedAction = "restore_full_profile_visibility";
  } else if (historicalEvidence && uniquenessPass && visualsCountReady && hasRows && !activeApproved) {
    classification = "legacy_approved_ready_to_restore";
    recommendedAction = "restore_full_profile_visibility";
  } else if (historicalEvidence && !hasRows) {
    classification = "content_remediation_required";
    recommendedAction = "content_remediation_required";
    migrationNote = "Legacy approved, but blocked by current content gates (missing presentation rows).";
  } else if (historicalEvidence && (!uniquenessPass || !visualsCountReady)) {
    classification = "image_remediation_required";
    recommendedAction = "image_remediation_required";
    migrationNote =
      "Legacy approved, but blocked by current image/content/source gates.";
  } else if (historicalEvidence && hasRows && (!roleMatchPass || !activeApproved)) {
    classification = "founder_preview_required";
    recommendedAction = "use_beInternalPreview_for_review";
    migrationNote =
      "Profile has content for founder review; external release still pending gate migration.";
  } else if (!historicalEvidence && locked) {
    classification = "no_legacy_evidence";
    recommendedAction = "keep_locked_shell";
  } else if (full) {
    classification = "already_full_profile";
    recommendedAction = "no_action";
  } else if (hasRows) {
    classification = "founder_preview_required";
    recommendedAction = "use_beInternalPreview_for_review";
  }

  // Refine: migrate-ready when historically approved, uniqueness+counts pass, not yet active approved
  if (
    historicalEvidence &&
    hasRows &&
    visualsCountReady &&
    uniquenessPass &&
    !activeApproved
  ) {
    classification = full
      ? "legacy_approved_pending_migration"
      : "legacy_approved_ready_to_restore";
    recommendedAction = "restore_full_profile_visibility";
  }

  const showsProfileInPreparation = locked;

  return {
    brandName: brand?.name || getLegacySeedBrand(slug)?.name || slug,
    slug,
    brandBasicsRecordId: brand?.id || getLegacySeedBrand(slug)?.recordId || null,
    currentOsState: display.brandExplorerDisplayState,
    currentDisplayState: display.brandExplorerDisplayState,
    shouldRenderFullProfile: full,
    currentExternalBehavior: full ? "full_profile" : "profile_in_preparation_shell",
    showsProfileInPreparation,
    historicalStatusFields: hist,
    legacyApprovalEvidence: {
      seedNamed,
      historicalReportReady,
      reportSources: [],
      legacyHistoricalApprovedFlag: brand?.legacyHistoricalApproved === true,
    },
    activeProfileFields: {
      readyForActiveProfile: hist.readyForActiveProfile,
      activeProfileApproved: hist.activeProfileApproved,
      founderVisualReviewPass: hist.founderVisualReviewPass,
    },
    releaseCohortMembership: {
      inPrimaryReleaseSlugs: inPrimary,
      inLegacySeedCohort: seedNamed,
      inActiveProfileConfig: Boolean(getActiveProfileBrandConfig(slug)),
    },
    missingGates,
    imageUniquenessPass: uniquenessPass,
    imageRoleMatchPass: roleMatchPass,
    galleryDistinctCount: imageUniqueness.galleryDistinctCount,
    propertyExampleDistinctCount: imageUniqueness.propertyExampleDistinctCount,
    blockers: display.blockers || [],
    classification,
    recommendedAction,
    migrationNote,
    companyValidatedUntouched: true,
  };
}

export async function runProfilePreparationVisibilityAudit({
  reportsDir = path.join(ROOT, "reports"),
  slugs = null,
  onlyLocked = false,
} = {}) {
  const candidates = slugs?.length ? slugs : discoverCandidateSlugs();
  const reportSignals = loadHistoricalReportSignals(reportsDir);
  const brandResults = [];

  for (const slug of candidates) {
    let brand = null;
    try {
      brand = await fetchBrandApi(slug);
    } catch (err) {
      brandResults.push({
        brandName: slug,
        slug,
        brandBasicsRecordId: getLegacySeedBrand(slug)?.recordId || null,
        currentOsState: "unknown",
        currentDisplayState: "unknown",
        shouldRenderFullProfile: false,
        currentExternalBehavior: "unknown",
        showsProfileInPreparation: true,
        classification: "no_legacy_evidence",
        recommendedAction: "unresolved_fetch_failed",
        error: err.message,
        companyValidatedUntouched: true,
      });
      continue;
    }
    if (!brand) {
      if (!getLegacySeedBrand(slug) && !reportSignals.get(slug)?.ready) continue;
      brandResults.push({
        brandName: slug,
        slug,
        brandBasicsRecordId: getLegacySeedBrand(slug)?.recordId || null,
        currentOsState: "not_found",
        currentDisplayState: "not_found",
        shouldRenderFullProfile: false,
        currentExternalBehavior: "not_found",
        showsProfileInPreparation: true,
        classification: isLegacyApprovedSeedSlug(slug) ? "content_remediation_required" : "no_legacy_evidence",
        recommendedAction: "unresolved_not_found",
        companyValidatedUntouched: true,
      });
      continue;
    }

    const blocks = brand.brandExplorer?.blocks || [];
    const imageUniqueness = evaluateImageUniqueness({
      brand,
      presentationRows: blocks,
      brandSlug: slug,
    });
    const imageRoleMatch = evaluateBrandImageRoleMatch({
      presentationRows: blocks,
      brandSlug: slug,
    });
    const seed = resolveLegacyApprovedSeed({
      slug,
      recordId: brand.id,
      brandName: brand.name,
    });
    const seedNamed = Boolean(seed) || isLegacyApprovedSeedSlug(slug);
    const historicalReportReady = reportSignals.get(slug)?.ready === true;
    const display = resolveBrandExplorerDisplayState(brand, {
      legacyHistoricalApproved: historicalReportReady || seedNamed || brand.legacyHistoricalApproved === true,
    });

    const row = classifyVisibilityRow({
      brand,
      slug,
      display,
      imageUniqueness,
      imageRoleMatch,
      historicalReportReady,
      seedNamed,
    });
    if (historicalReportReady) {
      row.legacyApprovalEvidence.reportSources = reportSignals.get(slug)?.sources?.slice(0, 8) || [];
    }

    if (onlyLocked && !row.showsProfileInPreparation) continue;
    brandResults.push(row);
  }

  const locked = brandResults.filter((b) => b.showsProfileInPreparation);
  const byClass = {};
  for (const b of brandResults) {
    byClass[b.classification] = (byClass[b.classification] || 0) + 1;
  }

  return {
    version: VISIBILITY_AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    primaryReleaseSlugs: [...PRIMARY_RELEASE_SLUGS],
    legacySeedSlugs: [...LEGACY_SEED_SLUGS],
    candidateCount: candidates.length,
    brandResults,
    profileInPreparationBrands: locked,
    summary: {
      brandCount: brandResults.length,
      profileInPreparationCount: locked.length,
      byClassification: byClass,
      readyToRestore: brandResults.filter((b) => b.classification === "legacy_approved_ready_to_restore")
        .length,
      pendingMigration: brandResults.filter(
        (b) => b.classification === "legacy_approved_pending_migration"
      ).length,
      imageRemediation: brandResults.filter((b) => b.classification === "image_remediation_required")
        .length,
      contentRemediation: brandResults.filter((b) => b.classification === "content_remediation_required")
        .length,
      founderPreviewRequired: brandResults.filter((b) => b.classification === "founder_preview_required")
        .length,
      noLegacyEvidence: brandResults.filter((b) => b.classification === "no_legacy_evidence").length,
    },
  };
}

export function writeProfilePreparationVisibilityAuditReports(
  report,
  { reportsDir = path.join(ROOT, "reports") } = {}
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    "# Brand Explorer — Profile in Preparation Visibility Audit",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    "",
    `- Candidates evaluated: **${report.summary.brandCount}**`,
    `- Showing Profile in Preparation: **${report.summary.profileInPreparationCount}**`,
    `- Ready to restore: **${report.summary.readyToRestore}**`,
    `- Pending migration (already full via legacy unlock): **${report.summary.pendingMigration}**`,
    `- Image remediation: **${report.summary.imageRemediation}**`,
    `- Content remediation: **${report.summary.contentRemediation}**`,
    `- Founder preview required: **${report.summary.founderPreviewRequired}**`,
    `- No legacy evidence: **${report.summary.noLegacyEvidence}**`,
    "",
    "## Classification counts",
    "",
  ];
  for (const [k, v] of Object.entries(report.summary.byClassification || {})) {
    md.push(`- ${k}: **${v}**`);
  }

  md.push(
    "",
    "## Profiles currently showing Profile in Preparation",
    "",
    "| Brand | Slug | Display State | Classification | Missing Gates | Recommended Action | Migration Note |",
    "| --- | --- | --- | --- | --- | --- | --- |"
  );
  for (const b of report.profileInPreparationBrands || []) {
    md.push(
      `| ${b.brandName} | ${b.slug} | ${b.currentDisplayState} | ${b.classification} | ${(b.missingGates || []).join("; ") || "—"} | ${b.recommendedAction} | ${b.migrationNote || "—"} |`
    );
  }

  md.push(
    "",
    "## Full matrix",
    "",
    "| Brand | Slug | Record ID | OS/Display State | shouldRenderFullProfile | External Behavior | Classification | In PRIMARY_RELEASE | Recommended Action |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"
  );
  for (const b of report.brandResults || []) {
    md.push(
      `| ${b.brandName} | ${b.slug} | ${b.brandBasicsRecordId || "—"} | ${b.currentDisplayState} | ${b.shouldRenderFullProfile} | ${b.currentExternalBehavior} | ${b.classification} | ${b.releaseCohortMembership?.inPrimaryReleaseSlugs ? "yes" : "no"} | ${b.recommendedAction} |`
    );
  }
  md.push("");
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}

export { LEGACY_SEED_BRANDS };
