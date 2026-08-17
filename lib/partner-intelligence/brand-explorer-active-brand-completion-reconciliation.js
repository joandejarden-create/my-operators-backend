/**
 * Brand Explorer — Active Brand Completion Reconciliation (read-only).
 *
 * IMPORTANT: discoverActiveBrandIdentities() is a historical CODE-UNION inventory.
 * It is NOT the canonical active universe. Prefer:
 *   lib/partner-intelligence/brand-explorer-active-universe.js
 *   Brand Basics Brand Status Active/Live (BRAND_STATUS_ACTIVE_FORMULA)
 *
 * This module evaluates PVQL / Tab Factory gates without writes for restore vs
 * remediate vs hold planning. Never writes Airtable. Never changes Company
 * Validated / Source / Registry / release.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listActiveProfileBrandSlugs, getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { FACTORY_SUPPORTED_SLUGS } from "./brand-explorer-active-profile-factory-rules.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import {
  LEGACY_SEED_BRANDS,
  LEGACY_SEED_SLUGS,
  getLegacySeedBrand,
} from "./brand-explorer-legacy-approved-profile-reconciliation.js";
import {
  VISIBILITY_RESTORED_RELEASE_SLUGS,
  isVisibilityRestoredReleaseSlug,
} from "./brand-explorer-profile-preparation-visibility-fix.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import {
  evaluateBrandPublicVisibility,
  isOwnerFacingPresentationRow,
} from "./brand-explorer-public-visibility-quality-lock.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { evaluateGoldenContentQuality } from "./brand-explorer-golden-content-quality.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";

export const RECONCILIATION_VERSION = "active-brand-completion-reconciliation-v1";
export const REPORT_JSON = "brand-explorer-active-brand-completion-reconciliation.json";
export const REPORT_MD = "brand-explorer-active-brand-completion-reconciliation.md";
export const REPORT_INVENTORY_MD = "brand-explorer-active-brand-inventory.md";
export const REPORT_RESTORE_MD = "brand-explorer-active-brand-restore-candidates.md";
export const REPORT_PLAN_MD = "brand-explorer-active-brand-remediation-plan.md";

export const CLASSIFICATION_BUCKETS = Object.freeze([
  "public_full_clean",
  "ready_to_restore_public_full",
  "minor_gate_fix_needed",
  "image_remediation_needed",
  "content_remediation_needed",
  "true_incomplete",
  "duplicate_or_slug_mapping_issue",
]);

/**
 * Explicit identity map for historically active brands outside LEGACY_SEED /
 * PRIMARY configs. Prefer recordId lookup for Brand Library.
 */
export const EXTENDED_ACTIVE_BRAND_IDENTITIES = Object.freeze([
  { slug: "radisson", recordId: "recywbx1YQSTCPqW1", name: "Radisson by Choice", historicalDoneEvidence: "ACTIVE_BRAND_AUDIT_TARGETS + Choice family active profile" },
  { slug: "radisson-blu", recordId: "recWPEvxBQxVVzSq3", name: "Radisson Blu by Choice", historicalDoneEvidence: "ACTIVE_BRAND_AUDIT_TARGETS + Choice family repair writer" },
  { slug: "radisson-red", recordId: "recmKqo7M7mLZgRqQ", name: "Radisson RED by Choice", historicalDoneEvidence: "ACTIVE_BRAND_BATCH + presentation fixtures (206 rows live)" },
  { slug: "radisson-collection", recordId: "rec2DDyPu38C6zDBC", name: "Radisson Collection", historicalDoneEvidence: "Radisson family probe; Brand Basics exists" },
  { slug: "tapestry-collection-by-hilton", recordId: "reccXxMHEh7NNRhIE", name: "Tapestry Collection by Hilton", historicalDoneEvidence: "WAVE1_EXPANSION_SLUGS" },
  { slug: "quality-inn", recordId: null, name: "Quality Inn", lookupNames: ["Quality Inn"], historicalDoneEvidence: "ACTIVE_BRAND_BATCH governance cohort" },
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function uniq(arr) {
  return [...new Set(arr.filter(Boolean))];
}

export function discoverActiveBrandIdentities() {
  const bySlug = new Map();

  function upsert({ slug, recordId = null, name = null, sources = [], historicalDoneEvidence = null }) {
    if (!slug) return;
    const prev = bySlug.get(slug) || {
      slug,
      recordId: null,
      name: null,
      sources: [],
      historicalDoneEvidence: [],
    };
    prev.recordId = prev.recordId || recordId || null;
    prev.name = prev.name || name || null;
    prev.sources = uniq([...prev.sources, ...sources]);
    if (historicalDoneEvidence) {
      prev.historicalDoneEvidence = uniq([
        ...(prev.historicalDoneEvidence || []),
        historicalDoneEvidence,
      ]);
    }
    bySlug.set(slug, prev);
  }

  for (const slug of PRIMARY_RELEASE_SLUGS) {
    const cfg = getActiveProfileBrandConfig(slug) || getDiscoveryBrandConfig(slug);
    upsert({
      slug,
      recordId: cfg?.recordId || null,
      name: cfg?.name || null,
      sources: ["PRIMARY_RELEASE_SLUGS"],
      historicalDoneEvidence: "PRIMARY_RELEASE_SLUGS / OS release cohort",
    });
  }
  for (const seed of LEGACY_SEED_BRANDS) {
    upsert({
      slug: seed.slug,
      recordId: seed.recordId,
      name: seed.name || null,
      sources: ["LEGACY_SEED_BRANDS"],
      historicalDoneEvidence: "LEGACY_SEED_BRANDS — founder historically finished profile",
    });
  }
  for (const slug of FACTORY_SUPPORTED_SLUGS) {
    const cfg = getActiveProfileBrandConfig(slug);
    upsert({
      slug,
      recordId: cfg?.recordId || null,
      name: cfg?.name || null,
      sources: ["FACTORY_SUPPORTED_SLUGS"],
    });
  }
  for (const slug of listActiveProfileBrandSlugs()) {
    const cfg = getActiveProfileBrandConfig(slug);
    upsert({
      slug,
      recordId: cfg?.recordId || null,
      name: cfg?.name || null,
      sources: ["ACTIVE_PROFILE_BRAND_CONFIGS"],
    });
  }
  for (const slug of VISIBILITY_RESTORED_RELEASE_SLUGS) {
    upsert({
      slug,
      sources: ["VISIBILITY_RESTORED_RELEASE_SLUGS"],
      historicalDoneEvidence: "Visibility-restored public cohort",
    });
  }
  for (const t of ACTIVE_BRAND_AUDIT_TARGETS) {
    upsert({
      slug: t.slug,
      recordId: t.recordId,
      name: t.name,
      sources: ["ACTIVE_BRAND_AUDIT_TARGETS"],
      historicalDoneEvidence: "ACTIVE_BRAND_AUDIT_TARGETS — historically Explorer-active",
    });
  }
  for (const slug of WAVE1_EXPANSION_SLUGS) {
    upsert({
      slug,
      sources: ["WAVE1_EXPANSION_SLUGS"],
    });
  }
  for (const ext of EXTENDED_ACTIVE_BRAND_IDENTITIES) {
    upsert({
      slug: ext.slug,
      recordId: ext.recordId,
      name: ext.name,
      sources: ["EXTENDED_ACTIVE_BRAND_IDENTITIES"],
      historicalDoneEvidence: ext.historicalDoneEvidence,
    });
  }

  return [...bySlug.values()].sort((a, b) => a.slug.localeCompare(b.slug));
}

async function fetchBrandApi(identity) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const seed = getLegacySeedBrand(identity.slug);
  const candidates = uniq([
    identity.recordId,
    seed?.recordId,
    identity.slug,
    identity.name,
    ...(EXTENDED_ACTIVE_BRAND_IDENTITIES.find((e) => e.slug === identity.slug)?.lookupNames || []),
  ]);

  for (const lookupId of candidates) {
    if (!lookupId) continue;
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
    try {
      await getBrandLibraryBrandById({ query: { brandId: lookupId }, headers: {} }, res);
    } catch (err) {
      continue;
    }
    if (res.statusCode === 200 && res.payload?.brand) {
      return { brand: res.payload.brand, lookupIdUsed: lookupId };
    }
  }
  return { brand: null, lookupIdUsed: null };
}

function countGallery(blocks) {
  return (blocks || []).filter(
    (b) =>
      /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) &&
      nz(b.imageUrl) &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  ).length;
}

function countOpenings(blocks) {
  return (blocks || []).filter(
    (b) =>
      nz(b.slotKey) === "footprint.openings" &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  ).length;
}

function readHistoricalDone(brand, identity) {
  const evidence = [...(identity.historicalDoneEvidence || [])];
  if (brand?.legacyHistoricalApproved === true) evidence.push("API legacyHistoricalApproved");
  if (brand?.founderVisualReviewPass === true) evidence.push("Founder Visual Review Pass");
  if (brand?.readyForActiveProfile === true || brand?.activeProfileApproved === true) {
    evidence.push("Ready for Active Profile / Active Profile Approved");
  }
  if (LEGACY_SEED_SLUGS.includes(identity.slug)) evidence.push("LEGACY_SEED_SLUGS");
  if (PRIMARY_RELEASE_SLUGS.includes(identity.slug)) evidence.push("PRIMARY_RELEASE_SLUGS");
  if (isVisibilityRestoredReleaseSlug(identity.slug)) evidence.push("VISIBILITY_RESTORED");
  const rows = brand?.brandExplorer?.blocks?.length || 0;
  if (rows >= 40) evidence.push(`Presentation depth (${rows} rows)`);
  return {
    historicallyDone: evidence.length > 0 && (rows >= 20 || brand?.legacyHistoricalApproved === true || LEGACY_SEED_SLUGS.includes(identity.slug) || PRIMARY_RELEASE_SLUGS.includes(identity.slug) || isVisibilityRestoredReleaseSlug(identity.slug)),
    evidence: uniq(evidence),
  };
}

export function classifyBrand(row) {
  const fails = row.gateFailures || [];
  const imageFails = fails.filter((f) =>
    /image_uniqueness|image_role|scenario_image|property_image|gallery_distinct|wrong_brand|role_match/i.test(f)
  );
  const contentFails = fails.filter((f) =>
    /rendered_field|tab_factory|empty_rendered|forbidden|generic_copy|golden|provenance|blank|completeness/i.test(f)
  );
  const displayFails = fails.filter((f) =>
    /display_consistency|company_validated|release|not_public|locked/i.test(f)
  );

  if (row.slugMappingIssue) {
    return {
      bucket: "duplicate_or_slug_mapping_issue",
      rationale: row.slugMappingIssue,
      blockerType: "administrative",
    };
  }
  if (!row.fetchOk) {
    return {
      bucket: "duplicate_or_slug_mapping_issue",
      rationale: "Brand Library fetch failed for all lookup candidates",
      blockerType: "administrative",
    };
  }
  if (row.publicFullProfile && row.pvqlPass) {
    return {
      bucket: "public_full_clean",
      rationale: "Already public-full and passes PVQL lock",
      blockerType: "none",
    };
  }

  const presentationSparse = (row.ownerFacingRowCount || 0) < 15 && (row.presentationRowCount || 0) < 25;
  if (presentationSparse && !row.historicallyDone) {
    return {
      bucket: "true_incomplete",
      rationale: `Sparse Presentation (ownerFacing=${row.ownerFacingRowCount}, all=${row.presentationRowCount}); no strong historical-done evidence`,
      blockerType: "substantive",
    };
  }
  if (presentationSparse && row.historicallyDone) {
    return {
      bucket: "true_incomplete",
      rationale: "Historically referenced but Presentation depth is too thin for restore — needs Tab Factory build",
      blockerType: "substantive",
    };
  }

  if (!row.publicFullProfile && row.historicallyDone) {
    const failFindings = row.failFindings || 0;
    const onlyDisplayOrRelease =
      (fails.length === 0 ||
        (fails.length <= 3 && contentFails.length === 0 && imageFails.length === 0) ||
        (displayFails.length && !contentFails.length && !imageFails.length)) &&
      failFindings === 0 &&
      row.tabFactoryPass === true;
    if (onlyDisplayOrRelease) {
      return {
        bucket: "ready_to_restore_public_full",
        rationale:
          "Historically done with passing tab-factory quality; not public-full — needs display/release reconciliation only",
        blockerType: "administrative",
      };
    }
    if (imageFails.length && failFindings <= 10 && contentFails.length <= 2) {
      return {
        bucket: "image_remediation_needed",
        rationale: `Historically done; image gates block: ${imageFails.join(", ") || "image"}`,
        blockerType: "substantive",
      };
    }
    if (failFindings > 0 && failFindings <= 10 && imageFails.length === 0) {
      return {
        bucket: "minor_gate_fix_needed",
        rationale: `Historically done; narrow field debt (failFindings=${failFindings}): ${contentFails.slice(0, 6).join(", ") || fails.slice(0, 4).join(", ")}`,
        blockerType: "substantive",
      };
    }
    if (imageFails.length && failFindings > 10) {
      return {
        bucket: "content_remediation_needed",
        rationale: `Historically done; image + content debt (failFindings=${failFindings})`,
        blockerType: "substantive",
      };
    }
    if (failFindings > 10 || contentFails.length > 0) {
      return {
        bucket: "content_remediation_needed",
        rationale: `Historically done but material content/gate debt (failFindings=${failFindings})`,
        blockerType: "substantive",
      };
    }
    return {
      bucket: "ready_to_restore_public_full",
      rationale:
        "Historically done / approved evidence present; not public-full — prioritize visibility reconciliation then re-gate",
      blockerType: "administrative",
    };
  }

  // Not public-full, not historically done (or weak evidence)
  if (imageFails.length && contentFails.length <= 2) {
    return {
      bucket: "image_remediation_needed",
      rationale: `Image remediation primary: ${imageFails.join(", ")}`,
      blockerType: "substantive",
    };
  }
  if ((row.failFindings || 0) > 0 && (row.failFindings || 0) <= 10 && imageFails.length === 0) {
    return {
      bucket: "minor_gate_fix_needed",
      rationale: `Narrow gate debt (${row.failFindings} field fails)`,
      blockerType: "substantive",
    };
  }
  if ((row.failFindings || 0) > 10 || contentFails.length > 3) {
    return {
      bucket: "content_remediation_needed",
      rationale: `Content remediation needed (failFindings=${row.failFindings})`,
      blockerType: "substantive",
    };
  }
  if ((row.ownerFacingRowCount || 0) < 15) {
    return {
      bucket: "true_incomplete",
      rationale: "Insufficient owner-facing Presentation for public-full",
      blockerType: "substantive",
    };
  }
  return {
    bucket: "minor_gate_fix_needed",
    rationale: "Default: profile present; re-check after display reconciliation",
    blockerType: "administrative",
  };
}

export function buildPlanEntry(row) {
  const bucket = row.classification.bucket;
  let effort = "hold";
  let recommendedAction = "hold_pending_source_evidence";
  let expectedCommand = "n/a";
  let founderReviewNeeded = false;
  let risk = "Low";

  switch (bucket) {
    case "public_full_clean":
      effort = "none";
      recommendedAction = "maintain_public_visibility_baseline";
      expectedCommand = "npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only";
      risk = "Low";
      break;
    case "ready_to_restore_public_full":
      effort = "immediate_restore";
      recommendedAction = "visibility_release_reconciliation_only";
      expectedCommand =
        "npm run brand-explorer-profile-preparation-visibility-fix -- --dry-run  (then gated apply for this slug)";
      founderReviewNeeded = row.founderVisualReviewPass !== true;
      risk = "Medium";
      break;
    case "minor_gate_fix_needed":
      effort = "quick_fix";
      recommendedAction = "targeted_field_or_suppression_patches_lt_10";
      expectedCommand = "npm run brand-explorer-public-profile-stabilization -- --brands " + row.slug;
      risk = "Medium";
      break;
    case "image_remediation_needed":
      effort = "image_only_remediation";
      recommendedAction = "gallery_scenario_property_image_remediation";
      expectedCommand =
        "npm run brand-explorer-image-role-match-audit -- --brands " +
        row.slug +
        " ; npm run brand-explorer-image-uniqueness-audit -- --brands " +
        row.slug;
      founderReviewNeeded = true;
      risk = "High";
      break;
    case "content_remediation_needed":
      effort = "content_light_remediation";
      recommendedAction = "targeted_presentation_field_gate_completion";
      expectedCommand = "npm run brand-explorer-public-profile-stabilization -- --brands " + row.slug;
      risk = "High";
      break;
    case "true_incomplete":
      effort = "full_rebuild";
      recommendedAction = "tab_factory_build_from_scratch";
      expectedCommand = "npm run brand-explorer-active-profile-factory -- --brand " + row.slug;
      founderReviewNeeded = true;
      risk = "High";
      break;
    case "duplicate_or_slug_mapping_issue":
      effort = "hold";
      recommendedAction = "resolve_slug_recordid_mapping";
      expectedCommand = "manual Brand Library / Brand Basics identity audit";
      risk = "Medium";
      break;
    default:
      break;
  }

  return {
    effort,
    recommendedAction,
    expectedCommand,
    riskLevel: risk,
    fieldsLikelyAffected:
      bucket === "ready_to_restore_public_full"
        ? ["Ready for Active Profile", "Active Profile Approved", "Founder Visual Review Pass", "display state"]
        : bucket === "image_remediation_needed"
          ? ["Presentation Image", "External Display Status", "Title/caption"]
          : bucket.includes("content") || bucket === "minor_gate_fix_needed"
            ? ["Presentation Body", "Title", "Case Summary*", "External Display Status"]
            : [],
    founderVisualReviewNeededBeforeRestore: founderReviewNeeded,
  };
}

export async function evaluateActiveBrand(identity) {
  const { brand, lookupIdUsed } = await fetchBrandApi(identity);
  if (!brand) {
    const base = {
      brandName: identity.name || identity.slug,
      slug: identity.slug,
      recordId: identity.recordId,
      fetchOk: false,
      lookupIdUsed,
      sources: identity.sources,
      publicFullProfile: false,
      currentOsState: null,
      currentPublicDisplay: null,
      founderPreviewDisplay: null,
      historicallyDone: Boolean(identity.historicalDoneEvidence?.length),
      historicalDoneEvidence: identity.historicalDoneEvidence || [],
      legacyApprovalEvidence: LEGACY_SEED_SLUGS.includes(identity.slug),
      presentationRowCount: 0,
      ownerFacingRowCount: 0,
      galleryCount: 0,
      propertyCount: 0,
      pvqlPass: null,
      tabFactoryPass: null,
      failFindings: null,
      gateFailures: ["brand_api_fetch_failed"],
      slugMappingIssue: identity.recordId
        ? `recordId ${identity.recordId} / slug ${identity.slug} did not resolve`
        : `slug ${identity.slug} unresolved`,
      companyValidatedUntouched: true,
    };
    const classification = classifyBrand(base);
    return {
      ...base,
      classification,
      plan: buildPlanEntry({ ...base, classification, founderVisualReviewPass: false }),
    };
  }

  const blocks = brand.brandExplorer?.blocks || [];
  const ownerFacing = blocks.filter(isOwnerFacingPresentationRow);
  const hist = readHistoricalDone(brand, identity);
  const html = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: false,
  });
  const htmlFounder = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: true,
  });

  const brandConfig =
    getActiveProfileBrandConfig(identity.slug) ||
    getDiscoveryBrandConfig(identity.slug) ||
    getLegacySeedBrand(identity.slug);

  const tabFactory = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerFacing,
    html,
    brandSlug: identity.slug,
    brandConfig,
  });
  const uniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: ownerFacing,
    brandSlug: identity.slug,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: ownerFacing,
    brandSlug: identity.slug,
  });
  const golden = evaluateGoldenContentQuality(brand, ownerFacing, html, {
    brandSlug: identity.slug,
  });
  const externalQl = evaluateBrandExternalQualityLock(brand, html, {
    brandSlug: identity.slug,
  });
  const founderQl = evaluateBrandExternalQualityLock(brand, htmlFounder, {
    brandSlug: identity.slug,
  });

  let pvql = null;
  try {
    pvql = await evaluateBrandPublicVisibility(identity.slug);
  } catch (err) {
    pvql = { lockPass: null, failures: [`pvql_error:${err.message}`], error: err.message };
  }

  const gateFailures = uniq([
    ...(pvql?.failures || []),
    ...(tabFactory.auditPass ? [] : ["tab_factory_audit"]),
    ...(tabFactory.completeness?.auditPass ? [] : ["rendered_field_completeness"]),
    ...(tabFactory.emptyScan?.pass ? [] : ["no_empty_rendered_components"]),
    ...(tabFactory.provenance?.pass ? [] : ["source_provenance_by_tab"]),
    ...(uniqueness.pass ? [] : ["image_uniqueness"]),
    ...(roleMatch.pass ? [] : ["image_role_match"]),
    ...(golden.pass ? [] : ["golden_content_quality"]),
  ]);

  const apiSlug = nz(brand.slug);
  const slugMappingIssue =
    apiSlug &&
    apiSlug !== identity.slug &&
    !/^[a-z0-9-]+$/.test(apiSlug) &&
    identity.slug !== apiSlug.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")
      ? `API slug "${apiSlug}" vs canonical "${identity.slug}" (lookup ${lookupIdUsed})`
      : null;

  const row = {
    brandName: brand.name || identity.name || identity.slug,
    slug: identity.slug,
    recordId: brand.id || identity.recordId,
    fetchOk: true,
    lookupIdUsed,
    sources: identity.sources,
    currentOsState: brand.brandExplorerDisplayState || null,
    currentPublicDisplay: brand.shouldRenderFullProfile
      ? "full_tabs_public"
      : externalQl.profileInPreparationRendered
        ? "profile_in_preparation"
        : "locked_or_hidden",
    founderPreviewDisplay: founderQl.actualRenderedFullProfile
      ? "full_tabs_internal_preview"
      : "limited_or_preparation",
    publicFullProfile: brand.shouldRenderFullProfile === true,
    founderVisualReviewPass: brand.founderVisualReviewPass === true,
    readyForActiveProfile:
      brand.readyForActiveProfile === true || brand.activeProfileApproved === true,
    companyValidated: brand.companyValidated === true,
    historicallyDone: hist.historicallyDone,
    historicalDoneEvidence: hist.evidence,
    legacyApprovalEvidence: LEGACY_SEED_SLUGS.includes(identity.slug) || brand.legacyHistoricalApproved === true,
    inPrimaryRelease: PRIMARY_RELEASE_SLUGS.includes(identity.slug),
    inVisibilityRestored: isVisibilityRestoredReleaseSlug(identity.slug),
    inLegacySeed: LEGACY_SEED_SLUGS.includes(identity.slug),
    presentationRowCount: blocks.length,
    ownerFacingRowCount: ownerFacing.length,
    galleryCount: countGallery(ownerFacing),
    propertyCount: countOpenings(ownerFacing),
    pvqlPass: pvql?.lockPass === true,
    pvqlInScope: pvql?.inPublicVisibilityLockScope === true,
    tabFactoryPass: tabFactory.auditPass === true,
    failFindings: tabFactory.failFindings,
    emptyRenderFailFindings: tabFactory.emptyRenderFailFindings,
    provenancePass: tabFactory.provenance?.pass === true,
    imageUniquenessPass: uniqueness.pass === true,
    galleryDistinctCount: uniqueness.galleryDistinctCount,
    scenarioDistinctCount: uniqueness.scenarioDistinctCount,
    propertyDistinctCount: uniqueness.propertyExampleDistinctCount,
    imageRoleMatchPass: roleMatch.pass === true,
    goldenPass: golden.pass === true,
    goldenFailures: golden.failures || [],
    gateFailures,
    whyNotPublicFull: brand.shouldRenderFullProfile
      ? null
      : [
          !hist.historicallyDone ? "weak_or_no_historical_done_evidence" : null,
          !brand.founderVisualReviewPass ? "founder_visual_review_not_pass" : null,
          !(brand.readyForActiveProfile || brand.activeProfileApproved)
            ? "active_profile_not_approved"
            : null,
          ownerFacing.length < 15 ? "thin_presentation" : null,
          uniqueness.galleryDistinctCount < 6 ? "gallery_distinct_lt_6" : null,
          !PRIMARY_RELEASE_SLUGS.includes(identity.slug) &&
          !isVisibilityRestoredReleaseSlug(identity.slug)
            ? "outside_primary_and_visibility_restored_cohorts"
            : null,
        ].filter(Boolean),
    slugMappingIssue,
    companyValidatedUntouched: true,
    noWrites: true,
  };

  const classification = classifyBrand(row);
  const plan = buildPlanEntry({ ...row, classification });

  return {
    ...row,
    classification,
    plan,
    fastestSafePath: plan.recommendedAction,
    blockerIsSubstantive: classification.blockerType === "substantive",
  };
}

export async function runActiveBrandCompletionReconciliation({ slugs = null } = {}) {
  const identities = discoverActiveBrandIdentities().filter(
    (id) => !slugs?.length || slugs.includes(id.slug)
  );
  const brands = [];
  for (const identity of identities) {
    console.log(`[recon] evaluating ${identity.slug}…`);
    brands.push(await evaluateActiveBrand(identity));
  }

  const byBucket = {};
  for (const b of CLASSIFICATION_BUCKETS) byBucket[b] = [];
  for (const b of brands) {
    const bucket = b.classification?.bucket || "true_incomplete";
    if (!byBucket[bucket]) byBucket[bucket] = [];
    byBucket[bucket].push(b.slug);
  }

  const byEffort = {};
  for (const b of brands) {
    const e = b.plan?.effort || "hold";
    if (!byEffort[e]) byEffort[e] = [];
    byEffort[e].push(b.slug);
  }

  const unclassified = brands.filter((b) => !CLASSIFICATION_BUCKETS.includes(b.classification?.bucket));

  return {
    version: RECONCILIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    readOnly: true,
    noAirtableWrites: true,
    companyValidatedUntouched: brands.every((b) => b.companyValidatedUntouched !== false),
    sourceLibraryUntouched: true,
    registryUntouched: true,
    releaseFieldsUntouched: true,
    inventorySize: brands.length,
    identitiesDiscovered: identities.length,
    summary: {
      brandCount: brands.length,
      publicFullClean: byBucket.public_full_clean.length,
      readyToRestore: byBucket.ready_to_restore_public_full.length,
      minorFix: byBucket.minor_gate_fix_needed.length,
      imageRemediation: byBucket.image_remediation_needed.length,
      contentRemediation: byBucket.content_remediation_needed.length,
      trueIncomplete: byBucket.true_incomplete.length,
      slugMapping: byBucket.duplicate_or_slug_mapping_issue.length,
      unclassifiedCount: unclassified.length,
      historicallyDoneCount: brands.filter((b) => b.historicallyDone).length,
      currentlyPublicFullCount: brands.filter((b) => b.publicFullProfile).length,
    },
    byBucket,
    byEffort,
    brands,
    acceptance: {
      allActiveListed: brands.length >= 20,
      everyBrandHasOsOrDisplayStatus: brands.every(
        (b) => b.currentOsState != null || b.fetchOk === false
      ),
      everyBrandClassified: unclassified.length === 0,
      historicallyDoneNotSilentlyIncomplete: brands
        .filter((b) => b.historicallyDone)
        .every((b) => b.classification.bucket !== "true_incomplete" || (b.ownerFacingRowCount || 0) < 15),
      companyValidatedUntouched: true,
      noContentRewritten: true,
    },
  };
}

function mdEscape(s) {
  return String(s ?? "").replace(/\|/g, "\\|");
}

export function writeActiveBrandCompletionReconciliationReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const main = [
    `# Active Brand Completion Reconciliation`,
    ``,
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    `Read-only: **true** · Airtable writes: **none** · Company Validated untouched: **${report.companyValidatedUntouched}**`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Inventory | ${report.summary.brandCount} |`,
    `| Currently public-full | ${report.summary.currentlyPublicFullCount} |`,
    `| Historically done evidence | ${report.summary.historicallyDoneCount} |`,
    `| A · public_full_clean | ${report.summary.publicFullClean} |`,
    `| B · ready_to_restore_public_full | ${report.summary.readyToRestore} |`,
    `| C · minor_gate_fix_needed | ${report.summary.minorFix} |`,
    `| D · image_remediation_needed | ${report.summary.imageRemediation} |`,
    `| E · content_remediation_needed | ${report.summary.contentRemediation} |`,
    `| F · true_incomplete | ${report.summary.trueIncomplete} |`,
    `| G · duplicate_or_slug_mapping_issue | ${report.summary.slugMapping} |`,
    ``,
    `## Classification buckets`,
    ``,
  ];
  for (const bucket of CLASSIFICATION_BUCKETS) {
    main.push(`### ${bucket}`);
    main.push(`Slugs: ${(report.byBucket[bucket] || []).join(", ") || "—"}`);
    main.push(``);
  }

  main.push(`## Full inventory`);
  main.push(``);
  main.push(
    `| Brand | Slug | Record ID | OS/Display State | Public Display | Hist. Done | PVQL | Tab Factory | Fail Findings | Bucket | Why not public-full |`
  );
  main.push(`| --- | --- | --- | --- | --- | --- | --- | --- | ---: | --- | --- |`);
  for (const b of report.brands) {
    main.push(
      `| ${mdEscape(b.brandName)} | \`${b.slug}\` | \`${b.recordId || "—"}\` | ${mdEscape(b.currentOsState || "—")} | ${mdEscape(b.currentPublicDisplay || "—")} | ${b.historicallyDone} | ${b.pvqlPass === true ? "pass" : b.pvqlPass === false ? "fail" : "—"} | ${b.tabFactoryPass === true ? "pass" : b.tabFactoryPass === false ? "fail" : "—"} | ${b.failFindings ?? "—"} | ${b.classification?.bucket} | ${mdEscape((b.whyNotPublicFull || []).join("; ") || "—")} |`
    );
  }

  main.push(``);
  main.push(`## Acceptance`);
  for (const [k, v] of Object.entries(report.acceptance || {})) {
    main.push(`- ${k}: **${v}**`);
  }

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, main.join("\n"));

  // Inventory-only report
  const inv = [
    `# Active Brand Inventory`,
    ``,
    `Discovered **${report.summary.brandCount}** active / historically-active Brand Explorer brands.`,
    ``,
    `| Brand | Slug | Record ID | Sources | Hist. Evidence | Presentation Rows | Gallery | Properties | Public Full |`,
    `| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |`,
    ...report.brands.map(
      (b) =>
        `| ${mdEscape(b.brandName)} | \`${b.slug}\` | \`${b.recordId || "—"}\` | ${(b.sources || []).join(", ")} | ${mdEscape((b.historicalDoneEvidence || []).slice(0, 3).join("; "))} | ${b.presentationRowCount} | ${b.galleryCount} | ${b.propertyCount} | ${b.publicFullProfile} |`
    ),
  ];
  const invPath = path.join(reportsDir, REPORT_INVENTORY_MD);
  fs.writeFileSync(invPath, inv.join("\n"));

  // Restore candidates
  const restore = report.brands.filter((b) =>
    ["public_full_clean", "ready_to_restore_public_full", "minor_gate_fix_needed"].includes(
      b.classification?.bucket
    )
  );
  const restoreMd = [
    `# Active Brand Restore Candidates`,
    ``,
    `Brands that are already clean or closest to public-full restore.`,
    ``,
    `| Brand | Slug | Bucket | Fastest safe path | Risk | Founder review? | Gate failures |`,
    `| --- | --- | --- | --- | --- | --- | --- |`,
    ...restore.map(
      (b) =>
        `| ${mdEscape(b.brandName)} | \`${b.slug}\` | ${b.classification.bucket} | ${mdEscape(b.fastestSafePath)} | ${b.plan?.riskLevel} | ${b.plan?.founderVisualReviewNeededBeforeRestore} | ${mdEscape((b.gateFailures || []).join(", ") || "—")} |`
    ),
    ``,
    `## Historically done but not public-full`,
    ``,
    ...report.brands
      .filter((b) => b.historicallyDone && !b.publicFullProfile)
      .map(
        (b) =>
          `### ${b.brandName} (\`${b.slug}\`)\n` +
          `- Why historically done: ${(b.historicalDoneEvidence || []).join("; ") || "—"}\n` +
          `- Why not public-full: ${(b.whyNotPublicFull || []).join("; ") || "—"}\n` +
          `- Gate blockers: ${(b.gateFailures || []).join(", ") || "—"}\n` +
          `- Blocker type: ${b.classification?.blockerType}\n` +
          `- Bucket: ${b.classification?.bucket}\n` +
          `- Fastest safe path: ${b.fastestSafePath}\n` +
          `- Command: \`${b.plan?.expectedCommand}\`\n`
      ),
  ];
  const restorePath = path.join(reportsDir, REPORT_RESTORE_MD);
  fs.writeFileSync(restorePath, restoreMd.join("\n"));

  // Remediation plan by effort
  const planMd = [
    `# Active Brand Remediation Plan`,
    ``,
    `Grouped by effort. No writes in this audit.`,
    ``,
  ];
  const effortOrder = [
    "none",
    "immediate_restore",
    "quick_fix",
    "image_only_remediation",
    "content_light_remediation",
    "full_rebuild",
    "hold",
  ];
  for (const effort of effortOrder) {
    const slugs = report.byEffort[effort] || [];
    planMd.push(`## ${effort} (${slugs.length})`);
    if (!slugs.length) {
      planMd.push(`—`);
      planMd.push(``);
      continue;
    }
    for (const slug of slugs) {
      const b = report.brands.find((x) => x.slug === slug);
      planMd.push(`### ${b.brandName} (\`${slug}\`)`);
      planMd.push(`- Bucket: ${b.classification.bucket}`);
      planMd.push(`- Action: ${b.plan.recommendedAction}`);
      planMd.push(`- Command: \`${b.plan.expectedCommand}\``);
      planMd.push(`- Risk: ${b.plan.riskLevel}`);
      planMd.push(
        `- Fields likely affected: ${(b.plan.fieldsLikelyAffected || []).join(", ") || "—"}`
      );
      planMd.push(
        `- Founder visual review before restore: ${b.plan.founderVisualReviewNeededBeforeRestore}`
      );
      planMd.push(``);
    }
  }
  const planPath = path.join(reportsDir, REPORT_PLAN_MD);
  fs.writeFileSync(planPath, planMd.join("\n"));

  // Durable doc
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(docsDir, { recursive: true });
  const docPath = path.join(docsDir, "brand-explorer-active-brand-completion-reconciliation.md");
  fs.writeFileSync(
    docPath,
    [
      `# Active Brand Completion Reconciliation`,
      ``,
      `Read-only audit reconciling all historically active Brand Explorer brands against PVQL / Tab Factory.`,
      ``,
      `## Run`,
      ``,
      "```bash",
      "npm run brand-explorer-active-brand-completion-reconciliation -- --dry-run",
      "```",
      ``,
      `## Outputs`,
      ``,
      `- \`reports/${REPORT_JSON}\``,
      `- \`reports/${REPORT_MD}\``,
      `- \`reports/${REPORT_INVENTORY_MD}\``,
      `- \`reports/${REPORT_RESTORE_MD}\``,
      `- \`reports/${REPORT_PLAN_MD}\``,
      ``,
      `## Rules`,
      ``,
      `- No Airtable writes`,
      `- No Company Validated / Source Library / Registry / release field changes`,
      `- No content rewrites during audit`,
      `- Historically done brands must not be silently treated as incomplete`,
      ``,
      `Latest run: see reports (generated ${report.generatedAt}).`,
      ``,
      `Inventory size: **${report.summary.brandCount}** · public-full clean: **${report.summary.publicFullClean}** · ready to restore: **${report.summary.readyToRestore}**`,
    ].join("\n")
  );

  return { jsonPath, mdPath, invPath, restorePath, planPath, docPath };
}
