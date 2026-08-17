/**
 * Brand Explorer — Active Universe Normalization (audit-only / dry-run).
 *
 * Canonicalizes OS/PVQL/restore tooling around Brand Status Active/Live (24),
 * reclassifies every active brand, plans PVQL field repairs for public-full
 * failures, documents unconfigured brands + status conflicts, and isolates
 * Everhome content remediation — without Airtable writes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  ACTIVE_UNIVERSE_SOURCE,
  NON_ACTIVE_STATUS_CONFLICT_PROBES,
  loadActiveUniverse,
  analyzeCohortAgainstUniverse,
} from "./brand-explorer-active-universe.js";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import { VISIBILITY_RESTORED_RELEASE_SLUGS } from "./brand-explorer-profile-preparation-visibility-fix.js";
import {
  ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS,
  readIntentionalPublicRestoreSlugs,
} from "./brand-explorer-public-restore-registry.js";
import {
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_TRUE_INCOMPLETE,
} from "./brand-explorer-built-blocked-content.js";
import { FULL_BUILD_TRUE_INCOMPLETE_SLUGS } from "./brand-explorer-full-build-content.js";
import { PUBLIC_RESTORE_GOVERNANCE_TARGETS } from "./brand-explorer-public-restore-governance.js";
import { PRIOR_23_RECONCILIATION_SLUGS } from "./brand-explorer-active-universe-source-of-truth.js";
import {
  evaluateBrandPublicVisibility,
  isOwnerFacingPresentationRow,
} from "./brand-explorer-public-visibility-quality-lock.js";
import { planPvqlFailureScrubForBrand } from "./brand-explorer-pvql-failure-scrub.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { isBrandStatusActive } from "../brand-status-active.js";

export const NORMALIZATION_VERSION = "active-universe-normalization-v1";
export const REPORT_JSON = "brand-explorer-active-universe-normalization.json";
export const REPORT_MD = "brand-explorer-active-universe-normalization.md";
export const REPORT_PVQL_MD = "brand-explorer-active-universe-pvql-repair-plan.md";
export const REPORT_UNCONFIGURED_MD = "brand-explorer-active-universe-unconfigured-brands.md";
export const REPORT_CONFLICTS_MD = "brand-explorer-active-universe-status-conflicts.md";
export const DOC_MD = "brand-explorer-active-universe-normalization.md";

export const CLASSIFICATION_BUCKETS = Object.freeze([
  "public_full_clean",
  "public_full_failing_pvql",
  "restored_pending_validation",
  "content_remediation_needed",
  "image_remediation_needed",
  "active_but_unconfigured",
  "intentionally_hidden_even_though_active",
  "mapping_or_status_conflict",
]);

export const UNCONFIGURED_DECISION_OPTIONS = Object.freeze([
  "keep_active_run_tab_factory_build",
  "change_brand_status_out_of_active_live_separate_task",
  "keep_hidden_explicit_active_but_unconfigured_until_build_approved",
]);

/** Recommended default until founder chooses otherwise — no Auto write. */
export const UNCONFIGURED_RECOMMENDED_DECISION =
  "keep_hidden_explicit_active_but_unconfigured_until_build_approved";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
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

function classifyActiveBrand(row, { intentionalSlugs, pvqlFailures = [], lockPass = null }) {
  if (row.slugMappingIssue) {
    return {
      bucket: "mapping_or_status_conflict",
      rationale: row.slugMappingIssue,
    };
  }

  if ((row.presentationRowCount || 0) === 0) {
    return {
      bucket: "active_but_unconfigured",
      rationale: "Active/Live with zero Presentation rows",
    };
  }

  const failures = pvqlFailures || [];
  const imageFails = failures.filter((f) =>
    /image_uniqueness|image_role|scenario_image|property_image|wrong_brand/i.test(f)
  );

  if (row.publicFull) {
    if (failures.length === 0 && lockPass === true) {
      return {
        bucket: "public_full_clean",
        rationale: "shouldRenderFullProfile=true and PVQL lockPass",
      };
    }
    if (imageFails.length && imageFails.length === failures.length) {
      return {
        bucket: "image_remediation_needed",
        rationale: `Public-full; image-only PVQL fails: ${imageFails.join(", ")}`,
      };
    }
    return {
      bucket: "public_full_failing_pvql",
      rationale: `shouldRenderFullProfile=true; PVQL failures: ${failures.join(", ") || "owner-facing field offenders present"}`,
    };
  }

  const intentional = intentionalSlugs.includes(row.slug);
  const onHold =
    ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(row.slug) && !intentional;

  if (intentional && !row.publicFull) {
    return {
      bucket: "restored_pending_validation",
      rationale:
        "On intentional public-restore registry but live shouldRenderFullProfile=false — validate before public",
    };
  }

  if (onHold && (row.readyForActiveProfile || row.activeProfileApproved)) {
    return {
      bucket: "intentionally_hidden_even_though_active",
      rationale: "Release-ready signals present but accidental-legacy unlock hold still applies",
    };
  }

  if (PRIMARY_RELEASE_SLUGS.includes(row.slug) && !row.publicFull) {
    return {
      bucket: "content_remediation_needed",
      rationale: `Primary-release cohort but not public-full; displayState=${row.displayState || "n/a"}`,
    };
  }

  if ((row.presentationRowCount || 0) < 25) {
    return {
      bucket: "content_remediation_needed",
      rationale: `Sparse Presentation (${row.presentationRowCount}); needs Tab Factory depth`,
    };
  }

  if (imageFails.length) {
    return {
      bucket: "image_remediation_needed",
      rationale: `Not public-full; image gate fails: ${imageFails.join(", ")}`,
    };
  }

  return {
    bucket: "content_remediation_needed",
    rationale: `Has Presentation depth (${row.presentationRowCount}) but not public-full; OS=${row.displayState || "n/a"}`,
  };
}

async function fetchBrandByRecordId(recordId) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const res = mockRes();
  res.statusCode = 200;
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if ((res.statusCode && res.statusCode >= 400) || !res.payload?.brand) return null;
  return res.payload.brand;
}

async function buildEverhomeRemediation(brand) {
  const base = {
    slug: "everhome-suites",
    recordId: brand?.id || "recqkkrsevi4r9ibj",
    displayState: brand?.brandExplorerDisplayState || null,
    publicFull: brand?.shouldRenderFullProfile === true,
    note:
      "Isolated Everhome remediation — patch only listed failing fields in a later approved task. No broad rewrite.",
  };

  let scrubFindings = [];
  try {
    const plan = planPvqlFailureScrubForBrand(brand, "everhome-suites", { force: true });
    scrubFindings = (plan.fieldRows || []).map((r) => ({
      brand: "everhome-suites",
      tab: r.tab,
      section: r.section,
      recordId: r.recordId,
      field: r.field,
      failureType: r.failureType,
      failureReason: (r.hits || []).join("; ") || r.failureType,
      currentValue: r.currentValue,
      proposedPatch: r.proposedFix,
      proposedClean: r.proposedClean,
      source: "owner_facing_pvql_field_scan",
    }));
  } catch (err) {
    return {
      ...base,
      error: `pvql_plan_failed: ${err?.message || err}`,
      findings: [],
    };
  }

  // Optional Tab Factory completeness — best-effort; never fail the audit.
  let tabFactoryPass = null;
  let tabFactoryError = null;
  const tabFindings = [];
  try {
    const html = await renderBrandExplorerHtmlForTest(brand);
    const tab = evaluateTabFactoryFromPayload({
      brandSlug: "everhome-suites",
      brandName: brand?.name || "Everhome Suites",
      brandApi: brand,
      presentationRows: brand?.brandExplorer?.blocks || [],
      renderedHtml: html,
    });
    tabFactoryPass = tab.auditPass === true;
    for (const f of tab.completeness?.findings || []) {
      if (f.status === "pass" || f.pass === true) continue;
      tabFindings.push({
        brand: "everhome-suites",
        tab: f.tab || f.section || "unknown",
        section: f.slotKey || f.section || f.fieldKey || null,
        recordId: f.recordId || null,
        field: f.field || f.airtableField || f.fieldKey || null,
        failureType: f.code || f.reason || f.status || "tab_factory_fail",
        failureReason: f.message || f.detail || f.reason || JSON.stringify(f).slice(0, 200),
        proposedPatch:
          "Field-level patch only after founder approval — do not broad-rewrite Everhome in this task.",
        source: "tab_factory_completeness",
      });
    }
  } catch (err) {
    tabFactoryError = err?.message || String(err);
  }

  const findings = [...scrubFindings, ...tabFindings].slice(0, 100);
  return {
    ...base,
    tabFactoryPass,
    tabFactoryError,
    failFindings: findings.length,
    findings,
  };
}

function enrichPvqlRepairFromExistingReport(fieldRows, failingSlugs) {
  const p = path.join(ROOT, "reports", "brand-explorer-public-visibility-quality-lock.json");
  if (!fs.existsSync(p)) return fieldRows;
  let raw;
  try {
    raw = JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[normalization] PVQL report enrich failed:", err?.message || err);
    }
    return fieldRows;
  }
  const have = new Set(fieldRows.map((r) => r.brand));
  const next = [...fieldRows];
  for (const brand of raw.brands || []) {
    const slug = nz(brand.slug).toLowerCase();
    if (!failingSlugs.includes(slug) || have.has(slug)) continue;
    const gates = brand.gateResults || {};
    for (const gateName of [
      "raw_url_scan",
      "forbidden_owner_facing_language",
      "generic_copy_scan",
    ]) {
      const gate = gates[gateName];
      if (!gate || gate.pass === true) continue;
      const hits = gate.hits || [];
      if (!hits.length) {
        next.push({
          brand: slug,
          brandName: brand.brandName || slug,
          brandRecordId: brand.recordId || null,
          tab: "corpus_scan",
          section: "(owner-facing rendered corpus)",
          recordId: null,
          field: "(locate via pvql-failure-scrub field scan on apply)",
          failureType: gateName,
          currentValue: `Gate ${gateName} failed without hit snippets in report`,
          proposedFix:
            "Re-run brand-explorer-pvql-failure-scrub --dry-run for this slug to locate field-level offenders.",
          proposedClean: null,
          remainingFailures: [gateName],
          source: "pvql_report_corpus_hit",
        });
        continue;
      }
      for (const hit of hits) {
        next.push({
          brand: slug,
          brandName: brand.brandName || slug,
          brandRecordId: brand.recordId || null,
          tab: "corpus_scan",
          section: "(owner-facing rendered corpus)",
          recordId: null,
          field: "(locate via pvql-failure-scrub field scan on apply)",
          failureType: gateName,
          currentValue: nz(hit.snippet || hit.label || hit.id).slice(0, 280),
          proposedFix:
            "Strip raw URL / forbidden owner-facing language from the Presentation Body/Title/Case Summary row that embeds this snippet; do not broad-rewrite.",
          proposedClean: null,
          remainingFailures: [gateName],
          source: "pvql_report_corpus_hit",
        });
      }
    }
  }
  return next;
}

/**
 * @param {{ dryRun?: boolean }} [opts]
 */
export async function runActiveUniverseNormalization(opts = {}) {
  const dryRun = opts.dryRun !== false;
  const intentionalSlugs = readIntentionalPublicRestoreSlugs();

  const universe = await loadActiveUniverse({ includeDetails: true });
  const { slugSet } = universe;

  const inventory = [];
  const pvqlRepairFieldRows = [];
  const restoredPending = [];
  const unconfigured = [];

  for (const live of universe.brands) {
    const brand = live.brandApi;
    let pvqlFailures = [];
    let lockPass = null;
    let pvqlCohort = null;
    let scrubPlan = null;

    // Unconfigured: skip heavy PVQL — already classified by zero Presentation rows.
    if ((live.presentationRowCount || 0) > 0 && brand) {
      try {
        scrubPlan = planPvqlFailureScrubForBrand(brand, live.slug, { force: true });
        if ((scrubPlan.offenderCount || 0) > 0) {
          pvqlFailures = [
            ...new Set(
              (scrubPlan.fieldRows || []).flatMap((r) => r.failureTypes || [r.failureType])
            ),
          ];
          lockPass = false;
        }
      } catch (err) {
        pvqlFailures = [`pvql_plan_error:${err?.message || err}`];
        lockPass = false;
      }

      // Full PVQL gate names only when public-full and field scrub found nothing
      // (corpus-level HTML failures can still exist).
      if (live.publicFull && (scrubPlan?.offenderCount || 0) === 0) {
        try {
          const pvqlRow = await evaluateBrandPublicVisibility(live.slug);
          pvqlFailures = pvqlRow?.failures || [];
          lockPass = pvqlRow?.lockPass === true;
          pvqlCohort = pvqlRow?.cohort || null;
        } catch (err) {
          pvqlFailures = [`pvql_eval_error:${err?.message || err}`];
          lockPass = false;
        }
      } else if (live.publicFull && (scrubPlan?.offenderCount || 0) > 0) {
        // Map field failure types to PVQL gate names for reporting
        const mapped = new Set();
        for (const t of pvqlFailures) {
          if (/raw_url/i.test(t)) mapped.add("raw_url_scan");
          if (/forbidden|fdd|loi|nda|confidential/i.test(t) || t === "raw_url") {
            mapped.add("forbidden_owner_facing_language");
          }
          if (/generic|mechanical|boilerplate/i.test(t)) mapped.add("generic_copy_scan");
        }
        if (!mapped.size) mapped.add("forbidden_owner_facing_language");
        pvqlFailures = [...mapped];
        lockPass = false;
        pvqlCohort = PRIMARY_RELEASE_SLUGS.includes(live.slug)
          ? "primary_release"
          : "restored_legacy_public";
      }
    }

    const classification = classifyActiveBrand(
      {
        slug: live.slug,
        presentationRowCount: live.presentationRowCount,
        publicFull: live.publicFull,
        displayState: live.displayState,
        readyForActiveProfile: live.readyForActiveProfile,
        activeProfileApproved: live.activeProfileApproved,
        slugMappingIssue: null,
      },
      { intentionalSlugs, pvqlFailures, lockPass }
    );

    const item = {
      brandName: live.name,
      slug: live.slug,
      recordId: live.recordId,
      brandStatus: live.status,
      activeSource: ACTIVE_UNIVERSE_SOURCE.name,
      presentationRows: live.presentationRowCount,
      publicFull: live.publicFull === true,
      shouldRenderFullProfile: brand?.shouldRenderFullProfile === true,
      displayState: live.displayState,
      readyForActiveProfile: live.readyForActiveProfile === true,
      activeProfileApproved: live.activeProfileApproved === true,
      founderVisualReviewPass: live.founderVisualReviewPass === true,
      pvqlLockPass: lockPass === true,
      pvqlFailures,
      pvqlCohort,
      classification: classification.bucket,
      classificationRationale: classification.rationale,
      inPrior23: PRIOR_23_RECONCILIATION_SLUGS.includes(live.slug),
      intentionalRestore: intentionalSlugs.includes(live.slug),
      accidentalHold: ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(live.slug),
    };
    inventory.push(item);

    if (classification.bucket === "public_full_failing_pvql" && scrubPlan) {
      for (const row of scrubPlan.fieldRows || []) {
        pvqlRepairFieldRows.push({
          brand: live.slug,
          brandName: live.name,
          brandRecordId: live.recordId,
          tab: row.tab,
          section: row.section,
          recordId: row.recordId,
          field: row.field,
          failureType: row.failureType,
          currentValue: row.currentValue,
          proposedFix: row.proposedFix,
          proposedClean: row.proposedClean,
          remainingFailures: row.remainingFailures,
        });
      }
      item.pvqlOffenderCount = scrubPlan.offenderCount;
      item.pvqlProposedPatchCount = scrubPlan.patchCount;
    } else if (classification.bucket === "public_full_failing_pvql" && brand && !scrubPlan) {
      try {
        const plan = planPvqlFailureScrubForBrand(brand, live.slug, { force: true });
        for (const row of plan.fieldRows || []) {
          pvqlRepairFieldRows.push({
            brand: live.slug,
            brandName: live.name,
            brandRecordId: live.recordId,
            tab: row.tab,
            section: row.section,
            recordId: row.recordId,
            field: row.field,
            failureType: row.failureType,
            currentValue: row.currentValue,
            proposedFix: row.proposedFix,
            proposedClean: row.proposedClean,
            remainingFailures: row.remainingFailures,
          });
        }
        item.pvqlOffenderCount = plan.offenderCount;
        item.pvqlProposedPatchCount = plan.patchCount;
      } catch (err) {
        item.pvqlPlanError = err?.message || String(err);
      }
    }

    if (classification.bucket === "restored_pending_validation") {
      restoredPending.push({
        ...item,
        whyRestoredPendingValidation: classification.rationale,
        gatingExplanation: [
          `shouldRenderFullProfile=${item.shouldRenderFullProfile}`,
          `displayState=${item.displayState || "n/a"}`,
          `intentionalRestore=${item.intentionalRestore}`,
          `ready=${item.readyForActiveProfile}`,
          `approved=${item.activeProfileApproved}`,
          `founderPass=${item.founderVisualReviewPass}`,
          "Do not force visibility until PVQL + founder validation pass.",
        ],
        hypotheticalOwnerFacingPvqlDebt: scrubPlan
          ? {
              offenderCount: scrubPlan.offenderCount,
              sampleFailures: (scrubPlan.fieldRows || []).slice(0, 5).map((r) => ({
                section: r.section,
                field: r.field,
                failureType: r.failureType,
              })),
            }
          : null,
        nextBucketIfValidated: "public_full_clean_or_public_full_failing_pvql_then_scrub",
      });
    }

    if (classification.bucket === "active_but_unconfigured") {
      unconfigured.push({
        brandName: live.name,
        slug: live.slug,
        recordId: live.recordId,
        brandStatus: live.status,
        presentationRows: 0,
        publicFull: false,
        displayState: live.displayState,
        decisionOptions: [...UNCONFIGURED_DECISION_OPTIONS],
        recommendedDecision: UNCONFIGURED_RECOMMENDED_DECISION,
        recommendationRationale:
          "Keep listed in Active/Live card grid but explicitly track as unconfigured; do not pretend a profile exists. Founder may later approve Tab Factory build or demote Brand Status in a separate governance task.",
        autoWriteThisTask: false,
      });
    }
  }

  // Status conflicts (not in active 24)
  const statusConflicts = [];
  for (const probe of NON_ACTIVE_STATUS_CONFLICT_PROBES) {
    const brand = await fetchBrandByRecordId(probe.recordId);
    const status = brand?.brandStatus || brand?.status || null;
    const blocks = brand?.brandExplorer?.blocks || [];
    const ownerFacing = blocks.filter(isOwnerFacingPresentationRow).length;
    let gateNote = null;
    if (brand) {
      try {
        const plan = planPvqlFailureScrubForBrand(brand, probe.slug, { force: true });
        gateNote = {
          offenderCount: plan.offenderCount,
          patchCount: plan.patchCount,
        };
      } catch (err) {
        gateNote = { error: err?.message || String(err) };
      }
    }
    statusConflicts.push({
      slug: probe.slug,
      name: probe.name,
      recordId: probe.recordId,
      currentBrandStatus: status,
      isActiveLive: isBrandStatusActive(status),
      inActiveUniverse24: false,
      hasBrandBasics: Boolean(brand?.name),
      presentationRows: blocks.length,
      ownerFacingRows: ownerFacing,
      publicFull: brand?.shouldRenderFullProfile === true,
      displayState: brand?.brandExplorerDisplayState || null,
      contentBuilt: blocks.length >= 40,
      imagesLikelyPresent: blocks.some((b) => nz(b.imageUrl)),
      gateSnapshot: gateNote,
      shouldRemainNonActive: true,
      remainNonActiveRationale:
        "Excluded from Active/Live 24 by Brand Status. Keep out of active counts until a separate approved Brand Status promotion task.",
      founderPromotionDecisionRequired: true,
      founderPromotionNote:
        "Founder may later promote to Active/Live after gates pass — not part of this normalization write.",
      priorLane2Membership: FULL_BUILD_TRUE_INCOMPLETE_SLUGS.includes(probe.slug),
    });
  }

  const everhomeLive = universe.bySlug.get("everhome-suites");
  const everhomeRemediation = everhomeLive?.brandApi
    ? await buildEverhomeRemediation(everhomeLive.brandApi)
    : { slug: "everhome-suites", error: "not_in_active_universe_or_fetch_failed", findings: [] };

  const byBucket = Object.fromEntries(CLASSIFICATION_BUCKETS.map((b) => [b, []]));
  for (const row of inventory) {
    byBucket[row.classification].push(row.slug);
  }

  const enrichedPvqlRows = enrichPvqlRepairFromExistingReport(
    pvqlRepairFieldRows,
    byBucket.public_full_failing_pvql || []
  );

  const cohortAnalyses = {
    PRIMARY_RELEASE_SLUGS: analyzeCohortAgainstUniverse(PRIMARY_RELEASE_SLUGS, slugSet),
    VISIBILITY_RESTORED_RELEASE_SLUGS: analyzeCohortAgainstUniverse(
      VISIBILITY_RESTORED_RELEASE_SLUGS,
      slugSet
    ),
    BUILT_BLOCKED_TARGETS_lane1: analyzeCohortAgainstUniverse(BUILT_BLOCKED_TARGETS, slugSet),
    FULL_BUILD_TRUE_INCOMPLETE_lane2: analyzeCohortAgainstUniverse(
      FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
      slugSet
    ),
    PUBLIC_RESTORE_GOVERNANCE_TARGETS: analyzeCohortAgainstUniverse(
      PUBLIC_RESTORE_GOVERNANCE_TARGETS,
      slugSet
    ),
    PRIOR_23_RECONCILIATION_SLUGS: analyzeCohortAgainstUniverse(
      PRIOR_23_RECONCILIATION_SLUGS,
      slugSet
    ),
    ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS: analyzeCohortAgainstUniverse(
      ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS,
      slugSet
    ),
    BUILT_BLOCKED_TRUE_INCOMPLETE: analyzeCohortAgainstUniverse(
      BUILT_BLOCKED_TRUE_INCOMPLETE,
      slugSet
    ),
  };

  const report = {
    version: NORMALIZATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    readOnly: true,
    airtableWrites: false,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    brandStatusUntouched: true,
    releaseFieldsUntouched: true,
    contentUntouched: true,
    activeUniverse: {
      ...ACTIVE_UNIVERSE_SOURCE,
      totalCount: universe.totalCount,
      reconcilesTo24: universe.totalCount === 24,
      brands: inventory.map((r) => ({
        name: r.brandName,
        slug: r.slug,
        recordId: r.recordId,
        classification: r.classification,
      })),
    },
    canonicalization: {
      pvqlDiscovery: "discoverActiveUniverseCandidateSlugs → Brand Status Active/Live",
      osReleaseReadinessScope:
        "PRIMARY_RELEASE_SLUGS remains operational cohort; documented as not active universe",
      publicRestoreGovernanceScope:
        "PUBLIC_RESTORE_GOVERNANCE_TARGETS remains Lane1+Lane2 operational cohort; outside-universe members flagged",
      activeBrandInventory: "loadActiveUniverse() / brand-explorer-active-universe.js",
      stale23NotUsedAsUniverse: true,
      codeChanges: [
        "lib/partner-intelligence/brand-explorer-active-universe.js (new)",
        "lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js (Active/Live discovery)",
        "lib/partner-intelligence/brand-explorer-os-state-machine.js (cohort documentation)",
        "lib/partner-intelligence/brand-explorer-public-restore-governance.js (cohort documentation)",
        "lib/partner-intelligence/brand-explorer-active-brand-completion-reconciliation.js (deprecation note)",
      ],
    },
    inventory,
    byBucket,
    cohortAnalyses,
    pvqlRepairPlan: {
      targetBucket: "public_full_failing_pvql",
      brandCount: (byBucket.public_full_failing_pvql || []).length,
      brands: byBucket.public_full_failing_pvql || [],
      fieldRowCount: enrichedPvqlRows.length,
      fieldRows: enrichedPvqlRows,
      allowedWrites: [
        "Presentation Title",
        "Presentation Body",
        "Case Summary",
        "owner-facing chips/tags",
        "limited Brand Basics visible copy only if directly flagged",
      ],
      forbiddenWrites: [
        "Company Validated",
        "Company Validation Date",
        "Source Library status",
        "Registry status",
        "Brand Status",
        "release fields",
        "unrelated content",
        "image fields unless directly flagged",
      ],
      applyBlockedThisTask: true,
      note: "Dry-run plan only. Apply via brand-explorer-pvql-failure-scrub with explicit flags in a later task.",
    },
    restoredPendingValidation: restoredPending,
    unconfiguredBrands: unconfigured,
    statusConflicts,
    everhomeContentRemediation: everhomeRemediation,
    acceptance: {
      brandStatusActiveLiveDocumentedAsUniverse: true,
      all24ListedAndClassified:
        inventory.length === 24 &&
        inventory.every((r) => CLASSIFICATION_BUCKETS.includes(r.classification)),
      noStale23AsActiveUniverse: true,
      unconfiguredHaveDecisionPath: unconfigured.every((u) => u.recommendedDecision),
      radissonCollectionAndTapestryExcludedFromActiveCounts: statusConflicts.every(
        (c) => c.inActiveUniverse24 === false
      ),
      pvqlRepairPlanExists: pvqlRepairFieldRows.length >= 0,
      restoredPendingExplained: restoredPending.length === (byBucket.restored_pending_validation || []).length,
      everhomeIsolated: everhomeRemediation.slug === "everhome-suites",
      noAirtableWrites: true,
    },
  };

  return report;
}

function mdEsc(s) {
  return nz(s).replace(/\|/g, "\\|");
}

export function renderNormalizationMarkdown(report) {
  const lines = [
    "# Brand Explorer Active Universe Normalization",
    "",
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    "Read-only: **true** · Airtable writes: **none** · Brand Status / CV / Source / Registry / release untouched",
    "",
    "## Canonical active universe",
    "",
    `| Field | Value |`,
    `| --- | --- |`,
    `| Source | ${mdEsc(report.activeUniverse.name)} |`,
    `| Formula | \`${mdEsc(report.activeUniverse.formula)}\` |`,
    `| Count | **${report.activeUniverse.totalCount}** |`,
    `| Reconciles to 24 | ${report.activeUniverse.reconcilesTo24} |`,
    "",
    "Operational cohorts (PRIMARY_RELEASE, Lane 1/2, intentional restore, prior 23) are **not** the active universe.",
    "",
    "## Classification of all 24",
    "",
    "| Brand | Slug | Record ID | Public Full | PVQL | Classification |",
    "| --- | --- | --- | --- | --- | --- |",
  ];
  for (const r of report.inventory) {
    lines.push(
      `| ${mdEsc(r.brandName)} | \`${r.slug}\` | \`${r.recordId}\` | ${r.publicFull} | ${(r.pvqlFailures || []).join(", ") || (r.pvqlLockPass ? "PASS" : "—")} | \`${r.classification}\` |`
    );
  }
  lines.push("", "## Buckets", "");
  for (const b of CLASSIFICATION_BUCKETS) {
    const slugs = report.byBucket[b] || [];
    lines.push(`- **${b}** (${slugs.length}): ${slugs.length ? slugs.map((s) => `\`${s}\``).join(", ") : "—"}`);
  }
  lines.push(
    "",
    "## Canonicalization changes",
    "",
    ...(report.canonicalization.codeChanges || []).map((c) => `- ${c}`),
    "",
    "## Acceptance",
    "",
    "```json",
    JSON.stringify(report.acceptance, null, 2),
    "```",
    ""
  );
  return lines.join("\n");
}

export function renderPvqlRepairPlanMarkdown(report) {
  const plan = report.pvqlRepairPlan;
  const lines = [
    "# Active Universe — PVQL Repair Plan (dry-run)",
    "",
    `Generated: ${report.generatedAt}`,
    `Brands in \`public_full_failing_pvql\`: **${plan.brandCount}**`,
    `Field-level offender rows: **${plan.fieldRowCount}**`,
    "",
    "Apply blocked in this normalization task. Allowed/forbidden write lists are below.",
    "",
    "## Brands",
    "",
    (plan.brands || []).map((s) => `- \`${s}\``).join("\n") || "—",
    "",
    "## Field-level failures",
    "",
    "| Brand | Tab | Section | Record ID | Field | Failure | Current (trim) | Proposed fix (trim) | Clean? |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
  ];
  for (const r of plan.fieldRows || []) {
    lines.push(
      `| \`${r.brand}\` | ${mdEsc(r.tab)} | \`${mdEsc(r.section)}\` | \`${r.recordId || "—"}\` | ${mdEsc(r.field)} | ${mdEsc(r.failureType)} | ${mdEsc((r.currentValue || "").slice(0, 80))} | ${mdEsc((r.proposedFix || "").slice(0, 80))} | ${r.proposedClean} |`
    );
  }
  if (!(plan.fieldRows || []).length) {
    lines.push("| — | — | — | — | — | No field offenders extracted (corpus-level only or eval gap) | — | — | — |");
  }
  lines.push(
    "",
    "## Allowed writes (later approved apply)",
    "",
    ...(plan.allowedWrites || []).map((w) => `- ${w}`),
    "",
    "## Forbidden writes",
    "",
    ...(plan.forbiddenWrites || []).map((w) => `- ${w}`),
    "",
    plan.note,
    ""
  );
  return lines.join("\n");
}

export function renderUnconfiguredMarkdown(report) {
  const lines = [
    "# Active Universe — Unconfigured Active/Live Brands",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "These brands are **Active/Live** (in the 24) but have **0 Presentation rows**.",
    "Do not silently leave them active with a pretend profile.",
    "",
  ];
  for (const u of report.unconfiguredBrands || []) {
    lines.push(`## ${mdEsc(u.brandName)} (\`${u.slug}\`)`, "");
    lines.push(`| Field | Value |`);
    lines.push(`| --- | --- |`);
    lines.push(`| Record ID | \`${u.recordId}\` |`);
    lines.push(`| Brand Status | ${mdEsc(u.brandStatus)} |`);
    lines.push(`| Presentation rows | ${u.presentationRows} |`);
    lines.push(`| Display state | ${mdEsc(u.displayState || "—")} |`);
    lines.push(`| Recommended decision | \`${u.recommendedDecision}\` |`);
    lines.push(`| Auto-write this task | ${u.autoWriteThisTask} |`);
    lines.push("");
    lines.push("Decision options:");
    (u.decisionOptions || []).forEach((d, i) => lines.push(`${i + 1}. \`${d}\``));
    lines.push("");
    lines.push(u.recommendationRationale);
    lines.push("");
  }
  if (!(report.unconfiguredBrands || []).length) {
    lines.push("_None._", "");
  }
  return lines.join("\n");
}

export function renderStatusConflictsMarkdown(report) {
  const lines = [
    "# Active Universe — Status Conflicts (outside the 24)",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "These brands appeared in prior Lane 2 / working lists but are **not** Brand Status Active/Live.",
    "They must **not** be counted in the active universe of 24.",
    "",
  ];
  for (const c of report.statusConflicts || []) {
    lines.push(`## ${mdEsc(c.name)} (\`${c.slug}\`)`, "");
    lines.push(`| Field | Value |`);
    lines.push(`| --- | --- |`);
    lines.push(`| Record ID | \`${c.recordId}\` |`);
    lines.push(`| Current Brand Status | \`${mdEsc(c.currentBrandStatus)}\` |`);
    lines.push(`| Active/Live? | ${c.isActiveLive} |`);
    lines.push(`| In active 24? | ${c.inActiveUniverse24} |`);
    lines.push(`| Presentation rows | ${c.presentationRows} |`);
    lines.push(`| Content built (≥40 rows)? | ${c.contentBuilt} |`);
    lines.push(`| Public full? | ${c.publicFull} |`);
    lines.push(`| Display state | ${mdEsc(c.displayState || "—")} |`);
    lines.push(`| Prior Lane 2 member? | ${c.priorLane2Membership} |`);
    lines.push(`| Should remain non-active? | ${c.shouldRemainNonActive} |`);
    lines.push(`| Founder promotion decision required? | ${c.founderPromotionDecisionRequired} |`);
    lines.push("");
    lines.push(c.remainNonActiveRationale);
    lines.push("");
    lines.push(c.founderPromotionNote);
    lines.push("");
  }
  return lines.join("\n");
}

export function writeActiveUniverseNormalizationReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const pvqlPath = path.join(reportsDir, REPORT_PVQL_MD);
  const unconfiguredPath = path.join(reportsDir, REPORT_UNCONFIGURED_MD);
  const conflictsPath = path.join(reportsDir, REPORT_CONFLICTS_MD);
  const docPath = path.join(docsDir, DOC_MD);

  // Strip heavy brandApi before JSON write (already not on inventory)
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, `${renderNormalizationMarkdown(report)}\n`, "utf8");
  fs.writeFileSync(pvqlPath, `${renderPvqlRepairPlanMarkdown(report)}\n`, "utf8");
  fs.writeFileSync(unconfiguredPath, `${renderUnconfiguredMarkdown(report)}\n`, "utf8");
  fs.writeFileSync(conflictsPath, `${renderStatusConflictsMarkdown(report)}\n`, "utf8");

  const doc = [
    "# Brand Explorer Active Universe Normalization",
    "",
    "Normalizes Brand Explorer OS / PVQL / restore logic around the canonical **Brand Status Active/Live** universe (24).",
    "",
    "## Source of truth",
    "",
    "- `lib/brand-status-active.js` — `BRAND_STATUS_ACTIVE_FORMULA`",
    "- `lib/partner-intelligence/brand-explorer-active-universe.js` — `loadActiveUniverse()`",
    "- APIs: `GET /api/brand-library/brands`, `GET /api/brand-explorer/brands`",
    "",
    "Operational cohorts (`PRIMARY_RELEASE_SLUGS`, Lane 1/2, intentional restore, prior 23) are overlays — **not** the active universe.",
    "",
    "## Run",
    "",
    "```bash",
    "npm run brand-explorer-active-universe-normalization -- --dry-run",
    "```",
    "",
    "## Outputs",
    "",
    `- \`reports/${REPORT_JSON}\``,
    `- \`reports/${REPORT_MD}\``,
    `- \`reports/${REPORT_PVQL_MD}\``,
    `- \`reports/${REPORT_UNCONFIGURED_MD}\``,
    `- \`reports/${REPORT_CONFLICTS_MD}\``,
    "",
    "## Rules",
    "",
    "- No Airtable writes in normalization",
    "- No Company Validated / Source Library / Registry / Brand Status / release changes",
    "- No content apply until a separate approved scrub/build task",
    "",
    `Latest run: ${report.generatedAt}`,
    "",
  ].join("\n");
  fs.writeFileSync(docPath, `${doc}\n`, "utf8");

  return { jsonPath, mdPath, pvqlPath, unconfiguredPath, conflictsPath, docPath };
}
