/**
 * v44 — Brand Explorer OS Release Baseline + Next Batch Router.
 *
 * Read-only. Freezes golden active_profile_ready brands as the release baseline,
 * fails on regression, and routes incomplete brands through OS without unlock.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  PRIMARY_RELEASE_SLUGS,
  INCOMPLETE_CONTROL_SLUGS,
  DEFAULT_OS_BRANDS,
} from "./brand-explorer-os-state-machine.js";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { scanInternalPreviewOwnerCopy } from "./brand-explorer-economics-chrome-remediation.js";

export const V44_VERSION = "v44";

export const V44_RELEASED_GOLDEN_SLUGS = Object.freeze([...PRIMARY_RELEASE_SLUGS]);
export const V44_INCOMPLETE_ROUTED_SLUGS = Object.freeze([...INCOMPLETE_CONTROL_SLUGS]);
export const V44_DEFAULT_BRANDS = Object.freeze([...DEFAULT_OS_BRANDS]);

/** Frozen minimums captured at first release baseline (must not regress). */
export const V44_FROZEN_RELEASE_EXPECTATIONS = Object.freeze({
  "everhome-suites": Object.freeze({
    displayState: "active_profile_ready",
    shouldRenderFullProfile: true,
    minGalleryImageUrls: 6,
    minPropertyImageUrls: 4,
    minExternalTabs: 10,
    companyValidated: false,
    releaseFieldsSet: true,
    expectedNextAction: "no_action",
  }),
  kimpton: Object.freeze({
    displayState: "active_profile_ready",
    shouldRenderFullProfile: true,
    minGalleryImageUrls: 6,
    minPropertyImageUrls: 5,
    minExternalTabs: 10,
    companyValidated: false,
    releaseFieldsSet: true,
    expectedNextAction: "no_action",
  }),
  "radisson-individuals-by-choice": Object.freeze({
    displayState: "active_profile_ready",
    shouldRenderFullProfile: true,
    minGalleryImageUrls: 6,
    minPropertyImageUrls: 3,
    minExternalTabs: 10,
    companyValidated: false,
    releaseFieldsSet: true,
    expectedNextAction: "no_action",
  }),
  "design-hotels": Object.freeze({
    displayState: "active_profile_ready",
    shouldRenderFullProfile: true,
    minGalleryImageUrls: 6,
    minPropertyImageUrls: 3,
    minExternalTabs: 10,
    companyValidated: false,
    releaseFieldsSet: true,
    expectedNextAction: "no_action",
  }),
  "hotel-indigo": Object.freeze({
    displayState: "active_profile_ready",
    shouldRenderFullProfile: true,
    minGalleryImageUrls: 6,
    minPropertyImageUrls: 3,
    minExternalTabs: 10,
    companyValidated: false,
    releaseFieldsSet: true,
    expectedNextAction: "no_action",
  }),
  "mgallery-collection": Object.freeze({
    displayState: "active_profile_ready",
    shouldRenderFullProfile: true,
    minGalleryImageUrls: 6,
    minPropertyImageUrls: 3,
    minExternalTabs: 10,
    companyValidated: false,
    releaseFieldsSet: true,
    expectedNextAction: "no_action",
  }),
  "small-luxury-hotels-of-the-world": Object.freeze({
    displayState: "active_profile_ready",
    shouldRenderFullProfile: true,
    minGalleryImageUrls: 6,
    minPropertyImageUrls: 3,
    minExternalTabs: 10,
    companyValidated: false,
    releaseFieldsSet: true,
    expectedNextAction: "no_action",
  }),
});

/** Expected OS next actions for incomplete routed brands (no unlock). Empty after lifestyle graduation. */
export const V44_EXPECTED_INCOMPLETE_ACTIONS = Object.freeze({});

/** Incomplete actions that are allowed while remaining externally locked. */
export const V44_ALLOWED_INCOMPLETE_ACTIONS = Object.freeze([
  "image_remediation",
  "apply_remediation",
  "apply_source_seed",
  "apply_draft",
  "internal_preview_review",
  "founder_visual_review",
  "no_action",
]);

/** Incomplete actions that must never appear (unlock path). */
export const V44_FORBIDDEN_INCOMPLETE_ACTIONS = Object.freeze([
  "apply_active_release",
]);

export const REPORT_JSON = "brand-explorer-v44-release-baseline.json";
export const REPORT_MD = "brand-explorer-v44-release-baseline.md";
export const REPORT_NEXT_BATCH_MD = "brand-explorer-v44-next-batch-router.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const HARD_FORBIDDEN_OWNER_COPY_IDS = Object.freeze([
  "fdd",
  "loi",
  "item_19",
  "fee_stack",
  "net_contribution",
]);

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchBrandApiShape(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
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
      return this;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`brand API fetch failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

/**
 * Capture live release-baseline snapshot for one brand.
 */
export async function captureV44BrandSnapshot(brandSlug) {
  const os = await evaluateBrandExplorerOsBrand(brandSlug);
  const brand = await fetchBrandApiShape(brandSlug);

  const internalHtml = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: true,
  });
  const externalHtml = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: false,
  });

  const ownerCopyHits = scanInternalPreviewOwnerCopy(stripHtml(internalHtml));
  const hardForbiddenOwnerCopy = ownerCopyHits.filter((h) =>
    HARD_FORBIDDEN_OWNER_COPY_IDS.includes(h.id)
  );
  const externalQl = evaluateBrandExternalQualityLock(brand, externalHtml, { brandSlug });

  const galleryImageUrlCount = os.metrics?.galleryCount ?? 0;
  const propertyImageUrlCount = os.metrics?.openingsCount ?? 0;
  const externalTabCount = (externalQl.tabsRenderedExternally || []).length;

  const companyValidated = os.metrics?.companyValidated === true;
  const founderVisualReviewPass = os.metrics?.founderVisualReviewPassed === true;
  const activeProfileApproved = os.metrics?.activeReleaseApproved === true;
  const readyForActiveProfile =
    brand?.readyForActiveProfile === true || activeProfileApproved === true;
  const releaseFieldsSet = founderVisualReviewPass === true && activeProfileApproved === true;

  const cohort = V44_RELEASED_GOLDEN_SLUGS.includes(brandSlug)
    ? "released_golden"
    : V44_INCOMPLETE_ROUTED_SLUGS.includes(brandSlug)
      ? "incomplete_routed"
      : "other";

  return {
    brandSlug,
    brandName: os.brandName || brand?.name || brandSlug,
    recordId: os.recordId || brand?.id || null,
    cohort,
    displayState: brand?.brandExplorerDisplayState || os.displayState || os.canonicalState,
    canonicalState: os.canonicalState,
    shouldRenderFullProfile: brand?.shouldRenderFullProfile === true,
    galleryImageUrlCount,
    propertyImageUrlCount,
    externalTabCount,
    companyValidated,
    releaseFields: {
      founderVisualReviewPass,
      activeProfileApproved,
      readyForActiveProfile,
      releaseFieldsSet,
    },
    goldenSuiteSignals: {
      galleryOk: galleryImageUrlCount >= 6,
      propertyOk: propertyImageUrlCount >= 3,
      hardForbiddenOwnerCopyCount: hardForbiddenOwnerCopy.length,
      hardForbiddenOwnerCopyIds: hardForbiddenOwnerCopy.map((h) => h.id),
    },
    externalQualityLock: {
      pass: externalQl.externalQualityLockPass === true,
      displayState: externalQl.displayState,
      shouldRenderFullProfile: externalQl.shouldRenderFullProfile === true,
      forbiddenStringsFound: externalQl.forbiddenStringsFound || 0,
      profileInPreparationRendered: externalQl.profileInPreparationRendered === true,
      tabsRenderedExternally: externalQl.tabsRenderedExternally || [],
    },
    routing: {
      allowedNextAction: os.routing?.allowedNextAction || null,
      blockedActions: os.routing?.blockedActions || [],
      exactNextCommand: os.routing?.exactNextCommand || null,
      rationale: os.routing?.rationale || null,
      remediationRequired: os.routing?.remediationRequired === true,
      founderReviewAllowed: os.routing?.founderReviewAllowed === true,
      activeReleaseAllowed: os.routing?.activeReleaseAllowed === true,
    },
    blockers: {
      trueBlockers: os.gateEval?.trueBlockers || [],
      failedGates: os.gateEval?.failedGates || [],
      falseBlockers: os.gateEval?.falseBlockers || [],
      stateConflicts: os.stateConflicts || [],
    },
  };
}

/**
 * Regression checks against frozen release expectations + incomplete lock.
 * @returns {{ pass: boolean, failures: string[], checks: object[] }}
 */
export function evaluateV44Regression(snapshots = []) {
  const failures = [];
  const checks = [];

  for (const snap of snapshots) {
    const slug = snap.brandSlug;

    if (V44_RELEASED_GOLDEN_SLUGS.includes(slug)) {
      const exp = V44_FROZEN_RELEASE_EXPECTATIONS[slug];
      const brandFailures = [];

      if (snap.displayState !== exp.displayState && snap.canonicalState !== exp.displayState) {
        brandFailures.push(
          `released_locked_or_wrong_state: live=${snap.displayState}/${snap.canonicalState} expected=${exp.displayState}`
        );
      }
      if (snap.shouldRenderFullProfile !== true) {
        brandFailures.push("released_shouldRenderFullProfile_false");
      }
      if ((snap.galleryImageUrlCount || 0) < exp.minGalleryImageUrls) {
        brandFailures.push(
          `released_gallery_imageurl_drop:${snap.galleryImageUrlCount}<${exp.minGalleryImageUrls}`
        );
      }
      if ((snap.propertyImageUrlCount || 0) < exp.minPropertyImageUrls) {
        brandFailures.push(
          `released_property_imageurl_drop:${snap.propertyImageUrlCount}<${exp.minPropertyImageUrls}`
        );
      }
      if ((snap.externalTabCount || 0) < exp.minExternalTabs) {
        brandFailures.push(
          `released_external_tabs_drop:${snap.externalTabCount}<${exp.minExternalTabs}`
        );
      }
      if (snap.companyValidated !== false) {
        brandFailures.push(`company_validated_changed: live=${snap.companyValidated}`);
      }
      if (snap.releaseFields?.releaseFieldsSet !== true) {
        brandFailures.push("release_fields_not_set");
      }
      if ((snap.goldenSuiteSignals?.hardForbiddenOwnerCopyCount || 0) > 0) {
        brandFailures.push(
          `forbidden_owner_copy:${(snap.goldenSuiteSignals.hardForbiddenOwnerCopyIds || []).join(",")}`
        );
      }
      if (snap.externalQualityLock?.pass !== true) {
        brandFailures.push("external_quality_lock_fail");
      }
      if (snap.routing?.allowedNextAction !== exp.expectedNextAction) {
        brandFailures.push(
          `released_unexpected_action:${snap.routing?.allowedNextAction} expected=${exp.expectedNextAction}`
        );
      }

      const pass = brandFailures.length === 0;
      checks.push({ brandSlug: slug, cohort: "released_golden", pass, failures: brandFailures });
      for (const f of brandFailures) failures.push(`${slug}: ${f}`);
      continue;
    }

    if (V44_INCOMPLETE_ROUTED_SLUGS.includes(slug)) {
      const brandFailures = [];
      if (snap.shouldRenderFullProfile === true) {
        brandFailures.push("incomplete_accidentally_unlocked_full_profile");
      }
      if ((snap.externalTabCount || 0) >= 5) {
        brandFailures.push(`incomplete_external_tabs_leak:${snap.externalTabCount}`);
      }
      if (snap.displayState === "active_profile_ready" || snap.canonicalState === "active_profile_ready") {
        brandFailures.push("incomplete_active_profile_ready");
      }
      if (snap.routing?.activeReleaseAllowed === true) {
        brandFailures.push("incomplete_active_release_allowed");
      }
      if (V44_FORBIDDEN_INCOMPLETE_ACTIONS.includes(snap.routing?.allowedNextAction)) {
        brandFailures.push(`incomplete_forbidden_action:${snap.routing.allowedNextAction}`);
      }
      if (
        snap.routing?.allowedNextAction &&
        !V44_ALLOWED_INCOMPLETE_ACTIONS.includes(snap.routing.allowedNextAction)
      ) {
        brandFailures.push(`incomplete_unexpected_action:${snap.routing.allowedNextAction}`);
      }
      // Company Validated must stay false for incompletes in this cohort too (untouched claim)
      if (snap.companyValidated === true) {
        brandFailures.push("incomplete_company_validated_true_unexpected");
      }

      const expectedAction = V44_EXPECTED_INCOMPLETE_ACTIONS[slug];
      const actionMatch = snap.routing?.allowedNextAction === expectedAction;
      if (!actionMatch) {
        brandFailures.push(
          `incomplete_action_drift: live=${snap.routing?.allowedNextAction} expected=${expectedAction}`
        );
      }

      const pass = brandFailures.length === 0;
      checks.push({
        brandSlug: slug,
        cohort: "incomplete_routed",
        pass,
        failures: brandFailures,
        expectedAction,
        liveAction: snap.routing?.allowedNextAction,
      });
      for (const f of brandFailures) failures.push(`${slug}: ${f}`);
    }
  }

  return {
    pass: failures.length === 0,
    failures,
    checks,
  };
}

/**
 * Build next-batch router rows + batch recommendation.
 */
export function buildV44NextBatchRouter(snapshots = []) {
  const incomplete = snapshots.filter((s) => V44_INCOMPLETE_ROUTED_SLUGS.includes(s.brandSlug));

  const rows = incomplete.map((s) => {
    const expectedAction = V44_EXPECTED_INCOMPLETE_ACTIONS[s.brandSlug];
    const exactBlocker =
      (s.blockers?.trueBlockers || [])[0] ||
      (s.blockers?.failedGates || [])[0] ||
      s.routing?.rationale ||
      "unknown";

    const batchProcessingPossible =
      [
        "image_remediation",
        "apply_remediation",
        "apply_source_seed",
        "apply_draft",
        "founder_visual_review",
        "internal_preview_review",
      ].includes(s.routing?.allowedNextAction) && s.routing?.activeReleaseAllowed !== true;

    return {
      brandSlug: s.brandSlug,
      brandName: s.brandName,
      currentState: s.canonicalState || s.displayState,
      displayState: s.displayState,
      shouldRenderFullProfile: s.shouldRenderFullProfile === true,
      exactBlocker,
      trueBlockers: s.blockers?.trueBlockers || [],
      failedGates: s.blockers?.failedGates || [],
      allowedNextAction: s.routing?.allowedNextAction || null,
      expectedNextAction: expectedAction,
      actionMatchesExpected: s.routing?.allowedNextAction === expectedAction,
      blockedActions: s.routing?.blockedActions || [],
      exactNextCommand: s.routing?.exactNextCommand || null,
      batchProcessingPossible,
      rationale: s.routing?.rationale || null,
    };
  });

  const byAction = rows.reduce((acc, r) => {
    const a = r.allowedNextAction || "unknown";
    (acc[a] ||= []).push(r.brandSlug);
    return acc;
  }, {});

  const designHotels = rows.find((r) => r.brandSlug === "design-hotels");
  const indigo = rows.find((r) => r.brandSlug === "hotel-indigo");
  const mgallery = rows.find((r) => r.brandSlug === "mgallery-collection");
  const slh = rows.find((r) => r.brandSlug === "small-luxury-hotels-of-the-world");

  const optionA = {
    id: "A",
    label: "Design Hotels alone (apply_remediation)",
    brands: designHotels ? ["design-hotels"] : [],
    feasible:
      designHotels?.allowedNextAction === "apply_remediation" &&
      designHotels?.batchProcessingPossible === true,
    why: "Single brand on apply_remediation — likely fastest path to move one incomplete forward without mixing image work.",
  };
  const optionB = {
    id: "B",
    label: "MGallery + Hotel Indigo (image_remediation)",
    brands: ["mgallery-collection", "hotel-indigo"].filter((slug) =>
      rows.some((r) => r.brandSlug === slug && r.allowedNextAction === "image_remediation")
    ),
    feasible:
      mgallery?.allowedNextAction === "image_remediation" &&
      indigo?.allowedNextAction === "image_remediation",
    why: "Shared image_remediation action; keep Design Hotels / SLH out of this batch.",
  };
  const optionC = {
    id: "C",
    label: "MGallery + SLH (lifestyle/collection)",
    brands: ["mgallery-collection", "small-luxury-hotels-of-the-world"].filter((slug) =>
      rows.some((r) => r.brandSlug === slug && r.allowedNextAction === "image_remediation")
    ),
    feasible:
      mgallery?.allowedNextAction === "image_remediation" &&
      slh?.allowedNextAction === "image_remediation",
    why: "Lifestyle/collection pairing on image_remediation; Hotel Indigo deferred.",
  };
  const optionE = {
    id: "E",
    label: "Indigo + MGallery + SLH (founder_visual_review)",
    brands: ["hotel-indigo", "mgallery-collection", "small-luxury-hotels-of-the-world"].filter(
      (slug) =>
        rows.some((r) => r.brandSlug === slug && r.allowedNextAction === "founder_visual_review")
    ),
    feasible:
      indigo?.allowedNextAction === "founder_visual_review" &&
      mgallery?.allowedNextAction === "founder_visual_review" &&
      slh?.allowedNextAction === "founder_visual_review",
    why: "v47 materialization landed; remaining incompletes share founder visual review while staying externally locked.",
  };
  const actionsSeparated = Object.keys(byAction).length >= 2 || rows.length <= 1;
  const optionD = {
    id: "D",
    label: "All incomplete as one batch",
    brands: rows.map((r) => r.brandSlug),
    feasible:
      rows.every((r) => r.batchProcessingPossible) &&
      actionsSeparated &&
      rows.every((r) => r.allowedNextAction !== "apply_active_release"),
    why: "Only if OS actions stay separated per brand. Higher coordination cost.",
  };

  // Prefer A when Design Hotels is uniquely on apply_remediation
  let preferred = "A";
  let preferredRationale =
    "Design Hotels is the only incomplete on apply_remediation — process it alone first to avoid mixing Presentation apply with visual-pack image work.";

  if (optionE.feasible) {
    preferred = "E";
    preferredRationale =
      "Image materialization complete for Indigo, MGallery, and SLH — next shared action is founder visual review (external profiles remain locked).";
  } else if (!optionA.feasible && optionB.feasible) {
    preferred = "B";
    preferredRationale =
      "Design Hotels is not on apply_remediation; start with the shared image_remediation pair MGallery + Hotel Indigo.";
  } else if (!optionA.feasible && !optionB.feasible && optionC.feasible) {
    preferred = "C";
    preferredRationale = "Fall back to MGallery + SLH image_remediation lifestyle/collection batch.";
  } else if (!optionA.feasible && optionD.feasible) {
    preferred = "D";
    preferredRationale =
      "No clean single-action subset; process remaining incompletes only with per-brand OS commands (do not unlock).";
  }

  return {
    incompleteRows: rows,
    byAction,
    options: { A: optionA, B: optionB, C: optionC, E: optionE, D: optionD },
    recommendation: {
      preferred,
      rationale: preferredRationale,
      doNotUnlock: true,
      doNotTouchReleasedContent: true,
      doNotChangeCompanyValidated: true,
    },
  };
}

/**
 * Full v44 dry-run / baseline capture.
 */
export async function runV44ReleaseBaseline({
  brands = V44_DEFAULT_BRANDS,
  dryRun = true,
} = {}) {
  if (!dryRun) {
    throw new Error("v44 release baseline is read-only. Use --dry-run only.");
  }

  const snapshots = [];
  for (const brandSlug of brands) {
    snapshots.push(await captureV44BrandSnapshot(brandSlug));
  }

  const releasedSnapshots = snapshots.filter((s) =>
    V44_RELEASED_GOLDEN_SLUGS.includes(s.brandSlug)
  );
  const incompleteSnapshots = snapshots.filter((s) =>
    V44_INCOMPLETE_ROUTED_SLUGS.includes(s.brandSlug)
  );

  const regression = evaluateV44Regression(snapshots);
  const nextBatch = buildV44NextBatchRouter(snapshots);

  const releaseBaseline = releasedSnapshots.map((s) => {
    const exp = V44_FROZEN_RELEASE_EXPECTATIONS[s.brandSlug];
    return {
      brandSlug: s.brandSlug,
      brandName: s.brandName,
      recordId: s.recordId,
      active_profile_ready:
        s.displayState === "active_profile_ready" || s.canonicalState === "active_profile_ready",
      shouldRenderFullProfile: s.shouldRenderFullProfile === true,
      galleryImageUrlCount: s.galleryImageUrlCount,
      propertyImageUrlCount: s.propertyImageUrlCount,
      externalTabCount: s.externalTabCount,
      companyValidated: s.companyValidated,
      companyValidatedUntouched: s.companyValidated === false,
      releaseFieldsSet: s.releaseFields?.releaseFieldsSet === true,
      releaseFields: s.releaseFields,
      goldenSuitePass:
        s.goldenSuiteSignals?.galleryOk === true &&
        s.goldenSuiteSignals?.propertyOk === true &&
        (s.goldenSuiteSignals?.hardForbiddenOwnerCopyCount || 0) === 0 &&
        s.shouldRenderFullProfile === true,
      externalQualityLockPass: s.externalQualityLock?.pass === true,
      frozenMinimums: exp,
      liveVsFrozen: {
        galleryOk: s.galleryImageUrlCount >= (exp?.minGalleryImageUrls || 6),
        propertyOk: s.propertyImageUrlCount >= (exp?.minPropertyImageUrls || 3),
        tabsOk: s.externalTabCount >= (exp?.minExternalTabs || 10),
      },
    };
  });

  return {
    version: V44_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    guardrails: {
      airtableWrites: false,
      activeRelease: false,
      unlock: false,
      companyValidatedChanges: false,
      releasedBrandContentChanges: false,
      incompleteBrandUnlock: false,
      processOnlyOsRoutedBrands: true,
    },
    frozenExpectations: V44_FROZEN_RELEASE_EXPECTATIONS,
    expectedIncompleteActions: V44_EXPECTED_INCOMPLETE_ACTIONS,
    releaseBaseline,
    snapshots,
    regression,
    nextBatch,
    summary: {
      releasedGoldenCount: releasedSnapshots.length,
      incompleteRoutedCount: incompleteSnapshots.length,
      regressionPass: regression.pass,
      regressionFailureCount: regression.failures.length,
      preferredNextBatch: nextBatch.recommendation.preferred,
      byAction: nextBatch.byAction,
      releasedAllActive:
        releasedSnapshots.length > 0 &&
        releasedSnapshots.every(
          (s) =>
            s.shouldRenderFullProfile === true &&
            (s.displayState === "active_profile_ready" ||
              s.canonicalState === "active_profile_ready")
        ),
      incompleteAllLocked:
        incompleteSnapshots.length === 0 ||
        incompleteSnapshots.every((s) => s.shouldRenderFullProfile !== true),
    },
  };
}

export function renderV44BaselineMarkdown(report) {
  const lines = [
    "# v44 Brand Explorer Release Baseline",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Read-only freeze of golden `active_profile_ready` brands. No Airtable writes. No unlock. No Company Validated changes.",
    "",
    "## Summary",
    "",
    `- Released golden: **${report.summary.releasedGoldenCount}**`,
    `- Incomplete routed: **${report.summary.incompleteRoutedCount}**`,
    `- Regression: **${report.summary.regressionPass ? "PASS" : "FAIL"}** (${report.summary.regressionFailureCount} failure(s))`,
    `- Released all active: **${report.summary.releasedAllActive}**`,
    `- Incomplete all locked: **${report.summary.incompleteAllLocked}**`,
    `- Preferred next batch: **${report.summary.preferredNextBatch}**`,
    "",
    "## Released golden baseline",
    "",
    "| Brand | State ready | Full profile | Gallery | Property | Tabs | Company Validated | Release fields | Golden | Ext lock |",
    "|---|---|---|---|---|---|---|---|---|---|",
  ];

  for (const row of report.releaseBaseline || []) {
    lines.push(
      `| ${row.brandSlug} | ${row.active_profile_ready} | ${row.shouldRenderFullProfile} | ${row.galleryImageUrlCount} | ${row.propertyImageUrlCount} | ${row.externalTabCount} | ${row.companyValidated} (untouched=${row.companyValidatedUntouched}) | ${row.releaseFieldsSet} | ${row.goldenSuitePass} | ${row.externalQualityLockPass} |`
    );
  }

  lines.push("", "## Frozen minimums (must not regress)", "");
  for (const [slug, exp] of Object.entries(report.frozenExpectations || {})) {
    lines.push(
      `- **${slug}**: state=\`${exp.displayState}\` gallery≥${exp.minGalleryImageUrls} property≥${exp.minPropertyImageUrls} tabs≥${exp.minExternalTabs} companyValidated=${exp.companyValidated}`
    );
  }

  lines.push("", "## Regression checks", "");
  if (report.regression?.pass) {
    lines.push("All regression checks passed.");
  } else {
    lines.push("Failures:");
    for (const f of report.regression?.failures || []) lines.push(`- ${f}`);
  }

  lines.push(
    "",
    "## Guardrails",
    "",
    "- No Airtable writes",
    "- No active release / unlock",
    "- No Company Validated changes",
    "- No content changes to released brands",
    "- Incomplete brands remain locked",
    "- Process new brands only when OS-routed",
    ""
  );

  return lines.join("\n");
}

export function renderV44NextBatchMarkdown(report) {
  const nb = report.nextBatch || {};
  const lines = [
    "# v44 Next Batch Router",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Routes incomplete brands using live OS state. Does **not** unlock. Does **not** process released golden brands.",
    "",
    "## Recommendation",
    "",
    `**Preferred: ${nb.recommendation?.preferred}** — ${nb.recommendation?.rationale || ""}`,
    "",
    "## Options",
    "",
  ];

  for (const opt of Object.values(nb.options || {})) {
    lines.push(
      `### ${opt.id}. ${opt.label}`,
      "",
      `- Feasible: **${opt.feasible}**`,
      `- Brands: ${(opt.brands || []).join(", ") || "(none)"}`,
      `- Why: ${opt.why}`,
      ""
    );
  }

  lines.push(
    "## Incomplete brand routing",
    "",
    "| Brand | State | Exact blocker | Next action | Expected | Batch OK | Exact next command |",
    "|---|---|---|---|---|---|---|"
  );

  for (const row of nb.incompleteRows || []) {
    const cmd = String(row.exactNextCommand || "(none)")
      .replace(/\s+/g, " ")
      .trim()
      .replace(/\|/g, "/");
    lines.push(
      `| ${row.brandSlug} | ${row.currentState} | ${String(row.exactBlocker).replace(/\|/g, "/")} | ${row.allowedNextAction} | ${row.expectedNextAction} | ${row.batchProcessingPossible} | \`${cmd}\` |`
    );
  }

  lines.push("", "## Per-brand detail", "");
  for (const row of nb.incompleteRows || []) {
    lines.push(`### ${row.brandSlug}`);
    lines.push(`- Current state: \`${row.currentState}\` (display=\`${row.displayState}\`)`);
    lines.push(`- Full profile: **${row.shouldRenderFullProfile}** (must stay false)`);
    lines.push(`- Exact blocker: ${row.exactBlocker}`);
    lines.push(`- Allowed next action: **${row.allowedNextAction}**`);
    lines.push(`- Blocked actions: ${(row.blockedActions || []).join(", ") || "(none)"}`);
    lines.push(`- Batch processing possible: **${row.batchProcessingPossible}**`);
    lines.push(`- Rationale: ${row.rationale || "—"}`);
    lines.push("```");
    lines.push(row.exactNextCommand || "(none)");
    lines.push("```");
    lines.push("");
  }

  lines.push(
    "## Actions by group",
    "",
    "```json",
    JSON.stringify(nb.byAction || {}, null, 2),
    "```",
    "",
    "## Guardrails",
    "",
    "- Do not unlock incomplete brands",
    "- Do not change Company Validated",
    "- Do not change released brand content",
    "- Only process brands routed by OS",
    ""
  );

  return lines.join("\n");
}

export function writeV44Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const nextBatchPath = path.join(reportsDir, REPORT_NEXT_BATCH_MD);

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, renderV44BaselineMarkdown(report), "utf8");
  fs.writeFileSync(nextBatchPath, renderV44NextBatchMarkdown(report), "utf8");

  return { jsonPath, mdPath, nextBatchPath };
}
