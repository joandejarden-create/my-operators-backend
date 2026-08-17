/**
 * Lane 4 — Unconfigured Active/Live full Tab Factory build
 * (BW Premier, BW Signature, Preferred Hotels & Resorts).
 *
 * Dry-run plans the required content packs / image pools / restore steps.
 * Apply is blocked until full content packs are registered (same bar as Lane 2).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS,
} from "./brand-explorer-full-build-content.js";
import {
  planFullTabFactoryBuild,
  applyFullTabFactoryBuild,
  FULL_BUILD_REQUIRED_APPLY_FLAGS,
} from "./brand-explorer-full-tab-factory-build.js";
import {
  applyPublicRestoreGovernance,
  planPublicRestoreGovernance,
  REQUIRED_APPLY_FLAGS as PUBLIC_RESTORE_GOVERNANCE_FLAGS,
} from "./brand-explorer-public-restore-governance.js";

export const UNCONFIGURED_FULL_BUILD_VERSION = "unconfigured-full-build-v1";

export const UNCONFIGURED_FULL_BUILD_TARGETS = Object.freeze([
  "bw-premier-collection",
  "bw-signature-collection",
  "preferred-hotels-and-resorts",
]);

export const UNCONFIGURED_IDENTITIES = Object.freeze({
  "bw-premier-collection": {
    recordId: "recwXZ5gVZ8ZH8ekA",
    name: "BW Premier Collection",
    parent: "BWH Hotels",
    positioning:
      "Upscale soft-brand / collection option for independents seeking BWH platform benefits with more independent positioning than core Best Western.",
  },
  "bw-signature-collection": {
    recordId: "recdeh1NsP4gjrv80",
    name: "BW Signature Collection",
    parent: "BWH Hotels",
    positioning:
      "Soft-brand / collection option emphasizing independent identity and conversion practicality within the BWH platform.",
  },
  "preferred-hotels-and-resorts": {
    recordId: "recwl5JOYxlChuCAr",
    name: "Preferred Hotels & Resorts",
    parent: "Preferred Hotels & Resorts",
    positioning:
      "Independent hotel representation / soft affiliation platform — distinguish from SLH, Design Hotels, Autograph, Tribute, Curio, Vignette.",
  },
});

export const UNCONFIGURED_FULL_BUILD_APPLY_FLAGS = Object.freeze([
  "--approve-unconfigured-full-build",
  "--confirm-target-brands-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes-before-gates",
  "--confirm-tab-factory-contracts",
  "--confirm-source-provenance-by-tab",
  "--confirm-image-uniqueness",
  "--confirm-image-role-match",
]);

export const UNCONFIGURED_PUBLIC_RESTORE_FLAGS = Object.freeze([
  "--approve-public-restore-governance",
  "--confirm-founder-visual-review-passed",
  "--confirm-fully-ready",
  "--confirm-public-visibility-quality-lock-passed",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-rewrites",
  "--confirm-no-image-writes",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function contentPackExists(slug) {
  const p = path.join(
    ROOT,
    "lib",
    "partner-intelligence",
    `brand-explorer-full-build-content-${slug}.js`
  );
  return fs.existsSync(p);
}

function galleryPoolExists(slug) {
  return fs.existsSync(path.join(ROOT, "fixtures", `lane2-${slug}-gallery-pool.json`));
}

function openingsFixtureExists(slug) {
  return fs.existsSync(
    path.join(ROOT, "fixtures", `brand-explorer-presentation-${slug}-footprint-openings.json`)
  );
}

export async function planUnconfiguredFullBuild({ brands = null } = {}) {
  const list = brands?.length ? brands : [...UNCONFIGURED_FULL_BUILD_TARGETS];
  const universe = await loadActiveUniverse({ includeDetails: true });
  const brandPlans = [];

  for (const slug of list) {
    if (!UNCONFIGURED_FULL_BUILD_TARGETS.includes(slug)) {
      throw new Error(`Not an unconfigured full-build target: ${slug}`);
    }
    const identity = UNCONFIGURED_IDENTITIES[slug];
    const live = universe.bySlug.get(slug) || universe.byRecordId.get(identity.recordId);
    const packExists = contentPackExists(slug);
    const poolExists = galleryPoolExists(slug);
    const openingsFx = openingsFixtureExists(slug);
    const inLegacyLane2 = FULL_BUILD_TRUE_INCOMPLETE_SLUGS.includes(slug);
    const inUnconfiguredCohort = UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS.includes(slug);
    const contentReady = packExists;
    const imagesReady = poolExists;
    const buildReady = contentReady; // content can apply without images; public restore still needs gates

    brandPlans.push({
      brandSlug: slug,
      ...identity,
      liveBrandStatus: live?.status || null,
      presentationRows: live?.presentationRowCount || 0,
      publicFull: live?.publicFull === true,
      displayState: live?.displayState || null,
      contentPackExists: packExists,
      galleryPoolExists: poolExists,
      openingsFixtureExists: openingsFx,
      registeredInLegacyLane2Cohort: inLegacyLane2,
      registeredInUnconfiguredCohort: inUnconfiguredCohort,
      buildReady,
      imagesReady,
      requiredSteps: [
        "Author brand-explorer-full-build-content-<slug>.js (~70+ presentation rows)",
        "Register in brand-explorer-full-build-content.js (UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS)",
        "Create fixtures/lane2-<slug>-gallery-pool.json (6 gallery + 3 scenario + 3 property URLs)",
        "Add property catalog entries for openings cards",
        "Dry-run full-tab-factory-build → apply content POSTs",
        "Image asset pack → image materialization apply",
        "Founder minor cleanup (Recent Momentum contract)",
        "Run full gate suite + PVQL",
        "Founder packet → approve_for_active_release",
        "Public restore governance apply (Basics release fields only)",
      ],
      ownerLensNotes: identity.positioning,
      blockedReason: packExists
        ? null
        : "Missing full-build content pack (~70+ Presentation rows)",
      imageBlockedReason: poolExists ? null : "Missing gallery image pool fixture",
    });
  }

  const ready = brandPlans.filter((b) => b.buildReady);
  const blocked = brandPlans.filter((b) => !b.buildReady);
  const imagesBlocked = brandPlans.filter((b) => !b.imagesReady);

  return {
    version: UNCONFIGURED_FULL_BUILD_VERSION,
    generatedAt: new Date().toISOString(),
    targets: list,
    brands: brandPlans,
    blockedUntilContentPacks: blocked.length > 0,
    imagesBlocked: imagesBlocked.length > 0,
    summary: {
      brands: brandPlans.length,
      buildReady: ready.length,
      blocked: blocked.length,
      blockedSlugs: blocked.map((b) => b.brandSlug),
      imagesBlockedSlugs: imagesBlocked.map((b) => b.brandSlug),
    },
    validation: {
      pass: blocked.length === 0,
      failedChecks: blocked.map(
        (b) => `content_pack_missing:${b.brandSlug}:${b.blockedReason}`
      ),
      warnings: imagesBlocked.map(
        (b) => `gallery_pool_missing:${b.brandSlug}:${b.imageBlockedReason}`
      ),
      note:
        "Content apply is allowed when content packs exist. Image materialization and public restore remain blocked until gallery pools + gates pass.",
    },
    nextImplementation: {
      pattern: "full-tab-factory-build (content) → lane2-image-materialization → public-restore-governance",
      referencePacks: [
        "brand-explorer-full-build-content-autograph-collection.js",
        "brand-explorer-full-build-content-vignette-collection.js",
      ],
      openingsFixturesPresent: brandPlans
        .filter((b) => b.openingsFixtureExists)
        .map((b) => b.brandSlug),
    },
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      brandStatusUntouched: true,
      noReleaseWritesBeforeGates: true,
      radissonCollectionExcluded: true,
      tapestryExcluded: true,
    },
  };
}

export async function applyUnconfiguredFullBuild({ report, apply = false, argv = [] } = {}) {
  if (!apply) return { applied: false, reason: "dry_run_only", results: [] };
  const missing = UNCONFIGURED_FULL_BUILD_APPLY_FLAGS.filter((f) => !argv.includes(f));
  if (missing.length) {
    return { applied: false, reason: "missing_apply_flags", missing, results: [] };
  }
  if (report?.blockedUntilContentPacks) {
    return {
      applied: false,
      reason: "content_packs_missing",
      failedChecks: report.validation?.failedChecks || [],
      results: [],
      guidance:
        "Author full-build content packs first, then re-run this lane. See reports/brand-explorer-unconfigured-full-build-summary.md",
    };
  }

  const brands = (report.brands || [])
    .filter((b) => b.buildReady)
    .map((b) => b.brandSlug);
  const factoryArgv = [
    ...argv,
    "--apply",
    ...FULL_BUILD_REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f)),
  ];
  // Map unconfigured confirmations onto full-tab-factory required flags when absent.
  const plan = await planFullTabFactoryBuild({ brands });
  const applyResult = await applyFullTabFactoryBuild({
    plan,
    apply: true,
    argv: factoryArgv,
  });
  return {
    applied: applyResult.applied === true,
    reason: applyResult.reason || "content_apply",
    planSummary: {
      brands: plan.brandResults?.length,
      blocked: (plan.brandResults || []).filter((b) => b.blocked).map((b) => b.brandSlug),
      patchCounts: Object.fromEntries(
        (plan.brandResults || []).map((b) => [b.brandSlug, (b.patches || []).length])
      ),
    },
    applyResult,
    imagesStillBlocked: report.imagesBlocked === true,
    guidance: report.imagesBlocked
      ? "Content applied (or planned). Still need gallery pools + image materialization before public restore."
      : "Content applied. Proceed to image gates + founder review + public restore.",
  };
}

export async function applyUnconfiguredPublicRestore({
  report,
  apply = false,
  argv = [],
} = {}) {
  if (!apply) return { applied: false, reason: "dry_run_only", results: [] };
  const missing = UNCONFIGURED_PUBLIC_RESTORE_FLAGS.filter((f) => !argv.includes(f));
  if (missing.length) {
    return { applied: false, reason: "missing_apply_flags", missing, results: [] };
  }
  if (report?.imagesBlocked) {
    return {
      applied: false,
      reason: "images_or_gates_incomplete",
      guidance:
        "Public restore blocked until gallery pools exist, image uniqueness/role-match pass, and founder packets recommend approve_for_active_release.",
      results: [],
    };
  }
  const brands = UNCONFIGURED_FULL_BUILD_TARGETS;
  const restoreArgv = [
    ...argv,
    "--apply",
    ...PUBLIC_RESTORE_GOVERNANCE_FLAGS.filter((f) => !argv.includes(f)),
  ];
  const plan = await planPublicRestoreGovernance({ brands });
  const result = await applyPublicRestoreGovernance({
    plan,
    apply: true,
    argv: restoreArgv,
  });
  return {
    applied: result?.applied === true,
    reason: result?.reason || "public_restore",
    results: result,
  };
}
export function writeUnconfiguredFullBuildReports(report, applyResult = null) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const out = { ...report, applyResult: applyResult || { applied: false } };
  const jsonPath = path.join(reportsDir, "brand-explorer-unconfigured-full-build-summary.json");
  const mdPath = path.join(reportsDir, "brand-explorer-unconfigured-full-build-summary.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");

  const lines = [
    "# Unconfigured Full Build Summary",
    "",
    `Version: \`${report.version}\` · ${report.generatedAt}`,
    `Blocked until content packs: **${report.blockedUntilContentPacks}**`,
    "",
  ];
  for (const b of report.brands || []) {
    const per = path.join(reportsDir, `brand-explorer-unconfigured-full-build-${b.brandSlug}.md`);
    const body = [
      `# Unconfigured Full Build — ${b.name}`,
      "",
      `| Field | Value |`,
      `| --- | --- |`,
      `| Slug | \`${b.brandSlug}\` |`,
      `| Record ID | \`${b.recordId}\` |`,
      `| Brand Status | ${b.liveBrandStatus || "—"} |`,
      `| Presentation rows | ${b.presentationRows} |`,
      `| Content pack exists | ${b.contentPackExists} |`,
      `| Gallery pool exists | ${b.galleryPoolExists} |`,
      `| Openings fixture exists | ${b.openingsFixtureExists} |`,
      `| Build ready | ${b.buildReady} |`,
      `| Blocked reason | ${b.blockedReason || "—"} |`,
      "",
      "## Owner lens",
      "",
      b.ownerLensNotes,
      "",
      "## Required steps",
      "",
      ...(b.requiredSteps || []).map((s, i) => `${i + 1}. ${s}`),
      "",
    ].join("\n");
    fs.writeFileSync(per, `${body}\n`, "utf8");

    // Founder packet placeholder
    const founder = path.join(reportsDir, `brand-explorer-founder-review-${b.brandSlug}.md`);
    fs.writeFileSync(
      founder,
      [
        `# Founder Review — ${b.name}`,
        "",
        "Status: **PENDING full Tab Factory build**",
        "",
        "Do not recommend `approve_for_active_release` until content pack, images (6/3/3), and gate suite pass.",
        "",
        `Blocked: ${b.blockedReason || "n/a"}`,
        "",
      ].join("\n") + "\n",
      "utf8"
    );

    lines.push(`## ${b.name} (\`${b.brandSlug}\`)`);
    lines.push("");
    lines.push(`- Build ready: **${b.buildReady}**`);
    lines.push(`- Blocked: ${b.blockedReason || "—"}`);
    lines.push(`- Detail: \`reports/brand-explorer-unconfigured-full-build-${b.brandSlug}.md\``);
    lines.push("");
  }
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}
