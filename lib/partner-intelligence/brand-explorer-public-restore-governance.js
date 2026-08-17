/**
 * Brand Explorer — Public Restore Governance.
 *
 * Formalizes public-full visibility for fullyReady built-blocked brands after
 * founder visual review. Does not rewrite content/images, Company Validated,
 * Source Library status, or Registry approval/status.
 *
 * Accidental legacy unlock (country / suburban / woodspring) is held until
 * brands appear in data/brand-explorer-public-restore-intentional.json.
 *
 * Dry-run by default. Apply requires explicit founder confirm flags.
 */
import fs from "fs";
import path from "path";
import {
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_IDENTITIES,
  BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
} from "./brand-explorer-built-blocked-content.js";
import {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS,
  FULL_BUILD_IDENTITIES,
  resolveFullBuildSlug,
} from "./brand-explorer-full-build-content.js";
import { verifyBuiltBlockedBrand } from "./brand-explorer-built-blocked-remediation.js";
import {
  runProfilePreparationVisibilityFix,
  applyProfilePreparationVisibilityFix,
  VISIBILITY_RESTORED_RELEASE_SLUGS,
} from "./brand-explorer-profile-preparation-visibility-fix.js";
import {
  ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS,
  INTENTIONAL_RESTORE_PATH,
  ROOT,
  isLegacyVisibilityUnlockHeld,
  isIntentionalPublicRestoreSlug,
  readIntentionalPublicRestoreSlugs,
  writeIntentionalPublicRestoreSlugs,
} from "./brand-explorer-public-restore-registry.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";

/** Lane 1 + Lane 2 restore cohort for governance. */
/**
 * Operational restore cohort (Lane 1 + Lane 2) — NOT the active universe.
 * Some targets (e.g. radisson-collection, tapestry) may be Draft/Under Review
 * and must not be counted in the Brand Status Active/Live 24.
 * Active universe: lib/partner-intelligence/brand-explorer-active-universe.js
 */
export const PUBLIC_RESTORE_GOVERNANCE_TARGETS = Object.freeze([
  ...BUILT_BLOCKED_TARGETS,
  ...FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  ...UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS,
]);

export const PUBLIC_RESTORE_GOVERNANCE_VERSION = "public-restore-governance-v1";
export const REPORT_JSON = "brand-explorer-public-restore-governance.json";
export const REPORT_MD = "brand-explorer-public-restore-governance.md";

export {
  ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS,
  INTENTIONAL_RESTORE_PATH,
  ROOT,
  isLegacyVisibilityUnlockHeld,
  isIntentionalPublicRestoreSlug,
  readIntentionalPublicRestoreSlugs,
  writeIntentionalPublicRestoreSlugs,
};

export const REQUIRED_APPLY_FLAGS = Object.freeze([
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

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export const SLUG_ALIASES = Object.freeze({
  country: "country-inn-suites",
  "country-inn": "country-inn-suites",
  "country-inn-suites": "country-inn-suites",
  quality: "quality-inn",
  "quality-inn": "quality-inn",
  radisson: "radisson",
  blu: "radisson-blu",
  "radisson-blu": "radisson-blu",
  red: "radisson-red",
  "radisson-red": "radisson-red",
  suburban: "suburban-studios",
  "suburban-studios": "suburban-studios",
  woodspring: "woodspring-suites",
  "woodspring-suites": "woodspring-suites",
  autograph: "autograph-collection",
  "autograph-collection": "autograph-collection",
  handwritten: "handwritten-collection",
  "handwritten-collection": "handwritten-collection",
  "radisson-collection": "radisson-collection",
  tapestry: "tapestry-collection-by-hilton",
  "tapestry-collection-by-hilton": "tapestry-collection-by-hilton",
  vignette: "vignette-collection",
  "vignette-collection": "vignette-collection",
});

export function resolvePublicRestoreSlug(raw) {
  const key = nz(raw).toLowerCase();
  if (SLUG_ALIASES[key]) return SLUG_ALIASES[key];
  return resolveFullBuildSlug(key);
}

export function resolvePublicRestoreBrands(rawList) {
  if (!rawList?.length) return [...PUBLIC_RESTORE_GOVERNANCE_TARGETS];
  return [...new Set(rawList.map(resolvePublicRestoreSlug))];
}

function resolveIdentity(slug) {
  return (
    getActiveProfileBrandConfig(slug) ||
    BUILT_BLOCKED_IDENTITIES[slug] ||
    FULL_BUILD_IDENTITIES[slug] ||
    null
  );
}

function readReadyFromFinalReadinessReport(slug, reportsDir = path.join(ROOT, "reports")) {
  const p = path.join(reportsDir, "brand-explorer-final-public-restore-readiness.json");
  try {
    if (!fs.existsSync(p)) return null;
    const report = JSON.parse(fs.readFileSync(p, "utf8"));
    const row = (report.brandResults || []).find((b) => b.brandSlug === slug);
    if (!row) return null;
    return {
      fullyReady:
        row.readyForPublicRestore === true &&
        row.recommendation === "approve_for_active_release" &&
        row.gateSuitePass === true,
      recommendation: row.recommendation,
      gateSuitePass: row.gateSuitePass,
      source: "final-public-restore-readiness",
    };
  } catch {
    return null;
  }
}

async function resolveFullyReady(slug, { reportsDir } = {}) {
  if (BUILT_BLOCKED_TARGETS.includes(slug)) {
    try {
      const verify = await verifyBuiltBlockedBrand(slug);
      return {
        fullyReady: verify?.fullyReady === true,
        verify,
        verifyError: null,
        source: "built-blocked-verify",
      };
    } catch (err) {
      return {
        fullyReady: false,
        verify: null,
        verifyError: err?.message || String(err),
        source: "built-blocked-verify",
      };
    }
  }
  if (
    FULL_BUILD_TRUE_INCOMPLETE_SLUGS.includes(slug) ||
    UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS.includes(slug)
  ) {
    const fromReport = readReadyFromFinalReadinessReport(slug, reportsDir);
    if (fromReport) {
      return {
        fullyReady: fromReport.fullyReady === true,
        verify: fromReport,
        verifyError: fromReport.fullyReady ? null : "lane2_not_ready_in_final_readiness_report",
        source: fromReport.source,
      };
    }
    // Fallback: inline readiness audit for this slug only
    const { auditBrandFinalPublicRestoreReadiness } = await import(
      "./brand-explorer-final-public-restore-readiness.js"
    );
    const row = await auditBrandFinalPublicRestoreReadiness(slug);
    return {
      fullyReady:
        row.readyForPublicRestore === true &&
        row.recommendation === "approve_for_active_release" &&
        row.gateSuitePass === true,
      verify: row,
      verifyError: row.readyForPublicRestore ? null : (row.failures || []).join(",") || "lane2_not_ready",
      source: "inline-final-readiness",
    };
  }
  return {
    fullyReady: false,
    verify: null,
    verifyError: "not_in_restore_cohort",
    source: "none",
  };
}

async function patchBasicsReleaseFields({ recordId, fields }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
    if (fields?.[forbidden] != null) throw new Error(`Forbidden field: ${forbidden}`);
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics PATCH failed: ${res.status}`);
  return json;
}

export function parsePublicRestoreApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export async function planPublicRestoreGovernance({
  brands = [...PUBLIC_RESTORE_GOVERNANCE_TARGETS],
  reportsDir = path.join(ROOT, "reports"),
} = {}) {
  const intentional = readIntentionalPublicRestoreSlugs();
  const brandResults = [];

  for (const slug of brands) {
    if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(slug)) {
      brandResults.push({
        slug,
        lane: "protected_public_full",
        action: "refuse",
        reason: "protected_public_full_baseline_untouched",
        fullyReady: null,
        publicRestoreEligible: false,
      });
      continue;
    }
    if (!PUBLIC_RESTORE_GOVERNANCE_TARGETS.includes(slug)) {
      brandResults.push({
        slug,
        lane: "out_of_scope",
        action: "refuse",
        reason: "not_in_public_restore_governance_targets",
        fullyReady: null,
        publicRestoreEligible: false,
      });
      continue;
    }

    const readiness = await resolveFullyReady(slug, { reportsDir });
    const fullyReady = readiness.fullyReady === true;
    const held = isLegacyVisibilityUnlockHeld(slug, { intentionalSlugs: intentional });
    const alreadyIntentional = intentional.includes(slug);
    const identity = resolveIdentity(slug) || {};
    const lane = BUILT_BLOCKED_TARGETS.includes(slug)
      ? "lane1_built_blocked"
      : "lane2_full_build";

    let action = "hold_founder_preview_only";
    let reason = "awaiting_founder_visual_review_and_explicit_restore_apply";
    if (!fullyReady) {
      action = "block_not_fully_ready";
      reason = readiness.verifyError || "verify_fullyReady_false";
    } else if (alreadyIntentional) {
      action = "already_intentional_public_restore";
      reason = "listed_in_intentional_restore_registry";
    } else if (held) {
      action = "hold_end_accidental_legacy_unlock";
      reason =
        "currently_or_previously_public_via_legacyVisibilityUnlock; held to founder-preview-only until restore apply";
    }

    brandResults.push({
      slug,
      recordId: identity.recordId || null,
      name: identity.name || slug,
      lane,
      fullyReady,
      verifyError: readiness.verifyError,
      verifySource: readiness.source,
      verifyGates: readiness.verify?.gates || null,
      accidentalLegacyUnlockHold: held,
      alreadyIntentional,
      publicRestoreEligible: fullyReady && !alreadyIntentional,
      visibilityPostureIfDryRun:
        held || !alreadyIntentional ? "founder_preview_only" : "intentional_public_full",
      plannedOnApply: fullyReady
        ? {
            addToIntentionalRegistry: !alreadyIntentional,
            writeBasicsReleaseFields: true,
            contentRewrites: false,
            imageWrites: false,
            companyValidatedWrites: false,
            sourceLibraryWrites: false,
            registryWrites: false,
          }
        : null,
      action,
      reason,
    });
  }

  const visibilityFixPlan = await runProfilePreparationVisibilityFix({
    reportsDir,
    slugs: brands.filter((s) => PUBLIC_RESTORE_GOVERNANCE_TARGETS.includes(s)),
  }).catch((err) => ({
    summary: null,
    error: err?.message || String(err),
  }));

  const summary = {
    brandCount: brandResults.length,
    fullyReadyCount: brandResults.filter((b) => b.fullyReady).length,
    eligibleRestoreCount: brandResults.filter((b) => b.publicRestoreEligible).length,
    heldAccidentalUnlockCount: brandResults.filter((b) => b.accidentalLegacyUnlockHold).length,
    alreadyIntentionalCount: brandResults.filter((b) => b.alreadyIntentional).length,
    protectedRefusedCount: brandResults.filter((b) => b.lane === "protected_public_full").length,
    lane1Count: brandResults.filter((b) => b.lane === "lane1_built_blocked").length,
    lane2Count: brandResults.filter((b) => b.lane === "lane2_full_build").length,
    intentionalRegistrySlugs: intentional,
    visibilityRestoredCodeCohort: [...VISIBILITY_RESTORED_RELEASE_SLUGS],
    accidentalLegacyUnlockHoldSlugs: [...ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS],
    note:
      "Dry-run does not restore public-full. Accidental legacy unlock held until intentional registry + founder-approved apply.",
  };

  return {
    version: PUBLIC_RESTORE_GOVERNANCE_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    brandResults,
    visibilityFixPlanSummary: visibilityFixPlan?.summary || null,
    summary,
    requiredApplyFlags: [...REQUIRED_APPLY_FLAGS],
  };
}

export async function applyPublicRestoreGovernance({
  plan,
  apply = false,
  argv = [],
  reportsDir = path.join(ROOT, "reports"),
} = {}) {
  const flagCheck = parsePublicRestoreApplyFlags(argv);
  if (!apply) {
    return { applied: false, reason: "dry_run_only", flagCheck, plan };
  }
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, flagCheck };
  }

  const eligible = (plan.brandResults || []).filter((b) => b.publicRestoreEligible && b.fullyReady);
  if (!eligible.length) {
    return { applied: false, reason: "no_eligible_brands", flagCheck, plan };
  }

  for (const row of eligible) {
    if (row.fullyReady !== true) {
      throw new Error(`Refuse restore: ${row.slug} not fullyReady`);
    }
    if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(row.slug)) {
      throw new Error(`Refuse restore: protected baseline ${row.slug}`);
    }
  }

  const intentionalBefore = readIntentionalPublicRestoreSlugs();
  const nextSlugs = [...new Set([...intentionalBefore, ...eligible.map((b) => b.slug)])];
  const registry = writeIntentionalPublicRestoreSlugs(nextSlugs);

  // Write Basics release fields for every eligible brand (visibility-fix willRestore
  // can skip legacy_approved_pending_migration; governance must not).
  const today = new Date().toISOString().slice(0, 10);
  const basicsResults = [];
  for (const row of eligible) {
    const recordId = row.recordId || resolveIdentity(row.slug)?.recordId;
    if (!recordId) {
      basicsResults.push({ brandSlug: row.slug, applied: false, reason: "missing_record_id" });
      continue;
    }
    const fields = {
      "Active Profile Approved": true,
      "Ready for Active Profile": true,
      "Active Profile Approved Date": today,
      "Founder Visual Review Pass": true,
    };
    try {
      await patchBasicsReleaseFields({ recordId, fields });
      basicsResults.push({
        brandSlug: row.slug,
        applied: true,
        recordId,
        fields: Object.keys(fields),
        sanitizedPayloadPreview: fields,
      });
      await new Promise((r) => setTimeout(r, 220));
    } catch (err) {
      basicsResults.push({
        brandSlug: row.slug,
        applied: false,
        recordId,
        reason: err.message,
      });
    }
  }

  // Best-effort visibility-fix path for any additional code-cohort side effects.
  let visibilityApply = null;
  try {
    const visibilityArgv = [
      "--apply",
      "--approve-profile-preparation-visibility-fix",
      "--confirm-legacy-status-evidence-reviewed",
      "--confirm-no-company-validation-changes",
      "--confirm-no-source-library-status-changes",
      "--confirm-no-registry-approval-changes",
      "--confirm-no-content-rewrites",
      "--confirm-founder-preview-access",
    ];
    const visibilityPlan = await runProfilePreparationVisibilityFix({
      reportsDir,
      slugs: eligible.map((b) => b.slug),
    });
    for (const row of visibilityPlan.plannedRestores || []) {
      if (
        eligible.some((e) => e.slug === row.slug) &&
        row.patch &&
        Object.keys(row.patch.fields || {}).length
      ) {
        row.willRestore = true;
      }
    }
    visibilityApply = await applyProfilePreparationVisibilityFix({
      report: visibilityPlan,
      apply: true,
      argv: visibilityArgv,
    });
  } catch (err) {
    visibilityApply = {
      applied: false,
      reason: err?.message || String(err),
      note: "Basics release fields + intentional registry already written; visibility-fix secondary path failed",
    };
  }

  const basicsFailed = basicsResults.filter((r) => r.applied !== true);
  return {
    applied: basicsFailed.length === 0,
    reason:
      basicsFailed.length === 0
        ? "public_restore_governance_applied"
        : "public_restore_partial_basics_errors",
    flagCheck,
    intentionalRegistry: registry,
    restoredSlugs: eligible.map((b) => b.slug),
    basicsResults,
    basicsFailed,
    visibilityApply,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    contentRewrites: false,
    imageWrites: false,
  };
}

export function writePublicRestoreGovernanceReports(result, { reportsDir = path.join(ROOT, "reports") } = {}) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");

  const lines = [
    `# Brand Explorer — Public Restore Governance`,
    ``,
    `- Version: \`${result.version || PUBLIC_RESTORE_GOVERNANCE_VERSION}\``,
    `- Generated: ${result.generatedAt || new Date().toISOString()}`,
    `- Mode: ${result.applyResult?.applied ? "**APPLY**" : "**dry-run**"}`,
    ``,
    `## Summary`,
    ``,
    `- Brands: ${result.summary?.brandCount ?? 0}`,
    `- Fully ready: ${result.summary?.fullyReadyCount ?? 0}`,
    `- Eligible restore: ${result.summary?.eligibleRestoreCount ?? 0}`,
    `- Held accidental unlock: ${result.summary?.heldAccidentalUnlockCount ?? 0}`,
    `- Intentional registry: ${(result.summary?.intentionalRegistrySlugs || []).join(", ") || "(empty)"}`,
    ``,
    `## Accidental legacy unlock hold`,
    ``,
    ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.map((s) => `- \`${s}\``).join("\n"),
    ``,
    `These stay founder-preview-only until listed in \`data/brand-explorer-public-restore-intentional.json\` via founder-approved \`--apply\`.`,
    ``,
    `## Brand results`,
    ``,
  ];
  for (const b of result.brandResults || []) {
    lines.push(
      `### ${b.name || b.slug}`,
      ``,
      `- Slug: \`${b.slug}\``,
      `- Fully ready: ${b.fullyReady}`,
      `- Action: \`${b.action}\``,
      `- Reason: ${b.reason}`,
      `- Accidental hold: ${b.accidentalLegacyUnlockHold}`,
      `- Restore eligible: ${b.publicRestoreEligible}`,
      ``
    );
  }
  lines.push(
    `## Apply command (founder approval required)`,
    ``,
    "```bash",
    `npm run brand-explorer-public-restore-governance -- --brands ${(result.brands || BUILT_BLOCKED_TARGETS).join(",")} --apply \\`,
    ...REQUIRED_APPLY_FLAGS.map((f, i) =>
      i === REQUIRED_APPLY_FLAGS.length - 1 ? `  ${f}` : `  ${f} \\`
    ),
    "```",
    ``
  );
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}
