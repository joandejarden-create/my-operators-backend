/**
 * Brand Explorer — Profile in Preparation visibility restore.
 *
 * Restores full-profile visibility for migrate-ready historically approved brands
 * without touching Company Validated, Source Library, Registry, or owner-facing copy.
 *
 * Dry-run by default. Apply requires explicit confirmation flags.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import {
  runProfilePreparationVisibilityAudit,
  writeProfilePreparationVisibilityAuditReports,
} from "./brand-explorer-profile-preparation-visibility-audit.js";
import { getLegacySeedBrand } from "./brand-explorer-legacy-approved-profile-reconciliation.js";

export const VISIBILITY_FIX_VERSION = "profile-preparation-visibility-fix-v1";
export const REPORT_JSON = "brand-explorer-profile-preparation-visibility-fix.json";
export const REPORT_MD = "brand-explorer-profile-preparation-visibility-fix.md";

/**
 * Code-side release cohort for visibility-restored legacy brands.
 * Merged into display/OS recognition without rewriting PRIMARY_RELEASE_SLUGS
 * until a brand is fully active-profile ready under current gates.
 */
export const VISIBILITY_RESTORED_RELEASE_SLUGS = Object.freeze([
  "ascend",
  "comfort-inn-suites",
  "curio-collection",
  "tribute-portfolio",
]);

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-profile-preparation-visibility-fix",
  "--confirm-legacy-status-evidence-reviewed",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-rewrites",
  "--confirm-founder-preview-access",
]);

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function parseVisibilityFixApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export function isVisibilityRestoredReleaseSlug(slug) {
  return VISIBILITY_RESTORED_RELEASE_SLUGS.includes(nz(slug).toLowerCase());
}

export function listVisibilityAwareReleaseSlugs() {
  return [...new Set([...PRIMARY_RELEASE_SLUGS, ...VISIBILITY_RESTORED_RELEASE_SLUGS])];
}

function buildRestorePatch(row) {
  if (
    row.classification !== "legacy_approved_ready_to_restore" &&
    row.classification !== "legacy_approved_pending_migration"
  ) {
    return null;
  }
  if (!row.brandBasicsRecordId) return null;
  if (row.imageUniquenessPass !== true) return null;
  if ((row.missingGates || []).includes("presentation_rows")) return null;
  if ((row.missingGates || []).includes("visual_asset_counts")) return null;

  const fields = {};
  if (!row.activeProfileFields?.activeProfileApproved) {
    fields["Active Profile Approved"] = true;
    fields["Ready for Active Profile"] = true;
    fields["Active Profile Approved Date"] = new Date().toISOString().slice(0, 10);
  }
  if (!row.activeProfileFields?.founderVisualReviewPass) {
    // Only when historical/seed evidence supports a prior finished profile.
    if (
      row.legacyApprovalEvidence?.seedNamed ||
      row.legacyApprovalEvidence?.historicalReportReady ||
      row.historicalStatusFields?.historicalApproved
    ) {
      fields["Founder Visual Review Pass"] = true;
    }
  }

  if (!Object.keys(fields).length) {
    return {
      table: "Brand Setup - Brand Basics",
      action: "NOOP",
      recordId: row.brandBasicsRecordId,
      brandSlug: row.slug,
      fields: {},
      reason: "already_has_release_fields_code_cohort_only",
      codeCohort: true,
    };
  }

  return {
    table: "Brand Setup - Brand Basics",
    action: "PATCH",
    recordId: row.brandBasicsRecordId,
    brandSlug: row.slug,
    fields,
    reason: "profile_preparation_visibility_restore",
    codeCohort: true,
    fieldMapping: Object.fromEntries(
      Object.keys(fields).map((k) => [k, `Brand Basics.${k}`])
    ),
  };
}

export async function runProfilePreparationVisibilityFix({
  reportsDir = path.join(ROOT, "reports"),
  slugs = null,
} = {}) {
  const audit = await runProfilePreparationVisibilityAudit({ reportsDir, slugs });
  writeProfilePreparationVisibilityAuditReports(audit, { reportsDir });

  const restoreCandidates = (audit.brandResults || []).filter(
    (b) =>
      b.classification === "legacy_approved_ready_to_restore" ||
      b.classification === "legacy_approved_pending_migration"
  );

  const planned = [];
  for (const row of restoreCandidates) {
    // Prefer explicit migrate-ready cohort; also allow any audit-classified restore.
    const inNamedCohort = VISIBILITY_RESTORED_RELEASE_SLUGS.includes(row.slug);
    const patch = buildRestorePatch(row);
    planned.push({
      ...row,
      inNamedRestoreCohort: inNamedCohort,
      willRestore: Boolean(patch) && (inNamedCohort || row.classification === "legacy_approved_ready_to_restore"),
      patch,
      validation: {
        pass: Boolean(patch),
        failedChecks: patch
          ? []
          : ["not_eligible_for_visibility_restore_writes"],
      },
    });
  }

  const willRestore = planned.filter((p) => p.willRestore);
  const byClass = {};
  for (const b of audit.brandResults || []) {
    byClass[b.classification] = (byClass[b.classification] || 0) + 1;
  }

  return {
    version: VISIBILITY_FIX_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    visibilityRestoredReleaseSlugs: [...VISIBILITY_RESTORED_RELEASE_SLUGS],
    primaryReleaseSlugsUntouched: [...PRIMARY_RELEASE_SLUGS],
    note:
      "PRIMARY_RELEASE_SLUGS left unchanged; visibility restored via code cohort + optional Brand Basics release fields.",
    auditSummary: audit.summary,
    plannedRestores: planned,
    brandResults: audit.brandResults,
    profileInPreparationBrands: audit.profileInPreparationBrands,
    summary: {
      byClassification: byClass,
      restorePlannedCount: willRestore.length,
      restoredProfiles: willRestore.map((p) => p.slug),
      migrationReadyProfiles: (audit.brandResults || [])
        .filter((b) =>
          ["legacy_approved_ready_to_restore", "legacy_approved_pending_migration"].includes(
            b.classification
          )
        )
        .map((b) => b.slug),
      imageRemediationProfiles: (audit.brandResults || [])
        .filter((b) => b.classification === "image_remediation_required")
        .map((b) => b.slug),
      contentRemediationProfiles: (audit.brandResults || [])
        .filter((b) => b.classification === "content_remediation_required")
        .map((b) => b.slug),
      noEvidenceProfiles: (audit.brandResults || [])
        .filter((b) => b.classification === "no_legacy_evidence")
        .map((b) => b.slug),
      founderPreviewRequired: (audit.brandResults || [])
        .filter((b) => b.classification === "founder_preview_required")
        .map((b) => b.slug),
    },
    forbiddenWrites: {
      companyValidated: false,
      companyValidationDate: false,
      sourceLibraryStatus: false,
      registryApproval: false,
      ownerFacingContent: false,
      imageReassignment: false,
    },
  };
}

export async function applyProfilePreparationVisibilityFix({
  report,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseVisibilityFixApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, flagCheck };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const results = [];
  for (const row of report.plannedRestores || []) {
    if (!row.willRestore || !row.patch) {
      results.push({ brandSlug: row.slug, applied: false, reason: row.classification });
      continue;
    }
    const patch = row.patch;
    if (patch.action === "NOOP" || !Object.keys(patch.fields || {}).length) {
      results.push({
        brandSlug: row.slug,
        applied: false,
        reason: "code_cohort_only_no_airtable_write",
        codeCohort: true,
      });
      continue;
    }
    for (const key of Object.keys(patch.fields || {})) {
      if (FORBIDDEN_WRITE_FIELDS.has(key)) {
        throw new Error(`Refuse forbidden field write: ${key}`);
      }
    }
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(patch.table)}/${patch.recordId}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: patch.fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(json.error?.message || `PATCH failed for ${row.slug}: ${res.status}`);
    }
    results.push({
      brandSlug: row.slug,
      applied: true,
      recordId: patch.recordId,
      fields: Object.keys(patch.fields),
      fieldMapping: patch.fieldMapping,
      sanitizedPayloadPreview: patch.fields,
    });
  }

  return {
    applied: true,
    results,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    contentUntouched: true,
  };
}

export function writeProfilePreparationVisibilityFixReports(
  report,
  { reportsDir = path.join(ROOT, "reports") } = {}
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    "# Brand Explorer — Profile in Preparation Visibility Fix",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    `Dry run: **${report.dryRun !== false}**`,
    "",
    report.note || "",
    "",
    `## Restore cohort (code)`,
    "",
    ...(report.visibilityRestoredReleaseSlugs || []).map((s) => `- \`${s}\``),
    "",
    `## Planned restores: **${report.summary?.restorePlannedCount || 0}**`,
    "",
    "| Brand | Slug | Classification | Will Restore | Patch Fields | Validation |",
    "| --- | --- | --- | --- | --- | --- |",
  ];
  for (const p of report.plannedRestores || []) {
    md.push(
      `| ${p.brandName} | ${p.slug} | ${p.classification} | ${p.willRestore} | ${Object.keys(p.patch?.fields || {}).join("; ") || "—"} | ${p.validation?.pass ? "pass" : (p.validation?.failedChecks || []).join("; ")} |`
    );
  }

  md.push(
    "",
    "## Separation buckets",
    "",
    `- Restored / planned: ${(report.summary?.restoredProfiles || []).join(", ") || "—"}`,
    `- Migration-ready: ${(report.summary?.migrationReadyProfiles || []).join(", ") || "—"}`,
    `- Image remediation: ${(report.summary?.imageRemediationProfiles || []).join(", ") || "—"}`,
    `- Content remediation: ${(report.summary?.contentRemediationProfiles || []).join(", ") || "—"}`,
    `- Founder preview required: ${(report.summary?.founderPreviewRequired || []).join(", ") || "—"}`,
    `- No evidence: ${(report.summary?.noEvidenceProfiles || []).join(", ") || "—"}`,
    "",
    "## Forbidden writes (confirmed untouched)",
    "",
    "- Company Validated",
    "- Company Validation Date",
    "- Source Library status",
    "- Registry approval/status",
    "- Owner-facing content",
    "- Image reassignment",
    ""
  );
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}

export function getVisibilityRestoreSeed(slug) {
  return getLegacySeedBrand(slug);
}
