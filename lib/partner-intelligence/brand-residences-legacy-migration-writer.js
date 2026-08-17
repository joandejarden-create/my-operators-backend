/**
 * Branded Residences Legacy Migration + Sync Writer.
 *
 * Migrates legacy Project Fit "Branded Residences Allowed" into first-class
 * Brand Basics residence fields without inferring No from silence.
 *
 * @see docs/data-intelligence/brand-residences-legacy-migration-writer.md
 */
import Airtable from "airtable";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import {
  MAP_BRAND_RESIDENCES,
  DEFAULT_RESIDENCES_STATUS,
  DEFAULT_RESIDENCES_REVIEW_STATUS,
  RESIDENCES_STATUS_VALUES,
  RESIDENCES_REVIEW_STATUS_VALUES,
  normalizeResidencesStatus,
  normalizeResidencesReviewStatus,
  buildResidencesApiShape,
} from "./brand-residences-status-setup.js";

export const WRITER_VERSION = "1";
export const REPORT_JSON_NAME = "brand-residences-legacy-migration-writer.json";
export const REPORT_MD_NAME = "brand-residences-legacy-migration-writer.md";
export const DOC_MD_NAME = "brand-residences-legacy-migration-writer.md";

export const APPLY_FLAG = "--approve-brand-residences-legacy-migration";
export const APPLY_FLAG_INFERENCE = "--confirm-no-unsupported-residences-inference";

export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
export const PROJECT_FIT_TABLE = "Brand Setup - Project Fit";
export const LEGACY_FIELD = "Branded Residences Allowed";

const CLASSIFICATIONS = Object.freeze({
  ALREADY_SYNCED: "already_synced",
  MIGRATE_FROM_LEGACY: "migrate_from_legacy",
  CONFLICT_REQUIRES_REVIEW: "conflict_requires_review",
  NO_DATA_DEFAULT_NOT_CONFIRMED: "no_data_default_not_confirmed",
  SOURCE_BACKED_NEW_VALUE_PRESERVED: "source_backed_new_value_preserved",
});

const SILENT_LEGACY_VALUES = new Set([
  "",
  "not measured",
  "n/a",
  "na",
  "—",
  "-",
  "unknown",
  "unclear",
]);

const CONDITIONAL_RE = /\b(case[- ]by[- ]case|conditional|subject to approval|selected markets?|special licensing|approval required)\b/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function resolveBrandTarget(brandArg) {
  const normalized = nz(brandArg).toLowerCase();
  const bySlug = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === normalized);
  if (bySlug) return bySlug;
  const byId = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.recordId === brandArg);
  if (byId) return byId;
  return { slug: normalized || "unknown", recordId: nz(brandArg), name: nz(brandArg) };
}

function companyValidatedSnapshot(fields = {}) {
  return {
    companyValidated: fields["Company Validated"] ?? null,
    companyValidationDate: fields["Company Validation Date"] ?? null,
  };
}

function normalizeLegacyAllowed(raw) {
  const v = nz(raw);
  if (!v || SILENT_LEGACY_VALUES.has(v.toLowerCase())) return null;
  if (/^yes$/i.test(v)) return "Yes";
  if (/^no$/i.test(v)) return "No";
  if (/case/i.test(v) || /conditional/i.test(v)) return "Case-by-Case";
  return null;
}

function legacyToNewStatus(legacyStatus, contextText = "") {
  if (!legacyStatus) return null;
  if (legacyStatus === "Case-by-Case") return "Case-by-Case";
  if (legacyStatus === "No") return "No";
  if (legacyStatus === "Yes") {
    if (CONDITIONAL_RE.test(contextText)) return "Case-by-Case";
    return "Yes";
  }
  return null;
}

function isSourceBackedNew(fields) {
  const review = normalizeResidencesReviewStatus(fields[MAP_BRAND_RESIDENCES.reviewStatus]);
  const sourceUrl = nz(fields[MAP_BRAND_RESIDENCES.sourceUrl]);
  return review === "Source-Backed" || (sourceUrl && /^https?:\/\//i.test(sourceUrl));
}

function newStatusIsPopulated(fields) {
  const raw = nz(fields[MAP_BRAND_RESIDENCES.status]);
  if (!raw) return false;
  const normalized = normalizeResidencesStatus(raw);
  return normalized !== DEFAULT_RESIDENCES_STATUS || raw !== "";
}

function statusesConflict(newStatus, legacyDerived) {
  if (!newStatusIsPopulated({ [MAP_BRAND_RESIDENCES.status]: newStatus })) return false;
  if (!legacyDerived) return false;
  return normalizeResidencesStatus(newStatus) !== legacyDerived;
}

function proposeReviewStatus({ proposedStatus, sourceUrl, fromLegacy, existingReview }) {
  const existing = normalizeResidencesReviewStatus(existingReview);
  if (existing === "Source-Backed" || existing === "Founder-Reviewed") return existing;
  if (nz(sourceUrl)) return "Source-Backed";
  if (fromLegacy && proposedStatus && proposedStatus !== DEFAULT_RESIDENCES_STATUS) {
    return "Founder-Reviewed";
  }
  if (proposedStatus === DEFAULT_RESIDENCES_STATUS) return DEFAULT_RESIDENCES_REVIEW_STATUS;
  return "Needs Review";
}

function buildMigrationNotes({ existingNotes, legacyRaw, fromLegacy }) {
  const existing = nz(existingNotes);
  if (existing) return existing;
  if (!fromLegacy || !legacyRaw) return "";
  return `Migrated from legacy Project Fit field "${LEGACY_FIELD}" (${legacyRaw}). Confirm against current brand materials before underwriting.`;
}

export function classifyBrandResidencesMigration({ basicsFields = {}, projectFitFields = {} } = {}) {
  const legacyRaw = nz(projectFitFields[LEGACY_FIELD]);
  const legacyNormalized = normalizeLegacyAllowed(legacyRaw);
  const contextText = [
    projectFitFields["Anything else about your commercial 'sweet spot' we should know?"],
    projectFitFields["Ideal Projects Additional Notes"],
    projectFitFields["Other (Text) - Owner Non-Negotiables"],
  ]
    .map(nz)
    .filter(Boolean)
    .join("\n");

  const currentStatusRaw = nz(basicsFields[MAP_BRAND_RESIDENCES.status]);
  const currentStatus = currentStatusRaw ? normalizeResidencesStatus(currentStatusRaw) : null;
  const currentReview = normalizeResidencesReviewStatus(basicsFields[MAP_BRAND_RESIDENCES.reviewStatus]);
  const currentSourceUrl = nz(basicsFields[MAP_BRAND_RESIDENCES.sourceUrl]);
  const currentNotes = nz(basicsFields[MAP_BRAND_RESIDENCES.notes]);
  const legacyDerived = legacyToNewStatus(legacyNormalized, contextText);
  const sourceBacked = isSourceBackedNew(basicsFields);

  let classification = CLASSIFICATIONS.NO_DATA_DEFAULT_NOT_CONFIRMED;
  let proposedStatus = DEFAULT_RESIDENCES_STATUS;
  let proposedReviewStatus = DEFAULT_RESIDENCES_REVIEW_STATUS;
  let proposedNotes = currentNotes;
  let proposedSourceUrl = currentSourceUrl || null;
  let action = "report_only";
  let blockers = [];

  if (sourceBacked && newStatusIsPopulated(basicsFields)) {
    if (legacyDerived && statusesConflict(currentStatusRaw, legacyDerived)) {
      classification = CLASSIFICATIONS.CONFLICT_REQUIRES_REVIEW;
      blockers.push("source_backed_new_conflicts_with_legacy");
      action = "block";
    } else {
      classification = CLASSIFICATIONS.SOURCE_BACKED_NEW_VALUE_PRESERVED;
      proposedStatus = currentStatus || DEFAULT_RESIDENCES_STATUS;
      proposedReviewStatus = currentReview;
      action = "preserve";
    }
  } else if (newStatusIsPopulated(basicsFields)) {
    if (legacyDerived && statusesConflict(currentStatusRaw, legacyDerived)) {
      classification = CLASSIFICATIONS.CONFLICT_REQUIRES_REVIEW;
      blockers.push("new_value_conflicts_with_legacy");
      action = "block";
    } else {
      classification = CLASSIFICATIONS.ALREADY_SYNCED;
      proposedStatus = currentStatus || DEFAULT_RESIDENCES_STATUS;
      proposedReviewStatus = currentReview;
      action = "none";
    }
  } else if (legacyDerived) {
    classification = CLASSIFICATIONS.MIGRATE_FROM_LEGACY;
    proposedStatus = legacyDerived;
    proposedReviewStatus = proposeReviewStatus({
      proposedStatus,
      sourceUrl: currentSourceUrl,
      fromLegacy: true,
      existingReview: basicsFields[MAP_BRAND_RESIDENCES.reviewStatus],
    });
    proposedNotes = buildMigrationNotes({
      existingNotes: currentNotes,
      legacyRaw,
      fromLegacy: true,
    });
    action = "update";
  } else {
    classification = CLASSIFICATIONS.NO_DATA_DEFAULT_NOT_CONFIRMED;
    proposedStatus = DEFAULT_RESIDENCES_STATUS;
    proposedReviewStatus = DEFAULT_RESIDENCES_REVIEW_STATUS;
    action = "report_only";
  }

  if (proposedStatus === "No" && !legacyNormalized) {
    blockers.push("unsupported_no_without_explicit_legacy");
  }
  if (proposedStatus === "Yes" && !["Source-Backed", "Founder-Reviewed"].includes(proposedReviewStatus)) {
    blockers.push("unsupported_yes_without_review");
  }

  const fieldsPatch = {};
  if (action === "update" || (action === "report_only" && classification === CLASSIFICATIONS.NO_DATA_DEFAULT_NOT_CONFIRMED)) {
    if (!currentStatusRaw && proposedStatus) fieldsPatch[MAP_BRAND_RESIDENCES.status] = proposedStatus;
    if (!nz(basicsFields[MAP_BRAND_RESIDENCES.reviewStatus]) && proposedReviewStatus) {
      fieldsPatch[MAP_BRAND_RESIDENCES.reviewStatus] = proposedReviewStatus;
    }
    if (!currentNotes && proposedNotes) fieldsPatch[MAP_BRAND_RESIDENCES.notes] = proposedNotes;
    if (!currentSourceUrl && proposedSourceUrl) fieldsPatch[MAP_BRAND_RESIDENCES.sourceUrl] = proposedSourceUrl;
  }

  return {
    classification,
    action,
    blockers,
    legacy: {
      raw: legacyRaw || null,
      normalized: legacyNormalized,
      derivedStatus: legacyDerived,
    },
    current: {
      status: currentStatusRaw || null,
      reviewStatus: nz(basicsFields[MAP_BRAND_RESIDENCES.reviewStatus]) || null,
      notes: currentNotes || null,
      sourceUrl: currentSourceUrl || null,
      apiShape: buildResidencesApiShape(basicsFields),
    },
    proposed: {
      status: proposedStatus,
      reviewStatus: proposedReviewStatus,
      notes: proposedNotes || null,
      sourceUrl: proposedSourceUrl,
    },
    fieldsPatch,
    needsUpdate:
      (action === "update" || classification === CLASSIFICATIONS.NO_DATA_DEFAULT_NOT_CONFIRMED) &&
      Object.keys(fieldsPatch).length > 0,
  };
}

async function getBase() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required");
  return new Airtable({ apiKey }).base(baseId);
}

async function fetchProjectFitForBrand(recordId, brandName) {
  const base = await getBase();
  const linkFieldNames = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];
  for (const linkField of linkFieldNames) {
    try {
      const formula = `FIND("${recordId}", ARRAYJOIN({${linkField}})) > 0`;
      const records = await base(PROJECT_FIT_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).all();
      if (records.length > 0) return { id: records[0].id, fields: records[0].fields || {} };
    } catch {
      continue;
    }
  }
  if (brandName) {
    const escaped = brandName.replace(/"/g, '\\"');
    const records = await base(PROJECT_FIT_TABLE)
      .select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 })
      .all();
    if (records.length > 0) return { id: records[0].id, fields: records[0].fields || {} };
  }
  return null;
}

async function patchBrandBasics(recordId, fields) {
  const base = await getBase();
  const rec = await base(BRAND_BASICS_TABLE).update(recordId, fields, { typecast: true });
  return { id: rec.id, fields: rec.fields || {} };
}

export async function buildBrandResidencesLegacyMigrationWriterReport(options = {}) {
  const allActive = Boolean(options.allActive);
  const brandArg = nz(options.brandIdOrName || "tribute-portfolio");
  const apply = Boolean(options.apply);
  const approveBatch = Boolean(options.approveBatch);
  const confirmNoUnsupportedInference = Boolean(options.confirmNoUnsupportedInference);
  const targets = allActive ? ACTIVE_BRAND_AUDIT_TARGETS : [resolveBrandTarget(brandArg)];

  const brandPlans = [];
  const applyBlockers = [];
  let companyValidatedBefore = null;

  for (const target of targets) {
    const basics = await fetchBrandBasics(target.recordId);
    const basicsFields = basics?.fields || {};
    if (!companyValidatedBefore) companyValidatedBefore = companyValidatedSnapshot(basicsFields);

    const projectFit = await fetchProjectFitForBrand(target.recordId, basics?.name || target.name);
    const plan = classifyBrandResidencesMigration({
      basicsFields,
      projectFitFields: projectFit?.fields || {},
    });

    brandPlans.push({
      brand: {
        slug: target.slug,
        name: basics?.name || target.name,
        recordId: target.recordId,
      },
      projectFitRecordId: projectFit?.id || null,
      ...plan,
    });

    if (plan.blockers.length) {
      applyBlockers.push(...plan.blockers.map((b) => `${target.slug}:${b}`));
    }
    if (plan.classification === CLASSIFICATIONS.CONFLICT_REQUIRES_REVIEW) {
      applyBlockers.push(`${target.slug}:conflict_requires_review`);
    }
  }

  const canApply = apply && approveBatch && confirmNoUnsupportedInference && applyBlockers.length === 0;
  const applyResults = { updated: [], skipped: [], errors: [] };

  if (canApply) {
    for (const plan of brandPlans) {
      if (!plan.needsUpdate || !Object.keys(plan.fieldsPatch).length) {
        applyResults.skipped.push({ recordId: plan.brand.recordId, reason: plan.classification });
        continue;
      }
      for (const [k, v] of Object.entries(plan.fieldsPatch)) {
        if (!RESIDENCES_STATUS_VALUES.includes(v) && k === MAP_BRAND_RESIDENCES.status) {
          applyResults.errors.push({ recordId: plan.brand.recordId, error: `invalid_status:${v}` });
          continue;
        }
        if (!RESIDENCES_REVIEW_STATUS_VALUES.includes(v) && k === MAP_BRAND_RESIDENCES.reviewStatus) {
          applyResults.errors.push({ recordId: plan.brand.recordId, error: `invalid_review_status:${v}` });
          continue;
        }
      }
      try {
        const updated = await patchBrandBasics(plan.brand.recordId, plan.fieldsPatch);
        applyResults.updated.push({
          recordId: plan.brand.recordId,
          fields: plan.fieldsPatch,
          classification: plan.classification,
        });
        const after = companyValidatedSnapshot(updated.fields);
        if (
          JSON.stringify(after) !== JSON.stringify(companyValidatedBefore)
        ) {
          applyBlockers.push(`${plan.brand.slug}:company_validated_changed`);
        }
      } catch (err) {
        applyResults.errors.push({
          recordId: plan.brand.recordId,
          error: err?.message || String(err),
        });
      }
    }
  }

  const tribute = brandPlans.find((p) => p.brand.recordId === "recCvV0PuZOi8c3hC") || brandPlans[0];
  const summary = {
    alreadySynced: brandPlans.filter((p) => p.classification === CLASSIFICATIONS.ALREADY_SYNCED).length,
    migrateFromLegacy: brandPlans.filter((p) => p.classification === CLASSIFICATIONS.MIGRATE_FROM_LEGACY).length,
    conflicts: brandPlans.filter((p) => p.classification === CLASSIFICATIONS.CONFLICT_REQUIRES_REVIEW).length,
    noDataDefault: brandPlans.filter((p) => p.classification === CLASSIFICATIONS.NO_DATA_DEFAULT_NOT_CONFIRMED).length,
    sourceBackedPreserved: brandPlans.filter((p) => p.classification === CLASSIFICATIONS.SOURCE_BACKED_NEW_VALUE_PRESERVED).length,
    wouldUpdate: brandPlans.filter((p) => p.needsUpdate).length,
  };

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply && canApply ? "apply" : "dry-run",
    airtableModified: applyResults.updated.length > 0,
    allActive,
    filesRead: [
      "AGENTS.md",
      "docs/data-intelligence/brand-residences-status-field.md",
      "api/brand-library.js",
      "lib/partner-intelligence/brand-residences-status-setup.js",
      "live Brand Setup - Brand Basics records",
      "live Brand Setup - Project Fit records",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-residences-legacy-migration-writer.js",
      "scripts/brand-residences-legacy-migration-writer.mjs",
      "docs/data-intelligence/brand-residences-legacy-migration-writer.md",
      "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
      "package.json",
    ],
    futureSetupWritesToBrandBasicsOnly: true,
    legacyFieldRetained: LEGACY_FIELD,
    legacyTable: PROJECT_FIT_TABLE,
    newFieldsTable: BRAND_BASICS_TABLE,
    classifications: CLASSIFICATIONS,
    brandPlans,
    summary,
    tributePlan: tribute || null,
    applyGates: {
      apply,
      approveBatch,
      confirmNoUnsupportedInference,
      canApply,
      applyBlockers,
    },
    applyResults: canApply ? applyResults : null,
    companyValidatedUntouched: !applyBlockers.some((b) => b.includes("company_validated_changed")),
    companyValidatedBefore,
    exactApplyCommand:
      "npm run brand-residences-legacy-migration-writer -- --all-active --apply --approve-brand-residences-legacy-migration --confirm-no-unsupported-residences-inference",
  };
}

export function buildBrandResidencesLegacyMigrationWriterMarkdown(report) {
  const lines = [];
  lines.push("# Brand Residences Legacy Migration Writer");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Summary");
  lines.push(`- Already synced: ${report.summary.alreadySynced}`);
  lines.push(`- Migrate from legacy: ${report.summary.migrateFromLegacy}`);
  lines.push(`- Conflicts: ${report.summary.conflicts}`);
  lines.push(`- No data (Not Confirmed): ${report.summary.noDataDefault}`);
  lines.push(`- Source-backed preserved: ${report.summary.sourceBackedPreserved}`);
  lines.push(`- Would update: ${report.summary.wouldUpdate}`);
  lines.push("");
  lines.push("## Brand plans");
  for (const plan of report.brandPlans || []) {
    lines.push(`### ${plan.brand.name}`);
    lines.push(`- Classification: \`${plan.classification}\``);
    lines.push(`- Legacy: ${plan.legacy.raw || "—"} → ${plan.legacy.derivedStatus || "—"}`);
    lines.push(`- Current: ${plan.current.status || "—"}`);
    lines.push(`- Proposed: **${plan.proposed.status}** · review **${plan.proposed.reviewStatus}**`);
    if (plan.blockers.length) lines.push(`- Blockers: ${plan.blockers.join(", ")}`);
    lines.push("");
  }
  lines.push("## Apply command");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  return lines.join("\n");
}
