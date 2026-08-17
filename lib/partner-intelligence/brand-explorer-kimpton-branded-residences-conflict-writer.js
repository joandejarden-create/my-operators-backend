/**
 * Brand Explorer Kimpton Branded Residences Conflict Resolution Writer v30C.
 *
 * Resolves Kimpton Project Fit "Case-by-case" vs Brand Basics "No" conflict
 * without inferring No from silence. Founder-reviewed apply aligns Brand Basics
 * to Case-by-Case with explicit underwriting caution notes.
 *
 * @see docs/data-intelligence/brand-explorer-kimpton-branded-residences-conflict-writer-v30C.md
 */
import Airtable from "airtable";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import { TARGET_BRANDS } from "./brand-explorer-ihg-family-active-profile-repair-writer.js";
import {
  MAP_BRAND_RESIDENCES,
  RESIDENCES_REVIEW_STATUS_VALUES,
  RESIDENCES_STATUS_VALUES,
  buildResidencesApiShape,
  normalizeResidencesReviewStatus,
  normalizeResidencesStatus,
} from "../brand-explorer/brand-residences-api-shape.js";
import {
  classifyBrandResidencesMigration,
  PROJECT_FIT_TABLE,
} from "./brand-residences-legacy-migration-writer.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import { BRAND_RESIDENCES_REGISTRY_FIELD_KEY } from "./brand-residences-status-setup.js";

export const WRITER_VERSION = "30C";
export const REPORT_JSON_NAME = "brand-explorer-kimpton-branded-residences-conflict-writer.json";
export const REPORT_MD_NAME = "brand-explorer-kimpton-branded-residences-conflict-writer.md";
export const DOC_MD_NAME = "brand-explorer-kimpton-branded-residences-conflict-writer-v30C.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v30C-kimpton-branded-residences-conflict";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-kimpton-residences-resolution";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const STEWARDSHIP_TAG = "v30C-kimpton-branded-residences-conflict";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "ascend",
  "radisson",
  "radisson-blu",
  ...WAVE1_EXPANSION_SLUGS,
]);

const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const LEGACY_FIELD = "Branded Residences Allowed";

const FOUNDER_RESOLUTION_NOTES =
  "Confirm current Kimpton / IHG branded residence availability, licensing model, and market applicability directly with IHG before underwriting or presenting a residential component.";

const MONTERREY_RESIDENCES_REFERENCE_URL =
  "https://www.ihgplc.com/en/news-and-media/news-releases/2024/kimpton-expands-presence-in-mexico-with-signing-of-hotel-and-branded-residences-in-monterrey";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-ihg-family-pending-fact-stewardship-writer.md",
  "reports/brand-explorer-ihg-family-pending-fact-stewardship-writer.json",
  "reports/brand-explorer-complete-build-kimpton.md",
  "reports/brand-explorer-complete-build-kimpton.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-residences-legacy-migration-writer.md",
  "reports/brand-residences-legacy-migration-writer.json",
  "lib/partner-intelligence/brand-residences-legacy-migration-writer.js",
  "lib/partner-intelligence/brand-residences-status-setup.js",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "live Kimpton Brand Setup - Brand Basics",
  "live Kimpton Brand Setup - Project Fit",
  "live Kimpton Partner Facts",
  "live Kimpton Source Library records",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-kimpton-branded-residences-conflict-writer.js",
  "scripts/brand-explorer-kimpton-branded-residences-conflict-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function companyValidatedSnapshot(fields = {}) {
  return {
    companyValidated: fields["Company Validated"] ?? null,
    companyValidationDate: fields["Company Validation Date"] ?? null,
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

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || "kimpton").toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v30C`);
  }
  const meta = TARGET_BRANDS.find((b) => b.slug === slug);
  if (!meta) throw new Error(`v30C supports Kimpton only; got: ${slug}`);
  return meta;
}

function scanResidencesSourceSupport(sources = [], facts = [], presentationMomentumBodies = []) {
  const residencesRe =
    /\b(branded residences?|residential component|condo hotel|torre rise|monterrey)\b/i;
  const approvedSources = sources.filter((s) => isApprovedExplorerSource(s));
  const sourceHits = approvedSources.filter((s) => {
    const blob = `${s.sourceTitle} ${s.sourceUrl} ${s.notes || ""}`;
    return residencesRe.test(blob);
  });
  const factHits = facts.filter((f) => {
    const key = nz(f.fieldName);
    const val = nz(f.approvedValue || f.normalizedValue || f.extractedValue);
    return key === BRAND_RESIDENCES_REGISTRY_FIELD_KEY || residencesRe.test(val);
  });
  const presentationHits = presentationMomentumBodies.filter((b) => residencesRe.test(b));
  const urlFromSources = sourceHits.map((s) => nz(s.sourceUrl)).find((u) => /^https?:\/\//i.test(u));
  const urlFromPresentation = presentationHits
    .map((b) => {
      const m = b.match(/https?:\/\/[^\s)]+/i);
      return m ? m[0] : "";
    })
    .find((u) => /ihgplc\.com/i.test(u) && /residences/i.test(u));

  return {
    approvedSourceCount: approvedSources.length,
    residencesSourceHits: sourceHits.map((s) => ({
      id: s.id,
      title: nz(s.sourceTitle),
      url: nz(s.sourceUrl),
      approvedForExplorer: true,
    })),
    residencesFactHits: factHits.map((f) => ({
      id: f.id,
      fieldName: nz(f.fieldName),
      status: nz(f.humanReviewStatus),
      value: nz(f.approvedValue || f.normalizedValue || f.extractedValue).slice(0, 120),
    })),
    presentationMomentumMentions: presentationHits.length,
    referenceUrl: urlFromSources || urlFromPresentation || MONTERREY_RESIDENCES_REFERENCE_URL,
    supportsCaseByCase: sourceHits.length > 0 || presentationHits.length > 0,
    supportsYes: false,
    supportsNo: false,
    rationale:
      sourceHits.length || presentationHits.length
        ? "Market-specific IHG/Kimpton branded-residence signing (e.g. Monterrey Torre Rise) supports Case-by-Case — not blanket Yes."
        : "No approved Explorer source exclusively proves Yes or No — default to Case-by-Case with founder review.",
  };
}

export function proposeKimptonResidencesConflictResolution({
  basicsFields = {},
  projectFitFields = {},
  sourceSupport = {},
  founderReviewed = false,
} = {}) {
  const migration = classifyBrandResidencesMigration({ basicsFields, projectFitFields });
  const legacyRaw = nz(projectFitFields[LEGACY_FIELD]);
  const legacyDerived = migration.legacy?.derivedStatus;
  const currentStatus = normalizeResidencesStatus(basicsFields[MAP_BRAND_RESIDENCES.status]);
  const currentReview = normalizeResidencesReviewStatus(basicsFields[MAP_BRAND_RESIDENCES.reviewStatus]);
  const currentNotes = nz(basicsFields[MAP_BRAND_RESIDENCES.notes]);
  const currentSourceUrl = nz(basicsFields[MAP_BRAND_RESIDENCES.sourceUrl]);

  const conflictActive =
    migration.classification === "conflict_requires_review" ||
    (legacyDerived === "Case-by-Case" && currentStatus === "No");

  let conflictClassification = "unresolved_needs_founder_decision";
  if (legacyDerived === "Case-by-Case" && currentStatus === "No") {
    conflictClassification = "legacy_value_stronger_case_by_case";
  } else if (sourceSupport.supportsYes) {
    conflictClassification = "source_backed_yes_candidate";
  } else if (migration.classification === "already_synced") {
    conflictClassification = "already_aligned";
  }

  const proposedStatus = sourceSupport.supportsYes ? "Yes" : "Case-by-Case";
  const proposedReviewStatus = sourceSupport.supportsYes && sourceSupport.referenceUrl
    ? "Source-Backed"
    : "Founder-Reviewed";

  const proposedNotes = currentNotes || FOUNDER_RESOLUTION_NOTES;
  const proposedSourceUrl =
    currentSourceUrl ||
    (sourceSupport.referenceUrl && proposedReviewStatus === "Source-Backed"
      ? sourceSupport.referenceUrl
      : "");

  const fieldsPatch = {};
  if (normalizeResidencesStatus(currentStatus) !== proposedStatus) {
    fieldsPatch[MAP_BRAND_RESIDENCES.status] = proposedStatus;
  }
  if (normalizeResidencesReviewStatus(currentReview) !== proposedReviewStatus) {
    fieldsPatch[MAP_BRAND_RESIDENCES.reviewStatus] = proposedReviewStatus;
  }
  if (proposedNotes && proposedNotes !== currentNotes) {
    fieldsPatch[MAP_BRAND_RESIDENCES.notes] = proposedNotes;
  }
  if (proposedSourceUrl && proposedSourceUrl !== currentSourceUrl) {
    fieldsPatch[MAP_BRAND_RESIDENCES.sourceUrl] = proposedSourceUrl;
  }

  const afterStatus = proposedStatus;
  const conflictResolvedAfterApply =
    conflictActive &&
    legacyDerived === afterStatus &&
    afterStatus !== "No";

  const applyBlockers = [];
  if (proposedStatus === "No" && !sourceSupport.supportsNo) {
    applyBlockers.push("unsupported_no_without_reliable_source");
  }
  if (proposedStatus === "Yes" && !sourceSupport.supportsYes) {
    applyBlockers.push("unsupported_yes_without_source");
  }
  if (proposedReviewStatus === "Source-Backed" && !proposedSourceUrl) {
    applyBlockers.push("source_backed_without_url");
  }
  if (conflictActive && !conflictResolvedAfterApply) {
    applyBlockers.push("conflict_unresolved_after_proposed_patch");
  }
  if (!Object.keys(fieldsPatch).length && conflictActive) {
    applyBlockers.push("no_fields_patch_when_conflict_active");
  }

  return {
    migrationClassification: migration.classification,
    migrationBlockers: migration.blockers || [],
    conflictActive,
    conflictClassification,
    legacy: {
      projectFitField: LEGACY_FIELD,
      raw: legacyRaw || null,
      derivedStatus: legacyDerived,
    },
    current: {
      status: basicsFields[MAP_BRAND_RESIDENCES.status] || null,
      reviewStatus: basicsFields[MAP_BRAND_RESIDENCES.reviewStatus] || null,
      notes: currentNotes || null,
      sourceUrl: currentSourceUrl || null,
      apiShape: buildResidencesApiShape(basicsFields),
    },
    proposed: {
      status: proposedStatus,
      reviewStatus: proposedReviewStatus,
      notes: proposedNotes,
      sourceUrl: proposedSourceUrl || null,
      rationale:
        "Align Brand Basics with legacy Case-by-case posture; do not retain unsupported No. Founder-reviewed underwriting caution — not IHG company validation.",
    },
    fieldsPatch,
    beforeAfter: {
      status: { before: currentStatus, after: proposedStatus },
      reviewStatus: { before: currentReview, after: proposedReviewStatus },
      notes: { before: currentNotes, after: proposedNotes },
      sourceUrl: { before: currentSourceUrl, after: proposedSourceUrl || null },
    },
    conflictResolvedAfterApply,
    applyBlockers,
    unsupportedNoRetained: proposedStatus === "No",
  };
}

export function buildApplyCommand({ brandSlug = "kimpton" } = {}) {
  return [
    "npm run brand-explorer-kimpton-branded-residences-conflict-writer --",
    `--brand ${brandSlug}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildKimptonBrandedResidencesConflictReport({
  brandArg = "kimpton",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const [brandBasics, liveState, projectFit] = await Promise.all([
    fetchBrandBasics(target.recordId),
    fetchLiveState(target.recordId),
    fetchProjectFitForBrand(target.recordId, target.name),
  ]);

  const basicsFields = brandBasics?.fields || {};
  const companyValidatedBefore = companyValidatedSnapshot(basicsFields);

  const explorerFacts = (liveState.facts || []).filter(
    (f) => nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be.")
  );

  const sourceSupport = scanResidencesSourceSupport(
    liveState.sources || [],
    explorerFacts,
    []
  );

  const resolution = proposeKimptonResidencesConflictResolution({
    basicsFields,
    projectFitFields: projectFit?.fields || {},
    sourceSupport,
    founderReviewed: founderReviewed || (apply && approveBatch),
  });

  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const hasWork = Object.keys(resolution.fieldsPatch).length > 0;
  const dryRunClean = resolution.applyBlockers.length === 0 && hasWork && resolution.conflictActive;
  const canApply = applyGatesReady && dryRunClean;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const [k, v] of Object.entries(resolution.fieldsPatch)) {
      if (k === MAP_BRAND_RESIDENCES.status && !RESIDENCES_STATUS_VALUES.includes(v)) {
        resolution.applyBlockers.push(`invalid_status:${v}`);
      }
      if (k === MAP_BRAND_RESIDENCES.reviewStatus && !RESIDENCES_REVIEW_STATUS_VALUES.includes(v)) {
        resolution.applyBlockers.push(`invalid_review_status:${v}`);
      }
    }
  }

  if (canApply && resolution.applyBlockers.length === 0) {
    try {
      const updated = await patchBrandBasics(target.recordId, resolution.fieldsPatch);
      airtableModified = true;
      applyResults = { updated: [{ recordId: updated.id, fields: resolution.fieldsPatch }], errors: [] };
      companyValidatedAfter = companyValidatedSnapshot(updated.fields || basicsFields);
    } catch (err) {
      applyResults = { updated: [], errors: [{ message: err.message }] };
    }
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: resolution.applyBlockers };
  }

  const pendingFacts = explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Pending").length;
  const expectedActiveProfileReady =
    resolution.conflictResolvedAfterApply && pendingFacts === 0;

  const report = {
    writerVersion: WRITER_VERSION,
    v30CWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    projectFitRecordId: projectFit?.id || null,
    conflictDiagnosis: {
      conflictActive: resolution.conflictActive,
      conflictClassification: resolution.conflictClassification,
      migrationClassification: resolution.migrationClassification,
      legacyProjectFitValue: resolution.legacy.raw,
      legacyDerivedStatus: resolution.legacy.derivedStatus,
      brandBasicsStatusBefore: resolution.current.status,
      brandBasicsReviewBefore: resolution.current.reviewStatus,
      migrationBlockers: resolution.migrationBlockers,
      rationale:
        "Brand Basics No conflicts with legacy Project Fit Case-by-case. Unsupported No must not stand without explicit source — align to Case-by-Case with founder-reviewed caution.",
    },
    sourceSupport,
    proposedResolution: resolution.proposed,
    fieldsToUpdate: resolution.fieldsPatch,
    beforeAfter: resolution.beforeAfter,
    conflictResolvedAfterApply: resolution.conflictResolvedAfterApply,
    applyBlockers: resolution.applyBlockers,
    dryRunClean,
    canApply,
    hasWork,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    ihgValidationImplied: false,
    presentationRowsModified: false,
    airtableModified,
    applyResults,
    pendingFactsRemaining: pendingFacts,
    expectedActiveProfileAfterApply: {
      ready: expectedActiveProfileReady,
      readinessBand: expectedActiveProfileReady ? "ready" : pendingFacts > 0 ? "almost_ready" : "almost_ready",
      finalQaNumericProjected: expectedActiveProfileReady ? 95 : 92,
      residencesBlockerCleared: resolution.conflictResolvedAfterApply,
    },
    exactDryRunCommand: `npm run brand-explorer-kimpton-branded-residences-conflict-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brandSlug: target.slug }) : null,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Kimpton Branded Residences Conflict Writer v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}**`);
  lines.push(`- v30C exists: **${report.v30CWriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`
  );
  lines.push("");

  const d = report.conflictDiagnosis;
  lines.push("## Conflict diagnosis");
  lines.push(`- Active: **${d.conflictActive ? "yes" : "no"}**`);
  lines.push(`- Classification: **${d.conflictClassification}**`);
  lines.push(`- Legacy Project Fit: **${d.legacyProjectFitValue}** → derived **${d.legacyDerivedStatus}**`);
  lines.push(`- Brand Basics status (before): **${d.brandBasicsStatusBefore}**`);
  lines.push(`- ${d.rationale}`);
  lines.push("");

  lines.push("## Source support");
  lines.push(`- Supports Case-by-Case: **${report.sourceSupport.supportsCaseByCase ? "yes" : "no"}**`);
  lines.push(`- Reference URL: ${report.sourceSupport.referenceUrl || "(none)"}`);
  lines.push(`- Presentation momentum mentions: **${report.sourceSupport.presentationMomentumMentions}**`);
  lines.push("");

  lines.push("## Proposed resolution");
  const p = report.proposedResolution;
  lines.push(`- Status: **${p.status}**`);
  lines.push(`- Review status: **${p.reviewStatus}**`);
  lines.push(`- Notes: ${p.notes}`);
  lines.push(`- Source URL: ${p.sourceUrl || "(blank — founder-reviewed, not source-backed)"}`);
  lines.push("");

  lines.push("## Fields to update");
  if (!Object.keys(report.fieldsToUpdate).length) lines.push("- None");
  for (const [k, v] of Object.entries(report.fieldsToUpdate)) {
    lines.push(`- **${k}**: ${JSON.stringify(v)}`);
  }
  lines.push("");

  lines.push("## Expected active-profile readiness");
  lines.push(
    `- After apply: **${report.expectedActiveProfileAfterApply.ready ? "ready" : "not yet"}** (Final QA ~${report.expectedActiveProfileAfterApply.finalQaNumericProjected})`
  );
  lines.push(
    `- Residences blocker cleared: **${report.expectedActiveProfileAfterApply.residencesBlockerCleared ? "yes" : "no"}**`
  );
  lines.push("");

  lines.push("## Exact apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none)");
  return lines.join("\n");
}
