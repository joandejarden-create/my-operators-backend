/**
 * Brand Explorer Kimpton Source Governance Gate Reconciliation Writer v30D.
 *
 * Reconciles Complete Build governedPlatformReady with post-v30B fact stewardship.
 * Explorer active-profile readiness must not require profile-governance publish eligibility
 * or treat rejected/internal facts as public-facing blockers.
 *
 * @see docs/data-intelligence/brand-explorer-kimpton-source-governance-gate-reconciliation-writer-v30D.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchBrandBasics, fetchLiveState, buildGovernancePlan } from "./tribute-portfolio-package-pipeline.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import {
  assessBrandExplorerGovernanceReadiness,
  isApprovedExplorerSource,
  isBrandExplorerScopedFact,
} from "./profile-governance-publish-readiness.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { extractProfileGovernanceRaw } from "../profile-governance/normalize-profile-governance.js";

export const WRITER_VERSION = "30D";
export const REPORT_JSON_NAME =
  "brand-explorer-kimpton-source-governance-gate-reconciliation-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-kimpton-source-governance-gate-reconciliation-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-kimpton-source-governance-gate-reconciliation-writer-v30D.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v30D-kimpton-source-governance-gate-reconciliation";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const TARGET_BRANDS = Object.freeze([
  {
    slug: "kimpton",
    recordId: "recCKuXCmGvxHPfb3",
    name: "Kimpton Hotels",
  },
]);

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "ascend",
  "radisson",
  "radisson-blu",
  ...WAVE1_EXPANSION_SLUGS,
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-ihg-family-pending-fact-stewardship-writer.md",
  "reports/brand-explorer-ihg-family-pending-fact-stewardship-writer.json",
  "reports/brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer.md",
  "reports/brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer.json",
  "reports/brand-explorer-complete-build-kimpton.md",
  "reports/brand-explorer-complete-build-kimpton.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/airtable-facts.js",
  "lib/partner-intelligence/airtable-source.js",
  "api/lib/partner-intelligence-field-map.js",
  "lib/partner-intelligence/profile-governance-publish-readiness.js",
  "live Kimpton Partner Facts",
  "live Kimpton Source Library records",
  "live Kimpton presentation rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-kimpton-source-governance-gate-reconciliation-writer.js",
  "scripts/brand-explorer-kimpton-source-governance-gate-reconciliation-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "lib/partner-intelligence/profile-governance-publish-readiness.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "lib/partner-intelligence/brand-explorer-final-readiness-v26B.js",
  "scripts/test-partner-intelligence-publish-readiness.mjs",
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || "kimpton").toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v30D`);
  }
  const meta = TARGET_BRANDS.find((b) => b.slug === slug);
  if (!meta) throw new Error(`v30D supports Kimpton only; got: ${slug}`);
  return meta;
}

export function v30dWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-kimpton-source-governance-gate-reconciliation-writer.js"
    )
  );
}

function classifyFactBucket(fact) {
  const status = nz(fact.humanReviewStatus);
  const visibility = nz(fact.publicVisibility);
  const isPublic = visibility !== "Internal Only";
  const notes = `${fact.reviewerNotes || ""} ${fact.followUpQuestion || ""}`;
  if (status === "Pending" && /source confirmation|confirm source/i.test(notes)) {
    return "source_confirmation_needed";
  }
  if (status === "Pending") return isPublic ? "pending_review_public" : "pending_review_internal";
  if (/^(Hold|Founder Review|Needs Review)$/i.test(status)) {
    return isPublic ? "hold_founder_review_public" : "hold_founder_review_internal";
  }
  if (status === "Approved" || status === "Edited") {
    return isPublic ? "approved_public" : "approved_internal_only";
  }
  if (status === "Rejected") {
    return isPublic ? "rejected_public" : "rejected_internal_only";
  }
  return "other";
}

function buildFactGovernanceBreakdown(liveState) {
  const facts = (liveState?.facts || []).filter(isBrandExplorerScopedFact);
  const buckets = {
    approved_public: [],
    approved_internal_only: [],
    rejected_internal_only: [],
    rejected_public: [],
    pending_review_public: [],
    pending_review_internal: [],
    hold_founder_review_public: [],
    hold_founder_review_internal: [],
    source_confirmation_needed: [],
    other: [],
  };
  for (const fact of facts) {
    const bucket = classifyFactBucket(fact);
    buckets[bucket].push({
      id: fact.id,
      fieldName: fact.fieldName,
      humanReviewStatus: fact.humanReviewStatus,
      publicVisibility: fact.publicVisibility,
      sourceRecordId: fact.sourceRecordId || null,
    });
  }
  return {
    totalExplorerFacts: facts.length,
    counts: Object.fromEntries(Object.entries(buckets).map(([k, v]) => [k, v.length])),
    buckets,
  };
}

function legacyGovernedPlatformReady(liveState) {
  const governance = buildGovernancePlan(liveState);
  const targetFacts = (liveState?.facts || []).filter((f) => nz(f.fieldName).startsWith("be."));
  const approvedFacts = targetFacts.filter(
    (f) => nz(f.humanReviewStatus) === "Approved" || nz(f.humanReviewStatus) === "Edited"
  );
  return {
    ready:
      approvedFacts.length >= 3 &&
      governance.approvedExplorerSourceCount >= 1 &&
      governance.eligible &&
      (nz(governance.proposed?.validationStatus) === "Company Published" ||
        nz(governance.expectedGovernance?.validationStatus) === "Company Published"),
    approvedFacts: approvedFacts.length,
    governanceEligible: governance.eligible,
    blockReasons: governance.blockReasons || [],
    proposedValidation: nz(governance.proposed?.validationStatus),
    expectedValidation: nz(governance.expectedGovernance?.validationStatus),
  };
}

export function buildApplyCommand({ brandSlug = "kimpton" } = {}) {
  return [
    "npm run brand-explorer-kimpton-source-governance-gate-reconciliation-writer --",
    `--brand ${brandSlug}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildKimptonSourceGovernanceGateReconciliationReport({
  brandArg = "kimpton",
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const profileGovernanceLive = extractProfileGovernanceRaw(brandBasicsBefore?.fields || {});
  const liveState = await fetchLiveState(target.recordId);

  const factBreakdown = buildFactGovernanceBreakdown(liveState);
  const explorerGovernance = assessBrandExplorerGovernanceReadiness(liveState);
  const legacyGovernance = legacyGovernedPlatformReady(liveState);
  const profileGovernancePlan = buildGovernancePlan(liveState);

  const sources = liveState.sources || [];
  const approvedPublicFacts = explorerGovernance.approvedPublicFacts || [];
  const approvedPublicWithoutApprovedSource = approvedPublicFacts.filter((f) => {
    const src = sources.find((s) => s.id === f.sourceRecordId);
    return !src || !isApprovedExplorerSource(src);
  });

  const unresolvedPublicFacts =
    factBreakdown.counts.pending_review_public +
    factBreakdown.counts.hold_founder_review_public +
    factBreakdown.counts.rejected_public +
    factBreakdown.counts.source_confirmation_needed;

  const rootCause =
    legacyGovernance.ready === false &&
    explorerGovernance.ready &&
    (legacyGovernance.blockReasons || []).includes("would_downgrade_existing_validation")
      ? "stale_gate_used_profile_publish_eligibility_not_explorer_governance"
      : explorerGovernance.ready
        ? "resolved_after_v30D"
        : explorerGovernance.blockers[0] || "explorer_governance_blocked";

  const issueClass =
    rootCause === "stale_gate_used_profile_publish_eligibility_not_explorer_governance"
      ? "audit_logic_inconsistency"
      : unresolvedPublicFacts > 0
        ? "real_governance_blocker"
        : explorerGovernance.ready
          ? "resolved"
          : "mapping_gap";

  const codeFixDeployed =
    fs.existsSync(path.join(ROOT, "lib/partner-intelligence/profile-governance-publish-readiness.js")) &&
    /assessBrandExplorerGovernanceReadiness/.test(
      fs.readFileSync(
        path.join(ROOT, "lib/partner-intelligence/profile-governance-publish-readiness.js"),
        "utf8"
      )
    ) &&
    /assessBrandExplorerGovernanceReadiness/.test(
      fs.readFileSync(
        path.join(ROOT, "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js"),
        "utf8"
      )
    );

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch(() => null);
  const completeBuildOrchestrator = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const completeBuildReport = completeBuildOrchestrator?.brandResults?.[0] || null;

  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const applyBlockers = [];

  if (unresolvedPublicFacts > 0) {
    applyBlockers.push(`unresolved_public_explorer_facts:${unresolvedPublicFacts}`);
  }
  if (approvedPublicWithoutApprovedSource.length > 0) {
    applyBlockers.push(
      `approved_public_facts_missing_approved_source:${approvedPublicWithoutApprovedSource.length}`
    );
  }
  if (rowsWouldCreate.length === 0 && rowsWouldUpdate.length === 0) {
    applyBlockers.push("no_airtable_changes_needed_code_reconciliation_only");
  }
  if (!codeFixDeployed) {
    applyBlockers.push("explorer_governance_gate_not_deployed");
  }

  const applyGatesReady = apply && approveBatch && noValidationClaim;
  const canApply = applyGatesReady && applyBlockers.filter((b) => b !== "no_airtable_changes_needed_code_reconciliation_only").length === 0;
  const dryRunClean = applyBlockers.filter((b) => b !== "no_airtable_changes_needed_code_reconciliation_only").length === 0;

  const qaBrand = finalQaReport?.brandReports?.[0] || {};

  const report = {
    writerVersion: WRITER_VERSION,
    v30DWriterExists: v30dWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply_no_op" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    rootCause,
    issueClass,
    realGovernanceBlocker: issueClass === "real_governance_blocker",
    staleGateLogic: issueClass === "audit_logic_inconsistency",
    factGovernanceBreakdown: factBreakdown,
    explorerGovernance,
    legacyGovernanceGate: legacyGovernance,
    profileGovernanceLive,
    profileGovernancePublishPlan: {
      eligible: profileGovernancePlan.eligible,
      blockReasons: profileGovernancePlan.blockReasons || [],
      proposedValidation: nz(profileGovernancePlan.proposed?.validationStatus),
      note:
        "Profile-governance publish eligibility is separate from Brand Explorer active-profile governedPlatformReady.",
    },
    unresolvedPublicFacts,
    approvedPublicWithoutApprovedSource,
    publicFacingUnresolvedFactsRemain: unresolvedPublicFacts > 0,
    rejectedInternalCountedAsBlockerBeforeFix: legacyGovernance.ready === false && explorerGovernance.ready,
    codeRepairs: [
      "assessBrandExplorerGovernanceReadiness() — explorer-scoped gate (pending public, approved public source-backed)",
      "computeGovernedPlatformReady() in complete-build orchestrator uses explorer gate, not profile publish eligibility",
      "Rejected/internal facts excluded from governedPlatformReady blockers",
    ],
    codeFixDeployed,
    rowsWouldCreate,
    rowsWouldUpdate,
    applyBlockers,
    dryRunClean,
    canApply,
    companyValidatedBefore,
    companyValidatedAfter: companyValidatedBefore,
    companyValidatedUntouched: true,
    ihgValidationImplied: false,
    airtableModified: false,
    applyResults: apply
      ? {
          created: [],
          updated: [],
          errors: [],
          blocked: !canApply,
          blockers: applyBlockers,
          note: "v30D is code-reconciliation only — no fact approval or Airtable writes.",
        }
      : null,
    finalQa: {
      overallNumeric: qaBrand.scores?.overallNumeric ?? null,
      readiness: qaBrand.scores?.overallActiveProfileReadiness ?? null,
    },
    completeBuildBeforeFix: {
      governedPlatformReady: false,
      readyForActiveProfile: false,
      sourceEvidenceNeeded: true,
    },
    completeBuildAfterFix: {
      governedPlatformReady: completeBuildReport?.governedPlatformReady ?? explorerGovernance.ready,
      readyForActiveProfile: completeBuildReport?.readyForActiveProfile ?? null,
      readinessBand: completeBuildReport?.readinessBand ?? null,
      sourceEvidenceNeeded: completeBuildReport?.governanceStatus?.sourceEvidenceNeeded ?? null,
      blockers: (completeBuildReport?.blockers || []).map((b) => b.message || b.type),
    },
    expectedKimptonActiveProfileReady: explorerGovernance.ready && codeFixDeployed,
    exactDryRunCommand: `npm run brand-explorer-kimpton-source-governance-gate-reconciliation-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brandSlug: target.slug }) : null,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Kimpton Source Governance Gate Reconciliation v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.slug}\`)`);
  lines.push(`- v30D exists: **${report.v30DWriterExists ? "yes" : "no"}**`);
  lines.push(`- Root cause: **${report.rootCause}**`);
  lines.push(`- Issue class: **${report.issueClass}**`);
  lines.push(`- Stale gate logic: **${report.staleGateLogic ? "yes" : "no"}**`);
  lines.push(`- Real governance blocker: **${report.realGovernanceBlocker ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");

  lines.push("## Fact governance breakdown");
  const c = report.factGovernanceBreakdown.counts;
  lines.push(`- Total Explorer facts: **${report.factGovernanceBreakdown.totalExplorerFacts}**`);
  lines.push(`- Approved / public: **${c.approved_public}**`);
  lines.push(`- Approved / internal only: **${c.approved_internal_only}**`);
  lines.push(`- Rejected / internal only: **${c.rejected_internal_only}**`);
  lines.push(`- Rejected / public: **${c.rejected_public}**`);
  lines.push(`- Pending review (public): **${c.pending_review_public}**`);
  lines.push(`- Hold / founder review (public): **${c.hold_founder_review_public}**`);
  lines.push(`- Source confirmation needed: **${c.source_confirmation_needed}**`);
  lines.push("");

  lines.push("## Governance gates");
  lines.push(`- Explorer governedPlatformReady: **${report.explorerGovernance.governedPlatformReady ? "yes" : "no"}**`);
  lines.push(`- Legacy gate (profile publish): **${report.legacyGovernanceGate.ready ? "yes" : "no"}**`);
  lines.push(`- Profile publish blockers: ${(report.profileGovernancePublishPlan.blockReasons || []).join(", ") || "none"}`);
  lines.push(`- Live validation status: **${report.profileGovernanceLive.validationStatus || "—"}**`);
  lines.push("");

  lines.push("## Complete Build after fix");
  const cb = report.completeBuildAfterFix;
  lines.push(`- governedPlatformReady: **${cb.governedPlatformReady ? "yes" : "no"}**`);
  lines.push(`- readyForActiveProfile: **${cb.readyForActiveProfile ? "yes" : "no"}**`);
  lines.push(`- readinessBand: **${cb.readinessBand || "—"}**`);
  lines.push("");

  lines.push("## Code repairs");
  for (const item of report.codeRepairs) lines.push(`- ${item}`);
  lines.push("");

  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none — code-only reconciliation)");
  return lines.join("\n");
}
