/**
 * Dealality Intelligence Production Workflow v1 — read-only planning + dry-run orchestration.
 * @see docs/data-intelligence/dealality-intelligence-production-workflow-v1.md
 */
import {
  assessPackageReadiness,
  GOVERNANCE_CHANGE_EQUIVALENT_STABLE,
  isStableGovernanceChangeClass,
} from "./profile-governance-publish-readiness.js";
import {
  buildPackageFromRecords,
  collectPackageBlockerLabels,
  factStatusCounts,
  findPackageInReadinessReport,
  recommendGovernanceFacts,
  buildApplyCommandPreview,
  buildPublishDryRunPreview,
} from "./stewardship-package.js";
import {
  extractProfileGovernanceRaw,
  normalizeProfileGovernance,
} from "../profile-governance/normalize-profile-governance.js";
import { NEVER_PUBLISH_API_KEYS } from "./profile-governance-publish.js";

export const WORKFLOW_VERSION = "v1";
export const REPORT_JSON_NAME = "intelligence-profile-workflow.json";
export const REPORT_MD_NAME = "intelligence-profile-workflow.md";

/** v1 implementation focus */
export const SUPPORTED_ENTITY_TYPES_V1 = ["brand", "operator"];

/** Documented for future expansion — not orchestrated in v1 */
export const FUTURE_ENTITY_TYPES = [
  "owner",
  "capital_partner",
  "advisor",
  "market_anchor",
  "deal_evidence",
];

export const WORKFLOW_STAGES = [
  { id: 0, key: "resolve_entity", label: "Resolve entity" },
  { id: 1, key: "source_discovery", label: "Source discovery" },
  { id: 2, key: "source_capture_register", label: "Source capture / register" },
  { id: 3, key: "source_stewardship", label: "Source stewardship" },
  { id: 4, key: "extraction_dry_run", label: "Extraction dry-run" },
  { id: 5, key: "fact_stewardship", label: "Fact stewardship" },
  { id: 6, key: "governance_publish_dry_run", label: "Governance publish dry-run" },
  { id: 7, key: "governance_apply", label: "Governance apply" },
  { id: 8, key: "platform_usage", label: "Platform usage / verify" },
];

export const COMPLETENESS_THRESHOLDS = {
  minApprovedSources: 1,
  sparseApprovedFacts: 2,
  usefulApprovedFacts: 5,
  highConfidenceMinSubstantiveFacts: 5,
};

/** Company-specific narrow extract scripts — do not remove; workflow delegates when keyed. */
export const NARROW_EXTRACT_BY_ENTITY = {
  "operator:recWPKu5laVZxsvpn": {
    label: "Hotel Equities",
    dryRun: "npm run hotel-equities-extract -- --dry-run",
    apply:
      "npm run hotel-equities-extract -- --apply --approve-hotel-equities-extract",
    planDoc: "docs/data-intelligence/hotel-equities-extraction-plan.md",
  },
  "operator:reciI2tYQBfMoMK9G": {
    label: "GHL Hoteles",
    dryRun: "npm run ghl-hoteles-extract -- --dry-run",
    apply: "npm run ghl-hoteles-extract -- --apply --approve-ghl-hoteles-extract",
    planDoc: "docs/data-intelligence/ghl-hoteles-extraction-plan.md",
  },
  "brand:receQkxgjlezsc1xg": {
    label: "Curio Collection by Hilton",
    dryRun: "npm run curio-clean-reextract -- --dry-run",
    apply: null,
    planDoc: "docs/data-intelligence/curio-clean-reextraction-plan.md",
  },
};

export const NEVER_WORKFLOW_WRITES = [
  "Company Validated",
  "Company Validation Date",
  "BAS / OAS / OCS scoring fields",
  "Deal Readiness outputs",
  "Explorer UI",
  "Airtable schema",
];

export function entityKey(entityType, targetRecId) {
  return `${entityType}:${targetRecId}`;
}

export function isSupportedEntityType(entityType) {
  return SUPPORTED_ENTITY_TYPES_V1.includes(entityType);
}

export function countSources(pkg) {
  const sources = pkg?.sources || [];
  const approvedExplorer = sources.filter((s) => s.approvedForExplorerUse === "Yes");
  const readyStatuses = new Set(["Approved", "Extracted", "Classified"]);
  const publishReady = approvedExplorer.filter((s) => readyStatuses.has(s.status));
  return {
    total: sources.length,
    approvedExplorer: approvedExplorer.length,
    publishReady: publishReady.length,
    captured: sources.filter((s) => s.status === "Captured").length,
    extracted: sources.filter((s) => s.status === "Extracted").length,
  };
}

export function assessCompleteness(pkg, factCounts) {
  const approvedFacts = factCounts?.approved || 0;
  const sourceCounts = countSources(pkg);
  let tier = "empty";
  if (sourceCounts.approvedExplorer >= COMPLETENESS_THRESHOLDS.minApprovedSources) {
    if (approvedFacts >= COMPLETENESS_THRESHOLDS.usefulApprovedFacts) tier = "useful";
    else if (approvedFacts >= COMPLETENESS_THRESHOLDS.sparseApprovedFacts) tier = "sparse";
    else if (approvedFacts >= 1) tier = "minimal";
    else tier = "sources_only";
  } else if (sourceCounts.total > 0) {
    tier = "sources_unapproved";
  }

  return {
    tier,
    approvedSourceCount: sourceCounts.approvedExplorer,
    approvedFactCount: approvedFacts,
    meetsMinimumPackage:
      sourceCounts.approvedExplorer >= COMPLETENESS_THRESHOLDS.minApprovedSources &&
      approvedFacts >= COMPLETENESS_THRESHOLDS.sparseApprovedFacts,
    meetsUsefulPackage:
      sourceCounts.approvedExplorer >= COMPLETENESS_THRESHOLDS.minApprovedSources &&
      approvedFacts >= COMPLETENESS_THRESHOLDS.usefulApprovedFacts,
  };
}

const DOWNGRADE_PROTECTION_BLOCKERS = new Set([
  "would_downgrade_existing_validation",
  "downgrade_blocked",
]);

/**
 * Live Setup governance is stronger than the current PI proposal; publish correctly blocked.
 * Queue should treat as Stage 8 protected — not an active governance-publish blocker.
 */
export function isStrongerLiveGovernancePreserved(changeClass, governanceNormalized, blockers = []) {
  if (changeClass !== "downgrade") return false;
  if (!governanceNormalized?.validationStatus) return false;
  if (!blockers.length) return true;
  return blockers.every((b) => DOWNGRADE_PROTECTION_BLOCKERS.has(b));
}

/**
 * Determine the next actionable workflow stage (0–8).
 * @param {object} input
 */
export function determineWorkflowStage(input) {
  const {
    entityType,
    targetRecId,
    pkg,
    readiness,
    readinessReportEntry,
    factCounts,
    governanceNormalized,
  } = input;

  const sources = pkg?.sources || [];
  const sc = countSources(pkg);
  const approvedFacts = factCounts?.approved || 0;
  const pendingFacts = factCounts?.pendingCandidates ?? factCounts?.pending ?? 0;
  const totalFacts = factCounts?.total || 0;
  const changeClass =
    readinessReportEntry?.changeClass ||
    readiness?.changeClass ||
    readiness?.assessment?.changeClass ||
    null;
  const eligible =
    readinessReportEntry?.eligible ?? readiness?.eligible ?? false;
  const blockers = readiness?.publishScopeBlockers || readiness?.blockReasons || [];

  if (!entityType || !targetRecId) {
    return stageResult(0, "resolve_entity", ["missing_entity_type_or_record_id"]);
  }

  if (!sources.length) {
    return stageResult(1, "source_discovery", ["no_linked_sources"]);
  }

  if (sc.approvedExplorer === 0) {
    const blockersHere = ["no_approved_explorer_sources"];
    if (sc.captured > 0) blockersHere.push("sources_captured_not_stewarded");
    return stageResult(3, "source_stewardship", blockersHere);
  }

  if (totalFacts === 0) {
    return stageResult(4, "extraction_dry_run", ["no_extracted_facts"]);
  }

  if (approvedFacts === 0) {
    const blockersHere = ["no_approved_facts"];
    if (pendingFacts > 0) blockersHere.push("pending_facts_awaiting_review");
    return stageResult(5, "fact_stewardship", blockersHere);
  }

  if (isStableGovernanceChangeClass(changeClass)) {
    return stageResult(8, "platform_usage", []);
  }

  if (
    changeClass === "downgrade" &&
    approvedFacts > 0 &&
    isStrongerLiveGovernancePreserved(changeClass, governanceNormalized, blockers)
  ) {
    return stageResult(8, "platform_usage", ["stronger_live_governance_preserved"]);
  }

  if (eligible && changeClass === "new") {
    return stageResult(7, "governance_apply", ["governance_not_yet_applied"]);
  }

  if (
    approvedFacts > 0 &&
    (changeClass === "new" ||
      (!governanceNormalized?.validationStatus &&
        !governanceNormalized?.displayLabel))
  ) {
    return stageResult(6, "governance_publish_dry_run", blockers);
  }

  return stageResult(6, "governance_publish_dry_run", blockers);
}

function stageResult(id, key, blockers) {
  const meta = WORKFLOW_STAGES.find((s) => s.id === id) || WORKFLOW_STAGES[0];
  return {
    stageId: id,
    stageKey: key,
    stageLabel: meta.label,
    blockers: [...new Set(blockers.filter(Boolean))],
  };
}

export function resolveNarrowExtract(entityType, targetRecId) {
  return NARROW_EXTRACT_BY_ENTITY[entityKey(entityType, targetRecId)] || null;
}

export function buildNextCommands(ctx) {
  const { entityType, targetRecId, stage, pkg, recommendedFactIds = [] } = ctx;
  const et = entityType;
  const id = targetRecId;
  const companyFolder = ctx.targetProfile?.name || "<Company Name>";
  const commands = [];
  const notes = [];

  const stewardDryRun = `npm run steward-partner-intelligence -- --entity-type ${et} --target-rec-id ${id} --dry-run --recompute`;
  const publishDryRun = buildPublishDryRunPreview({ entityType: et, targetRecId: id });
  const audit = "npm run audit-partner-intelligence-publish-readiness";
  const narrow = resolveNarrowExtract(et, id);

  switch (stage.stageKey) {
    case "source_discovery":
      commands.push(
        `npm run partner-reference:search -- --operator "${companyFolder}"`,
        `npm run partner-reference:init-folder -- --company "${companyFolder}" --dry-run`
      );
      notes.push("Verify official URLs before capture; avoid third-party first-pass unless scoped.");
      break;
    case "source_capture_register":
      commands.push(
        `npm run partner-reference:init-folder -- --company "${companyFolder}" --dry-run`,
        `npm run partner-reference:download -- --url "<official-url>" --company "${companyFolder}" --type website-capture --title "<title>" --operator-id ${id} --profile-type Operator --dry-run`
      );
      notes.push("Replace placeholders; use --apply --register only after URL review.");
      break;
    case "source_stewardship":
      commands.push(stewardDryRun);
      if (pkg?.sources?.length) {
        const unapproved = (pkg.sources || [])
          .filter((s) => s.approvedForExplorerUse !== "Yes")
          .map((s) => s.id);
        if (unapproved.length) {
          commands.push(
            buildApplyCommandPreview({
              entityType: et,
              targetRecId: id,
              approveSourceIds: unapproved,
              approveFactIds: [],
            })
          );
          notes.push("Review each source before apply; exclude noisy/wrong-entity sources.");
        }
      }
      break;
    case "extraction_dry_run":
      if (narrow) {
        commands.push(narrow.dryRun);
        notes.push(`Narrow extract: ${narrow.planDoc}`);
      } else {
        commands.push(
          "# No company-specific narrow extract registered — add script or use general extraction path after plan review"
        );
      }
      commands.push(stewardDryRun);
      break;
    case "fact_stewardship":
      commands.push(stewardDryRun);
      if (recommendedFactIds.length) {
        commands.push(
          buildApplyCommandPreview({
            entityType: et,
            targetRecId: id,
            approveSourceIds: [],
            approveFactIds: recommendedFactIds,
          })
        );
      }
      notes.push("Approve only strong facts; leave weak/noisy facts Pending or Rejected.");
      break;
    case "governance_publish_dry_run":
      commands.push(audit, publishDryRun);
      break;
    case "governance_apply":
      commands.push(publishDryRun);
      commands.push(
        `npm run publish-partner-intelligence-profile-governance -- --apply --entity-type ${et} --target-rec-id ${id}`
      );
      notes.push("Apply only after dry-run review; never sets Company Validated.");
      break;
    case "platform_usage":
      commands.push(audit, publishDryRun, stewardDryRun);
      notes.push("Package stable — use for Explorer, alignment snapshots, outreach prep.");
      break;
    default:
      commands.push(stewardDryRun);
  }

  return {
    commands: [...new Set(commands)],
    notes,
    narrowExtract: narrow,
  };
}

export function assertNoCompanyValidatedWritePath() {
  return NEVER_PUBLISH_API_KEYS.includes("companyValidated") &&
    NEVER_PUBLISH_API_KEYS.includes("companyValidationDate");
}

/**
 * Build full workflow plan from loaded records (read-only).
 */
export function buildIntelligenceProfileWorkflowPlan({
  entityType,
  targetRecId,
  targetProfile,
  sources,
  facts,
  published,
  readinessReport,
}) {
  if (!isSupportedEntityType(entityType)) {
    throw new Error(
      `Unsupported entity type "${entityType}". v1 supports: ${SUPPORTED_ENTITY_TYPES_V1.join(", ")}`
    );
  }

  const pkg = buildPackageFromRecords({
    sources,
    facts,
    published: published || [],
    entityType,
    targetRecId,
  });

  const readiness = assessPackageReadiness(pkg, targetProfile);
  const readinessEntry = findPackageInReadinessReport(readinessReport, entityType, targetRecId);
  const factCounts = factStatusCounts(pkg.facts || []);
  const blockerSummary = collectPackageBlockerLabels(pkg, targetProfile);
  const completeness = assessCompleteness(pkg, factCounts);
  const governanceRaw = targetProfile?.fields
    ? extractProfileGovernanceRaw(targetProfile.fields, { entityType })
    : {};
  const governanceNormalized = normalizeProfileGovernance(targetProfile?.fields || {}, {
    entityType,
    sourceTable:
      entityType === "brand" ? "Brand Setup - Brand Basics" : "Operator Setup - Master",
  });

  const recommended = recommendGovernanceFacts(pkg.facts || [], entityType, {
    stewardSourceIds: (pkg.sources || []).map((s) => s.id),
  });

  const stage = determineWorkflowStage({
    entityType,
    targetRecId,
    pkg,
    readiness,
    readinessReportEntry: readinessEntry,
    factCounts,
    governanceNormalized,
  });

  const next = buildNextCommands({
    entityType,
    targetRecId,
    stage,
    pkg,
    targetProfile,
    recommendedFactIds: recommended.map((f) => f.id),
  });

  const sourceCounts = countSources(pkg);

  return {
    workflowVersion: WORKFLOW_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "plan",
    entityType,
    targetRecId,
    entityName: targetProfile?.name || readinessEntry?.entityName || null,
    supportedEntityTypes: SUPPORTED_ENTITY_TYPES_V1,
    futureEntityTypes: FUTURE_ENTITY_TYPES,
    currentStage: stage,
    completeness,
    sourceCounts,
    factCounts,
    governance: {
      live: governanceRaw,
      normalized: governanceNormalized,
      changeClass: readinessEntry?.changeClass || readiness.changeClass || null,
      eligible: readinessEntry?.eligible ?? readiness.eligible,
      expectedChip: readinessEntry?.proposed?.expectedGovernance || null,
    },
    blockers: {
      workflow: stage.blockers,
      publishScope: readiness.publishScopeBlockers || readiness.blockReasons || [],
      labels: blockerSummary.labels,
    },
    package: {
      sourceIds: (pkg.sources || []).map((s) => s.id),
      factIds: (pkg.facts || []).map((f) => f.id),
      approvedExplorerSourceIds: (pkg.sources || [])
        .filter((s) => s.approvedForExplorerUse === "Yes")
        .map((s) => s.id),
      excludedFromPublishScope: readiness.excludedSourceIds || [],
    },
    recommendedFactIds: recommended.map((f) => f.id),
    nextCommands: next.commands,
    nextNotes: next.notes,
    narrowExtract: next.narrowExtract,
    safety: {
      neverWrites: NEVER_WORKFLOW_WRITES,
      companyValidatedWritePathBlocked: assertNoCompanyValidatedWritePath(),
      applyOrchestrationInV1: false,
    },
    readinessSummary: readinessEntry
      ? {
          changeClass: readinessEntry.changeClass,
          eligible: readinessEntry.eligible,
          publishScopeSourceCount: readinessEntry.publishScopeSourceCount,
          publishScopeApprovedFactCount: readinessEntry.publishScopeApprovedFactCount,
        }
      : null,
  };
}

export function buildWorkflowMarkdown(plan) {
  const lines = [
    "# Dealality Intelligence Profile Workflow",
    "",
    `Generated: ${plan.generatedAt}`,
    `Workflow: **${plan.workflowVersion}**`,
    `Entity: ${plan.entityType} — **${plan.entityName || plan.targetRecId}**`,
    `Target record: \`${plan.targetRecId}\``,
    "",
    "## Current stage",
    "",
    `**Stage ${plan.currentStage.stageId}** — ${plan.currentStage.stageLabel} (\`${plan.currentStage.stageKey}\`)`,
    "",
  ];

  if (plan.currentStage.blockers.length) {
    lines.push("Blockers:", "");
    for (const b of plan.currentStage.blockers) lines.push(`- ${b}`);
    lines.push("");
  }

  lines.push(
    "## Package snapshot",
    "",
    `- Sources (linked): ${plan.sourceCounts.total}`,
    `- Approved for Explorer Use: ${plan.sourceCounts.approvedExplorer}`,
    `- Facts (linked): ${plan.factCounts.total}`,
    `- Approved facts: ${plan.factCounts.approved}`,
    `- Pending facts: ${plan.factCounts.pendingCandidates ?? plan.factCounts.pending ?? 0}`,
    `- Completeness tier: **${plan.completeness.tier}**`,
    `- Governance change class: **${plan.governance.changeClass || "—"}**`,
    `- Publish eligible: **${plan.governance.eligible ? "yes" : "no"}**`,
    ""
  );

  if (plan.governance.expectedChip) {
    lines.push(
      "## Expected Explorer chip",
      "",
      `- **${plan.governance.expectedChip.displayLabel || "—"}**`,
      `- ${plan.governance.expectedChip.displaySubtitle || "—"}`,
      ""
    );
  }

  lines.push("## Next recommended commands", "");
  for (const cmd of plan.nextCommands) lines.push("```bash", cmd, "```", "");
  if (plan.nextNotes.length) {
    lines.push("## Notes", "");
    for (const n of plan.nextNotes) lines.push(`- ${n}`);
    lines.push("");
  }

  lines.push("## Safety (v1)", "");
  for (const item of plan.safety.neverWrites) lines.push(`- Does not write: ${item}`);
  lines.push(
    `- Company Validated write path blocked in publish layer: **${plan.safety.companyValidatedWritePathBlocked ? "yes" : "no"}**`,
    `- Apply orchestration in workflow CLI: **${plan.safety.applyOrchestrationInV1 ? "enabled" : "disabled — use explicit scripts"}**`,
    ""
  );

  return lines.join("\n");
}
