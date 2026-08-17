/**
 * Approved Intelligence Field Suggestions v1 — Mode B review queue (read-only).
 * @see docs/data-intelligence/approved-intelligence-field-suggestions-v1.md
 */
import { PUBLISH_MODES } from "./approved-intelligence-field-publishing.js";

export const SUGGESTIONS_VERSION = "v1";
export const REPORT_JSON_NAME = "approved-intelligence-field-suggestions.json";
export const REPORT_MD_NAME = "approved-intelligence-field-suggestions.md";

export const SUGGESTION_STATUSES = {
  proposed: "Proposed",
  needsReview: "Needs Review",
  approvedForPublish: "Approved For Publish",
  rejected: "Rejected",
  superseded: "Superseded",
};

export const RISK_LEVELS = {
  low: "Low",
  medium: "Medium",
  high: "High",
};

export const SUGGESTION_APPLY_FLAGS = [
  "--apply",
  "--publish-apply",
  "--write",
  "--approve-suggestions",
];

const SUGGESTION_ELIGIBLE_MODES = new Set([
  PUBLISH_MODES.suggestedUpdate,
  PUBLISH_MODES.controlledPublishCandidate,
]);

export function suggestionsReportFileNames(targetRecId) {
  return {
    perEntityJson: `approved-intelligence-field-suggestions-${targetRecId}.json`,
    perEntityMd: `approved-intelligence-field-suggestions-${targetRecId}.md`,
    latestJson: REPORT_JSON_NAME,
    latestMd: REPORT_MD_NAME,
  };
}

export function rejectSuggestionsApplyFlags(argv = process.argv) {
  for (const flag of SUGGESTION_APPLY_FLAGS) {
    if (argv.includes(flag)) {
      return {
        rejected: true,
        flag,
        message:
          "[approved-intelligence-field-suggestions] Write/apply mode is disabled in v1. Report-only.",
      };
    }
  }
  return { rejected: false };
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function buildSourceSummary(source) {
  if (!source) return null;
  return {
    sourceId: source.id,
    sourceTitle: source.sourceTitle || source.title || null,
    sourceUrl: source.sourceUrl || source.url || null,
    sourceType: source.sourceType || null,
    sourceOrigin: source.sourceOrigin || null,
    approvedForExplorerUse: source.approvedForExplorerUse || null,
    status: source.status || null,
  };
}

/**
 * Assess risk for a suggestion from audit mapping + source context.
 */
export function assessSuggestionRisk(mapping, source, governance = {}) {
  const blockers = mapping.blockers || [];
  const policy = mapping.policy || {};

  if (policy.identityField || blockers.includes("identity_field_populated_no_overwrite")) {
    return { level: RISK_LEVELS.high, reasons: ["identity_field_risk"] };
  }

  if (blockers.includes("deal_fit_or_scoring_inference_risk")) {
    return { level: RISK_LEVELS.high, reasons: ["scoring_or_fit_implication"] };
  }

  if (
    mapping.liveValuePopulated ||
    blockers.includes("destination_field_populated") ||
    blockers.includes("select_option_validation_required")
  ) {
    const reasons = [];
    if (mapping.liveValuePopulated) reasons.push("destination_populated");
    if (blockers.includes("select_option_validation_required")) {
      reasons.push("select_option_validation");
    }
    return { level: RISK_LEVELS.medium, reasons };
  }

  if (
    mapping.publishMode === PUBLISH_MODES.controlledPublishCandidate &&
    !mapping.liveValuePopulated &&
    governance.displayAllowed !== false &&
    source?.approvedForExplorerUse === "Yes"
  ) {
    return { level: RISK_LEVELS.low, reasons: ["blank_destination_official_source"] };
  }

  return { level: RISK_LEVELS.medium, reasons: ["default_review"] };
}

export function buildRecommendation(mapping, risk) {
  if (mapping.publishMode === PUBLISH_MODES.controlledPublishCandidate && risk.level === RISK_LEVELS.low) {
    return "Review and approve for controlled publish when destination remains blank.";
  }
  if (risk.level === RISK_LEVELS.high) {
    return "Do not auto-publish; steward must explicitly approve any overwrite.";
  }
  if (blockersInclude(mapping, "select_option_validation_required")) {
    return "Validate select options against Airtable allowed values before publish.";
  }
  if (mapping.liveValuePopulated) {
    return "Compare proposed vs live value; approve only if proposed is clearly better.";
  }
  return "Review proposed value and evidence before approving for publish.";
}

function blockersInclude(mapping, key) {
  return (mapping.blockers || []).includes(key);
}

function initialSuggestionStatus(mapping, risk) {
  if (risk.level === RISK_LEVELS.high) return SUGGESTION_STATUSES.needsReview;
  if (mapping.publishMode === PUBLISH_MODES.controlledPublishCandidate) {
    return SUGGESTION_STATUSES.proposed;
  }
  return SUGGESTION_STATUSES.needsReview;
}

/**
 * Build one suggestion row from audit mapping.
 */
export function mappingToSuggestion(mapping, ctx) {
  const { entityType, targetRecId, entityName, sourceById, governance, factById } = ctx;
  const source = mapping.sourceId ? sourceById.get(mapping.sourceId) : null;
  const fact = mapping.factId ? factById.get(mapping.factId) : null;
  const risk = assessSuggestionRisk(mapping, source, governance);

  return {
    suggestionId: `${targetRecId}:${mapping.factId}:${mapping.fieldKey}`,
    status: initialSuggestionStatus(mapping, risk),
    entityType,
    targetRecId,
    entityName,
    sourceFactId: mapping.factId,
    sourceId: mapping.sourceId,
    factKey: mapping.fieldKey,
    displayLabel: mapping.displayLabel,
    approvedValue: mapping.approvedValue,
    destinationTable: mapping.destinationTable,
    destinationField: mapping.destinationField,
    destinationFieldType: mapping.destinationFieldType,
    consumptionPath: mapping.consumptionPath,
    currentLiveValue: mapping.liveValue,
    liveValuePopulated: mapping.liveValuePopulated,
    proposedValue: mapping.proposedValue || mapping.approvedValue,
    classification: mapping.publishMode,
    publishMode: mapping.publishMode,
    riskLevel: risk.level,
    riskReasons: risk.reasons,
    recommendation: buildRecommendation(mapping, risk),
    evidenceSummary: {
      evidenceText: fact?.evidenceText || null,
      pageSectionAnchor: fact?.pageSectionAnchor || null,
      confidenceLevel: fact?.confidenceLevel || null,
      extractionType: fact?.extractionType || null,
    },
    sourceSummary: buildSourceSummary(source),
    blockers: mapping.blockers || [],
    policyNotes: mapping.policy?.notes || mapping.notes || null,
  };
}

/**
 * Build suggestions report from field publishing audit.
 */
export function buildFieldSuggestionsFromAudit(audit, sources = [], facts = []) {
  const sourceById = new Map((sources || []).map((s) => [s.id, s]));
  const factById = new Map((facts || []).map((f) => [f.id, f]));
  const governance = audit.governance || {};

  const ctx = {
    entityType: audit.entityType,
    targetRecId: audit.targetRecId,
    entityName: audit.entityName,
    sourceById,
    factById,
    governance,
  };

  const suggestions = [];
  const excludedMappings = [];
  const excludedFacts = audit.excludedFacts || [];

  for (const mapping of audit.mappings || []) {
    if (!SUGGESTION_ELIGIBLE_MODES.has(mapping.publishMode)) {
      excludedMappings.push({
        factId: mapping.factId,
        fieldKey: mapping.fieldKey,
        publishMode: mapping.publishMode,
        reason:
          mapping.publishMode === PUBLISH_MODES.evidenceOnly
            ? "evidence_only"
            : mapping.publishMode === PUBLISH_MODES.blocked
              ? "blocked"
              : "not_eligible",
        blockers: mapping.blockers || [],
      });
      continue;
    }
    suggestions.push(mappingToSuggestion(mapping, ctx));
  }

  const controlledCandidates = suggestions.filter(
    (s) => s.classification === PUBLISH_MODES.controlledPublishCandidate
  );
  const suggestedOnly = suggestions.filter(
    (s) => s.classification === PUBLISH_MODES.suggestedUpdate
  );

  const riskCounts = {
    low: suggestions.filter((s) => s.riskLevel === RISK_LEVELS.low).length,
    medium: suggestions.filter((s) => s.riskLevel === RISK_LEVELS.medium).length,
    high: suggestions.filter((s) => s.riskLevel === RISK_LEVELS.high).length,
  };

  return {
    suggestionsVersion: SUGGESTIONS_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "suggestions",
    entityType: audit.entityType,
    targetRecId: audit.targetRecId,
    entityName: audit.entityName,
    governance: audit.governance,
    summary: {
      totalSuggestions: suggestions.length,
      controlledPublishCandidates: controlledCandidates.length,
      suggestedOnlyUpdates: suggestedOnly.length,
      excludedMappings: excludedMappings.length,
      excludedFacts: excludedFacts.length,
      riskCounts,
      byStatus: {
        proposed: suggestions.filter((s) => s.status === SUGGESTION_STATUSES.proposed).length,
        needsReview: suggestions.filter((s) => s.status === SUGGESTION_STATUSES.needsReview).length,
      },
    },
    suggestions,
    controlledPublishCandidates: controlledCandidates,
    suggestedOnlyUpdates: suggestedOnly,
    excludedMappings,
    excludedFacts,
    nextRecommendedAction:
      controlledCandidates.length > 0
        ? "Review controlled publish candidates first; approve selected suggestions before any future write path."
        : suggestedOnly.length > 0
          ? "Review suggested-only updates; no blank-field auto-publish candidates."
          : "No field suggestions — all mappings evidence-only or blocked.",
    safety: {
      readOnly: true,
      applyEnabled: false,
      noPlatformWrites: true,
      noGovernanceWrites: true,
      noCompanyValidatedWrites: true,
    },
    sourceAudit: {
      auditVersion: audit.auditVersion,
      generatedAt: audit.generatedAt,
    },
  };
}

export function buildFieldSuggestionsMarkdown(report) {
  const lines = [
    "# Approved Intelligence Field Suggestions",
    "",
    `Generated: ${report.generatedAt}`,
    `Suggestions: **${report.suggestionsVersion}** (read-only — Mode B)`,
    `Entity: ${report.entityType} — **${report.entityName}** (\`${report.targetRecId}\`)`,
    "",
    "## Executive summary",
    "",
    `| Metric | Count |`,
    `|--------|------:|`,
    `| Total suggestions | ${report.summary.totalSuggestions} |`,
    `| Controlled publish candidates | ${report.summary.controlledPublishCandidates} |`,
    `| Suggested-only updates | ${report.summary.suggestedOnlyUpdates} |`,
    `| Excluded mappings | ${report.summary.excludedMappings} |`,
    `| Excluded facts (not approved) | ${report.summary.excludedFacts} |`,
    `| Risk Low / Medium / High | ${report.summary.riskCounts.low} / ${report.summary.riskCounts.medium} / ${report.summary.riskCounts.high} |`,
    "",
    `**Next action:** ${report.nextRecommendedAction}`,
    "",
    "## Proposed suggestions",
    "",
    "| Field | Destination | Live → Proposed | Risk | Status |",
    "|-------|-------------|------------------|------|--------|",
  ];

  for (const s of report.suggestions) {
    const dest = s.destinationTable ? `${s.destinationField}` : "—";
    const live = (s.currentLiveValue || "—").slice(0, 30);
    const prop = (s.proposedValue || "").slice(0, 40);
    lines.push(
      `| \`${s.factKey}\` | ${dest} | ${live} → ${prop} | **${s.riskLevel}** | ${s.status} |`
    );
  }
  lines.push("");

  if (report.controlledPublishCandidates.length) {
    lines.push("## Controlled publish candidates", "");
    for (const s of report.controlledPublishCandidates) {
      lines.push(`### ${s.displayLabel} (\`${s.factKey}\`)`, "");
      lines.push(`- Fact: \`${s.sourceFactId}\` · Source: \`${s.sourceId}\``);
      lines.push(`- Destination: **${s.destinationTable}** → \`${s.destinationField}\``);
      lines.push(`- Proposed: ${s.proposedValue}`);
      lines.push(`- Risk: **${s.riskLevel}** — ${s.recommendation}`);
      lines.push("");
    }
  }

  if (report.suggestedOnlyUpdates.length) {
    lines.push("## Suggested-only updates", "");
    for (const s of report.suggestedOnlyUpdates) {
      lines.push(`- **${s.factKey}** → \`${s.destinationField}\` (${s.riskLevel})`);
      lines.push(`  - Live: ${(s.currentLiveValue || "—").slice(0, 80)}`);
      lines.push(`  - Proposed: ${(s.proposedValue || "").slice(0, 80)}`);
    }
    lines.push("");
  }

  if (report.excludedMappings.length) {
    lines.push("## Excluded mappings", "");
    for (const e of report.excludedMappings) {
      lines.push(`- \`${e.fieldKey}\` — ${e.reason} (${e.publishMode})`);
    }
    lines.push("");
  }

  if (report.excludedFacts?.length) {
    lines.push("## Excluded facts (not approved)", "");
    for (const f of report.excludedFacts) {
      lines.push(`- \`${f.fieldKey}\` — ${f.humanReviewStatus}`);
    }
    lines.push("");
  }

  lines.push("## Risks", "");
  for (const s of report.suggestions.filter((x) => x.riskLevel !== RISK_LEVELS.low)) {
    lines.push(
      `- **${s.factKey}** (${s.riskLevel}): ${(s.riskReasons || []).join(", ")} — ${s.recommendation}`
    );
  }
  if (!report.suggestions.some((x) => x.riskLevel !== RISK_LEVELS.low)) {
    lines.push("_No elevated-risk suggestions._");
  }
  lines.push("");

  lines.push("## Safety", "");
  lines.push("- Report-only v1 — no platform field writes");
  lines.push("- No governance or Company Validated writes");
  lines.push("- Future controlled publish writes only **Approved For Publish** suggestions");
  lines.push("");

  return lines.join("\n");
}

export { PUBLISH_MODES };
