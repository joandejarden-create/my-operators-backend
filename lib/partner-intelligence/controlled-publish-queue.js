/**
 * Controlled Publish Queue v2.1 — batch field publishing readiness (read-only).
 * @see docs/data-intelligence/controlled-publish-queue-v2-1.md
 */
import { PUBLISH_MODES } from "./approved-intelligence-field-publishing.js";
import {
  RISK_LEVELS,
  buildFieldSuggestionsFromAudit,
} from "./approved-intelligence-field-suggestions.js";
import {
  SUPPORTED_ENTITY_TYPES_V2,
  V2_ALLOWED_OPERATOR_DESTINATIONS,
  validateControlledPublishGates,
} from "./controlled-platform-field-publishing.js";
import { PRIORITY_QUEUE_ENTRIES } from "./intelligence-production-queue.js";
import { loadFieldPublishingAuditForEntity } from "./field-publishing-entity-loader.js";

export const QUEUE_VERSION = "v2.1";
export const REPORT_JSON_NAME = "controlled-publish-queue.json";
export const REPORT_MD_NAME = "controlled-publish-queue.md";

export const QUEUE_STATUSES = {
  readyForControlledPublish: "ready_for_controlled_publish",
  suggestedOnly: "suggested_only",
  blocked: "blocked",
  needsStewardReview: "needs_steward_review",
  noApprovedFacts: "no_approved_facts",
  noDestinationMapping: "no_destination_mapping",
  alreadyPublished: "already_published",
  unresolved: "unresolved",
};

export const APPLY_FLAGS = [
  "--apply",
  "--publish-apply",
  "--approve-controlled-field-publish",
  "--write",
];

export function rejectControlledPublishQueueApplyFlags(argv = process.argv) {
  for (const flag of APPLY_FLAGS) {
    if (argv.includes(flag)) {
      return {
        rejected: true,
        flag,
        message:
          "[controlled-publish-queue] Apply/write mode is disabled in v2.1. Use dry-run commands from queue output only.",
      };
    }
  }
  return { rejected: false };
}

/** Map Airtable destination column → v2 CLI --destination-field key */
export function resolveDestinationFieldKey(destinationField, factKey) {
  for (const [key, spec] of Object.entries(V2_ALLOWED_OPERATOR_DESTINATIONS)) {
    if (spec.destinationField === destinationField && spec.expectedFactKey === factKey) {
      return key;
    }
  }
  if (destinationField === "specificMarkets") return "specificMarkets";
  return null;
}

export function buildDryRunCommand(entry) {
  const destKey = entry.destinationFieldKey || resolveDestinationFieldKey(
    entry.destinationField,
    entry.factKey
  );
  if (!destKey || !entry.sourceFactId) return null;
  return `npm run controlled-platform-field-publishing -- --entity-type ${entry.entityType} --target-rec-id ${entry.targetRecId} --fact-id ${entry.sourceFactId} --destination-field ${destKey} --dry-run`;
}

function isAllowlistedV2(entityType, destinationFieldKey) {
  return (
    entityType === "operator" &&
    destinationFieldKey &&
    Boolean(V2_ALLOWED_OPERATOR_DESTINATIONS[destinationFieldKey])
  );
}

/**
 * Classify one suggestion/mapping row for queue reporting.
 */
export function classifyPublishQueueItem(suggestion, mapping, entityType) {
  const destFieldKey = resolveDestinationFieldKey(
    suggestion?.destinationField || mapping?.destinationField,
    suggestion?.factKey || mapping?.fieldKey
  );
  const publishMode = suggestion?.publishMode || mapping?.publishMode;
  const riskLevel = suggestion?.riskLevel || RISK_LEVELS.medium;
  const livePopulated =
    suggestion?.liveValuePopulated ?? mapping?.liveValuePopulated ?? false;
  const blockers = suggestion?.blockers || mapping?.blockers || [];

  if (publishMode === PUBLISH_MODES.blocked) {
    return { status: QUEUE_STATUSES.blocked, reasons: blockers };
  }

  if (!mapping && !suggestion) {
    return { status: QUEUE_STATUSES.noDestinationMapping, reasons: ["no_mapping"] };
  }

  if (
    livePopulated &&
    (publishMode === PUBLISH_MODES.suggestedUpdate ||
      blockers.includes("destination_field_populated"))
  ) {
    const reasons = ["destination_populated"];
    if (isAllowlistedV2(entityType, destFieldKey)) {
      reasons.push("controlled_publish_v2_allowlisted_field");
    }
    return { status: QUEUE_STATUSES.alreadyPublished, reasons };
  }

  const readyCandidate =
    publishMode === PUBLISH_MODES.controlledPublishCandidate &&
    riskLevel === RISK_LEVELS.low &&
    !livePopulated &&
    isAllowlistedV2(entityType, destFieldKey);

  if (readyCandidate) {
    return { status: QUEUE_STATUSES.readyForControlledPublish, reasons: [] };
  }

  const needsReview =
    riskLevel === RISK_LEVELS.medium ||
    riskLevel === RISK_LEVELS.high ||
    blockers.includes("identity_field_populated_no_overwrite") ||
    blockers.includes("select_option_validation_required") ||
    blockers.includes("destination_field_populated");

  if (publishMode === PUBLISH_MODES.suggestedUpdate || needsReview) {
    if (needsReview) {
      return {
        status: QUEUE_STATUSES.needsStewardReview,
        reasons: blockers.length ? blockers : ["medium_or_high_risk"],
      };
    }
    return { status: QUEUE_STATUSES.suggestedOnly, reasons: blockers };
  }

  if (publishMode === PUBLISH_MODES.controlledPublishCandidate && !readyCandidate) {
    return {
      status: QUEUE_STATUSES.needsStewardReview,
      reasons: ["controlled_candidate_not_v2_ready"],
    };
  }

  return { status: QUEUE_STATUSES.blocked, reasons: ["unclassified"] };
}

export function buildPublishQueueItem(suggestion, mapping, entityMeta, classification) {
  const destFieldKey = resolveDestinationFieldKey(
    suggestion.destinationField,
    suggestion.factKey
  );
  const item = {
    entityType: entityMeta.entityType,
    targetRecId: entityMeta.targetRecId,
    entityName: entityMeta.entityName,
    sourceFactId: suggestion.sourceFactId,
    factKey: suggestion.factKey,
    displayLabel: suggestion.displayLabel,
    destinationTable: suggestion.destinationTable,
    destinationField: suggestion.destinationField,
    destinationFieldKey: destFieldKey,
    proposedValue: suggestion.proposedValue || suggestion.approvedValue,
    currentLiveValue: suggestion.currentLiveValue,
    liveValuePopulated: suggestion.liveValuePopulated,
    riskLevel: suggestion.riskLevel,
    publishMode: suggestion.publishMode,
    classification: suggestion.classification,
    queueStatus: classification.status,
    reasons: classification.reasons,
    blockers: suggestion.blockers || [],
    v2Allowlisted: isAllowlistedV2(entityMeta.entityType, destFieldKey),
    dryRunCommand: null,
  };

  if (classification.status === QUEUE_STATUSES.readyForControlledPublish) {
    item.dryRunCommand = buildDryRunCommand(item);
  }

  return item;
}

export function buildUnresolvedEntityEntry(priorityEntry) {
  return {
    resolved: false,
    entityName: priorityEntry.entityName,
    entityType: priorityEntry.entityType,
    targetRecId: priorityEntry.targetRecId,
    trackerPriority: priorityEntry.trackerPriority,
    queueStatus: QUEUE_STATUSES.unresolved,
    unresolvedReason: priorityEntry.unresolvedReason || "missing_record_id",
    summary: {
      approvedFacts: 0,
      totalMappings: 0,
      controlledCandidates: 0,
      suggestedOnly: 0,
      blocked: 0,
      evidenceOnly: 0,
      readyForControlledPublish: 0,
      alreadyPublished: 0,
      needsStewardReview: 0,
      riskCounts: { low: 0, medium: 0, high: 0 },
    },
    items: [],
    readyItems: [],
    nextAction: "Resolve record ID in priority tracker",
    nextCommands: [],
  };
}

/**
 * Build queue entry for one resolved entity.
 */
export function buildEntityControlledPublishQueueEntry({
  entityType,
  targetRecId,
  entityName,
  trackerPriority,
  audit,
  suggestionsReport,
  sources,
  facts,
}) {
  const mappingByFactKey = new Map((audit.mappings || []).map((m) => [m.factId, m]));
  const items = [];
  const readyItems = [];
  const alreadyPublishedItems = [];
  const stewardReviewItems = [];
  const suggestedOnlyItems = [];
  const blockedItems = [];

  let evidenceOnly = audit.summary?.evidenceOnly || 0;

  for (const suggestion of suggestionsReport.suggestions || []) {
    const mapping = mappingByFactKey.get(suggestion.sourceFactId);
    const classification = classifyPublishQueueItem(suggestion, mapping, entityType);
    const item = buildPublishQueueItem(suggestion, mapping, {
      entityType,
      targetRecId,
      entityName,
    }, classification);

    // Cross-check with v2 gates for ready items
    if (classification.status === QUEUE_STATUSES.readyForControlledPublish) {
      const fact = facts.find((f) => f.id === suggestion.sourceFactId);
      const source = sources.find((s) => s.id === suggestion.sourceId);
      const gate = validateControlledPublishGates({
        entityType,
        targetRecId,
        destinationFieldKey: item.destinationFieldKey,
        suggestion,
        fact,
        source,
        governance: suggestionsReport.governance,
        liveValue: suggestion.currentLiveValue,
        applyRequested: false,
        approvalPresent: false,
        factId: suggestion.sourceFactId,
      });
      if (!gate.ok) {
        item.queueStatus = QUEUE_STATUSES.needsStewardReview;
        item.reasons = gate.failures;
        stewardReviewItems.push(item);
      } else {
        readyItems.push(item);
      }
    } else if (classification.status === QUEUE_STATUSES.alreadyPublished) {
      alreadyPublishedItems.push(item);
    } else if (classification.status === QUEUE_STATUSES.needsStewardReview) {
      stewardReviewItems.push(item);
    } else if (classification.status === QUEUE_STATUSES.suggestedOnly) {
      suggestedOnlyItems.push(item);
    } else if (classification.status === QUEUE_STATUSES.blocked) {
      blockedItems.push(item);
    }

    items.push(item);
  }

  const riskCounts = {
    low: items.filter((i) => i.riskLevel === RISK_LEVELS.low).length,
    medium: items.filter((i) => i.riskLevel === RISK_LEVELS.medium).length,
    high: items.filter((i) => i.riskLevel === RISK_LEVELS.high).length,
  };

  const approvedFacts = audit.summary?.approvedFacts || 0;
  let entityStatus = QUEUE_STATUSES.suggestedOnly;
  if (!approvedFacts) entityStatus = QUEUE_STATUSES.noApprovedFacts;
  else if (readyItems.length) entityStatus = QUEUE_STATUSES.readyForControlledPublish;
  else if (alreadyPublishedItems.length && !stewardReviewItems.length && !suggestedOnlyItems.length) {
    entityStatus = QUEUE_STATUSES.alreadyPublished;
  } else if (stewardReviewItems.length) entityStatus = QUEUE_STATUSES.needsStewardReview;
  else if (blockedItems.length && !items.length) entityStatus = QUEUE_STATUSES.blocked;

  if (entityType !== "operator" && SUPPORTED_ENTITY_TYPES_V2.indexOf(entityType) === -1) {
    entityStatus =
      readyItems.length === 0 && items.length
        ? QUEUE_STATUSES.needsStewardReview
        : QUEUE_STATUSES.noApprovedFacts;
  }

  const nextCommands = readyItems
    .map((i) => i.dryRunCommand)
    .filter(Boolean);

  return {
    resolved: true,
    entityName,
    entityType,
    targetRecId,
    trackerPriority,
    queueStatus: entityStatus,
    governance: suggestionsReport.governance,
    summary: {
      approvedFacts,
      totalMappings: audit.mappings?.length || 0,
      controlledCandidates: audit.summary?.controlledPublishCandidate || 0,
      suggestedOnly: audit.summary?.suggestedUpdate || 0,
      blocked: audit.summary?.blocked || 0,
      evidenceOnly,
      readyForControlledPublish: readyItems.length,
      alreadyPublished: alreadyPublishedItems.length,
      needsStewardReview: stewardReviewItems.length,
      riskCounts,
    },
    items,
    readyItems,
    alreadyPublishedItems,
    stewardReviewItems,
    suggestedOnlyItems,
    blockedItems,
    excludedFacts: suggestionsReport.excludedFacts || [],
    excludedMappings: audit.mappings?.filter(
      (m) => m.publishMode === PUBLISH_MODES.evidenceOnly || m.publishMode === PUBLISH_MODES.blocked
    ) || [],
    nextAction: readyItems.length
      ? "Run controlled publish dry-run for ready candidates"
      : stewardReviewItems.length
        ? "Steward review suggested-only / populated destinations"
        : alreadyPublishedItems.length
          ? "Allowlisted fields published — future changes need steward review"
          : approvedFacts
            ? "No controlled publish candidates — monitor suggestions"
            : "Build approved fact package first",
    nextCommands,
  };
}

export function summarizeControlledPublishQueue(entries) {
  const resolved = entries.filter((e) => e.resolved);
  return {
    totalEntities: entries.length,
    resolved: resolved.length,
    unresolved: entries.length - resolved.length,
    entitiesWithControlledCandidates: resolved.filter(
      (e) => e.summary.controlledCandidates > 0
    ).length,
    entitiesReadyForPublish: resolved.filter((e) => e.readyItems?.length > 0).length,
    totalControlledCandidates: resolved.reduce(
      (n, e) => n + (e.summary.controlledCandidates || 0),
      0
    ),
    totalReadyForControlledPublish: resolved.reduce(
      (n, e) => n + (e.summary.readyForControlledPublish || 0),
      0
    ),
    totalSuggestedOnly: resolved.reduce(
      (n, e) => n + (e.suggestedOnlyItems?.length || 0),
      0
    ),
    totalNeedsStewardReview: resolved.reduce(
      (n, e) => n + (e.summary.needsStewardReview || 0),
      0
    ),
    totalAlreadyPublished: resolved.reduce(
      (n, e) => n + (e.summary.alreadyPublished || 0),
      0
    ),
    totalBlocked: resolved.reduce((n, e) => n + (e.blockedItems?.length || 0), 0),
    totalEvidenceOnly: resolved.reduce((n, e) => n + (e.summary.evidenceOnly || 0), 0),
  };
}

export function filterQueueEntries(entries, filters = {}) {
  let result = [...entries];

  if (filters.entityType) {
    result = result.filter((e) => e.entityType === filters.entityType);
  }

  if (filters.readyOnly) {
    result = result.filter((e) => (e.readyItems?.length || 0) > 0);
  }

  if (filters.blockedOnly) {
    result = result.filter(
      (e) =>
        e.queueStatus === QUEUE_STATUSES.blocked ||
        e.queueStatus === QUEUE_STATUSES.unresolved ||
        (e.blockedItems?.length || 0) > 0
    );
  }

  if (filters.suggestedOnly) {
    result = result.filter(
      (e) =>
        (e.suggestedOnlyItems?.length || 0) > 0 ||
        (e.stewardReviewItems?.length || 0) > 0
    );
  }

  if (filters.risk) {
    const risk = filters.risk;
    result = result.filter((e) =>
      (e.items || []).some((i) => i.riskLevel === risk)
    );
  }

  return result;
}

export function buildControlledPublishQueue({ entries, filters = {}, generatedAt }) {
  const filtered = filterQueueEntries(entries, filters);
  const summary = summarizeControlledPublishQueue(filtered);

  const allReady = [];
  const allSteward = [];
  const allAlreadyPublished = [];
  const allBlocked = [];

  for (const e of filtered) {
    if (e.readyItems?.length) allReady.push(...e.readyItems.map((i) => ({ ...i, entityName: e.entityName })));
    if (e.stewardReviewItems?.length) {
      allSteward.push(
        ...e.stewardReviewItems.map((i) => ({ ...i, entityName: e.entityName }))
      );
    }
    if (e.suggestedOnlyItems?.length) {
      allSteward.push(
        ...e.suggestedOnlyItems.map((i) => ({ ...i, entityName: e.entityName }))
      );
    }
    if (e.alreadyPublishedItems?.length) {
      allAlreadyPublished.push(
        ...e.alreadyPublishedItems.map((i) => ({ ...i, entityName: e.entityName }))
      );
    }
    if (!e.resolved || e.queueStatus === QUEUE_STATUSES.unresolved) {
      allBlocked.push({
        entityName: e.entityName,
        entityType: e.entityType,
        targetRecId: e.targetRecId,
        reason: e.unresolvedReason || "unresolved",
        nextAction: e.nextAction,
      });
    }
    if (e.queueStatus === QUEUE_STATUSES.noApprovedFacts) {
      allBlocked.push({
        entityName: e.entityName,
        entityType: e.entityType,
        targetRecId: e.targetRecId,
        reason: "no_approved_facts",
        nextAction: e.nextAction,
      });
    }
  }

  const nextCommands = [...new Set(filtered.flatMap((e) => e.nextCommands || []))];

  return {
    queueVersion: QUEUE_VERSION,
    generatedAt: generatedAt || new Date().toISOString(),
    mode: "plan",
    filters,
    summary,
    entities: filtered,
    readyForControlledPublish: allReady,
    needsStewardReview: allSteward,
    alreadyPublished: allAlreadyPublished,
    blocked: allBlocked,
    nextCommands,
    safety: {
      readOnly: true,
      applyEnabled: false,
      v2SupportedEntityTypes: SUPPORTED_ENTITY_TYPES_V2,
      v2AllowlistedDestinations: Object.keys(V2_ALLOWED_OPERATOR_DESTINATIONS),
    },
  };
}

export function buildControlledPublishQueueMarkdown(queue) {
  const { summary } = queue;
  const lines = [
    "# Controlled Publish Queue",
    "",
    `Generated: ${queue.generatedAt}`,
    `Queue: **${queue.queueVersion}** (read-only)`,
    "",
  ];

  if (Object.keys(queue.filters).length) {
    lines.push("## Filters", "");
    for (const [k, v] of Object.entries(queue.filters)) {
      if (v != null && v !== false) lines.push(`- ${k}: \`${v}\``);
    }
    lines.push("");
  }

  lines.push(
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Total entities | ${summary.totalEntities} |`,
    `| Resolved | ${summary.resolved} |`,
    `| Unresolved | ${summary.unresolved} |`,
    `| Entities with controlled candidates (audit) | ${summary.entitiesWithControlledCandidates} |`,
    `| Entities ready for controlled publish | ${summary.entitiesReadyForPublish} |`,
    `| Total ready items | ${summary.totalReadyForControlledPublish} |`,
    `| Suggested-only items | ${summary.totalSuggestedOnly} |`,
    `| Needs steward review | ${summary.totalNeedsStewardReview} |`,
    `| Already published / populated | ${summary.totalAlreadyPublished} |`,
    `| Blocked items | ${summary.totalBlocked} |`,
    ""
  );

  lines.push("## Ready for controlled publish", "");
  if (!queue.readyForControlledPublish.length) {
    lines.push("_None in current filter._", "");
  } else {
    lines.push(
      "| Entity | Fact ID | Fact key | Destination | Proposed | Risk |",
      "|--------|---------|----------|-------------|----------|------|"
    );
    for (const i of queue.readyForControlledPublish) {
      lines.push(
        `| ${i.entityName} | \`${i.sourceFactId}\` | \`${i.factKey}\` | \`${i.destinationField}\` | ${String(i.proposedValue).slice(0, 40)} | ${i.riskLevel} |`
      );
    }
    lines.push("");
  }

  lines.push("## Suggested only / needs steward review", "");
  if (!queue.needsStewardReview.length) {
    lines.push("_None in current filter._", "");
  } else {
    for (const i of queue.needsStewardReview) {
      lines.push(
        `- **${i.entityName}** — \`${i.factKey}\` → \`${i.destinationField}\` (${i.riskLevel}): ${(i.reasons || []).join("; ") || i.queueStatus}`
      );
    }
    lines.push("");
  }

  lines.push("## Already published / destination populated", "");
  if (!queue.alreadyPublished.length) {
    lines.push("_None in current filter._", "");
  } else {
    for (const i of queue.alreadyPublished) {
      lines.push(
        `- **${i.entityName}** — \`${i.factKey}\` → \`${i.destinationField}\` · live: ${String(i.currentLiveValue || "").slice(0, 60)} · fact \`${i.sourceFactId}\` — future changes require steward review`
      );
    }
    lines.push("");
  }

  lines.push("## Blocked / no mapping", "");
  if (!queue.blocked.length) {
    lines.push("_None in current filter._", "");
  } else {
    for (const b of queue.blocked) {
      lines.push(`- **${b.entityName}** (\`${b.targetRecId || "TBD"}\`): ${b.reason}`);
    }
    lines.push("");
  }

  lines.push("## Exact next commands (dry-run only)", "");
  if (!queue.nextCommands.length) {
    lines.push("_No controlled publish dry-runs recommended._", "");
  } else {
    for (const cmd of queue.nextCommands) {
      lines.push("```bash", cmd, "```", "");
    }
    lines.push(
      "> **Do not run apply** without explicit steward approval and `--approve-controlled-field-publish`.",
      ""
    );
  }

  lines.push("## Safety", "");
  lines.push("- Read-only queue — no Airtable writes");
  lines.push("- v2 allowlist: operator `specificMarkets` only");
  lines.push("");

  return lines.join("\n");
}

export { PRIORITY_QUEUE_ENTRIES };
