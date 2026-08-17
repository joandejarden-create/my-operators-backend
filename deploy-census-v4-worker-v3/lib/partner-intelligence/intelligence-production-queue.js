/**
 * Dealality Intelligence Production Queue v1.1 — batch status + next-action queue.
 * Reuses v1 workflow stage detection; read-only only.
 * @see docs/data-intelligence/dealality-intelligence-production-workflow-v1.md
 */
import {
  WORKFLOW_STAGES,
  buildIntelligenceProfileWorkflowPlan,
  buildNextCommands,
  determineWorkflowStage,
  isStrongerLiveGovernancePreserved,
  isSupportedEntityType,
} from "./intelligence-profile-workflow.js";
import { isStableGovernanceChangeClass } from "./profile-governance-publish-readiness.js";

export const QUEUE_VERSION = "v1.1";
export const REPORT_JSON_NAME = "intelligence-production-queue.json";
export const REPORT_MD_NAME = "intelligence-production-queue.md";

/**
 * Priority entities from partner-intelligence-priority-profile-production-tracker.md.
 * Records marked TBD are included as unresolved placeholders.
 */
export const PRIORITY_QUEUE_ENTRIES = [
  {
    entityName: "GHL Hoteles (GHL Holding)",
    entityType: "operator",
    targetRecId: "reciI2tYQBfMoMK9G",
    trackerPriority: "completed",
  },
  {
    entityName: "Hotel Equities (CALA)",
    entityType: "operator",
    targetRecId: "recWPKu5laVZxsvpn",
    trackerPriority: "completed",
  },
  {
    entityName: "Arbor Lodging (CALA)",
    entityType: "operator",
    targetRecId: "recF5Z87OAqFgndoq",
    trackerPriority: "completed",
  },
  {
    entityName: "Kimpton Hotels",
    entityType: "brand",
    targetRecId: "recCKuXCmGvxHPfb3",
    trackerPriority: "completed",
  },
  {
    entityName: "Curio Collection by Hilton",
    entityType: "brand",
    targetRecId: "receQkxgjlezsc1xg",
    trackerPriority: "completed",
  },
  {
    entityName: "Aimbridge Hospitality (LATAM)",
    entityType: "operator",
    targetRecId: "recGWxIJqnYHkJZFD",
    trackerPriority: "next",
    unresolvedReason: null,
  },
  {
    entityName: "Best Western Plus",
    entityType: "brand",
    targetRecId: "rec5KPgalPPAFl7UZ",
    trackerPriority: "next",
  },
  {
    entityName: "Hilton Garden Inn",
    entityType: "brand",
    targetRecId: "recrvdAjRlXxPvPPF",
    trackerPriority: "next",
  },
  {
    entityName: "Radisson Blu (Choice)",
    entityType: "brand",
    targetRecId: "recWPEvxBQxVVzSq3",
    trackerPriority: "next",
  },
  {
    entityName: "Viento Sur Gestión Hotelera (CALA)",
    entityType: "operator",
    targetRecId: "recZPHT2zqc8K6itx",
    trackerPriority: "next",
  },
];

const APPLY_FLAGS = ["--apply", "--publish-apply", "--approve-stewardship"];

export function rejectQueueApplyFlags(argv = process.argv) {
  for (const flag of APPLY_FLAGS) {
    if (argv.includes(flag)) {
      return {
        rejected: true,
        flag,
        message:
          "[intelligence-production-queue] Apply mode is disabled in v1.1. Use explicit steward/extract/publish scripts from queue output.",
      };
    }
  }
  return { rejected: false };
}

export function isPlatformReady(entry) {
  if (!entry?.resolved) return false;
  return entry.currentStage?.stageId === 8 && entry.readyForPlatformUsage === true;
}

function isPublishScopeDowngradeProtectionOnly(publishScope = []) {
  return (
    publishScope.length > 0 &&
    publishScope.every((b) => b === "would_downgrade_existing_validation")
  );
}

export function isSeriousBlock(entry) {
  if (!entry.resolved || isPlatformReady(entry)) return false;
  if (
    entry.currentStage?.stageId === 8 &&
    entry.changeClass === "downgrade" &&
    (entry.currentStage?.blockers || []).includes("stronger_live_governance_preserved")
  ) {
    return false;
  }
  if (entry.changeClass === "downgrade") return true;
  const publishScope = entry.blockers?.publishScope || [];
  if (publishScope.length > 0) {
    if (
      entry.currentStage?.stageId === 8 &&
      isPublishScopeDowngradeProtectionOnly(publishScope)
    ) {
      return false;
    }
    return true;
  }
  if ((entry.blockers?.labels || []).length > 0) return true;
  return false;
}

export function isBlockedEntry(entry) {
  if (!entry?.resolved) return true;
  if (isPlatformReady(entry)) return false;
  if (isSeriousBlock(entry)) return true;
  const stageId = entry.currentStage?.stageId;
  return stageId != null && stageId < 8;
}

export function summarizeNextAction(stage) {
  const map = {
    resolve_entity: "Resolve entity record ID and type",
    source_discovery: "Discover and link official sources",
    source_capture_register: "Capture/register official sources",
    source_stewardship: "Review and approve Explorer-use sources",
    extraction_dry_run: "Run extraction dry-run",
    fact_stewardship: "Review and approve strong facts",
    governance_publish_dry_run: "Run governance publish dry-run",
    governance_apply: "Apply governance after founder approval",
    platform_usage: "Monitor / optional enrichment (stronger live governance preserved when applicable)",
  };
  return map[stage?.stageKey] || stage?.stageLabel || "Review workflow plan";
}

export function buildUnresolvedQueueEntry(priorityEntry) {
  return {
    resolved: false,
    entityName: priorityEntry.entityName,
    entityType: priorityEntry.entityType,
    targetRecId: priorityEntry.targetRecId,
    trackerPriority: priorityEntry.trackerPriority,
    unresolvedReason: priorityEntry.unresolvedReason || "missing_record_id",
    currentStage: {
      stageId: 0,
      stageKey: "resolve_entity",
      stageLabel: "Resolve entity",
      blockers: ["unresolved_record_id"],
    },
    sourceCount: null,
    approvedSourceCount: null,
    factCount: null,
    approvedFactCount: null,
    governanceStatus: null,
    externalDisplayStatus: null,
    companyValidated: null,
    blockers: {
      workflow: ["unresolved_record_id"],
      publishScope: [],
      labels: [],
    },
    nextAction: "Resolve Airtable record ID in priority tracker",
    nextCommand:
      "npm run intelligence-profile-workflow -- --entity-type {type} --target-rec-id rec... --plan",
    nextCommands: [],
    readyForPlatformUsage: false,
    status: "unresolved",
  };
}

/**
 * Transform a v1 workflow plan into a queue row.
 */
export function buildQueueEntryFromPlan(plan, priorityMeta = {}) {
  const allBlockers = [
    ...new Set([
      ...(plan.blockers?.workflow || []),
      ...(plan.blockers?.publishScope || []),
      ...(plan.blockers?.labels || []),
    ]),
  ];

  const governanceLive = plan.governance?.live || {};
  const changeClass = plan.governance?.changeClass || null;
  const stageId = plan.currentStage?.stageId;
  const governancePreserved =
    stageId === 8 &&
    (isStableGovernanceChangeClass(changeClass) ||
      (changeClass === "downgrade" &&
        (plan.currentStage?.blockers || []).includes("stronger_live_governance_preserved")));
  const readyForPlatformUsage = governancePreserved;

  let status = "in_progress";
  if (readyForPlatformUsage) status = "platform_ready";
  else if (allBlockers.length) status = "blocked";
  else if (plan.currentStage?.stageId === 8) status = "platform_ready";

  const primaryCommand = plan.nextCommands?.[0] || null;

  return {
    resolved: true,
    entityName: plan.entityName,
    entityType: plan.entityType,
    targetRecId: plan.targetRecId,
    trackerPriority: priorityMeta.trackerPriority || null,
    currentStage: plan.currentStage,
    sourceCount: plan.sourceCounts?.total ?? 0,
    approvedSourceCount: plan.sourceCounts?.approvedExplorer ?? 0,
    factCount: plan.factCounts?.total ?? 0,
    approvedFactCount: plan.factCounts?.approved ?? 0,
    governanceStatus:
      governanceLive.validationStatus ||
      plan.governance?.expectedChip?.validationStatus ||
      null,
    externalDisplayStatus:
      governanceLive.externalDisplayStatus ||
      plan.governance?.expectedChip?.externalDisplayStatus ||
      null,
    displayLabel: governancePreserved
      ? plan.governance?.normalized?.displayLabel ||
        plan.governance?.expectedChip?.displayLabel ||
        null
      : plan.governance?.expectedChip?.displayLabel ||
        plan.governance?.normalized?.displayLabel ||
        null,
    companyValidated: Boolean(governanceLive.companyValidated),
    changeClass,
    publishEligible: plan.governance?.eligible ?? null,
    completenessTier: plan.completeness?.tier || null,
    blockers: plan.blockers,
    allBlockers,
    nextAction: summarizeNextAction(plan.currentStage),
    nextCommand: primaryCommand,
    nextCommands: plan.nextCommands || [],
    readyForPlatformUsage,
    status,
  };
}

export function categorizeQueueEntry(entry) {
  if (!entry.resolved) return "unresolved";
  const stageId = entry.currentStage?.stageId;
  if (stageId === 8 && entry.readyForPlatformUsage) return "complete";
  if (stageId <= 2) return "needs_sources";
  if (stageId === 3) return "needs_source_approval";
  if (stageId === 4) return "needs_extraction";
  if (stageId === 5) return "needs_fact_approval";
  if (stageId === 6 || stageId === 7) return "needs_governance_publish";
  return "blocked";
}

export function summarizeQueue(entries) {
  const resolved = entries.filter((e) => e.resolved);
  const summary = {
    totalPackages: entries.length,
    resolved: resolved.length,
    unresolved: entries.length - resolved.length,
    completeStage8: 0,
    platformReady: 0,
    blocked: 0,
    needsSources: 0,
    needsSourceApproval: 0,
    needsExtraction: 0,
    needsFactApproval: 0,
    needsGovernancePublish: 0,
    unresolvedCount: 0,
  };

  for (const entry of entries) {
    const cat = categorizeQueueEntry(entry);
    switch (cat) {
      case "complete":
        summary.completeStage8 += 1;
        if (entry.readyForPlatformUsage) summary.platformReady += 1;
        break;
      case "needs_sources":
        summary.needsSources += 1;
        break;
      case "needs_source_approval":
        summary.needsSourceApproval += 1;
        break;
      case "needs_extraction":
        summary.needsExtraction += 1;
        break;
      case "needs_fact_approval":
        summary.needsFactApproval += 1;
        break;
      case "needs_governance_publish":
        summary.needsGovernancePublish += 1;
        break;
      case "unresolved":
        summary.unresolvedCount += 1;
        break;
      default:
        break;
    }
    if (isSeriousBlock(entry)) summary.blocked += 1;
  }

  return summary;
}

export function filterQueueEntries(entries, filters = {}) {
  let result = [...entries];

  if (filters.entityType) {
    result = result.filter((e) => e.entityType === filters.entityType);
  }

  if (filters.stage != null) {
    const stageNum = Number(filters.stage);
    result = result.filter((e) => e.currentStage?.stageId === stageNum);
  }

  if (filters.readyOnly) {
    result = result.filter((e) => isPlatformReady(e));
  }

  if (filters.blockedOnly) {
    result = result.filter((e) => isBlockedEntry(e));
  }

  return result;
}

export function buildProductionQueue({
  entries,
  filters = {},
  generatedAt = new Date().toISOString(),
}) {
  const filtered = filterQueueEntries(entries, filters);
  const summary = summarizeQueue(filtered);
  const platformReady = filtered.filter((e) => isPlatformReady(e));
  const blocked = filtered.filter((e) => isBlockedEntry(e) && !isPlatformReady(e));

  return {
    queueVersion: QUEUE_VERSION,
    generatedAt,
    mode: "plan",
    filters,
    summary,
    entries: filtered,
    platformReady,
    blocked,
    safety: {
      readOnly: true,
      applyOrchestrationEnabled: false,
      neverWrites: [
        "Company Validated",
        "Company Validation Date",
        "source/fact approval",
        "governance publish",
        "Airtable schema",
      ],
    },
  };
}

export function buildQueueMarkdown(queue) {
  const { summary, entries, platformReady, blocked, filters } = queue;
  const lines = [
    "# Dealality Intelligence Production Queue",
    "",
    `Generated: ${queue.generatedAt}`,
    `Queue: **${queue.queueVersion}**`,
    `Mode: **${queue.mode}** (read-only)`,
    "",
  ];

  if (Object.keys(filters).length) {
    lines.push("## Filters", "");
    for (const [k, v] of Object.entries(filters)) {
      if (v != null && v !== false) lines.push(`- ${k}: \`${v}\``);
    }
    lines.push("");
  }

  lines.push(
    "## Executive summary",
    "",
    `| Metric | Count |`,
    `|--------|------:|`,
    `| Total packages | ${summary.totalPackages} |`,
    `| Resolved | ${summary.resolved} |`,
    `| Unresolved (missing rec ID) | ${summary.unresolvedCount} |`,
    `| Complete / Stage 8 | ${summary.completeStage8} |`,
    `| Platform-ready | ${summary.platformReady} |`,
    `| Blocked (in progress) | ${summary.blocked} |`,
    `| Needs sources | ${summary.needsSources} |`,
    `| Needs source approval | ${summary.needsSourceApproval} |`,
    `| Needs extraction | ${summary.needsExtraction} |`,
    `| Needs fact approval | ${summary.needsFactApproval} |`,
    `| Needs governance publish | ${summary.needsGovernancePublish} |`,
    ""
  );

  lines.push(
    "## Queue table",
    "",
    "| Entity | Type | Stage | Status | Sources (appr/total) | Facts (appr/total) | Blockers | Next action |",
    "|--------|------|------:|--------|---------------------:|-------------------:|----------|-------------|"
  );

  for (const e of entries) {
    const stage = e.resolved
      ? `${e.currentStage?.stageId} · ${e.currentStage?.stageKey}`
      : "—";
    const src = e.resolved
      ? `${e.approvedSourceCount}/${e.sourceCount}`
      : "—";
    const facts = e.resolved
      ? `${e.approvedFactCount}/${e.factCount}`
      : "—";
    const blockers = e.resolved
      ? (e.allBlockers || []).slice(0, 3).join("; ") || "—"
      : e.unresolvedReason || "unresolved";
    lines.push(
      `| ${e.entityName} | ${e.entityType} | ${stage} | ${e.status} | ${src} | ${facts} | ${blockers} | ${e.nextAction} |`
    );
  }
  lines.push("");

  lines.push("## Platform-ready profiles", "");
  if (!platformReady.length) {
    lines.push("_None in current filter._", "");
  } else {
    for (const e of platformReady) {
      lines.push(
        `- **${e.entityName}** (\`${e.entityType}:${e.targetRecId}\`) — ${e.displayLabel || e.governanceStatus || "governed"} · change class \`${e.changeClass}\``
      );
    }
    lines.push("");
  }

  lines.push("## Blocked profiles", "");
  if (!blocked.length) {
    lines.push("_None in current filter._", "");
  } else {
    for (const e of blocked) {
      lines.push(`### ${e.entityName}`, "");
      lines.push(`- Record: \`${e.targetRecId}\` (${e.entityType})`);
      lines.push(`- Stage: ${e.currentStage?.stageId} — ${e.currentStage?.stageKey}`);
      const bl = e.allBlockers?.length
        ? e.allBlockers
        : e.blockers?.workflow || [e.unresolvedReason];
      for (const b of bl) lines.push(`- Blocker: ${b}`);
      lines.push("");
    }
  }

  lines.push("## Next actions", "");
  for (const e of entries) {
    if (isPlatformReady(e)) continue;
    lines.push(`### ${e.entityName} (\`${e.targetRecId || "TBD"}\`)`, "");
    lines.push(`**${e.nextAction}**`, "");
    const cmds = e.nextCommands?.length ? e.nextCommands : e.nextCommand ? [e.nextCommand] : [];
    for (const cmd of cmds) {
      lines.push("```bash", cmd, "```", "");
    }
  }

  lines.push("## Safety (v1.1)", "");
  lines.push("- Read-only queue — no Airtable writes");
  lines.push("- No apply orchestration — use printed commands with explicit approval");
  for (const item of queue.safety.neverWrites) {
    lines.push(`- Does not write: ${item}`);
  }
  lines.push("");

  return lines.join("\n");
}

export {
  buildIntelligenceProfileWorkflowPlan,
  determineWorkflowStage,
  buildNextCommands,
  WORKFLOW_STAGES,
  isSupportedEntityType,
};
