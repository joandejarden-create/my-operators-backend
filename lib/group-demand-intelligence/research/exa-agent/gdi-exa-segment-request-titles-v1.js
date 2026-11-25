/**
 * GDI Exa Agent segment-specific request titles V1.
 *
 * Observability-only: rewrites the first markdown H1 of outbound Exa Agent
 * prompts so Exa request history shows the actual demand segment.
 * Does not change research missions, schemas, contract IDs, budgets, or gates.
 */

import { listFourteenWorkflowSegmentIds } from "../universal-contact-research-contract-v1.js";

export const GDI_EXA_SEGMENT_REQUEST_TITLES_V1 =
  "gdi-exa-segment-request-titles-v1";

/** Founder-facing display names for the 14 canonical workflow segment IDs. */
export const GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES = Object.freeze({
  associations_conferences_events: "ASSOCIATION / CONFERENCE / EVENTS",
  corporate_accounts_scaling: "CORPORATE ACCOUNTS / SCALING COMPANIES",
  corporate_meetings_incentives: "CORPORATE MEETINGS / INCENTIVES",
  government_public_sector: "GOVERNMENT / PUBLIC SECTOR",
  sports_entertainment: "SPORTS / ENTERTAINMENT",
  education_scientific: "EDUCATION / SCIENTIFIC",
  projects_infrastructure: "PROJECTS / INFRASTRUCTURE",
  social_special_events: "SOCIAL / SPECIAL EVENTS",
  local_catering_day_meetings: "LOCAL CATERING / DAY MEETINGS",
  corporate_celebrations_private_events: "CORPORATE CELEBRATIONS / PRIVATE EVENTS",
  travel_trade_group_intermediaries: "TRAVEL TRADE / GROUP INTERMEDIARIES",
  relocation_extended_stay: "RELOCATION / EXTENDED STAY",
  preferred_lodging_partnerships: "PREFERRED LODGING / PARTNERSHIPS",
  professional_event_planners: "PROFESSIONAL EVENT PLANNERS",
});

/**
 * Historical ledger / campaign aliases → canonical 14 workflow IDs.
 * Mirrors full-e2e-fourteen-segment-discovery-execution-v1 SEGMENT_TO_LEDGER_WORKFLOW.
 */
const ALIAS_TO_CANONICAL = Object.freeze({
  associations_conferences_events: "associations_conferences_events",
  association_events: "associations_conferences_events",
  associations_government: "associations_conferences_events",
  corporate_accounts_scaling: "corporate_accounts_scaling",
  scaling_companies: "corporate_accounts_scaling",
  scaling: "corporate_accounts_scaling",
  corporate_meetings_incentives: "corporate_meetings_incentives",
  corporate_meetings_training: "corporate_meetings_incentives",
  mice_incentive_travel: "corporate_meetings_incentives",
  government_public_sector: "government_public_sector",
  government: "government_public_sector",
  sports_entertainment: "sports_entertainment",
  sports: "sports_entertainment",
  entertainment_production: "sports_entertainment",
  education_scientific: "education_scientific",
  medical_academic: "education_scientific",
  projects_infrastructure: "projects_infrastructure",
  projects: "projects_infrastructure",
  social_special_events: "social_special_events",
  weddings_social_private_events: "social_special_events",
  social: "social_special_events",
  local_catering_day_meetings: "local_catering_day_meetings",
  local_meetings_catering: "local_catering_day_meetings",
  corporate_celebrations_private_events: "corporate_celebrations_private_events",
  travel_trade_group_intermediaries: "travel_trade_group_intermediaries",
  relocation_extended_stay: "relocation_extended_stay",
  preferred_lodging_partnerships: "preferred_lodging_partnerships",
  preferred_lodging: "preferred_lodging_partnerships",
  professional_event_planners: "professional_event_planners",
  professional_planners_dmc: "professional_event_planners",
});

export const EXA_REQUEST_STAGE = Object.freeze({
  DISCOVERY: "DISCOVERY",
  DEEP_RESEARCH: "DEEP_RESEARCH",
});

const MODE_TO_STAGE = Object.freeze({
  discovery: EXA_REQUEST_STAGE.DISCOVERY,
  deep_research: EXA_REQUEST_STAGE.DEEP_RESEARCH,
  DISCOVERY: EXA_REQUEST_STAGE.DISCOVERY,
  DEEP_RESEARCH: EXA_REQUEST_STAGE.DEEP_RESEARCH,
});

export function normalizeCanonicalExaSegmentId(segmentId) {
  const raw = String(segmentId || "").trim();
  if (!raw) return null;
  const key = raw.toLowerCase().replace(/[\s-]+/g, "_");
  const canonical = ALIAS_TO_CANONICAL[key] || null;
  if (!canonical) return null;
  const allowed = new Set(listFourteenWorkflowSegmentIds());
  return allowed.has(canonical) ? canonical : null;
}

export function resolveExaSegmentRequestDisplayName(segmentId) {
  const canonical = normalizeCanonicalExaSegmentId(segmentId);
  if (!canonical) return null;
  return GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES[canonical] || null;
}

export function normalizeExaRequestStage(stageOrMode) {
  const s = MODE_TO_STAGE[String(stageOrMode || "").trim()];
  return s || null;
}

/**
 * Concise human-readable opportunity label for Exa dashboard H1 (≤60 chars).
 */
export function formatExaRequestOpportunityName(raw = "") {
  const s = String(raw || "")
    .replace(/\s+/g, " ")
    .replace(/[|#`]/g, "")
    .trim();
  if (!s) return null;
  if (s.length <= 60) return s;
  return `${s.slice(0, 57).trimEnd()}…`;
}

/**
 * Build the first-line Exa prompt heading for a segment + stage.
 * Founder convention:
 *   `# [DEMAND SEGMENT] | [DISCOVERY|DEEP RESEARCH] | [OPPORTUNITY NAME]`
 * Opportunity name is optional; omitted when empty.
 * Throws when segment/stage cannot be resolved — never defaults to Associations.
 */
export function buildExaSegmentRequestHeading({
  segmentId,
  stage,
  opportunityName = null,
} = {}) {
  const canonical = normalizeCanonicalExaSegmentId(segmentId);
  if (!canonical) {
    const err = new Error(
      `EXA_SEGMENT_TITLE_UNKNOWN_OR_MISSING:${String(segmentId || "") || "(empty)"}`,
    );
    err.code = "EXA_SEGMENT_TITLE_UNKNOWN_OR_MISSING";
    err.segmentId = segmentId || null;
    throw err;
  }
  const stageNorm = normalizeExaRequestStage(stage);
  if (!stageNorm) {
    const err = new Error(
      `EXA_SEGMENT_TITLE_UNKNOWN_STAGE:${String(stage || "") || "(empty)"}`,
    );
    err.code = "EXA_SEGMENT_TITLE_UNKNOWN_STAGE";
    err.stage = stage || null;
    throw err;
  }
  const display = GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES[canonical];
  const stageLabel =
    stageNorm === EXA_REQUEST_STAGE.DISCOVERY ? "DISCOVERY" : "DEEP RESEARCH";
  const opp = formatExaRequestOpportunityName(opportunityName);
  return opp
    ? `# ${display} | ${stageLabel} | ${opp}`
    : `# ${display} | ${stageLabel}`;
}

const H1_RE = /^#\s+[^\r\n]+/m;

/**
 * Ensure the submitted Exa query starts with the segment-specific heading.
 * Replaces an existing leading H1 when present; otherwise prepends.
 */
export function applyExaSegmentRequestTitle({
  query,
  segmentId,
  stage,
  opportunityName = null,
} = {}) {
  const heading = buildExaSegmentRequestHeading({
    segmentId,
    stage,
    opportunityName,
  });
  const body = String(query || "");
  // Normalize CRLF from Windows-checked-in prompt markdown so the H1 is clean LF.
  const trimmed = body.replace(/^\uFEFF/, "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (!trimmed.trim()) return `${heading}\n`;
  if (H1_RE.test(trimmed)) {
    return trimmed.replace(H1_RE, heading);
  }
  return `${heading}\n\n${trimmed}`;
}

export function extractExaQueryHeading(query) {
  const m = String(query || "")
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .match(/^#\s+.+$/m);
  return m ? m[0].trim() : null;
}

/** Per-request ledger row for segment execution visibility. */
export function createExaSegmentRequestLedgerEntry({
  segmentId,
  stage,
  status = "SUBMITTED",
  providerRunId = null,
  costUsd = null,
  at = null,
  candidateId = null,
  label = null,
  opportunityName = null,
  error = null,
} = {}) {
  const canonical = normalizeCanonicalExaSegmentId(segmentId);
  if (!canonical) {
    const err = new Error(
      `EXA_SEGMENT_LEDGER_UNKNOWN_SEGMENT:${String(segmentId || "") || "(empty)"}`,
    );
    err.code = "EXA_SEGMENT_LEDGER_UNKNOWN_SEGMENT";
    throw err;
  }
  const stageNorm = normalizeExaRequestStage(stage);
  if (!stageNorm) {
    const err = new Error(
      `EXA_SEGMENT_LEDGER_UNKNOWN_STAGE:${String(stage || "") || "(empty)"}`,
    );
    err.code = "EXA_SEGMENT_LEDGER_UNKNOWN_STAGE";
    throw err;
  }
  const oppName = formatExaRequestOpportunityName(opportunityName || label);
  return {
    version: GDI_EXA_SEGMENT_REQUEST_TITLES_V1,
    segmentId: canonical,
    displayTitle: GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES[canonical],
    stage: stageNorm,
    status: String(status || "SUBMITTED").toUpperCase(),
    providerRunId: providerRunId || null,
    costUsd: costUsd == null || !Number.isFinite(Number(costUsd)) ? null : Number(costUsd),
    at: at || new Date().toISOString(),
    candidateId: candidateId || null,
    label: label || null,
    opportunityName: oppName,
    error: error || null,
    heading: buildExaSegmentRequestHeading({
      segmentId: canonical,
      stage: stageNorm,
      opportunityName: oppName,
    }),
  };
}

function emptySegmentRow(segmentId) {
  return {
    segmentId,
    displayTitle: GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES[segmentId],
    discovery: "SKIPPED",
    deepResearch: "SKIPPED",
    requestsSubmitted: 0,
    requestsCompleted: 0,
    requestsFailed: 0,
    requestsSkipped: 0,
    candidatesResearched: 0,
    costUsd: 0,
    requestIds: [],
    timestamps: [],
    skipReasons: [],
  };
}

/**
 * Aggregate actual submitted Exa requests into a 14-segment coverage summary.
 * A segment is EXECUTED only when at least one matching request was submitted.
 */
export function buildExaFourteenSegmentCoverageReport({
  entries = [],
  plannedSegmentIds = null,
  skipNotes = [],
} = {}) {
  const rows = {};
  for (const id of listFourteenWorkflowSegmentIds()) {
    rows[id] = emptySegmentRow(id);
  }

  const candidateSets = {
    DISCOVERY: {},
    DEEP_RESEARCH: {},
  };
  for (const stage of Object.keys(candidateSets)) {
    for (const id of Object.keys(rows)) candidateSets[stage][id] = new Set();
  }

  for (const raw of entries || []) {
    let entry;
    try {
      entry =
        raw?.segmentId && raw?.stage
          ? createExaSegmentRequestLedgerEntry(raw)
          : null;
    } catch {
      entry = null;
    }
    if (!entry) continue;
    const row = rows[entry.segmentId];
    if (!row) continue;

    row.requestsSubmitted += 1;
    row.requestIds.push(entry.providerRunId || entry.label || entry.at);
    row.timestamps.push(entry.at);
    if (entry.costUsd != null) row.costUsd = Number((row.costUsd + entry.costUsd).toFixed(6));

    if (entry.candidateId) {
      candidateSets[entry.stage][entry.segmentId].add(String(entry.candidateId));
    }

    const status = entry.status;
    if (status === "COMPLETED" || status === "OK" || status === "CACHED") {
      row.requestsCompleted += 1;
    } else if (status === "FAILED" || status === "ERROR") {
      row.requestsFailed += 1;
    } else if (status === "SKIPPED" || status === "BLOCKED") {
      row.requestsSkipped += 1;
      if (entry.error) row.skipReasons.push(entry.error);
    }

    if (entry.stage === EXA_REQUEST_STAGE.DISCOVERY) {
      row.discovery =
        status === "FAILED" || status === "ERROR"
          ? "FAILED"
          : status === "SKIPPED" || status === "BLOCKED"
            ? "SKIPPED"
            : "EXECUTED";
    } else {
      row.deepResearch =
        status === "FAILED" || status === "ERROR"
          ? "FAILED"
          : status === "SKIPPED" || status === "BLOCKED"
            ? "SKIPPED"
            : "EXECUTED";
    }
  }

  for (const id of Object.keys(rows)) {
    const disc = candidateSets.DISCOVERY[id].size;
    const deep = candidateSets.DEEP_RESEARCH[id].size;
    rows[id].candidatesResearched = disc + deep;
  }

  const planned = new Set(
    (plannedSegmentIds || listFourteenWorkflowSegmentIds())
      .map((s) => normalizeCanonicalExaSegmentId(s))
      .filter(Boolean),
  );
  for (const note of skipNotes || []) {
    const id = normalizeCanonicalExaSegmentId(note?.segmentId);
    if (!id || !rows[id]) continue;
    if (note?.reason) rows[id].skipReasons.push(String(note.reason));
    if (rows[id].requestsSubmitted === 0) {
      rows[id].discovery = "SKIPPED";
      rows[id].deepResearch = "SKIPPED";
    }
  }
  for (const id of Object.keys(rows)) {
    if (!planned.has(id) && rows[id].requestsSubmitted === 0) {
      rows[id].skipReasons.push("NOT_IN_PLANNED_QUEUE");
    }
  }

  const table = listFourteenWorkflowSegmentIds().map((id) => {
    const r = rows[id];
    return {
      segment: r.displayTitle,
      segmentId: id,
      discovery: r.discovery,
      deepResearch: r.deepResearch,
      requests: r.requestsSubmitted,
      costUsd: Number(r.costUsd.toFixed(6)),
    };
  });

  const executedDiscovery = table.filter((r) => r.discovery === "EXECUTED").map((r) => r.segmentId);
  const executedDeep = table.filter((r) => r.deepResearch === "EXECUTED").map((r) => r.segmentId);
  const zeroRequests = table.filter((r) => r.requests === 0).map((r) => r.segmentId);

  return {
    version: GDI_EXA_SEGMENT_REQUEST_TITLES_V1,
    contract: "GDI_EXA_FOURTEEN_SEGMENT_COVERAGE_REPORT_V1",
    generatedAt: new Date().toISOString(),
    totalExaSpendUsd: Number(
      table.reduce((a, r) => a + Number(r.costUsd || 0), 0).toFixed(6),
    ),
    segmentsDiscoveredAgainst: executedDiscovery,
    segmentsDeepResearched: executedDeep,
    segmentsWithZeroRequests: zeroRequests,
    table,
    bySegmentId: rows,
  };
}

export function formatExaFourteenSegmentCoverageMarkdown(report) {
  const lines = [
    "| Segment | Discovery | Deep Research | Requests | Cost |",
    "|---|---|---|---:|---:|",
  ];
  for (const row of report?.table || []) {
    lines.push(
      `| ${row.segment} | ${row.discovery} | ${row.deepResearch} | ${row.requests} | $${Number(row.costUsd || 0).toFixed(2)} |`,
    );
  }
  lines.push("");
  lines.push(`**Total Exa spend:** $${Number(report?.totalExaSpendUsd || 0).toFixed(2)}`);
  return lines.join("\n");
}
