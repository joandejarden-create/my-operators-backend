#!/usr/bin/env node
/**
 * Regression: Exa Agent prompt H1 titles must reflect the actual GDI segment.
 * Mocked only — no paid provider calls.
 */
import assert from "node:assert/strict";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";
import {
  GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES,
  GDI_EXA_SEGMENT_REQUEST_TITLES_V1,
  applyExaSegmentRequestTitle,
  buildExaSegmentRequestHeading,
  buildExaFourteenSegmentCoverageReport,
  createExaSegmentRequestLedgerEntry,
  extractExaQueryHeading,
  formatExaFourteenSegmentCoverageMarkdown,
  normalizeCanonicalExaSegmentId,
} from "../lib/group-demand-intelligence/research/exa-agent/gdi-exa-segment-request-titles-v1.js";
import {
  buildExaEventsAgentCreatePayload,
  buildExaEventsAgentQuery,
  EXA_EVENTS_DISCOVERY_CONTRACT_V1,
  EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-events-contracts-v1.js";
import {
  buildExaAgentQuery,
  EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE,
  EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-contracts-v1.js";
import { quoteExaAgentRun, EXA_AGENT_EFFORT } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (e) {
    results.push({ name, ok: false, error: String(e?.message || e) });
  }
}

const FOURTEEN = listFourteenWorkflowSegmentIds();
assert.equal(FOURTEEN.length, 14);

check("each_of_14_canonical_segments_correct_heading", () => {
  for (const id of FOURTEEN) {
    const display = GDI_EXA_SEGMENT_REQUEST_DISPLAY_NAMES[id];
    assert.ok(display, `missing display for ${id}`);
    const disc = buildExaSegmentRequestHeading({
      segmentId: id,
      stage: "discovery",
    });
    const deep = buildExaSegmentRequestHeading({
      segmentId: id,
      stage: "deep_research",
    });
    assert.equal(disc, `# ${display} | DISCOVERY`);
    assert.equal(deep, `# ${display} | DEEP RESEARCH`);
  }
});

check("stage_a_produces_discovery_in_title", () => {
  const h = buildExaSegmentRequestHeading({
    segmentId: "corporate_meetings_incentives",
    stage: "DISCOVERY",
  });
  assert.match(h, /\| DISCOVERY$/);
  assert.doesNotMatch(h, /DEEP RESEARCH/);
});

check("stage_b_produces_deep_research_in_title", () => {
  const h = buildExaSegmentRequestHeading({
    segmentId: "relocation_extended_stay",
    stage: "DEEP_RESEARCH",
  });
  assert.match(h, /\| DEEP RESEARCH$/);
  assert.doesNotMatch(h, /\| DISCOVERY$/);
});

check("education_scientific_cannot_receive_association_heading", () => {
  const q = buildExaEventsAgentQuery({
    mode: "deep_research",
    contextBlock: "Segment: education_scientific",
    segmentId: "education_scientific",
  });
  const heading = extractExaQueryHeading(q);
  assert.equal(heading, "# EDUCATION / SCIENTIFIC | DEEP RESEARCH");
  assert.doesNotMatch(q, /^# ASSOCIATION \/ CONFERENCE \/ EVENTS/m);
});

check("projects_infrastructure_cannot_receive_association_heading", () => {
  const q = buildExaEventsAgentQuery({
    mode: "discovery",
    contextBlock: "Segment: projects_infrastructure",
    segmentId: "projects_infrastructure",
  });
  const heading = extractExaQueryHeading(q);
  assert.equal(heading, "# PROJECTS / INFRASTRUCTURE | DISCOVERY");
  assert.doesNotMatch(heading, /ASSOCIATION/);
});

check("unknown_segments_do_not_silently_default_to_associations", () => {
  assert.equal(normalizeCanonicalExaSegmentId("not_a_real_segment"), null);
  assert.throws(
    () =>
      buildExaSegmentRequestHeading({
        segmentId: "not_a_real_segment",
        stage: "discovery",
      }),
    /EXA_SEGMENT_TITLE_UNKNOWN_OR_MISSING/,
  );
  assert.throws(
    () =>
      buildExaEventsAgentCreatePayload({
        mode: "discovery",
        contextBlock: "x",
        segmentId: null,
      }),
    /EXA_SEGMENT_TITLE_UNKNOWN_OR_MISSING/,
  );
  assert.throws(
    () =>
      applyExaSegmentRequestTitle({
        query: "# ASSOCIATION / CONFERENCE / EVENTS — EXA AGENT DEEP RESEARCH CONTRACT V1\n\nbody",
        segmentId: "",
        stage: "deep_research",
      }),
    /EXA_SEGMENT_TITLE_UNKNOWN_OR_MISSING/,
  );
});

check("contract_ids_and_output_schemas_remain_compatible", () => {
  const disc = buildExaEventsAgentCreatePayload({
    mode: "discovery",
    contextBlock: "hotel context",
    segmentId: "local_catering_day_meetings",
  });
  const deep = buildExaEventsAgentCreatePayload({
    mode: "deep_research",
    contextBlock: "candidate context",
    segmentId: "professional_event_planners",
  });
  assert.equal(disc.meta.contractId, EXA_EVENTS_DISCOVERY_CONTRACT_V1);
  assert.equal(deep.meta.contractId, EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1);
  assert.ok(disc.body.outputSchema);
  assert.ok(deep.body.outputSchema);
  assert.match(disc.body.query, /Contract ID: gdi-exa-events-discovery-v1/);
  assert.match(deep.body.query, /Contract ID: gdi-exa-events-deep-research-v1/);

  const scalingDisc = buildExaAgentQuery({
    mode: "discovery",
    contextBlock: "scaling",
    segmentId: "corporate_accounts_scaling",
  });
  const scalingDeep = buildExaAgentQuery({
    mode: "deep_research",
    contextBlock: "scaling deep",
    segmentId: "projects_infrastructure",
  });
  assert.match(scalingDisc, new RegExp(EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE));
  assert.match(scalingDeep, new RegExp(EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1));
  assert.equal(
    extractExaQueryHeading(scalingDisc),
    "# CORPORATE ACCOUNTS / SCALING COMPANIES | DISCOVERY",
  );
  assert.equal(
    extractExaQueryHeading(scalingDeep),
    "# PROJECTS / INFRASTRUCTURE | DEEP RESEARCH",
  );
});

check("exa_call_count_and_cost_accounting_unchanged", () => {
  const medium = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
  const high = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.HIGH });
  assert.equal(medium.maxUsd, 0.1);
  assert.equal(high.maxUsd, 0.5);
  assert.equal(medium.bounded, true);
  assert.equal(high.bounded, true);
});

check("run_ledger_reflects_actual_submitted_requests_only", () => {
  const entries = [
    createExaSegmentRequestLedgerEntry({
      segmentId: "education_scientific",
      stage: "discovery",
      status: "COMPLETED",
      providerRunId: "agent_run_mock_edu_1",
      costUsd: 0.1,
      candidateId: null,
    }),
    createExaSegmentRequestLedgerEntry({
      segmentId: "education_scientific",
      stage: "deep_research",
      status: "COMPLETED",
      providerRunId: "agent_run_mock_edu_2",
      costUsd: 0.5,
      candidateId: "cand_edu_1",
    }),
    createExaSegmentRequestLedgerEntry({
      segmentId: "projects_infrastructure",
      stage: "discovery",
      status: "FAILED",
      providerRunId: "agent_run_mock_proj_1",
      costUsd: 0,
      error: "PROVIDER_TIMEOUT",
    }),
  ];
  const report = buildExaFourteenSegmentCoverageReport({ entries });
  assert.equal(report.version, GDI_EXA_SEGMENT_REQUEST_TITLES_V1);
  assert.deepEqual(report.segmentsDiscoveredAgainst.sort(), [
    "education_scientific",
  ]);
  assert.deepEqual(report.segmentsDeepResearched, ["education_scientific"]);
  assert.ok(report.segmentsWithZeroRequests.includes("associations_conferences_events"));
  assert.ok(report.segmentsWithZeroRequests.includes("sports_entertainment"));
  assert.equal(report.bySegmentId.education_scientific.requestsSubmitted, 2);
  assert.equal(report.bySegmentId.education_scientific.discovery, "EXECUTED");
  assert.equal(report.bySegmentId.education_scientific.deepResearch, "EXECUTED");
  assert.equal(report.bySegmentId.projects_infrastructure.discovery, "FAILED");
  assert.equal(report.bySegmentId.projects_infrastructure.deepResearch, "SKIPPED");
  assert.equal(report.totalExaSpendUsd, 0.6);
  // Planned-but-not-run segments stay SKIPPED, not EXECUTED.
  assert.equal(report.bySegmentId.associations_conferences_events.discovery, "SKIPPED");
  assert.equal(report.bySegmentId.associations_conferences_events.requestsSubmitted, 0);
});

check("coverage_report_distinguishes_scheduled_executed_completed_failed_skipped", () => {
  const report = buildExaFourteenSegmentCoverageReport({
    entries: [
      createExaSegmentRequestLedgerEntry({
        segmentId: "local_catering_day_meetings",
        stage: "discovery",
        status: "COMPLETED",
        costUsd: 0.1,
        providerRunId: "r1",
      }),
      createExaSegmentRequestLedgerEntry({
        segmentId: "relocation_extended_stay",
        stage: "deep_research",
        status: "SKIPPED",
        error: "BUDGET_CAP",
        costUsd: 0,
      }),
    ],
    plannedSegmentIds: [
      "local_catering_day_meetings",
      "relocation_extended_stay",
      "sports_entertainment",
    ],
    skipNotes: [
      { segmentId: "sports_entertainment", reason: "NOT_SCHEDULED_THIS_WAVE" },
    ],
  });
  const md = formatExaFourteenSegmentCoverageMarkdown(report);
  assert.match(md, /LOCAL CATERING \/ DAY MEETINGS \| EXECUTED \| SKIPPED/);
  assert.match(md, /RELOCATION \/ EXTENDED STAY \| SKIPPED \| SKIPPED/);
  assert.ok(
    report.bySegmentId.sports_entertainment.skipReasons.includes(
      "NOT_SCHEDULED_THIS_WAVE",
    ),
  );
  assert.equal(report.table.length, 14);
});

check("historical_alias_normalization", () => {
  assert.equal(
    normalizeCanonicalExaSegmentId("association_events"),
    "associations_conferences_events",
  );
  assert.equal(
    normalizeCanonicalExaSegmentId("scaling_companies"),
    "corporate_accounts_scaling",
  );
  assert.equal(
    normalizeCanonicalExaSegmentId("preferred_lodging"),
    "preferred_lodging_partnerships",
  );
  const h = buildExaSegmentRequestHeading({
    segmentId: "association_events",
    stage: "deep_research",
  });
  assert.equal(h, "# ASSOCIATION / CONFERENCE / EVENTS | DEEP RESEARCH");
});

check("first_line_of_submitted_prompt_is_segment_heading", () => {
  const pack = buildExaEventsAgentCreatePayload({
    mode: "deep_research",
    contextBlock: "Segment: education_scientific\nHotel: YOTEL",
    segmentId: "education_scientific",
  });
  const first = extractExaQueryHeading(pack.body.query);
  assert.equal(first, "# EDUCATION / SCIENTIFIC | DEEP RESEARCH");
  assert.equal(extractExaQueryHeading(pack.meta.requestHeading), first);
  assert.equal(pack.meta.segmentId, "education_scientific");
});

check("opportunity_name_appended_to_heading", () => {
  const h = buildExaSegmentRequestHeading({
    segmentId: "preferred_lodging_partnerships",
    stage: "deep_research",
    opportunityName: "The Lane",
  });
  assert.equal(
    h,
    "# PREFERRED LODGING / PARTNERSHIPS | DEEP RESEARCH | The Lane",
  );
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: GDI_EXA_SEGMENT_REQUEST_TITLES_V1,
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
