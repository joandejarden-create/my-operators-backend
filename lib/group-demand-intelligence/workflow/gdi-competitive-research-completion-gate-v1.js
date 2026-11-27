/**
 * Permanent Competitive Account Discovery completion gate.
 *
 * Distinguishes research-not-executed from research-executed-zero-result.
 * Integrates with GDI Research Intelligence / Research Receipt — not a
 * parallel reporting engine.
 *
 * Contract: gdi-competitive-research-completion-gate-v1
 */

export const GDI_COMPETITIVE_RESEARCH_COMPLETION_GATE_V1 =
  "gdi-competitive-research-completion-gate-v1";

export const COMPETITIVE_RESEARCH_EXECUTION_STATE = Object.freeze({
  NOT_EXECUTED: "NOT_EXECUTED",
  PARTIAL_HISTORICAL: "PARTIAL_HISTORICAL",
  EXECUTED_ZERO_QUALIFIED: "EXECUTED_ZERO_QUALIFIED",
  EXECUTED_WITH_QUALIFIED: "EXECUTED_WITH_QUALIFIED",
  CANDIDATES_DISCOVERED_NOT_QUALIFIED: "CANDIDATES_DISCOVERED_NOT_QUALIFIED",
  QUALIFIED_MISSING_FROM_STAGED: "QUALIFIED_MISSING_FROM_STAGED",
  MAPPING_DEFECT: "MAPPING_DEFECT",
});

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Build a completion record for one hotel Full E2E / Research Intelligence surface.
 */
export function buildCompetitiveResearchCompletionRecord(input = {}) {
  const compSetEstablished = input.compSetEstablished === true;
  const competitiveResearchExecuted = input.competitiveResearchExecuted === true;
  const competitorsResearched = num(input.competitorsResearched) ?? 0;
  const competitiveCandidatesDiscovered =
    num(input.competitiveCandidatesDiscovered) ?? 0;
  const candidatesDeeplyResearched =
    num(input.candidatesDeeplyResearched) ?? 0;
  const candidatesScreenedOut = num(input.candidatesScreenedOut) ?? 0;
  const competitiveOpportunitiesQualified =
    num(input.competitiveOpportunitiesQualified) ?? 0;
  const competitiveOpportunitiesStaged =
    num(input.competitiveOpportunitiesStaged) ?? 0;
  const remainingCommercialResearchRequirements = Array.isArray(
    input.remainingCommercialResearchRequirements,
  )
    ? input.remainingCommercialResearchRequirements.map(text).filter(Boolean)
    : [];
  const actualResearchExpenditureUsd = num(input.actualResearchExpenditureUsd);

  let executionState = COMPETITIVE_RESEARCH_EXECUTION_STATE.NOT_EXECUTED;
  if (!competitiveResearchExecuted) {
    executionState = COMPETITIVE_RESEARCH_EXECUTION_STATE.NOT_EXECUTED;
  } else if (
    competitiveCandidatesDiscovered > 0 &&
    competitiveOpportunitiesQualified === 0 &&
    competitiveOpportunitiesStaged === 0
  ) {
    executionState =
      COMPETITIVE_RESEARCH_EXECUTION_STATE.CANDIDATES_DISCOVERED_NOT_QUALIFIED;
  } else if (
    competitiveOpportunitiesQualified > 0 &&
    competitiveOpportunitiesStaged === 0
  ) {
    executionState =
      COMPETITIVE_RESEARCH_EXECUTION_STATE.QUALIFIED_MISSING_FROM_STAGED;
  } else if (
    competitiveOpportunitiesStaged > 0 ||
    competitiveOpportunitiesQualified > 0
  ) {
    executionState =
      COMPETITIVE_RESEARCH_EXECUTION_STATE.EXECUTED_WITH_QUALIFIED;
  } else if (input.partialHistorical === true) {
    executionState = COMPETITIVE_RESEARCH_EXECUTION_STATE.PARTIAL_HISTORICAL;
  } else {
    executionState =
      COMPETITIVE_RESEARCH_EXECUTION_STATE.EXECUTED_ZERO_QUALIFIED;
  }

  const zeroQualifiedIsAcceptable =
    competitiveResearchExecuted === true &&
    executionState ===
      COMPETITIVE_RESEARCH_EXECUTION_STATE.EXECUTED_ZERO_QUALIFIED;

  const mustNotReportAsSuccessfulZero =
    competitiveResearchExecuted !== true &&
    competitiveOpportunitiesStaged === 0;

  return {
    contractId: GDI_COMPETITIVE_RESEARCH_COMPLETION_GATE_V1,
    hotelId: text(input.hotelId) || null,
    hotelName: text(input.hotelName) || null,
    metrics: {
      compSetEstablished,
      competitiveResearchExecuted,
      competitorsResearched,
      competitiveCandidatesDiscovered,
      candidatesDeeplyResearched,
      candidatesScreenedOut,
      competitiveOpportunitiesQualified,
      competitiveOpportunitiesStaged,
      remainingCommercialResearchRequirements,
      actualResearchExpenditureUsd,
    },
    executionState,
    zeroQualifiedIsAcceptable,
    mustNotReportAsSuccessfulZero,
    customerFacingNote:
      mustNotReportAsSuccessfulZero
        ? "Competitive research has not been completed for this hotel. A Competitive count of zero does not mean Competitive Account Discovery finished with no findings."
        : zeroQualifiedIsAcceptable
          ? "Competitive research completed with no qualified competitive opportunities after evidence-backed screening."
          : competitiveOpportunitiesStaged > 0
            ? `${competitiveOpportunitiesStaged} competitive opportunity(ies) in STAGED inventory.`
            : "Competitive research status recorded for founder/admin review.",
    internalNote: text(input.internalNote) || null,
  };
}

/**
 * Research Intelligence / Receipt-compatible metric rows (internal).
 */
export function competitiveCompletionMetricsForResearchIntelligence(record) {
  const m = record?.metrics || {};
  return [
    {
      key: "compSetEstablished",
      label: "Comp-Set Established",
      value: m.compSetEstablished === true ? 1 : 0,
      note: m.compSetEstablished === true ? "Yes" : "No",
    },
    {
      key: "competitiveResearchExecuted",
      label: "Competitive Research Executed",
      value: m.competitiveResearchExecuted === true ? 1 : 0,
      note:
        record?.executionState ||
        (m.competitiveResearchExecuted ? "Executed" : "Not executed"),
    },
    {
      key: "competitorsResearched",
      label: "Competitors Researched",
      value: m.competitorsResearched,
    },
    {
      key: "competitiveCandidatesDiscovered",
      label: "Competitive Candidates Discovered",
      value: m.competitiveCandidatesDiscovered,
    },
    {
      key: "candidatesDeeplyResearched",
      label: "Candidates Deeply Researched",
      value: m.candidatesDeeplyResearched,
    },
    {
      key: "candidatesScreenedOut",
      label: "Candidates Screened Out",
      value: m.candidatesScreenedOut,
    },
    {
      key: "competitiveOpportunitiesQualified",
      label: "Competitive Opportunities Qualified",
      value: m.competitiveOpportunitiesQualified,
    },
    {
      key: "competitiveOpportunitiesStaged",
      label: "Competitive Opportunities Staged",
      value: m.competitiveOpportunitiesStaged,
    },
    {
      key: "actualResearchExpenditureUsd",
      label: "Competitive Research Expenditure (USD)",
      value: m.actualResearchExpenditureUsd,
      availability:
        m.actualResearchExpenditureUsd == null ? "UNAVAILABLE" : undefined,
    },
  ];
}

export default {
  GDI_COMPETITIVE_RESEARCH_COMPLETION_GATE_V1,
  COMPETITIVE_RESEARCH_EXECUTION_STATE,
  buildCompetitiveResearchCompletionRecord,
  competitiveCompletionMetricsForResearchIntelligence,
};
