/**
 * Meaningful research sufficiency for a demand segment.
 *
 * Does NOT hard-code minIndependentTasks >= 2 as completion law.
 * Two shallow tasks ≠ depth. One high-quality task with multi-family
 * evidence + follow-up + dispositions may be sufficient.
 */

import { SEGMENT_RUN_STATUS } from "./research-constants-v1.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";

/**
 * @param {object} run
 * @returns {{ sufficient: boolean, reason: string, signals: object }}
 */
export function assessSegmentResearchSufficiency(run = {}) {
  const sourceFamilies = new Set(run.sourceFamilies || []);
  const independentTasks = Number(run.independentTasks || 0);
  const followUps = Number(run.followUpPasses || run.followUpDepth || 0);
  const strategiesTried = Number(
    run.strategiesTried || (run.strategies || []).length || 0
  );
  const hasDisposition =
    Number.isInteger(run.candidatesDiscovered) &&
    Number.isInteger(run.candidatesTerminated);
  const allDisposed =
    hasDisposition && run.candidatesDiscovered === run.candidatesTerminated;
  const unresolvedBlockers = Array.isArray(run.unresolvedBlockers)
    ? run.unresolvedBlockers.filter(Boolean)
    : [];
  const stoppingReason = String(run.stoppingReason || "").trim();
  const furtherUnlikely = run.furtherResearchUnlikely === true;
  const materialCandidatesDispositioned =
    allDisposed &&
    (run.candidatesDiscovered === 0 ||
      Number(run.materialCandidatesDispositioned ?? run.candidatesTerminated) > 0);

  const signals = {
    independentTasks,
    sourceFamilyCount: sourceFamilies.size,
    followUps,
    strategiesTried,
    allDisposed,
    unresolvedBlockerCount: unresolvedBlockers.length,
    stoppingReason: stoppingReason || null,
    furtherUnlikely,
  };

  // Classic failure: one task, single family, no follow-up
  if (independentTasks <= 1 && sourceFamilies.size <= 1 && followUps === 0) {
    return {
      sufficient: false,
      reason: "SINGLE_SHALLOW_TASK",
      signals,
    };
  }

  // Task-count inflation: multiple tasks but no breadth / follow-up / dispositions
  if (
    independentTasks >= 2 &&
    sourceFamilies.size < 2 &&
    followUps === 0 &&
    !allDisposed
  ) {
    return {
      sufficient: false,
      reason: "TASK_COUNT_INFLATION_WITHOUT_BREADTH",
      signals,
    };
  }

  if (!hasDisposition) {
    return {
      sufficient: false,
      reason: "MISSING_CANDIDATE_DISPOSITIONS",
      signals,
    };
  }

  if (!allDisposed) {
    return {
      sufficient: false,
      reason: "UNRESOLVED_CANDIDATES",
      signals,
    };
  }

  // High-quality single (or few) task path
  const multiFamily = sourceFamilies.size >= 2;
  const followUpOrStrategyBreadth = followUps >= 1 || strategiesTried >= 2;
  const blockersTerminal =
    unresolvedBlockers.length === 0 ||
    (furtherUnlikely && Boolean(stoppingReason));

  if (
    multiFamily &&
    materialCandidatesDispositioned &&
    followUpOrStrategyBreadth &&
    blockersTerminal
  ) {
    return {
      sufficient: true,
      reason: "SUFFICIENT_BREADTH_FOLLOWUP_DISPOSITIONS",
      signals,
    };
  }

  // Honest zero-yield with documented depth
  if (
    run.candidatesDiscovered === 0 &&
    multiFamily &&
    (followUps >= 1 || independentTasks >= 2 || (furtherUnlikely && stoppingReason))
  ) {
    return {
      sufficient: true,
      reason: "HONEST_ZERO_WITH_DOCUMENTED_DEPTH",
      signals,
    };
  }

  // Explicit evidence-supported stop for the segment research wave
  if (
    furtherUnlikely &&
    stoppingReason &&
    multiFamily &&
    allDisposed
  ) {
    return {
      sufficient: true,
      reason: "EVIDENCE_SUPPORTED_STOP",
      signals,
    };
  }

  return {
    sufficient: false,
    reason: "INSUFFICIENT_RESEARCH_DEPTH",
    signals,
  };
}

/**
 * Distinguishes a plan, an executed search, and completed buyer research.
 * A disposed name or a credited provider family is not verified demand.
 */
export function classifyWorkstreamResearchPosture(state = {}, opportunities = []) {
  if (state.applicable === false) return "NOT_APPLICABLE";
  if (state.status === SEGMENT_RUN_STATUS.BLOCKED || state.completionDecision === "BLOCKED_DEFENSIBLE") {
    return "BLOCKED";
  }
  const ids = new Set([...(state.candidateIds || []), ...(state.completedCandidateIds || [])]);
  const rows = (opportunities || []).filter((row) => ids.has(row.id || row.opportunityId));
  const verified = rows.filter((row) => {
    const sources = Array.isArray(row.sources) ? row.sources : [];
    const official = row.officialUrl || row.officialSource || row.officialHousingUrl
      || sources.some((item) => /^https?:/i.test(typeof item === "string" ? item : item?.url || ""));
    if (!official) return false;
    return evaluateProspectPublicationEligibility(row).hotelFacing === true;
  });
  if (verified.length > 0 || (Array.isArray(state.verifiedDemandMechanisms) && state.verifiedDemandMechanisms.length > 0)) {
    return "DEMAND_VERIFIED";
  }
  const actions = state.researchActions || [];
  const searched = actions.some((action) => /discovery|paid_multisegment|search|event_to_demand/i.test(`${action.kind || ""} ${action.strategy || ""}`));
  if (rows.some((row) => row.officialUrl || row.officialSource) || (state.evidenceRefs || []).some((ref) => /^https?:\/\//i.test(String(ref)))) {
    return "SOURCES_INVESTIGATED";
  }
  if (ids.size > 0) return "BUYERS_DISCOVERED";
  if (searched) return "EXECUTED";
  return "PLANNED";
}

export function classifySegmentRunStatus(run = {}) {
  if (run.applicable === false) {
    return {
      status: SEGMENT_RUN_STATUS.NOT_APPLICABLE,
      reason: run.applicabilityReason || "DECLARED_NOT_APPLICABLE",
    };
  }
  if (run.blocked) {
    return {
      status: SEGMENT_RUN_STATUS.BLOCKED,
      reason: run.blockedReason || "BLOCKED",
    };
  }
  if (run.failed) {
    return {
      status: SEGMENT_RUN_STATUS.FAILED,
      reason: run.error || "FAILED",
    };
  }

  const assessment = assessSegmentResearchSufficiency(run);
  if (!assessment.sufficient) {
    return {
      status:
        assessment.reason === "UNRESOLVED_CANDIDATES" ||
        assessment.reason === "MISSING_CANDIDATE_DISPOSITIONS"
          ? SEGMENT_RUN_STATUS.IN_PROGRESS
          : SEGMENT_RUN_STATUS.INSUFFICIENT_RESEARCH_DEPTH,
      reason: assessment.reason,
      metrics: assessment.signals,
    };
  }

  if (run.candidatesDiscovered === 0) {
    return {
      status: SEGMENT_RUN_STATUS.NO_SIGNAL_FOUND_AFTER_SUFFICIENT_RESEARCH,
      reason: assessment.reason,
      metrics: assessment.signals,
    };
  }

  return {
    status: SEGMENT_RUN_STATUS.COMPLETE,
    reason: assessment.reason,
    metrics: assessment.signals,
  };
}
