/**
 * Action learning record + future recommender ranking design.
 * Structured components only — do not learn from free-text titles alone.
 */

export const ACTION_LEARNING_RECORD_SCHEMA = "ADP_ACTION_LEARNING_RECORD_V1";

export function createEmptyLearningRecord(partial = {}) {
  return {
    schema: ACTION_LEARNING_RECORD_SCHEMA,
    actionInstanceId: partial.actionInstanceId || null,
    actionPatternId: partial.actionPatternId || null,
    variantId: partial.variantId || null,
    propertyId: partial.propertyId || null,
    hotelAttributes: partial.hotelAttributes || {},
    issuePattern: partial.issuePattern || null,
    implementationStepsUsed: partial.implementationStepsUsed || [],
    sourcesChanged: partial.sourcesChanged || [],
    completionTime: partial.completionTime || null,
    metricBefore: partial.metricBefore || null,
    metricNextRun: partial.metricNextRun || null,
    metricSecondNextRun: partial.metricSecondNextRun || null,
    evidenceChanges: partial.evidenceChanges || [],
    outcomeClassification: partial.outcomeClassification || null,
    causationClaimed: false,
    note: "Subsequent monitoring may be recorded for learning. Monitoring alone does not establish causation.",
  };
}

/**
 * Future recommender ranking dimensions (architecture only for V1).
 * Do not rank by historical outcome alone.
 */
export const ACTION_RECOMMENDER_RANK_DIMENSIONS = Object.freeze([
  { id: "evidence_match", weightHint: "highest", note: "Observed issue vector match" },
  { id: "applicability", weightHint: "high", note: "Hotel type / traveler need fit" },
  { id: "specificity", weightHint: "high", note: "Playbook can be made property-specific" },
  { id: "operational_feasibility", weightHint: "medium", note: "Owner can execute with available sources" },
  { id: "historical_subsequent_outcomes", weightHint: "medium", note: "Never sole rank key" },
  { id: "confidence", weightHint: "medium", note: "Evidence completeness + source authority" },
]);

export const ACTION_RECOMMENDER_INPUT_VECTOR = Object.freeze([
  "observedIssueVector",
  "hotelType",
  "travelerNeed",
  "sourceGap",
  "competitiveEvidence",
  "historicalActionOutcomes",
]);

export function describeFutureRecommenderDesign() {
  return {
    version: "ADP_ACTION_RECOMMENDER_DESIGN_V1",
    status: "DESIGN_ONLY",
    inputVector: ACTION_RECOMMENDER_INPUT_VECTOR,
    rankDimensions: ACTION_RECOMMENDER_RANK_DIMENSIONS,
    rule: "Candidate patterns are scored by evidence match first; historical outcomes never rank alone.",
  };
}
