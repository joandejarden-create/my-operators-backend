/**
 * Validate executable action instances against ADP Action Intelligence gates.
 */

import {
  ACTION_INTELLIGENCE_GATES,
  REQUIRED_EXECUTABLE_FIELDS,
  isBannedGenericActionText,
} from "./action-executability-contract-v1.js";
import { getActionPattern } from "./action-pattern-library-v1.js";

export function validateExecutableActionInstance(action) {
  const failures = [];
  const gates = Object.fromEntries(ACTION_INTELLIGENCE_GATES.map((g) => [g, true]));

  for (const field of REQUIRED_EXECUTABLE_FIELDS) {
    const v = action?.[field];
    // targetDate may be null until management confirms ("Date to be confirmed")
    if (field === "targetDate") {
      if (!Object.prototype.hasOwnProperty.call(action || {}, "targetDate")) {
        failures.push(`missing_or_empty:${field}`);
        gates.ADP_ACTION_EXECUTABILITY_STANDARD = false;
      }
      continue;
    }
    const emptyArray = Array.isArray(v) && v.length === 0;
    const emptyObj =
      v && typeof v === "object" && !Array.isArray(v) && Object.keys(v).length === 0;
    if (v == null || v === "" || emptyArray || emptyObj) {
      failures.push(`missing_or_empty:${field}`);
      if (field === "definitionOfDone") gates.ADP_ACTION_DEFINITION_OF_DONE_COMPLETE = false;
      if (field === "trace") gates.ADP_ACTION_TRACEABILITY = false;
      if (field === "targetSources") gates.ADP_ACTION_TARGET_SOURCE_SPECIFICITY = false;
      if (field === "implementationSteps") {
        gates.ADP_ACTION_IMPLEMENTATION_STEPS_COMPLETE = false;
      }
      gates.ADP_ACTION_EXECUTABILITY_STANDARD = false;
    }
  }

  if (isBannedGenericActionText(action?.actionTitle) || isBannedGenericActionText(action?.recommendedAction)) {
    failures.push("generic_final_recommendation");
    gates.ADP_ACTION_NO_GENERIC_FINAL_RECOMMENDATIONS = false;
    gates.ADP_ACTION_EXECUTABILITY_STANDARD = false;
  }

  if (!Array.isArray(action?.implementationSteps) || action.implementationSteps.length < 4) {
    failures.push("implementation_steps_too_few");
    gates.ADP_ACTION_IMPLEMENTATION_STEPS_COMPLETE = false;
    gates.ADP_ACTION_EXECUTABILITY_STANDARD = false;
  }

  if (!Array.isArray(action?.targetSources) || action.targetSources.length < 1) {
    failures.push("target_sources_missing");
    gates.ADP_ACTION_TARGET_SOURCE_SPECIFICITY = false;
  }

  if (!Array.isArray(action?.definitionOfDone) || action.definitionOfDone.length < 3) {
    failures.push("definition_of_done_incomplete");
    gates.ADP_ACTION_DEFINITION_OF_DONE_COMPLETE = false;
  }

  const dodText = (action?.definitionOfDone || []).join(" ").toLowerCase();
  if (/^content updated$/.test(dodText.trim()) || dodText === "content updated") {
    failures.push("vague_definition_of_done");
    gates.ADP_ACTION_DEFINITION_OF_DONE_COMPLETE = false;
  }

  const trace = action?.trace || {};
  if (
    !trace.observedMetric &&
    !trace.scenarioOrTerritory &&
    !trace.evidenceExample &&
    !trace.sourceGap
  ) {
    failures.push("trace_incomplete");
    gates.ADP_ACTION_TRACEABILITY = false;
  }
  if (!trace.actionPatternId && !action?.actionPatternId) {
    failures.push("trace_missing_pattern");
    gates.ADP_ACTION_TRACEABILITY = false;
  }

  const pattern = getActionPattern(action?.actionPatternId);
  if (!pattern) {
    failures.push("unknown_action_pattern");
    gates.ADP_ACTION_PATTERN_LEARNING_STRUCTURE_COMPLETE = false;
  } else if (!action?.learningRecord?.actionPatternId) {
    failures.push("learning_record_missing");
    gates.ADP_ACTION_PATTERN_LEARNING_STRUCTURE_COMPLETE = false;
  }

  if (!action?.accountableOwnerRole) {
    failures.push("missing_owner");
    gates.ADP_ACTION_EXECUTABILITY_STANDARD = false;
  }

  const ok = failures.length === 0;
  if (!ok) gates.ADP_ACTION_EXECUTABILITY_STANDARD = false;

  return { ok, failures, gates };
}

export function validateExecutableActionAgenda(actions) {
  const results = (actions || []).map((a) => ({
    actionId: a.actionId,
    ...validateExecutableActionInstance(a),
  }));
  const ok = results.every((r) => r.ok);
  const aggregateGates = Object.fromEntries(
    ACTION_INTELLIGENCE_GATES.map((g) => [g, results.every((r) => r.gates[g])])
  );
  return {
    ok,
    aggregateGates,
    results,
    rejectedCount: results.filter((r) => !r.ok).length,
  };
}
