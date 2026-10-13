/**
 * Evidence/policy adjudication for Jev vs current GDI disagreements.
 * Does NOT treat either side as automatic ground truth.
 */

export const ADJUDICATION = Object.freeze({
  JEV_CORRECT: "JEV_CORRECT",
  CURRENT_GDI_CORRECT: "CURRENT_GDI_CORRECT",
  BOTH_ACCEPTABLE: "BOTH_ACCEPTABLE",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
});

export const JEV_ERROR_CAUSE = Object.freeze({
  MISSING_CONTEXT: "MISSING_CONTEXT",
  AMBIGUOUS_CHOICES: "AMBIGUOUS_CHOICES",
  INPUT_TOO_THIN: "INPUT_TOO_THIN",
  POLICY_CONTEXT_MISSING: "POLICY_CONTEXT_MISSING",
  CONFIDENCE_MISCALIBRATED: "CONFIDENCE_MISCALIBRATED",
  OTHER: "OTHER",
});

export const GDI_ERROR_CAUSE = Object.freeze({
  ROUTING_TOO_GENERIC: "ROUTING_TOO_GENERIC",
  PREMATURE_STOP: "PREMATURE_STOP",
  BAD_PRIORITY: "BAD_PRIORITY",
  BAD_CADENCE: "BAD_CADENCE",
  OTHER: "OTHER",
});

const PURSUE = new Set([
  "RESEARCH_NOW",
  "LIKELY_ACTIONABLE",
  "STRONG_DEMAND_SIGNAL",
  "FOLLOWUP_HIGH_VALUE",
  "CONTINUE_RESEARCH",
  "SPECIFIC_EVENT_CANDIDATE",
  "LODGING_HOUSING",
  "INCREASE_CADENCE",
]);

const STOP = new Set([
  "STOP",
  "STOP_NO_USEFUL_EVIDENCE",
  "STOP_NO_USEFUL_NEW_EVIDENCE",
  "LIKELY_REJECT",
  "NO_DEMAND_SIGNAL",
  "PAUSE",
  "RETIRE_CANDIDATE",
  "PRIVACY_REJECT",
]);

/**
 * Adjudicate one disagreement from structured evidence + policy.
 */
export function adjudicateDisagreement(row = {}) {
  const {
    decisionType,
    existingGdiDecision,
    jevDecision,
    jevConfidence,
    context = {},
    policyContext = {},
    hardGates = [],
    inputNotes = {},
  } = row;

  const conf = typeof jevConfidence === "number" ? jevConfidence : null;
  const gates = hardGates.length
    ? hardGates
    : Array.isArray(policyContext.hardGates)
      ? policyContext.hardGates
      : [];

  // Hard gates: if Jev pursued against a hard reject, GDI is correct.
  if (gates.length && PURSUE.has(jevDecision) && STOP.has(existingGdiDecision)) {
    return finalize({
      adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
      jevErrorCause: JEV_ERROR_CAUSE.POLICY_CONTEXT_MISSING,
      explanation: `Hard gate ${gates.join(",")} defeats Jev pursue-like ${jevDecision}; current GDI reject/stop stands.`,
      whyGdi: `Deterministic hard gate(s): ${gates.join(",")}`,
      whyJev: `Selected ${jevDecision} despite hard-policy context`,
      whichBetter: "CURRENT_GDI",
    });
  }

  // Hard gates where Jev differed but policy would override → BOTH if Jev not pursue-harmful
  if (gates.length && !PURSUE.has(jevDecision)) {
    return finalize({
      adjudication: ADJUDICATION.BOTH_ACCEPTABLE,
      explanation: `Hard gates present (${gates.join(",")}); Jev ${jevDecision} vs GDI ${existingGdiDecision} — policy remains authoritative.`,
      whyGdi: `Existing decision under hard gate path`,
      whyJev: `Non-pursue alternative ${jevDecision}`,
      whichBetter: "BOTH",
    });
  }

  if (decisionType === "RESEARCH_PLAYBOOK") {
    return adjudicatePlaybook(row, conf);
  }
  if (decisionType === "TARGET_RESEARCH_PRIORITY") {
    return adjudicatePriority(row, conf);
  }
  if (decisionType === "FOLLOWUP_TYPE") {
    return adjudicateFollowupType(row, conf);
  }
  if (decisionType === "GENERATOR_CADENCE") {
    return adjudicateCadence(row, conf);
  }
  if (decisionType === "OPPORTUNITY_PREQUAL") {
    return adjudicatePrequal(row, conf);
  }
  if (decisionType === "STOP_CONTINUE") {
    return adjudicateStop(row, conf);
  }
  if (decisionType === "SOURCE_UTILITY") {
    if (policyContext.noValidSource && jevDecision !== "NOISE") {
      return finalize({
        adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
        jevErrorCause: JEV_ERROR_CAUSE.POLICY_CONTEXT_MISSING,
        explanation: "NO_VALID_SOURCE gate → NOISE is correct; Jev softer label is wrong for policy.",
        whyGdi: "NO_VALID_SOURCE ⇒ NOISE",
        whyJev: String(jevDecision),
        whichBetter: "CURRENT_GDI",
      });
    }
  }

  // Thin input from known V1 bias
  if (inputNotes.forcedLodgingMissing === true) {
    return finalize({
      adjudication: ADJUDICATION.INSUFFICIENT_EVIDENCE,
      jevErrorCause: JEV_ERROR_CAUSE.INPUT_TOO_THIN,
      explanation:
        "V1 canary always set missingFields=[lodgingEvidence], biasing playbook toward LODGING_HOUSING. Disagreement is not a clean capability signal.",
      whyGdi: String(existingGdiDecision),
      whyJev: String(jevDecision),
      whichBetter: "UNKNOWN",
    });
  }

  if (!context || Object.keys(context).length < 2) {
    return finalize({
      adjudication: ADJUDICATION.INSUFFICIENT_EVIDENCE,
      jevErrorCause: JEV_ERROR_CAUSE.INPUT_TOO_THIN,
      explanation: "Insufficient structured context to adjudicate.",
      whyGdi: String(existingGdiDecision),
      whyJev: String(jevDecision),
      whichBetter: "UNKNOWN",
    });
  }

  // Near-synonym / both reasonable routing
  if (areCompatibleChoices(decisionType, existingGdiDecision, jevDecision, context)) {
    return finalize({
      adjudication: ADJUDICATION.BOTH_ACCEPTABLE,
      explanation: "Both choices are policy-acceptable for the available evidence.",
      whyGdi: String(existingGdiDecision),
      whyJev: String(jevDecision),
      whichBetter: "BOTH",
      gdiErrorCause: GDI_ERROR_CAUSE.ROUTING_TOO_GENERIC,
    });
  }

  return finalize({
    adjudication: ADJUDICATION.INSUFFICIENT_EVIDENCE,
    explanation: "No strong evidence-based preference between sides.",
    whyGdi: String(existingGdiDecision),
    whyJev: String(jevDecision),
    whichBetter: "UNKNOWN",
  });
}

function adjudicatePlaybook(row, conf) {
  const { existingGdiDecision, jevDecision, context = {}, inputNotes = {} } = row;
  const missing = Array.isArray(context.missingFields) ? context.missingFields : [];
  const oppType = String(context.opportunityType || "");
  const targetType = String(context.targetType || "");

  if (inputNotes.forcedLodgingMissing === true && jevDecision === "LODGING_HOUSING") {
    // Type-default GDI may still be better for PE venues / DG monitoring without real lodging gap
    if (targetType === "PRIVATE_EVENT_VENUE" && existingGdiDecision === "PRIVATE_EVENT_SIGNAL") {
      return finalize({
        adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
        jevErrorCause: JEV_ERROR_CAUSE.INPUT_TOO_THIN,
        explanation:
          "Forced lodging-missing input steered Jev to LODGING_HOUSING; PE venue targets should prefer PRIVATE_EVENT_SIGNAL first.",
        whyGdi: "Target-type default PRIVATE_EVENT_SIGNAL matches venue monitoring purpose",
        whyJev: "Responded to synthetic lodging gap",
        whichBetter: "CURRENT_GDI",
      });
    }
    return finalize({
      adjudication: ADJUDICATION.INSUFFICIENT_EVIDENCE,
      jevErrorCause: JEV_ERROR_CAUSE.INPUT_TOO_THIN,
      explanation: "Synthetic lodging missingFields bias makes disagreement non-comparable.",
      whyGdi: String(existingGdiDecision),
      whyJev: String(jevDecision),
      whichBetter: "UNKNOWN",
    });
  }

  // Real lodging gap for overflow / housing opportunities
  if (
    jevDecision === "LODGING_HOUSING" &&
    (oppType === "OVERFLOW_HOUSING" ||
      missing.includes("lodgingEvidence") ||
      missing.includes("roomDemand") ||
      context.roomDemandStatus === "UNKNOWN" ||
      context.lodgingEvidence === "unknown")
  ) {
    if (
      existingGdiDecision === "DEMAND_GENERATOR" ||
      existingGdiDecision === "EVENT_FUTURE_CYCLE" ||
      existingGdiDecision === "GENERAL_FOLLOWUP"
    ) {
      return finalize({
        adjudication: ADJUDICATION.JEV_CORRECT,
        gdiErrorCause: GDI_ERROR_CAUSE.ROUTING_TOO_GENERIC,
        explanation:
          "Lodging/housing is the unresolved commercial gap; type-default playbook is too generic.",
        whyGdi: `Type-default ${existingGdiDecision}`,
        whyJev: "Gap-specific LODGING_HOUSING",
        whichBetter: "JEV",
      });
    }
  }

  // Future cycle gap
  if (
    jevDecision === "EVENT_FUTURE_CYCLE" &&
    (context.futureDateStatus === "UNKNOWN" ||
      context.futureCycleEvidenceState === "FUTURE_CYCLE_UNCONFIRMED" ||
      missing.includes("futureCycle"))
  ) {
    return finalize({
      adjudication: ADJUDICATION.JEV_CORRECT,
      gdiErrorCause: GDI_ERROR_CAUSE.ROUTING_TOO_GENERIC,
      explanation: "Future-cycle confirmation is the open gap.",
      whyGdi: String(existingGdiDecision),
      whyJev: "EVENT_FUTURE_CYCLE",
      whichBetter: "JEV",
    });
  }

  if (
    existingGdiDecision === jevDecision ||
    areCompatibleChoices("RESEARCH_PLAYBOOK", existingGdiDecision, jevDecision, context)
  ) {
    return finalize({
      adjudication: ADJUDICATION.BOTH_ACCEPTABLE,
      explanation: "Playbook choices are compatible for this target.",
      whyGdi: String(existingGdiDecision),
      whyJev: String(jevDecision),
      whichBetter: "BOTH",
    });
  }

  if ((conf || 0) >= 0.9 && !missing.length && !oppType) {
    return finalize({
      adjudication: ADJUDICATION.INSUFFICIENT_EVIDENCE,
      jevErrorCause: JEV_ERROR_CAUSE.CONFIDENCE_MISCALIBRATED,
      explanation: "High-confidence playbook disagreement without clear gap evidence.",
      whyGdi: String(existingGdiDecision),
      whyJev: String(jevDecision),
      whichBetter: "UNKNOWN",
    });
  }

  return finalize({
    adjudication: ADJUDICATION.INSUFFICIENT_EVIDENCE,
    explanation: "Playbook disagreement without decisive gap evidence.",
    whyGdi: String(existingGdiDecision),
    whyJev: String(jevDecision),
    whichBetter: "UNKNOWN",
  });
}

function adjudicatePriority(row, conf) {
  const { existingGdiDecision, jevDecision, context = {} } = row;
  const streak = Number(context.consecutiveNoChangeRuns || 0);
  const priority = String(context.priority || "");
  const last = String(context.lastResult || "");

  if (existingGdiDecision === "RESEARCH_NOW" && jevDecision === "DEFER") {
    if (streak >= 3 || last === "NO_CHANGE") {
      return finalize({
        adjudication: ADJUDICATION.JEV_CORRECT,
        gdiErrorCause: GDI_ERROR_CAUSE.BAD_PRIORITY,
        explanation: "Priority=HIGH alone should not force RESEARCH_NOW after repeated no-change.",
        whyGdi: "HIGH → RESEARCH_NOW rule",
        whyJev: "DEFER after no-change streak / weak yield",
        whichBetter: "JEV",
      });
    }
    if (priority === "HIGH" && streak === 0 && context.researchReason === "due") {
      return finalize({
        adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
        jevErrorCause: JEV_ERROR_CAUSE.MISSING_CONTEXT,
        explanation: "Due HIGH target with no no-change streak should research now.",
        whyGdi: "RESEARCH_NOW for due HIGH",
        whyJev: "DEFER",
        whichBetter: "CURRENT_GDI",
      });
    }
    return finalize({
      adjudication: ADJUDICATION.BOTH_ACCEPTABLE,
      explanation: "Priority timing is ambiguous without richer yield history.",
      whyGdi: String(existingGdiDecision),
      whyJev: String(jevDecision),
      whichBetter: "BOTH",
    });
  }

  if (jevDecision === "RETIRE_CANDIDATE") {
    return finalize({
      adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
      jevErrorCause: JEV_ERROR_CAUSE.OTHER,
      explanation: "RETIRE_CANDIDATE must remain advisory; current GDI keep/defer is safer in V1.1.",
      whyGdi: String(existingGdiDecision),
      whyJev: "RETIRE_CANDIDATE",
      whichBetter: "CURRENT_GDI",
    });
  }

  return finalize({
    adjudication: ADJUDICATION.BOTH_ACCEPTABLE,
    explanation: "Priority alternatives are within advisory tolerance.",
    whyGdi: String(existingGdiDecision),
    whyJev: String(jevDecision),
    whichBetter: "BOTH",
  });
}

function adjudicateFollowupType(row) {
  const { existingGdiDecision, jevDecision, context = {} } = row;
  const missing = new Set((context.missingFields || []).map(String));
  const map = {
    lodgingEvidence: "LODGING",
    roomDemand: "ROOM_DEMAND",
    futureCycle: "FUTURE_CYCLE",
    geo: "GEO",
    venue: "VENUE",
    source: "SOURCE",
    partnership: "PARTNERSHIP",
  };
  for (const [field, choice] of Object.entries(map)) {
    if (missing.has(field) && jevDecision === choice) {
      return finalize({
        adjudication: ADJUDICATION.JEV_CORRECT,
        gdiErrorCause:
          existingGdiDecision === "NONE" || existingGdiDecision === "PROGRAM"
            ? GDI_ERROR_CAUSE.ROUTING_TOO_GENERIC
            : GDI_ERROR_CAUSE.OTHER,
        explanation: `Missing ${field} aligns with follow-up ${choice}.`,
        whyGdi: String(existingGdiDecision),
        whyJev: String(jevDecision),
        whichBetter: "JEV",
      });
    }
  }
  return finalize({
    adjudication: ADJUDICATION.BOTH_ACCEPTABLE,
    explanation: "Follow-up type alternatives both plausible.",
    whyGdi: String(existingGdiDecision),
    whyJev: String(jevDecision),
    whichBetter: "BOTH",
  });
}

function adjudicateCadence(row) {
  const { existingGdiDecision, jevDecision, context = {} } = row;
  const streak = Number(context.consecutiveNoChangeRuns || 0);
  if (streak >= 4 && jevDecision === "RELAX_CADENCE" && existingGdiDecision === "KEEP_CADENCE") {
    return finalize({
      adjudication: ADJUDICATION.JEV_CORRECT,
      gdiErrorCause: GDI_ERROR_CAUSE.BAD_CADENCE,
      explanation: "Long no-change streak supports relaxing cadence.",
      whyGdi: "KEEP_CADENCE default",
      whyJev: "RELAX_CADENCE",
      whichBetter: "JEV",
    });
  }
  if (jevDecision === "REVIEW_FOR_PAUSE" && streak < 2) {
    return finalize({
      adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
      jevErrorCause: JEV_ERROR_CAUSE.CONFIDENCE_MISCALIBRATED,
      explanation: "Pause review premature without longer no-change history.",
      whyGdi: String(existingGdiDecision),
      whyJev: "REVIEW_FOR_PAUSE",
      whichBetter: "CURRENT_GDI",
    });
  }
  return finalize({
    adjudication: ADJUDICATION.BOTH_ACCEPTABLE,
    explanation: "Cadence advisory band — both acceptable.",
    whyGdi: String(existingGdiDecision),
    whyJev: String(jevDecision),
    whichBetter: "BOTH",
  });
}

function adjudicatePrequal(row) {
  const { existingGdiDecision, jevDecision, policyContext = {}, hardGates = [] } = row;
  if (hardGates.length || policyContext.pastEvent || policyContext.fullyPlaced) {
    if (existingGdiDecision === "LIKELY_REJECT") {
      return finalize({
        adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
        jevErrorCause: JEV_ERROR_CAUSE.POLICY_CONTEXT_MISSING,
        explanation: "Hard commercial reject remains correct vs softer Jev prequal.",
        whyGdi: "LIKELY_REJECT under hard gate",
        whyJev: String(jevDecision),
        whichBetter: "CURRENT_GDI",
      });
    }
  }
  if (
    existingGdiDecision === "LIKELY_ACTIONABLE" &&
    jevDecision === "NEEDS_MORE_EVIDENCE" &&
    (row.context?.missingFields || []).length
  ) {
    return finalize({
      adjudication: ADJUDICATION.BOTH_ACCEPTABLE,
      explanation: "TRUE actionable label vs more-evidence prequal can both be honest before CQ.",
      whyGdi: "LIKELY_ACTIONABLE",
      whyJev: "NEEDS_MORE_EVIDENCE",
      whichBetter: "BOTH",
    });
  }
  return finalize({
    adjudication: ADJUDICATION.INSUFFICIENT_EVIDENCE,
    explanation: "Prequal disagreement needs full CQ field review.",
    whyGdi: String(existingGdiDecision),
    whyJev: String(jevDecision),
    whichBetter: "UNKNOWN",
  });
}

function adjudicateStop(row) {
  const { existingGdiDecision, jevDecision, context = {} } = row;
  const missing = context.missingFields || [];
  if (
    jevDecision === "CONTINUE_RESEARCH" &&
    existingGdiDecision === "STOP_NO_USEFUL_NEW_EVIDENCE" &&
    missing.length === 0 &&
    (context.queriesAlreadyRun || 0) >= 5
  ) {
    return finalize({
      adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
      jevErrorCause: JEV_ERROR_CAUSE.CONFIDENCE_MISCALIBRATED,
      explanation: "Exhausted query budget with no missing fields → stop is correct.",
      whyGdi: "STOP after exhausted research",
      whyJev: "CONTINUE_RESEARCH",
      whichBetter: "CURRENT_GDI",
    });
  }
  if (
    existingGdiDecision === "CONTINUE_RESEARCH" &&
    STOP.has(jevDecision) &&
    missing.includes("lodgingEvidence")
  ) {
    return finalize({
      adjudication: ADJUDICATION.CURRENT_GDI_CORRECT,
      jevErrorCause: JEV_ERROR_CAUSE.OTHER,
      gdiErrorCause: null,
      explanation: "Open lodging gap should not be stopped.",
      whyGdi: "CONTINUE with lodging gap",
      whyJev: String(jevDecision),
      whichBetter: "CURRENT_GDI",
    });
  }
  return finalize({
    adjudication: ADJUDICATION.INSUFFICIENT_EVIDENCE,
    explanation: "Stop/continue needs outcome-linked validation; keep shadow.",
    whyGdi: String(existingGdiDecision),
    whyJev: String(jevDecision),
    whichBetter: "UNKNOWN",
  });
}

function areCompatibleChoices(decisionType, a, b, context = {}) {
  if (String(a) === String(b)) return true;
  const pairs = [
    ["DEMAND_GENERATOR", "EVENT_FUTURE_CYCLE"],
    ["DEMAND_GENERATOR", "GENERAL_FOLLOWUP"],
    ["EVENT_FUTURE_CYCLE", "GENERAL_FOLLOWUP"],
    ["LODGING", "ROOM_DEMAND"],
    ["DEFER", "LOWER_PRIORITY"],
    ["KEEP_CADENCE", "RELAX_CADENCE"],
    ["LIKELY_WATCH", "NEEDS_MORE_EVIDENCE"],
    ["LOW_VALUE", "NOISE"],
  ];
  for (const [x, y] of pairs) {
    if ((a === x && b === y) || (a === y && b === x)) return true;
  }
  if (
    decisionType === "RESEARCH_PLAYBOOK" &&
    context.opportunityType === "OVERFLOW_HOUSING" &&
    ((a === "LODGING_HOUSING" && b === "EVENT_FUTURE_CYCLE") ||
      (b === "LODGING_HOUSING" && a === "EVENT_FUTURE_CYCLE"))
  ) {
    return true;
  }
  return false;
}

function finalize( partial ) {
  return {
    adjudication: partial.adjudication,
    jevErrorCause: partial.jevErrorCause || null,
    gdiErrorCause: partial.gdiErrorCause || null,
    explanation: partial.explanation,
    whyGdi: partial.whyGdi,
    whyJev: partial.whyJev,
    whichBetter: partial.whichBetter,
  };
}

/**
 * Confidence calibration bins.
 */
export function binConfidence(confidence) {
  if (typeof confidence !== "number" || Number.isNaN(confidence)) return null;
  if (confidence < 0.5) return "below_0.50";
  if (confidence < 0.6) return "0.50–0.59";
  if (confidence < 0.7) return "0.60–0.69";
  if (confidence < 0.8) return "0.70–0.79";
  if (confidence < 0.9) return "0.80–0.89";
  return "0.90–1.00";
}

export function summarizeAdjudications(rows = []) {
  const counts = {
    JEV_CORRECT: 0,
    CURRENT_GDI_CORRECT: 0,
    BOTH_ACCEPTABLE: 0,
    INSUFFICIENT_EVIDENCE: 0,
  };
  const highConf = {
    HIGH_CONF_JEV_CORRECT: 0,
    HIGH_CONF_GDI_CORRECT: 0,
    HIGH_CONF_BOTH: 0,
    HIGH_CONF_INSUFFICIENT: 0,
  };
  const jevCauses = {};
  const gdiCauses = {};
  const bins = {};

  for (const r of rows) {
    const a = r.adjudication || r.result?.adjudication;
    if (counts[a] != null) counts[a] += 1;
    const conf = r.jevConfidence ?? r.confidence;
    const high = (conf || 0) >= 0.7;
    if (high) {
      if (a === "JEV_CORRECT") highConf.HIGH_CONF_JEV_CORRECT += 1;
      else if (a === "CURRENT_GDI_CORRECT") highConf.HIGH_CONF_GDI_CORRECT += 1;
      else if (a === "BOTH_ACCEPTABLE") highConf.HIGH_CONF_BOTH += 1;
      else highConf.HIGH_CONF_INSUFFICIENT += 1;
    }
    const jc = r.jevErrorCause || r.result?.jevErrorCause;
    const gc = r.gdiErrorCause || r.result?.gdiErrorCause;
    if (jc) jevCauses[jc] = (jevCauses[jc] || 0) + 1;
    if (gc) gdiCauses[gc] = (gdiCauses[gc] || 0) + 1;

    const bin = binConfidence(conf);
    if (bin) {
      if (!bins[bin]) {
        bins[bin] = {
          n: 0,
          JEV_CORRECT: 0,
          CURRENT_GDI_CORRECT: 0,
          BOTH_ACCEPTABLE: 0,
          INSUFFICIENT_EVIDENCE: 0,
        };
      }
      bins[bin].n += 1;
      if (bins[bin][a] != null) bins[bin][a] += 1;
    }
  }

  return { counts, highConf, jevCauses, gdiCauses, bins };
}
