/**
 * GDI Jev decision types + choice catalogs + System One question builders.
 */

export const JEV_DECISION_TYPE = Object.freeze({
  TARGET_RESEARCH_PRIORITY: "TARGET_RESEARCH_PRIORITY",
  RESEARCH_PLAYBOOK: "RESEARCH_PLAYBOOK",
  MATERIAL_CHANGE: "MATERIAL_CHANGE",
  FOLLOWUP_VALUE: "FOLLOWUP_VALUE",
  FOLLOWUP_TYPE: "FOLLOWUP_TYPE",
  SIGNAL_RELEVANCE: "SIGNAL_RELEVANCE",
  LODGING_SIGNAL_STRENGTH: "LODGING_SIGNAL_STRENGTH",
  EVENT_FORWARDNESS: "EVENT_FORWARDNESS",
  LOCAL_NO_ROOM_RISK: "LOCAL_NO_ROOM_RISK",
  SOURCE_UTILITY: "SOURCE_UTILITY",
  GENERATOR_CADENCE: "GENERATOR_CADENCE",
  PRIVATE_EVENT_SIGNAL_QUALITY: "PRIVATE_EVENT_SIGNAL_QUALITY",
  VENUE_PARTNERSHIP_ROUTING: "VENUE_PARTNERSHIP_ROUTING",
  OPPORTUNITY_PREQUAL: "OPPORTUNITY_PREQUAL",
  STOP_CONTINUE: "STOP_CONTINUE",
});

export const CHOICES = Object.freeze({
  TARGET_RESEARCH_PRIORITY: [
    "RESEARCH_NOW",
    "DEFER",
    "LOWER_PRIORITY",
    "PAUSE",
    "RETIRE_CANDIDATE",
  ],
  RESEARCH_PLAYBOOK: [
    "EVENT_FUTURE_CYCLE",
    "LODGING_HOUSING",
    "GEO_VERIFICATION",
    "SOURCE_AUTHORITY",
    "TRAINING_PROGRAM",
    "GOVERNMENT_PROJECT",
    "CORPORATE_MOBILIZATION",
    "SPORTS_HOUSING",
    "PRIVATE_EVENT_SIGNAL",
    "VENUE_PARTNERSHIP",
    "DEMAND_GENERATOR",
    "GENERAL_FOLLOWUP",
    "STOP",
  ],
  MATERIAL_CHANGE: ["MATERIAL_CHANGE", "NON_MATERIAL_CHANGE", "UNCERTAIN"],
  FOLLOWUP_VALUE: [
    "FOLLOWUP_HIGH_VALUE",
    "FOLLOWUP_LOW_VALUE",
    "STOP_NO_USEFUL_EVIDENCE",
  ],
  FOLLOWUP_TYPE: [
    "LODGING",
    "FUTURE_CYCLE",
    "GEO",
    "VENUE",
    "SOURCE",
    "ROOM_DEMAND",
    "PROGRAM",
    "MARKET",
    "PARTNERSHIP",
    "NONE",
  ],
  SIGNAL_RELEVANCE: [
    "STRONG_DEMAND_SIGNAL",
    "POSSIBLE_DEMAND_SIGNAL",
    "WEAK_SIGNAL",
    "NO_DEMAND_SIGNAL",
  ],
  LODGING_SIGNAL_STRENGTH: ["STRONG", "MODERATE", "WEAK", "NONE", "UNKNOWN"],
  EVENT_FORWARDNESS: [
    "CURRENT_FUTURE",
    "HISTORICAL_ONLY",
    "AMBIGUOUS",
    "UNDATED",
  ],
  LOCAL_NO_ROOM_RISK: [
    "LIKELY_LODGING_DEMAND",
    "POSSIBLE_LODGING_DEMAND",
    "LIKELY_LOCAL_NO_ROOM",
    "UNKNOWN",
  ],
  SOURCE_UTILITY: [
    "PRIMARY_EVIDENCE",
    "SUPPORTING_EVIDENCE",
    "LOW_VALUE",
    "NOISE",
  ],
  GENERATOR_CADENCE: [
    "INCREASE_CADENCE",
    "KEEP_CADENCE",
    "RELAX_CADENCE",
    "REVIEW_FOR_PAUSE",
  ],
  PRIVATE_EVENT_SIGNAL_QUALITY: [
    "SPECIFIC_EVENT_CANDIDATE",
    "VENUE_ACTIVITY_EVIDENCE",
    "PARTNERSHIP_EVIDENCE",
    "PRIVACY_REJECT",
    "NOISE",
  ],
  VENUE_PARTNERSHIP_ROUTING: [
    "RESEARCH_ACTIVITY",
    "RESEARCH_LODGING",
    "RESEARCH_PARTNER_STATUS",
    "RESEARCH_CONTACT_PATH",
    "READY_FOR_POLICY_CHECK",
    "STOP",
  ],
  OPPORTUNITY_PREQUAL: [
    "LIKELY_ACTIONABLE",
    "LIKELY_WATCH",
    "LIKELY_REJECT",
    "NEEDS_MORE_EVIDENCE",
  ],
  STOP_CONTINUE: [
    "CONTINUE_RESEARCH",
    "RUN_SPECIFIC_FOLLOWUP",
    "STOP_NO_USEFUL_NEW_EVIDENCE",
  ],
});

const INSTRUCTIONS = Object.freeze({
  TARGET_RESEARCH_PRIORITY:
    "From structured research-target state only, choose the next monitoring priority. RESEARCH_NOW when due and historically productive; DEFER when recently researched with no material change; LOWER_PRIORITY when yield is weak; PAUSE when stalled; RETIRE_CANDIDATE only when permanently unproductive. Prefer DEFER over RETIRE when uncertain. Do not invent facts.",
  RESEARCH_PLAYBOOK:
    "Which research playbook best addresses the current unresolved evidence gap? Use only provided target type, missing evidence, lodging/geo/source status, and evidenceSummary. Prefer the gap-specific playbook over a generic type default. Prefer STOP only when no useful research path remains. Do not invent facts.",
  MATERIAL_CHANGE:
    "Classify whether the described change is commercially material for hotel group/demand pursuit. New URL alone is NON_MATERIAL_CHANGE. Use only materialFieldsChanged and evidenceSummary.",
  FOLLOWUP_VALUE:
    "Decide whether a second research pass is likely to add commercially useful evidence given missingFields, prior failures, and evidenceSummary.",
  FOLLOWUP_TYPE:
    "Which single follow-up type best closes the highest-value unresolved evidence gap? Choose NONE if no useful follow-up remains.",
  SIGNAL_RELEVANCE:
    "Triage demand-signal relevance for hotel lodging/group demand from provided evidence only. Not final qualification.",
  LODGING_SIGNAL_STRENGTH:
    "Rate lodging/housing demand evidence strength using only provided lodging fields and evidenceSummary.",
  EVENT_FORWARDNESS:
    "Classify whether the event/program is current/future vs historical-only using provided date/status fields. Prefer AMBIGUOUS when undated.",
  LOCAL_NO_ROOM_RISK:
    "Estimate lodging vs local no-room risk from provided attendance/geo/lodging evidence. UNKNOWN when insufficient.",
  SOURCE_UTILITY:
    "Rate how useful the cited source is as commercial evidence. Do not invent source authority beyond provided fields.",
  GENERATOR_CADENCE:
    "Recommend research cadence adjustment from yield, no-change streak, program timing, and last material signal. REVIEW_FOR_PAUSE only when repeatedly unproductive; never permanent suppress.",
  PRIVATE_EVENT_SIGNAL_QUALITY:
    "Classify private-event related signal quality. Prefer PRIVACY_REJECT for couple celebrations/personal pages when hardPolicyContext or privacyFlags indicate it.",
  VENUE_PARTNERSHIP_ROUTING:
    "Choose the next venue-partnership research gap to close. Do not promote partnership readiness alone.",
  OPPORTUNITY_PREQUAL:
    "Pre-triage commercial likelihood before deterministic Commercial Quality. NEEDS_MORE_EVIDENCE when vital fields missing. Respect hardPolicyContext; do not invent dates/attendance/rooms.",
  STOP_CONTINUE:
    "Decide whether continuing research is useful given queries already run, missing fields, prior failures, and cost. Prefer CONTINUE or RUN_SPECIFIC_FOLLOWUP when a lodging/future-cycle/geo/source path remains open.",
});

const CHOICE_NOTES = Object.freeze({
  RESEARCH_PLAYBOOK: {
    EVENT_FUTURE_CYCLE: "best next step is confirming or finding a future cycle / date",
    LODGING_HOUSING: "best next step is lodging, housing, or room-block evidence",
    GEO_VERIFICATION: "best next step is geographic fit verification",
    SOURCE_AUTHORITY: "best next step is finding or validating an authoritative source",
    PRIVATE_EVENT_SIGNAL: "best next step is private-event venue/activity signal research",
    VENUE_PARTNERSHIP: "best next step is venue partnership status/activity research",
    DEMAND_GENERATOR: "best next step is demand-generator / organization program monitoring",
    GENERAL_FOLLOWUP: "generic bounded follow-up when no sharper gap is known",
    STOP: "no useful research path remains",
  },
  FOLLOWUP_TYPE: {
    LODGING: "lodging / housing evidence gap",
    FUTURE_CYCLE: "future date / cycle gap",
    GEO: "geography gap",
    VENUE: "venue gap",
    SOURCE: "source authority gap",
    ROOM_DEMAND: "room demand / peak rooms gap",
    NONE: "no useful follow-up",
  },
  TARGET_RESEARCH_PRIORITY: {
    RESEARCH_NOW: "research in this cycle",
    DEFER: "defer to a later scheduled cycle",
    LOWER_PRIORITY: "keep monitoring but lower urgency",
    PAUSE: "temporarily pause research",
    RETIRE_CANDIDATE: "candidate for retirement (advisory only; policy must not auto-retire)",
  },
  GENERATOR_CADENCE: {
    INCREASE_CADENCE: "increase monitoring frequency",
    KEEP_CADENCE: "keep current cadence",
    RELAX_CADENCE: "relax cadence without permanent suppression",
    REVIEW_FOR_PAUSE: "review for temporary pause",
  },
});

function criteriaFromChoices(choices, notes = {}) {
  const out = {};
  for (const c of choices) {
    out[c] = notes[c] || c.replace(/_/g, " ").toLowerCase();
  }
  return out;
}

/**
 * Build TypeSafe System One questions object for one GDI decision.
 * @param {string} decisionType
 * @param {{ choices?: string[] }} [opts]
 */
export function buildJevQuestion(decisionType, opts = {}) {
  const all = CHOICES[decisionType];
  if (!all) {
    throw new Error(`unknown_jev_decision_type:${decisionType}`);
  }
  const choices =
    Array.isArray(opts.choices) && opts.choices.length
      ? opts.choices.filter((c) => all.includes(c))
      : all;
  const finalChoices = choices.length ? choices : all;
  return {
    [decisionType]: {
      type: "choice",
      instructions: INSTRUCTIONS[decisionType] || `Select ${decisionType}`,
      criteria: criteriaFromChoices(finalChoices, CHOICE_NOTES[decisionType] || {}),
    },
  };
}

export function isValidChoice(decisionType, selected) {
  const choices = CHOICES[decisionType] || [];
  return choices.includes(String(selected || ""));
}

/** Hard-gate codes that always defeat Jev recommendations. */
export const HARD_GATES = Object.freeze({
  PAST_EVENT: "PAST_EVENT",
  FULLY_PLACED: "FULLY_PLACED",
  NO_GEOGRAPHIC_FIT: "NO_GEOGRAPHIC_FIT",
  EXCLUSIVE_PARTNER_FOUND: "EXCLUSIVE_PARTNER_FOUND",
  NO_VALID_SOURCE: "NO_VALID_SOURCE",
  PRIVACY_REJECT: "PRIVACY_REJECT",
  INVALID_DATE: "INVALID_DATE",
  DUPLICATE_CANONICAL_OPPORTUNITY: "DUPLICATE_CANONICAL_OPPORTUNITY",
});
