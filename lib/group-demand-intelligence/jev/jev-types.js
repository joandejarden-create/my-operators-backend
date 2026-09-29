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
  /** Research-direction only — SAFE APPLY must not create customer facts. */
  HIDDEN_DEMAND_NEXT_LAYER: "HIDDEN_DEMAND_NEXT_LAYER",
  /** Next structured source to fetch — SAFE APPLY research direction only. */
  STRUCTURED_SOURCE_PRIORITY: "STRUCTURED_SOURCE_PRIORITY",
  /** Exhibitor company → team evidence path — SAFE APPLY research direction only. */
  EXHIBITOR_TEAM_RESEARCH_PATH: "EXHIBITOR_TEAM_RESEARCH_PATH",
  /** Lodging evidence next step — SAFE APPLY research direction only. */
  LODGING_EVIDENCE_NEXT_STEP: "LODGING_EVIDENCE_NEXT_STEP",
  /** Validate source before fetch/extract — SAFE APPLY research direction only. */
  SOURCE_VALIDATION_NEXT_STEP: "SOURCE_VALIDATION_NEXT_STEP",
  /** Evidence-gap next research action — SAFE APPLY research direction only; never mutates facts. */
  EVIDENCE_GAP_NEXT_ACTION: "EVIDENCE_GAP_NEXT_ACTION",
  // Contact Intelligence V1.1 — shadow routing only
  CONTACT_SOURCE_PATH: "CONTACT_SOURCE_PATH",
  CONTACT_FOLLOWUP_TYPE: "CONTACT_FOLLOWUP_TYPE",
  NAMED_PERSON_WORTH_PURSUING: "NAMED_PERSON_WORTH_PURSUING",
  FUNCTIONAL_PATH_SUFFICIENT: "FUNCTIONAL_PATH_SUFFICIENT",
  STOP_CONTACT_RESEARCH: "STOP_CONTACT_RESEARCH",
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
  HIDDEN_DEMAND_NEXT_LAYER: [
    "EXHIBITOR",
    "VENDOR",
    "AGENCY",
    "CREW",
    "CORPORATE_TEAM",
    "DELEGATION",
    "TOUR_OPERATOR",
    "EDUCATION",
    "SPORTS_ADJACENT",
    "SOCIAL",
    "ASSOCIATION_SUBGROUP",
    "STOP",
  ],
  STRUCTURED_SOURCE_PRIORITY: [
    "EXHIBITOR_DIRECTORY",
    "SPONSOR_DIRECTORY",
    "PROGRAM_PDF",
    "HOUSING_PDF",
    "REGISTRATION_PDF",
    "PARTICIPANT_LIST",
    "STAFF_DIRECTORY",
    "TOUR_SCHEDULE",
    "TEAM_SCHEDULE",
    "PROGRAM_PAGE",
    "NEWS_RELEASE",
    "PROFESSIONAL_PROFILE",
    "STOP",
  ],
  EXHIBITOR_TEAM_RESEARCH_PATH: [
    "COMPANY_EVENT_PAGE",
    "NEWSROOM",
    "SPEAKER_ROSTER",
    "PROFESSIONAL_STAFF",
    "AGENCY_RELATIONSHIP",
    "TRAVEL_EVIDENCE",
    "FUNCTIONAL_CONTACT",
    "STOP_LOW_PROBABILITY",
    "STOP_SUFFICIENT",
  ],
  LODGING_EVIDENCE_NEXT_STEP: [
    "HOUSING_SOURCE",
    "COMPANY_TRAVEL_PAGE",
    "EVENT_REGISTRATION",
    "TEAM_ROSTER",
    "AGENCY_SOURCE",
    "STOP_PLAUSIBLE_ONLY",
    "STOP_SUFFICIENT",
    "STOP_LOW_VALUE",
  ],
  SOURCE_VALIDATION_NEXT_STEP: [
    "FETCH_STATIC",
    "FETCH_RENDERED",
    "FETCH_PDF",
    "FIND_OFFICIAL_EVENT_PAGE",
    "FIND_DIRECTORY",
    "FIND_HOUSING",
    "REJECT_WRONG_GEO",
    "REJECT_LOW_RICHNESS",
    "STOP",
  ],
  EVIDENCE_GAP_NEXT_ACTION: [
    "VERIFY_MARKET",
    "VERIFY_FUTURE_CYCLE",
    "VERIFY_DATES",
    "FIND_OFFICIAL_EVENT_PAGE",
    "FIND_OFFICIAL_HOUSING_PAGE",
    "FIND_REGISTRATION_PAGE",
    "FIND_TRAVEL_ACCOMMODATION_PAGE",
    "FIND_EVENT_MANUAL",
    "FIND_TEAM_MANUAL",
    "FIND_HOSTING_BID",
    "FIND_FUTURE_HOST_PAGE",
    "VERIFY_HOUSING_STATUS",
    "VERIFY_HOTEL_SELECTION_STATUS",
    "VERIFY_OVERFLOW",
    "VERIFY_ORGANIZER_CONTROL",
    "VERIFY_ROOM_DEMAND",
    "VERIFY_EVENT_SCALE",
    "VERIFY_WHO",
    "VERIFY_WHO_ROLE",
    "VERIFY_ACTION_PATH",
    "WAIT_FOR_TRIGGER",
    "STOP_NO_PUBLIC_PATH",
  ],
  CONTACT_SOURCE_PATH: [
    "OFFICIAL_STAFF",
    "EVENT_PROGRAM_PAGE",
    "HOUSING",
    "REGISTRATION",
    "VENUE_SALES",
    "DEPARTMENT_PAGE",
    "PDF_PROSPECTUS",
    "DOMAIN_RESOLUTION",
    "GENERAL_OFFICIAL_CONTACT",
    "NONE",
  ],
  CONTACT_FOLLOWUP_TYPE: [
    "SEARCH_NAMED_OWNER",
    "SEARCH_FUNCTIONAL_CONTACT",
    "SEARCH_OFFICIAL_DOMAIN",
    "SEARCH_STAFF_DIRECTORY",
    "SEARCH_PROGRAM_OFFICE",
    "STOP",
  ],
  NAMED_PERSON_WORTH_PURSUING: ["YES", "NO", "UNCERTAIN"],
  FUNCTIONAL_PATH_SUFFICIENT: ["SUFFICIENT", "IMPROVABLE", "WEAK", "UNKNOWN"],
  STOP_CONTACT_RESEARCH: [
    "CONTINUE",
    "STOP_SUFFICIENT_PATH",
    "STOP_NO_PUBLIC_EVIDENCE",
    "STOP_LOW_VALUE",
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
  HIDDEN_DEMAND_NEXT_LAYER:
    "Choose the next hidden-demand research layer under a market demand generator. Prefer EXHIBITOR/VENDOR/CREW/AGENCY/CORPORATE_TEAM/DELEGATION/TOUR_OPERATOR/EDUCATION/SPORTS_ADJACENT/SOCIAL/ASSOCIATION_SUBGROUP when evidence suggests that lane. Prefer STOP when further layering is unlikely to yield lodging-addressable entities. Research direction only — do not invent organizations, people, or lodging demand.",
  STRUCTURED_SOURCE_PRIORITY:
    "Choose the single next structured source type to fetch for hidden-demand entity extraction. Prefer EXHIBITOR_DIRECTORY, SPONSOR_DIRECTORY, PROGRAM_PDF, HOUSING_PDF, REGISTRATION_PDF, PARTICIPANT_LIST, TOUR_SCHEDULE, or TEAM_SCHEDULE over GENERIC_SERP. Prefer HOUSING_PDF when lodging evidence is missing. Prefer STOP when remaining budget is low or structured sources are exhausted. Research direction only — do not invent entities or lodging demand.",
  EXHIBITOR_TEAM_RESEARCH_PATH:
    "Choose the next research path to find traveling-team evidence for an exhibitor/participating company. Prefer COMPANY_EVENT_PAGE or NEWSROOM before PROFESSIONAL_STAFF. Prefer TRAVEL_EVIDENCE when team is known but lodging/travel is missing. Prefer FUNCTIONAL_CONTACT when team exists but WHO is missing. Prefer STOP_LOW_PROBABILITY for local/directory-only entities. Prefer STOP_SUFFICIENT when team+travel+WHO are adequate. Research direction only — do not invent people, teams, or lodging.",
  SOURCE_VALIDATION_NEXT_STEP:
    "Before fetching a hidden-demand source, choose the next validation action. Prefer REJECT_WRONG_GEO when geography is non-NYC. Prefer REJECT_LOW_RICHNESS for UI chrome/calendar/prospectus-without-list. Prefer FETCH_STATIC for exhibitor/sponsor directories. Prefer FETCH_PDF for housing/program PDFs. Prefer FETCH_RENDERED for a2zinc/expocad portals. Prefer FIND_DIRECTORY or FIND_HOUSING when only an event homepage is known. Prefer STOP when budget is exhausted. Research direction only — do not invent events, companies, or lodging.",
  LODGING_EVIDENCE_NEXT_STEP:
    "Choose the next lodging-evidence research step. Prefer HOUSING_SOURCE or COMPANY_TRAVEL_PAGE when lodging is unknown/weak. Prefer TEAM_ROSTER when multi-person attendance would strengthen inference. Prefer STOP_SUFFICIENT when lodging is CONFIRMED or STRONG_INFERENCE. Prefer STOP_PLAUSIBLE_ONLY when only medium-indirect evidence exists. Prefer STOP_LOW_VALUE for directory-only locals. Research direction only — do not invent lodging demand or room counts.",
  EVIDENCE_GAP_NEXT_ACTION:
    "Given hotel, market, event, resolved/unresolved evidence dimensions, and primary blocker only, choose ONE permitted next public-research action most likely to resolve the primary blocker. Prefer VERIFY_MARKET when market is unresolved. Prefer FIND_OFFICIAL_HOUSING_PAGE / FIND_TRAVEL_ACCOMMODATION_PAGE / VERIFY_HOUSING_STATUS for housing/commercial openness gaps. Prefer WAIT_FOR_TRIGGER when the future event is real but housing is not yet published. Prefer STOP_NO_PUBLIC_PATH when no public path remains. Do NOT invent facts, promote opportunities, set WHO, override grades/status/readiness, or claim lodging demand. Research direction only.",
  CONTACT_SOURCE_PATH:
    "Choose the single best official source path to recover public contact intelligence next. Prefer official staff/housing/registration/program pages over general people search. Use DOMAIN_RESOLUTION when no official domain is known. Use NONE when contact path is already sufficient. Do not invent people or domains.",
  CONTACT_FOLLOWUP_TYPE:
    "Choose the next contact follow-up action. Prefer SEARCH_OFFICIAL_DOMAIN or SEARCH_STAFF_DIRECTORY before SEARCH_NAMED_OWNER. STOP when a usable functional or named path already exists. Do not invent contacts.",
  NAMED_PERSON_WORTH_PURSUING:
    "Decide whether spending budget to find a named owner is justified given current functional path strength and commercial priority. Prefer NO when a strong functional path already exists.",
  FUNCTIONAL_PATH_SUFFICIENT:
    "Rate whether the current functional/organization contact path is commercially sufficient for sales outreach without a named person.",
  STOP_CONTACT_RESEARCH:
    "Decide whether to stop further contact research. STOP_SUFFICIENT_PATH when named or strong functional path exists. STOP_NO_PUBLIC_EVIDENCE when official sources are exhausted. STOP_LOW_VALUE for watchlist/noise. CONTINUE only when a bounded official path remains.",
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
