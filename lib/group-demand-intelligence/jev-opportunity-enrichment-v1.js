/**
 * Jev adaptive opportunity enrichment routing (SAFE APPLY).
 * Decides WHERE to research next — does not invent facts, WHO, lodging, or timing.
 * JEV PERSON APPLY = NO.
 */

export const ENRICHMENT_NEXT_STEP = Object.freeze({
  RESEARCH_IDENTITY: "RESEARCH_IDENTITY",
  RESEARCH_EVENT: "RESEARCH_EVENT",
  RESEARCH_LODGING: "RESEARCH_LODGING",
  RESEARCH_TEAM: "RESEARCH_TEAM",
  RESEARCH_WHO: "RESEARCH_WHO",
  RESEARCH_HOW: "RESEARCH_HOW",
  RESEARCH_TIMING: "RESEARCH_TIMING",
  DEFER: "DEFER",
  STOP_SUFFICIENT: "STOP_SUFFICIENT",
  STOP_PUBLIC_DATA_CEILING: "STOP_PUBLIC_DATA_CEILING",
});

/**
 * @param {object} packet — evidence + gap flags from enrichment
 * @returns {{ decision, confidence, rationale, safeApply }}
 */
export function evaluateOpportunityEnrichmentNextStep(packet = {}) {
  const gaps = {
    identity: packet.identityGap === true || !packet.organizationName,
    event: packet.eventGap === true || !packet.eventTiming,
    lodging: packet.lodgingGap === true,
    team: packet.teamGap === true,
    who: packet.whoGap === true || packet.whoNotResearched === true,
    how: packet.howGap === true,
    timing: packet.timingGap === true,
    summary: packet.summaryThin === true,
  };

  if (packet.publicDataCeiling === true && !gaps.lodging && !gaps.event) {
    return {
      decision: ENRICHMENT_NEXT_STEP.STOP_PUBLIC_DATA_CEILING,
      confidence: 0.75,
      rationale: "public_data_ceiling_reached",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }

  if (
    packet.summaryAdequate === true &&
    packet.whoResearched === true &&
    !gaps.lodging &&
    !gaps.identity
  ) {
    return {
      decision: ENRICHMENT_NEXT_STEP.STOP_SUFFICIENT,
      confidence: 0.8,
      rationale: "summary_and_who_sufficient",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }

  if (gaps.identity) {
    return {
      decision: ENRICHMENT_NEXT_STEP.RESEARCH_IDENTITY,
      confidence: 0.85,
      rationale: "missing_organization_identity",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }
  if (gaps.timing || gaps.event) {
    return {
      decision: ENRICHMENT_NEXT_STEP.RESEARCH_TIMING,
      confidence: 0.8,
      rationale: "missing_event_timing",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }
  if (gaps.team) {
    return {
      decision: ENRICHMENT_NEXT_STEP.RESEARCH_TEAM,
      confidence: 0.75,
      rationale: "missing_team_evidence",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }
  if (gaps.lodging) {
    return {
      decision: ENRICHMENT_NEXT_STEP.RESEARCH_LODGING,
      confidence: 0.8,
      rationale: "missing_lodging_evidence",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }
  if (gaps.who) {
    return {
      decision: ENRICHMENT_NEXT_STEP.RESEARCH_WHO,
      confidence: 0.8,
      rationale: "who_not_researched",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }
  if (gaps.how) {
    return {
      decision: ENRICHMENT_NEXT_STEP.RESEARCH_HOW,
      confidence: 0.7,
      rationale: "how_reachability_gap",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }
  if (gaps.summary) {
    return {
      decision: ENRICHMENT_NEXT_STEP.RESEARCH_EVENT,
      confidence: 0.65,
      rationale: "summary_thin_need_event_context",
      safeApply: true,
      type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
    };
  }

  return {
    decision: ENRICHMENT_NEXT_STEP.DEFER,
    confidence: 0.5,
    rationale: "no_clear_gap",
    safeApply: true,
    type: "OPPORTUNITY_ENRICHMENT_NEXT_STEP",
  };
}

/**
 * Build Jev packet from opportunity + quality evals.
 */
export function buildEnrichmentJevPacket(opp = {}, summaryEval = {}, who = {}) {
  return {
    organizationName: opp.organizationName || null,
    eventTiming: Boolean(opp.eventStartDate || opp.eventYear || opp.eventDateDisplay),
    identityGap: !opp.organizationName,
    eventGap: !(opp.eventStartDate || opp.eventYear),
    lodgingGap: !(
      opp.lodgingEvidence?.roomBlockMentioned ||
      opp.lodgingEvidence?.housingPageFound ||
      opp.lodging?.roomBlockMentioned ||
      /overflow|housing|hotel TBA/i.test(`${opp.venueStatus || ""} ${opp.hotelOpportunityThesis || ""}`)
    ),
    teamGap: !(opp.teamSupported || opp.teamEvidence),
    whoGap: who.pathClass === "NOT_RESEARCHED",
    whoNotResearched: who.pathClass === "NOT_RESEARCHED",
    whoResearched: who.pathClass && who.pathClass !== "NOT_RESEARCHED",
    howGap: who.pathClass === "NAMED_PARTIAL" || who.pathClass === "NO_CONTACT_AFTER_RESEARCH",
    timingGap: !(opp.eventStartDate || opp.eventYear),
    summaryThin: summaryEval.quality === "THIN" || summaryEval.quality === "INVALID",
    summaryAdequate:
      summaryEval.quality === "STRONG" || summaryEval.quality === "ADEQUATE",
    publicDataCeiling: Boolean(opp.publicContactCeilingReason),
  };
}
