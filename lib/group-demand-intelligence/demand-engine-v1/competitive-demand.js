/**
 * Competitive demand patterns + fit hypotheses (FACT vs INFERENCE vs UNKNOWN).
 */

import { classifyDemandEngine } from "./classify-engine.js";

export const FIT_CLASS = Object.freeze({
  STRONG_FIT: "STRONG_FIT",
  PLAUSIBLE_FIT: "PLAUSIBLE_FIT",
  WEAK_FIT: "WEAK_FIT",
  NO_FIT: "NO_FIT",
});

export const EVIDENCE_KIND = Object.freeze({
  FACT: "FACT",
  INFERENCE: "INFERENCE",
  UNKNOWN: "UNKNOWN",
});

/**
 * Could target hotel compete next cycle? Internal qualification only.
 */
export function evaluateTargetHotelCompeteFit(pattern = {}, hotelProfile = {}) {
  const rooms = Number(hotelProfile.rooms || hotelProfile.roomCount || 0);
  const group = Number(pattern.estimatedGroupSize || 0);
  const geoOk =
    pattern.targetGeographyOk === true ||
    (hotelProfile.geoTokens || []).some((t) =>
      String(pattern.historicMarkets || [])
        .toLowerCase()
        .includes(String(t).toLowerCase())
    );

  if (pattern.targetHotelFit === FIT_CLASS.NO_FIT) return FIT_CLASS.NO_FIT;

  let score = 0;
  if (geoOk) score += 2;
  if (rooms > 0 && group > 0) {
    if (group <= rooms * 0.7) score += 2;
    else if (group <= rooms) score += 1;
    else score -= 1;
  } else {
    score += 1; // unknown size — don't kill
  }
  if (pattern.lodgingPattern && /block|overflow|housing/i.test(pattern.lodgingPattern)) score += 1;
  if (hotelProfile.airportAccess) score += 1;

  if (score >= 5) return FIT_CLASS.STRONG_FIT;
  if (score >= 3) return FIT_CLASS.PLAUSIBLE_FIT;
  if (score >= 1) return FIT_CLASS.WEAK_FIT;
  return FIT_CLASS.NO_FIT;
}

/**
 * Why they chose competitor hotel — separate FACT / INFERENCE / UNKNOWN.
 */
export function buildCompetitiveHotelFitHypothesis(input = {}) {
  const facts = [];
  const inferences = [];
  const unknowns = [];

  if (input.hostHotel && input.evidenceUrl) {
    facts.push({
      kind: EVIDENCE_KIND.FACT,
      claim: `Event/meeting associated with host hotel ${input.hostHotel}`,
      evidenceUrl: input.evidenceUrl,
    });
  } else if (input.hostHotel) {
    unknowns.push({
      kind: EVIDENCE_KIND.UNKNOWN,
      claim: `Host hotel named as ${input.hostHotel} without corroborating source URL`,
    });
  }

  if (input.officialRoomBlock === true) {
    facts.push({
      kind: EVIDENCE_KIND.FACT,
      claim: "Official room block / housing evidence present",
      evidenceUrl: input.housingEvidenceUrl || input.evidenceUrl || null,
    });
  }

  if (input.organizationStatement) {
    facts.push({
      kind: EVIDENCE_KIND.FACT,
      claim: input.organizationStatement,
      evidenceUrl: input.evidenceUrl || null,
    });
  }

  if (input.repeatUse === true) {
    facts.push({
      kind: EVIDENCE_KIND.FACT,
      claim: "Repeat use of host hotel / brand evidenced across cycles",
    });
  }

  // Inferences — never promoted as fact
  if (input.airportProximityMentioned) {
    inferences.push({
      kind: EVIDENCE_KIND.INFERENCE,
      claim: "Airport convenience may have influenced selection",
    });
  }
  if (input.meetingSpaceHypothesis) {
    inferences.push({
      kind: EVIDENCE_KIND.INFERENCE,
      claim: input.meetingSpaceHypothesis,
    });
  }
  if (input.priceValueHypothesis) {
    inferences.push({
      kind: EVIDENCE_KIND.INFERENCE,
      claim: input.priceValueHypothesis,
    });
  }

  if (!facts.length && !inferences.length) {
    unknowns.push({
      kind: EVIDENCE_KIND.UNKNOWN,
      claim: "Selection rationale not evidenced",
    });
  }

  return {
    hostHotel: input.hostHotel || null,
    facts,
    inferences,
    unknowns,
    // Convenience flat list with kinds for reporting
    claims: [...facts, ...inferences, ...unknowns],
  };
}

/**
 * Build a competitive demand pattern record.
 */
export function buildCompetitiveDemandPattern(input = {}) {
  const cls = classifyDemandEngine({
    title: input.eventType || input.title,
    organizationName: input.organization,
  });
  const patternId =
    input.patternId ||
    `cdp_${String(input.organization || "org")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .slice(0, 40)}_${cls.demandEngine}`;

  const targetHotelFit = evaluateTargetHotelCompeteFit(
    {
      ...input,
      estimatedGroupSize: input.estimatedGroupSize,
      lodgingPattern: input.lodgingPattern,
      historicMarkets: input.historicMarkets,
      targetGeographyOk: input.targetGeographyOk,
    },
    input.hotelProfile || {}
  );

  const hypothesis = buildCompetitiveHotelFitHypothesis(input);

  return {
    patternId,
    organization: input.organization || null,
    demandEngine: cls.demandEngine,
    subsegment: cls.subsegment,
    historicHotels: input.historicHotels || (input.hostHotel ? [input.hostHotel] : []),
    historicMarkets: input.historicMarkets || [],
    historicCycles: input.historicCycles || [],
    estimatedGroupSize: input.estimatedGroupSize ?? null,
    lodgingPattern: input.lodgingPattern || null,
    agency: input.agency || null,
    decisionMaker: input.decisionMaker || null,
    selectionFactors: {
      facts: hypothesis.facts,
      inferences: hypothesis.inferences,
      unknowns: hypothesis.unknowns,
    },
    repeatCadence: input.repeatCadence || null,
    nextExpectedCycle: input.nextExpectedCycle || null,
    targetHotelFit,
    evidenceSet: input.evidenceSet || (input.evidenceUrl ? [{ url: input.evidenceUrl }] : []),
    couldCompeteNextCycle:
      targetHotelFit === FIT_CLASS.STRONG_FIT || targetHotelFit === FIT_CLASS.PLAUSIBLE_FIT,
  };
}

/**
 * Detect competitor-host signals from opportunity text (conservative).
 */
export function extractCompetitivePatternsFromOpportunities(opportunities = [], hotelProfile = {}) {
  const patterns = [];
  const hostRe =
    /\b(?:hosted at|at the|host hotel[:\s]+|venue[:\s]+)([A-Z][\w\s&'-]{2,40}(?:Hotel|Marriott|Hilton|Hyatt|InterContinental|Novotel|Ibis|Ritz|Westin|Sheraton))\b/i;

  for (const opp of opportunities || []) {
    const blob = `${opp.title || ""} ${opp.summaryWhat || ""} ${opp.venueStatus || ""} ${opp.hotelOpportunityThesis || ""}`;
    const m = blob.match(hostRe);
    const competitorNamed =
      m?.[1] ||
      (/marriott|hilton|hyatt|ibis|novotel|intercontinental/i.test(String(opp.venueStatus || ""))
        ? String(opp.venueStatus)
        : null);
    if (!competitorNamed) continue;

    patterns.push(
      buildCompetitiveDemandPattern({
        organization: opp.organizationName || opp.company,
        title: opp.title,
        hostHotel: competitorNamed.trim(),
        evidenceUrl: opp.officialSource || opp.discoverySource || null,
        historicMarkets: [opp.eventLocationSummary || opp.destinationStatus].filter(Boolean),
        historicCycles: [opp.eventStartDate || opp.eventYear].filter(Boolean),
        lodgingPattern: opp.lodgingEvidence ? "housing_signal_present" : null,
        officialRoomBlock: Boolean(opp.lodgingEvidence?.roomBlockMentioned),
        airportProximityMentioned: /airport/i.test(blob),
        hotelProfile,
        opportunityId: opp.id,
      })
    );
  }
  return patterns;
}
