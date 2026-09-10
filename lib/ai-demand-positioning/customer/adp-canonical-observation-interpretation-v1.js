/**
 * ADP_CANONICAL_OBSERVATION_INTERPRETATION_V1
 *
 * ONE observation → ONE canonical interpretation consumed by metrics, rankings,
 * displacement, evidence, and customer UI. Presentation layers must not re-derive.
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import { createHash } from "crypto";
import { getGovernedSubjectMentioned, getGovernedSubjectRank } from "../subject-presence/canonical-subject-presence-v1.js";
import { extractAndResolveCompetitors } from "../intelligence/competitor-name-resolution.js";
import { canonicalizeForProperty } from "../metrics/adp-property-entity-registries.js";
import { classifyPositionFormat } from "../metrics/position-extraction.js";
import { extractCompetitorExplicitRanks } from "../metrics/competitor-rank-extraction-v1.js";

export const ADP_CANONICAL_OBSERVATION_INTERPRETATION_V1 =
  "ADP_CANONICAL_OBSERVATION_INTERPRETATION_V1";
export const SINGLE_CANONICAL_OBSERVATION_INTERPRETATION_PATH =
  "SINGLE_CANONICAL_OBSERVATION_INTERPRETATION_PATH";

function hashText(t) {
  return createHash("sha256").update(String(t || "")).digest("hex");
}

/**
 * Build the single canonical interpretation for one observation.
 * Does not mutate rawResponse.
 */
export function buildCanonicalObservationInterpretation(obs, propertyProfile, scenarios = []) {
  const propertyId = propertyProfile?.propertyId || null;
  const scenario = (scenarios || []).find((s) => s.scenarioId === obs?.scenarioId) || null;
  const territoryId = scenario?.intent || obs?.intent || null;
  const raw = obs?.rawResponse || "";
  const rawResponseHash = hashText(raw);

  const subjectMentioned = getGovernedSubjectMentioned(obs);
  const subjectRank = getGovernedSubjectRank(obs);

  const competitorNames = Array.isArray(obs?.competitorsMentioned)
    ? obs.competitorsMentioned
    : extractAndResolveCompetitors(raw, propertyProfile);

  const competitors = [];
  const unresolvedEntities = [];
  for (const rawObservedName of competitorNames) {
    const canonicalHotelId = propertyId
      ? canonicalizeForProperty(propertyId, rawObservedName)
      : null;
    if (!canonicalHotelId) {
      unresolvedEntities.push({ rawObservedName, reason: "UNRESOLVED_CANONICAL_ID" });
      continue;
    }
    competitors.push({
      rawObservedName,
      canonicalHotelId,
      entityId: canonicalHotelId,
      mentioned: true,
      rank: null,
      confidence: "resolved",
    });
  }

  const competitorRanks = extractCompetitorExplicitRanks(raw, propertyProfile);
  for (const row of competitors) {
    const hit = competitorRanks.find((r) => r.canonicalHotelId === row.canonicalHotelId);
    if (hit) {
      row.rank = hit.rank;
      row.rankClass = hit.rankClass;
      row.confidence = hit.confidence || row.confidence;
    }
  }

  const rankedHotels = competitors
    .filter((c) => c.rank != null)
    .sort((a, b) => a.rank - b.rank)
    .map((c) => ({
      canonicalHotelId: c.canonicalHotelId,
      rank: c.rank,
      rawObservedName: c.rawObservedName,
      rankClass: c.rankClass,
    }));

  const subjectAbsent = !subjectMentioned;
  const candidateCompetitors = subjectAbsent ? competitors.map((c) => c.canonicalHotelId) : [];
  const governingCompetitorId =
    rankedHotels[0]?.canonicalHotelId || candidateCompetitors[0] || null;

  const evidenceType = subjectMentioned
    ? "POSITIVE_PRESENCE"
    : candidateCompetitors.length
      ? "COMPETITIVE_DISPLACEMENT"
      : "SUBJECT_ABSENT_NO_COMPETITOR";

  return {
    object: ADP_CANONICAL_OBSERVATION_INTERPRETATION_V1,
    propertyId,
    periodId: obs?.periodId || null,
    scenarioId: obs?.scenarioId || null,
    territoryId,
    provider: obs?.provider || null,
    observationId: obs?.observationId || obs?.id || null,
    rawResponseHash,
    subject: {
      canonicalHotelId: propertyProfile?.entityId || propertyProfile?.canonicalEntityId || "__subject__",
      mentioned: subjectMentioned,
      rank: subjectRank,
      confidence: obs?.governedInterpretation ? "governed_path_a" : "projected",
    },
    competitors,
    rankedHotels,
    unresolvedEntities,
    displacement: {
      subjectAbsent,
      candidateCompetitors,
      governingCompetitorId,
      ruleUsed: "SUBJECT_ABSENT_PLUS_COMPETITOR_PRESENT_SCENARIO_CREDIT",
    },
    evidenceType,
    evidenceRefs: [
      {
        observationId: obs?.observationId || obs?.id || null,
        evidenceType,
        territoryId,
        provider: obs?.provider || null,
      },
    ],
    formatClass: classifyPositionFormat(raw),
  };
}

export function attachCanonicalInterpretation(obs, propertyProfile, scenarios = []) {
  const interpretation = buildCanonicalObservationInterpretation(obs, propertyProfile, scenarios);
  return {
    ...obs,
    canonicalInterpretation: interpretation,
    canonicalInterpretationVersion: ADP_CANONICAL_OBSERVATION_INTERPRETATION_V1,
  };
}
