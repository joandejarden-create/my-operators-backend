/**
 * Canonical ADP subject presence / rank — Option A (parse-time / reprocess-time).
 * Metrics must consume governedInterpretation fields, never Path-B re-parse at read time.
 */

import {
  detectPropertyMention,
  buildNameVariants,
} from "../execution/response-parser.js";
import { extractPropertyRank } from "../metrics/position-extraction.js";

export const CANONICAL_SUBJECT_PARSER_VERSION = "adp_canonical_subject_parser_v1";
export const CANONICAL_SUBJECT_RESOLVER_VERSION = "adp_canonical_subject_resolver_v1";
export const GOVERNED_INTERPRETATION_SCHEMA = "adp_governed_subject_interpretation_v1";

/**
 * Compute governed subject presence + rank from raw response.
 * Mention = detectPropertyMention (token-boundary Path A).
 * Rank = extractPropertyRank position fields ONLY when subject is mentioned under Path A
 *        (does not use Path B weak mention variants to decide presence).
 */
export function computeCanonicalSubjectPresence(rawResponse, propertyProfile, options = {}) {
  const mention = detectPropertyMention(rawResponse || "", propertyProfile);
  const rankFull = extractPropertyRank(rawResponse || "", propertyProfile);

  const subjectMentioned = Boolean(mention.mentioned);
  // Rank only credits when Path A confirms subject presence (prevents Path B false presence)
  let subjectRank = null;
  let rankEligible = false;
  let rankSource = null;
  let positionConfidence = null;

  if (subjectMentioned) {
    // Prefer Path B rank extraction when the same response is subject-present under Path A
    if (rankFull.mentioned && rankFull.position != null) {
      subjectRank = rankFull.position;
      rankEligible = Boolean(rankFull.rankEligible);
      rankSource = rankFull.rankSource;
      positionConfidence = rankFull.positionConfidence;
    } else if (mention.position != null) {
      subjectRank = mention.position;
      rankEligible = false;
      rankSource = "parser_light_position";
      positionConfidence = "low";
    }
  }

  return {
    schema: GOVERNED_INTERPRETATION_SCHEMA,
    subjectMentioned,
    subjectRank,
    rankEligible,
    rankSource,
    positionConfidence,
    canonicalSubjectEntity: propertyProfile?.name || null,
    matchedVariant: mention.matchedVariant || null,
    matchReason: subjectMentioned
      ? mention.matchReason ||
        (mention.matchedVariant ? `ALIAS_OR_VARIANT:${mention.matchedVariant}` : "CANONICAL_NAME")
      : "NO_SUBJECT_MATCH",
    matchProvenance: {
      method: mention.contextualResolution
        ? "contextual_entity_resolution_v1"
        : "token_boundary_haystack",
      variantsConsidered: (buildNameVariants(propertyProfile) || []).length,
      context: mention.context || null,
      contextualResolution: mention.contextualResolution || null,
    },
    parserVersion: CANONICAL_SUBJECT_PARSER_VERSION,
    resolverVersion: CANONICAL_SUBJECT_RESOLVER_VERSION,
    computedAt: options.timestamp || new Date().toISOString(),
  };
}

/**
 * Attach provenance-preserving governed interpretation onto an observation.
 * Never mutates original mentioned/position/rawResponse — stores originals snapshot.
 */
export function applyGovernedInterpretation(obs, propertyProfile, options = {}) {
  const governed = computeCanonicalSubjectPresence(obs?.rawResponse || "", propertyProfile, options);
  const previousMentioned = obs?.mentioned;
  const previousRank = obs?.position ?? null;

  const correctionReason =
    options.correctionReason ||
    (Boolean(previousMentioned) !== governed.subjectMentioned
      ? "CANONICAL_OPTION_A_REPROCESS"
      : "CANONICAL_OPTION_A_ALIGN");

  return {
    ...obs,
    // Immutable historical fields preserved as-is on the observation
    mentioned: obs.mentioned,
    position: obs.position,
    context: obs.context,
    rawResponse: obs.rawResponse,
    // Original snapshot (first reprocess wins unless force)
    originalParse:
      obs.originalParse ||
      {
        mentioned: previousMentioned,
        position: previousRank,
        context: obs.context ?? null,
        rankEligible: obs.rankEligible ?? null,
        rankSource: obs.rankSource ?? null,
        parserVersion: obs.parserVersion || null,
        resolverVersion: obs.resolverVersion || null,
        capturedAt: options.timestamp || new Date().toISOString(),
      },
    governedInterpretation: {
      ...governed,
      reprocessTimestamp: options.timestamp || new Date().toISOString(),
      correctionReason,
      previousValue: {
        mentioned: previousMentioned,
        position: previousRank,
      },
      correctedValue: {
        subjectMentioned: governed.subjectMentioned,
        subjectRank: governed.subjectRank,
      },
    },
  };
}

/** Single accessor for all metrics — governed first. */
export function getGovernedSubjectMentioned(obs) {
  if (obs?.governedInterpretation && typeof obs.governedInterpretation.subjectMentioned === "boolean") {
    return obs.governedInterpretation.subjectMentioned;
  }
  return Boolean(obs?.mentioned);
}

export function getGovernedSubjectRank(obs) {
  if (obs?.governedInterpretation && "subjectRank" in obs.governedInterpretation) {
    return obs.governedInterpretation.subjectRank;
  }
  return obs?.position ?? null;
}

export function getGovernedRankEligible(obs) {
  if (obs?.governedInterpretation && typeof obs.governedInterpretation.rankEligible === "boolean") {
    return obs.governedInterpretation.rankEligible;
  }
  return Boolean(obs?.rankEligible);
}

/**
 * Project observations for metric consumers: mentioned/position mirror governed state.
 * Does not re-parse raw text.
 */
export function projectGovernedObservations(observations) {
  return (observations || []).map((obs) => {
    if (!obs?.governedInterpretation) {
      return obs;
    }
    return {
      ...obs,
      mentioned: getGovernedSubjectMentioned(obs),
      position: getGovernedSubjectRank(obs),
      rankEligible: getGovernedRankEligible(obs),
      rankSource: obs.governedInterpretation.rankSource ?? obs.rankSource,
      positionConfidence: obs.governedInterpretation.positionConfidence ?? obs.positionConfidence,
      context: obs.governedInterpretation.matchProvenance?.context ?? obs.context,
      _subjectPresenceSource: "governedInterpretation",
    };
  });
}
