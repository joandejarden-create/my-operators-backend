/**
 * Hybrid recommendation classifier — deterministic evidence + constrained adjudicator.
 * Entity presence is never re-decided here.
 */

import {
  extractEntityLocalEvidence,
  buildTypedSections,
} from "../recommendation-evidence-v4_1.js";
import {
  decideRecommendationRoleFromEvidence,
  questionStatusFromRecommendationRole,
  RECOMMENDATION_CLASSIFIER_VERSION,
} from "../recommendation-classifier-v4_1.js";
import { buildHybridRouteRecord } from "./evidence-state.js";
import { runConstrainedAdjudicator, estimateAdjudicatorCallCostUsd } from "./adjudicator-client.js";

export const HYBRID_CLASSIFIER_VERSION =
  "ai_visibility_hybrid_recommendation_classifier_v1";

const ROLE_RANK = [
  "negative_or_qualified",
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "source_only",
  "no_mention",
];

function pickBestMention(hits) {
  if (!hits?.length) return null;
  return hits
    .slice()
    .sort(
      (a, b) =>
        ROLE_RANK.indexOf(a.role) - ROLE_RANK.indexOf(b.role) ||
        a.mentionPosition - b.mentionPosition
    )[0];
}

function cueFactsFromEvidence(evidence) {
  const ev = evidence?.recommendationEvidence || {};
  return {
    directNegativeCue: Boolean(ev.directNegativeCue),
    directPositiveCue: Boolean(ev.directPositiveCue),
    sectionPositiveCue: Boolean(ev.sectionPositiveCue),
    leadCue: Boolean(ev.leadCue),
    rankCue: Boolean(ev.rankCue),
    considerationSetCue: Boolean(ev.considerationSetCue),
    comparatorCue: Boolean(ev.comparatorCue),
    descriptiveCue: Boolean(ev.descriptiveCue),
    incidentalCue: Boolean(ev.incidentalCue),
    sourceOnlyCue: Boolean(ev.sourceOnlyCue),
    confirmedRankStructure: Boolean(
      evidence?.confirmedRankStructure || evidence?.structure?.confirmedRankStructure
    ),
    sectionType: evidence?.sectionType || null,
    propagationSource: evidence?.propagationSource || null,
    propagationDistance: evidence?.propagationDistance ?? null,
  };
}

/**
 * Classify one entity mention span via hybrid routing.
 */
export async function classifyHybridRecommendationRole(args = {}) {
  const {
    text,
    start,
    end,
    rawMention,
    canonicalEntityName,
    canonicalEntityId,
    entityPresent = true,
    typedSections,
    callAdjudicator = true,
    adjudicatorOptions = {},
  } = args;

  if (!entityPresent) {
    return {
      role: "no_mention",
      route: "DETERMINISTIC",
      evidenceState: "DECISIVE",
      LIVE_PROVIDER_CALL: false,
      questionStatus: questionStatusFromRecommendationRole("no_mention", false),
      hybridVersion: HYBRID_CLASSIFIER_VERSION,
      deterministicVersion: RECOMMENDATION_CLASSIFIER_VERSION,
    };
  }

  const sections = typedSections || buildTypedSections(String(text || ""));
  const evidence = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention,
    canonicalEntityName,
    canonicalEntityId,
    typedSections: sections,
  });
  const decided = decideRecommendationRoleFromEvidence(evidence, { entityPresent: true });
  const routeRec = buildHybridRouteRecord({
    evidence,
    deterministicRole: decided.role,
    entityPresent: true,
  });

  const base = {
    DETERMINISTIC_ROLE: decided.role,
    EVIDENCE_STATE: routeRec.EVIDENCE_STATE,
    AMBIGUITY_REASONS: routeRec.AMBIGUITY_REASONS,
    PLAUSIBLE_ROLES: routeRec.PLAUSIBLE_ROLES,
    ROUTE: routeRec.ROUTE,
    evidence,
    cueFacts: cueFactsFromEvidence(evidence),
    deterministicReason: decided.reason,
    hybridVersion: HYBRID_CLASSIFIER_VERSION,
    deterministicVersion: RECOMMENDATION_CLASSIFIER_VERSION,
    LIVE_PROVIDER_CALL: false,
  };

  if (routeRec.ROUTE === "DETERMINISTIC") {
    return {
      ...base,
      role: decided.role,
      finalRole: decided.role,
      questionStatus: questionStatusFromRecommendationRole(decided.role, true),
      source: "deterministic",
    };
  }

  if (routeRec.ROUTE === "ABSTAIN" || !callAdjudicator) {
    return {
      ...base,
      role: null,
      finalRole: null,
      abstained: true,
      questionStatus: null,
      source: "abstain",
    };
  }

  // ADJUDICATOR
  const entityLocalEvidence = String(
    evidence.localListItem || evidence.localSentence || ""
  ).slice(0, 1200);
  const adj = await runConstrainedAdjudicator({
    entityName: canonicalEntityName,
    entityLocalEvidence,
    sectionHeading: evidence.sectionHeading || evidence.parentHeading,
    sectionIntro: evidence.sectionIntro,
    structuralEvidence: evidence.structure,
    cueFacts: base.cueFacts,
    plausibleRoles: routeRec.PLAUSIBLE_ROLES,
    ambiguityReasons: routeRec.AMBIGUITY_REASONS,
    evidence,
    entityPresent: true,
    ...adjudicatorOptions,
  });

  if (!adj.ok) {
    return {
      ...base,
      role: null,
      finalRole: null,
      abstained: true,
      adjudicationFailed: true,
      adjudicationErrors: adj.errors || [adj.error || adj.code],
      LIVE_PROVIDER_CALL: Boolean(adj.LIVE_PROVIDER_CALL),
      actualCostUsd: adj.actualCostUsd ?? null,
      questionStatus: null,
      source: "adjudication_validation_failed",
    };
  }

  return {
    ...base,
    role: adj.selectedRole,
    finalRole: adj.selectedRole,
    questionStatus: questionStatusFromRecommendationRole(adj.selectedRole, true),
    source: "adjudicator",
    LIVE_PROVIDER_CALL: true,
    adjudicator: {
      selectedRole: adj.selectedRole,
      evidenceRefs: adj.evidenceRefs,
      taxonomyRule: adj.taxonomyRule,
      ambiguityResolved: adj.ambiguityResolved,
      model: adj.model,
      actualCostUsd: adj.actualCostUsd,
      latencyMs: adj.latencyMs,
    },
    actualCostUsd: adj.actualCostUsd,
  };
}

export { pickBestMention, cueFactsFromEvidence, estimateAdjudicatorCallCostUsd };
