/**
 * Observation Integrity ledger — exhaustive per-observation audit.
 */

import {
  ASSURANCE_RESOLVER_VERSION,
  DISAGREEMENT_CLASSES,
  ADP_MEASUREMENT_ASSURANCE_VERSION,
} from "./version.js";
import { refInterpretObservation } from "./reference-metrics.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { detectStructuralDualPath } from "../certification/single-canonical-subject-presence-path.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";

function observationStatus(obs) {
  if (obs?.dryRun) return "DRY_RUN";
  if (obs?.error || obs?.status === "FAILED" || obs?.status === "ERROR") return "FAILED";
  if (obs?.rawResponse) return "SUCCESS";
  if (obs?.parsed) return "PARSED_EMPTY";
  return "UNKNOWN";
}

function classifySubjectDisagreement(stored, audit) {
  if (stored === audit.mentioned) {
    return { agreement: true, disagreementClass: null, reviewerStatus: "AUTO_AGREE" };
  }
  if (stored === true && audit.mentioned === false) {
    return {
      agreement: false,
      disagreementClass: DISAGREEMENT_CLASSES.SUBJECT_FALSE_POSITIVE,
      reviewerStatus: "AUTO_CLASSIFIED",
    };
  }
  if (stored === false && audit.mentioned === true) {
    return {
      agreement: false,
      disagreementClass: DISAGREEMENT_CLASSES.SUBJECT_FALSE_NEGATIVE,
      reviewerStatus: "AUTO_CLASSIFIED",
    };
  }
  return {
    agreement: false,
    disagreementClass: DISAGREEMENT_CLASSES.OTHER,
    reviewerStatus: "MANUAL_REVIEW",
  };
}

function refineNohoClassification(row, raw) {
  if (row.propertyId !== "adp_now_now_noho") return row;
  if (row.storedMentioned && !row.auditMentioned) {
    const hasFull = /now\s*now\s*noho/i.test(raw || "");
    const hasKnown = /\bknown\b/i.test(raw || "");
    if (!hasFull && hasKnown) {
      return {
        ...row,
        disagreementClass: DISAGREEMENT_CLASSES.SUBJECT_FALSE_POSITIVE,
        disagreementDetail: "FALSE_POSITIVE_SUBSTRING_NOW_IN_KNOWN",
        reviewerStatus: "AUTO_CLASSIFIED",
        goldSubjectMentioned: false,
      };
    }
    if (!hasFull) {
      return {
        ...row,
        disagreementClass: DISAGREEMENT_CLASSES.PARSER_VERSION_DRIFT,
        disagreementDetail: "STORED_TRUE_WITHOUT_CANONICAL_NAME",
        reviewerStatus: "AUTO_CLASSIFIED",
        goldSubjectMentioned: false,
      };
    }
  }
  return row;
}

/**
 * Build exhaustive observation ledger for a period.
 */
export function buildObservationLedger({
  propertyId,
  period,
  scenarios,
  propertyProfile,
}) {
  const scenarioById = new Map((scenarios || []).map((s) => [s.scenarioId, s]));
  const rows = [];
  let agree = 0;
  let disagree = 0;
  let fp = 0;
  let fn = 0;
  let manual = 0;
  const projected = enrichObservationsWithRank(period?.observations || [], propertyProfile);
  let enrichFlips = 0;

  for (let i = 0; i < (period?.observations || []).length; i++) {
    const obs = period.observations[i];
    const scenario = scenarioById.get(obs.scenarioId);
    const audit = refInterpretObservation(obs, propertyProfile);
    const projectedMentioned = Boolean(projected[i]?.mentioned);
    const governedMentioned =
      obs.governedInterpretation != null
        ? getGovernedSubjectMentioned(obs)
        : audit.mentioned;
    const stored = Boolean(obs.mentioned);

    if (obs.governedInterpretation && projectedMentioned !== governedMentioned) {
      enrichFlips += 1;
    }

    let classInfo = classifySubjectDisagreement(stored, { mentioned: governedMentioned });
    // After Option A reprocess: original≠governed is a provenance correction, not an active dual-path flip
    if (obs.governedInterpretation && stored !== governedMentioned) {
      classInfo = {
        agreement: false,
        disagreementClass: DISAGREEMENT_CLASSES.PARSER_VERSION_DRIFT,
        reviewerStatus: "AUTO_CLASSIFIED_PROVENANCE_CORRECTION",
      };
    } else if (stored === governedMentioned) {
      classInfo = { agreement: true, disagreementClass: null, reviewerStatus: "AUTO_AGREE" };
    }

    if (stored === governedMentioned) agree += 1;
    else disagree += 1;

    let rankAgree = true;
    let rankClass = null;
    if (audit.mentioned && obs.position != null && audit.position != null && obs.position !== audit.position) {
      rankAgree = false;
      rankClass = DISAGREEMENT_CLASSES.WRONG_RANK;
    }

    let row = {
      propertyId,
      periodId: period.periodId,
      observationId: obs.observationId || `${obs.scenarioId}::${obs.provider}`,
      scenarioId: obs.scenarioId,
      scenarioPrompt: scenario?.query || scenario?.prompt || null,
      demandTerritory: scenario?.intent || null,
      provider: obs.provider,
      rawResponseRef: obs.observationId
        ? `period:${period.periodId}/obs:${obs.observationId}`
        : `period:${period.periodId}/scenario:${obs.scenarioId}/provider:${obs.provider}`,
      observationStatus: observationStatus(obs),
      productionParserVersion: obs.parserVersion || null,
      entityResolverVersion: ASSURANCE_RESOLVER_VERSION,
      storedMentioned: stored,
      storedRank: obs.position ?? null,
      detectedSubjectAlias: audit.matchedVariant,
      canonicalSubjectEntity: propertyProfile.name,
      detectedCompetitors: obs.competitorsMentioned || [],
      extractedAttributes: obs.attributesRecognized || [],
      independentAudit: {
        mentioned: audit.mentioned,
        matchedVariant: audit.matchedVariant,
        position: audit.position,
        rankEligible: audit.rankEligible,
        rankSource: audit.rankSource,
        context: audit.context,
      },
      enrichPathMentioned: projectedMentioned,
      agreement: classInfo.agreement && rankAgree,
      disagreementClass: classInfo.agreement
        ? rankClass
        : classInfo.disagreementClass || rankClass,
      disagreementDetail: null,
      reviewerStatus: classInfo.reviewerStatus,
      goldSubjectMentioned: governedMentioned,
      goldRank: obs.governedInterpretation?.subjectRank ?? audit.position,
      auditMentioned: governedMentioned,
      assuranceVersion: ADP_MEASUREMENT_ASSURANCE_VERSION,
    };

    row = refineNohoClassification(row, obs.rawResponse || "");
    if (row.reviewerStatus === "AUTO_CLASSIFIED_PROVENANCE_CORRECTION") {
      // keep provenance classification
    } else if (
      obs.governedInterpretation &&
      stored !== governedMentioned &&
      row.disagreementClass === DISAGREEMENT_CLASSES.SUBJECT_FALSE_POSITIVE
    ) {
      row.reviewerStatus = "AUTO_CLASSIFIED_PROVENANCE_CORRECTION";
      row.disagreementClass = DISAGREEMENT_CLASSES.PARSER_VERSION_DRIFT;
    }
    if (row.reviewerStatus === "MANUAL_REVIEW") manual += 1;

    rows.push(row);
  }

  // Active FPs = not yet corrected via governedInterpretation
  fp = rows.filter(
    (r) =>
      r.disagreementClass === DISAGREEMENT_CLASSES.SUBJECT_FALSE_POSITIVE &&
      r.reviewerStatus !== "AUTO_CLASSIFIED_PROVENANCE_CORRECTION"
  ).length;
  fn = rows.filter(
    (r) =>
      r.disagreementClass === DISAGREEMENT_CLASSES.SUBJECT_FALSE_NEGATIVE &&
      r.reviewerStatus !== "AUTO_CLASSIFIED_PROVENANCE_CORRECTION"
  ).length;
  const provenanceCorrections = rows.filter(
    (r) => r.reviewerStatus === "AUTO_CLASSIFIED_PROVENANCE_CORRECTION"
  ).length;
  agree = rows.filter((r) => r.storedMentioned === r.goldSubjectMentioned).length;
  disagree = rows.length - agree;
  manual = rows.filter((r) => r.reviewerStatus === "MANUAL_REVIEW").length;

  const structural = detectStructuralDualPath();

  return {
    propertyId,
    periodId: period?.periodId,
    totalObservations: rows.length,
    subjectAgreement: agree,
    subjectDisagreement: disagree,
    falsePositives: fp,
    falseNegatives: fn,
    provenanceCorrections,
    manualReviewCount: manual,
    enrichPathFlips: enrichFlips,
    dualPathActive: structural.dualPathActive,
    rows,
  };
}
