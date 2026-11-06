/**
 * ADP metric semantics certification V1 — fail-closed Full E2E gate.
 *
 * Verifies registered definitions, grains, payload mapping, and competitor
 * displacement wording. Does NOT invent metrics from 0–100% alone.
 */

import {
  ADP_METRIC_DEFINITIONS,
  ADP_METRIC_SEMANTICS_REGISTRY_V1,
  getMetricDefinition,
  listMetricIds,
} from "../metrics/adp-metric-semantics-registry-v1.js";
import {
  COMPETITOR_PRESENT_CARD,
  COMPETITOR_PRESENT_COUNTING_GRAIN,
  enrichCompetitorPresentPayload,
} from "../metrics/competitor-present-scenario-copy-v1.js";
import { computeConsiderationMetrics } from "../metrics/consideration-rate.js";
import { computePositionMetrics, MIN_RANK_SAMPLE } from "../metrics/position-metrics.js";
import { computeCompetitorPresentGaps } from "../metrics/competitor-present-gaps.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { buildOptionalExecutiveMetrics } from "../metrics/optional-executive-metrics.js";

export const ADP_METRIC_SEMANTICS_CERTIFICATION_V1 =
  "ADP_METRIC_SEMANTICS_CERTIFICATION_V1";

export const METRIC_CERT_STATES = Object.freeze({
  METRIC_CERTIFIED: "METRIC_CERTIFIED",
  PARTIALLY_CERTIFIED: "PARTIALLY_CERTIFIED",
  EVIDENCE_BLOCKED: "EVIDENCE_BLOCKED",
  PRESENTATION_BLOCKED: "PRESENTATION_BLOCKED",
  NOT_MEASURED: "NOT_MEASURED",
});

/**
 * Static registry + copy contracts (no period required).
 */
export function certifyMetricSemanticsRegistry() {
  const failures = [];
  const warnings = [];

  if (!ADP_METRIC_DEFINITIONS.length) {
    failures.push("REGISTRY_EMPTY");
  }
  const ids = listMetricIds();
  if (new Set(ids).size !== ids.length) {
    failures.push("DUPLICATE_METRIC_IDS");
  }

  for (const m of ADP_METRIC_DEFINITIONS) {
    if (!m.id || !m.displayTitle || !m.grain || !m.numerator || !m.denominator) {
      failures.push(`INCOMPLETE_DEFINITION:${m.id || "?"}`);
    }
    if (!m.producerFn || !m.payloadPath) {
      failures.push(`MISSING_PRODUCER_OR_PAYLOAD:${m.id}`);
    }
    if (!m.ownerExplanation || m.ownerExplanation.length < 20) {
      warnings.push(`THIN_OWNER_COPY:${m.id}`);
    }
  }

  const displacement = getMetricDefinition("scenarios_with_competitive_displacement");
  if (!displacement) {
    failures.push("MISSING_DISPLACEMENT_METRIC");
  } else {
    if (displacement.grain !== COMPETITOR_PRESENT_COUNTING_GRAIN) {
      failures.push("DISPLACEMENT_GRAIN_MISMATCH");
    }
    if (displacement.displayTitle !== COMPETITOR_PRESENT_CARD.title) {
      failures.push("DISPLACEMENT_TITLE_DRIFT");
    }
    if (/yours did not$/i.test(COMPETITOR_PRESENT_CARD.subtitle.trim())) {
      failures.push("DISPLACEMENT_AMBIGUOUS_SUBTITLE");
    }
    if (!/at least one AI provider/i.test(COMPETITOR_PRESENT_CARD.subtitle)) {
      failures.push("DISPLACEMENT_SUBTITLE_MISSING_RESPONSE_GRAIN");
    }
  }

  const enriched = enrichCompetitorPresentPayload({ scenarioCount: 1 });
  if (enriched.displayTitle !== COMPETITOR_PRESENT_CARD.title) {
    failures.push("ENRICH_DISPLAY_TITLE_MISMATCH");
  }
  if (enriched.countingGrain !== COMPETITOR_PRESENT_COUNTING_GRAIN) {
    failures.push("ENRICH_GRAIN_MISMATCH");
  }

  return {
    version: ADP_METRIC_SEMANTICS_CERTIFICATION_V1,
    registryVersion: ADP_METRIC_SEMANTICS_REGISTRY_V1,
    metricCount: ADP_METRIC_DEFINITIONS.length,
    metricIds: ids,
    failures,
    warnings,
    passed: failures.length === 0,
  };
}

/**
 * Independent recalculation vs optional executive payload for one period.
 */
export function certifyPeriodMetricSemantics({
  period,
  scenarios,
  profile,
  stagedExecutiveMetrics = null,
} = {}) {
  const registry = certifyMetricSemanticsRegistry();
  const failures = [...registry.failures];
  const warnings = [...registry.warnings];
  const checks = [];

  if (!period?.observations?.length) {
    return {
      version: ADP_METRIC_SEMANTICS_CERTIFICATION_V1,
      propertyId: period?.propertyId || null,
      periodId: period?.periodId || null,
      hotelState: METRIC_CERT_STATES.EVIDENCE_BLOCKED,
      registry,
      checks,
      failures: [...failures, "NO_OBSERVATIONS"],
      warnings,
      passed: false,
    };
  }
  if (!scenarios?.length || !profile) {
    return {
      version: ADP_METRIC_SEMANTICS_CERTIFICATION_V1,
      propertyId: period?.propertyId || null,
      periodId: period?.periodId || null,
      hotelState: METRIC_CERT_STATES.EVIDENCE_BLOCKED,
      registry,
      checks,
      failures: [...failures, "NO_SCENARIOS_OR_PROFILE"],
      warnings,
      passed: false,
    };
  }

  const enriched = enrichObservationsWithRank(period.observations, profile);
  const consideration = computeConsiderationMetrics(enriched, scenarios, profile);
  const position = computePositionMetrics(enriched, scenarios, profile);
  const gaps = computeCompetitorPresentGaps(enriched, scenarios, profile);
  const optional = buildOptionalExecutiveMetrics(period, scenarios, profile) || {};
  const staged = stagedExecutiveMetrics || optional;

  function check(id, ok, detail) {
    checks.push({ id, ok, detail });
    if (!ok) failures.push(id);
  }

  // Partition invariant when we have a complete eligible scenario set
  const eligible = consideration.eligibleScenarios || 0;
  const captured = consideration.capturedScenarios || 0;
  const fullyAbsent = Math.max(0, eligible - captured);
  check(
    "SCENARIO_PARTITION",
    eligible === 0 || captured + fullyAbsent === eligible,
    { eligible, captured, fullyAbsent }
  );

  // Rank invariants
  if (position.rankEligibleObservations >= MIN_RANK_SAMPLE) {
    check(
      "RANK_NUMBER_ONE_LE_TOP_THREE",
      position.numberOneCount <= position.top3Count,
      {
        numberOneCount: position.numberOneCount,
        top3Count: position.top3Count,
      }
    );
    check(
      "RANK_TOP_THREE_LE_ELIGIBLE",
      position.top3Count <= position.rankEligibleObservations,
      {
        top3Count: position.top3Count,
        rankEligible: position.rankEligibleObservations,
      }
    );
  } else {
    warnings.push("RANK_SAMPLE_INSUFFICIENT");
  }

  // Competitor displacement grain
  check(
    "DISPLACEMENT_FULLY_ABSENT_LE_RESPONSE",
    gaps.competitorPresentScenariosFullyAbsent <= gaps.competitorPresentScenarios,
    {
      responseLevel: gaps.competitorPresentScenarios,
      fullyAbsentCp: gaps.competitorPresentScenariosFullyAbsent,
      subjectFullyAbsent: fullyAbsent,
    }
  );
  check(
    "DISPLACEMENT_FULLY_ABSENT_LE_SUBJECT_ABSENT",
    gaps.competitorPresentScenariosFullyAbsent <= fullyAbsent,
    {
      fullyAbsentCp: gaps.competitorPresentScenariosFullyAbsent,
      subjectFullyAbsent: fullyAbsent,
    }
  );
  // RESPONSE_LEVEL may exceed subject fully-absent — document, do not fail
  if (gaps.competitorPresentScenarios > fullyAbsent) {
    warnings.push("RESPONSE_LEVEL_EXCEEDS_SUBJECT_FULLY_ABSENT_EXPECTED");
  }

  // Payload mapping
  const cps = staged.competitorPresentScenarios;
  if (cps) {
    check(
      "PAYLOAD_DISPLACEMENT_COUNT",
      Number(cps.scenarioCount) === Number(gaps.competitorPresentScenarios),
      { staged: cps.scenarioCount, independent: gaps.competitorPresentScenarios }
    );
    check(
      "PAYLOAD_DISPLACEMENT_TITLE",
      cps.displayTitle === COMPETITOR_PRESENT_CARD.title ||
        cps.displayTitle == null /* older payload until rebuild */,
      { displayTitle: cps.displayTitle }
    );
    if (cps.displayTitle && cps.displayTitle !== COMPETITOR_PRESENT_CARD.title) {
      failures.push("STALE_DISPLACEMENT_DISPLAY_TITLE");
    }
    if (
      cps.countingGrain &&
      cps.countingGrain !== COMPETITOR_PRESENT_COUNTING_GRAIN
    ) {
      failures.push("STALE_DISPLACEMENT_COUNTING_GRAIN");
    }
  } else if (gaps.competitorPresentScenarios != null) {
    warnings.push("PAYLOAD_MISSING_COMPETITOR_PRESENT_BLOCK");
  }

  if (staged.considerationRate) {
    check(
      "PAYLOAD_CONSIDERATION",
      Number(staged.considerationRate.rate) ===
        Number(consideration.observationConsiderationRate) &&
        Number(staged.considerationRate.comparableObservations) ===
          Number(consideration.comparableObservations),
      {
        staged: staged.considerationRate,
        independent: {
          rate: consideration.observationConsiderationRate,
          comparable: consideration.comparableObservations,
        },
      }
    );
  }

  if (staged.scenarioPresence) {
    check(
      "PAYLOAD_SCENARIO_PRESENCE",
      Number(staged.scenarioPresence.capturedScenarios) ===
        Number(consideration.capturedScenarios) &&
        Number(staged.scenarioPresence.eligibleScenarios) ===
          Number(consideration.eligibleScenarios),
      {
        staged: staged.scenarioPresence,
        independent: {
          captured: consideration.capturedScenarios,
          eligible: consideration.eligibleScenarios,
        },
      }
    );
  }

  const calcFailures = failures.filter(
    (f) =>
      !String(f).startsWith("REGISTRY") &&
      f !== "DISPLACEMENT_TITLE_DRIFT" &&
      f !== "DISPLACEMENT_AMBIGUOUS_SUBTITLE"
  );
  let hotelState = METRIC_CERT_STATES.METRIC_CERTIFIED;
  if (failures.length) {
    hotelState =
      calcFailures.length && !registry.passed
        ? METRIC_CERT_STATES.PRESENTATION_BLOCKED
        : calcFailures.length
          ? METRIC_CERT_STATES.PARTIALLY_CERTIFIED
          : METRIC_CERT_STATES.PRESENTATION_BLOCKED;
  }
  if (!period.observations.length) hotelState = METRIC_CERT_STATES.EVIDENCE_BLOCKED;

  return {
    version: ADP_METRIC_SEMANTICS_CERTIFICATION_V1,
    propertyId: period.propertyId,
    periodId: period.periodId,
    hotelState,
    registry,
    independent: {
      considerationRate: consideration.observationConsiderationRate,
      presentObservations: consideration.presentObservations,
      comparableObservations: consideration.comparableObservations,
      scenarioPresenceRate: consideration.scenarioConsiderationCoverage,
      capturedScenarios: consideration.capturedScenarios,
      eligibleScenarios: consideration.eligibleScenarios,
      subjectFullyAbsentScenarios: fullyAbsent,
      numberOneRate: position.numberOneRate,
      top3Rate: position.top3Rate,
      rankEligibleN: position.rankEligibleObservations,
      competitorPresentScenarios: gaps.competitorPresentScenarios,
      competitorPresentScenariosFullyAbsent:
        gaps.competitorPresentScenariosFullyAbsent,
      competitorPresentObservations: gaps.competitorPresentObservations,
    },
    checks,
    failures,
    warnings,
    passed: failures.length === 0,
  };
}

/**
 * Full E2E stage helper — $0, observation reuse only.
 */
export function runAdpMetricSemanticsCertificationStage({
  period = null,
  scenarios = null,
  profile = null,
  stagedExecutiveMetrics = null,
} = {}) {
  const registry = certifyMetricSemanticsRegistry();
  if (!period) {
    return {
      stageId: "adp_metric_semantics_certification",
      qualityGate: {
        passed: registry.passed,
        reasons: registry.passed
          ? ["registry_ok_no_period"]
          : registry.failures,
      },
      hotelState: METRIC_CERT_STATES.NOT_MEASURED,
      registry,
      notes: "No period loaded — registry-only certification",
    };
  }
  const periodCert = certifyPeriodMetricSemantics({
    period,
    scenarios,
    profile,
    stagedExecutiveMetrics,
  });
  return {
    stageId: "adp_metric_semantics_certification",
    qualityGate: {
      passed: periodCert.passed,
      reasons: periodCert.passed
        ? ["metric_semantics_certified"]
        : periodCert.failures,
    },
    ...periodCert,
  };
}
