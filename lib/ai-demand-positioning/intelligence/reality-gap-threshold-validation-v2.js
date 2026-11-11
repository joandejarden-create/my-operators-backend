/**
 * Independent validation of 15–39% partial-recognition thresholds
 * against original Core provider responses (no paid monitoring).
 */

import { loadPropertyProfile, loadAllPeriods } from "../data-model.js";
import { computeRealityGap, REALITY_GAP_RECOGNITION_THRESHOLDS_V1 } from "./reality-gap.js";
import { ATTRIBUTE_MEASUREMENT_STATE_V1 } from "../property-attributes/adp-hotel-attribute-intelligence-standard-v1.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";
import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";
import { isAttributeEligibleForCoreAiTesting } from "../execution/response-parser.js";

export const ADP_REALITY_GAP_THRESHOLD_VALIDATION_V2 =
  "ADP_REALITY_GAP_THRESHOLD_VALIDATION_V2";

function pickCorePeriod(propertyId) {
  const periods = loadAllPeriods(propertyId) || [];
  const scored = periods
    .map((p) => ({
      p,
      obs: Array.isArray(p.observations) ? p.observations.length : 0,
      fam: String(p.measurementFamily || ""),
      cert: String(p.certificationStatus || p.status || ""),
    }))
    .filter((x) => !/LOYALTY/i.test(x.fam))
    .sort((a, b) => {
      const ac = /CERTIFIED|PRODUCTION|DEMO/.test(a.cert) ? 1 : 0;
      const bc = /CERTIFIED|PRODUCTION|DEMO/.test(b.cert) ? 1 : 0;
      if (bc !== ac) return bc - ac;
      return b.obs - a.obs;
    });
  return scored[0]?.p || periods[0] || null;
}

function extractPromptSnippet(obs) {
  return String(obs.promptText || obs.prompt || obs.scenarioPrompt || "").slice(0, 240);
}

function extractResponseSnippet(obs) {
  return String(obs.rawResponse || obs.responseText || "").slice(0, 280);
}

/**
 * Validate that a classification's recognition rate matches recount from attributesRecognized
 * on subject-mentioned observations only (prompt never counted).
 */
function recountRecognition(obsList, attributeId) {
  const subjectMentioned = obsList.filter(
    (o) => getGovernedSubjectMentioned(o) === true || o.mentioned === true
  );
  let count = 0;
  const evidenceObs = [];
  for (const obs of subjectMentioned) {
    const attrs = obs.attributesRecognized || [];
    if (attrs.includes(attributeId)) {
      count += 1;
      evidenceObs.push({
        observationId: obs.observationId || obs.id || null,
        provider: obs.provider || obs.model || null,
        inPrompt: extractPromptSnippet(obs).toLowerCase().includes(
          String(attributeId).replace(/_/g, " ")
        ),
        attrInResponseParsed: true,
        responseSnippet: extractResponseSnippet(obs),
      });
    }
  }
  return {
    subjectMentionedDenom: subjectMentioned.length,
    recognitionCount: count,
    recognitionRate:
      subjectMentioned.length > 0 ? count / subjectMentioned.length : 0,
    evidenceObs: evidenceObs.slice(0, 3),
  };
}

function expectedState(rate) {
  const thr = REALITY_GAP_RECOGNITION_THRESHOLDS_V1;
  if (rate >= thr.recognizedMinRate) return ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_RECOGNIZED;
  if (rate >= thr.partialMinRate) {
    return ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_PARTIALLY_RECOGNIZED;
  }
  return ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_NOT_RECOGNIZED;
}

/**
 * Run portfolio-wide threshold validation.
 * @returns {{ ok: boolean, failures: object[], examples: object, summary: object }}
 */
export function validateRealityGapPartialThresholdsV2(propertyIds = null) {
  const ids = propertyIds || listCanonicalAdp31PropertyIds();
  const failures = [];
  const examples = {
    recognized: [],
    partial: [],
    missed: [],
  };
  let hotelsMeasured = 0;
  let attrsChecked = 0;
  let promptLeakageSuspects = 0;

  for (const propertyId of ids) {
    const profile = loadPropertyProfile(propertyId);
    const period = pickCorePeriod(propertyId);
    if (!profile || !period?.observations?.length) {
      failures.push({
        propertyId,
        code: "NO_CORE_OBSERVATIONS",
        detail: "Cannot validate thresholds without Core observations",
      });
      continue;
    }
    hotelsMeasured += 1;
    const rg = computeRealityGap(period.observations, profile, {
      methodologyId: "RG_BINARY_GAP_V1",
    });

    // Denominator consistency
    if (rg.eligibilityGrain !== "SUBJECT_MENTIONED_CORPUS") {
      failures.push({
        propertyId,
        code: "WRONG_GRAIN",
        detail: rg.eligibilityGrain,
      });
    }

    for (const row of rg.attributeClassifications || []) {
      if (
        row.measurementState ===
          ATTRIBUTE_MEASUREMENT_STATE_V1.VERIFIED_ATTRIBUTE_NOT_TESTED ||
        row.measurementState ===
          ATTRIBUTE_MEASUREMENT_STATE_V1.INSUFFICIENT_OBSERVATION_EVIDENCE
      ) {
        continue;
      }
      if (!isAttributeEligibleForCoreAiTesting(row.attribute)) {
        failures.push({
          propertyId,
          code: "UNTESTED_IN_DENOMINATOR",
          attribute: row.attribute,
        });
        continue;
      }
      attrsChecked += 1;
      const recount = recountRecognition(period.observations, row.attribute);
      if (recount.subjectMentionedDenom !== row.total) {
        failures.push({
          propertyId,
          code: "DENOMINATOR_MISMATCH",
          attribute: row.attribute,
          classifiedTotal: row.total,
          recountedDenom: recount.subjectMentionedDenom,
        });
      }
      if (recount.recognitionCount !== row.count) {
        failures.push({
          propertyId,
          code: "NUMERATOR_MISMATCH",
          attribute: row.attribute,
          classifiedCount: row.count,
          recountedCount: recount.recognitionCount,
        });
      }
      const expected = expectedState(recount.recognitionRate);
      if (expected !== row.measurementState) {
        failures.push({
          propertyId,
          code: "THRESHOLD_STATE_MISMATCH",
          attribute: row.attribute,
          rate: recount.recognitionRate,
          expected,
          actual: row.measurementState,
        });
      }

      // Prompt leakage: attribute must not be classified recognized solely from prompt
      // (we only count attributesRecognized from response parser — verify evidence has response)
      for (const ev of recount.evidenceObs) {
        if (!ev.responseSnippet || ev.responseSnippet.length < 20) {
          promptLeakageSuspects += 1;
          failures.push({
            propertyId,
            code: "EMPTY_RESPONSE_EVIDENCE",
            attribute: row.attribute,
            observationId: ev.observationId,
          });
        }
      }

      const sample = {
        propertyId,
        hotelName: profile.name,
        attribute: row.attribute,
        label: row.label,
        measurementState: row.measurementState,
        recognitionRatePct: row.recognitionRate,
        count: row.count,
        total: row.total,
        periodId: period.periodId,
        providerSample: recount.evidenceObs[0]?.provider || null,
        responseSnippet: recount.evidenceObs[0]?.responseSnippet || null,
      };
      if (
        row.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_RECOGNIZED &&
        examples.recognized.length < 6
      ) {
        examples.recognized.push(sample);
      }
      if (
        row.measurementState ===
          ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_PARTIALLY_RECOGNIZED &&
        examples.partial.length < 8
      ) {
        examples.partial.push(sample);
      }
      if (
        row.measurementState === ATTRIBUTE_MEASUREMENT_STATE_V1.TESTED_NOT_RECOGNIZED &&
        examples.missed.length < 6
      ) {
        examples.missed.push(sample);
      }
    }
  }

  // Require representative examples across bands
  if (examples.recognized.length < 2) {
    failures.push({
      code: "INSUFFICIENT_RECOGNIZED_EXAMPLES",
      detail: examples.recognized.length,
    });
  }
  if (examples.partial.length < 2) {
    failures.push({
      code: "INSUFFICIENT_PARTIAL_EXAMPLES",
      detail: examples.partial.length,
    });
  }
  if (examples.missed.length < 2) {
    failures.push({
      code: "INSUFFICIENT_MISSED_EXAMPLES",
      detail: examples.missed.length,
    });
  }

  const ok = failures.length === 0;
  return {
    gate: ADP_REALITY_GAP_THRESHOLD_VALIDATION_V2,
    ok,
    thresholdValidationPassed: ok,
    thresholds: REALITY_GAP_RECOGNITION_THRESHOLDS_V1,
    summary: {
      hotelsChecked: ids.length,
      hotelsMeasured,
      attrsChecked,
      failureCount: failures.length,
      promptLeakageSuspects,
      exampleCounts: {
        recognized: examples.recognized.length,
        partial: examples.partial.length,
        missed: examples.missed.length,
      },
    },
    failures: failures.slice(0, 80),
    examples,
  };
}
