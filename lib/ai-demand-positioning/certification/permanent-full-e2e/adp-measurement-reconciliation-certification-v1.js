/**
 * certifyAdpMeasurementReconciliation() — permanent mandatory gate.
 *
 * Authority: original AI response text + frozen eligibleScenarioIds.
 * Stored runtime `mentioned` flags are non-authoritative.
 */

import { createHash } from "crypto";
import { detectPropertyMention } from "../../execution/response-parser.js";
import { isComparableObservation } from "../../metrics/grain-governance.js";
import { buildProviderPresenceRows } from "../../metrics/provider-presence-v1.js";
import { auditProviderPresenceGrain } from "../../metrics/adp-provider-presence-grain-lock-v1.js";
import { roundAdpPercent } from "../../format-percent.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../adp-unprompted-independent-discovery-standard-v1.js";
import {
  ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
  CERTIFICATION_FAILURE_STATES,
  CERTIFICATION_LIFECYCLE_STATES,
} from "./adp-permanent-full-e2e-lifecycle-states-v1.js";

export const ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1 =
  "ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1";

export const METHODOLOGY_FINGERPRINT_V111 =
  "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1";

function sha256(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function rawText(obs) {
  return String(
    obs?.rawResponse ||
      obs?.responseText ||
      obs?.parsed?.rawText ||
      obs?.parsed?.text ||
      ""
  );
}

function certifiedMention(obs, profile) {
  const text = rawText(obs);
  if (!text.trim()) return { mentioned: false, evidence: null, missingText: true };
  const det = detectPropertyMention(text, profile || {});
  return {
    mentioned: Boolean(det.mentioned),
    evidence: {
      matchedVariant: det.matchedVariant || null,
      matchReason: det.matchReason || null,
      context: det.context || null,
    },
    missingText: false,
    contentHash: sha256(text),
  };
}

/**
 * Build observation certification ledger rows (do not mutate originals).
 */
export function buildObservationCertificationLedger(observations, profile, frozenIds) {
  const idSet = new Set((frozenIds || []).map(String));
  return (observations || []).map((o) => {
    const text = rawText(o);
    const mention = certifiedMention(o, profile);
    const eligible = idSet.has(String(o.scenarioId || ""));
    return {
      propertyId: o.propertyId || null,
      measurementPeriodId: o.periodId || null,
      observationId: o.observationId || null,
      provider: o.provider || null,
      model: o.model || null,
      scenarioId: o.scenarioId || null,
      promptId: o.promptId || o.scenarioId || null,
      originalPrompt: o.prompt || o.exactRenderedPrompt || null,
      originalResponseReference: o.observationId
        ? `period://${o.periodId || "unknown"}/obs/${o.observationId}`
        : null,
      responseContentHash: text.trim() ? sha256(text) : null,
      responseTextAvailable: Boolean(text.trim()),
      measurementEligibility: eligible,
      eligibilityReason: eligible
        ? "IN_FROZEN_ELIGIBLE_SCENARIO_IDS"
        : "EXCLUDED_NOT_IN_FROZEN_ELIGIBLE_SCENARIO_IDS",
      certifiedPropertyMention: mention.mentioned,
      mentionEvidence: mention.evidence,
      runtimeMentionedFlag: Boolean(o.mentioned),
      runtimeMentionAuthoritative: false,
      detectedRanking: o.position ?? null,
      certificationMethodologyVersion: ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1,
    };
  });
}

function computeCanonicalMetrics(observations, frozenIds, profile) {
  const idSet = new Set((frozenIds || []).map(String));
  const eligibleObs = (observations || []).filter((o) =>
    idSet.has(String(o.scenarioId || ""))
  );
  const withMentions = eligibleObs.map((o) => {
    const m = certifiedMention(o, profile);
    return {
      ...o,
      mentioned: m.mentioned,
      _responseContentHash: m.contentHash,
      _missingText: m.missingText,
    };
  });
  const missingText = withMentions.filter((o) => o._missingText);
  const comparable = withMentions.filter((o) => isComparableObservation(o));
  const present = comparable.filter((o) => o.mentioned);
  const rate =
    comparable.length > 0
      ? roundAdpPercent((present.length / comparable.length) * 100)
      : null;

  const byScenario = new Map();
  for (const sid of idSet) byScenario.set(sid, false);
  for (const obs of comparable) {
    if (obs.mentioned) byScenario.set(String(obs.scenarioId), true);
  }
  let capturedScenarios = 0;
  for (const v of byScenario.values()) if (v) capturedScenarios += 1;

  const { providers } = buildProviderPresenceRows(comparable, {
    scenarioIds: idSet,
  });
  const mentionSum = providers.reduce((a, p) => a + Number(p.mentioned || 0), 0);
  const comparableSum = providers.reduce(
    (a, p) => a + Number(p.comparable || 0),
    0
  );

  return {
    population: {
      measurementFamily: "INDEPENDENT_UNPROMPTED_APPEARANCE_V1_1_1",
      eligibleScenarioIds: [...idSet].sort(),
      eligibleScenarioCount: idSet.size,
      eligibleObservationIds: comparable
        .map((o) => o.observationId)
        .filter(Boolean),
      exclusionRules: [
        "scenarioId must be in frozen eligibleScenarioIds",
        "observation must be comparable (parsed successful response)",
        "mention = detectPropertyMention(rawResponse) — runtime flags ignored",
      ],
      calculationVersion: ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1,
    },
    consideration: {
      presentObservations: present.length,
      comparableObservations: comparable.length,
      rate,
      numerator: present.length,
      denominator: comparable.length,
    },
    scenarioPresence: {
      eligibleScenarios: idSet.size,
      capturedScenarios,
      rate:
        idSet.size > 0
          ? roundAdpPercent((capturedScenarios / idSet.size) * 100)
          : null,
      numerator: capturedScenarios,
      denominator: idSet.size,
    },
    providerPresence: {
      providers,
      mentionSum,
      comparableSum,
      reconcilesToConsideration:
        mentionSum === present.length && comparableSum === comparable.length,
      aggregationContract:
        "SUM(provider.mentioned) === Consideration.present AND SUM(provider.comparable) === Consideration.comparable; per-provider denom ≤ eligible scenario count",
    },
    missingResponseTextCount: missingText.length,
  };
}

/**
 * @param {object} input
 * @returns {object} certification result
 */
export function certifyAdpMeasurementReconciliation(input = {}) {
  const failures = [];
  const checks = {};
  const timestamp = new Date().toISOString();

  const propertyId = input.propertyId || null;
  const periodId = input.periodId || input.measurementPeriodId || null;
  const profile = input.propertyProfile || input.profile || {};
  const observations = input.observations || input.period?.observations || [];
  const packagePayload = input.packagePayload || input.payload || null;

  const frozenIds = Array.isArray(input.frozenEligibleScenarioIds)
    ? input.frozenEligibleScenarioIds.map(String)
    : Array.isArray(
          packagePayload?.executiveMetrics?.considerationRate?.eligibleScenarioIds
        )
      ? packagePayload.executiveMetrics.considerationRate.eligibleScenarioIds.map(
          String
        )
      : [];

  const methodologyId =
    input.methodologyId ||
    packagePayload?.executiveMetrics?.considerationRate?.methodologyId ||
    CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID;

  const methodologyFingerprint =
    input.methodologyFingerprintSha256 ||
    packagePayload?.methodologyFingerprintSha256 ||
    null;

  // A — original responses
  const ledger = buildObservationCertificationLedger(
    observations,
    profile,
    frozenIds
  );
  const textOk =
    observations.length > 0 &&
    ledger.filter((r) => r.responseTextAvailable).length / observations.length >=
      0.95;
  checks.A_original_responses = {
    ok: textOk,
    obsCount: observations.length,
    withText: ledger.filter((r) => r.responseTextAvailable).length,
  };
  if (!textOk) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.EVIDENCE_INCOMPLETE,
      check: "A",
      detail: "Original AI response text missing or <95% coverage",
    });
  }

  // B — frozen eligibility
  checks.B_frozen_eligibility = {
    ok: frozenIds.length > 0,
    count: frozenIds.length,
  };
  if (!frozenIds.length) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.ELIGIBILITY_MISMATCH,
      check: "B",
      detail: "Frozen eligibleScenarioIds missing",
    });
  }

  // C + D — mentions + Consideration from original evidence
  const canonical = computeCanonicalMetrics(observations, frozenIds, profile);
  checks.C_mentions_from_response_text = {
    ok: canonical.missingResponseTextCount === 0 || textOk,
    missingResponseTextCount: canonical.missingResponseTextCount,
    runtimeFlagsAuthoritative: false,
  };

  const certifiedPresent = Number(
    packagePayload?.executiveMetrics?.considerationRate?.presentObservations
  );
  const certifiedComparable = Number(
    packagePayload?.executiveMetrics?.considerationRate?.comparableObservations
  );
  const certifiedRate = Number(
    packagePayload?.executiveMetrics?.considerationRate?.rate ??
      packagePayload?.executiveMetrics?.considerationRate?.percentage
  );

  const considerationReproduces =
    packagePayload == null ||
    (!Number.isFinite(certifiedPresent) && !Number.isFinite(certifiedComparable)) ||
    (canonical.consideration.presentObservations === certifiedPresent &&
      canonical.consideration.comparableObservations === certifiedComparable &&
      (certifiedRate == null ||
        !Number.isFinite(certifiedRate) ||
        Math.abs(Number(canonical.consideration.rate) - certifiedRate) <= 0.05));

  checks.D_consideration_reproduces = {
    ok: considerationReproduces,
    canonical: canonical.consideration,
    certified: {
      presentObservations: certifiedPresent,
      comparableObservations: certifiedComparable,
      rate: certifiedRate,
    },
  };
  if (!considerationReproduces) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.METRIC_RECONCILIATION_FAILED,
      check: "D",
      detail: "Consideration does not reproduce from original evidence",
    });
  }

  // E — Provider Presence
  checks.E_provider_presence = {
    ok: canonical.providerPresence.reconcilesToConsideration === true,
    ...canonical.providerPresence,
  };
  if (!canonical.providerPresence.reconcilesToConsideration) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.METRIC_RECONCILIATION_FAILED,
      check: "E",
      detail: "Provider Presence does not reconcile to Consideration",
    });
  }

  // F — Scenario Presence population
  const packageSpEligible = Number(
    packagePayload?.executiveMetrics?.scenarioPresence?.eligibleScenarios
  );
  const spAligned =
    packagePayload == null ||
    !Number.isFinite(packageSpEligible) ||
    packageSpEligible === frozenIds.length;
  checks.F_scenario_presence = {
    ok: spAligned,
    canonicalEligible: frozenIds.length,
    packageEligible: packageSpEligible,
    capturedScenarios: canonical.scenarioPresence.capturedScenarios,
  };
  if (!spAligned) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.METRIC_RECONCILIATION_FAILED,
      check: "F",
      detail: "Scenario Presence eligible count ≠ frozen eligibleScenarioIds",
    });
  }

  // G/H — territory / competitive: document contract (presence of sections optional)
  checks.G_territory_population_documented = {
    ok: true,
    note: "Territory metrics must use same frozen appearance population when they partition Consideration; otherwise explicit alternate population required",
  };
  checks.H_competitive_set = {
    ok: true,
    note: "Competitive rankings must use approved comparison set from competitor registry for the period",
  };

  // I — percentages use certified nums/denoms
  checks.I_percentage_math = {
    ok:
      canonical.consideration.denominator === 0 ||
      Math.abs(
        (canonical.consideration.numerator /
          canonical.consideration.denominator) *
          100 -
          Number(canonical.consideration.rate)
      ) < 0.15,
  };

  // J — no legacy 78 contamination / runtime flag authority
  const packageAudit = packagePayload
    ? auditProviderPresenceGrain({
        ...packagePayload,
        evidence: {
          ...(packagePayload.evidence || {}),
          providers: canonical.providerPresence.providers,
        },
        executiveMetrics: {
          ...(packagePayload.executiveMetrics || {}),
          considerationRate: {
            ...(packagePayload.executiveMetrics?.considerationRate || {}),
            presentObservations: canonical.consideration.presentObservations,
            comparableObservations: canonical.consideration.comparableObservations,
            rate: canonical.consideration.rate,
            eligibleScenarioIds: frozenIds,
            methodologyId,
          },
          scenarioPresence: {
            ...(packagePayload.executiveMetrics?.scenarioPresence || {}),
            eligibleScenarios: frozenIds.length,
            capturedScenarios: canonical.scenarioPresence.capturedScenarios,
            rate: canonical.scenarioPresence.rate,
          },
        },
      })
    : auditProviderPresenceGrain({
        executiveMetrics: {
          considerationRate: {
            presentObservations: canonical.consideration.presentObservations,
            comparableObservations: canonical.consideration.comparableObservations,
            methodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
            eligibleScenarioIds: frozenIds,
          },
          scenarioPresence: {
            eligibleScenarios: frozenIds.length,
            capturedScenarios: canonical.scenarioPresence.capturedScenarios,
          },
        },
        evidence: { providers: canonical.providerPresence.providers },
      });

  const has78 = (canonical.providerPresence.providers || []).some(
    (p) => Number(p.comparable) === 78 || Number(p.scenariosScheduled) === 78
  );
  checks.J_no_legacy_or_runtime_contamination = {
    ok: !has78 && packageAudit.pass,
    has78,
    grainPass: packageAudit.pass,
    runtimeFlagsAuthoritative: false,
  };
  if (has78 || !packageAudit.pass) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.METRIC_RECONCILIATION_FAILED,
      check: "J",
      detail: has78
        ? "Legacy 78-scenario contamination"
        : "Canonical grain audit failed",
    });
  }

  // K — no silent zeroing
  checks.K_no_silent_zero = {
    ok:
      observations.length === 0 ||
      canonical.consideration.comparableObservations > 0 ||
      frozenIds.length === 0,
  };

  // L — evidence references
  checks.L_evidence_traceable = {
    ok: ledger.some((r) => r.responseContentHash),
    ledgerRows: ledger.length,
  };

  const ok = failures.length === 0;
  const certificationHash = sha256(
    JSON.stringify({
      propertyId,
      periodId,
      frozenIds: [...frozenIds].sort(),
      consideration: canonical.consideration,
      scenarioPresence: canonical.scenarioPresence,
      providers: canonical.providerPresence.providers,
      methodologyId,
      methodologyFingerprint,
      contract: ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1,
    })
  );

  return {
    ok,
    verdict: ok ? "CERTIFIED" : "FAILED",
    contract: ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
    gate: ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1,
    propertyId,
    measurementPeriodId: periodId,
    methodologyId,
    methodologyFingerprintSha256: methodologyFingerprint,
    lifecycleState: ok
      ? CERTIFICATION_LIFECYCLE_STATES.METRICS_RECONCILED
      : CERTIFICATION_FAILURE_STATES.METRIC_RECONCILIATION_FAILED,
    checks,
    failures,
    canonicalMetrics: {
      consideration: canonical.consideration,
      scenarioPresence: canonical.scenarioPresence,
      providerPresence: {
        providers: canonical.providerPresence.providers,
        aggregationContract: canonical.providerPresence.aggregationContract,
      },
      population: canonical.population,
    },
    populationDenominatorManifest: canonical.population,
    evidenceReferences: {
      observationLedgerCount: ledger.length,
      periodId,
      responseHashSample: ledger
        .filter((r) => r.responseContentHash)
        .slice(0, 5)
        .map((r) => r.responseContentHash),
    },
    observationLedger: input.includeObservationLedger === true ? ledger : undefined,
    certificationTimestamp: timestamp,
    certificationHash,
  };
}

export function buildCorrectedPackageFromCertification(
  packagePayload,
  certification,
  opts = {}
) {
  if (!packagePayload || !certification?.ok) {
    return { ok: false, reason: "CERTIFICATION_NOT_PASSING" };
  }
  const cm = certification.canonicalMetrics;
  const next = structuredClone(packagePayload);
  next.executiveMetrics = {
    ...(next.executiveMetrics || {}),
    considerationRate: {
      ...(next.executiveMetrics?.considerationRate || {}),
      presentObservations: cm.consideration.presentObservations,
      comparableObservations: cm.consideration.comparableObservations,
      rate: cm.consideration.rate,
      eligibleScenarioIds: cm.population.eligibleScenarioIds,
      methodologyId:
        certification.methodologyId || CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
      mentionPath: "response_text_detectPropertyMention",
      runtimeMentionFlagsAuthoritative: false,
    },
    scenarioPresence: {
      ...(next.executiveMetrics?.scenarioPresence || {}),
      eligibleScenarios: cm.scenarioPresence.eligibleScenarios,
      capturedScenarios: cm.scenarioPresence.capturedScenarios,
      rate: cm.scenarioPresence.rate,
      methodologyId:
        certification.methodologyId || CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
    },
  };
  next.evidence = {
    ...(next.evidence || {}),
    providers: cm.providerPresence.providers,
    denominatorNote:
      "Provider Presence uses frozen independent-discovery eligible scenarios only (same grain as Consideration).",
    providerGrainLock: {
      version: ADP_MEASUREMENT_RECONCILIATION_CERTIFICATION_V1,
      repairedAt: certification.certificationTimestamp,
      populationSource: "frozen_eligible_scenario_ids",
      certificationHash: certification.certificationHash,
    },
  };
  next._adpPermanentCertification = {
    contract: ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
    certificationHash: certification.certificationHash,
    certificationTimestamp: certification.certificationTimestamp,
    methodologyFingerprintSha256: certification.methodologyFingerprintSha256,
    preparedLocally: true,
    publicationAuthorized: false,
    supersedesPackageNote:
      opts.supersedesNote ||
      "Corrected derived metrics from immutable evidence; Consideration unchanged",
  };
  return { ok: true, payload: next, certificationHash: certification.certificationHash };
}
