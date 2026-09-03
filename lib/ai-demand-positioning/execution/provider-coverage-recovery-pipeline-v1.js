/**
 * Permanent ADP pipeline step: after OBSERVATION_COMPLETE, detect provider gaps
 * and optionally run bounded same-period recovery before certification.
 *
 * Paid recovery requires founderApproval=true (or ADP_PROVIDER_RECOVERY_APPROVED=1).
 * Never creates a new period. Never publishes.
 */

import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import {
  buildProviderCoverageGapLedger,
  evaluateProviderCoverageRecoveryGate,
  classifyResidualProviderCoverage,
  RECOMMENDED_RETRY_POLICY,
  DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP,
} from "../measurement-assurance/provider-coverage-recovery-v1.js";
import { recoverMissingObservationsInPeriod } from "./same-period-provider-recovery-v1.js";
import { analyzeProviderInstability } from "./provider-instability-monitor-v1.js";
import { isComparableObservation } from "../metrics/grain-governance.js";

export const PROVIDER_RECOVERY_PIPELINE_STEP = "PROVIDER_COVERAGE_RECOVERY";

/**
 * Detect → classify → (optional) recover → return certification-ready status.
 */
export async function runProviderCoverageRecoveryPipelineStep({
  period,
  propertyProfile,
  providers = null,
  founderApproval = false,
  dryRun = true,
  policy = RECOMMENDED_RETRY_POLICY,
  onProgress = null,
}) {
  const scenarios = buildScenarioUniverse(propertyProfile);
  const ledgerBefore = buildProviderCoverageGapLedger({
    propertyId: period.propertyId,
    period,
    scenarios,
    propertyProfile,
  });
  const gateBefore = evaluateProviderCoverageRecoveryGate(ledgerBefore);
  const instabilityBefore = analyzeProviderInstability({ period, scenarios });

  const envApproved = process.env.ADP_PROVIDER_RECOVERY_APPROVED === "1";
  const approved = Boolean(founderApproval || envApproved);

  const targetsByProvider = Object.create(null);
  for (const m of ledgerBefore.recoverableUnattempted) {
    if (providers && !providers.includes(m.provider)) continue;
    if (!targetsByProvider[m.provider]) targetsByProvider[m.provider] = [];
    const sc = scenarios.find((s) => s.scenarioId === m.scenarioId);
    targetsByProvider[m.provider].push({
      observationId: m.observationId,
      scenarioId: m.scenarioId,
      intent: m.intent,
      query: sc?.query || null,
    });
  }

  const execution = [];
  let recoveredTotal = 0;
  let residualTotal = 0;

  if (Object.keys(targetsByProvider).length && approved) {
    for (const [provider, targets] of Object.entries(targetsByProvider)) {
      const outcome = await recoverMissingObservationsInPeriod({
        period,
        propertyProfile,
        targets,
        provider,
        policy,
        dryRun,
        onProgress,
        save: !dryRun,
      });
      recoveredTotal += outcome.recovered;
      residualTotal += outcome.residualMissing;
      execution.push(outcome);
    }
  }

  const ledgerAfter = buildProviderCoverageGapLedger({
    propertyId: period.propertyId,
    period,
    scenarios,
    propertyProfile,
  });
  const gateAfter = evaluateProviderCoverageRecoveryGate(ledgerAfter);
  const residual = classifyResidualProviderCoverage(ledgerAfter);
  const instabilityAfter = analyzeProviderInstability({ period, scenarios });

  return {
    step: PROVIDER_RECOVERY_PIPELINE_STEP,
    defectClass: DEFECT_UNRECOVERED_PROVIDER_COVERAGE_GAP,
    periodId: period.periodId,
    propertyId: period.propertyId,
    approved,
    dryRun,
    skippedPaidRecovery: Boolean(Object.keys(targetsByProvider).length) && !approved,
    before: {
      gate: gateBefore,
      missing: ledgerBefore.missingObservations.length,
      recoverableUnattempted: ledgerBefore.recoverableUnattempted.length,
      residualCoverage: ledgerBefore.residualCoverage,
    },
    after: {
      gate: gateAfter,
      missing: ledgerAfter.missingObservations.length,
      recoverableUnattempted: ledgerAfter.recoverableUnattempted.length,
      residualCoverage: residual,
    },
    recoveredTotal,
    residualTotal,
    execution,
    instability: { before: instabilityBefore, after: instabilityAfter },
    next:
      gateAfter.blocksFullCertification
        ? "ASSURANCE_REVIEW_REQUIRED — material recoverable gaps remain unattempted"
        : "Proceed to full Measurement Assurance → certification decision",
    certificationMayProceed: !gateAfter.blocksFullCertification,
  };
}

/**
 * Build recovery targets for a single provider from a period (helper for scripts).
 */
export function listRecoverableTargets(period, scenarios, provider) {
  return (period.observations || [])
    .filter((o) => o.provider === provider && !isComparableObservation(o))
    .map((o) => {
      const sc = scenarios.find((s) => s.scenarioId === o.scenarioId) || {};
      return {
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        intent: sc.intent || null,
        query: sc.query || null,
      };
    });
}
