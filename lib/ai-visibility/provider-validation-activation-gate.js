/**
 * Per-provider validation activation gate (Phase 3B.2).
 * Evaluates pipeline health after first 3 logical validation calls — not recommendation content.
 */

export function evaluateProviderValidationActivationGate(result = {}) {
  const reasons = [];
  const planned = Number(result.planned ?? 3);
  const succeeded = Number(result.succeeded ?? 0);
  const failed = Number(result.failed ?? 0);
  const authErrors = Number(result.authErrors ?? 0);
  const parseFailures = Number(result.parseFailures ?? 0);
  const storageFailures = Number(result.storageFailures ?? 0);
  const resolverMalfunctions = Number(result.resolverMalfunctions ?? 0);
  const classifierMalfunctions = Number(result.classifierMalfunctions ?? 0);
  const identityMissing = Number(result.identityMissing ?? 0);
  const timeouts = Number(result.timeouts ?? 0);

  if (authErrors > 0) reasons.push("provider_auth_error");
  if (succeeded < 2) reasons.push(`success_${succeeded}_below_floor_2_of_${planned}`);
  if (parseFailures > 0) reasons.push(`parse_failures_${parseFailures}`);
  if (storageFailures > 0) reasons.push(`storage_failures_${storageFailures}`);
  if (resolverMalfunctions > 0) reasons.push(`resolver_malfunction_${resolverMalfunctions}`);
  if (classifierMalfunctions > 0) reasons.push(`classifier_malfunction_${classifierMalfunctions}`);
  if (identityMissing >= 2) reasons.push(`identity_missing_systemic_${identityMissing}`);
  if (timeouts >= 2) reasons.push(`timeouts_systemic_${timeouts}`);
  if (succeeded + failed < Math.min(3, planned)) reasons.push("activation_sample_incomplete");

  const pass = reasons.length === 0;
  return {
    ACTIVATION_GATE: pass ? "PASS" : "FAIL",
    RESULT: pass ? "PASS" : "FAIL",
    REASONS: reasons,
    COUNTS: {
      planned,
      succeeded,
      failed,
      authErrors,
      parseFailures,
      storageFailures,
      resolverMalfunctions,
      classifierMalfunctions,
      identityMissing,
      timeouts,
    },
    evaluatedAt: new Date().toISOString(),
  };
}
