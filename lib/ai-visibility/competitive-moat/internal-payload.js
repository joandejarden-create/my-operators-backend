/**
 * Internal-only benchmark payload — full diagnostics for admin/internal use.
 */

export const INTERNAL_BENCHMARK_PAYLOAD_VERSION = "internal_benchmark_payload_v1";

/**
 * Build internal benchmark diagnostics payload.
 */
export function buildInternalBenchmarkPayload(opts = {}) {
  return {
    payloadVersion: INTERNAL_BENCHMARK_PAYLOAD_VERSION,
    accessClass: "INTERNAL_ADMIN",
    subjectEntityId: opts.subjectEntityId,
    entityType: opts.entityType || "BRAND",
    indexResult: opts.indexResult || null,
    benchmarkMembers: opts.benchmarkMembers || [],
    allCompetitorScores: opts.allCompetitorScores || [],
    allCompetitorPresenceRates: opts.allCompetitorPresenceRates || {},
    commonCohortIntersection: opts.commonCohort || null,
    normalizationDiagnostics: opts.normalizationDiagnostics || null,
    promptProviderCoverage: opts.promptProviderCoverage || null,
    cohortSelectionExplanation: opts.cohortSelectionExplanation || null,
    classifierVersions: opts.classifierVersions || {},
    methodologyVersion: opts.methodologyVersion || "benchmark_engine_v1",
    historicalCompetitorSeries: opts.historicalCompetitorSeries || [],
    researchOutputs: (opts.researchOutputs || []).map((r) => ({
      ...r,
      researchOnly: true,
    })),
    datasetNamespace: opts.datasetNamespace || "DEMO_VALIDATION",
  };
}
