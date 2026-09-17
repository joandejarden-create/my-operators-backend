/**
 * Parallel fallback / escalation gate for GDI research.
 * REUSABLE_RESEARCH_LOGIC — Parallel is selective, never automatic co-primary.
 *
 * Routing: NATIVE → confidence/completeness gate → PARALLEL only when needed.
 */

export const PARALLEL_ESCALATION_TRIGGER = Object.freeze({
  LOW_NATIVE_CONFIDENCE: "LOW_NATIVE_CONFIDENCE",
  INSUFFICIENT_OFFICIAL_SOURCES: "INSUFFICIENT_OFFICIAL_SOURCES",
  UNRESOLVED_VENUE_STATUS: "UNRESOLVED_VENUE_STATUS",
  UNRESOLVED_WHO: "UNRESOLVED_WHO",
  SOURCE_CONTRADICTION: "SOURCE_CONTRADICTION",
  SPARSE_OPPORTUNITY_DISCOVERY: "SPARSE_OPPORTUNITY_DISCOVERY",
  HIGH_VALUE_NEEDS_CORROBORATION: "HIGH_VALUE_NEEDS_CORROBORATION",
  CRITICAL_FIELD_UNKNOWN: "CRITICAL_FIELD_UNKNOWN",
});

export const PARALLEL_ESCALATION_POLICY = Object.freeze({
  mode: "GATED_FALLBACK",
  neverAutomaticCoPrimary: true,
  parallelIsNotCanonicalTruth: true,
  minConfidenceToSkip: 55,
  minOfficialSourceCountToSkip: 1,
  maxNativeDiscoveriesBeforeSparse: 2,
});

/**
 * Decide whether Native research should escalate to Parallel.
 * @returns {{ escalate: boolean, triggers: string[], rationale: string, policy: object }}
 */
export function shouldEscalateToParallel({
  nativeResult = null,
  taskKind = null,
  hotelContext = null,
  force = false,
} = {}) {
  if (force) {
    return {
      escalate: true,
      triggers: ["FORCED"],
      rationale: "Forced escalation for evaluation/benchmark.",
      policy: PARALLEL_ESCALATION_POLICY,
      hotelId: hotelContext?.hotelId || null,
      taskKind,
    };
  }

  const triggers = [];
  const conf = Number(
    nativeResult?.confidence ??
      nativeResult?.evidenceConfidence ??
      nativeResult?.researchCompleteness?.confidence ??
      NaN
  );
  if (Number.isFinite(conf) && conf < PARALLEL_ESCALATION_POLICY.minConfidenceToSkip) {
    triggers.push(PARALLEL_ESCALATION_TRIGGER.LOW_NATIVE_CONFIDENCE);
  }

  const sources = nativeResult?.sourceEvidence || nativeResult?.sources || [];
  const officialCount = Array.isArray(sources)
    ? sources.filter((s) => {
        const a = String(s.authority || s.sourceAuthority || s.url || "").toLowerCase();
        return /official|first.?party|\.gov|\.org|hotel|marriott|hilton/.test(a);
      }).length
    : 0;
  if (officialCount < PARALLEL_ESCALATION_POLICY.minOfficialSourceCountToSkip) {
    triggers.push(PARALLEL_ESCALATION_TRIGGER.INSUFFICIENT_OFFICIAL_SOURCES);
  }

  const venue =
    nativeResult?.venueStatus ||
    nativeResult?.sourcingStatus ||
    nativeResult?.venueSourcingStatus;
  if (!venue || venue === "UNKNOWN" || /unknown|unresolved/i.test(String(venue))) {
    triggers.push(PARALLEL_ESCALATION_TRIGGER.UNRESOLVED_VENUE_STATUS);
  }

  const who =
    nativeResult?.contactEvidence ||
    nativeResult?.primaryContact ||
    nativeResult?.who;
  if (!who || (typeof who === "object" && !who.name && !who.organization)) {
    triggers.push(PARALLEL_ESCALATION_TRIGGER.UNRESOLVED_WHO);
  }

  if (
    Array.isArray(nativeResult?.contradictions) &&
    nativeResult.contradictions.length > 0
  ) {
    triggers.push(PARALLEL_ESCALATION_TRIGGER.SOURCE_CONTRADICTION);
  }

  const discoveryCount =
    nativeResult?.discoveryCount ??
    nativeResult?.opportunitiesDiscovered ??
    (Array.isArray(nativeResult?.opportunities) ? nativeResult.opportunities.length : null);
  if (
    discoveryCount != null &&
    discoveryCount <= PARALLEL_ESCALATION_POLICY.maxNativeDiscoveriesBeforeSparse
  ) {
    triggers.push(PARALLEL_ESCALATION_TRIGGER.SPARSE_OPPORTUNITY_DISCOVERY);
  }

  const completeness = nativeResult?.researchCompleteness || {};
  const criticalUnknown =
    completeness.hasEvent === false ||
    completeness.hasOrg === false ||
    nativeResult?.factEstimateInference?.roomDemand === "UNKNOWN";
  if (criticalUnknown) {
    triggers.push(PARALLEL_ESCALATION_TRIGGER.CRITICAL_FIELD_UNKNOWN);
  }

  if (
    taskKind === "HIGH_VALUE_CORROBORATION" ||
    nativeResult?.needsCorroboration === true
  ) {
    triggers.push(PARALLEL_ESCALATION_TRIGGER.HIGH_VALUE_NEEDS_CORROBORATION);
  }

  const escalate = triggers.length >= 2;
  return {
    escalate,
    triggers,
    rationale: escalate
      ? `Escalate to Parallel: ${triggers.join(", ")}`
      : triggers.length
        ? `Native retained — single weak signal (${triggers.join(", ")}) does not meet gate`
        : "Native confidence/completeness sufficient — skip Parallel",
    policy: PARALLEL_ESCALATION_POLICY,
    hotelId: hotelContext?.hotelId || null,
    taskKind,
  };
}

/**
 * Simulate Webhound-unavailable routing for GDI research paths.
 */
export function resolveResearchProviderPath({
  webhoundUnavailable = process.env.WEBHOUND_UNAVAILABLE === "true" ||
    process.env.WEBHOUND_DISABLED === "1" ||
    process.env.WEBHOUND_DISABLED === "true",
  nativeResult = null,
  preferParallelWhenGated = true,
} = {}) {
  const gate = shouldEscalateToParallel({ nativeResult });
  if (webhoundUnavailable) {
    return {
      primary: "NATIVE",
      fallback: gate.escalate && preferParallelWhenGated ? "PARALLEL" : null,
      webhound: "UNAVAILABLE",
      classification: "EVALUATION_ONLY",
      gate,
      audit: {
        providerPath:
          gate.escalate && preferParallelWhenGated
            ? "NATIVE_PLUS_GATED_PARALLEL"
            : "NATIVE_ONLY",
        webhoundRequired: false,
      },
    };
  }
  return {
    primary: "NATIVE",
    fallback: gate.escalate && preferParallelWhenGated ? "PARALLEL" : null,
    webhound: "EVALUATION_ONLY",
    classification: "EVALUATION_ONLY",
    gate,
    audit: {
      providerPath:
        gate.escalate && preferParallelWhenGated
          ? "NATIVE_PLUS_GATED_PARALLEL"
          : "NATIVE_ONLY",
      webhoundRequired: false,
    },
  };
}
