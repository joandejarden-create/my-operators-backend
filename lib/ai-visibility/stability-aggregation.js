/**
 * Repeated-testing / stability aggregation.
 * Descriptive states only. No numeric confidence, p-values, or pooled pseudo-scores.
 * Does not change Presence / QM / citation / P0C raw gap semantics.
 */

export const STABILITY_AGGREGATION_VERSION = "ai_visibility_stability_aggregation_v1";

export const REPEAT_TYPES = Object.freeze(["EXACT_REPEAT", "CONTROLLED_VARIANT"]);

export const RECURRENCE_STATES = Object.freeze([
  "INSUFFICIENT_OBSERVATIONS",
  "ONE_OFF",
  "EARLY_REPEATED_EVIDENCE",
  "RECURRENT",
  "INFREQUENT",
]);

export const STABILITY_STATES = Object.freeze([
  "INSUFFICIENT_OBSERVATIONS",
  "CONSISTENTLY_PRESENT",
  "CONSISTENTLY_ABSENT",
  "MIXED",
  "CHANGING",
]);

export const CROSS_PROVIDER_STATES = Object.freeze([
  "INSUFFICIENT_PROVIDER_COVERAGE",
  "ALIGNED_PRESENT",
  "ALIGNED_ABSENT",
  "PARTIAL_ALIGNMENT",
  "HIGH_VARIABILITY",
]);

export const TIME_WINDOWS = Object.freeze([
  "SAME_RUN_REPETITION",
  "SHORT_TERM",
  "LONGITUDINAL",
]);

export const MIN_OBSERVATIONS = Object.freeze({
  INSUFFICIENT: 1,
  EARLY_REPEATED: 2,
  STABILITY_ELIGIBLE: 3,
  note: "N=3 is eligible for descriptive stability. It is not high confidence.",
});

export const STABILITY_METHODOLOGY_COPY =
  "AI responses can vary between runs. Dealality uses repeated observations on priority owner-decision prompts to distinguish recurring patterns from one-off results. We report the number and consistency of observed responses rather than assigning artificial confidence scores.";

export const FORBIDDEN_STABILITY_COPY = Object.freeze([
  "high confidence",
  "95% reliable",
  "statistically significant",
  "model certainty",
  "probability",
  "confidence score",
  "stability score",
  "reliability score",
]);

const MS_DAY = 86400000;

export function stabilityGrainKey(input = {}) {
  return [
    input.promptId || "",
    input.brandId || "",
    String(input.provider || "").toLowerCase(),
    String(input.language || "en").toLowerCase(),
    input.geographyKey || input.geography || "",
    input.repeatType || "EXACT_REPEAT",
  ].join("|");
}

export function classifyTimeWindow(firstObservedAt, lastObservedAt) {
  if (!firstObservedAt || !lastObservedAt) return null;
  const a = Date.parse(firstObservedAt);
  const b = Date.parse(lastObservedAt);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  const spanMs = Math.abs(b - a);
  if (spanMs < MS_DAY) return "SAME_RUN_REPETITION";
  if (spanMs < 14 * MS_DAY) return "SHORT_TERM";
  return "LONGITUDINAL";
}

/**
 * Stage B is days after the Aug 14 baseline. Never label this validation
 * LONGITUDINALLY_STABLE / MULTI_WEEK_STABLE / LONG_TERM_PATTERN.
 */
export function classifyStageBTimeWindow(firstObservedAt, lastObservedAt) {
  const w = classifyTimeWindow(firstObservedAt, lastObservedAt);
  if (!w) return null;
  if (w === "LONGITUDINAL") return "SHORT_TERM";
  return w;
}

function toIso(value) {
  if (!value) return null;
  const t = Date.parse(value);
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

function uniqueSortedDates(values = []) {
  const set = new Set();
  for (const v of values) {
    const iso = toIso(v);
    if (!iso) continue;
    set.add(iso.slice(0, 10));
  }
  return [...set].sort();
}

function presentForBrand(obs, brandId) {
  if (!brandId) {
    return Array.isArray(obs.presentEntityIds) && obs.presentEntityIds.length > 0
      ? true
      : Boolean(obs.presenceObserved);
  }
  const ids = obs.presentEntityIds || [];
  if (ids.includes(brandId)) return true;
  const mentions = obs.mentions || obs.payload?.mentions || [];
  return mentions.some(
    (m) =>
      m.entityId === brandId ||
      m.resolvedEntityId === brandId ||
      m.canonicalEntityId === brandId
  );
}

function citationDomains(obs) {
  const citations = obs.citations || obs.payload?.citations || [];
  const domains = [];
  for (const c of citations) {
    const d = String(c.domain || c.sourceDomain || "").trim().toLowerCase();
    if (d) domains.push(d);
  }
  return [...new Set(domains)];
}

/**
 * Classify recurrence + stability for one grain.
 * Exact repeats and controlled variants must not share a denominator.
 */
export function aggregateStabilitySeries(observations = [], options = {}) {
  const brandId = options.brandId || null;
  const repeatType = options.repeatType || "EXACT_REPEAT";
  const sorted = [...observations].sort((a, b) =>
    String(a.timestamp || a.completedAt || a.savedAt || "").localeCompare(
      String(b.timestamp || b.completedAt || b.savedAt || "")
    )
  );
  const n = sorted.length;
  const dates = uniqueSortedDates(
    sorted.map((o) => o.timestamp || o.completedAt || o.savedAt || o.date)
  );
  const firstObservedAt = dates[0] ? `${dates[0]}T00:00:00.000Z` : toIso(sorted[0]?.timestamp);
  const lastObservedAt = dates.length
    ? `${dates[dates.length - 1]}T00:00:00.000Z`
    : toIso(sorted[n - 1]?.timestamp);
  const timeWindow = classifyTimeWindow(firstObservedAt, lastObservedAt);

  const presenceFlags = sorted.map((o) => presentForBrand(o, brandId));
  const presenceCount = presenceFlags.filter(Boolean).length;
  const absenceCount = n - presenceCount;
  const presenceFraction = n ? presenceCount / n : null;

  let recurrenceState = "INSUFFICIENT_OBSERVATIONS";
  if (n === 1) recurrenceState = "ONE_OFF";
  else if (n === 2) recurrenceState = "EARLY_REPEATED_EVIDENCE";
  else if (n >= 3) {
    if (presenceCount === n || absenceCount === n) recurrenceState = "RECURRENT";
    else if (presenceCount <= 1) recurrenceState = "INFREQUENT";
    else recurrenceState = "RECURRENT";
  }

  let stabilityState = "INSUFFICIENT_OBSERVATIONS";
  if (n >= MIN_OBSERVATIONS.STABILITY_ELIGIBLE) {
    if (presenceCount === n) stabilityState = "CONSISTENTLY_PRESENT";
    else if (absenceCount === n) stabilityState = "CONSISTENTLY_ABSENT";
    else if (timeWindow === "LONGITUDINAL") {
      const mid = Math.floor(n / 2);
      const early = presenceFlags.slice(0, mid);
      const late = presenceFlags.slice(mid);
      const earlyRate = early.filter(Boolean).length / (early.length || 1);
      const lateRate = late.filter(Boolean).length / (late.length || 1);
      stabilityState = Math.abs(lateRate - earlyRate) >= 0.5 ? "CHANGING" : "MIXED";
    } else {
      stabilityState = "MIXED";
    }
  }

  const domainCounts = new Map();
  for (const obs of sorted) {
    for (const d of citationDomains(obs)) {
      domainCounts.set(d, (domainCounts.get(d) || 0) + 1);
    }
  }
  const sourceRecurrenceSummary = [...domainCounts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 8)
    .map(([domain, count]) => ({
      domain,
      citedIn: count,
      ofObservations: n,
      label: `cited in ${count} of ${n} responses`,
    }));

  return {
    version: STABILITY_AGGREGATION_VERSION,
    grain: options.grain || null,
    promptId: options.promptId || sorted[0]?.promptId || null,
    brandId,
    provider: options.provider || sorted[0]?.provider || null,
    language: options.language || sorted[0]?.language || null,
    geographyKey: options.geographyKey || null,
    repeatType,
    observationCount: n,
    exactRepeatCount: repeatType === "EXACT_REPEAT" ? n : 0,
    variantCount: repeatType === "CONTROLLED_VARIANT" ? n : 0,
    firstObservedAt,
    lastObservedAt,
    runDates: dates,
    timeWindow,
    presenceCount,
    absenceCount,
    presenceFraction,
    presenceLabel: n ? `Present in ${presenceCount} of ${n} observed runs` : "No observations",
    recurrenceState,
    stabilityState,
    sourceRecurrenceSummary,
    NUMERIC_CONFIDENCE: false,
    TREND_SEPARATE: true,
  };
}

/**
 * Cross-provider alignment is a separate contextual layer.
 * Do not pool provider results into one fraction.
 */
export function classifyCrossProviderAlignment(providerSeries = []) {
  const usable = providerSeries.filter(
    (s) => (s.observationCount || 0) >= 1 && s.recurrenceState !== "INSUFFICIENT_OBSERVATIONS"
  );
  if (usable.length < 2) {
    return {
      crossProviderAlignment: "INSUFFICIENT_PROVIDER_COVERAGE",
      providerCoverage: usable.length,
      providersCompared: usable.map((s) => s.provider),
    };
  }
  const presentish = usable.filter(
    (s) =>
      s.stabilityState === "CONSISTENTLY_PRESENT" ||
      (s.observationCount === 1 && s.presenceCount === 1) ||
      (s.observationCount === 2 && s.presenceCount === 2)
  );
  const absentish = usable.filter(
    (s) =>
      s.stabilityState === "CONSISTENTLY_ABSENT" ||
      (s.observationCount === 1 && s.presenceCount === 0) ||
      (s.observationCount === 2 && s.presenceCount === 0)
  );
  let crossProviderAlignment = "PARTIAL_ALIGNMENT";
  if (presentish.length === usable.length) crossProviderAlignment = "ALIGNED_PRESENT";
  else if (absentish.length === usable.length) crossProviderAlignment = "ALIGNED_ABSENT";
  else if (presentish.length && absentish.length && presentish.length + absentish.length === usable.length) {
    crossProviderAlignment = "HIGH_VARIABILITY";
  } else if (usable.some((s) => s.stabilityState === "MIXED" || s.stabilityState === "CHANGING")) {
    crossProviderAlignment = "HIGH_VARIABILITY";
  }
  return {
    crossProviderAlignment,
    providerCoverage: usable.length,
    providersCompared: usable.map((s) => s.provider),
    note: "Provider-specific series remain authoritative. This is context, not a pooled confidence score.",
  };
}

export function attachStabilityMetadataToGap(gap = {}, stability = null) {
  return {
    ...gap,
    stability: stability || null,
  };
}

export function attachStabilityMetadataToTruth(comparison = {}, representationStability = null) {
  return {
    ...comparison,
    representationStability: representationStability || null,
    truthConfidence: null,
  };
}

export function observationRecordTemplate(fields = {}) {
  return {
    runId: fields.runId || null,
    observationId: fields.observationId || null,
    promptId: fields.promptId || null,
    promptVersion: fields.promptVersion || "1",
    promptTextHash: fields.promptTextHash || null,
    repeatType: REPEAT_TYPES.includes(fields.repeatType) ? fields.repeatType : "EXACT_REPEAT",
    provider: fields.provider || null,
    model: fields.model || null,
    geography: fields.geography || null,
    language: fields.language || null,
    timestamp: fields.timestamp || null,
    responseReference: fields.responseReference || fields.responseId || fields.evidenceId || null,
    presenceOutput: fields.presenceOutput ?? null,
    citationOutputs: fields.citationOutputs || null,
    associationOutputs: fields.associationOutputs || null,
    truthClaimSpans: fields.truthClaimSpans || null,
    overwriteHistorical: false,
  };
}
