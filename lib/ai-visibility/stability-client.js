/**
 * Client-safe repeated-testing copy. No numeric confidence.
 */

import { STABILITY_METHODOLOGY_COPY } from "./stability-aggregation.js";

export const CLIENT_STABILITY_COPY = Object.freeze({
  methodology: STABILITY_METHODOLOGY_COPY,
  repeated: "Repeated",
  mixed: "Mixed",
  early: "Early signal",
  acrossProviders: "Across providers",
  consistentSample: "Observed consistently in this sample",
  variedProviders: "Results varied across providers",
  earlyOne: "Early signal — 1 observation",
});

export function formatPresenceFraction(presenceCount, observationCount) {
  const n = Number(observationCount) || 0;
  const p = Number(presenceCount) || 0;
  if (!n) return "No observations";
  return `Present in ${p} of ${n} observed runs`;
}

export function formatExecutiveEvidenceLanguage(summary = {}) {
  const n = Number(summary.observationCount) || 0;
  if (n <= 0) return null;
  if (n === 1) return CLIENT_STABILITY_COPY.earlyOne;
  if (summary.stabilityState === "MIXED" || summary.stabilityState === "CHANGING") {
    return `Mixed · ${summary.presenceCount} of ${n} observations`;
  }
  if (
    summary.stabilityState === "CONSISTENTLY_PRESENT" ||
    summary.stabilityState === "CONSISTENTLY_ABSENT" ||
    summary.recurrenceState === "RECURRENT"
  ) {
    return `Repeated · ${summary.presenceCount} of ${n} observed runs`;
  }
  if (summary.recurrenceState === "EARLY_REPEATED_EVIDENCE") {
    return `Repeated · ${summary.presenceCount} of ${n} observed runs`;
  }
  return formatPresenceFraction(summary.presenceCount, n);
}

export function formatCrossProviderLanguage(alignment = {}) {
  const d = Number(alignment.providerCoverage) || 0;
  if (d < 2) return null;
  if (alignment.crossProviderAlignment === "ALIGNED_PRESENT") {
    return `Across providers · ${d} of ${d} providers`;
  }
  if (alignment.crossProviderAlignment === "ALIGNED_ABSENT") {
    return `Across providers · 0 of ${d} providers present`;
  }
  if (
    alignment.crossProviderAlignment === "PARTIAL_ALIGNMENT" ||
    alignment.crossProviderAlignment === "HIGH_VARIABILITY"
  ) {
    return CLIENT_STABILITY_COPY.variedProviders;
  }
  return null;
}

/**
 * Evidence-support metadata only. Does not rank or select findings.
 */
export function classifyExecutiveEvidenceSupportLabel(input = {}) {
  const n = Number(input.observationCount) || 0;
  const providers = Number(input.providerCount) || 0;
  const alignment = input.crossProviderAlignment || null;
  const stability = input.stabilityState || null;
  const recurrence = input.recurrenceState || null;
  if (n <= 0) return "INSUFFICIENT";
  if (alignment === "HIGH_VARIABILITY" || alignment === "PARTIAL_ALIGNMENT") {
    return "PROVIDER_VARIABLE";
  }
  if (stability === "MIXED" || stability === "CHANGING") return "MIXED";
  if (n === 1) return "EARLY_SIGNAL";
  if (recurrence === "RECURRENT") return "RECURRENT";
  if (n >= 2 || providers >= 2) return "REPEATED";
  return "INSUFFICIENT";
}

export function formatFindingSupportDescriptor(input = {}) {
  const n = Number(input.observationCount) || 0;
  const providers = Number(input.providerCount) || 0;
  const span = input.dateSpanLabel || null;
  if (!n) return null;
  if (providers >= 2 && n >= 2) {
    return `Repeated across ${providers} providers / ${n} observations`;
  }
  if (n === 1) return CLIENT_STABILITY_COPY.earlyOne;
  if (input.stabilityState === "MIXED") return `Mixed across providers`;
  const bits = [formatPresenceFraction(input.presenceCount, n)];
  if (span) bits.push(span);
  return bits.join(" · ");
}

export function formatDetailStability(summary = {}, alignment = null) {
  if (!summary || !summary.observationCount) {
    return {
      observations: "0",
      recurrence: "INSUFFICIENT_OBSERVATIONS",
      providerCoverage: alignment?.providerCoverage || 0,
      firstObserved: null,
      lastObserved: null,
    };
  }
  return {
    observations: String(summary.observationCount),
    recurrence: summary.recurrenceState,
    stability: summary.stabilityState,
    providerCoverage: alignment?.providerCoverage || (summary.provider ? 1 : 0),
    firstObserved: summary.firstObservedAt || null,
    lastObserved: summary.lastObservedAt || null,
    presenceLabel: summary.presenceLabel || null,
  };
}

export function enrichRowWithObservationSummary(row = {}, series = null) {
  if (series) {
    return {
      ...row,
      observationSummary: formatDetailStability(series),
    };
  }
  const present = row.presenceObserved === true || row.brandStatus === "Present";
  const n = 1;
  return {
    ...row,
    observationSummary: {
      observations: "1",
      recurrence: "ONE_OFF",
      stability: "INSUFFICIENT_OBSERVATIONS",
      providerCoverage: 1,
      firstObserved: row.batchDate || null,
      lastObserved: row.batchDate || null,
      presenceLabel: `Present in ${present ? 1 : 0} of ${n} observed runs`,
    },
  };
}
