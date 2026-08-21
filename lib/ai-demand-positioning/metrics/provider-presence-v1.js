/**
 * Customer-facing provider presence — ADP_MEASUREMENT_CONTRACT_V1 grain.
 *
 * Canonical rule (derived from COMPARABLE_OBSERVATION_RULE + observation grain):
 *   Provider Presence Rate =
 *     subject-present comparable observations for provider
 *     / comparable observations for provider
 *
 * Failed / missing / dryRun / unparsed-empty provider calls are OMITTED.
 * Missing provider observation ≠ measured zero.
 *
 * Counts (do not overload):
 *   scenariosScheduled — planned scenario universe asked on this provider
 *   observationsCaptured / comparable — successful responses eligible for the metric
 *   mentioned — subject present among comparable
 */

import { roundAdpPercent } from "../format-percent.js";
import { isComparableObservation } from "./grain-governance.js";

export const ADP_PROVIDER_DENOMINATOR_GRAIN = "comparable_observations";
export const ADP_PROVIDER_PRESENCE_FORMULA_V1 =
  "subject_present_comparable / comparable_observations_for_provider";

/**
 * @param {Array<object>} observations — typically period.observations (may include failed)
 * @returns {{ providers: object[], providerCitations: Record<string, number|null>, grain: string }}
 */
export function buildProviderPresenceRows(observations) {
  const providerStats = {};
  for (const obs of observations || []) {
    const provider = obs?.provider;
    if (!provider) continue;
    if (!providerStats[provider]) {
      providerStats[provider] = {
        scenariosScheduled: 0,
        comparable: 0,
        mentioned: 0,
        withCitations: 0,
      };
    }
    providerStats[provider].scenariosScheduled += 1;
    if (!isComparableObservation(obs)) continue;
    providerStats[provider].comparable += 1;
    if (obs.mentioned) providerStats[provider].mentioned += 1;
    if (obs.sourcesCited?.length) providerStats[provider].withCitations += 1;
  }

  const providers = Object.entries(providerStats)
    .map(([provider, s]) => {
      const comparable = s.comparable;
      const scheduled = s.scenariosScheduled;
      const incomplete = comparable < scheduled;
      const presence =
        comparable > 0 ? roundAdpPercent((s.mentioned / comparable) * 100) : null;
      return {
        provider,
        // Distinct counts — never overload one field for scheduled vs comparable.
        scenariosScheduled: scheduled,
        scheduled,
        observationsCaptured: comparable,
        comparable,
        captured: comparable,
        mentioned: s.mentioned,
        excludedFromMetric: Math.max(0, scheduled - comparable),
        incompleteCoverage: incomplete,
        coverageNote: incomplete ? `${comparable} of ${scheduled} observations captured` : null,
        denominatorGrain: ADP_PROVIDER_DENOMINATOR_GRAIN,
        formula: ADP_PROVIDER_PRESENCE_FORMULA_V1,
        // Metric denominator alias for older UI that reads `total` as the rate denom.
        // total === comparable (never scheduled when coverage is incomplete).
        total: comparable,
        presence,
        presenceUnavailable: comparable === 0,
      };
    })
    .sort((a, b) => {
      const ap = a.presence == null ? -1 : a.presence;
      const bp = b.presence == null ? -1 : b.presence;
      return bp - ap;
    });

  const providerCitations = {};
  for (const [provider, s] of Object.entries(providerStats)) {
    providerCitations[provider] =
      s.comparable > 0 ? roundAdpPercent((s.withCitations / s.comparable) * 100) : null;
  }

  return {
    providers,
    providerCitations,
    grain: ADP_PROVIDER_DENOMINATOR_GRAIN,
    formula: ADP_PROVIDER_PRESENCE_FORMULA_V1,
  };
}
