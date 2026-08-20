/**
 * ADP customer display contract — UI visibility flags (not payload removal).
 */

export const AI_DEMAND_CAPTURE_CUSTOMER_RENDER = 0;
export const EXECUTIVE_METRICS_CARD_COUNT = 5;
export const EXECUTIVE_METRICS_ALWAYS_VISIBLE = true;

/** Re-export governed rank threshold for UI state parity with measurement layer. */
export { MIN_RANK_SAMPLE as EXECUTIVE_METRICS_MIN_RANK_SAMPLE } from "../metrics/position-metrics.js";

/**
 * Uncertified CORE benchmark — customer terminology V1.
 * Internal status enum BENCHMARK_DEVELOPING may remain; customer label must not say "Developing".
 */
export const BENCHMARK_UNCERTIFIED_LABEL = "Benchmark not yet certified";
export const BENCHMARK_UNCERTIFIED_LINE1 = "Benchmark not";
export const BENCHMARK_UNCERTIFIED_LINE2 = "yet certified";
export const BENCHMARK_UNCERTIFIED_SHORT_EXPLANATION =
  "Individual comparable-hotel presence can be measured before the overall benchmark is certified.";
export const BENCHMARK_UNCERTIFIED_SHORT_EXPLANATION_EXTENDED =
  "Individual CORE hotel presence can be measured before the overall benchmark is certified. The benchmark is published only after required coverage and stability checks pass.";
export const CORE_BENCHMARK_TOOLTIP_BODY =
  "The average AI presence rate of governed comparable hotels relevant to this demand territory. Only CORE comparable hotels are included in the benchmark.\n\n" +
  "A CORE hotel with 0% observed AI presence remains a valid measured result. Missing or unavailable provider/observation scope is not converted to zero.\n\n" +
  "Why \"Benchmark not yet certified\"?\n\n" +
  "You may still see AI Presence values for individual CORE hotels in the Competitive Set. Those measurements can be valid before the combined benchmark is certified. Dealality only publishes the benchmark once the comparable set and the subject-vs-benchmark comparison pass the required coverage and stability checks.";
