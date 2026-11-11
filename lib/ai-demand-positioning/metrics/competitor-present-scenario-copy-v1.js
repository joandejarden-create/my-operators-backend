/**
 * Canonical customer-facing copy for Scenarios With Competitive Displacement.
 * Grain: RESPONSE_LEVEL_ABSENCE — must never be described as full-scenario absence alone.
 *
 * Payload key remains `competitorPresentScenarios` for backward compatibility.
 * Display title is the customer-facing standard.
 */

export const COMPETITOR_PRESENT_SCENARIO_COPY_V1 =
  "COMPETITOR_PRESENT_SCENARIO_COPY_V1";

export const COMPETITOR_PRESENT_COUNTING_GRAIN = "RESPONSE_LEVEL_ABSENCE";
export const COMPETITOR_PRESENT_FULLY_ABSENT_GRAIN = "SCENARIO_LEVEL_ABSENCE";

/** @deprecated Prefer COMPETITOR_PRESENT_CARD.title — kept for grep/tests during rename */
export const LEGACY_COMPETITOR_PRESENT_TITLE = "Competitor-Present Scenarios";

export const COMPETITOR_PRESENT_CARD = Object.freeze({
  title: "Scenarios With Competitive Displacement",
  /** Short card subtitle — must match RESPONSE_LEVEL_ABSENCE */
  subtitle:
    "Traveler scenarios where at least one AI provider named a competitor without naming this hotel",
  tip: Object.freeze({
    definition:
      "Traveler scenarios where at least one AI provider named a competitor without naming this hotel — even if another AI model named your hotel for the same scenario.",
    formula:
      "Count of unique traveler scenarios where ≥1 provider response omits your hotel and names a comparable hotel in that same response (RESPONSE_LEVEL_ABSENCE).",
    grain:
      "Each traveler scenario counts once. This is not the same as scenarios where your hotel was missing from every provider. Full-scenario absence is a separate count (SCENARIO_LEVEL_ABSENCE) and cannot be larger than total subject-absent scenarios.",
    whyTrack:
      "These are scenarios where at least one AI answer sent attention to another hotel instead of yours. Useful for competitive displacement review without implying you were absent everywhere.",
    important:
      "This is not lost bookings and not a verified competitor recommendation or preference score. It counts competitor appearance while your hotel is omitted in at least one answer. A hotel AI named is not automatically your true competitor. Response-level competitive displacement can exceed full-scenario absence when providers disagree.",
  }),
  insufficientSubtitle:
    "More comparable competitive observations are needed to calculate this metric.",
  insufficientTipImportant:
    "This metric requires enough comparable competitive observations. It is not shown as 0 scenarios when data is insufficient.",
});

/**
 * Assert payload grain metadata is present for staged/customer consumers.
 */
export function enrichCompetitorPresentPayload(gapsPayload) {
  if (!gapsPayload || typeof gapsPayload !== "object") return gapsPayload;
  return {
    ...gapsPayload,
    countingGrain:
      gapsPayload.countingGrain || COMPETITOR_PRESENT_COUNTING_GRAIN,
    fullyAbsentCountingGrain:
      gapsPayload.fullyAbsentCountingGrain ||
      COMPETITOR_PRESENT_FULLY_ABSENT_GRAIN,
    displayTitle: COMPETITOR_PRESENT_CARD.title,
    displaySubtitle: COMPETITOR_PRESENT_CARD.subtitle,
    metricId: "scenarios_with_competitive_displacement",
    labelNote:
      gapsPayload.labelNote ||
      "scenarioCount uses RESPONSE_LEVEL_ABSENCE (may exceed full scenario-absence). fullyAbsentScenarioCount uses SCENARIO_LEVEL_ABSENCE.",
  };
}
