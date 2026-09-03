/**
 * Visual Integrity — redundant lower status/callout containers.
 *
 * Defect: REDUNDANT_STATUS_CONTAINER
 * Rule: do not render a secondary status box/banner when the data state
 * is already communicated elsewhere in the same section (e.g. KPI deltas).
 */

export const VISUAL_DEFECT_REDUNDANT_STATUS_CONTAINER = "REDUNDANT_STATUS_CONTAINER";

/** Known redundant Trends patterns (lower box under chart). */
export const TRENDS_REDUNDANT_STATUS_PATTERNS = Object.freeze([
  /Awaiting next comparable monitoring period/i,
]);

/**
 * @param {{
 *   comparablePeriodCount: number,
 *   lowerStatusVisible: boolean,
 *   lowerStatusText?: string,
 *   kpiDeltaTexts?: string[],
 * }} input
 */
export function auditTrendsRedundantStatusContainer(input) {
  const defects = [];
  const count = Number(input.comparablePeriodCount) || 0;
  const text = String(input.lowerStatusText || "").trim();
  const lowerVisible = Boolean(input.lowerStatusVisible) && Boolean(text);

  if (count >= 1 && lowerVisible) {
    const matchesKnown = TRENDS_REDUNDANT_STATUS_PATTERNS.some((re) => re.test(text));
    if (matchesKnown) {
      defects.push({
        code: VISUAL_DEFECT_REDUNDANT_STATUS_CONTAINER,
        detail:
          "Trends lower status/callout duplicates one-period state already shown in KPI cards / chart; remove container",
        comparablePeriodCount: count,
        lowerStatusText: text.slice(0, 160),
      });
    }
  }

  // When 2+ periods exist, any "awaiting next comparable" copy (lower OR KPI) is stale.
  if (count >= 2) {
    const kpi = input.kpiDeltaTexts || [];
    const awaitingInKpi = kpi.some((t) => /awaiting next comparable/i.test(String(t || "")));
    if (awaitingInKpi || (lowerVisible && /awaiting next comparable/i.test(text))) {
      defects.push({
        code: VISUAL_DEFECT_REDUNDANT_STATUS_CONTAINER,
        detail: "Awaiting-next-period copy must not render when comparablePeriodCount >= 2",
        comparablePeriodCount: count,
      });
    }
  }

  return {
    status: defects.length ? "FAIL" : "PASS",
    defectClass: VISUAL_DEFECT_REDUNDANT_STATUS_CONTAINER,
    defects,
  };
}
