/**
 * Selective numeric anchors — ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS
 * Prefer 1–3; hard max 4 with justification. No KPI enumeration.
 */

export const ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS = "ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS";
export const ADP_EXECUTIVE_NUMERIC_REFERENCE_PARITY = "ADP_EXECUTIVE_NUMERIC_REFERENCE_PARITY";
export const ADP_EXECUTIVE_NO_KPI_ENUMERATION = "ADP_EXECUTIVE_NO_KPI_ENUMERATION";

function fmtPct(n) {
  if (typeof n !== "number") return null;
  return Number.isInteger(n) ? `${n}%` : `${Math.round(n * 10) / 10}%`;
}

function fmtIndex(n) {
  if (typeof n !== "number") return null;
  return String(Math.round(n));
}

/**
 * @param {object} input
 * @param {object} selection — from selectPrimaryInsightV3
 */
export function selectNumericAnchorsV3(input, selection) {
  const anchors = [];
  const primary = selection?.primary;
  if (!primary) return { anchors: [], rule: ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS };

  const push = (a) => {
    if (!a?.displayValue || anchors.length >= 4) return;
    if (anchors.some((x) => x.metricId === a.metricId || x.displayValue === a.displayValue)) return;
    anchors.push({
      metricId: a.metricId,
      displayValue: a.displayValue,
      section: a.section || "KEY_INSIGHT",
      reason: a.reason,
      paritySource: a.paritySource || a.metricId,
      removingWeakensInsight: true,
      anchorType: a.anchorType || "severity_indicator",
    });
  };

  for (const m of primary.supportingMetrics || []) {
    if (m.metricId?.includes("considerationRate") && typeof m.value === "number") {
      push({
        metricId: m.metricId,
        displayValue: fmtPct(m.value),
        reason: "Establishes severity of consideration / entry",
        anchorType: "severity_indicator",
      });
    }
    if (m.metricId?.includes("scenarioPresence") && typeof m.value === "number") {
      // Only keep SP if it forms a contradiction pair with consideration
      const c = input.aiConsideration;
      if (typeof c === "number" && m.value - c >= 20) {
        push({
          metricId: m.metricId,
          displayValue: fmtPct(m.value),
          reason: "Contradiction pair with consideration",
          anchorType: "contradiction_pair",
        });
      }
    }
    if (m.metricId?.includes(".index") && typeof m.value === "number") {
      push({
        metricId: m.metricId,
        displayValue: fmtIndex(m.value),
        reason: "Peer-relative Presence Index contrast",
        anchorType: "peer_relative_index",
      });
    }
    if (m.metricId?.includes("displacementCount") && typeof m.value === "number") {
      push({
        metricId: m.metricId,
        displayValue: String(m.value),
        reason: "Displacement magnitude",
        anchorType: "displacement_magnitude",
      });
    }
    if (m.metricId?.includes("recognitionRate") && typeof m.value === "number") {
      push({
        metricId: m.metricId,
        displayValue: fmtPct(m.value),
        reason: "Reality Gap recognition rate",
        anchorType: "reality_gap_rate",
      });
    }
    if (
      m.metricId?.includes("topThree") &&
      typeof m.value === "number" &&
      input.rankMetrics?.customerNarrativeEligible === true
    ) {
      push({
        metricId: m.metricId,
        displayValue: fmtPct(m.value),
        reason: "Rank-when-present contrast",
        anchorType: "contradiction_pair",
      });
    }
  }

  // Prefer KEY_INSIGHT placement; trim to 3 unless justified
  let justificationForFourth = null;
  if (anchors.length > 3) {
    justificationForFourth = "Fourth anchor retained for contradiction pair completeness";
    anchors.splice(4);
  }
  if (anchors.length > 3 && !justificationForFourth) {
    anchors.length = 3;
  }

  return {
    anchors: anchors.slice(0, justificationForFourth ? 4 : 3),
    preferredBand: { min: 1, max: 3 },
    hardMax: 4,
    justificationForFourth,
    rule: ADP_EXECUTIVE_SELECTIVE_NUMERIC_ANCHORS,
    noKpiEnumeration: true,
  };
}
