/**
 * Native discovery completeness gate — decide whether Parallel escalation is warranted.
 * Blind-safe: uses candidate counts / vertical coverage only (no Webhound).
 */

export const NATIVE_DISCOVERY_DECISION = Object.freeze({
  NATIVE_SUFFICIENT: "NATIVE_SUFFICIENT",
  ESCALATE_PARALLEL: "ESCALATE_PARALLEL",
  INSUFFICIENT_NO_ESCALATE: "INSUFFICIENT_NO_ESCALATE",
});

const MIN_CANDIDATES_SUFFICIENT = 8;
const MIN_CANDIDATES_HARD_FLOOR = 3;

/**
 * @param {{ candidates?: object[], contract?: object }} opts
 */
export function evaluateNativeDiscoveryCompleteness({
  candidates = [],
  contract = null,
} = {}) {
  const list = Array.isArray(candidates) ? candidates : [];
  const verticalsSeen = new Set();
  for (const c of list) {
    const v = String(c.vertical || c.segment || "").trim().toLowerCase();
    if (v) verticalsSeen.add(v);
  }

  const expectedVerticals = Array.isArray(contract?.verticals)
    ? contract.verticals.map((v) => String(v.id || v.name || v).toLowerCase())
    : [];
  const missingVerticals = expectedVerticals.filter((v) => !verticalsSeen.has(v));

  const escalateGaps = [];
  if (list.length < MIN_CANDIDATES_SUFFICIENT) {
    escalateGaps.push({
      code: "LOW_CANDIDATE_COUNT",
      detail: `native_candidates=${list.length} threshold=${MIN_CANDIDATES_SUFFICIENT}`,
    });
  }
  if (missingVerticals.length) {
    escalateGaps.push({
      code: "MISSING_VERTICAL_COVERAGE",
      detail: missingVerticals.slice(0, 8).join(","),
    });
  }

  let decision = NATIVE_DISCOVERY_DECISION.NATIVE_SUFFICIENT;
  if (list.length < MIN_CANDIDATES_HARD_FLOOR) {
    decision = escalateGaps.length
      ? NATIVE_DISCOVERY_DECISION.ESCALATE_PARALLEL
      : NATIVE_DISCOVERY_DECISION.INSUFFICIENT_NO_ESCALATE;
  } else if (escalateGaps.length) {
    decision = NATIVE_DISCOVERY_DECISION.ESCALATE_PARALLEL;
  }

  return {
    decision,
    candidateCount: list.length,
    verticalsSeen: [...verticalsSeen],
    missingVerticals,
    escalateGaps,
    thresholds: {
      minSufficient: MIN_CANDIDATES_SUFFICIENT,
      hardFloor: MIN_CANDIDATES_HARD_FLOOR,
    },
  };
}

/**
 * Convert gate escalateGaps into Parallel task gap descriptors.
 */
export function buildParallelEscalationGaps(gate, existingCandidates = []) {
  const gaps = Array.isArray(gate?.escalateGaps) ? gate.escalateGaps : [];
  if (!gaps.length) return [];
  return gaps.map((g) => ({
    code: g.code,
    detail: g.detail,
    existingCount: Array.isArray(existingCandidates) ? existingCandidates.length : 0,
  }));
}
