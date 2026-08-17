/**
 * Rank-change actionability classification (deterministic).
 */

export const RANK_CHANGE_SENSITIVITY = Object.freeze({
  RANK_SENSITIVE: "Rank-sensitive",
  CONFIDENCE_SENSITIVE: "Confidence-sensitive",
  ELIGIBILITY_SENSITIVE: "Eligibility-sensitive",
  FINAL_SELECTION_SENSITIVE: "Final-selection-sensitive",
  INFORMATIONAL_ONLY: "Informational only",
});

/**
 * Classify a ranking-change validation item for advisor/owner wording.
 */
export function classifyRankChangeSensitivity(item = {}) {
  const impact = String(item.impact || "");
  const criticality = String(item.criticality || "");
  const phase = String(item.phase || "");

  let sensitivity = RANK_CHANGE_SENSITIVITY.INFORMATIONAL_ONLY;
  if (impact === "eligibility") sensitivity = RANK_CHANGE_SENSITIVITY.ELIGIBILITY_SENSITIVE;
  else if (impact === "rank") sensitivity = RANK_CHANGE_SENSITIVITY.RANK_SENSITIVE;
  else if (impact === "confidence") sensitivity = RANK_CHANGE_SENSITIVITY.CONFIDENCE_SENSITIVE;
  else if (impact === "alignment" && /final|proposal/i.test(phase + criticality)) {
    sensitivity = RANK_CHANGE_SENSITIVITY.FINAL_SELECTION_SENSITIVE;
  } else if (impact === "alignment") sensitivity = RANK_CHANGE_SENSITIVITY.RANK_SENSITIVE;

  if (/useful_not_critical/i.test(criticality) && sensitivity === RANK_CHANGE_SENSITIVITY.RANK_SENSITIVE) {
    sensitivity = RANK_CHANGE_SENSITIVITY.INFORMATIONAL_ONLY;
  }

  const validateNext = String(item.question || "")
    .replace(/^Confirm /i, "Confirm ")
    .replace(/^Verify /i, "Verify ")
    .replace(/^Obtain /i, "Request ");

  return {
    ...item,
    sensitivity,
    material: sensitivity !== RANK_CHANGE_SENSITIVITY.INFORMATIONAL_ONLY,
    actionable: Boolean(item.question),
    ownerAdvisorWording: validateNext,
    tooSpeculative: /speculate|might|could invent/i.test(String(item.question || "")),
  };
}

export function classifyRankChangeList(items = []) {
  return (items || []).map(classifyRankChangeSensitivity);
}
