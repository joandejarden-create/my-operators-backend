import { normalizeKey } from "./match-current-census.js";

export function reconciliationConfidenceToScore(confidence) {
  const c = normalizeKey(confidence);
  if (c === "high") return 80;
  if (c === "medium") return 65;
  if (c === "low") return 50;
  return 45;
}
