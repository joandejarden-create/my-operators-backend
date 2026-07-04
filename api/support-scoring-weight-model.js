/**
 * GET /api/support/scoring-weight-model
 * Admin-only — scoring weight source of truth (task 2.02).
 */
import { getScoringWeightModelRunbook } from "../lib/support/scoring-weight-model-runbook.js";

export function getScoringWeightModelHandler(_req, res) {
  const runbook = getScoringWeightModelRunbook();
  return res.status(200).json({
    ok: true,
    success: true,
    ...runbook,
  });
}
