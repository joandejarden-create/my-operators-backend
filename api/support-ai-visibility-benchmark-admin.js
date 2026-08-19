/**
 * GET /api/support/ai-visibility-benchmark-admin
 * Admin-only — Brand AI prompt themes + benchmark cohort peers.
 */
import { getAiVisibilityBenchmarkAdminRunbook } from "../lib/support/ai-visibility-benchmark-admin-runbook.js";

export function getAiVisibilityBenchmarkAdminHandler(_req, res) {
  const runbook = getAiVisibilityBenchmarkAdminRunbook();
  return res.status(200).json({
    ok: true,
    success: true,
    ...runbook,
  });
}
