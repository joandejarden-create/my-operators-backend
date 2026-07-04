/**
 * GET /api/support/owner-pilot-provisioning-runbook
 * Admin-only — sensitive operational runbook content (not served from public/).
 */
import { getOwnerPilotProvisioningRunbook } from "../lib/support/owner-pilot-provisioning-runbook.js";

export function getOwnerPilotProvisioningRunbookHandler(_req, res) {
  const runbook = getOwnerPilotProvisioningRunbook();
  return res.status(200).json({
    ok: true,
    success: true,
    ...runbook,
  });
}
