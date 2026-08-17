/**
 * Gate Validation Scorecard + AI Intelligence validation APIs.
 * Admin OR internal runbook OR founder/demo constellation — not normal clients.
 */

import { canUseDemoFounderNavOverrides } from "../lib/dealality/demo-stakeholder-workspace.js";
import { isInternalRunbookAdmin } from "../lib/dealality/internal-runbook-admin.js";
import { WORKSPACE_ADMIN } from "../lib/company-workspace-access.js";

export function canAccessAiIntelligenceValidation(user, req = {}) {
  if (!user) return false;
  if (user.isAdmin || user.flags?.isAdmin === true) return true;
  const workspaces = Array.isArray(user.workspaceAccess) ? user.workspaceAccess : [];
  if (workspaces.includes(WORKSPACE_ADMIN)) return true;
  if (
    isInternalRunbookAdmin({
      email: user.email || req.memberstackAuth?.email,
      dealality: { isAdmin: user.isAdmin, flags: user.flags },
      companyName: user.companyName,
    })
  ) {
    return true;
  }
  if (canUseDemoFounderNavOverrides(user)) return true;
  return false;
}

export function requireAiIntelligenceValidationAccess(req, res, next) {
  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
    });
  }
  if (!canAccessAiIntelligenceValidation(u, req)) {
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden_validation_scorecard",
      message: "AI Intelligence Validation Scorecard access required.",
    });
  }
  return next();
}
