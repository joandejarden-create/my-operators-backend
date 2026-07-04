/**
 * Gate My Operator Deals / operator-deal-requests routes by operator workspace access.
 * Demo workspace alone does not pass — requires Operator (or Admin / explicit demo-safe bypass).
 */

import { isOperatorDealsDemoBypassEnabled } from "../lib/dealality/operator-deals-demo-bypass.js";
import { userCanAccessOperatorWorkspace } from "../lib/dealality/user-workspace-gates.js";

export function requireOperatorDealsAccess(req, res, next) {
  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
    });
  }

  if (isOperatorDealsDemoBypassEnabled()) {
    return next();
  }

  if (userCanAccessOperatorWorkspace(u)) {
    return next();
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_workspace",
    message: "My Operator Deals requires Operator workspace access.",
    role: u.role,
    legacyRole: u.legacyRole || u.role,
    workspaceAccess: u.workspaceAccess || [],
  });
}
