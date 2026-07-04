/**
 * Gate My Deals routes by owner workspace access (not legacy role string alone).
 * Demo workspace alone does not pass — requires Owner (or Admin).
 */

import { userCanAccessOwnerWorkspace } from "../lib/dealality/user-workspace-gates.js";

export function requireMyDealsAccess(req, res, next) {
  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
    });
  }

  if (userCanAccessOwnerWorkspace(u)) {
    return next();
  }

  if (u.isBrand || u.isOperator) {
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden_role",
      message: "My Deals is not available for this account type.",
      role: u.role,
      legacyRole: u.legacyRole || u.role,
      workspaceAccess: u.workspaceAccess || [],
    });
  }

  if (u.role === "unknown" || !u.role) {
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden_role",
      message:
        "Account role is not configured for My Deals. Set Platform Role or User Type on the Users record, or link a Company Profile with Owner workspace access.",
      role: u.role,
    });
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_workspace",
    message: "My Deals requires Owner workspace access.",
    role: u.role,
    workspaceAccess: u.workspaceAccess || [],
  });
}
