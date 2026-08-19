/**
 * Gate Operator AI Intelligence routes — Operator workspace (or Admin).
 * Owners are not entitled by default.
 */

import { userCanAccessOperatorWorkspace } from "../lib/dealality/user-workspace-gates.js";

export function requireOperatorAiVisibilityAccess(req, res, next) {
  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
    });
  }

  if (userCanAccessOperatorWorkspace(u)) {
    return next();
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_workspace",
    message: "Operator AI Intelligence requires Operator workspace access.",
    workspaceAccess: u.workspaceAccess || [],
  });
}
