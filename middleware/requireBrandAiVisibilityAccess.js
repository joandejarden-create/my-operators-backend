/**
 * Gate Brand AI Visibility routes — Brand workspace (or Admin).
 */

import { userCanAccessBrandWorkspace } from "../lib/dealality/user-workspace-gates.js";

export function requireBrandAiVisibilityAccess(req, res, next) {
  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
    });
  }

  if (userCanAccessBrandWorkspace(u)) {
    return next();
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_workspace",
    message: "Brand AI Visibility requires Brand workspace access.",
    workspaceAccess: u.workspaceAccess || [],
  });
}
