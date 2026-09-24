/**
 * Restrict route to platform admins (Company Profile → Workspace Access Admin via dealalityUser).
 * Requires memberstackAuth + requireDealalityUser before this middleware.
 *
 * Local QA: approved demo identity may pass via hasLocalDealalityAdminAccess
 * (localhost/dev only — never production email elevation).
 */

import { WORKSPACE_ADMIN } from "../lib/company-workspace-access.js";
import { hasLocalDealalityAdminAccess } from "../lib/dealality/local-dealality-admin-access.js";

export function requireAdminAccess(req, res, next) {
  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
    });
  }

  if (u.isAdmin || u.flags?.isAdmin === true) {
    return next();
  }

  const workspaces = Array.isArray(u.workspaceAccess) ? u.workspaceAccess : [];
  if (workspaces.includes(WORKSPACE_ADMIN)) {
    return next();
  }

  // Local/dev only — never production email elevation
  if (hasLocalDealalityAdminAccess(u, req)) {
    return next();
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_admin",
    message: "Admin access required.",
  });
}
