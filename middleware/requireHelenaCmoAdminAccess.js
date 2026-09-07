/**
 * Gate Admin Resources → Helena CMO Founder Console.
 *
 * Canonical policy: platform admin OR founder/demo constellation
 * (same intent as AI Demand Reviews founder/admin access).
 * Does not weaken requireAdminAccess elsewhere.
 */

import { canUseDemoFounderNavOverrides } from "../lib/dealality/demo-stakeholder-workspace.js";
import { isInternalRunbookAdmin } from "../lib/dealality/internal-runbook-admin.js";
import { WORKSPACE_ADMIN } from "../lib/company-workspace-access.js";

export function canAccessHelenaCmoAdmin(user, req = {}) {
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

export function requireHelenaCmoAdminAccess(req, res, next) {
  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
    });
  }
  if (!canAccessHelenaCmoAdmin(u, req)) {
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden_helena_cmo_admin",
      message: "Helena CMO Founder Console requires Dealality founder/admin access.",
    });
  }
  return next();
}
