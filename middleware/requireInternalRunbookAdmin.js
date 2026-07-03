/**
 * Owner Pilot Runbook — internal platform admin only (not customer Workspace Access Admin).
 */
import { isInternalRunbookAdmin } from "../lib/dealality/internal-runbook-admin.js";

export function requireInternalRunbookAdmin(req, res, next) {
  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
    });
  }

  if (
    isInternalRunbookAdmin({
      email: u.email || req.memberstackAuth?.email,
      dealality: {
        isAdmin: u.isAdmin,
        flags: u.flags,
      },
      companyName: u.companyName,
    })
  ) {
    return next();
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_admin",
    message: "Internal runbook access required.",
  });
}
