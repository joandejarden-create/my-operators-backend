/**
 * Gate My Deals routes by Dealality role (Phase 1: admin + owner only).
 */

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

  if (u.isAdmin || u.isOwner) {
    return next();
  }

  if (u.isBrand || u.isOperator) {
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden_role",
      message: "My Deals is not available for this account type.",
      role: u.role,
    });
  }

  if (u.role === "unknown" || !u.role) {
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden_role",
      message: "Account role is not configured for My Deals. Set Platform Role or User Type on the Users record.",
      role: u.role,
    });
  }

  return next();
}
