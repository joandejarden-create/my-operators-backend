/**
 * Owner-side Operator Deal Request create / by-deals read (Phase 3).
 * Owner + admin only; brand and operator cannot create inbound ODR rows.
 */

export function requireOwnerOdrCreateAccess(req, res, next) {
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

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_role",
    message: "Only owner or admin accounts can create operator deal requests.",
    role: u.role,
  });
}
