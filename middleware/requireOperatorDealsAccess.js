/**
 * Gate My Operator Deals / operator-deal-requests routes (Phase 1: operator + admin only).
 */

import { isOperatorDealsDemoBypassEnabled } from "../lib/dealality/operator-deals-demo-bypass.js";

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

  if (u.isAdmin || u.isOperator) {
    return next();
  }

  return res.status(403).json({
    ok: false,
    success: false,
    error: "forbidden_role",
    message: "My Operator Deals is not available for this account type.",
    role: u.role,
  });
}
