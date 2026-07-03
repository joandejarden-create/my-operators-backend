/**
 * Gate owner-scoped BDR reads/writes — runs auth only when query/body requires it.
 */

import { memberstackAuth } from "./memberstackAuth.js";
import { requireDealalityUser } from "./requireDealalityUser.js";
import {
  requireOwnerBdrActivityAccess,
  requireOwnerBdrDealMetaAccess,
} from "./requireOwnerBdrActivityAccess.js";

function runStack(req, res, next, stack) {
  let i = 0;
  const step = () => {
    if (i >= stack.length) return next();
    const fn = stack[i++];
    fn(req, res, step);
  };
  step();
}

const ownerUserAuth = [memberstackAuth, requireDealalityUser];

export function gateOwnerBdrActivity(req, res, next) {
  const q = req.query || {};
  const hasDealIds = Boolean(q.dealIds && String(q.dealIds).trim());
  const dealId = q.dealId != null ? String(q.dealId).trim() : "";
  const hasDealId = dealId.startsWith("rec");
  if (!hasDealIds && !hasDealId) {
    return next();
  }
  return runStack(req, res, next, [...ownerUserAuth, requireOwnerBdrActivityAccess]);
}

export function gateOwnerBdrDealMeta(req, res, next) {
  const raw = req.query?.ids ?? req.query?.dealIds ?? "";
  const hasIds = String(raw)
    .split(",")
    .map((s) => s.trim())
    .some((id) => id.startsWith("rec"));
  if (!hasIds) {
    return next();
  }
  return runStack(req, res, next, [...ownerUserAuth, requireOwnerBdrDealMetaAccess]);
}

export function gateOwnerBdrDealIdsQuery(req, res, next) {
  const dealIdsParam = req.query?.dealIds;
  if (!dealIdsParam || !String(dealIdsParam).trim()) {
    return next();
  }
  return runStack(req, res, next, ownerUserAuth);
}

export function gateOwnerBdrListAll(req, res, next) {
  const allParam = req.query?.all;
  if (allParam !== "1" && allParam !== "true") {
    return next();
  }
  return runStack(req, res, next, [...ownerUserAuth, requireAdminForListAll]);
}

function requireAdminForListAll(req, res, next) {
  const u = req.dealalityUser;
  if (!u?.isAdmin) {
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden",
      message: "Admin access required for listAll.",
    });
  }
  return next();
}
