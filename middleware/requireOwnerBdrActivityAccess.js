/**
 * Owner-scoped BDR activity/deal-meta query params.
 * Brand-only legacy queries (brand= without dealId/dealIds) pass through when authenticated as brand.
 */

import {
  assertOwnerDealAccess,
  filterDealIdsWithAirtable,
  normalizeDealIds,
  ownerMustEnforceDealAccess,
} from "../lib/dealality/owner-deal-id-access.js";

function readBearer(req) {
  const raw = req.headers.authorization || req.headers.Authorization;
  if (!raw || typeof raw !== "string") return null;
  const m = raw.match(/^Bearer\s+(\S+)$/i);
  return m ? m[1] : null;
}

export async function requireOwnerBdrActivityAccess(req, res, next) {
  const q = req.query || {};
  const dealIdsRaw = q.dealIds ?? q.deal_ids;
  const dealId = q.dealId != null ? String(q.dealId).trim() : "";
  const hasDealIds = Boolean(dealIdsRaw && String(dealIdsRaw).trim());
  const hasDealId = dealId.startsWith("rec");
  const token = readBearer(req);

  if ((hasDealIds || hasDealId) && !token) {
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message: "Send Authorization: Bearer <Memberstack member JWT>.",
    });
  }

  if (!token) {
    return next();
  }

  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({ ok: false, success: false, error: "User context missing." });
  }

  if (!ownerMustEnforceDealAccess(u)) {
    return next();
  }

  if (hasDealIds) {
    const filtered = await filterDealIdsWithAirtable(u, dealIdsRaw);
    req.query.dealIds = filtered.join(",");
    if (filtered.length === 0) {
      req.ownerBdrActivityEmpty = true;
    }
  }

  if (hasDealId) {
    const check = await assertOwnerDealAccess(u, dealId);
    if (!check.ok) {
      return res.status(check.status).json({
        success: false,
        error: check.error || "forbidden",
        message: check.message || "You do not have access to this deal.",
      });
    }
  }

  return next();
}

export async function requireOwnerBdrDealMetaAccess(req, res, next) {
  const raw = req.query.ids ?? req.query.dealIds ?? "";
  const ids = normalizeDealIds(raw);
  const token = readBearer(req);

  if (ids.length && !token) {
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message: "Send Authorization: Bearer <Memberstack member JWT>.",
    });
  }

  if (!token || !ids.length) {
    return next();
  }

  const u = req.dealalityUser;
  if (!u) {
    return res.status(500).json({ ok: false, success: false, error: "User context missing." });
  }

  if (!ownerMustEnforceDealAccess(u)) {
    return next();
  }

  const filtered = await filterDealIdsWithAirtable(u, ids);
  req.query.ids = filtered.join(",");
  if (!filtered.length) {
    req.ownerBdrDealMetaEmpty = true;
  }
  return next();
}
