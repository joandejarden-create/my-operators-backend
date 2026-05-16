/**
 * Verify Authorization: Bearer <Memberstack JWT>.
 * Sets req.memberstack, req.memberstackMemberId, req.memberstackEmail.
 */

import { verifyMemberstackToken } from "../lib/memberstack/verify-token.js";

function readBearerToken(req) {
  const raw = req.headers.authorization || req.headers.Authorization;
  if (!raw || typeof raw !== "string") return null;
  const m = raw.match(/^Bearer\s+(\S+)$/i);
  return m ? m[1] : null;
}

export async function memberstackAuth(req, res, next) {
  const token = readBearerToken(req);
  if (!token) {
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message: "Send Authorization: Bearer <Memberstack member JWT>.",
    });
  }

  if (token.startsWith("mem_")) {
    console.warn("[memberstackAuth] Bearer value looks like a member id (mem_…), not a JWT — use the eyJ… session token.");
    return res.status(401).json({
      ok: false,
      success: false,
      error: "invalid_token",
      message: "Bearer token must be the Memberstack JWT (eyJ…), not the mem_… member id.",
    });
  }

  try {
    const verified = await verifyMemberstackToken(token);
    req.memberstack = verified.raw;
    req.memberstackMemberId = verified.id;
    req.memberstackEmail = verified.email || null;
    req.memberstackVerifiedVia = verified.verifiedVia;
    return next();
  } catch (err) {
    const msg = (err && err.message) || String(err);
    const expired = /expired|exp/i.test(msg);
    if (!expired) {
      console.warn("[memberstackAuth] token verification failed:", msg.slice(0, 120));
    }
    return res.status(401).json({
      ok: false,
      success: false,
      error: expired ? "token_expired" : "invalid_token",
      message: expired ? "Token expired." : "Invalid or unverifiable Memberstack token.",
    });
  }
}
