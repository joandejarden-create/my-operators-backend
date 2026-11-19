/**
 * Verify Authorization: Bearer <Memberstack JWT>.
 * Sets req.memberstack, req.memberstackMemberId, req.memberstackEmail.
 */

import { verifyMemberstackToken } from "../lib/memberstack/verify-token.js";
import {
  classifyMemberstackId,
  classifyVerifiedEmail,
  traceDealalityAdminAuthDecision,
} from "../lib/dealality/governed-platform-admin-emails.js";
import { resolveLocalDemoEmailForVerifiedMemberstackId } from "../lib/dealality/local-demo-memberstack-id-binding.js";

function readBearerToken(req) {
  const raw = req.headers.authorization || req.headers.Authorization;
  if (!raw || typeof raw !== "string") return null;
  const m = raw.match(/^Bearer\s+(\S+)$/i);
  return m ? m[1] : null;
}

/** Parity with api/me pickEmailFromPayload — nested JWT email claims. */
function pickEmailFromPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const candidates = [
    payload.email,
    payload.auth?.email,
    payload.member?.email,
    payload.data?.email,
    payload.data?.auth?.email,
  ];
  for (const c of candidates) {
    const s = c != null ? String(c).trim().toLowerCase() : "";
    if (s && s.includes("@")) return s;
  }
  return null;
}

export async function memberstackAuth(req, res, next) {
  const token = readBearerToken(req);
  const corr =
    req.dealalityAuthCorr ||
    `adp_auth_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
  req.dealalityAuthCorr = corr;
  const route = req.originalUrl || req.url || null;

  // Local dev bypass: only when NO Bearer token is present.
  // If the browser/session sends a real Memberstack JWT, always prefer it —
  // otherwise DEV_AUTH_BYPASS_EMAIL silently impersonates a different identity
  // and Airtable Users lookup fails for the signed-in Memberstack account.
  // Never deploy this env var to production.
  const devBypassEmail = process.env.DEV_AUTH_BYPASS_EMAIL;
  if (!token && devBypassEmail && process.env.NODE_ENV !== "production") {
    req.memberstack = { id: "dev_bypass", email: devBypassEmail };
    req.memberstackMemberId = "dev_bypass";
    req.memberstackEmail = devBypassEmail;
    req.memberstackVerifiedVia = "dev_bypass";
    traceDealalityAdminAuthDecision({
      correlationId: corr,
      phase: "memberstackAuth",
      route,
      bearerPresent: false,
      jwtOutcome: "dev_bypass_no_bearer",
      memberstackIdClass: "dev_bypass",
      verifiedEmailClass: classifyVerifiedEmail(devBypassEmail),
      verifiedViaClass: "dev_bypass",
      hasMemberstackId: true,
      hasVerifiedEmailClaim: true,
      middleware: "memberstackAuth",
    });
    return next();
  }

  if (!token) {
    traceDealalityAdminAuthDecision({
      correlationId: corr,
      phase: "memberstackAuth",
      route,
      bearerPresent: false,
      jwtOutcome: "missing",
      memberstackIdClass: "missing",
      verifiedEmailClass: "missing",
      middleware: "memberstackAuth",
      denyReason: "authentication_required",
      status: 401,
    });
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message: "Send Authorization: Bearer <Memberstack member JWT>.",
      corr,
    });
  }

  if (token.startsWith("mem_")) {
    console.warn("[memberstackAuth] Bearer value looks like a member id (mem_…), not a JWT — use the eyJ… session token.");
    traceDealalityAdminAuthDecision({
      correlationId: corr,
      phase: "memberstackAuth",
      route,
      bearerPresent: true,
      jwtOutcome: "mem_id_not_jwt",
      memberstackIdClass: "other",
      verifiedEmailClass: "missing",
      middleware: "memberstackAuth",
      denyReason: "invalid_token",
      status: 401,
    });
    return res.status(401).json({
      ok: false,
      success: false,
      error: "invalid_token",
      message: "Bearer token must be the Memberstack JWT (eyJ…), not the mem_… member id.",
      corr,
    });
  }

  try {
    const verified = await verifyMemberstackToken(token);
    req.memberstack = verified.raw;
    req.memberstackMemberId = verified.id;
    req.memberstackEmail =
      (verified.email && String(verified.email).trim().toLowerCase()) ||
      pickEmailFromPayload(verified.raw) ||
      null;
    req.memberstackVerifiedVia = verified.verifiedVia;
    req.memberstackEmailLookup = verified.emailLookup || (req.memberstackEmail ? "jwt_claim" : null);
    // Local-only exact mem id → Demo email (gitignored env). Never from client headers.
    if (!req.memberstackEmail) {
      const bound = resolveLocalDemoEmailForVerifiedMemberstackId(verified.id, req);
      if (bound) {
        req.memberstackEmail = bound;
        req.memberstackEmailLookup = "local_demo_member_id_binding";
      }
    }
    traceDealalityAdminAuthDecision({
      correlationId: corr,
      phase: "memberstackAuth",
      route,
      bearerPresent: true,
      jwtOutcome: "verified",
      memberstackIdClass: classifyMemberstackId(verified.id),
      memberstackId: verified.id || null,
      verifiedEmailClass: classifyVerifiedEmail(req.memberstackEmail),
      verifiedViaClass: String(verified.verifiedVia || "other"),
      hasMemberstackId: true,
      hasVerifiedEmailClaim: Boolean(req.memberstackEmail),
      emailLookupClass: req.memberstackEmailLookup || null,
      middleware: "memberstackAuth",
    });
    return next();
  } catch (err) {
    const msg = (err && err.message) || String(err);
    const expired = /expired|exp/i.test(msg);
    if (!expired) {
      console.warn("[memberstackAuth] token verification failed:", msg.slice(0, 120));
    }
    traceDealalityAdminAuthDecision({
      correlationId: corr,
      phase: "memberstackAuth",
      route,
      bearerPresent: true,
      jwtOutcome: expired ? "expired" : "invalid",
      memberstackIdClass: "missing",
      verifiedEmailClass: "missing",
      middleware: "memberstackAuth",
      denyReason: expired ? "token_expired" : "invalid_token",
      status: 401,
    });
    return res.status(401).json({
      ok: false,
      success: false,
      error: expired ? "token_expired" : "invalid_token",
      message: expired ? "Token expired." : "Invalid or unverifiable Memberstack token.",
      corr,
    });
  }
}
