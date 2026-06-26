/**
 * Optional Memberstack + Dealality user — continues without auth when Bearer absent.
 * Sets req.dealalityUser when a valid session is present.
 */

import { verifyMemberstackToken } from "../lib/memberstack/verify-token.js";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";

function readBearerToken(req) {
  const raw = req.headers.authorization || req.headers.Authorization;
  if (!raw || typeof raw !== "string") return null;
  const m = raw.match(/^Bearer\s+(\S+)$/i);
  return m ? m[1] : null;
}

export async function optionalDealalityAuth(req, res, next) {
  const token = readBearerToken(req);
  if (!token || token.startsWith("mem_")) {
    return next();
  }

  try {
    const verified = await verifyMemberstackToken(token);
    req.memberstack = verified.raw;
    req.memberstackMemberId = verified.id;
    req.memberstackEmail = verified.email || null;
    req.memberstackVerifiedVia = verified.verifiedVia;

    const result = await resolveDealalityUser({
      memberstackId: req.memberstackMemberId,
      email: req.memberstackEmail,
    });

    if (result.found) {
      req.dealalityUser = {
        email: result.email,
        memberstackId: result.memberstackId || req.memberstackMemberId,
        role: result.role,
        isAdmin: result.isAdmin,
        isDemo: result.isDemo,
        isOwner: result.isOwner,
        isBrand: result.isBrand,
        isOperator: result.isOperator,
        userRecordId: result.userRecordId,
      };
    }
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn(
        "[optionalDealalityAuth] auth skipped:",
        (err && err.message) || err
      );
    }
  }

  return next();
}
