/**
 * Optional Memberstack + Dealality user — continues without auth when Bearer absent.
 * Sets req.dealalityUser when a valid session is present.
 *
 * Local owner-app only: DEV_AUTH_BYPASS_EMAIL applies when
 * X-Dealality-Owner-App: 1 on localhost/dev (never production, never share-only).
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { verifyMemberstackToken } from "../lib/memberstack/verify-token.js";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";

function readBearerToken(req) {
  const raw = req.headers.authorization || req.headers.Authorization;
  if (!raw || typeof raw !== "string") return null;
  const m = raw.match(/^Bearer\s+(\S+)$/i);
  return m ? m[1] : null;
}

function ownerAppHeaderPresent(req) {
  const v = req.headers?.["x-dealality-owner-app"] || req.headers?.["X-Dealality-Owner-App"];
  return v === "1" || v === "true";
}

function isLocalDevHost(req) {
  if (process.env.NODE_ENV === "production") return false;
  const host = String(req.hostname || req.headers?.host || "").toLowerCase();
  return (
    host.startsWith("localhost") ||
    host.startsWith("127.0.0.1") ||
    host.startsWith("[::1]")
  );
}

function parseEmailList(raw) {
  return String(raw || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function loadAdpOwnerAppAdminEmails() {
  const fromEnv = parseEmailList(process.env.ADP_OWNER_APP_ADMIN_EMAILS);
  const path = join(
    process.cwd(),
    "data/ai-demand-positioning/owner-property-access/assignments.v1.json"
  );
  let fromDoc = [];
  if (existsSync(path)) {
    try {
      const doc = JSON.parse(readFileSync(path, "utf8"));
      fromDoc = (doc?.adminEmails || []).map((e) => String(e).toLowerCase().trim()).filter(Boolean);
    } catch {
      fromDoc = [];
    }
  }
  return new Set([...fromEnv, ...fromDoc]);
}

/**
 * Airtable may mark founder/demo as role=owner with isAdmin=false.
 * ADP owner-app still grants platform admin via ADP_OWNER_APP_ADMIN_EMAILS /
 * assignments.v1.json adminEmails — elevate dealalityUser.isAdmin for consistency.
 */
function elevateAdpOwnerAppAdminFlags(req) {
  const email = String(req.memberstackEmail || req.dealalityUser?.email || "")
    .toLowerCase()
    .trim();
  if (!email) return;
  if (!loadAdpOwnerAppAdminEmails().has(email)) return;
  if (!req.dealalityUser) {
    req.dealalityUser = {
      email,
      memberstackId: req.memberstackMemberId || null,
      role: "admin",
      isAdmin: true,
      isDemo: false,
      isOwner: true,
      isBrand: false,
      isOperator: false,
      userRecordId: null,
    };
    return;
  }
  req.dealalityUser.isAdmin = true;
  if (!req.dealalityUser.role || req.dealalityUser.role === "owner") {
    req.dealalityUser.role = "admin";
  }
}

/**
 * DEV_BYPASS_INTERNAL_ONLY — owner-app header + local/dev host + non-production.
 * Never activates for external share-only requests (no Owner-App header).
 */
function tryDevAuthBypass(req) {
  const email = process.env.DEV_AUTH_BYPASS_EMAIL;
  if (!email) return false;
  if (process.env.NODE_ENV === "production") return false;
  if (!isLocalDevHost(req)) return false;
  if (!ownerAppHeaderPresent(req)) return false;
  req.memberstack = { id: "dev_bypass", email };
  req.memberstackMemberId = "dev_bypass";
  req.memberstackEmail = email;
  req.memberstackVerifiedVia = "dev_bypass";
  req.dealalityUser = {
    email,
    memberstackId: "dev_bypass",
    role: "admin",
    isAdmin: true,
    isDemo: false,
    isOwner: true,
    isBrand: false,
    isOperator: false,
    userRecordId: null,
  };
  return true;
}

function isShareCapabilityBearer(token) {
  const t = String(token || "");
  return t.startsWith("adpshare.v1.") || t.startsWith("baiparent.v1.");
}

export async function optionalDealalityAuth(req, res, next) {
  // Prefer real Bearer JWT when present; only then fall back to local owner-app bypass.
  const token = readBearerToken(req);
  if (!token || token.startsWith("mem_")) {
    if (tryDevAuthBypass(req)) return next();
    return next();
  }

  // MEMBERSTACK_JWT_NOT_SHARE_CAPABILITY — never treat share capability as Memberstack JWT.
  if (isShareCapabilityBearer(token)) {
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
    elevateAdpOwnerAppAdminFlags(req);
  } catch (err) {
    // JWT failed — allow governed local owner-app bypass for internal QA.
    if (tryDevAuthBypass(req)) return next();
    if (process.env.NODE_ENV !== "production") {
      console.warn(
        "[optionalDealalityAuth] auth skipped:",
        (err && err.message) || err
      );
    }
  }

  return next();
}
