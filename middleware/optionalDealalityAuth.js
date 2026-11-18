/**
 * Optional Memberstack + Dealality user — continues without auth when Bearer absent.
 * Sets req.dealalityUser when a valid session is present.
 *
 * Local owner-app only: DEV_AUTH_BYPASS_EMAIL applies when
 * X-Dealality-Owner-App: 1 on localhost/dev (never production, never share-only).
 *
 * Parity with requireDealalityUser for Radar /app embeds:
 *   - Attach workspaceAccess + flags from Airtable
 *   - Local demo fallback for approved demo email when no Users row
 *   - ADP owner-app admin email elevation (server allowlist, not header-only)
 */

import { verifyMemberstackToken } from "../lib/memberstack/verify-token.js";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";
import {
  attachLocalDemoRequestContext,
  resolveLocalDemoDealalityUser,
} from "../lib/dealality/local-demo-dealality-user.js";
import {
  isAdpOwnerAppAdminEmail,
  loadAdpOwnerAppAdminEmails,
  normalizeIdentityEmail,
} from "../lib/dealality/adp-owner-app-admin-emails.js";

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

function identityEmailsFromReq(req) {
  return [
    normalizeIdentityEmail(req.memberstackEmail),
    normalizeIdentityEmail(req.dealalityUser?.email),
  ].filter(Boolean);
}

function buildAirtableDealalityUser(result, req) {
  return {
    email: result.email,
    memberstackId: result.memberstackId || req.memberstackMemberId,
    role: result.role,
    primaryRole: result.primaryRole,
    legacyRole: result.legacyRole,
    workspaceAccess: result.workspaceAccess || [],
    flags: result.flags,
    companyType: result.companyType,
    isOwnerOperator: result.isOwnerOperator,
    canAccessOwnerWorkspace: result.canAccessOwnerWorkspace,
    canAccessOperatorWorkspace: result.canAccessOperatorWorkspace,
    canAccessBrandWorkspace: result.canAccessBrandWorkspace,
    canAccessDemoWorkspace: result.canAccessDemoWorkspace,
    isDemo: result.isDemo,
    demoPreviewWorkspaces: result.demoPreviewWorkspaces || [],
    roleRaw: result.roleRaw,
    companyId: result.companyId,
    companyIds: result.companyIds || [],
    companyName: result.companyName,
    status: result.status,
    isAdmin: result.isAdmin,
    isOwner: result.isOwner,
    isBrand: result.isBrand,
    isOperator: result.isOperator,
    operatorDealRequestEligible: result.operatorDealRequestEligible,
    reviewBeforeOutreach: result.reviewBeforeOutreach,
    userRecordId: result.userRecordId,
    activeWorkspace: result.activeWorkspace || null,
    userSource: "AIRTABLE",
    airtableUserFound: true,
  };
}

/**
 * Airtable may mark founder/demo as role=owner with isAdmin=false.
 * ADP owner-app still grants platform admin via ADP_OWNER_APP_ADMIN_EMAILS /
 * assignments.v1.json adminEmails — elevate dealalityUser.isAdmin for consistency.
 *
 * Checks Memberstack email AND resolved Airtable email (either may authorize).
 * Exported so Radar internal-preview (/app#/opportunity-radar) can share the same
 * owner-app admin elevation without inventing a second allowlist.
 */
export function elevateAdpOwnerAppAdminFlags(req) {
  const emails = identityEmailsFromReq(req);
  if (!emails.length) return;
  if (!isAdpOwnerAppAdminEmail(emails)) return;
  const email = emails[0];
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
      workspaceAccess: [],
      flags: { isAdmin: true },
      userRecordId: null,
      userSource: "ADP_OWNER_APP_ADMIN_EMAIL",
    };
    return;
  }
  req.dealalityUser.isAdmin = true;
  if (!req.dealalityUser.flags || typeof req.dealalityUser.flags !== "object") {
    req.dealalityUser.flags = {};
  }
  req.dealalityUser.flags.isAdmin = true;
  if (!req.dealalityUser.role || req.dealalityUser.role === "owner") {
    req.dealalityUser.role = "admin";
  }
}

/** @deprecated use loadAdpOwnerAppAdminEmails from lib — kept for existing test imports */
export { loadAdpOwnerAppAdminEmails };

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
    workspaceAccess: [],
    flags: { isAdmin: true },
    userRecordId: null,
    userSource: "LOCAL_DEV_AUTH_BYPASS",
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
      req.dealalityUser = buildAirtableDealalityUser(result, req);
    } else {
      // Parity with requireDealalityUser: approved local demo identity without Users row.
      const localDemo = resolveLocalDemoDealalityUser({
        memberstackId: req.memberstackMemberId,
        email: req.memberstackEmail,
        req,
      });
      if (localDemo) {
        req.dealalityUser = attachLocalDemoRequestContext({ ...localDemo }, req);
      }
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
