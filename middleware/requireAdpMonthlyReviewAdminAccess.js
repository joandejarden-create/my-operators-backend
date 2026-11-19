/**
 * Gate Admin Resources → AI Demand Reviews + ADP Action Plan.
 * Founder + platform admin — not customer Workspace Access Admin alone unless also admin/founder.
 *
 * Local QA: approved demo identity via hasLocalDealalityAdminAccess (localhost/dev only).
 * Production: never grants from email alone.
 *
 * Doctrine: ADP_LOCAL_ADMIN_AUTH_SINGLE_SOURCE / ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY
 */

import { isInternalRunbookAdmin } from "../lib/dealality/internal-runbook-admin.js";
import {
  hasLocalDealalityAdminAccess,
  ADP_LOCAL_ADMIN_AUTH_SINGLE_SOURCE,
  ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY,
} from "../lib/dealality/local-dealality-admin-access.js";
import { WORKSPACE_ADMIN } from "../lib/company-workspace-access.js";
import {
  classifyMemberstackId,
  classifyVerifiedEmail,
  traceDealalityAdminAuthDecision,
} from "../lib/dealality/governed-platform-admin-emails.js";

export { ADP_LOCAL_ADMIN_AUTH_SINGLE_SOURCE, ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY };

export function canAccessAdpMonthlyReviewAdmin(user, req = {}, env = process.env) {
  if (!user) return false;
  if (user.isAdmin || user.flags?.isAdmin === true) return true;
  const workspaces = Array.isArray(user.workspaceAccess) ? user.workspaceAccess : [];
  if (workspaces.includes(WORKSPACE_ADMIN)) return true;
  if (
    isInternalRunbookAdmin({
      email: user.email || req.memberstackEmail || req.memberstackAuth?.email,
      dealality: { isAdmin: user.isAdmin, flags: user.flags },
      companyName: user.companyName,
    })
  ) {
    return true;
  }
  // Local/dev only — never production email elevation
  if (hasLocalDealalityAdminAccess(user, req, env)) return true;
  return false;
}

export function requireAdpMonthlyReviewAdminAccess(req, res, next) {
  const u = req.dealalityUser;
  const corr = req.dealalityAuthCorr || null;
  const route = req.originalUrl || req.url || null;
  const localQa = u ? hasLocalDealalityAdminAccess(u, req) : false;
  if (!u) {
    traceDealalityAdminAuthDecision({
      correlationId: corr,
      phase: "requireAdpMonthlyReviewAdminAccess",
      route,
      bearerPresent: Boolean(req.headers?.authorization || req.headers?.Authorization),
      memberstackIdClass: classifyMemberstackId(req.memberstackMemberId),
      verifiedEmailClass: classifyVerifiedEmail(req.memberstackEmail),
      middleware: "requireAdpMonthlyReviewAdminAccess",
      denyReason: "user_context_missing",
      status: 500,
    });
    return res.status(500).json({
      ok: false,
      success: false,
      error: "server_error",
      message: "User context missing.",
      corr,
    });
  }
  if (!canAccessAdpMonthlyReviewAdmin(u, req)) {
    traceDealalityAdminAuthDecision({
      correlationId: corr,
      phase: "requireAdpMonthlyReviewAdminAccess",
      route,
      bearerPresent: Boolean(req.headers?.authorization || req.headers?.Authorization),
      jwtOutcome: req.memberstackVerifiedVia === "dev_bypass" ? "dev_bypass_no_bearer" : "verified",
      memberstackIdClass: classifyMemberstackId(req.memberstackMemberId),
      memberstackId: req.memberstackMemberId || null,
      verifiedEmailClass: classifyVerifiedEmail(req.memberstackEmail),
      resolution: req.dealalityAuthResolution || null,
      finalizePath: req.dealalityAuthFinalizePath || null,
      elevated:
        u.governedPlatformAdminElevated === true || u.localQaAdminElevated === true,
      isAdmin: u.isAdmin === true,
      localQaAccess: localQa,
      userSource: u.userSource || null,
      hasMemberstackId: Boolean(req.memberstackMemberId),
      hasVerifiedEmailClaim: Boolean(req.memberstackEmail),
      verifiedViaClass: String(req.memberstackVerifiedVia || "missing"),
      middleware: "requireAdpMonthlyReviewAdminAccess",
      denyReason: "forbidden_adp_monthly_review_admin",
      status: 403,
    });
    return res.status(403).json({
      ok: false,
      success: false,
      error: "forbidden_adp_monthly_review_admin",
      message: "AI Demand Reviews admin access required.",
      corr,
    });
  }
  traceDealalityAdminAuthDecision({
    correlationId: corr,
    phase: "requireAdpMonthlyReviewAdminAccess",
    route,
    bearerPresent: Boolean(req.headers?.authorization || req.headers?.Authorization),
    memberstackIdClass: classifyMemberstackId(req.memberstackMemberId),
    verifiedEmailClass: classifyVerifiedEmail(req.memberstackEmail),
    resolution: req.dealalityAuthResolution || null,
    finalizePath: req.dealalityAuthFinalizePath || null,
    elevated: u.governedPlatformAdminElevated === true || u.localQaAdminElevated === true,
    isAdmin: u.isAdmin === true,
    localQaAccess: localQa,
    userSource: u.userSource || null,
    hasMemberstackId: Boolean(req.memberstackMemberId),
    hasVerifiedEmailClaim: Boolean(req.memberstackEmail),
    verifiedViaClass: String(req.memberstackVerifiedVia || "missing"),
    middleware: "requireAdpMonthlyReviewAdminAccess",
    status: 200,
  });
  return next();
}
