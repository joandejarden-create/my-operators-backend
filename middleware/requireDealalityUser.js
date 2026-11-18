/**
 * Load Airtable Users row → req.dealalityUser (403 if missing or inactive).
 */

import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";
import {
  applyDemoStakeholderActiveWorkspace,
  canUseDemoFounderNavOverrides,
  getDemoStakeholderWorkspaces,
  isDemoStakeholderConstellation,
  readActiveWorkspaceHeader,
} from "../lib/dealality/demo-stakeholder-workspace.js";
import {
  applyDemoBrandPortfolioContext,
  readDemoBrandPortfolioHeader,
} from "../lib/dealality/demo-brand-portfolio-context.js";
import { resolveWorkspaceOptions } from "../lib/dealality/resolve-workspace-options.js";
import {
  attachLocalDemoRequestContext,
  resolveLocalDemoDealalityUser,
} from "../lib/dealality/local-demo-dealality-user.js";
import { resolveLocalDevBypassAdminUser } from "../lib/dealality/local-dev-bypass-admin-user.js";
import {
  elevateGovernedPlatformAdminOnRequest,
  finalizeDealalityUserAuthorization,
  traceDealalityAdminAuthDecision,
} from "../lib/dealality/governed-platform-admin-emails.js";

function verifiedViaClass(req) {
  const via = String(req.memberstackVerifiedVia || "").toLowerCase();
  if (!via) return "missing";
  if (via === "dev_bypass") return "dev_bypass";
  if (via.includes("jwks")) return "jwks";
  if (via.includes("rest")) return "rest";
  if (via.includes("member_lookup")) return "member_lookup";
  return "other";
}

function attachAndFinalize(req, dealalityUser, resolution) {
  const fin = finalizeDealalityUserAuthorization(req, dealalityUser);
  req.dealalityUser = dealalityUser;
  req.dealalityAuthResolution = resolution;
  req.dealalityAuthFinalizePath = fin.path;
  traceDealalityAdminAuthDecision({
    resolution,
    finalizePath: fin.path,
    elevated: fin.elevated,
    isAdmin: dealalityUser.isAdmin === true,
    userSource: dealalityUser.userSource || null,
    hasMemberstackId: Boolean(req.memberstackMemberId),
    hasVerifiedEmailClaim: Boolean(req.memberstackEmail),
    verifiedViaClass: verifiedViaClass(req),
  });
  return dealalityUser;
}

export async function requireDealalityUser(req, res, next) {
  if (!req.memberstackMemberId) {
    return res.status(401).json({
      ok: false,
      success: false,
      error: "authentication_required",
      message: "Memberstack identity missing; run memberstackAuth first.",
    });
  }

  try {
    const result = await resolveDealalityUser({
      memberstackId: req.memberstackMemberId,
      email: req.memberstackEmail,
    });

    if (!result.found) {
      if (result.reason === "user_inactive") {
        return res.status(403).json({
          ok: false,
          success: false,
          error: "user_inactive",
          message: "This account is inactive.",
          status: result.status || null,
        });
      }

      // LOCAL DEV ONLY — approved demo identity with no Airtable Users row.
      // Production never activates (LOCAL_DEMO_USER_FALLBACK_DISABLED_IN_PRODUCTION).
      const localDemo = resolveLocalDemoDealalityUser({
        memberstackId: req.memberstackMemberId,
        email: req.memberstackEmail,
        req,
      });
      if (localDemo) {
        const dealalityUser = attachLocalDemoRequestContext({ ...localDemo }, req);
        // CRITICAL: must finalize elevation before next() — local-demo previously
        // returned with isAdmin=false and skipped governed elevation.
        attachAndFinalize(req, dealalityUser, "local_demo_fallback");
        return next();
      }

      // LOCAL DEV ONLY — DEV_AUTH_BYPASS_EMAIL founder path (memberstackId=dev_bypass).
      // Parity with SPA local owner-app admin tools; never production.
      const localBypassAdmin = resolveLocalDevBypassAdminUser({
        memberstackId: req.memberstackMemberId,
        email: req.memberstackEmail,
        verifiedVia: req.memberstackVerifiedVia,
        req,
      });
      if (localBypassAdmin) {
        // Already isAdmin=true; finalize is a no-op skip for dev_bypass.
        attachAndFinalize(req, localBypassAdmin, "local_dev_bypass");
        return next();
      }

      // Governed founder/demo emails (assignments.v1.json) — stub admin when Users row missing.
      const governedStub = elevateGovernedPlatformAdminOnRequest(req);
      if (governedStub) {
        attachAndFinalize(req, governedStub, "governed_stub_missing_users_row");
        return next();
      }

      return res.status(403).json({
        ok: false,
        success: false,
        error: "user_not_found",
        message: "No Users row matched this Memberstack account in Airtable.",
        memberstackId: req.memberstackMemberId,
        hint: "Add slug or Unique Webflow ID on Users, or match by email.",
      });
    }

    const dealalityUser = {
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

    // Demo Dealality stakeholder switching: header selects Owner/Brand/Operator context.
    // Non-constellation / non-demo / non-founder-nav users are unchanged.
    // No query-param impersonation.
    if (
      isDemoStakeholderConstellation(dealalityUser) ||
      dealalityUser.isDemo ||
      dealalityUser.canAccessDemoWorkspace ||
      canUseDemoFounderNavOverrides(dealalityUser)
    ) {
      dealalityUser.demoStakeholderWorkspaces = getDemoStakeholderWorkspaces(dealalityUser);
      applyDemoStakeholderActiveWorkspace(
        dealalityUser,
        readActiveWorkspaceHeader(req)
      );
    }

    // Always attach canonical switcher options (production or demo).
    dealalityUser.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealalityUser);

    // Demo Brand Portfolio context (showcase) — separate from workspace switch.
    // Honored only for demo/founder; ignored for production clients.
    applyDemoBrandPortfolioContext(dealalityUser, readDemoBrandPortfolioHeader(req));

    // Portfolio must never shrink workspace options.
    dealalityUser.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealalityUser);

    attachAndFinalize(req, dealalityUser, "airtable_users_row");
    return next();
  } catch (err) {
    // Local/dev only: an unreachable Airtable base must not block the approved
    // QA identity, and must not fall open for any other account.
    const localDemo = resolveLocalDemoDealalityUser({
      memberstackId: req.memberstackMemberId,
      email: req.memberstackEmail,
      req,
    });
    if (localDemo) {
      const dealalityUser = attachLocalDemoRequestContext({ ...localDemo }, req);
      // Same finalize requirement as the !found local-demo branch.
      attachAndFinalize(req, dealalityUser, "local_demo_airtable_error");
      return next();
    }
    console.error("[requireDealalityUser] lookup error:", (err && err.message) || err);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "airtable_error",
      message: "Failed to load user from Airtable.",
    });
  }
}
