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

    req.dealalityUser = dealalityUser;
    return next();
  } catch (err) {
    console.error("[requireDealalityUser] lookup error:", (err && err.message) || err);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "airtable_error",
      message: "Failed to load user from Airtable.",
    });
  }
}
