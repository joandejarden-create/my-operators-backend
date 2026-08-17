/**
 * Normalize authenticated Dealality user → AI Visibility viewer context.
 * Adapter over resolveDealalityUser / req.dealalityUser — not a second auth system.
 */

export const VIEWER_CONTEXT_VERSION = "ai_visibility_viewer_context_v1";

/**
 * @param {object|null|undefined} dealalityUser — output of resolveDealalityUser or req.dealalityUser
 * @returns {object|null}
 */
export function normalizeAiVisibilityViewerContext(dealalityUser) {
  if (!dealalityUser || typeof dealalityUser !== "object") return null;
  if (dealalityUser.found === false) return null;

  const companyIds = [...new Set([...(dealalityUser.companyIds || [])].filter(Boolean))];
  if (dealalityUser.companyId && !companyIds.includes(dealalityUser.companyId)) {
    companyIds.push(dealalityUser.companyId);
  }

  const workspaceAccess = Array.isArray(dealalityUser.workspaceAccess)
    ? [...dealalityUser.workspaceAccess]
    : [];

  const roles = [];
  if (dealalityUser.isAdmin || dealalityUser.flags?.isAdmin) roles.push("admin");
  if (dealalityUser.isOwner || dealalityUser.canAccessOwnerWorkspace) roles.push("owner");
  if (dealalityUser.isBrand || dealalityUser.canAccessBrandWorkspace) roles.push("brand");
  if (dealalityUser.isOperator || dealalityUser.canAccessOperatorWorkspace) roles.push("operator");
  if (dealalityUser.isDemo || dealalityUser.canAccessDemoWorkspace) roles.push("demo");

  return {
    viewerContextVersion: VIEWER_CONTEXT_VERSION,
    viewerUserId: dealalityUser.userRecordId || null,
    memberstackUserId: dealalityUser.memberstackId || null,
    viewerWorkspaceId: dealalityUser.activeWorkspace || workspaceAccess[0] || null,
    viewerCompanyId: dealalityUser.companyId || companyIds[0] || null,
    viewerCompanyIds: companyIds,
    viewerCompanyName: dealalityUser.companyName || null,
    viewerCompanyType: dealalityUser.companyType || dealalityUser.companyTypeRaw || null,
    workspaceAccess,
    roles: [...new Set(roles)],
    isAdmin: !!(dealalityUser.isAdmin || dealalityUser.flags?.isAdmin),
    isOwner: !!(dealalityUser.isOwner || dealalityUser.canAccessOwnerWorkspace),
    isBrand: !!(dealalityUser.isBrand || dealalityUser.canAccessBrandWorkspace),
    isOperator: !!(dealalityUser.isOperator || dealalityUser.canAccessOperatorWorkspace),
    isDemo: !!(dealalityUser.isDemo || dealalityUser.canAccessDemoWorkspace),
    flags: dealalityUser.flags || null,
  };
}

/**
 * Build viewer from an explicit fixture (unit tests) without Airtable.
 * @param {Partial<object>} partial
 */
export function buildFixtureViewerContext(partial = {}) {
  const base = {
    found: true,
    userRecordId: partial.viewerUserId || "recUserFixture",
    memberstackId: partial.memberstackUserId || "ms_fixture",
    companyId: partial.viewerCompanyId || null,
    companyIds: partial.viewerCompanyIds || (partial.viewerCompanyId ? [partial.viewerCompanyId] : []),
    companyName: partial.viewerCompanyName || null,
    companyType: partial.viewerCompanyType || null,
    workspaceAccess: partial.workspaceAccess || [],
    activeWorkspace: partial.viewerWorkspaceId || null,
    isAdmin: !!partial.isAdmin,
    isOwner: !!partial.isOwner,
    isBrand: !!partial.isBrand,
    isOperator: !!partial.isOperator,
    isDemo: !!partial.isDemo,
    canAccessOwnerWorkspace: !!partial.isOwner,
    canAccessBrandWorkspace: !!partial.isBrand,
    canAccessOperatorWorkspace: !!partial.isOperator,
    canAccessDemoWorkspace: !!partial.isDemo,
    flags: partial.flags || { isAdmin: !!partial.isAdmin },
  };
  return normalizeAiVisibilityViewerContext(base);
}
