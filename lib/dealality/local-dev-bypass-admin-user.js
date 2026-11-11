/**
 * LOCAL DEV_AUTH_BYPASS admin user resolution
 *
 * When localhost/dev uses DEV_AUTH_BYPASS_EMAIL and Airtable has no Users row
 * for that identity, resolve a governed local admin context so AI Admin /
 * Full ADP PDF APIs match the SPA shell's local owner-app bypass.
 *
 * Doctrine:
 *   ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY
 *   LOCAL_DEV_AUTH_BYPASS_ADMIN_USER_V1
 *
 * Never production.
 * Never grants from an arbitrary email — only the configured DEV_AUTH_BYPASS_EMAIL
 * when memberstackAuth actually applied the bypass (memberstackId === "dev_bypass").
 * Not a substitute for MEMBERSTACK_AUTHENTICATED_PDF_PASS.
 */

import {
  isLocalDevRequest,
  normalizeAdminEmail,
  ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY,
} from "./local-dealality-admin-access.js";

export const LOCAL_DEV_AUTH_BYPASS_ADMIN_USER_V1 =
  "LOCAL_DEV_AUTH_BYPASS_ADMIN_USER_V1";
export const LOCAL_DEV_AUTH_BYPASS_USER_SOURCE = "LOCAL_DEV_AUTH_BYPASS";

/**
 * @param {NodeJS.ProcessEnv} [env]
 */
export function configuredDevAuthBypassEmail(env = process.env) {
  if (String(env.NODE_ENV || "").toLowerCase() === "production") return "";
  return normalizeAdminEmail(env.DEV_AUTH_BYPASS_EMAIL);
}

/**
 * @param {{
 *   memberstackId?: string|null,
 *   email?: string|null,
 *   verifiedVia?: string|null,
 *   req?: object,
 *   env?: NodeJS.ProcessEnv,
 * }} input
 */
export function isEligibleForLocalDevBypassAdminUser(input = {}) {
  const env = input.env || process.env;
  if (String(env.NODE_ENV || "").toLowerCase() === "production") return false;
  if (!isLocalDevRequest(input.req || {}, env)) return false;

  const bypassEmail = configuredDevAuthBypassEmail(env);
  if (!bypassEmail) return false;

  const memberstackId = String(input.memberstackId || "").trim();
  const verifiedVia = String(
    input.verifiedVia ||
      input.req?.memberstackVerifiedVia ||
      ""
  ).trim();
  // Only the intentional memberstackAuth bypass path — not a forged JWT id.
  if (memberstackId !== "dev_bypass" && verifiedVia !== "dev_bypass") {
    return false;
  }

  const email = normalizeAdminEmail(
    input.email ||
      input.req?.memberstackEmail ||
      input.req?.memberstackAuth?.email
  );
  if (!email || email !== bypassEmail) return false;
  return true;
}

/**
 * @param {{
 *   memberstackId?: string|null,
 *   email?: string|null,
 *   verifiedVia?: string|null,
 *   req?: object,
 *   env?: NodeJS.ProcessEnv,
 * }} input
 * @returns {object|null}
 */
export function resolveLocalDevBypassAdminUser(input = {}) {
  if (!isEligibleForLocalDevBypassAdminUser(input)) return null;
  const env = input.env || process.env;
  const email = configuredDevAuthBypassEmail(env);

  return {
    email,
    memberstackId: "dev_bypass",
    firstName: "Local",
    lastName: "Founder",
    profilePhotoUrl: null,
    userSource: LOCAL_DEV_AUTH_BYPASS_USER_SOURCE,
    airtableUserFound: false,
    userRecordId: null,
    role: "admin",
    primaryRole: "admin",
    legacyRole: "admin",
    workspaceAccess: ["Admin"],
    flags: {
      isOwner: true,
      isOperator: false,
      isBrand: false,
      isDemo: false,
      isAdmin: true,
      isOwnerOperator: false,
    },
    companyType: null,
    isOwnerOperator: false,
    canAccessOwnerWorkspace: true,
    canAccessOperatorWorkspace: true,
    canAccessBrandWorkspace: true,
    canAccessDemoWorkspace: false,
    isDemo: false,
    demoPreviewWorkspaces: [],
    roleRaw: "Admin",
    roleSource: LOCAL_DEV_AUTH_BYPASS_USER_SOURCE,
    companyId: null,
    companyIds: [],
    companyProfileId: null,
    companyName: null,
    status: "active",
    isAdmin: true,
    isOwner: true,
    isBrand: false,
    isOperator: false,
    operatorDealRequestEligible: false,
    reviewBeforeOutreach: false,
    operatorExplorerEligible: false,
    thirdPartyManagementAvailable: false,
    activeWorkspace: null,
    founderNavOverridesAvailable: true,
    localDevAuthBypassAdmin: true,
    doctrine: {
      LOCAL_DEV_AUTH_BYPASS_ADMIN_USER_V1,
      ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY,
    },
  };
}

/**
 * /api/me payload for LOCAL_DEV_AUTH_BYPASS (no Airtable Users row).
 */
export function buildLocalDevBypassAdminMeResponse(input = {}) {
  const base = resolveLocalDevBypassAdminUser(input);
  if (!base) return null;
  const tokenPayload =
    input.tokenPayload && typeof input.tokenPayload === "object"
      ? input.tokenPayload
      : {};

  return {
    success: true,
    ok: true,
    memberstackId: "dev_bypass",
    memberstack: {
      id: "dev_bypass",
      tokenIssuedAt: tokenPayload.iat != null ? tokenPayload.iat : null,
      tokenExpiresAt: tokenPayload.exp != null ? tokenPayload.exp : null,
    },
    user: {
      email: base.email,
      firstName: base.firstName,
      lastName: base.lastName,
      profilePhotoUrl: null,
    },
    airtable: {
      userRecordId: null,
      airtableUserId: null,
      email: base.email,
      firstName: base.firstName,
      lastName: base.lastName,
      profilePhotoUrl: null,
      companyProfileId: null,
      companyName: null,
      userSource: LOCAL_DEV_AUTH_BYPASS_USER_SOURCE,
      airtableUserFound: false,
    },
    permissions: {
      allowedBrandNames: [],
      allowedBrandRecordIds: [],
      allowedRegions: [],
      allowedOperatingCompanyNames: [],
      allowedOperatorSetupIds: [],
      primaryOperatingCompanyName: null,
    },
    dealality: {
      role: "admin",
      primaryRole: "admin",
      legacyRole: "admin",
      roleRaw: "Admin",
      roleSource: LOCAL_DEV_AUTH_BYPASS_USER_SOURCE,
      companyType: null,
      companyProfileId: null,
      companyIds: [],
      companyName: null,
      profilePhotoUrl: null,
      workspaceAccess: ["Admin"],
      flags: { ...base.flags },
      isOwner: true,
      isBrand: false,
      isOperator: false,
      isDemo: false,
      isAdmin: true,
      isOwnerOperator: false,
      isInternalRunbookAdmin: true,
      canAccessOwnerWorkspace: true,
      canAccessOperatorWorkspace: true,
      canAccessBrandWorkspace: true,
      canAccessDemoWorkspace: false,
      localDealalityAdminAccess: true,
      adpMonthlyReviewAdmin: true,
      localDevAuthBypassAdmin: true,
      founderNavOverridesAvailable: true,
      userSource: LOCAL_DEV_AUTH_BYPASS_USER_SOURCE,
      airtableUserFound: false,
    },
    accountAccess: {
      state: "active",
      pendingApproval: false,
      accountStatus: "active",
      userTitle: null,
      userMessage: null,
      suppressBrandAssignmentToast: true,
    },
    meta: {
      matchedBy: "local_dev_auth_bypass_admin",
      userSource: LOCAL_DEV_AUTH_BYPASS_USER_SOURCE,
      airtableUserFound: false,
      warnings: [
        LOCAL_DEV_AUTH_BYPASS_ADMIN_USER_V1,
        ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY,
        "NOT_MEMBERSTACK_AUTHENTICATED_PDF_PASS",
      ],
      doctrine: {
        LOCAL_DEV_AUTH_BYPASS_ADMIN_USER_V1,
        ADP_LOCAL_ADMIN_NAV_PAGE_API_AUTH_PARITY,
      },
    },
  };
}
