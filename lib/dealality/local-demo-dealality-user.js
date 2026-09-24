/**
 * Canonical LOCAL DEMO Dealality user resolution.
 *
 * When authenticated Memberstack identity `dealalitydemo@dealality.com` has no
 * Airtable Users row, local/dev only may resolve a complete synthetic Dealality
 * user context — never in production, never as a fake Airtable row.
 *
 * Doctrine:
 *   DEALALITY_LOCAL_DEMO_USER_RESOLUTION
 *   LOCAL_DEMO_USER_FALLBACK_DISABLED_IN_PRODUCTION
 *   SINGLE_CANONICAL_LOCAL_DEMO_USER_CONTEXT
 *   LOCAL_DEMO_IDENTITY_RESOLUTION_NAV_PAGE_API_PARITY
 *
 * Resolution order (callers must follow):
 *   Memberstack OK → Airtable Users lookup → if found use real user
 *   → else if local demo eligible → resolveLocalDemoDealalityUser
 *   → else deny
 */

import {
  DEMO_PREVIEW_WORKSPACES,
  WORKSPACE_DEMO,
  WORKSPACE_OWNER,
} from "../company-workspace-access.js";
import {
  applyDemoStakeholderActiveWorkspace,
  enrichDealalityMeForDemoStakeholder,
  MAP_DEMO_STAKEHOLDER_COMPANIES,
  readActiveWorkspaceHeader,
} from "./demo-stakeholder-workspace.js";
import {
  applyDemoBrandPortfolioContext,
  canUseDemoBrandPortfolioSwitch,
  listDemoBrandPortfolioOptions,
  readDemoBrandPortfolioHeader,
} from "./demo-brand-portfolio-context.js";
import { resolveWorkspaceOptions } from "./resolve-workspace-options.js";
import {
  APPROVED_LOCAL_DEALALITY_QA_ADMIN_EMAILS,
  hasLocalDealalityAdminAccess,
  isLocalDevRequest,
  normalizeAdminEmail,
} from "./local-dealality-admin-access.js";

export const DEALALITY_LOCAL_DEMO_USER_RESOLUTION =
  "DEALALITY_LOCAL_DEMO_USER_RESOLUTION";
export const LOCAL_DEMO_USER_FALLBACK_DISABLED_IN_PRODUCTION =
  "LOCAL_DEMO_USER_FALLBACK_DISABLED_IN_PRODUCTION";
export const SINGLE_CANONICAL_LOCAL_DEMO_USER_CONTEXT =
  "SINGLE_CANONICAL_LOCAL_DEMO_USER_CONTEXT";
export const LOCAL_DEMO_IDENTITY_RESOLUTION_NAV_PAGE_API_PARITY =
  "LOCAL_DEMO_IDENTITY_RESOLUTION_NAV_PAGE_API_PARITY";

/** Explicit source marker — never pretend airtableUserFound. */
export const LOCAL_DEMO_USER_SOURCE = "LOCAL_DEMO";

export const APPROVED_LOCAL_DEMO_EMAIL = "dealalitydemo@dealality.com";

const LOCAL_DEMO_FIRST_NAME = "Demo";
const LOCAL_DEMO_LAST_NAME = "Dealality";

/**
 * Production must never activate this fallback — even for the same email.
 * @param {NodeJS.ProcessEnv} [env]
 */
export function isLocalDemoUserFallbackDisabledInProduction(env = process.env) {
  return String(env.NODE_ENV || "").toLowerCase() === "production";
}

/**
 * @param {string|null|undefined} email
 * @param {object} [req]
 * @param {NodeJS.ProcessEnv} [env]
 */
export function isEligibleForLocalDemoUserResolution(email, req = {}, env = process.env) {
  if (isLocalDemoUserFallbackDisabledInProduction(env)) return false;
  if (!isLocalDevRequest(req, env)) return false;
  const normalized = normalizeAdminEmail(
    email || req?.memberstackEmail || req?.memberstackAuth?.email
  );
  if (!normalized) return false;
  if (normalized !== APPROVED_LOCAL_DEMO_EMAIL) return false;
  if (!APPROVED_LOCAL_DEALALITY_QA_ADMIN_EMAILS.includes(normalized)) return false;
  return true;
}

/**
 * Complete Dealality user object for middleware + admin helpers.
 * Compatible with requireDealalityUser consumers and hasLocalDealalityAdminAccess.
 *
 * @param {{
 *   memberstackId?: string|null,
 *   email?: string|null,
 *   req?: object,
 *   env?: NodeJS.ProcessEnv,
 * }} input
 * @returns {object|null}
 */
export function resolveLocalDemoDealalityUser(input = {}) {
  const req = input.req || {};
  const env = input.env || process.env;
  const emailHint =
    input.email ||
    req.memberstackEmail ||
    req.memberstackAuth?.email ||
    APPROVED_LOCAL_DEMO_EMAIL;

  if (!isEligibleForLocalDemoUserResolution(emailHint, req, env)) {
    return null;
  }

  const owner = MAP_DEMO_STAKEHOLDER_COMPANIES.Owner;
  const brand = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand;
  const companyIds = [owner.companyId, brand.companyId].filter(Boolean);
  const workspaceAccess = [...DEMO_PREVIEW_WORKSPACES, WORKSPACE_DEMO];

  return {
    email: APPROVED_LOCAL_DEMO_EMAIL,
    memberstackId: input.memberstackId || req.memberstackMemberId || null,
    firstName: LOCAL_DEMO_FIRST_NAME,
    lastName: LOCAL_DEMO_LAST_NAME,
    profilePhotoUrl: null,
    /** Distinguish from real Airtable Users — do not fake airtableUserFound. */
    userSource: LOCAL_DEMO_USER_SOURCE,
    airtableUserFound: false,
    userRecordId: null,
    role: "owner",
    primaryRole: "owner",
    legacyRole: "owner",
    workspaceAccess,
    flags: {
      isOwner: true,
      isOperator: true,
      isBrand: true,
      isDemo: true,
      isAdmin: false,
      isOwnerOperator: true,
    },
    companyType: "Demo",
    isOwnerOperator: true,
    canAccessOwnerWorkspace: true,
    canAccessOperatorWorkspace: true,
    canAccessBrandWorkspace: true,
    canAccessDemoWorkspace: true,
    isDemo: true,
    demoPreviewWorkspaces: [...DEMO_PREVIEW_WORKSPACES],
    roleRaw: "Demo",
    roleSource: LOCAL_DEMO_USER_SOURCE,
    companyId: owner.companyId,
    companyIds,
    companyProfileId: owner.companyId,
    companyName: owner.companyName,
    status: "active",
    isAdmin: false,
    isOwner: true,
    isBrand: true,
    isOperator: true,
    operatorDealRequestEligible: false,
    reviewBeforeOutreach: false,
    operatorExplorerEligible: false,
    thirdPartyManagementAvailable: false,
    activeWorkspace: WORKSPACE_OWNER,
    demoStakeholderMode: true,
    demoStakeholderWorkspaces: [...DEMO_PREVIEW_WORKSPACES],
    founderNavOverridesAvailable: true,
  };
}

/**
 * Apply the same demo stakeholder + portfolio enrichment used for real demo Users rows.
 * Mutates and returns the user for requireDealalityUser.
 *
 * @param {object} dealalityUser
 * @param {object} req
 */
export function attachLocalDemoRequestContext(dealalityUser, req) {
  if (!dealalityUser || dealalityUser.userSource !== LOCAL_DEMO_USER_SOURCE) {
    return dealalityUser;
  }
  dealalityUser.demoStakeholderWorkspaces = [...DEMO_PREVIEW_WORKSPACES];
  applyDemoStakeholderActiveWorkspace(dealalityUser, readActiveWorkspaceHeader(req));
  dealalityUser.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealalityUser);
  applyDemoBrandPortfolioContext(dealalityUser, readDemoBrandPortfolioHeader(req));
  dealalityUser.canonicalWorkspaceOptions = resolveWorkspaceOptions(dealalityUser);
  return dealalityUser;
}

/**
 * Full /api/me success payload for LOCAL_DEMO (no Airtable Users row).
 *
 * @param {{
 *   memberstackId: string,
 *   email?: string|null,
 *   req: object,
 *   tokenPayload?: object,
 *   matchedBy?: string,
 *   env?: NodeJS.ProcessEnv,
 * }} input
 */
export function buildLocalDemoMeResponse(input) {
  const req = input.req || {};
  const env = input.env || process.env;
  const base = resolveLocalDemoDealalityUser({
    memberstackId: input.memberstackId,
    email: input.email || APPROVED_LOCAL_DEMO_EMAIL,
    req,
    env,
  });
  if (!base) return null;

  let dealality = {
    role: base.role,
    primaryRole: base.primaryRole,
    legacyRole: base.legacyRole,
    roleRaw: base.roleRaw,
    roleSource: base.roleSource,
    userRoleRaw: base.roleRaw,
    companyType: base.companyType,
    companyTypeRaw: base.companyType,
    companyProfileId: base.companyProfileId,
    companyIds: base.companyIds,
    companyName: base.companyName,
    profilePhotoUrl: null,
    workspaceAccess: base.workspaceAccess,
    flags: { ...base.flags },
    isOwner: base.isOwner,
    isBrand: base.isBrand,
    isOperator: base.isOperator,
    isDemo: true,
    isAdmin: false,
    isOwnerOperator: true,
    isInternalRunbookAdmin: false,
    canAccessOwnerWorkspace: true,
    canAccessOperatorWorkspace: true,
    canAccessBrandWorkspace: true,
    canAccessDemoWorkspace: true,
    demoPreviewWorkspaces: [...DEMO_PREVIEW_WORKSPACES],
    demoStakeholderWorkspaces: [...DEMO_PREVIEW_WORKSPACES],
    demoStakeholderMode: true,
    thirdPartyManagementAvailable: false,
    activeWorkspace: WORKSPACE_OWNER,
    operatorExplorerEligible: false,
    operatorDealRequestEligible: false,
    reviewBeforeOutreach: false,
    userSource: LOCAL_DEMO_USER_SOURCE,
    airtableUserFound: false,
  };

  dealality = enrichDealalityMeForDemoStakeholder(dealality, {
    companyIds: base.companyIds,
    requestedWorkspace: readActiveWorkspaceHeader(req),
  });

  const portfolioProbe = {
    ...dealality,
    companyIds: base.companyIds,
    isAdmin: false,
    flags: dealality.flags,
  };
  applyDemoBrandPortfolioContext(portfolioProbe, readDemoBrandPortfolioHeader(req));
  dealality.demoBrandPortfolioSwitchAvailable = canUseDemoBrandPortfolioSwitch(portfolioProbe);
  dealality.demoBrandPortfolioOptions = listDemoBrandPortfolioOptions();
  dealality.demoBrandPortfolioKey = portfolioProbe.demoBrandPortfolioKey || null;
  dealality.demoBrandPortfolioLabel =
    portfolioProbe.demoBrandPortfolio?.canonicalCompanyName || null;
  dealality.canonicalWorkspaceOptions = resolveWorkspaceOptions({
    ...dealality,
    companyIds: base.companyIds,
  });

  const adminProbe = {
    ...dealality,
    email: APPROVED_LOCAL_DEMO_EMAIL,
    companyIds: base.companyIds,
  };
  // Local demo is never platform isAdmin; ADP admin tools use local elevation only.
  dealality.localDealalityAdminAccess = hasLocalDealalityAdminAccess(adminProbe, req, env);
  dealality.adpMonthlyReviewAdmin = dealality.localDealalityAdminAccess === true;
  dealality.founderNavOverridesAvailable = true;

  const tokenPayload =
    input.tokenPayload && typeof input.tokenPayload === "object" ? input.tokenPayload : {};

  return {
    success: true,
    memberstackId: input.memberstackId,
    memberstack: {
      id: input.memberstackId,
      tokenIssuedAt: tokenPayload.iat != null ? tokenPayload.iat : null,
      tokenExpiresAt: tokenPayload.exp != null ? tokenPayload.exp : null,
    },
    user: {
      email: APPROVED_LOCAL_DEMO_EMAIL,
      firstName: LOCAL_DEMO_FIRST_NAME,
      lastName: LOCAL_DEMO_LAST_NAME,
      profilePhotoUrl: null,
    },
    airtable: {
      userRecordId: null,
      airtableUserId: null,
      email: APPROVED_LOCAL_DEMO_EMAIL,
      firstName: LOCAL_DEMO_FIRST_NAME,
      lastName: LOCAL_DEMO_LAST_NAME,
      profilePhotoUrl: null,
      companyProfileId: base.companyProfileId,
      companyName: base.companyName,
      userSource: LOCAL_DEMO_USER_SOURCE,
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
    dealality,
    accountAccess: {
      state: "active",
      pendingApproval: false,
      accountStatus: "active",
      userTitle: null,
      userMessage: null,
      suppressBrandAssignmentToast: true,
    },
    meta: {
      usersTable: null,
      memberstackMatchFields: [],
      brandBasicsLinkField: null,
      operatorSetupLinkField: null,
      regionsField: null,
      operatorMappingStatus: "local_demo_no_user_record",
      matchedBy: input.matchedBy || "local_demo_fallback",
      warnings: [
        DEALALITY_LOCAL_DEMO_USER_RESOLUTION,
        LOCAL_DEMO_USER_SOURCE,
        "airtable_users_row_absent_local_demo_resolved",
      ],
      userSource: LOCAL_DEMO_USER_SOURCE,
      airtableUserFound: false,
      doctrine: {
        DEALALITY_LOCAL_DEMO_USER_RESOLUTION,
        LOCAL_DEMO_USER_FALLBACK_DISABLED_IN_PRODUCTION,
        SINGLE_CANONICAL_LOCAL_DEMO_USER_CONTEXT,
        LOCAL_DEMO_IDENTITY_RESOLUTION_NAV_PAGE_API_PARITY,
      },
    },
  };
}
