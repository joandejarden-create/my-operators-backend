/**
 * GET|POST /api/me — Phase A: verify Memberstack JWT, resolve Airtable Users row, return permissions context.
 *
 * Env (optional unless noted):
 * - MEMBERSTACK_APP_ID — Memberstack app id (audience); recommended for verifyToken.
 * - AIRTABLE_ME_USERS_TABLE — Users table id or name (default: same as intake, tbl6shiyz2wdUqE5F).
 * - AIRTABLE_ME_USERS_MEMBERSTACK_FIELDS — comma-separated field names OR field ids for Memberstack member id (default: intake Unique_Webflow_ID). Add a second field (e.g. slug) if ids live there instead.
 * - AIRTABLE_ME_USERS_BRAND_BASICS_LINK — linked field on Users → Brand Basics (default: Brand Setup - Brand Basics).
 * - AIRTABLE_ME_USERS_REGIONS_FIELD — field on Users for allowed regions (default: HO - PI - Regions Where You Operate / Invest).
 * - AIRTABLE_BRAND_SETUP_BASICS_TABLE — table for Brand Name lookup (default: Brand Setup - Brand Basics).
 * - AIRTABLE_BRAND_NAME_FIELD — field on Brand Basics (default: Brand Name).
 *
 * Memberstack Admin init expects a secret for other admin methods; verifyToken uses JWKS and does not need the secret for signature check.
 */

import Airtable from "airtable";
import { verifyMemberstackToken } from "../lib/memberstack/verify-token.js";
import { shouldSyncMemberstackIdToUsersRow } from "../lib/memberstack/environment.js";

import {
  INTAKE_USERS_TABLE,
  INTAKE_USERS_EMAIL,
  INTAKE_USERS_FIRST_NAME,
  INTAKE_USERS_LAST_NAME,
  INTAKE_USERS_UNIQUE_WEBFLOW_ID,
} from "./schemas/intake-deal-fields.js";
import { extractLinkedRecordIds, cellToString } from "../lib/airtable-utils.js";
import { roleInfoFromUserFieldsAsync } from "../lib/dealality/resolve-user.js";
import { resolveOperatorScope, MAP_OPERATOR_SCOPE } from "../lib/dealality/resolve-operator-scope.js";
import { resolveAccountAccessStatus } from "../lib/dealality/account-access-status.js";
import { isInternalRunbookAdmin } from "../lib/dealality/internal-runbook-admin.js";
import {
  enrichDealalityMeForDemoStakeholder,
  readActiveWorkspaceHeader,
} from "../lib/dealality/demo-stakeholder-workspace.js";
import {
  applyDemoBrandPortfolioContext,
  listDemoBrandPortfolioOptions,
  readDemoBrandPortfolioHeader,
  canUseDemoBrandPortfolioSwitch,
} from "../lib/dealality/demo-brand-portfolio-context.js";
import { resolveWorkspaceOptions } from "../lib/dealality/resolve-workspace-options.js";
import {
  emailFromUserFields,
  profilePhotoUrlFromFields,
  PUF,
} from "../lib/airtable/platform-users-table.js";

const BRAND_BASICS_TABLE = process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";
const BRAND_NAME_FIELD = process.env.AIRTABLE_BRAND_NAME_FIELD || "Brand Name";
const USERS_COMPANY_FIELD =
  process.env.AIRTABLE_USERS_COMPANY_LINK_FIELD || "Company Profile";
const COMPANY_PROFILE_TABLE =
  process.env.AIRTABLE_COMPANY_PROFILE_TABLE || "Company Profile";
const COMPANY_NAME_FIELD =
  process.env.AIRTABLE_COMPANY_PROFILE_NAME_FIELD || "Company Name";

const USERS_TABLE = process.env.AIRTABLE_ME_USERS_TABLE || INTAKE_USERS_TABLE;
const BRAND_LINK_FIELD =
  process.env.AIRTABLE_ME_USERS_BRAND_BASICS_LINK || "Brand Setup - Brand Basics";
const REGIONS_FIELD =
  process.env.AIRTABLE_ME_USERS_REGIONS_FIELD || "HO - PI - Regions Where You Operate / Invest";

const MEMBERSTACK_MATCH_FIELDS = (process.env.AIRTABLE_ME_USERS_MEMBERSTACK_FIELDS || INTAKE_USERS_UNIQUE_WEBFLOW_ID)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

function airtableStringLiteral(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function bearerToken(req) {
  const raw = req.headers.authorization || req.headers.Authorization;
  if (!raw || typeof raw !== "string") return null;
  const m = raw.match(/^Bearer\s+(\S+)$/i);
  return m ? m[1] : null;
}

/** Normalize Airtable cell values to display strings (multi-select, lookup, single string). */
function cellToStringList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) {
    const out = [];
    for (const item of value) {
      if (item == null) continue;
      if (typeof item === "string") {
        const t = item.trim();
        if (t) out.push(t);
      } else if (typeof item === "number" && !Number.isNaN(item)) {
        out.push(String(item));
      } else if (typeof item === "object" && item !== null && typeof item.name === "string") {
        const t = item.name.trim();
        if (t) out.push(t);
      }
    }
    return out;
  }
  if (typeof value === "string") {
    const t = value.trim();
    return t ? [t] : [];
  }
  if (typeof value === "number" && !Number.isNaN(value)) return [String(value)];
  if (typeof value === "object" && typeof value.name === "string") {
    const t = value.name.trim();
    return t ? [t] : [];
  }
  return [];
}

async function fetchBrandNamesForLinkedIds(base, linkedIds) {
  const ids = (linkedIds || []).filter(Boolean);
  if (ids.length === 0) return { names: [], recordIds: [] };

  const unique = [...new Set(ids)];
  const names = [];
  const recordIds = [];

  const batchSize = 8;
  for (let i = 0; i < unique.length; i += batchSize) {
    const chunk = unique.slice(i, i + batchSize);
    const orParts = chunk.map((id) => `RECORD_ID() = '${airtableStringLiteral(id)}'`);
    const formula = `OR(${orParts.join(",")})`;
    const rows = await base(BRAND_BASICS_TABLE)
      .select({ filterByFormula: formula, maxRecords: chunk.length })
      .firstPage();
    for (const row of rows) {
      recordIds.push(row.id);
      const nm = row.fields && row.fields[BRAND_NAME_FIELD];
      const str = cellToStringList(nm)[0] || "";
      if (str) names.push(str);
    }
  }

  return { names: [...new Set(names)], recordIds: [...new Set(recordIds)] };
}

function buildUserLookupFormula(memberstackId) {
  const lit = airtableStringLiteral(memberstackId);
  const parts = MEMBERSTACK_MATCH_FIELDS.map((field) => `{${field}} = '${lit}'`);
  if (parts.length === 1) return parts[0];
  return `OR(${parts.join(",")})`;
}

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

async function findUserByEmail(base, email) {
  const lit = airtableStringLiteral(email.toLowerCase());
  const fields = [INTAKE_USERS_EMAIL, "Email"];
  for (let i = 0; i < fields.length; i++) {
    const formula = `LOWER({${fields[i]}}) = '${lit}'`;
    const rows = await base(USERS_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
    if (rows.length) return rows;
  }
  return [];
}

async function getMe(req, res) {
  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ success: false, error: "method_not_allowed" });
  }

  const token = bearerToken(req);
  if (!token) {
    return res.status(401).json({
      success: false,
      error: "authentication_required",
      message: "Send Authorization: Bearer <Memberstack member JWT>.",
    });
  }

  let verified;
  try {
    verified = await verifyMemberstackToken(token);
  } catch (err) {
    const msg = (err && err.message) || String(err);
    const expired = /expired|exp/i.test(msg);
    return res.status(401).json({
      success: false,
      error: expired ? "token_expired" : "invalid_token",
      message: expired ? "Token expired." : "Invalid or unverifiable Memberstack token.",
    });
  }

  const memberstackId = verified && verified.id;
  if (!memberstackId || typeof memberstackId !== "string") {
    return res.status(401).json({
      success: false,
      error: "invalid_token_payload",
      message: "Verified token did not include a member id.",
    });
  }

  const emailFromToken =
    (verified.email && String(verified.email).trim().toLowerCase()) ||
    pickEmailFromPayload(verified.raw);

  const tokenPayload =
    verified.raw && typeof verified.raw === "object" ? verified.raw : {};

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    return res.status(500).json({ success: false, error: "server_misconfigured" });
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const formula = buildUserLookupFormula(memberstackId);

  let userRows;
  let matchedBy = "memberstack_id";
  try {
    userRows = await base(USERS_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
    if (!userRows.length && emailFromToken) {
      userRows = await findUserByEmail(base, emailFromToken);
      if (userRows.length) matchedBy = "email";
    }
  } catch (err) {
    console.error("[api/me] Airtable user lookup error:", err.message);
    return res.status(500).json({
      success: false,
      error: "airtable_error",
      message: err.message || "Airtable lookup failed.",
    });
  }

  if (!userRows.length) {
    return res.status(404).json({
      success: false,
      error: "user_not_found",
      message: "No Users row matched this Memberstack id for the configured fields.",
      memberstackId,
      email: emailFromToken || null,
      hint: "Confirm Users row has Memberstack id in slug / Unique Webflow ID, or email matches the signed-in Memberstack account.",
    });
  }

  const rec = userRows[0];
  const fields = rec.fields || {};
  const warnings = [];
  if (matchedBy === "email") {
    warnings.push("user_matched_by_email");
    const existingMsId =
      cellToStringList(fields[INTAKE_USERS_UNIQUE_WEBFLOW_ID])[0] ||
      cellToStringList(fields["Unique Webflow ID"])[0] ||
      cellToStringList(fields["Unique_Webflow_ID"])[0] ||
      cellToStringList(fields.Slug)[0] ||
      "";
    if (!shouldSyncMemberstackIdToUsersRow(existingMsId, memberstackId)) {
      warnings.push("memberstack_id_sync_skipped_preserve_live_id");
    } else {
      try {
        const syncFields = { [INTAKE_USERS_UNIQUE_WEBFLOW_ID]: memberstackId };
        const slugFieldId = process.env.AIRTABLE_USERS_SLUG_FIELD || "fldEgbHu5MvfyrxgE";
        if (
          MEMBERSTACK_MATCH_FIELDS.includes("slug") ||
          MEMBERSTACK_MATCH_FIELDS.includes("Slug") ||
          MEMBERSTACK_MATCH_FIELDS.includes(slugFieldId)
        ) {
          syncFields[slugFieldId] = memberstackId;
        }
        await base(USERS_TABLE).update(rec.id, syncFields, { typecast: true });
        warnings.push("memberstack_id_synced_to_users_row");
      } catch (syncErr) {
        console.warn("[api/me] could not sync Memberstack id to Users row:", syncErr.message);
        warnings.push("memberstack_id_sync_failed");
      }
    }
  }

  const email =
    emailFromUserFields(fields) ||
    cellToStringList(fields[INTAKE_USERS_EMAIL])[0] ||
    null;
  const firstName =
    cellToStringList(fields[PUF.firstName])[0] ||
    cellToStringList(fields[INTAKE_USERS_FIRST_NAME])[0] ||
    null;
  const lastName =
    cellToStringList(fields[PUF.lastName])[0] ||
    cellToStringList(fields[INTAKE_USERS_LAST_NAME])[0] ||
    null;
  const profilePhotoUrl = profilePhotoUrlFromFields(fields) || null;
  const companyProfileIds = extractLinkedRecordIds(fields[USERS_COMPANY_FIELD]);
  const companyProfileId = companyProfileIds[0] || null;
  let companyName = null;
  if (companyProfileId) {
    try {
      const companyRec = await base(COMPANY_PROFILE_TABLE).find(companyProfileId);
      companyName =
        cellToString(companyRec.fields && companyRec.fields[COMPANY_NAME_FIELD]) || null;
    } catch (companyErr) {
      console.warn("[api/me] company name lookup failed:", companyErr.message);
      warnings.push("company_profile_lookup_failed");
    }
  }
  let dealalityRole;
  try {
    dealalityRole = await roleInfoFromUserFieldsAsync(base, fields);
  } catch (roleErr) {
    console.warn("[api/me] role resolution failed:", roleErr.message);
    dealalityRole = {
      role: "unknown",
      roleRaw: null,
      roleSource: null,
      isAdmin: false,
      isOwner: false,
      isBrand: false,
      isOperator: false,
    };
    warnings.push("role_resolve_failed");
  }
  if (dealalityRole.roleSource === "company") {
    warnings.push("role_from_company_type");
  }
  if (Array.isArray(dealalityRole.warnings)) {
    for (const w of dealalityRole.warnings) {
      if (w) warnings.push(w);
    }
  }

  const brandLinks = fields[BRAND_LINK_FIELD];
  const linkedBrandIds = Array.isArray(brandLinks) ? brandLinks.filter(Boolean) : [];
  if (!linkedBrandIds.length && brandLinks != null) {
    warnings.push("brand_link_empty_or_unexpected_shape");
  }

  let allowedBrandNames = [];
  let allowedBrandRecordIds = [];
  try {
    const brandRes = await fetchBrandNamesForLinkedIds(base, linkedBrandIds);
    allowedBrandNames = brandRes.names;
    allowedBrandRecordIds = brandRes.recordIds;
    if (linkedBrandIds.length && !allowedBrandNames.length) {
      warnings.push("brand_basics_linked_but_brand_name_missing");
    }
  } catch (e) {
    console.warn("[api/me] brand name resolution failed:", e.message);
    warnings.push("brand_name_resolve_failed");
  }

  const allowedRegions = cellToStringList(fields[REGIONS_FIELD]);
  if (!allowedRegions.length && fields[REGIONS_FIELD] != null && fields[REGIONS_FIELD] !== "") {
    warnings.push("regions_field_present_but_unparsed");
  }

  if (!process.env.MEMBERSTACK_APP_ID) {
    warnings.push("memberstack_app_id_not_set_audience_not_validated");
  }

  let operatorScope = {
    allowedOperatingCompanyNames: [],
    allowedOperatorSetupIds: [],
    primaryOperatingCompanyName: null,
    mappingStatus: "no_user_record",
    warnings: [],
  };
  const operatorWorkspaceAllowed =
    dealalityRole.canAccessOperatorWorkspace ||
    dealalityRole.isOperator ||
    dealalityRole.isAdmin;
  if (operatorWorkspaceAllowed) {
    try {
      operatorScope = await resolveOperatorScope({
        userRecordId: rec.id,
        isAdmin: dealalityRole.isAdmin,
        isOperator: dealalityRole.isOperator || dealalityRole.canAccessOperatorWorkspace,
      });
      if (operatorScope.warnings?.length) {
        warnings.push(...operatorScope.warnings);
      }
    } catch (opErr) {
      console.warn("[api/me] operator scope resolution failed:", opErr.message);
      warnings.push("operator_scope_resolve_failed");
      operatorScope = {
        ...operatorScope,
        mappingStatus: "lookup_error",
        warnings: ["operator_scope_resolve_failed"],
      };
    }
  }

  const accountStatusField =
    (process.env.SIGNUP_AIRTABLE_STATUS_FIELD || "Account Status").trim();
  const accountStatusRaw = accountStatusField
    ? cellToString(fields[accountStatusField]) || null
    : null;
  const accountAccess = resolveAccountAccessStatus({
    dealalityRole,
    accountStatusRaw,
  });

  let dealality = {
    /** Legacy nav / old consumers — use workspaceAccess for permissions. */
    role: dealalityRole.role,
    primaryRole: dealalityRole.primaryRole || dealalityRole.role,
    legacyRole: dealalityRole.legacyRole || dealalityRole.role,
    roleRaw: dealalityRole.roleRaw,
    roleSource: dealalityRole.roleSource || null,
    userRoleRaw: dealalityRole.userRoleRaw || null,
    companyType: dealalityRole.companyType || dealalityRole.companyTypeRaw || null,
    companyTypeRaw: dealalityRole.companyTypeRaw || null,
    companyProfileId,
    companyIds: companyProfileIds,
    companyName,
    profilePhotoUrl,
    workspaceAccess: dealalityRole.workspaceAccess || [],
    flags: dealalityRole.flags || {
      isOwner: !!dealalityRole.isOwner,
      isOperator: !!dealalityRole.isOperator,
      isBrand: !!dealalityRole.isBrand,
      isDemo: !!dealalityRole.isDemo,
      isAdmin: !!dealalityRole.isAdmin,
      isOwnerOperator: !!dealalityRole.isOwnerOperator,
    },
    isOwner: dealalityRole.isOwner,
    isBrand: dealalityRole.isBrand,
    isOperator: dealalityRole.isOperator,
    isDemo: !!dealalityRole.isDemo,
    isAdmin: dealalityRole.isAdmin,
    isOwnerOperator: !!dealalityRole.isOwnerOperator,
    isInternalRunbookAdmin: isInternalRunbookAdmin({
      email,
      dealality: dealalityRole,
      companyName,
    }),
    canAccessOwnerWorkspace: !!dealalityRole.canAccessOwnerWorkspace,
    canAccessOperatorWorkspace: !!dealalityRole.canAccessOperatorWorkspace,
    canAccessBrandWorkspace: !!dealalityRole.canAccessBrandWorkspace,
    canAccessDemoWorkspace: !!dealalityRole.canAccessDemoWorkspace,
    demoPreviewWorkspaces: Array.isArray(dealalityRole.demoPreviewWorkspaces)
      ? dealalityRole.demoPreviewWorkspaces
      : [],
    demoStakeholderWorkspaces: [],
    demoStakeholderMode: false,
    thirdPartyManagementAvailable: !!dealalityRole.thirdPartyManagementAvailable,
    activeWorkspace: dealalityRole.activeWorkspace || null,
    operatorExplorerEligible: !!dealalityRole.operatorExplorerEligible,
    operatorDealRequestEligible: !!dealalityRole.operatorDealRequestEligible,
    reviewBeforeOutreach: !!dealalityRole.reviewBeforeOutreach,
    thirdPartyManagementAvailabilityStatus:
      dealalityRole.thirdPartyManagementAvailabilityStatus || null,
    eligibilitySource: dealalityRole.eligibilitySource || null,
  };

  dealality = enrichDealalityMeForDemoStakeholder(dealality, {
    companyIds: companyProfileIds,
    requestedWorkspace: readActiveWorkspaceHeader(req),
  });

  // Attach demo Brand Portfolio options + active key for workspace UI.
  const portfolioProbe = {
    ...dealality,
    companyIds: companyProfileIds,
    isAdmin: dealality.isAdmin,
    flags: dealality.flags,
  };
  applyDemoBrandPortfolioContext(portfolioProbe, readDemoBrandPortfolioHeader(req));
  dealality.demoBrandPortfolioSwitchAvailable = canUseDemoBrandPortfolioSwitch(portfolioProbe);
  dealality.demoBrandPortfolioOptions = listDemoBrandPortfolioOptions();
  dealality.demoBrandPortfolioKey = portfolioProbe.demoBrandPortfolioKey || null;
  dealality.demoBrandPortfolioLabel =
    portfolioProbe.demoBrandPortfolio?.canonicalCompanyName || null;

  // Re-assert canonical options after portfolio attach (portfolio must not shrink list).
  dealality.canonicalWorkspaceOptions = resolveWorkspaceOptions({
    ...dealality,
    companyIds: companyProfileIds,
  });

  return res.json({
    success: true,
    memberstackId,
    memberstack: {
      id: memberstackId,
      tokenIssuedAt: tokenPayload.iat != null ? tokenPayload.iat : null,
      tokenExpiresAt: tokenPayload.exp != null ? tokenPayload.exp : null,
    },
    user: {
      email,
      firstName,
      lastName,
      profilePhotoUrl,
    },
    airtable: {
      userRecordId: rec.id,
      airtableUserId: rec.id,
      email,
      firstName,
      lastName,
      profilePhotoUrl,
      companyProfileId: dealality.companyProfileId || companyProfileId,
      companyName: dealality.companyName || companyName,
    },
    permissions: {
      allowedBrandNames,
      allowedBrandRecordIds,
      allowedRegions,
      allowedOperatingCompanyNames: operatorScope.allowedOperatingCompanyNames || [],
      allowedOperatorSetupIds: operatorScope.allowedOperatorSetupIds || [],
      primaryOperatingCompanyName: operatorScope.primaryOperatingCompanyName || null,
    },
    dealality,
    accountAccess,
    meta: {
      usersTable: USERS_TABLE,
      memberstackMatchFields: MEMBERSTACK_MATCH_FIELDS,
      brandBasicsLinkField: BRAND_LINK_FIELD,
      operatorSetupLinkField: MAP_OPERATOR_SCOPE.usersOperatorSetupLink,
      regionsField: REGIONS_FIELD,
      operatorMappingStatus: operatorScope.mappingStatus || null,
      matchedBy,
      warnings: [...new Set(warnings)],
    },
  });
}

export { getMe };
