#!/usr/bin/env node
/**
 * Pilot user verification by email — provisioning readiness checks.
 *
 * Usage:
 *   node scripts/verify-pilot-user-by-email.mjs --email pilot@example.com
 *   node scripts/verify-pilot-user-by-email.mjs --email pilot@example.com --allow-pending
 *   node scripts/verify-pilot-user-by-email.mjs --email pilot@example.com --allow-test-memberstack-id
 */
import "../load-env.js";
import Airtable from "airtable";
import axios from "axios";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";
import { cellToString, extractLinkedRecordIds } from "../lib/airtable-utils.js";
import {
  MEMBERSTACK_MEMBER_ID_FIELD_NAMES,
  DEALS_COMPANY_LINK_FIELD,
  getUsersStatusFieldCandidates,
  memberstackIdLabel,
  memberstackSlugLabel,
  workspaceAccessSourceLabel,
  COMPANY_WORKSPACE_ACCESS_FIELD,
} from "../lib/pilot-provisioning/pilot-field-registry.js";
import {
  readMemberstackIdsFromUserFields,
  validateMemberstackIdPair,
  validateWorkspaceAccessSource,
  detectUsersAccountStatus,
  validateAccountStatus,
  classifyDealAccessPath,
  detectDealsCompanyProfileField,
  isTestMemberstackId,
} from "../lib/pilot-provisioning/pilot-validators.js";

const USERS_TABLE = process.env.AIRTABLE_INTAKE_USERS_TABLE || "tbl6shiyz2wdUqE5F";
const CP_TABLE = process.env.AIRTABLE_COMPANY_PROFILE_TABLE || "tblItyfH6MlOnMKZ9";
const DEALS_TABLE = process.env.AIRTABLE_INTAKE_DEALS_TABLE || "tblbvSxjiIhXzW6XW";

function parseArgs(argv) {
  const out = {
    email: null,
    allowPending: false,
    allowTestMemberstackId: false,
  };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--email" && argv[i + 1]) out.email = String(argv[++i]).trim().toLowerCase();
    else if (argv[i] === "--allow-pending") out.allowPending = true;
    else if (argv[i] === "--allow-test-memberstack-id") out.allowTestMemberstackId = true;
  }
  return out;
}

function escapeFormula(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function findMemberstackByEmail(apiKey, email) {
  const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(/\/$/, "");
  const headers = { "X-API-KEY": apiKey, "Content-Type": "application/json" };
  let after;
  for (let page = 0; page < 100; page++) {
    const params = { limit: 100, order: "ASC" };
    if (after != null) params.after = after;
    const res = await axios.get(`${BASE}/members`, { headers, params, validateStatus: () => true });
    if (res.status !== 200) {
      throw new Error(`Memberstack list HTTP ${res.status}: ${JSON.stringify(res.data).slice(0, 200)}`);
    }
    const batch = res.data?.data || [];
    const found = batch.find((m) => memberEmail(m) === email);
    if (found) return found;
    if (!res.data?.hasNextPage) break;
    after = res.data?.endCursor;
    if (after == null) break;
  }
  return null;
}

function memberEmail(member) {
  return (
    (typeof member?.auth?.email === "string" && member.auth.email.trim().toLowerCase()) ||
    (typeof member?.email === "string" && member.email.trim().toLowerCase()) ||
    ""
  );
}

async function analyzePilotDeals(base, userRecordId, companyIds) {
  const rows = await base(DEALS_TABLE).select({ pageSize: 100 }).all();
  const allowed = [];
  const userOnly = [];
  const missingCompanyProfile = [];
  const companyLinked = [];

  for (const rec of rows) {
    const paths = classifyDealAccessPath(rec.fields, userRecordId, companyIds);
    const dealalityUser = {
      isAdmin: false,
      isOwner: true,
      userRecordId,
      companyId: companyIds[0] || null,
      companyIds,
    };
    if (!dealRecordAllowedForUser(rec.fields, dealalityUser)) continue;

    allowed.push({ id: rec.id, name: cellToString(rec.fields?.Name) || rec.id, paths });

    if (paths.viaUser && !paths.viaCompany) userOnly.push(rec.id);
    if (!paths.dealCompanyIds.length) missingCompanyProfile.push(rec.id);
    if (paths.viaCompany) companyLinked.push(rec.id);
  }

  return { allowed, userOnly, missingCompanyProfile, companyLinked };
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.email) {
    console.error(
      "Usage: node scripts/verify-pilot-user-by-email.mjs --email <email> [--allow-pending] [--allow-test-memberstack-id]"
    );
    process.exit(1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const lit = escapeFormula(args.email);
  const rows = await base(USERS_TABLE)
    .select({ filterByFormula: `LOWER({Email}) = '${lit}'`, maxRecords: 1 })
    .firstPage();

  console.log("=== Verify pilot user ===");
  console.log("email:", args.email);
  console.log("statusFieldCandidates:", getUsersStatusFieldCandidates().join(", "));
  console.log("memberstackIdFields:", memberstackIdLabel(), "+", memberstackSlugLabel());
  console.log("workspaceAccessSource:", workspaceAccessSourceLabel());

  if (!rows.length) {
    console.log("FAIL: No Airtable Users row");
    process.exit(1);
  }

  const userRec = rows[0];
  const f = userRec.fields || {};
  const msIds = readMemberstackIdsFromUserFields(f);
  const cpIds = extractLinkedRecordIds(f["Company Profile"]);
  const statusInfo = detectUsersAccountStatus(f);

  const problems = [];
  const warnings = [];

  const msValidation = validateMemberstackIdPair(msIds, {
    allowTestId: args.allowTestMemberstackId,
  });
  problems.push(...msValidation.problems);
  warnings.push(...msValidation.warnings);

  const statusValidation = validateAccountStatus(statusInfo, { allowPending: args.allowPending });
  problems.push(...statusValidation.problems);
  warnings.push(...statusValidation.warnings);

  console.log("\n--- Airtable Users ---");
  console.log(
    JSON.stringify(
      {
        recordId: userRec.id,
        email: cellToString(f.Email),
        firstName: cellToString(f["First Name"]),
        lastName: cellToString(f["Last Name"]),
        accountStatusField: statusInfo.fieldName,
        accountStatus: statusInfo.value,
        platformRole: cellToString(f["Platform Role"]) || cellToString(f["User Type"]) || cellToString(f.Role),
        memberstackMemberId: msIds.primary,
        memberstackMemberIdMirror: msIds.mirror,
        memberstackIdFieldsMatch: msIds.primary && msIds.primary === msIds.mirror,
        isTestMemberstackId: msIds.primary ? isTestMemberstackId(msIds.primary) : false,
        companyProfileIds: cpIds,
        dealsLinkedOnUser: Array.isArray(f.Deals) ? f.Deals.length : 0,
      },
      null,
      2
    )
  );

  if (!cpIds.length) problems.push("missing_company_profile_link");

  let primaryCompanyFields = null;
  for (const cpId of cpIds) {
    const cp = await base(CP_TABLE).find(cpId);
    const cf = cp.fields || {};
    if (!primaryCompanyFields) primaryCompanyFields = cf;
    const wsCheck = validateWorkspaceAccessSource(f, cf);
    if (cpId === cpIds[0]) {
      problems.push(...wsCheck.problems);
      warnings.push(...wsCheck.warnings);
    }
    console.log("\n--- Company Profile ---");
    console.log(
      JSON.stringify(
        {
          recordId: cp.id,
          companyName: cellToString(cf["Company Name"]),
          workspaceAccessSource: wsCheck.workspaceAccessSource || workspaceAccessSourceLabel(),
          workspaceAccessField: COMPANY_WORKSPACE_ACCESS_FIELD,
          workspaceAccess: cf[COMPANY_WORKSPACE_ACCESS_FIELD] || cf["Workspace Access"] || [],
          companyType: cellToString(cf["Company Type"]),
          companyTypeTags: cf["Company Type Tags"] || [],
        },
        null,
        2
      )
    );
  }

  const dealsFieldProbe = await detectDealsCompanyProfileField(base, DEALS_TABLE, DEALS_COMPANY_LINK_FIELD, {
    apiKey,
    baseId,
  });
  console.log("\n--- Deals table schema probe ---");
  console.log(JSON.stringify(dealsFieldProbe, null, 2));
  if (dealsFieldProbe.present === false) {
    warnings.push("deals_table_missing_company_profile_field");
  }

  const resolved = await resolveDealalityUser({ email: args.email, memberstackId: msIds.primary || undefined });
  console.log("\n--- resolveDealalityUser ---");
  console.log(
    JSON.stringify(
      {
        found: resolved.found,
        reason: resolved.reason,
        userRecordId: resolved.userRecordId,
        isAdmin: resolved.isAdmin,
        isOwner: resolved.isOwner,
        workspaceAccess: resolved.workspaceAccess,
        legacyRole: resolved.legacyRole,
        primaryRole: resolved.primaryRole,
        companyName: resolved.companyName,
        accessWarnings: resolved.accessWarnings,
      },
      null,
      2
    )
  );

  if (!resolved.found) problems.push(`resolve_failed:${resolved.reason}`);
  if (resolved.found && !resolved.isAdmin && !resolved.canAccessOwnerWorkspace) {
    problems.push("no_owner_workspace_access");
  }

  if (resolved.found && (resolved.isOwner || resolved.isAdmin)) {
    const dealAnalysis = await analyzePilotDeals(base, resolved.userRecordId, resolved.companyIds || cpIds);
    console.log("\n--- My Deals scope ---");
    console.log(
      JSON.stringify(
        {
          allowedDealCount: dealAnalysis.allowed.length,
          viaUserIdOnlyCount: dealAnalysis.userOnly.length,
          missingDealCompanyProfileCount: dealAnalysis.missingCompanyProfile.length,
          viaCompanyProfileCount: dealAnalysis.companyLinked.length,
          userIdOnlyDealIds: dealAnalysis.userOnly,
          missingCompanyProfileDealIds: dealAnalysis.missingCompanyProfile,
        },
        null,
        2
      )
    );

    if (!resolved.isAdmin && dealAnalysis.allowed.length === 0) problems.push("zero_my_deals");
    if (dealAnalysis.userOnly.length) warnings.push("pilot_deals_visible_via_user_id_only");
    if (dealsFieldProbe.present !== false && dealAnalysis.missingCompanyProfile.length) {
      warnings.push("pilot_deals_missing_company_profile_link");
    }
  }

  const msKey = (
    process.env.MEMBERSTACK_LIVE_SECRET_KEY ||
    process.env.MEMBERSTACK_SECRET_KEY ||
    ""
  ).trim();
  if (msKey) {
    const msMember = await findMemberstackByEmail(msKey, args.email);
    console.log("\n--- Memberstack (live API) ---");
    if (!msMember) {
      console.log("member: NOT FOUND");
      problems.push("memberstack_member_not_found");
    } else {
      console.log(
        JSON.stringify(
          {
            id: msMember.id,
            email: memberEmail(msMember),
            verified: msMember.verified,
            isTestModeId: isTestMemberstackId(msMember.id),
            plans: (msMember.planConnections || []).map((p) => p.planId || p.id).filter(Boolean),
          },
          null,
          2
        )
      );
      if (msIds.primary && msMember.id !== msIds.primary) {
        problems.push(`memberstack_id_mismatch:airtable=${msIds.primary},memberstack=${msMember.id}`);
      }
      if (isTestMemberstackId(msMember.id) && !args.allowTestMemberstackId) {
        warnings.push("live_memberstack_api_returned_test_mode_id");
      }
    }
  } else {
    console.log("\n--- Memberstack ---");
    console.log("skipped: no MEMBERSTACK_SECRET_KEY");
  }

  console.log("\n=== Summary ===");
  if (warnings.length) console.log("WARNINGS:", [...new Set(warnings)].join(", "));
  if (problems.length) {
    console.log("PROBLEMS:", [...new Set(problems)].join(", "));
    process.exit(2);
  }
  console.log("OK: user appears properly linked and configured for pilot");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
