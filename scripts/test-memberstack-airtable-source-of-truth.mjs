/**
 * Memberstack identity vs Airtable authorization — unit tests (no Airtable API).
 *   node scripts/test-memberstack-airtable-source-of-truth.mjs
 */
import assert from "node:assert/strict";
import { mergeUserAndCompanyFields } from "../lib/dealality/resolve-user.js";
import {
  buildDealalityAccessContext,
  canAccessBrandWorkspace,
  canAccessDemoWorkspace,
  canAccessOperatorWorkspace,
  canAccessOwnerWorkspace,
  getPrimaryRoleFromFlags,
  getLegacyRoleFromFlags,
  getWorkspaceFlags,
} from "../lib/company-workspace-access.js";
import {
  buildMemberstackSyncPatch,
  MS_SYNC_NEVER_WRITE,
} from "../lib/memberstack/upsert-member-identity-to-airtable.js";
import {
  memberstackRoleHintConflictWarnings,
  workspacesImpliedByMemberstackRoleHint,
} from "../lib/memberstack/memberstack-role-hint.js";

// --- Airtable wins over Users row for workspace (company profile) ---
const companyOo = {
  "Company Type": "Hotel Owner - Operator",
  "Workspace Access": ["Owner", "Operator"],
};
const userOperatorOnly = {
  "Platform Role": "Operator",
  "Workspace Access": ["Operator"],
};
const mergedOo = mergeUserAndCompanyFields(userOperatorOnly, companyOo);
const ctxOo = buildDealalityAccessContext(mergedOo);
assert.deepEqual(ctxOo.workspaceAccess, ["Owner", "Operator"]);
assert.equal(ctxOo.primaryRole, "owner-operator");
assert.equal(ctxOo.legacyRole, "owner");
assert.equal(ctxOo.flags.isOwnerOperator, true);
assert.equal(ctxOo.flags.isOwner, true);
assert.equal(ctxOo.flags.isOperator, true);

// --- Owner only ---
const mergedOwner = mergeUserAndCompanyFields(
  { "Platform Role": "owner" },
  { "Workspace Access": ["Owner"] }
);
assert.deepEqual(buildDealalityAccessContext(mergedOwner).workspaceAccess, ["Owner"]);
assert.equal(getPrimaryRoleFromFlags(getWorkspaceFlags(mergedOwner)), "owner");

// --- Operator only ---
const mergedOp = mergeUserAndCompanyFields(
  {},
  { "Workspace Access": ["Operator"], "Company Type": "Hotel Management Company" }
);
assert.deepEqual(buildDealalityAccessContext(mergedOp).workspaceAccess, ["Operator"]);

// --- Demo only ---
const demoOnly = { "Workspace Access": ["Demo"] };
const demoCtx = buildDealalityAccessContext(demoOnly);
assert.equal(demoCtx.isDemo, true);
assert.equal(demoCtx.flags.isDemo, true);
assert.equal(demoCtx.flags.isAdmin, false);
assert.equal(canAccessDemoWorkspace(demoOnly), true);
assert.equal(canAccessOwnerWorkspace(demoOnly), false);
assert.equal(canAccessOperatorWorkspace(demoOnly), false);
assert.equal(canAccessBrandWorkspace(demoOnly), false);
assert.deepEqual(demoCtx.demoPreviewWorkspaces, ["Owner", "Operator", "Brand"]);

// --- Demo + Owner ---
const demoOwner = { "Workspace Access": ["Demo", "Owner"] };
const demoOwnerCtx = buildDealalityAccessContext(demoOwner);
assert.equal(demoOwnerCtx.isDemo, true);
assert.equal(demoOwnerCtx.flags.isOwner, true);
assert.equal(canAccessOwnerWorkspace(demoOwner), true);
assert.equal(canAccessOperatorWorkspace(demoOwner), false);

// --- Demo + Operator ---
const demoOp = { "Workspace Access": ["Demo", "Operator"] };
assert.equal(buildDealalityAccessContext(demoOp).flags.isOperator, true);
assert.equal(canAccessOperatorWorkspace(demoOp), true);

// --- Memberstack role hint conflict ---
assert.deepEqual(workspacesImpliedByMemberstackRoleHint("Operator"), ["Operator"]);
const conflict = memberstackRoleHintConflictWarnings("Operator", ["Owner"]);
assert.ok(conflict.includes("memberstack_role_hint_conflicts_with_airtable_workspace"));

// --- Sync must not overwrite protected fields ---
const existingUser = {
  "Workspace Access": ["Owner", "Operator"],
  "User Type": "Hotel Owner - Operator",
  "Company Profile": ["reccQJUKO2RAY9zhE"],
};
const msPatch = {
  "Workspace Access": ["Operator"],
  "User Type": "Operator",
  "Company Profile": ["recOTHER000000000"],
  Email: "demo@dealality.com",
  "Unique Webflow ID": "mem_sb_test",
};
const built = buildMemberstackSyncPatch(existingUser, msPatch);
assert.ok(!("Workspace Access" in built.patch));
assert.ok(!("Company Profile" in built.patch));
assert.ok(!("User Type" in built.patch));
assert.equal(built.patch.Email, "demo@dealality.com");
assert.ok(MS_SYNC_NEVER_WRITE.has("Workspace Access"));

// --- Fill-if-blank: new User Type when empty ---
const blankRole = buildMemberstackSyncPatch({ "User Type": "" }, { "User Type": "Operator" });
assert.equal(blankRole.patch["User Type"], "Operator");

console.log("test-memberstack-airtable-source-of-truth: ok");
