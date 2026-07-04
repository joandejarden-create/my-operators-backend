import assert from "node:assert/strict";
import {
  isOwnerOperatorCompanyTypeString,
  normalizeCompanyTypeToFilterKey,
} from "../lib/company-type-normalize.js";
import {
  buildDealalityAccessContext,
  canAccessDemoWorkspace,
  canAccessOperatorWorkspace,
  canAccessOwnerWorkspace,
  getLegacyRoleFromFlags,
  getPrimaryRoleFromFlags,
  getWorkspaceFlags,
  isOwnerOperatorCompany,
  isThirdPartyManagementAvailable,
  isThirdPartyManagementUnavailable,
  normalizeWorkspaceAccess,
  WORKSPACE_DEMO,
} from "../lib/company-workspace-access.js";

assert.equal(normalizeCompanyTypeToFilterKey("Hotel Owner - Operator"), "OWNER_OPERATOR");
assert.equal(isOwnerOperatorCompanyTypeString("Hotel Owner - Operator"), true);

const ownerFields = { "Company Type": "Hotel Owner" };
const ownerFlags = getWorkspaceFlags(ownerFields);
assert.equal(ownerFlags.isOwner, true);
assert.equal(ownerFlags.isDemo, false);
assert.deepEqual(normalizeWorkspaceAccess(ownerFields), ["Owner"]);

const demoOnly = { "Workspace Access": ["Demo"] };
const demoFlags = getWorkspaceFlags(demoOnly);
assert.equal(demoFlags.isDemo, true);
assert.equal(demoFlags.isAdmin, false);
assert.equal(demoFlags.isOwner, false);
assert.equal(demoFlags.isOperator, false);
assert.equal(demoFlags.isBrand, false);
assert.equal(canAccessDemoWorkspace(demoOnly), true);
assert.equal(canAccessOwnerWorkspace(demoOnly), false);
assert.equal(canAccessOperatorWorkspace(demoOnly), false);
const demoCtx = buildDealalityAccessContext(demoOnly);
assert.equal(demoCtx.isDemo, true);
assert.deepEqual(demoCtx.demoPreviewWorkspaces, ["Owner", "Operator", "Brand"]);

const demoOwner = { "Workspace Access": ["Demo", "Owner"] };
const demoOwnerFlags = getWorkspaceFlags(demoOwner);
assert.equal(demoOwnerFlags.isDemo, true);
assert.equal(demoOwnerFlags.isOwner, true);
assert.equal(canAccessOwnerWorkspace(demoOwner), true);

const demoOperator = { "Workspace Access": ["Demo", "Operator"] };
assert.equal(getWorkspaceFlags(demoOperator).isOperator, true);
assert.equal(canAccessOperatorWorkspace(demoOperator), true);

assert.equal(isThirdPartyManagementAvailable({ "Third-Party Management Availability": "Yes" }), true);
assert.equal(
  isThirdPartyManagementAvailable({ "Third-Party Management Availability": "Case-by-Case" }),
  true
);
assert.equal(
  isThirdPartyManagementAvailable({ "Third-Party Management Availability": "case-by-case" }),
  true
);
assert.equal(isThirdPartyManagementUnavailable({ "Third-Party Management Availability": "No" }), true);
assert.equal(
  isThirdPartyManagementAvailable({ "Third-Party Management Availability": "Unknown / To Confirm" }),
  false
);

assert.equal(normalizeWorkspaceAccess({ "Workspace Access": [WORKSPACE_DEMO] })[0], "Demo");

const ownerOperatorCompany = {
  "Company Type": "Hotel Owner - Operator",
  "Workspace Access": ["Owner", "Operator"],
};
const ooFlags = getWorkspaceFlags(ownerOperatorCompany);
assert.equal(ooFlags.isOwner, true);
assert.equal(ooFlags.isOperator, true);
assert.equal(getPrimaryRoleFromFlags(ooFlags), "owner-operator");
assert.equal(getLegacyRoleFromFlags(ooFlags), "owner");
assert.deepEqual(normalizeWorkspaceAccess(ownerOperatorCompany), ["Owner", "Operator"]);
const ooCtx = buildDealalityAccessContext(ownerOperatorCompany);
assert.equal(ooCtx.flags.isOwnerOperator, true);

console.log("test-company-workspace-access: ok");
