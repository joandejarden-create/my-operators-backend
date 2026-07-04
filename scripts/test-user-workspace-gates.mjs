import assert from "node:assert/strict";
import {
  userCanAccessBrandWorkspace,
  userCanAccessDemoWorkspace,
  userCanAccessOwnerWorkspace,
  userCanAccessOperatorWorkspace,
} from "../lib/dealality/user-workspace-gates.js";

const hotelOwner = {
  role: "owner",
  isOwner: true,
  isOperator: false,
  workspaceAccess: ["Owner"],
};
assert.equal(userCanAccessOwnerWorkspace(hotelOwner), true);
assert.equal(userCanAccessOperatorWorkspace(hotelOwner), false);
assert.equal(userCanAccessDemoWorkspace(hotelOwner), false);

const demoOnly = {
  role: "unknown",
  isDemo: true,
  isAdmin: false,
  flags: { isDemo: true, isOwner: false, isOperator: false, isBrand: false, isAdmin: false },
  workspaceAccess: ["Demo"],
  canAccessDemoWorkspace: true,
};
assert.equal(userCanAccessDemoWorkspace(demoOnly), true);
assert.equal(userCanAccessOwnerWorkspace(demoOnly), false);
assert.equal(userCanAccessOperatorWorkspace(demoOnly), false);
assert.equal(userCanAccessBrandWorkspace(demoOnly), false);

const demoPlusOwner = {
  isDemo: true,
  isOwner: true,
  flags: { isDemo: true, isOwner: true },
  workspaceAccess: ["Demo", "Owner"],
  canAccessOwnerWorkspace: true,
};
assert.equal(userCanAccessDemoWorkspace(demoPlusOwner), true);
assert.equal(userCanAccessOwnerWorkspace(demoPlusOwner), true);
assert.equal(userCanAccessOperatorWorkspace(demoPlusOwner), false);

const demoPlusOperator = {
  isDemo: true,
  isOperator: true,
  flags: { isDemo: true, isOperator: true },
  workspaceAccess: ["Demo", "Operator"],
  canAccessOperatorWorkspace: true,
};
assert.equal(userCanAccessOperatorWorkspace(demoPlusOperator), true);
assert.equal(userCanAccessOwnerWorkspace(demoPlusOperator), false);

const ownerOperator = {
  role: "owner",
  primaryRole: "owner-operator",
  flags: { isOwner: true, isOperator: true, isOwnerOperator: true },
  workspaceAccess: ["Owner", "Operator"],
  canAccessOwnerWorkspace: true,
  canAccessOperatorWorkspace: true,
};
assert.equal(userCanAccessOwnerWorkspace(ownerOperator), true);
assert.equal(userCanAccessOperatorWorkspace(ownerOperator), true);

const admin = { isAdmin: true, role: "admin", flags: { isAdmin: true } };
assert.equal(userCanAccessOwnerWorkspace(admin), true);
assert.equal(userCanAccessOperatorWorkspace(admin), true);
assert.notEqual(userCanAccessDemoWorkspace(admin), true);

console.log("test-user-workspace-gates: ok");
