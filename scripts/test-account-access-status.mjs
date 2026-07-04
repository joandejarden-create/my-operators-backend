import assert from "node:assert/strict";
import { resolveAccountAccessStatus } from "../lib/dealality/account-access-status.js";

const pending = resolveAccountAccessStatus({
  dealalityRole: {
    isAdmin: false,
    workspaceAccess: [],
    canAccessOwnerWorkspace: false,
    canAccessBrandWorkspace: false,
    canAccessOperatorWorkspace: false,
  },
  accountStatusRaw: "Pending",
});
assert.equal(pending.state, "pending_approval");
assert.equal(pending.pendingApproval, true);
assert.equal(pending.suppressBrandAssignmentToast, true);

const activeOwner = resolveAccountAccessStatus({
  dealalityRole: {
    isAdmin: false,
    workspaceAccess: ["Owner"],
    canAccessOwnerWorkspace: true,
    isOwner: true,
  },
  accountStatusRaw: "Active",
});
assert.equal(activeOwner.state, "active");
assert.equal(activeOwner.pendingApproval, false);

console.log("test-account-access-status: ok");
