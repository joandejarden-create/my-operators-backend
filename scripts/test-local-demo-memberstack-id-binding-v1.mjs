#!/usr/bin/env node
/**
 * Local Demo member-ID binding — exact id only, local/dev only, no prefix grant.
 */
import assert from "node:assert/strict";
import {
  isLocalDemoMemberstackIdBindingEnabled,
  loadLocalDemoMemberstackIdBindings,
  resolveLocalDemoEmailForVerifiedMemberstackId,
} from "../lib/dealality/local-demo-memberstack-id-binding.js";
import { APPROVED_LOCAL_DEMO_EMAIL } from "../lib/dealality/local-demo-dealality-user.js";
import { finalizeDealalityUserAuthorization } from "../lib/dealality/governed-platform-admin-emails.js";
import { canAccessAdpMonthlyReviewAdmin } from "../middleware/requireAdpMonthlyReviewAdminAccess.js";

const DEMO_SB = "mem_sb_cmm8t8p1x00230spt95ya48zy";
const OTHER_SB = "mem_sb_other_sandbox_member_xxx";
const localReq = { hostname: "localhost", headers: { host: "localhost:8080" } };

const enabledEnv = {
  NODE_ENV: "development",
  DEALALITY_LOCAL_DEMO_MEMBERSTACK_ID_BINDING: "1",
  DEALALITY_LOCAL_DEMO_MEMBERSTACK_IDS: DEMO_SB,
};

assert.equal(isLocalDemoMemberstackIdBindingEnabled(enabledEnv), true);
assert.equal(
  isLocalDemoMemberstackIdBindingEnabled({
    ...enabledEnv,
    NODE_ENV: "production",
  }),
  false
);
assert.equal(
  isLocalDemoMemberstackIdBindingEnabled({
    NODE_ENV: "development",
    DEALALITY_LOCAL_DEMO_MEMBERSTACK_ID_BINDING: "0",
  }),
  false
);

assert.deepEqual([...loadLocalDemoMemberstackIdBindings(enabledEnv)], [DEMO_SB]);

assert.equal(
  resolveLocalDemoEmailForVerifiedMemberstackId(DEMO_SB, localReq, enabledEnv),
  APPROVED_LOCAL_DEMO_EMAIL
);
assert.equal(
  resolveLocalDemoEmailForVerifiedMemberstackId(OTHER_SB, localReq, enabledEnv),
  null,
  "other sandbox members must not elevate"
);
assert.equal(
  resolveLocalDemoEmailForVerifiedMemberstackId("mem_sb_", localReq, enabledEnv),
  null,
  "prefix alone must not elevate"
);
assert.equal(
  resolveLocalDemoEmailForVerifiedMemberstackId(DEMO_SB, localReq, {
    NODE_ENV: "production",
    DEALALITY_LOCAL_DEMO_MEMBERSTACK_ID_BINDING: "1",
    DEALALITY_LOCAL_DEMO_MEMBERSTACK_IDS: DEMO_SB,
  }),
  null,
  "production disabled"
);

const user = {
  email: APPROVED_LOCAL_DEMO_EMAIL,
  memberstackId: DEMO_SB,
  role: "owner",
  isAdmin: false,
  flags: { isAdmin: false, isOwner: true },
  workspaceAccess: ["Owner"],
  userSource: "AIRTABLE",
};
const req = {
  ...localReq,
  memberstackMemberId: DEMO_SB,
  memberstackEmail: APPROVED_LOCAL_DEMO_EMAIL,
  memberstackVerifiedVia: "jwks",
};
const fin = finalizeDealalityUserAuthorization(req, user, enabledEnv);
assert.equal(fin.elevated, true);
assert.equal(user.isAdmin, true);
assert.equal(canAccessAdpMonthlyReviewAdmin(user, req, enabledEnv), true);

console.log(
  JSON.stringify({
    ok: true,
    gate: "local-demo-memberstack-id-binding-v1",
    exactIdElevates: true,
    otherSandboxDenied: true,
    prefixDenied: true,
    productionDisabled: true,
  })
);
