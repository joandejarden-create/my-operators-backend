#!/usr/bin/env node
/**
 * Regression: every requireDealalityUser resolution path must finalize
 * governed elevation before next(). /api/me and admin gate share the decision.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  finalizeDealalityUserAuthorization,
  elevateGovernedPlatformAdmin,
  elevateGovernedPlatformAdminOnRequest,
  isGovernedPlatformAdminEmail,
} from "../lib/dealality/governed-platform-admin-emails.js";
import { canAccessAdpMonthlyReviewAdmin } from "../middleware/requireAdpMonthlyReviewAdminAccess.js";
import {
  resolveLocalDemoDealalityUser,
  attachLocalDemoRequestContext,
  LOCAL_DEMO_USER_SOURCE,
  APPROVED_LOCAL_DEMO_EMAIL,
} from "../lib/dealality/local-demo-dealality-user.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const localReq = {
  hostname: "localhost",
  headers: { host: "localhost:8080" },
};

// --- Unit: finalize paths ---

const founderOwner = {
  email: "joandejarden@hotmail.com",
  memberstackId: "mem_founder",
  role: "owner",
  isAdmin: false,
  flags: { isAdmin: false, isOwner: true },
  workspaceAccess: ["Owner"],
  userSource: "AIRTABLE",
};
const founderReq = {
  memberstackMemberId: "mem_founder",
  memberstackEmail: "joandejarden@hotmail.com",
  memberstackVerifiedVia: "jwks",
};
let fin = finalizeDealalityUserAuthorization(founderReq, founderOwner);
assert.equal(fin.elevated, true);
assert.equal(fin.path, "jwt_email");
assert.equal(founderOwner.isAdmin, true);
assert.equal(
  canAccessAdpMonthlyReviewAdmin(founderOwner, localReq),
  true,
  "verified founder JWT + Airtable owner/isAdmin false → admin"
);

const missingEmailClaim = {
  email: "joandejarden@hotmail.com",
  memberstackId: "mem_founder2",
  role: "owner",
  isAdmin: false,
  flags: { isAdmin: false },
  workspaceAccess: ["Owner"],
  userSource: "AIRTABLE",
};
fin = finalizeDealalityUserAuthorization(
  {
    memberstackMemberId: "mem_founder2",
    memberstackEmail: null,
    memberstackVerifiedVia: "jwks+member_lookup",
  },
  missingEmailClaim
);
assert.equal(fin.elevated, true);
assert.equal(fin.path, "airtable_email_bound");

const spoofed = {
  email: "joandejarden@hotmail.com",
  isAdmin: false,
  flags: {},
  userSource: "AIRTABLE",
};
assert.equal(elevateGovernedPlatformAdmin(spoofed, null), false);
assert.equal(elevateGovernedPlatformAdminOnRequest({ memberstackEmail: "joandejarden@hotmail.com" }), null);
assert.equal(
  finalizeDealalityUserAuthorization(
    { memberstackMemberId: "dev_bypass", memberstackVerifiedVia: "dev_bypass" },
    spoofed
  ).elevated,
  false
);

const customer = {
  email: "customer@example.com",
  memberstackId: "mem_cust",
  role: "owner",
  isAdmin: false,
  flags: { isAdmin: false },
  workspaceAccess: ["Owner"],
  userSource: "AIRTABLE",
};
fin = finalizeDealalityUserAuthorization(
  {
    memberstackMemberId: "mem_cust",
    memberstackEmail: "customer@example.com",
    memberstackVerifiedVia: "jwks",
  },
  customer
);
assert.equal(fin.elevated, false);
assert.equal(customer.isAdmin, false);
assert.equal(canAccessAdpMonthlyReviewAdmin(customer, localReq), false);

const missingUsersGoverned = elevateGovernedPlatformAdminOnRequest({
  memberstackMemberId: "mem_gov",
  memberstackEmail: "dealalitydemo@dealality.com",
  memberstackVerifiedVia: "rest",
});
assert.ok(missingUsersGoverned);
assert.equal(missingUsersGoverned.isAdmin, true);

// LOCAL_DEMO fallback — the early-return defect case
const demoUser = resolveLocalDemoDealalityUser({
  memberstackId: "mem_demo",
  email: APPROVED_LOCAL_DEMO_EMAIL,
  req: { ...localReq, memberstackEmail: APPROVED_LOCAL_DEMO_EMAIL },
});
assert.ok(demoUser);
assert.equal(demoUser.isAdmin, false, "raw local-demo starts non-admin");
assert.equal(demoUser.userSource, LOCAL_DEMO_USER_SOURCE);
attachLocalDemoRequestContext(demoUser, localReq);
fin = finalizeDealalityUserAuthorization(
  {
    memberstackMemberId: "mem_demo",
    memberstackEmail: APPROVED_LOCAL_DEMO_EMAIL,
    memberstackVerifiedVia: "jwks",
    ...localReq,
  },
  demoUser
);
assert.equal(fin.elevated, true);
assert.ok(
  fin.path === "local_demo_canonical" || fin.path === "jwt_email",
  `unexpected finalize path ${fin.path}`
);
assert.equal(demoUser.isAdmin, true);

// LOCAL_DEMO with missing JWT email claim still elevates via canonical email
const demoNoClaim = resolveLocalDemoDealalityUser({
  memberstackId: "mem_demo2",
  email: APPROVED_LOCAL_DEMO_EMAIL,
  req: { ...localReq, memberstackEmail: APPROVED_LOCAL_DEMO_EMAIL },
});
attachLocalDemoRequestContext(demoNoClaim, localReq);
fin = finalizeDealalityUserAuthorization(
  {
    memberstackMemberId: "mem_demo2",
    memberstackEmail: null,
    memberstackVerifiedVia: "jwks",
  },
  demoNoClaim
);
assert.equal(fin.elevated, true);
assert.ok(
  fin.path === "local_demo_canonical" || fin.path === "airtable_email_bound",
  `unexpected path ${fin.path}`
);
assert.equal(demoNoClaim.isAdmin, true);
assert.equal(
  canAccessAdpMonthlyReviewAdmin(demoUser, localReq),
  true,
  "local-demo after finalize must pass admin gate"
);

// --- Source wiring: no early next() without finalize ---
const requireUserSrc = fs.readFileSync(
  path.join(root, "middleware/requireDealalityUser.js"),
  "utf8"
);
assert.match(requireUserSrc, /finalizeDealalityUserAuthorization|attachAndFinalize/);
assert.match(requireUserSrc, /local_demo_fallback/);
assert.match(requireUserSrc, /local_demo_airtable_error/);
// Early local-demo return must call attachAndFinalize before next
const localDemoIdx = requireUserSrc.indexOf("local_demo_fallback");
const nextAfterDemo = requireUserSrc.indexOf("return next()", localDemoIdx);
const finalizeBeforeNext = requireUserSrc.lastIndexOf("attachAndFinalize", nextAfterDemo);
assert.ok(finalizeBeforeNext > 0 && finalizeBeforeNext < nextAfterDemo);

const meSrc = fs.readFileSync(path.join(root, "api/me.js"), "utf8");
assert.match(meSrc, /finalizeDealalityUserAuthorization/);

assert.equal(isGovernedPlatformAdminEmail(APPROVED_LOCAL_DEMO_EMAIL), true);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "adp-admin-auth-finalize-elevation-v1",
      founderJwtOwnerElevated: true,
      localDemoFinalizeElevated: true,
      missingEmailClaimBoundElevated: true,
      customerDenied: true,
      spoofDenied: true,
      meParityWired: true,
    },
    null,
    2
  )
);
