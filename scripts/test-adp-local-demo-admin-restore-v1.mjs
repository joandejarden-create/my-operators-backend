/**
 * Gate: ADP_LOCAL_DEMO_ADMIN_RESTORE_V1
 *
 * Restores original localhost behavior:
 * verified Memberstack dealalitydemo@dealality.com → full platform admin
 * even when Airtable says role=owner / isAdmin=false.
 *
 * Single mechanism: finalizeDealalityUserAuthorization + hasLocalDealalityAdminAccess.
 * No second override module. Invalid JWT must not fall open to DEV bypass.
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  hasLocalDealalityAdminAccess,
  APPROVED_LOCAL_DEALALITY_QA_ADMIN_EMAILS,
} from "../lib/dealality/local-dealality-admin-access.js";
import { finalizeDealalityUserAuthorization } from "../lib/dealality/governed-platform-admin-emails.js";
import { canAccessAdpMonthlyReviewAdmin } from "../middleware/requireAdpMonthlyReviewAdminAccess.js";
import {
  APPROVED_LOCAL_DEMO_EMAIL,
  LOCAL_DEMO_USER_SOURCE,
  resolveLocalDemoDealalityUser,
  attachLocalDemoRequestContext,
} from "../lib/dealality/local-demo-dealality-user.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

assert.deepEqual([...APPROVED_LOCAL_DEALALITY_QA_ADMIN_EMAILS], [
  "dealalitydemo@dealality.com",
]);

const env = { NODE_ENV: "development" };

function localReq(overrides = {}) {
  return {
    hostname: "localhost",
    headers: { host: "localhost:8080" },
    memberstackMemberId: "mem_demo_verified",
    memberstackEmail: APPROVED_LOCAL_DEMO_EMAIL,
    memberstackVerifiedVia: "jwks",
    ...overrides,
  };
}

// Airtable owner / isAdmin false — still elevates for verified demo JWT
{
  const req = localReq();
  const user = {
    email: APPROVED_LOCAL_DEMO_EMAIL,
    memberstackId: "mem_demo_verified",
    role: "owner",
    isAdmin: false,
    flags: { isAdmin: false },
    workspaceAccess: [],
    userSource: "AIRTABLE",
  };
  assert.equal(hasLocalDealalityAdminAccess(user, req, env), true);
  const fin = finalizeDealalityUserAuthorization(req, user, env);
  assert.equal(fin.elevated, true);
  assert.equal(user.isAdmin, true);
  assert.equal(canAccessAdpMonthlyReviewAdmin(user, req, env), true);
}

// LOCAL_DEMO synthetic path
{
  const req = localReq({ memberstackMemberId: "mem_demo2" });
  const demo = resolveLocalDemoDealalityUser({
    memberstackId: "mem_demo2",
    email: APPROVED_LOCAL_DEMO_EMAIL,
    req,
    env,
  });
  assert.ok(demo);
  assert.equal(demo.isAdmin, false);
  assert.equal(demo.userSource, LOCAL_DEMO_USER_SOURCE);
  attachLocalDemoRequestContext(demo, req);
  const fin = finalizeDealalityUserAuthorization(req, demo, env);
  assert.equal(fin.elevated, true);
  assert.equal(demo.isAdmin, true);
}

// Customer denied
{
  const req = localReq({
    memberstackMemberId: "mem_cust",
    memberstackEmail: "customer@example.com",
  });
  const user = {
    email: "customer@example.com",
    memberstackId: "mem_cust",
    isAdmin: false,
    flags: {},
    workspaceAccess: [],
  };
  assert.equal(hasLocalDealalityAdminAccess(user, req, env), false);
  const fin = finalizeDealalityUserAuthorization(req, user, {
    ...env,
    ADP_OWNER_APP_ADMIN_EMAILS: "",
  });
  assert.equal(fin.elevated, false);
}

// Production denied for local QA path
{
  const req = localReq();
  const user = {
    email: APPROVED_LOCAL_DEMO_EMAIL,
    memberstackId: "mem_demo_verified",
    isAdmin: false,
    flags: {},
    workspaceAccess: [],
  };
  assert.equal(
    hasLocalDealalityAdminAccess(user, req, { NODE_ENV: "production" }),
    false
  );
}

// Spoofed header without verified claim
{
  const req = localReq({
    memberstackEmail: null,
    headers: { host: "localhost:8080", "x-user-email": APPROVED_LOCAL_DEMO_EMAIL },
  });
  const user = {
    email: "other@example.com",
    memberstackId: "mem_x",
    isAdmin: false,
    flags: {},
    workspaceAccess: [],
  };
  assert.equal(hasLocalDealalityAdminAccess(user, req, env), false);
}

// No second override module
assert.equal(existsSync(join(root, "lib/dealality/local-admin-override.js")), false);
const governed = readFileSync(
  join(root, "lib/dealality/governed-platform-admin-emails.js"),
  "utf8"
);
assert.doesNotMatch(governed, /local-admin-override/);
assert.match(governed, /finalizeDealalityUserAuthorization/);
assert.match(governed, /hasLocalDealalityAdminAccess/);

// Invalid JWT must not fall back to bypass in optionalDealalityAuth
const opt = readFileSync(join(root, "middleware/optionalDealalityAuth.js"), "utf8");
assert.match(opt, /must NEVER fall back to DEV_AUTH_BYPASS/i);
assert.doesNotMatch(
  opt,
  /catch \(err\) \{[\s\S]*tryDevAuthBypass\(req\)/
);

console.log(
  JSON.stringify({
    ok: true,
    gate: "adp-local-demo-admin-restore-v1",
    demoJwtElevates: true,
    localDemoElevates: true,
    customerDenied: true,
    productionBlocked: true,
    noSecondOverrideModule: true,
    invalidJwtNoBypassFallback: true,
  })
);
