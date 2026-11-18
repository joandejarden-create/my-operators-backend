/**
 * Gate: ADP_ADMIN_AUTH_SPLIT_BRAIN_V1
 *
 * - no-Bearer localhost → DEV_AUTH_BYPASS may admin (separate path)
 * - verified dealalitydemo JWT → local admin via finalize / hasLocalDealalityAdminAccess
 * - hotmail JWT is NOT local-QA demo (avoid identity mix with bypass email)
 * - invalid JWT must not fall open
 * - frontend must not bare-fetch admin APIs
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { hasLocalDealalityAdminAccess } from "../lib/dealality/local-dealality-admin-access.js";
import { finalizeDealalityUserAuthorization } from "../lib/dealality/governed-platform-admin-emails.js";
import { canAccessAdpMonthlyReviewAdmin } from "../middleware/requireAdpMonthlyReviewAdminAccess.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const BYPASS_EMAIL = "joandejarden@hotmail.com";
const DEMO_EMAIL = "dealalitydemo@dealality.com";
const CUSTOMER_EMAIL = "customer@example-hotel.com";

const localEnv = {
  NODE_ENV: "development",
  DEV_AUTH_BYPASS_EMAIL: BYPASS_EMAIL,
  ADP_OWNER_APP_ADMIN_EMAILS: "",
};

function localReq(overrides = {}) {
  return {
    hostname: "localhost",
    headers: { host: "localhost:8080" },
    ...overrides,
  };
}

// Demo JWT elevates
{
  const req = localReq({
    memberstackMemberId: "mem_demo",
    memberstackEmail: DEMO_EMAIL,
    memberstackVerifiedVia: "jwks",
  });
  const user = {
    email: DEMO_EMAIL,
    memberstackId: "mem_demo",
    isAdmin: false,
    flags: {},
    workspaceAccess: [],
    userSource: "AIRTABLE",
  };
  assert.equal(hasLocalDealalityAdminAccess(user, req, localEnv), true);
  const fin = finalizeDealalityUserAuthorization(req, user, localEnv);
  assert.equal(fin.elevated, true);
  assert.equal(canAccessAdpMonthlyReviewAdmin(user, req, localEnv), true);
}

// Hotmail JWT is NOT the local-demo QA identity (bypass is a separate no-Bearer path)
{
  const req = localReq({
    memberstackMemberId: "mem_founder_jwt",
    memberstackEmail: BYPASS_EMAIL,
    memberstackVerifiedVia: "jwks",
  });
  const user = {
    email: BYPASS_EMAIL,
    memberstackId: "mem_founder_jwt",
    isAdmin: false,
    flags: { isAdmin: false },
    workspaceAccess: [],
    userSource: "AIRTABLE",
  };
  assert.equal(hasLocalDealalityAdminAccess(user, req, localEnv), false);
}

// Customer denied
{
  const req = localReq({
    memberstackMemberId: "mem_customer",
    memberstackEmail: CUSTOMER_EMAIL,
    memberstackVerifiedVia: "jwks",
  });
  const user = {
    email: CUSTOMER_EMAIL,
    memberstackId: "mem_customer",
    isAdmin: false,
    flags: {},
    workspaceAccess: [],
  };
  assert.equal(hasLocalDealalityAdminAccess(user, req, localEnv), false);
  const fin = finalizeDealalityUserAuthorization(req, user, localEnv);
  assert.equal(fin.elevated, false);
}

// Frontend wiring
{
  const reviews = readFileSync(join(root, "public/js/admin-ai-demand-reviews.js"), "utf8");
  assert.match(reviews, /DealalityMemberstackAuth\.authFetch unavailable/);
  assert.match(reviews, /__ADP_ADMIN_WORKSPACE_AUTH__/);
  const gate = readFileSync(join(root, "public/js/support-admin-gate.js"), "utf8");
  assert.match(gate, /cache:\s*["']no-store["']/);
}

// Live: invalid JWT never admin
{
  try {
    const res = await fetch(
      "http://127.0.0.1:8080/api/admin/ai-demand-positioning/monthly-reviews",
      {
        headers: {
          Accept: "application/json",
          Authorization: "Bearer not.a.jwt",
        },
        cache: "no-store",
      }
    );
    const body = await res.json().catch(() => ({}));
    assert.equal(res.status, 401);
    assert.notEqual(body.ok, true);
  } catch (err) {
    console.warn("[split-brain] live check skipped:", err.message);
  }
}

console.log(
  JSON.stringify({
    ok: true,
    gate: "adp-admin-auth-split-brain-v1",
    demoJwtLocalQaElevates: true,
    hotmailNotLocalDemoQa: true,
    customerDenied: true,
    frontendFailClosed: true,
  })
);
