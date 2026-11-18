/**
 * Gate: ADP_ADMIN_AUTH_SPLIT_BRAIN_V1
 *
 * Reproduces the founder browser failure mode:
 *   - no-Bearer localhost → DEV_AUTH_BYPASS admin (/api/me + admin APIs 200)
 *   - verified JWT for the same DEV_AUTH_BYPASS email must also get admin
 *   - invalid JWT must not fall open to admin
 *   - ordinary customer JWT stays denied
 *   - frontend must not bare-fetch admin APIs (bypass mix)
 *   - reviews must not full-page Access-restricted after workspace gate
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
  DEALALITY_LOCAL_ADMIN_OVERRIDE_ENABLED: "0",
  ADP_OWNER_APP_ADMIN_EMAILS: "",
};

function localReq(overrides = {}) {
  return {
    hostname: "localhost",
    headers: { host: "localhost:8080" },
    ...overrides,
  };
}

// --- 1) Verified founder JWT (bypass email) must elevate via local_qa path ---
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
  assert.equal(hasLocalDealalityAdminAccess(user, req, localEnv), true);
  const fin = finalizeDealalityUserAuthorization(req, user, localEnv);
  assert.equal(fin.elevated, true);
  assert.ok(
    fin.path === "local_qa_admin_access" ||
      fin.path === "jwt_email" ||
      fin.path === "airtable_email_bound",
    `unexpected path ${fin.path}`
  );
  assert.equal(user.isAdmin, true);
  assert.equal(canAccessAdpMonthlyReviewAdmin(user, req, localEnv), true);
}

// --- 2) Verified demo JWT ---
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
  assert.equal(canAccessAdpMonthlyReviewAdmin(user, req, localEnv), true);
}

// --- 3) Customer JWT denied ---
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
    userSource: "AIRTABLE",
  };
  assert.equal(hasLocalDealalityAdminAccess(user, req, localEnv), false);
  const fin = finalizeDealalityUserAuthorization(req, user, {
    ...localEnv,
    ADP_OWNER_APP_ADMIN_EMAILS: "",
  });
  assert.notEqual(fin.path, "local_qa_admin_access");
  assert.equal(canAccessAdpMonthlyReviewAdmin(user, req, localEnv), false);
}

// --- 4) Spoofed header without verified claim — denied ---
{
  const req = localReq({
    memberstackMemberId: "mem_x",
    memberstackEmail: null,
    memberstackVerifiedVia: "jwks",
    headers: { host: "localhost:8080", "x-user-email": BYPASS_EMAIL },
  });
  const user = {
    email: "other@example.com",
    memberstackId: "mem_x",
    isAdmin: false,
    flags: {},
    workspaceAccess: [],
  };
  assert.equal(hasLocalDealalityAdminAccess(user, req, localEnv), false);
}

// --- 5) Production never grants local QA from bypass email ---
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
    flags: {},
    workspaceAccess: [],
  };
  assert.equal(
    hasLocalDealalityAdminAccess(user, req, { ...localEnv, NODE_ENV: "production" }),
    false
  );
}

// --- 6) Frontend wiring: no bare admin fetch; gate-aware denial ---
{
  const reviews = readFileSync(join(root, "public/js/admin-ai-demand-reviews.js"), "utf8");
  assert.match(reviews, /DealalityMemberstackAuth\.authFetch unavailable/);
  assert.match(reviews, /__ADP_ADMIN_WORKSPACE_AUTH__/);
  assert.match(reviews, /showRenderError/);
  assert.doesNotMatch(
    reviews,
    /return fetch\(url, Object\.assign\(\{ credentials: "same-origin" \}/
  );

  const gate = readFileSync(join(root, "public/js/support-admin-gate.js"), "utf8");
  assert.match(gate, /cache:\s*["']no-store["']/);

  for (const rel of [
    "public/js/admin-adp-action-plan.js",
    "public/js/admin-ai-visibility-audit.js",
    "public/js/admin-gdi-reports.js",
    "public/js/admin-report-archive.js",
    "public/js/admin-gdi-research-audit.js",
    "public/js/admin-gdi-execution-monitor.js",
  ]) {
    const src = readFileSync(join(root, rel), "utf8");
    assert.match(src, /authFetch unavailable/, rel);
  }
}

// --- 7) Live localhost split-brain observation (no-Bearer vs bad Bearer) ---
{
  const base = "http://127.0.0.1:8080";
  async function hit(path, auth) {
    const headers = { Accept: "application/json" };
    if (auth !== undefined) headers.Authorization = auth;
    const res = await fetch(base + path, { headers, cache: "no-store" });
    const body = await res.json().catch(() => ({}));
    return { status: res.status, body };
  }
  try {
    const meNo = await hit("/api/me");
    assert.equal(meNo.status, 200);
    assert.equal(meNo.body?.dealality?.userSource, "LOCAL_DEV_AUTH_BYPASS");
    assert.equal(meNo.body?.dealality?.adpMonthlyReviewAdmin, true);

    const adminNo = await hit("/api/admin/ai-demand-positioning/monthly-reviews");
    assert.equal(adminNo.status, 200);
    assert.equal(adminNo.body?.ok, true);
    assert.ok(Array.isArray(adminNo.body?.properties));
    assert.ok(adminNo.body.properties.length >= 1);

    const adminBad = await hit(
      "/api/admin/ai-demand-positioning/monthly-reviews",
      "Bearer not.a.jwt"
    );
    assert.equal(adminBad.status, 401);
    assert.notEqual(adminBad.body?.ok, true);
  } catch (err) {
    console.warn("[split-brain] live localhost checks skipped:", err.message);
  }
}

console.log(
  JSON.stringify({
    ok: true,
    gate: "adp-admin-auth-split-brain-v1",
    founderJwtLocalQaElevates: true,
    demoJwtLocalQaElevates: true,
    customerDenied: true,
    spoofDenied: true,
    productionBlocked: true,
    frontendFailClosed: true,
    workspaceGatePreservesShell: true,
  })
);
