/**
 * Gate: DEALALITY_LOCAL_ADMIN_OVERRIDE_V1
 * Local-only platform-admin override — never production / never spoof / never DEV bypass.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  shouldApplyLocalAdminOverride,
  isLocalAdminOverrideFlagEnabled,
  configuredLocalAdminOverrideEmail,
} from "../lib/dealality/local-admin-override.js";
import { finalizeDealalityUserAuthorization } from "../lib/dealality/governed-platform-admin-emails.js";
import { canAccessAdpMonthlyReviewAdmin } from "../middleware/requireAdpMonthlyReviewAdminAccess.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const OVERRIDE_EMAIL = "dealalitydemo@dealality.com";
const CUSTOMER_EMAIL = "customer@example.com";

function baseReq(overrides = {}) {
  return {
    memberstackMemberId: "mem_verified_demo",
    memberstackEmail: OVERRIDE_EMAIL,
    memberstackVerifiedVia: "jwks",
    hostname: "localhost",
    headers: { host: "localhost:8080" },
    ...overrides,
  };
}

function baseUser(overrides = {}) {
  return {
    email: OVERRIDE_EMAIL,
    memberstackId: "mem_verified_demo",
    role: "owner",
    isAdmin: false,
    flags: { isAdmin: false },
    workspaceAccess: [],
    userSource: "AIRTABLE",
    ...overrides,
  };
}

const enabledEnv = {
  NODE_ENV: "development",
  DEALALITY_LOCAL_ADMIN_OVERRIDE_ENABLED: "1",
  DEALALITY_LOCAL_ADMIN_OVERRIDE_EMAIL: OVERRIDE_EMAIL,
};

const disabledEnv = {
  NODE_ENV: "development",
  DEALALITY_LOCAL_ADMIN_OVERRIDE_ENABLED: "0",
  DEALALITY_LOCAL_ADMIN_OVERRIDE_EMAIL: OVERRIDE_EMAIL,
};

assert.equal(isLocalAdminOverrideFlagEnabled(disabledEnv), false);
assert.equal(isLocalAdminOverrideFlagEnabled(enabledEnv), true);
assert.equal(configuredLocalAdminOverrideEmail(enabledEnv), OVERRIDE_EMAIL);

// Happy path — verified demo JWT on localhost with flag
assert.equal(shouldApplyLocalAdminOverride(baseReq(), enabledEnv), true);
const happy = baseUser();
const fin = finalizeDealalityUserAuthorization(baseReq(), happy, enabledEnv);
assert.equal(fin.elevated, true);
assert.equal(fin.path, "local_admin_override");
assert.equal(happy.isAdmin, true);
assert.equal(happy.localAdminOverrideElevated, true);
assert.equal(canAccessAdpMonthlyReviewAdmin(happy, baseReq(), enabledEnv), true);

// Flag disabled — no override path (may still elevate via governed allowlist)
const offUser = baseUser();
const finOff = finalizeDealalityUserAuthorization(baseReq(), offUser, disabledEnv);
assert.notEqual(finOff.path, "local_admin_override");

// Production always off
assert.equal(
  shouldApplyLocalAdminOverride(baseReq(), { ...enabledEnv, NODE_ENV: "production" }),
  false
);
const prodUser = baseUser();
const finProd = finalizeDealalityUserAuthorization(
  baseReq(),
  prodUser,
  { ...enabledEnv, NODE_ENV: "production" }
);
assert.notEqual(finProd.path, "local_admin_override");

// Non-local host
assert.equal(
  shouldApplyLocalAdminOverride(
    baseReq({ hostname: "app.dealality.com", headers: { host: "app.dealality.com" } }),
    enabledEnv
  ),
  false
);

// Missing / invalid JWT identity
assert.equal(
  shouldApplyLocalAdminOverride(baseReq({ memberstackMemberId: null }), enabledEnv),
  false
);
assert.equal(
  shouldApplyLocalAdminOverride(
    baseReq({ memberstackMemberId: "dev_bypass", memberstackVerifiedVia: "dev_bypass" }),
    enabledEnv
  ),
  false
);

// DEV_AUTH_BYPASS must not activate override (finalize also skips)
const bypassUser = baseUser({ memberstackId: "dev_bypass" });
const finBypass = finalizeDealalityUserAuthorization(
  baseReq({
    memberstackMemberId: "dev_bypass",
    memberstackVerifiedVia: "dev_bypass",
  }),
  bypassUser,
  enabledEnv
);
assert.equal(finBypass.path, "skip_dev_bypass");
assert.equal(bypassUser.localAdminOverrideElevated, undefined);

// Spoofed header email without verified claim — no elevation via override
assert.equal(
  shouldApplyLocalAdminOverride(
    baseReq({
      memberstackEmail: null,
      headers: { host: "localhost:8080", "x-user-email": OVERRIDE_EMAIL },
    }),
    enabledEnv
  ),
  false
);

// Different customer JWT — not elevated by override
assert.equal(
  shouldApplyLocalAdminOverride(baseReq({ memberstackEmail: CUSTOMER_EMAIL }), enabledEnv),
  false
);
const customer = baseUser({
  email: CUSTOMER_EMAIL,
  memberstackId: "mem_customer",
  isAdmin: false,
});
const finCust = finalizeDealalityUserAuthorization(
  baseReq({
    memberstackMemberId: "mem_customer",
    memberstackEmail: CUSTOMER_EMAIL,
  }),
  customer,
  {
    ...enabledEnv,
    // Ensure customer is not on governed allowlist for this unit assertion
    ADP_OWNER_APP_ADMIN_EMAILS: "",
  }
);
// Customer may still fail elevate; must not be local_admin_override
assert.notEqual(finCust.path, "local_admin_override");
assert.equal(customer.localAdminOverrideElevated, undefined);

// Missing email claim
assert.equal(
  shouldApplyLocalAdminOverride(baseReq({ memberstackEmail: null }), enabledEnv),
  false
);

// Wiring: finalize + me host pass-through
const governedSrc = readFileSync(
  join(root, "lib/dealality/governed-platform-admin-emails.js"),
  "utf8"
);
assert.match(governedSrc, /shouldApplyLocalAdminOverride/);
assert.match(governedSrc, /local_admin_override/);
const meSrc = readFileSync(join(root, "api/me.js"), "utf8");
assert.match(meSrc, /hostname:\s*req\.hostname/);

console.log(
  JSON.stringify({
    ok: true,
    gate: "adp-local-admin-override-v1",
    overrideElevates: true,
    productionBlocked: true,
    bypassBlocked: true,
    spoofBlocked: true,
    customerBlocked: true,
    flagOffBlocksOverridePath: true,
  })
);
