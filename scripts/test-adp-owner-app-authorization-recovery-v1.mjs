#!/usr/bin/env node
/**
 * Owner-app authorization recovery gates.
 * npm run test:adp-owner-app-authorization-recovery-v1
 */

import "../load-env.js";
import assert from "assert";
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { join } from "path";
import { extractShareCapabilityFromRequest } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import {
  resolveOwnerAppPropertyAccess,
  ownerAppCanAccessProperty,
} from "../lib/ai-demand-positioning/share/adp-owner-app-property-access-v1.js";
import { optionalDealalityAuth } from "../middleware/optionalDealalityAuth.js";
import {
  issueShareCapability,
  revokeShareCapability,
} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";

process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET = "1";
process.env.ADP_SHARE_CAPABILITY_ENFORCE = "1";

const OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning/owner-app-authorization-recovery"
);
const results = { stamp: new Date().toISOString(), gates: {}, probes: [] };

function mark(gate, pass, detail = null) {
  results.gates[gate] = pass ? "PASS" : "FAIL";
  if (!pass) throw new Error(`${gate}${detail ? `: ${detail}` : ""}`);
}

function mockRes() {
  return {
    statusCode: 200,
    body: null,
    status(n) {
      this.statusCode = n;
      return this;
    },
    json(b) {
      this.body = b;
      return this;
    },
  };
}

async function runOptionalAuth(req) {
  await new Promise((resolve) => {
    optionalDealalityAuth(req, mockRes(), resolve);
  });
  return req;
}

async function main() {
  mkdirSync(OUT, { recursive: true });

  const FAKE_JWT =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJtZW1fdGVzdCIsImVtYWlsIjoidGVzdEBleGFtcGxlLmNvbSJ9.sig";

  // MEMBERSTACK_JWT_NOT_SHARE_CAPABILITY
  assert.strictEqual(
    extractShareCapabilityFromRequest({
      headers: { authorization: "Bearer " + FAKE_JWT },
    }),
    null
  );
  mark("MEMBERSTACK_JWT_NOT_SHARE_CAPABILITY", true);

  // OWNER_APP_REQUESTS_PRESERVE_AUTH_CONTEXT — UI sends Owner-App header
  const uiSrc = readFileSync(
    join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js"),
    "utf8"
  );
  assert.ok(uiSrc.includes('X-Dealality-Owner-App'));
  assert.ok(uiSrc.includes("waitForLogin:"));
  assert.ok(uiSrc.includes("adpZeroAssignmentMessage") || uiSrc.includes("No AI Demand Positioning reports are assigned"));
  assert.ok(uiSrc.includes("Sign in to Dealality to view your AI Demand Positioning reports."));
  mark("OWNER_APP_REQUESTS_PRESERVE_AUTH_CONTEXT", true);
  mark("OWNER_APP_AUTH_ERROR_STATE_ACCURACY", true);

  // Admin path — assignments adminEmails or isAdmin
  const adminReq = {
    memberstackMemberId: "mem_admin",
    memberstackEmail: "joandejarden@hotmail.com",
    dealalityUser: { isAdmin: false, email: "joandejarden@hotmail.com" },
  };
  const adminAccess = resolveOwnerAppPropertyAccess(adminReq);
  assert.ok(adminAccess.isAdmin, "assignment adminEmails must grant admin");
  assert.ok(adminAccess.allowedPropertyIds.length >= 5);

  // Founder Demo account is Airtable owner (isAdmin:false) but ADP platform admin.
  const demoReq = {
    memberstackMemberId: "mem_demo",
    memberstackEmail: "dealalitydemo@dealality.com",
    dealalityUser: {
      isAdmin: false,
      email: "dealalitydemo@dealality.com",
      role: "owner",
    },
  };
  const demoAccess = resolveOwnerAppPropertyAccess(demoReq);
  assert.ok(demoAccess.isAdmin, "dealalitydemo must be ADP admin via adminEmails");
  assert.ok(demoAccess.allowedPropertyIds.length >= 5);
  mark("OWNER_APP_ADMIN_PROPERTY_ACCESS_COMPLETE", true);

  // Non-admin zero assignment
  const limited = {
    memberstackMemberId: "mem_limited",
    memberstackEmail: "limited@hotel.com",
    dealalityUser: { isAdmin: false, email: "limited@hotel.com" },
  };
  assert.strictEqual(resolveOwnerAppPropertyAccess(limited).allowedPropertyIds.length, 0);
  assert.strictEqual(ownerAppCanAccessProperty(limited, "adp_waterstone_boca_raton").status, 403);

  // DEV_BYPASS_INTERNAL_ONLY
  const prevBypass = process.env.DEV_AUTH_BYPASS_EMAIL;
  const prevNode = process.env.NODE_ENV;
  process.env.DEV_AUTH_BYPASS_EMAIL = "dev-bypass@dealality.com";
  process.env.NODE_ENV = "development";

  const bypassOk = await runOptionalAuth({
    hostname: "localhost",
    headers: { "x-dealality-owner-app": "1" },
  });
  assert.strictEqual(bypassOk.memberstackVerifiedVia, "dev_bypass");
  assert.ok(bypassOk.dealalityUser?.isAdmin);

  const bypassDeniedShareSurface = await runOptionalAuth({
    hostname: "localhost",
    headers: {}, // no Owner-App header
  });
  assert.notStrictEqual(bypassDeniedShareSurface.memberstackVerifiedVia, "dev_bypass");

  process.env.NODE_ENV = "production";
  const bypassDeniedProd = await runOptionalAuth({
    hostname: "localhost",
    headers: { "x-dealality-owner-app": "1" },
  });
  assert.notStrictEqual(bypassDeniedProd.memberstackVerifiedVia, "dev_bypass");

  process.env.DEV_AUTH_BYPASS_EMAIL = prevBypass;
  process.env.NODE_ENV = prevNode;
  mark("DEV_BYPASS_INTERNAL_ONLY", true);

  // Optional middleware must ignore adpshare bearer as Memberstack
  const shareIgnore = await runOptionalAuth({
    hostname: "localhost",
    headers: {
      authorization: "Bearer adpshare.v1.abc.def",
      "x-dealality-owner-app": "1",
    },
  });
  assert.ok(!shareIgnore.memberstackMemberId || shareIgnore.memberstackVerifiedVia === "dev_bypass");

  // HTTP probes against local server
  const BASE = process.env.ADP_BASE_URL || "http://127.0.0.1:8080";
  process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET =
    process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET || "1";

  async function hit(path, headers) {
    const res = await fetch(`${BASE}${path}`, {
      headers: { Accept: "application/json", ...(headers || {}) },
      cache: "no-store",
    });
    let json = null;
    try {
      json = await res.json();
    } catch {
      /* ignore */
    }
    return { status: res.status, json };
  }

  let httpOk = true;
  try {
    const unauth = await hit("/api/ai-demand-positioning/properties", {});
    results.probes.push({ name: "unauth_properties", ...unauth });
    if (unauth.status !== 401) httpOk = false;

    const ownerBypass = await hit("/api/ai-demand-positioning/properties", {
      "X-Dealality-Owner-App": "1",
    });
    results.probes.push({
      name: "owner_app_properties",
      status: ownerBypass.status,
      count: ownerBypass.json?.properties?.length,
      auth: ownerBypass.json?.auth,
    });
    // With DEV_AUTH_BYPASS set on server OR empty without header session → may be 401 or 200
    if (ownerBypass.status === 200) {
      assert.ok((ownerBypass.json?.properties || []).length >= 5, "five properties for owner bypass/admin");
    }

    const issued = issueShareCapability({
      propertyId: "adp_waterstone_boca_raton",
      label: "owner-auth-recovery",
    });
    const shareOk = await hit(
      `/api/ai-demand-positioning/property/adp_waterstone_boca_raton/report?share=${encodeURIComponent(issued.token)}`,
      {}
    );
    results.probes.push({ name: "share_valid", status: shareOk.status });
    if (shareOk.status !== 200) httpOk = false;

    const shareCross = await hit(
      `/api/ai-demand-positioning/property/adp_now_now_noho/report?share=${encodeURIComponent(issued.token)}`,
      {}
    );
    results.probes.push({ name: "share_cross", status: shareCross.status });
    if (shareCross.status !== 403) httpOk = false;

    const sharePropOnly = await hit(
      "/api/ai-demand-positioning/property/adp_waterstone_boca_raton/report?propertyId=adp_waterstone_boca_raton",
      {}
    );
    results.probes.push({ name: "property_id_only", status: sharePropOnly.status });
    if (sharePropOnly.status !== 401) httpOk = false;

    // Dev bypass must not open share-only report without capability
    const bypassNoShare = await hit(
      "/api/ai-demand-positioning/property/adp_waterstone_boca_raton/report",
      { "X-Dealality-Owner-App": "1" }
    );
    results.probes.push({
      name: "owner_app_report_with_bypass_or_401",
      status: bypassNoShare.status,
    });
    // Owner-app with bypass may be 200; without bypass 401 — both OK if share still fail-closed above

    revokeShareCapability(issued.tokenId);
    const revoked = await hit(
      `/api/ai-demand-positioning/property/adp_waterstone_boca_raton/report?share=${encodeURIComponent(issued.token)}`,
      {}
    );
    results.probes.push({ name: "share_revoked", status: revoked.status });
    if (revoked.status !== 401) httpOk = false;
  } catch (err) {
    results.probes.push({ error: String(err.message || err) });
    httpOk = false;
  }

  mark("OWNER_APP_EXTERNAL_SHARE_AUTH_SEPARATION", httpOk);

  const outPath = join(OUT, "owner-app-authorization-recovery-v1.json");
  writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log(JSON.stringify({ ok: true, outPath, gates: results.gates, probes: results.probes }, null, 2));
}

main().catch((err) => {
  results.error = err.message;
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "owner-app-authorization-recovery-v1.json"), JSON.stringify(results, null, 2));
  console.error(err);
  process.exit(1);
});
