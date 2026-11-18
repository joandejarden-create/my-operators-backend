#!/usr/bin/env node
/**
 * /app#/opportunity-radar auth parity for internal-preview hotels.
 * Deterministic — no Airtable, no network.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import {
  evaluateRadarInternalPreviewAccess,
  isAuthorizedForRadarInternalPreviewHotels,
  RADAR_INTERNAL_PREVIEW_ACCESS_POLICY,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import { requireRadarInternalPreviewAccess } from "../middleware/requireRadarInternalPreviewAccess.js";
import { elevateAdpOwnerAppAdminFlags } from "../middleware/optionalDealalityAuth.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const gateSrc = fs.readFileSync(
  path.join(ROOT, "middleware/requireRadarInternalPreviewAccess.js"),
  "utf8"
);
const optionalSrc = fs.readFileSync(
  path.join(ROOT, "middleware/optionalDealalityAuth.js"),
  "utf8"
);
const serverSrc = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
const mappingSrc = fs.readFileSync(
  path.join(ROOT, "public/brand-presence-mapping.js"),
  "utf8"
);
const authorizerSrc = fs.readFileSync(
  path.join(ROOT, "lib/hotel-intelligence/radar-internal-preview-hotels-v1.js"),
  "utf8"
);

assert.match(gateSrc, /elevateAdpOwnerAppAdminFlags/);
assert.match(gateSrc, /evaluateRadarInternalPreviewAccess|isAuthorizedForRadarInternalPreviewHotels/);
assert.match(gateSrc, /forbidden_radar_workspace/);
assert.doesNotMatch(gateSrc, /u\.isDemo\s*===\s*true/);

assert.match(optionalSrc, /workspaceAccess:\s*result\.workspaceAccess/);
assert.match(optionalSrc, /resolveLocalDemoDealalityUser/);

assert.match(authorizerSrc, /RADAR_INTERNAL_PREVIEW_ACCESS_POLICY/);
assert.match(authorizerSrc, /ALLOW_RADAR_APP_WORKSPACE_PRINCIPAL/);
assert.match(authorizerSrc, /WORKSPACE_OWNER/);
assert.match(authorizerSrc, /evaluateRadarInternalPreviewAccess/);

assert.match(serverSrc, /optionalDealalityAuth,\s*\n\s*requireRadarInternalPreviewAccess/);
assert.doesNotMatch(
  serverSrc.match(
    /\/api\/brand-presence\/internal-preview-hotels[\s\S]{0,500}getBrandPresenceInternalPreviewHotels/
  )?.[0] || "",
  /memberstackAuth/
);

assert.match(mappingSrc, /waitForRadarEmbedJwt/);
assert.match(mappingSrc, /isOpportunityRadarShareMode\(\)/);
assert.match(
  fs.readFileSync(path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"), "utf8"),
  /brand-presence-mapping\.js\?v=radar-hpc-missing-coords-reconcile-1/
);

const localReq = { hostname: "localhost", headers: { host: "localhost:8080" } };

// Founder-reported shape: Owner+Operator, isAdmin false, isDemo false → ALLOW
{
  const user = {
    email: "dealalitydemo@dealality.com",
    isAdmin: false,
    isDemo: false,
    workspaceAccess: ["Owner", "Operator"],
    userSource: "AIRTABLE",
  };
  const decision = evaluateRadarInternalPreviewAccess(user, localReq);
  assert.equal(decision.allowed, true);
  assert.equal(decision.reason, "ALLOW_RADAR_APP_WORKSPACE_PRINCIPAL");
  assert.equal(decision.policy, RADAR_INTERNAL_PREVIEW_ACCESS_POLICY);
  assert.equal(decision.checks.workspaceOwner, true);
  assert.equal(decision.checks.workspaceOperator, true);
  assert.equal(decision.checks.isAdmin, false);
  assert.equal(isAuthorizedForRadarInternalPreviewHotels(user, localReq), true);
}

// Brand-only WA → ALLOW (Radar nav includes brand)
assert.equal(
  evaluateRadarInternalPreviewAccess(
    { isAdmin: false, workspaceAccess: ["Brand"], userSource: "AIRTABLE" },
    localReq
  ).reason,
  "ALLOW_RADAR_APP_WORKSPACE_PRINCIPAL"
);

// No WA, not admin → DENY (even with owner-app header)
{
  const decision = evaluateRadarInternalPreviewAccess(
    {
      email: "outsider@example.com",
      isAdmin: false,
      isDemo: false,
      workspaceAccess: [],
      userSource: "AIRTABLE",
    },
    {
      ...localReq,
      headers: { ...localReq.headers, "x-dealality-owner-app": "1" },
      memberstackEmail: "outsider@example.com",
    }
  );
  assert.equal(decision.allowed, false);
  assert.equal(decision.reason, "DENY_NO_RADAR_WORKSPACE_ENTITLEMENT");
}

// Missing user → deny
assert.equal(isAuthorizedForRadarInternalPreviewHotels(null, localReq), false);

// Platform admin → allow
assert.equal(
  evaluateRadarInternalPreviewAccess({ isAdmin: true, workspaceAccess: [] }, localReq).reason,
  "ALLOW_PLATFORM_ADMIN"
);

// Gate middleware: Owner/Operator session passes without admin elevation dependency
{
  const req = {
    hostname: "localhost",
    headers: { host: "localhost:8080" },
    memberstackEmail: "owner.op@example.com",
    dealalityUser: {
      email: "owner.op@example.com",
      isAdmin: false,
      isDemo: false,
      role: "owner",
      workspaceAccess: ["Owner", "Operator"],
      userSource: "AIRTABLE",
    },
  };
  let status = 500;
  requireRadarInternalPreviewAccess(
    req,
    {
      status(code) {
        status = code;
        return { json() {} };
      },
    },
    () => {
      status = 200;
    }
  );
  assert.equal(status, 200, "Owner/Operator Bearer session must pass Radar HPC gate");
}

// Gate middleware: empty WA → 403 with reason code (non-production)
{
  const req = {
    hostname: "localhost",
    headers: { host: "localhost:8080" },
    dealalityUser: {
      email: "nobody@example.com",
      isAdmin: false,
      workspaceAccess: [],
      userSource: "AIRTABLE",
    },
  };
  let status = 200;
  let body = null;
  requireRadarInternalPreviewAccess(
    req,
    {
      status(code) {
        status = code;
        return {
          json(payload) {
            body = payload;
          },
        };
      },
    },
    () => {
      status = 200;
    }
  );
  assert.equal(status, 403);
  assert.equal(body?.error, "forbidden_radar_workspace");
  assert.equal(body?.decision?.reason, "DENY_NO_RADAR_WORKSPACE_ENTITLEMENT");
}

// Elevation still works for ADP admin emails without WA
{
  const req = {
    hostname: "localhost",
    headers: { host: "localhost:8080" },
    memberstackEmail: "dealalitydemo@dealality.com",
    dealalityUser: {
      email: "dealalitydemo@dealality.com",
      isAdmin: false,
      workspaceAccess: [],
      userSource: "AIRTABLE",
    },
  };
  elevateAdpOwnerAppAdminFlags(req);
  assert.equal(req.dealalityUser.isAdmin, true);
}

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-app-shell-internal-preview-auth-v1",
    route: "/api/brand-presence/internal-preview-hotels",
    appRoute: "/app#/opportunity-radar",
    policy: RADAR_INTERNAL_PREVIEW_ACCESS_POLICY,
    founderShapeReason: "ALLOW_RADAR_APP_WORKSPACE_PRINCIPAL",
  })
);
