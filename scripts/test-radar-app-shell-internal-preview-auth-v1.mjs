#!/usr/bin/env node
/**
 * /app#/opportunity-radar auth parity for internal-preview hotels.
 * Deterministic — no Airtable, no network.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import { isAuthorizedForRadarInternalPreviewHotels } from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import { requireRadarInternalPreviewAccess } from "../middleware/requireRadarInternalPreviewAccess.js";
import { elevateAdpOwnerAppAdminFlags } from "../middleware/optionalDealalityAuth.js";
import { isAdpOwnerAppAdminEmail } from "../lib/dealality/adp-owner-app-admin-emails.js";

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

assert.match(
  gateSrc,
  /elevateAdpOwnerAppAdminFlags/,
  "Radar gate must reuse ADP owner-app admin elevation"
);
assert.match(
  gateSrc,
  /isAuthorizedForRadarInternalPreviewHotels/,
  "Radar gate must use canonical internal-preview authorizer"
);
assert.match(gateSrc, /forbidden_admin/, "non-admin must remain forbidden");
assert.doesNotMatch(
  gateSrc,
  /u\.isDemo\s*===\s*true/,
  "must not authorize from isDemo + client owner-app header alone"
);

assert.match(
  optionalSrc,
  /workspaceAccess:\s*result\.workspaceAccess/,
  "optionalDealalityAuth must attach Airtable workspaceAccess (requireDealalityUser parity)"
);
assert.match(
  optionalSrc,
  /resolveLocalDemoDealalityUser/,
  "optionalDealalityAuth must resolve local demo when Users row is missing"
);
assert.match(
  optionalSrc,
  /identityEmailsFromReq|isAdpOwnerAppAdminEmail/,
  "elevation must consider Memberstack and Airtable emails"
);

assert.match(
  authorizerSrc,
  /WORKSPACE_ADMIN/,
  "authorizer must honor Workspace Admin from Airtable"
);
assert.match(
  authorizerSrc,
  /isAdpOwnerAppAdminEmail/,
  "authorizer must honor server-side ADP admin email allowlist"
);

assert.match(
  optionalSrc,
  /export function elevateAdpOwnerAppAdminFlags/,
  "ADP elevation helper must be exported for Radar parity"
);
assert.match(
  serverSrc,
  /optionalDealalityAuth,\s*\n\s*requireRadarInternalPreviewAccess/,
  "route must use optionalDealalityAuth then Radar gate"
);
const routeBlock =
  serverSrc.match(
    /\/api\/brand-presence\/internal-preview-hotels[\s\S]{0,500}getBrandPresenceInternalPreviewHotels/
  )?.[0] || "";
assert.ok(routeBlock, "internal-preview route block must exist");
assert.doesNotMatch(
  routeBlock,
  /memberstackAuth/,
  "hard memberstackAuth must not remain on internal-preview (blocks app-shell JWT fallback)"
);

assert.match(mappingSrc, /X-Dealality-Owner-App/);
assert.match(mappingSrc, /waitForRadarEmbedJwt/);
assert.match(mappingSrc, /isOpportunityRadarShareMode\(\)/);
assert.match(mappingSrc, /skipped:\s*'share_mode'|skipped: 'share_mode'/);
assert.match(
  fs.readFileSync(path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"), "utf8"),
  /brand-presence-mapping\.js\?v=radar-hpc-auth-parity-1/
);

// Behavioral: owner role + approved demo email on localhost → authorized via local QA
assert.equal(
  isAuthorizedForRadarInternalPreviewHotels(
    {
      email: "dealalitydemo@dealality.com",
      isAdmin: false,
      isDemo: false,
      workspaceAccess: ["Owner", "Operator"],
    },
    { hostname: "localhost", headers: { host: "localhost:8080" } }
  ),
  true,
  "dealalitydemo local QA must authorize even when Airtable isDemo=false"
);

assert.equal(
  isAuthorizedForRadarInternalPreviewHotels(
    {
      email: "customer@example.com",
      isAdmin: false,
      isDemo: false,
      workspaceAccess: ["Owner"],
    },
    {
      hostname: "localhost",
      headers: { host: "localhost:8080", "x-dealality-owner-app": "1" },
      memberstackEmail: "customer@example.com",
    }
  ),
  false,
  "non-admin customer must stay forbidden even with owner-app header"
);

assert.equal(
  isAuthorizedForRadarInternalPreviewHotels(
    {
      email: "brand@example.com",
      isAdmin: false,
      workspaceAccess: ["Admin"],
    },
    { hostname: "localhost", headers: { host: "localhost:8080" } }
  ),
  true,
  "Workspace Admin must authorize without client header tricks"
);

assert.ok(
  isAdpOwnerAppAdminEmail("dealalitydemo@dealality.com"),
  "assignments allowlist must include dealalitydemo"
);

// Gate after elevation for ADP admin email with owner-role Airtable shape
{
  const req = {
    hostname: "localhost",
    headers: { host: "localhost:8080", "x-dealality-owner-app": "1" },
    memberstackEmail: "dealalitydemo@dealality.com",
    dealalityUser: {
      email: "dealalitydemo@dealality.com",
      isAdmin: false,
      isDemo: false,
      role: "owner",
      workspaceAccess: ["Owner", "Operator"],
    },
  };
  elevateAdpOwnerAppAdminFlags(req);
  assert.equal(req.dealalityUser.isAdmin, true, "elevation must set isAdmin for ADP admin email");
  let status = 200;
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
  assert.equal(status, 200, "elevated demo owner session must pass Radar gate");
}

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-app-shell-internal-preview-auth-v1",
    route: "/api/brand-presence/internal-preview-hotels",
    appRoute: "/app#/opportunity-radar",
    authBasis: "verified_identity_plus_server_allowlists",
  })
);
