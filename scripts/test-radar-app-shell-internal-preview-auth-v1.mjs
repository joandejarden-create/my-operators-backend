#!/usr/bin/env node
/**
 * /app#/opportunity-radar auth parity for internal-preview hotels.
 * Deterministic — no Airtable, no network.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";

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
assert.match(
  gateSrc,
  /forbidden_admin/,
  "non-admin must remain forbidden"
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

// Client still sends owner-app + auth headers; share mode still skips merge.
assert.match(mappingSrc, /X-Dealality-Owner-App/);
assert.match(mappingSrc, /isOpportunityRadarShareMode\(\)/);
assert.match(mappingSrc, /skipped:\s*'share_mode'|skipped: 'share_mode'/);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-app-shell-internal-preview-auth-v1",
    route: "/api/brand-presence/internal-preview-hotels",
    appRoute: "/app#/opportunity-radar",
  })
);
