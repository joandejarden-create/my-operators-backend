#!/usr/bin/env node
/**
 * Canonical /app#/opportunity-radar → Hotel Explorer wiring gate (Phase 1.3).
 * Free / deterministic — no Airtable, no network.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const APP_JS = path.join(ROOT, "public/app.js");
const HOST = path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html");
const MAPPING = path.join(ROOT, "public/brand-presence-mapping.js");
const SHARE = path.join(ROOT, "public/hotel-explorer-share.html");
const SERVER = path.join(ROOT, "server.js");
const INTERNAL_API = path.join(ROOT, "api/brand-presence-internal-preview.js");
const INTERNAL_LIB = path.join(
  ROOT,
  "lib/hotel-intelligence/radar-internal-preview-hotels-v1.js"
);

const appJs = fs.readFileSync(APP_JS, "utf8");
const host = fs.readFileSync(HOST, "utf8");
const mapping = fs.readFileSync(MAPPING, "utf8");
const share = fs.readFileSync(SHARE, "utf8");
const server = fs.readFileSync(SERVER, "utf8");
const internalApi = fs.readFileSync(INTERNAL_API, "utf8");
const internalLib = fs.readFileSync(INTERNAL_LIB, "utf8");

assert.match(
  appJs,
  /'\/opportunity-radar'\s*:\s*\{\s*file:\s*'\/deal-capture-radar-with-ranked-list\.html'/,
  "canonical opportunity-radar must embed ranked-list Radar host"
);
assert.match(
  appJs,
  /preview.*inventory|inventory.*preview|'inventory'[\s\S]*'preview'/,
  "app shell must forward inventory/preview into embeds"
);
assert.match(
  appJs,
  /Also forward parent \/app\?preview=staged/,
  "app shell documents staged preview forward into embeds"
);

assert.match(host, /hotel-explorer\.js/, "ranked-list host loads Hotel Explorer");
assert.match(host, /hotel-detail-panel\.js/, "ranked-list host keeps detail panel");
assert.match(host, /auth-helper\.js/, "ranked-list host loads auth helper for internal-preview");
assert.match(
  host,
  /hotel-detail-panel\.js[\s\S]*hotel-explorer\.js/,
  "Hotel Explorer must load after Hotel Detail Panel"
);

assert.match(
  mapping,
  /mergeAuthorizedInternalPreviewHotels/,
  "map client must attempt authorized internal-preview merge"
);
assert.match(
  mapping,
  /isOpportunityRadarShareMode\(\)/,
  "share mode must remain gated"
);
assert.match(
  mapping,
  /internal-preview-hotels/,
  "client calls internal-preview-hotels endpoint"
);

assert.match(
  server,
  /\/api\/brand-presence\/internal-preview-hotels/,
  "server registers internal-preview-hotels route"
);
assert.match(
  server,
  /memberstackAuth[\s\S]*requireDealalityUser[\s\S]*requireAdminAccess[\s\S]*getBrandPresenceInternalPreviewHotels/,
  "internal-preview route must require auth + admin"
);

assert.match(
  internalApi,
  /internalPreview query flags do not authorize/,
  "query-alone unlock must be rejected"
);
assert.match(
  internalLib,
  /Hotel Property Census/,
  "Bethesda inclusion reads Hotel Property Census"
);
assert.match(
  internalLib,
  /authorizedByQueryAlone:\s*false/,
  "query alone never authorizes"
);

assert.doesNotMatch(
  share,
  /recLuxvwwxID7U2B8|bethesda/i,
  "Bethesda must not appear in hotel-explorer-share packs"
);

// Five Radar sections remain available via HotelDetailPanel embed contract
const hdp = fs.readFileSync(path.join(ROOT, "public/js/hotel-detail-panel.js"), "utf8");
for (const tab of ["submarket", "area", "demand", "access", "hotel"]) {
  assert.match(
    hdp,
    new RegExp(tab, "i"),
    `Hotel Detail Panel must retain ${tab} tab support`
  );
}

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-app-opportunity-radar-he-wiring-v1",
    canonicalRoute: "/opportunity-radar",
    embedFile: "/deal-capture-radar-with-ranked-list.html",
    bethesdaSharePackAbsent: true,
    internalPreviewAuthRequired: true,
  })
);
