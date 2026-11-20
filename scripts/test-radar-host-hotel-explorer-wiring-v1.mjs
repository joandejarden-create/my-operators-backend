#!/usr/bin/env node
/**
 * Static wiring gate: ranked-list Radar host must load the FULL canonical
 * Hotel Explorer (not a reduced 5-panel HDP shell).
 * HotelDetailPanel.open delegates to HotelExplorer.open when PRIMARY.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { assertHtmlLoadsCanonicalExplorerAssets } from "../lib/hotel-intelligence/he-canonical-explorer-assets-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOST = path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html");
const HE_JS = path.join(ROOT, "public/js/hotel-explorer.js");
const HDP_JS = path.join(ROOT, "public/js/hotel-detail-panel.js");
const SHARE = path.join(ROOT, "public/hotel-explorer-share.html");

const html = fs.readFileSync(HOST, "utf8");
const heJs = fs.readFileSync(HE_JS, "utf8");
const hdpJs = fs.readFileSync(HDP_JS, "utf8");
const shareHtml = fs.readFileSync(SHARE, "utf8");

const assets = assertHtmlLoadsCanonicalExplorerAssets(html, { label: "radar-host" });
assert.equal(
  assets.ok,
  true,
  `Radar host missing canonical HE assets: css=${assets.missingCss.join(",")} js=${assets.missingJs.join(",")}`,
);

assert.match(html, /hotel-detail-panel\.js/, "host must keep Hotel Detail Panel");
assert.match(html, /hotel-explorer\.js/, "host must load Hotel Explorer");
assert.match(html, /hotel-intelligence-workspace\.js/, "host must load HI workspace");
assert.match(html, /hotel-explorer\.css/, "host must load Hotel Explorer CSS");
assert.match(
  html,
  /hotel-intelligence-workspace\.css/,
  "host must load HI workspace CSS for tab parity",
);
assert.match(
  html,
  /hotel-detail-panel\.js[\s\S]*hotel-intelligence-workspace\.js[\s\S]*hotel-explorer\.js/,
  "Hotel Explorer must load after HDP + HI workspace",
);

assert.match(heJs, /__DEALALITY_HOTEL_EXPLORER_PRIMARY\s*=\s*true/);
assert.match(hdpJs, /__DEALALITY_HOTEL_EXPLORER_PRIMARY/);
assert.match(hdpJs, /HotelExplorer\.open/);

// Customer share packs must NOT gain Bethesda via this Phase 1 work.
assert.doesNotMatch(
  shareHtml,
  /recLuxvwwxID7U2B8|bethesda/i,
  "Bethesda must not appear in hotel-explorer-share packs",
);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-host-hotel-explorer-wiring-v1",
    host: "public/deal-capture-radar-with-ranked-list.html",
    hePrimaryFlag: true,
    canonicalAssetsOk: assets.ok,
    bethesdaSharePackAbsent: true,
  }),
);
