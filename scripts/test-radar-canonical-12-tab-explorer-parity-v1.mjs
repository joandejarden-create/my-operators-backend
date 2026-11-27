#!/usr/bin/env node
/**
 * Opportunity Radar must mount the COMPLETE canonical 12-tab Hotel Explorer.
 * Fails when Radar hosts a reduced HDP-only shell (missing HI workspace assets).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HE_ACTIVE_TAB_IDS,
  HE_ALL_TAB_IDS,
  HE_CANONICAL_TAB_IDS,
  HE_HI_WORKSPACE_TAB_IDS,
  HE_PRESERVED_DISABLED_TAB_IDS,
  HE_RADAR_EMBED_TAB_IDS,
  assertHtmlLoadsCanonicalExplorerAssets,
} from "../lib/hotel-intelligence/he-canonical-explorer-assets-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const RADAR = path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html");
const PREVIEW = path.join(ROOT, "public/hotel-explorer-preview.html");
const HE_JS = path.join(ROOT, "public/js/hotel-explorer.js");
const HDP_JS = path.join(ROOT, "public/js/hotel-detail-panel.js");
const APP_JS = path.join(ROOT, "public/app.js");

const radarHtml = fs.readFileSync(RADAR, "utf8");
const previewHtml = fs.readFileSync(PREVIEW, "utf8");
const heJs = fs.readFileSync(HE_JS, "utf8");
const hdpJs = fs.readFileSync(HDP_JS, "utf8");
const appJs = fs.readFileSync(APP_JS, "utf8");

const radarCheck = assertHtmlLoadsCanonicalExplorerAssets(radarHtml, {
  label: "opportunity-radar-host",
});
const previewCheck = assertHtmlLoadsCanonicalExplorerAssets(previewHtml, {
  label: "hotel-explorer-preview",
});

assert.equal(
  radarCheck.ok,
  true,
  `Radar missing canonical HE assets: css=${radarCheck.missingCss.join(",")} js=${radarCheck.missingJs.join(",")}`,
);
assert.equal(
  previewCheck.ok,
  true,
  `Preview missing canonical HE assets: css=${previewCheck.missingCss.join(",")} js=${previewCheck.missingJs.join(",")}`,
);

assert.match(
  appJs,
  /'\/opportunity-radar'\s*:\s*\{\s*file:\s*'\/deal-capture-radar-with-ranked-list\.html'/,
);

assert.match(heJs, /__DEALALITY_HOTEL_EXPLORER_PRIMARY\s*=\s*true/);
assert.match(hdpJs, /HotelExplorer\.open/);
assert.match(heJs, /HotelIntelligenceWorkspace/);
assert.match(heJs, /HotelContactIntelligence/);
assert.match(heJs, /HotelExplorerRoomsMeetingSpace/);
assert.match(heJs, /mountGuestSentiment/);

for (const tab of HE_ALL_TAB_IDS) {
  assert.match(
    heJs,
    new RegExp(`id:\\s*"${tab}"`),
    `hotel-explorer.js must register tab ${tab}`,
  );
}
assert.equal(HE_CANONICAL_TAB_IDS.length, 12);
assert.equal(HE_ALL_TAB_IDS.length, 12);
assert.equal(HE_ACTIVE_TAB_IDS.length, 11);
assert.ok(HE_PRESERVED_DISABLED_TAB_IDS.includes("sentiment"));
assert.match(heJs, /ALL_TABS/);
assert.match(radarHtml, /he-guest-sentiment-feature-flag-v1\.js/);
assert.match(previewHtml, /he-guest-sentiment-feature-flag-v1\.js/);

for (const tab of HE_RADAR_EMBED_TAB_IDS) {
  assert.match(heJs, new RegExp(`${tab}:`), `RADAR_TABS must include ${tab}`);
}
for (const tab of HE_HI_WORKSPACE_TAB_IDS) {
  assert.match(heJs, new RegExp(`${tab}:`), `HI_TABS must include ${tab}`);
}

// Reduced 5-panel embed remains for location tabs only — not a substitute for HI tabs.
assert.match(hdpJs, /data-panel="overview"/);
assert.match(hdpJs, /data-panel="submarket-snapshot"/);
assert.match(hdpJs, /data-panel="area-hotels"/);
assert.match(hdpJs, /data-panel="demand-drivers"/);
assert.match(hdpJs, /data-panel="access-connectivity"/);
assert.doesNotMatch(
  hdpJs,
  /data-panel="ownership"|data-panel="contacts"|data-panel="sentiment"/,
  "HDP embed must not become a second ownership/contacts/sentiment implementation",
);

// Legacy Census must not appear as Contact Intelligence SoT on Radar host page scripts.
assert.doesNotMatch(
  radarHtml,
  /Hotel Census(?!.*Property)/i,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:radar-canonical-12-tab-explorer-parity-v1",
      tabCount: HE_CANONICAL_TAB_IDS.length,
      activeCustomerTabs: HE_ACTIVE_TAB_IDS.length,
      preservedDisabledTabs: HE_PRESERVED_DISABLED_TAB_IDS,
      radarAssetsOk: radarCheck.ok,
      previewAssetsOk: previewCheck.ok,
      hiWorkspaceRequiredTabs: HE_HI_WORKSPACE_TAB_IDS.length,
      radarEmbedTabs: HE_RADAR_EMBED_TAB_IDS.length,
      certifications: {
        OPPORTUNITY_RADAR_CANONICAL_EXPLORER: true,
        TWELVE_TAB_COMPONENT_PARITY: true,
        ELEVEN_ACTIVE_CUSTOMER_TABS: true,
        GUEST_SENTIMENT_TAB_STATUS: "DISABLED_BY_FOUNDER",
        FULL_HE_E2E_VALIDATED: false,
      },
    },
    null,
    2,
  ),
);
