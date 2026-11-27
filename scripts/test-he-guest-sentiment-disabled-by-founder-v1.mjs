#!/usr/bin/env node
/**
 * HE Guest Sentiment founder disable — 11 active tabs + preserved disabled feature.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HE_ACTIVE_TAB_IDS,
  HE_ALL_TAB_IDS,
  HE_CANONICAL_TAB_IDS,
  HE_HI_WORKSPACE_ACTIVE_TAB_IDS,
  HE_PRESERVED_DISABLED_TAB_IDS,
  HE_CANONICAL_JS,
} from "../lib/hotel-intelligence/he-canonical-explorer-assets-v1.js";
import {
  HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER,
  buildGuestSentimentDisabledByFounderResponse,
  getHeGuestSentimentFeatureState,
  isHeGuestSentimentEnabled,
} from "../lib/hotel-intelligence/guest-sentiment/he-guest-sentiment-feature-flag-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

assert.equal(isHeGuestSentimentEnabled({}), false);
assert.equal(isHeGuestSentimentEnabled({ HE_GUEST_SENTIMENT_ENABLED: "false" }), false);
assert.equal(isHeGuestSentimentEnabled({ HE_GUEST_SENTIMENT_ENABLED: "true" }), true);

const off = getHeGuestSentimentFeatureState({ HE_GUEST_SENTIMENT_ENABLED: "false" });
assert.equal(off.status, HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER);
assert.equal(off.overridesPublicationRights, false);
assert.equal(off.scrapingAuthorized, false);
assert.equal(off.paidProviders, false);

const body = buildGuestSentimentDisabledByFounderResponse("recLuxvwwxID7U2B8");
assert.equal(body.status, HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER);
assert.equal(body.disabledByFounder, true);
assert.equal(body.available, false);
assert.equal(body.platforms.length, 0);
assert.equal(body.applyAuthorized, false);

assert.equal(HE_ALL_TAB_IDS.length, 12);
assert.equal(HE_CANONICAL_TAB_IDS.length, 12);
assert.equal(HE_ACTIVE_TAB_IDS.length, 11);
assert.deepEqual(HE_PRESERVED_DISABLED_TAB_IDS, ["sentiment"]);
assert.ok(!HE_ACTIVE_TAB_IDS.includes("sentiment"));
assert.ok(HE_ALL_TAB_IDS.includes("sentiment"));
assert.ok(!HE_HI_WORKSPACE_ACTIVE_TAB_IDS.includes("sentiment"));
assert.ok(HE_CANONICAL_JS.some((s) => s.includes("he-guest-sentiment-feature-flag-v1.js")));

const he = read("public/js/hotel-explorer.js");
assert.ok(he.includes("ALL_TABS"));
assert.ok(he.includes("filterActiveTabs") || he.includes("isGuestSentimentEnabled"));
assert.ok(he.includes("unavailableHtml") || he.includes("Guest Sentiment is temporarily"));

const flagJs = read("public/js/he-guest-sentiment-feature-flag-v1.js");
assert.ok(flagJs.includes("DISABLED_BY_FOUNDER"));
assert.ok(flagJs.includes("ENV_DEFAULT_ENABLED = false"));

const api = read("api/hotel-explorer-guest-sentiment.js");
assert.ok(api.includes("isHeGuestSentimentEnabled"));
assert.ok(api.includes("buildGuestSentimentDisabledByFounderResponse"));
assert.ok(api.includes("X-Dealality-Guest-Sentiment-Scraping"));

const server = read("server.js");
assert.ok(server.includes("/api/hotel-explorer/feature-flags"));
assert.ok(server.includes("getHeGuestSentimentFeatureState"));

const envEx = read(".env.example");
assert.ok(envEx.includes("HE_GUEST_SENTIMENT_ENABLED=false"));

const radar = read("public/deal-capture-radar-with-ranked-list.html");
const preview = read("public/hotel-explorer-preview.html");
assert.ok(radar.includes("he-guest-sentiment-feature-flag-v1.js"));
assert.ok(preview.includes("he-guest-sentiment-feature-flag-v1.js"));

const score = read("scripts/he50-final-completion-scorecard-v1.mjs");
assert.ok(score.includes('GUEST_SENTIMENT_TAB_STATUS: "DISABLED_BY_FOUNDER"'));
assert.ok(score.includes('FULL_HE_E2E_VALIDATED: "NO"'));

// Enabling env still does not claim rights override in flag module.
const on = getHeGuestSentimentFeatureState({ HE_GUEST_SENTIMENT_ENABLED: "true" });
assert.equal(on.enabled, true);
assert.equal(on.overridesPublicationRights, false);
assert.equal(on.status, "CONFIG_ENABLED_RIGHTS_GATED");

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:he-guest-sentiment-disabled-by-founder-v1",
      activeTabs: HE_ACTIVE_TAB_IDS.length,
      allTabs: HE_ALL_TAB_IDS.length,
      preservedDisabled: HE_PRESERVED_DISABLED_TAB_IDS,
      defaultStatus: HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER,
    },
    null,
    2,
  ),
);
