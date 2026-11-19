#!/usr/bin/env node
/**
 * HE Guest Sentiment — Brand / Operator subsection gates (HE-QL-022).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildGuestSentimentView,
  SENTIMENT_PLATFORMS,
  proposedAirtableSentimentSchema,
} from "../lib/hotel-intelligence/guest-sentiment/guest-sentiment-model-v1.js";
import {
  GUEST_THEME_TAXONOMY,
  mapPlatformCategoryToTheme,
  MIN_SAMPLE_FOR_PERCENT,
} from "../lib/hotel-intelligence/guest-sentiment/guest-theme-taxonomy-v1.js";
import {
  resolveGuestSentimentForHotel,
  buildGuestSentimentAirtableDryRun,
  loadStagedGuestSentimentPacket,
} from "../lib/hotel-intelligence/guest-sentiment/guest-sentiment-staged-store-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const BETH = "recLuxvwwxID7U2B8";
const KGPV = "recUNycnMwOVFX0hc";

// Taxonomy covers required hospitality themes
{
  const ids = new Set(GUEST_THEME_TAXONOMY.map((t) => t.id));
  for (const need of [
    "staff_service",
    "guestroom_condition",
    "cleanliness",
    "sleep_noise",
    "location_access",
    "food_beverage",
    "breakfast",
    "value",
    "amenities",
    "meetings_events",
    "maintenance",
    "accessibility",
    "arrival_departure",
  ]) {
    assert.ok(ids.has(need), "missing theme " + need);
  }
  assert.equal(mapPlatformCategoryToTheme("Cleanliness"), "cleanliness");
  assert.equal(mapPlatformCategoryToTheme("Value for money"), "value");
  assert.equal(MIN_SAMPLE_FOR_PERCENT, 30);
}

// Live mode hides staged packet (no live_ui_authorized)
{
  const live = resolveGuestSentimentForHotel(
    { id: BETH, name: "Bethesda Marriott" },
    { founderPreview: false }
  );
  assert.equal(live.visible, false, "live must hide unauthorized staged sentiment");
}

// Founder proposed preview shows Bethesda staged platforms without averaging
{
  const preview = resolveGuestSentimentForHotel(
    { id: BETH, name: "Bethesda Marriott" },
    { founderPreview: true }
  );
  assert.equal(preview.visible, true);
  assert.equal(preview.proposedPreview, true);
  assert.ok(preview.platforms.length >= 2);
  const scales = new Set(preview.platforms.map((p) => p.scaleMax));
  assert.ok(scales.size >= 2, "must retain incompatible native scales");
  assert.equal(preview.trends.available, false, "no fabricated trends");
  assert.ok(
    String(preview.rightsChip || "").toLowerCase().includes("rights"),
    "rights chip required in proposed preview"
  );
  // No invented Google platform when unresolved
  assert.ok(!preview.platforms.some((p) => p.platform === "GOOGLE"));
}

// Wrong hotel / KGPV without packet → hidden
{
  const kgpv = resolveGuestSentimentForHotel(
    { id: KGPV, name: "Krystal Grand Puerto Vallarta" },
    { founderPreview: true }
  );
  assert.equal(kgpv.visible, false);
}

// Property match: staged packet only for Bethesda HPC
{
  const pkt = loadStagedGuestSentimentPacket(BETH);
  assert.ok(pkt);
  assert.equal(pkt.packet.hpcId, BETH);
  for (const o of pkt.packet.observations) {
    assert.ok(o.listingUrl, "listing URL required");
    assert.ok(o.platformPropertyId, "platform property id required");
    assert.notEqual(o.rightsStatus, "LICENSED_OK");
  }
}

// Never average ratings across platforms in view
{
  const view = buildGuestSentimentView(
    { id: BETH, name: "Bethesda Marriott" },
    {
      live_ui_authorized: false,
      founder_preview_render: true,
      observations: [
        {
          platform: "TRIPADVISOR",
          rating: 4.1,
          scaleMax: 5,
          reviewCount: 10,
          listingUrl: "https://www.tripadvisor.com/x",
          platformPropertyId: "d1",
          rightsStatus: "RIGHTS_REVIEW_REQUIRED",
          categoryScores: [],
        },
        {
          platform: "BOOKING",
          rating: 8.0,
          scaleMax: 10,
          reviewCount: 10,
          listingUrl: "https://www.booking.com/x",
          platformPropertyId: "b1",
          rightsStatus: "RIGHTS_REVIEW_REQUIRED",
          categoryScores: [],
        },
      ],
      history: [],
    },
    { founderPreview: true }
  );
  assert.ok(!JSON.stringify(view).includes("averageRating"));
  assert.ok(!view.platforms.some((p) => p.ratingDisplay && p.ratingDisplay.includes("6.05")));
}

// Small sample: percentClaimAllowed false
{
  const view = buildGuestSentimentView(
    { id: "recXXXXXXXXXXXXXX", name: "Test" },
    {
      live_ui_authorized: true,
      totalReviewSample: 5,
      observations: [
        {
          platform: "BOOKING",
          rating: 8,
          scaleMax: 10,
          reviewCount: 5,
          listingUrl: "https://example.com",
          categoryScores: [{ label: "Cleanliness", score: 9, scaleMax: 10 }],
        },
      ],
      history: [],
    }
  );
  assert.ok(view.themes.length);
  assert.equal(view.themes[0].percentClaimAllowed, false);
}

// Unsupported: empty observations stay hidden even with founder preview
{
  const empty = buildGuestSentimentView(
    { id: BETH },
    { founder_preview_render: true, observations: [] },
    { founderPreview: true }
  );
  assert.equal(empty.visible, false);
}

// Duplicate platform property ids should not invent second hotel match for other HPC
{
  const other = loadStagedGuestSentimentPacket("recAAAAAAAAAAAAAAAA");
  assert.equal(other, null);
}

// Airtable dry-run never apply-authorized
{
  const dry = buildGuestSentimentAirtableDryRun(BETH);
  assert.equal(dry.applyAuthorized, false);
  assert.ok(dry.observations.length >= 1);
  const schema = proposedAirtableSentimentSchema();
  assert.equal(schema.applyAuthorized, false);
}

// UI: Guest Sentiment inside Brand/Operator — not a new tab
{
  const ws = read("public/js/hotel-intelligence-workspace.js");
  assert.ok(ws.includes("data-hiw-guest-sentiment-host"));
  assert.ok(ws.includes("Guest Sentiment"));
  assert.ok(ws.includes("mountGuestSentiment"));
  assert.ok(!/id:\s*"guest-sentiment"|id:\s*"sentiment"/.test(ws));
  const tabs = ws.match(/var TABS = \[[\s\S]*?\];/);
  assert.ok(tabs);
  assert.ok(!/Guest Sentiment/.test(tabs[0]));
  assert.ok(ws.includes("Brand / Operator"));
  assert.ok(ws.includes("data-hiw-hotel-news-host"), "news host preserved");
}

{
  const hex = read("public/js/hotel-explorer.js");
  assert.ok(hex.includes("mountGuestSentiment"));
}

{
  const server = read("server.js");
  assert.ok(server.includes("/api/hotel-explorer/hotels/:hotelId/guest-sentiment"));
  const api = read("api/hotel-explorer-guest-sentiment.js");
  assert.ok(api.includes("X-Dealality-Write"));
  assert.ok(api.includes("X-Dealality-Paid-Providers"));
  assert.ok(!/\.create\(|\.update\(/.test(api));
  assert.ok(!/apify|rapid|demand.?api|places\.googleapis/i.test(api));
}

{
  const css = read("public/css/hotel-explorer.css");
  assert.ok(css.includes("hiw-guest-sentiment-host"));
  assert.ok(css.includes("hiw-sentiment-link"));
}

{
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  assert.ok(std.includes("Guest Sentiment"));
  assert.ok(std.includes("Brand / Operator"));
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-022"));
}

assert.ok(SENTIMENT_PLATFORMS.TRIPADVISOR.scaleMax === 5);
assert.ok(SENTIMENT_PLATFORMS.BOOKING.scaleMax === 10);

console.log("PASS test:he-guest-sentiment-brand-operator-v1");
