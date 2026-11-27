#!/usr/bin/env node
/**
 * HE Guest Sentiment / Reputation dashboard — Brand / Operator (HE-QL-022 / HE-QL-025 / HE-QL-029).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildGuestSentimentView,
  SENTIMENT_PLATFORMS,
  REPUTATION_OVERVIEW_PLATFORMS_V1,
  buildReputationPlatformOverview,
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

// Live mode shows seven-platform shell but withholds unauthorized scores
{
  const live = resolveGuestSentimentForHotel(
    { id: BETH, name: "Bethesda Marriott" },
    { founderPreview: false }
  );
  assert.equal(live.visible, true, "live must show seven-platform shell");
  assert.equal(live.liveRightsRestricted, true, "live must mark rights-restricted shell");
  assert.equal(live.platforms.length, 0, "live must not copy Proposed scores");
  assert.equal(live.platformOverview?.length, 7);
  assert.ok(
    live.platformOverview.some((p) => p.status === "RIGHTS_RESTRICTED"),
    "live must flag rights-restricted platforms without scores"
  );
  assert.ok(!live.platformOverview.some((p) => p.available === true && p.rating != null));
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
  // Available platforms stay accurate; overview adds catalog (no invented Google scores)
  assert.ok(!preview.platforms.some((p) => p.platform === "GOOGLE"));
  assert.equal(preview.dashboardVariant, "guest-reputation-experience-v2");
  assert.equal(preview.platformOverview?.length, 7);
  assert.equal(preview.overviewPlatformCount, REPUTATION_OVERVIEW_PLATFORMS_V1.length);
  const available = preview.platformOverview.filter((p) => p.available);
  const unavailable = preview.platformOverview.filter((p) => !p.available);
  assert.equal(available.length, 3);
  assert.equal(unavailable.length, 4);
  assert.ok(unavailable.every((p) => p.rating == null && p.reviewCount == null));
  assert.ok(unavailable.every((p) => p.statusLabel));
  assert.ok(preview.categoryPerformance?.available === true);
  assert.ok(preview.categoryPerformance?.platforms?.length >= 1);
  assert.equal((preview.executiveInsights || []).length, 4);
  const roles = new Set(preview.executiveInsights.map((c) => c.role));
  assert.ok(roles.has("RELATIVE_STRENGTHS"));
  assert.ok(roles.has("AREAS_TO_INVESTIGATE"));
  assert.ok(roles.has("RECOMMENDED_FOLLOW_UP"));
  assert.ok(roles.has("TREND_DATA_CONFIDENCE"));
  const investigate = preview.executiveInsights.find(
    (c) => c.id === "areas_to_investigate"
  );
  assert.ok(
    /value/i.test(String(investigate?.finding || "")) ||
      /value/i.test(String(investigate?.metric || "")),
    "Bethesda Value for money should surface as relative investigate — not complaint proof"
  );
  assert.ok(!/regularly complain|frequently praised/i.test(JSON.stringify(preview)));
  assert.equal(preview.reviewThemes?.available, false);
  assert.ok(
    preview.platforms.every((p) => Array.isArray(p.categoryScores)),
    "platforms expose categoryScores for dashboard bars"
  );
  // Different coverage packet shape still builds without averaging
  const sparse = buildGuestSentimentView(
    { id: "recTESTSPARSE0001", name: "Sparse Coverage Hotel" },
    {
      founder_preview_render: true,
      live_ui_authorized: false,
      observations: [
        {
          platform: "TRIPADVISOR",
          rating: 3.8,
          scaleMax: 5,
          reviewCount: 12,
          observedAt: "2026-11-19",
          listingUrl: "https://www.tripadvisor.com/example",
          categoryScores: [{ label: "Cleanliness", score: 4.0, scaleMax: 5 }],
        },
      ],
      history: [],
    },
    { founderPreview: true }
  );
  assert.equal(sparse.visible, true);
  assert.equal(sparse.platforms.length, 1);
  assert.equal(sparse.platformOverview.length, 7);
  assert.equal(sparse.platformOverview.filter((p) => p.available).length, 1);
  assert.equal(sparse.categoryPerformance.platforms.length, 1);
  assert.ok(!String(JSON.stringify(sparse)).includes("Frequently praised"));
}

// KGPV / no packet — founder preview shows seven unavailable cards (no invented scores)
{
  const kgpv = resolveGuestSentimentForHotel(
    { id: KGPV, name: "Krystal Grand Puerto Vallarta" },
    { founderPreview: true }
  );
  assert.equal(kgpv.visible, true);
  assert.equal(kgpv.platforms.length, 0);
  assert.equal(kgpv.platformOverview.length, 7);
  assert.ok(kgpv.platformOverview.every((p) => p.available === false));
  assert.ok(kgpv.platformOverview.every((p) => p.rating == null && p.reviewCount == null));
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

// Empty observations in founder preview → seven unavailable cards, no invented zeros
{
  const empty = buildGuestSentimentView(
    { id: BETH, name: "Empty Hotel" },
    { founder_preview_render: true, observations: [] },
    { founderPreview: true }
  );
  assert.equal(empty.visible, true);
  assert.equal(empty.platformOverview.length, 7);
  assert.ok(empty.platformOverview.every((p) => p.available === false));
  assert.ok(!JSON.stringify(empty).includes('"rating":0'));
}

// Live unauthorized without founder preview stays hidden
{
  const liveHidden = buildGuestSentimentView(
    { id: BETH },
    { founder_preview_render: true, observations: [] },
    { founderPreview: false }
  );
  assert.equal(liveHidden.visible, false);
}

{
  assert.equal(REPUTATION_OVERVIEW_PLATFORMS_V1.length, 7);
  const overview = buildReputationPlatformOverview([]);
  assert.equal(overview.length, 7);
  assert.ok(overview.every((p) => p.available === false));
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

// UI: Guest Sentiment tab hosts reputation + news (HE-QL-038)
{
  const ws = read("public/js/hotel-intelligence-workspace.js");
  assert.ok(ws.includes("data-hiw-guest-sentiment-host"));
  assert.ok(ws.includes("Guest Reputation"));
  assert.ok(ws.includes("hiw-rep-scorecards"));
  assert.ok(ws.includes("Category Performance"));
  assert.ok(ws.includes("Sources, Methodology &amp; Limitations"));
  assert.ok(ws.includes("hiw-rep-scorecard--unavailable"));
  assert.ok(ws.includes("platformOverview"));
  assert.ok(ws.includes("hasReputationOverview"));
  assert.ok(ws.includes("hiw-rep-insights--executive"));
  assert.ok(!ws.includes("Frequently praised"));
  assert.ok(ws.includes("mountGuestSentiment"));
  assert.ok(/id:\s*"sentiment"/.test(ws));
  const tabs = ws.match(/var TABS = \[[\s\S]*?\];/);
  assert.ok(tabs);
  assert.ok(/Guest Sentiment/.test(tabs[0]));
  assert.ok(!/Brand \/ Operator/.test(tabs[0]));
  assert.ok(!/hiw-section__h">Franchise &amp; Brand Status/.test(ws));
  assert.ok(!/hiw-section__h">Current Operator/.test(ws));
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
  assert.ok(api.includes("executiveInsights"));
  assert.ok(api.includes("categoryPerformance"));
  assert.ok(api.includes("reviewThemes"));
  assert.ok(!/\.create\(|\.update\(/.test(api));
  assert.ok(!/apify|rapid|demand.?api|places\.googleapis/i.test(api));
}

{
  const css = read("public/css/hotel-explorer.css");
  assert.ok(css.includes("hiw-guest-sentiment-host"));
  assert.ok(css.includes("hiw-sentiment-link"));
  assert.ok(css.includes("hiw-rep-scorecard"));
  assert.ok(css.includes("hiw-rep-scorecard--unavailable"));
  assert.ok(css.includes("hiw-rep-bar__fill"));
  // Dynamic Chain Scale accent (HE-QL-038) — not hardcoded cyan or pinned single purple
  const repBlock = css.slice(css.indexOf("/* Guest Reputation"));
  assert.ok(!/#38bdf8/.test(repBlock), "reputation CSS must not use #38bdf8");
  assert.ok(
    /\.hiw-rep-bar__fill[\s\S]*?background:\s*var\(--accent--primary-1/.test(repBlock)
  );
  assert.ok(
    /\.hiw-sentiment-link\s*\{[\s\S]*?color:\s*var\(--accent--primary-1/.test(repBlock)
  );
  assert.ok(!/linear-gradient\(90deg,\s*#6c72ff\s*0%,\s*#38bdf8/.test(css));
}

{
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  assert.ok(std.includes("Guest Sentiment"));
  assert.ok(std.includes("Category Performance"));
  assert.ok(std.includes("HE-QL-029"));
  assert.ok(std.includes("HE-QL-038"));
  assert.ok(std.includes("Seven-platform overview"));
  assert.ok(std.includes("Dynamic Chain Scale accents"));
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-022"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-025"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-029"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-038"));
}

assert.ok(SENTIMENT_PLATFORMS.TRIPADVISOR.scaleMax === 5);
assert.ok(SENTIMENT_PLATFORMS.BOOKING.scaleMax === 10);

console.log("PASS test:he-guest-sentiment-brand-operator-v1");
