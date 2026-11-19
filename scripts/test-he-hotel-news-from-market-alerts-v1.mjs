#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HOTEL_ARTICLE_MATCH,
  classifyHotelArticleRelevance,
  selectHotelNewsArticles,
  planHpcHotelAssociation,
  parseHpcIdsFromTags,
  buildHotelNewsIdentity,
} from "../lib/hotel-intelligence/hotel-news-from-market-alerts-v1.js";
import { planMarketAlertsHotelAssociations } from "../lib/hotel-intelligence/market-alerts-hotel-association-sync-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const beth = buildHotelNewsIdentity({
  id: "recLuxvwwxID7U2B8",
  name: "Bethesda Marriott",
  brand: "Marriott Hotels",
  managementCompany: "Marriott International",
  economicOwner: "Mullen Real Estate Capital",
  city: "Bethesda",
  aliases: ["Marriott Bethesda"],
});

// Brand-only → not hotel news
{
  const v = classifyHotelArticleRelevance(beth, {
    Title: "Marriott International Fuels Growth Plans in Greece",
    Summary: "Marriott International announced nine deal signings.",
    "Brand Involved": "Marriott",
    Tags: ["RSS"],
  });
  assert.equal(v.classification, HOTEL_ARTICLE_MATCH.ORGANIZATION_CONTEXT_ONLY);
}

// Sponsor-only → not hotel news
{
  const v = classifyHotelArticleRelevance(beth, {
    Title: "Mullen Real Estate Capital expands Mid-Atlantic portfolio",
    Summary: "The sponsor acquired assets across the region.",
    "Owner / Developer": "Mullen Real Estate Capital",
  });
  assert.equal(v.classification, HOTEL_ARTICLE_MATCH.ORGANIZATION_CONTEXT_ONLY);
}

// Hotel-specific via Hotel / Project
{
  const v = classifyHotelArticleRelevance(beth, {
    Title: "Bethesda Marriott sold to regional sponsor",
    Summary: "Transaction closes in Maryland.",
    "Hotel / Project": "Bethesda Marriott",
  });
  assert.equal(v.classification, HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC);
}

// Former / alias name
{
  const v = classifyHotelArticleRelevance(beth, {
    Title: "Marriott Bethesda completes lobby renovation",
    Summary: "Guest areas refreshed.",
    "Hotel / Project": "Marriott Bethesda",
  });
  assert.ok(
    v.classification === HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC ||
      v.classification === HOTEL_ARTICLE_MATCH.MULTI_HOTEL_SPECIFIC
  );
}

// Canonical HPC tag
{
  const v = classifyHotelArticleRelevance(beth, {
    Title: "Unrelated headline without name",
    Summary: "No property tokens.",
    Tags: ["RSS", "hpc:recLuxvwwxID7U2B8"],
  });
  assert.equal(v.classification, HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC);
  assert.deepEqual(parseHpcIdsFromTags(["hpc:recLuxvwwxID7U2B8", "RSS"]), [
    "recLuxvwwxID7U2B8",
  ]);
}

// Dedup + hide empty
{
  const rows = [
    {
      id: "a1",
      fields: {
        Title: "Bethesda Marriott ownership update",
        "Hotel / Project": "Bethesda Marriott",
        "Source URL": "https://example.com/beth-1?utm_source=x",
        "Source Name": "Hotel Dive",
        "Published At": "2024-01-15T00:00:00.000Z",
        Category: "Deals",
        "Dedupe ID": "dup1",
      },
    },
    {
      id: "a2",
      fields: {
        Title: "Bethesda Marriott ownership update (syndicate)",
        "Hotel / Project": "Bethesda Marriott",
        "Source URL": "https://example.com/beth-1",
        "Source Name": "Other",
        "Published At": "2024-01-14T00:00:00.000Z",
        Category: "Deals",
        "Dedupe ID": "dup1",
      },
    },
    {
      id: "a3",
      fields: {
        Title: "Marriott International quarterly results",
        "Brand Involved": "Marriott",
        "Source URL": "https://example.com/marriott-q",
        Category: "Capital",
      },
    },
  ];
  const selected = selectHotelNewsArticles(beth, rows, { limit: 5 });
  assert.equal(selected.visible, true);
  assert.equal(selected.total, 1);
  assert.equal(selected.articles.length, 1);
  assert.ok(!/ORGANIZATION_CONTEXT|HOTEL_SPECIFIC|recLux/.test(JSON.stringify(selected.articles[0].relevanceSummary)));
}

{
  const empty = selectHotelNewsArticles(beth, [], { limit: 5 });
  assert.equal(empty.visible, false);
  assert.equal(empty.articles.length, 0);
}

// Idempotent association
{
  const first = planHpcHotelAssociation(["RSS"], "recLuxvwwxID7U2B8");
  assert.equal(first.changed, true);
  const second = planHpcHotelAssociation(first.nextTags, "recLuxvwwxID7U2B8");
  assert.equal(second.changed, false);
  assert.equal(second.reason, "already_associated");
}

{
  const sync = planMarketAlertsHotelAssociations(
    [
      {
        id: "recAlert1",
        fields: {
          Title: "Bethesda Marriott sold",
          "Hotel / Project": "Bethesda Marriott",
          Tags: ["RSS"],
        },
      },
    ],
    [beth],
    { apply: false }
  );
  assert.equal(sync.applyAuthorized, false);
  assert.equal(sync.wouldWriteCount, 0);
  assert.equal(sync.planned, 1);
  assert.equal(sync.plans[0].tagPlan.changed, true);
}

// Wiring
const ws = read("public/js/hotel-intelligence-workspace.js");
assert.ok(ws.includes("Hotel News &amp; Articles") || ws.includes("data-hiw-hotel-news-host"));
assert.ok(ws.includes("mountHotelNewsArticles"));
assert.ok(read("public/js/hotel-explorer.js").includes("mountHotelNewsArticles"));
assert.ok(read("server.js").includes("/api/hotel-explorer/hotels/:hotelId/news-articles"));
assert.ok(read("api/hotel-explorer-news-articles.js").includes("X-Dealality-Write"));
assert.ok(!/table\.create\(|\.update\(/.test(read("api/hotel-explorer-news-articles.js")));

const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
assert.ok(std.includes("Hotel News & Articles") || std.includes("hotel-news-from-market-alerts"));

console.log("PASS test:he-hotel-news-from-market-alerts-v1");
