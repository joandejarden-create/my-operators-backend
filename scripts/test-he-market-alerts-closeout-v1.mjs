#!/usr/bin/env node
/**
 * Market Alerts ↔ HE closeout gates (HE-QL-023).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyHotelArticleRelevance,
  selectHotelNewsArticles,
  planHpcHotelAssociation,
  planHpcAssocFieldMerge,
  buildHotelNewsIdentity,
  HOTEL_ARTICLE_MATCH,
  parseHpcIdsFromTags,
  parseHpcIdsFromAssocField,
} from "../lib/hotel-intelligence/hotel-news-from-market-alerts-v1.js";
import { planMarketAlertsHotelAssociations } from "../lib/hotel-intelligence/market-alerts-hotel-association-sync-v1.js";
import {
  toMarketAlertsEvidenceCitation,
  marketAlertsArticlesToCitations,
  mergeMarketAlertsIntoCitations,
  proposedHpcHotelRecordIdsFieldSchema,
} from "../lib/hotel-intelligence/market-alerts-evidence-citations-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const hotelA = buildHotelNewsIdentity({
  id: "recHotelAAAAAAA01",
  name: "Alpha Boutique Hotel",
  brand: "Independent",
  city: "Austin",
});
const hotelB = buildHotelNewsIdentity({
  id: "recHotelBBBBBBB02",
  name: "Beta River Inn",
  brand: "Independent",
  city: "Austin",
});
const beth = buildHotelNewsIdentity({
  id: "recLuxvwwxID7U2B8",
  name: "Bethesda Marriott",
  brand: "Marriott Hotels",
  city: "Bethesda",
});

const articleOne = {
  id: "recAlertOne00001",
  fields: {
    Title: "Alpha Boutique Hotel completes lobby renovation",
    "Hotel / Project": "Alpha Boutique Hotel",
    "Source URL": "https://example.com/alpha-reno",
    "Source Name": "Hotel Dive",
    "Published At": "2026-09-01T00:00:00.000Z",
    Category: "Supply",
    "Dedupe ID": "dedupe-alpha-1",
    Tags: ["RSS"],
  },
};
const articleTwo = {
  id: "recAlertTwo00002",
  fields: {
    Title: "Alpha Boutique Hotel sold to regional sponsor",
    "Hotel / Project": "Alpha Boutique Hotel",
    "Source URL": "https://example.com/alpha-sale",
    "Source Name": "Costar News",
    "Published At": "2026-10-01T00:00:00.000Z",
    Category: "Deals",
    "Dedupe ID": "dedupe-alpha-2",
    Tags: ["RSS", "hpc:recHotelAAAAAAA01"],
  },
};
const articleMulti = {
  id: "recAlertMulti0003",
  fields: {
    Title: "Austin pair: Alpha Boutique Hotel and Beta River Inn announce joint CapEx",
    "Hotel / Project": "Alpha Boutique Hotel; Beta River Inn",
    "Source URL": "https://example.com/austin-pair",
    "Source Name": "Local Biz",
    "Published At": "2026-08-15T00:00:00.000Z",
    Category: "Supply",
    "Dedupe ID": "dedupe-multi-1",
    Tags: ["RSS"],
    "HPC Hotel Record IDs": "recHotelAAAAAAA01\nrecHotelBBBBBBB02",
  },
};
const articleDupUrl = {
  id: "recAlertDup00004",
  fields: {
    Title: "Alpha Boutique Hotel completes lobby renovation (wire)",
    "Hotel / Project": "Alpha Boutique Hotel",
    "Source URL": "https://example.com/alpha-reno?utm=x",
    "Source Name": "Wire",
    "Published At": "2026-09-02T00:00:00.000Z",
    Category: "Supply",
    "Dedupe ID": "dedupe-alpha-1",
    Tags: ["RSS"],
  },
};

// 1) Hotel with one verified article
{
  const selected = selectHotelNewsArticles(hotelA, [articleOne], { limit: 5 });
  assert.equal(selected.visible, true);
  assert.equal(selected.total, 1);
  assert.equal(selected.articles[0].id, "recAlertOne00001");
}

// 2) Hotel with multiple verified articles
{
  const selected = selectHotelNewsArticles(hotelA, [articleOne, articleTwo, articleMulti], {
    limit: 10,
  });
  assert.ok(selected.total >= 2);
  assert.ok(selected.articles.length >= 2);
}

// 3) Article linked to more than one hotel (assoc field + multi match)
{
  const forA = classifyHotelArticleRelevance(hotelA, articleMulti.fields);
  const forB = classifyHotelArticleRelevance(hotelB, articleMulti.fields);
  assert.equal(forA.classification, HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC);
  assert.equal(forB.classification, HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC);
  assert.ok(forA.reasons.includes("canonical_hpc_association"));
  assert.ok(forB.reasons.includes("canonical_hpc_association"));
  assert.deepEqual(parseHpcIdsFromAssocField(articleMulti.fields["HPC Hotel Record IDs"]), [
    "recHotelAAAAAAA01",
    "recHotelBBBBBBB02",
  ]);
  const selA = selectHotelNewsArticles(hotelA, [articleMulti], { limit: 5 });
  const selB = selectHotelNewsArticles(hotelB, [articleMulti], { limit: 5 });
  assert.equal(selA.articles[0].id, selB.articles[0].id);
}

// 4) Hotel with no matching articles → hidden
{
  const empty = selectHotelNewsArticles(beth, [articleOne, articleTwo, articleMulti], {
    limit: 5,
  });
  assert.equal(empty.visible, false);
  assert.equal(empty.articles.length, 0);
}

// 5) Article supporting a fact in Sources & Evidence (citation shape)
{
  const selected = selectHotelNewsArticles(hotelA, [articleTwo], { limit: 5 });
  const cites = marketAlertsArticlesToCitations(selected.articles, {
    hotelName: hotelA.name,
  });
  assert.equal(cites.length, 1);
  const c = cites[0];
  assert.equal(c.market_alerts_id, "recAlertTwo00002");
  assert.ok(c.url);
  assert.ok(c.publisher || c.provider);
  assert.ok(c.publication_date || c.publishedAt, "publication date distinct from access/verify");
  assert.ok(!c.observed_date, "do not stuff publication into observed_date");
  assert.ok(c.claim || c.excerpt);
  assert.equal(c.canonicalSource, "MarketAlerts");
  assert.equal(c.verification_status, "VERIFIED_HPC_ASSOCIATION");
  assert.equal(c.source_type, "Market Alerts");
}

// 6) Duplicate prevention + association idempotence + GET-only wiring
{
  const selected = selectHotelNewsArticles(hotelA, [articleOne, articleDupUrl], { limit: 5 });
  assert.equal(selected.total, 1, "dedupe by Dedupe ID / URL");
  const cites = marketAlertsArticlesToCitations(selected.articles);
  const merged = mergeMarketAlertsIntoCitations(cites, cites);
  assert.equal(merged.length, 1);
  const tag1 = planHpcHotelAssociation(["RSS"], hotelA.hpcId);
  const tag2 = planHpcHotelAssociation(tag1.nextTags, hotelA.hpcId);
  assert.equal(tag1.changed, true);
  assert.equal(tag2.changed, false);
  const field1 = planHpcAssocFieldMerge("", hotelA.hpcId);
  const field2 = planHpcAssocFieldMerge(field1.nextValue, hotelA.hpcId);
  assert.equal(field1.changed, true);
  assert.equal(field2.changed, false);
  const sync = planMarketAlertsHotelAssociations(
    [articleOne, articleMulti],
    [hotelA, hotelB],
    { apply: false, assocFieldAvailable: false }
  );
  assert.equal(sync.applyAuthorized, false);
  assert.equal(sync.wouldWriteCount, 0);
  assert.equal(sync.multiHotelCapable, true);
  assert.ok(sync.plans.every((p) => p.createsArticleDuplicate === false));
}

{
  const schema = proposedHpcHotelRecordIdsFieldSchema();
  assert.equal(schema.applyAuthorized, false);
  assert.equal(schema.fieldName, "HPC Hotel Record IDs");
}

// UI / API: Sources mount + GET-only news
{
  const ws = read("public/js/hotel-intelligence-workspace.js");
  assert.ok(ws.includes("mountMarketAlertsEvidenceForSources"));
  assert.ok(ws.includes("marketAlertsCitations"));
  assert.ok(ws.includes("Market Alerts"));
  assert.ok(ws.includes("market_alerts_id"));
  assert.ok(read("public/js/hotel-explorer.js").includes("mountMarketAlertsEvidenceForSources"));
  const api = read("api/hotel-explorer-news-articles.js");
  assert.ok(api.includes("evidenceCitations"));
  assert.ok(api.includes("X-Dealality-Write"));
  assert.ok(api.includes("writeOnRead"));
  assert.ok(!/\.create\(|\.update\(/.test(api));
  assert.ok(read("lib/hotel-intelligence/market-alerts-evidence-citations-v1.js").includes(
    "mergeMarketAlertsIntoCitations"
  ));
}

{
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  assert.ok(std.includes("Sources & Evidence"));
  assert.ok(/Market Alerts/.test(std));
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-023"));
}

// Citation helper smoke
{
  const cite = toMarketAlertsEvidenceCitation({
    id: "recX",
    headline: "Test",
    url: "https://example.com/t",
    publisher: "Pub",
    publishedAt: "2026-01-01",
    relevanceSummary: "Supports renovation claim",
    matchReasons: ["canonical_hpc_association"],
  });
  assert.equal(cite.verification_status, "VERIFIED_HPC_ASSOCIATION");
}

console.log("PASS test:he-market-alerts-closeout-v1");
