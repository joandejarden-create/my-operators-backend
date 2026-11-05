/**
 *   node scripts/test-ava-digital-authority-v2-identity-and-links-v1.mjs
 */
import assert from "node:assert/strict";
import {
  buildPropertyUrlIdentityGraph,
  classifyDestinationAgainstGraph,
  isBrandCorporateHomepage,
  PROPERTY_URL_CLASSES,
} from "../lib/ai-visibility-audit-action-plan/property-url-identity-graph-v1.js";
import { extractAnchors } from "../lib/ai-visibility-audit-action-plan/digital-authority-discovery-v2.js";

assert.equal(isBrandCorporateHomepage("https://www.marriott.com/"), true);
assert.equal(
  isBrandCorporateHomepage(
    "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/"
  ),
  false
);

const graph = buildPropertyUrlIdentityGraph(
  { property: { propertyId: "adp_x", name: "Test Hotel", website: "https://www.testhotel.com/" } },
  {
    brandPropertyPages: [
      "https://www.marriott.com/en-us/hotels/abcd-test-hotel/overview/",
    ],
  }
);
assert.ok(graph.urls.some((u) => u.urlClass === PROPERTY_URL_CLASSES.HOTEL_OWNED_SITE));
assert.ok(graph.urls.some((u) => u.urlClass === PROPERTY_URL_CLASSES.BRAND_PROPERTY_PAGE));

const hotelMatch = classifyDestinationAgainstGraph(
  "https://www.testhotel.com/meetings/",
  graph
);
assert.equal(hotelMatch.match, true);

const brandHome = classifyDestinationAgainstGraph("https://www.marriott.com/", graph);
assert.equal(brandHome.match, false);

const html = `<html><body><a href="https://www.testhotel.com/">Official</a><a href="/rooms">Rooms</a></body></html>`;
const anchors = extractAnchors(html, "https://example.com/page");
assert.ok(anchors.some((a) => /testhotel\.com/.test(a.href)));
assert.ok(anchors.some((a) => a.href.includes("example.com/rooms")));

console.log(JSON.stringify({ ok: true, graphUrls: graph.urls.length, anchors: anchors.length }, null, 2));
