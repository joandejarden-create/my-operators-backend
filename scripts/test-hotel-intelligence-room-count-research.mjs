#!/usr/bin/env node
/**
 * Unit tests for Room Count Research Engine (no live network).
 */
import assert from "node:assert/strict";
import {
  extractRoomCountsFromText,
  extractMultilingualRoomPhrases,
  classifySourceUrl,
  SOURCE_CATEGORIES,
  scoreRoomCountResearch,
  buildRoomCountQueries,
  RESEARCH_STATUS,
} from "../lib/hotel-intelligence/room-count-research/index.js";

// English explicit phrase
{
  const r = extractRoomCountsFromText(
    "The resort is featuring 184 guestrooms and suites overlooking the bay."
  );
  assert.ok(r.best);
  assert.equal(r.best.count, 184);
  assert.notEqual(r.best.confidence, "Hold");
}

// Spanish
{
  const hits = extractMultilingualRoomPhrases(
    "El hotel cuenta con 250 habitaciones y un spa."
  );
  assert.ok(hits.some((h) => h.count === 250 && h.language === "es"));
}

// Portuguese
{
  const hits = extractMultilingualRoomPhrases("Hotel com 120 quartos no centro.");
  assert.ok(hits.some((h) => h.count === 120 && h.language === "pt"));
}

// French
{
  const hits = extractMultilingualRoomPhrases("Hôtel de 95 chambres près de la plage.");
  assert.ok(hits.some((h) => h.count === 95 && h.language === "fr"));
}

// Do NOT treat room-type catalog length as keys — bare "King Room" alone yields no hit
{
  const r = extractRoomCountsFromText("King Room\nSuite\nDeluxe Room");
  assert.equal(r.best, null);
}

// Trust classification
assert.equal(
  classifySourceUrl("https://www.marriott.com/hotels/travel/example"),
  SOURCE_CATEGORIES.OFFICIAL_BRAND
);
assert.equal(
  classifySourceUrl("https://www.booking.com/hotel/mx/x.html"),
  SOURCE_CATEGORIES.OTHER
);

// Queries are capped / targeted
{
  const qs = buildRoomCountQueries({
    hotel_name: "Hotel Test",
    city: "Cancun",
    country: "Mexico",
    brand: "Independent",
  });
  assert.ok(qs.length <= 5);
  assert.ok(qs[0].q.includes("Hotel Test"));
}

// Confidence: multi-source agreement
{
  const scored = scoreRoomCountResearch([
    {
      value: 184,
      source_category: SOURCE_CATEGORIES.OFFICIAL_HOTEL,
      quote: "Featuring 184 guestrooms.",
      url: "https://hotel.example/about",
    },
    {
      value: 184,
      source_category: SOURCE_CATEGORIES.OFFICIAL_BRAND,
      quote: "184 guest rooms",
      url: "https://brand.example/hotel",
    },
  ]);
  assert.equal(scored.candidate_room_count, 184);
  assert.equal(scored.research_status, RESEARCH_STATUS.FOUND_MULTI_SOURCE);
  assert.ok(scored.confidence >= 0.9);
}

// Conflict flagged
{
  const scored = scoreRoomCountResearch([
    {
      value: 184,
      source_category: SOURCE_CATEGORIES.OFFICIAL_HOTEL,
      quote: "184 guestrooms",
    },
    {
      value: 160,
      source_category: SOURCE_CATEGORIES.NEWS,
      quote: "160-room hotel",
    },
  ]);
  assert.equal(scored.research_status, RESEARCH_STATUS.CONFLICT);
  assert.equal(scored.review_required, true);
}

console.log("ok - room-count research extract + confidence");
