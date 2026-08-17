/**
 * Unit tests for Rooms / Keys extractor + mixed-use guardrails.
 */
import assert from "node:assert/strict";
import {
  extractRoomsKeysFromOfficialHtml,
  extractHotelOnlySplit,
  assessRoomsClaim,
  selectBestRoomsHit,
  isFalsePositiveRoomCount,
  assessMixedUseRisk,
} from "../lib/research-engine-v2/production-census-rooms-keys-extractor.js";

function test(name, fn) {
  try {
    fn();
    console.log(`[PASS] ${name}`);
  } catch (err) {
    console.error(`[FAIL] ${name}`);
    throw err;
  }
}

test("official hotel room count phrase → High", () => {
  const html = `<p>The hotel with 120 guest rooms offers an outdoor pool.</p>`;
  const pack = extractRoomsKeysFromOfficialHtml(html);
  const best = selectBestRoomsHit(pack.hits);
  assert.equal(best?.count, 120);
  assert.equal(best?.confidence, "High");
  assert.equal(best?.rejected, false);
});

test("json_ld numberOfRooms → High", () => {
  const html = `<script type="application/ld+json">{"@type":"Hotel","numberOfRooms":85}</script>`;
  const pack = extractRoomsKeysFromOfficialHtml(html);
  const best = selectBestRoomsHit(pack.hits);
  assert.equal(best?.count, 85);
  assert.equal(best?.method, "json_ld_numberOfRooms");
  assert.equal(best?.confidence, "High");
});

test("rooms plus residences split → hotel only 80", () => {
  const hit = extractHotelOnlySplit("The project includes 80 hotel rooms and 40 residences.");
  assert.equal(hit?.count, 80);
  assert.equal(hit?.hotel_only, true);
  assert.ok(hit?.note?.includes("40"));
});

test("units ambiguity → Hold", () => {
  const html = `<p>A 200 units mixed-use development near the beach.</p>`;
  const pack = extractRoomsKeysFromOfficialHtml(html);
  const held = pack.hits.find((h) => h.method === "phrase_N_units");
  assert.ok(held);
  assert.equal(held.confidence, "Hold");
  assert.equal(held.rejected, true);
});

test("including residences → Hold", () => {
  const risk = assessMixedUseRisk("120 keys including residences on the upper floors");
  assert.equal(risk.mixed_use_risk, true);
});

test("conflicting / false-positive VIC 22 claim rejected", () => {
  const assessed = assessRoomsClaim({
    value: 22,
    confidence: "Medium",
    source: "IHG hoteldetail page text (explicit room count)",
    evidence_url: "https://www.ihg.com/example/hoteldetail",
  });
  assert.equal(assessed.ok, false);
  assert.match(assessed.reason, /false_positive/);
});

test("JS \\x22rooms escape is false positive", () => {
  const html = String.raw`max\":20},\x22rooms\x22:{\x22Rooms\x22:\x22Rooms\x22`;
  assert.equal(isFalsePositiveRoomCount(html, 22, "loose_rooms"), true);
});

test("old press planned pipeline → Hold", () => {
  const html = `<p>The planned 200-key resort is under development near Cancun.</p>`;
  const pack = extractRoomsKeysFromOfficialHtml(html);
  const hit = pack.hits.find((h) => h.count === 200);
  assert.ok(hit);
  assert.equal(hit.confidence, "Hold");
});

test("suite-friendly rooms and suites phrase → High", () => {
  const html = `<div>Features 64 rooms and suites in the historic center.</div>`;
  const pack = extractRoomsKeysFromOfficialHtml(html);
  const best = selectBestRoomsHit(pack.hits);
  assert.equal(best?.count, 64);
  assert.equal(best?.confidence, "High");
});

console.log("All production-census-rooms-keys-extractor tests passed.");
