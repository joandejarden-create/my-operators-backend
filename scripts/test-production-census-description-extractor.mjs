/**
 * Unit tests for census description extractor (no Airtable / no network).
 */
import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  isBookingBoilerplate,
  assessDescriptionQuality,
  extractOfficialPageEnrichment,
  buildGroundedAiSummary,
  selectBestDescriptionHit,
} from "../lib/research-engine-v2/production-census-description-extractor.js";
import {
  isPropertyLevelUrl,
  evaluateProviderReadiness,
} from "../lib/research-engine-v2/production-census-description-extraction.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const FIXTURE = join(
  ROOT,
  "fixtures/research-engine-v2/census-description-ihg-sample.html"
);
const TMP = join(ROOT, "reports/_tmp-desc-ihg.html");

function test(name, fn) {
  try {
    fn();
    console.log(`[PASS] ${name}`);
  } catch (err) {
    console.error(`[FAIL] ${name}`);
    throw err;
  }
}

test("rejects booking boilerplate", () => {
  assert.equal(
    isBookingBoilerplate(
      "Official site of Holiday Inn X. Read guest reviews and book your stay with our Best Price Guarantee."
    ),
    true
  );
  assert.equal(
    assessDescriptionQuality(
      "Official site of Holiday Inn X. Read guest reviews and book your stay with our Best Price Guarantee. Kids stay and eat free."
    ).ok,
    false
  );
});

test("accepts substantive narrative", () => {
  const text =
    "Located minutes from downtown Queretaro, this hotel offers an outdoor pool, on-site restaurant, and meeting space for guests.";
  const q = assessDescriptionQuality(text);
  assert.equal(q.ok, true);
  assert.ok(["High", "Medium"].includes(q.confidence));
  const summary = buildGroundedAiSummary(text);
  assert.ok(summary && summary.includes("outdoor pool"));
});

test("property-level URL classifier", () => {
  assert.equal(
    isPropertyLevelUrl(
      "https://www.ihg.com/holidayinn/hotels/us/en/tuxtla-gutierrez/tgzmx/hoteldetail"
    ),
    true
  );
  assert.equal(isPropertyLevelUrl("https://www.ihg.com/mexico"), false);
  assert.equal(
    isPropertyLevelUrl("https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap"),
    false
  );
  assert.equal(
    isPropertyLevelUrl("https://www.hilton.com/en/hotels/cywcedt-doubletree-celaya/"),
    true
  );
});

test("provider readiness prefers mapbox permanent", () => {
  const blocked = evaluateProviderReadiness({
    GEOCODING_PROVIDER: "google",
    GOOGLE_MAPS_API_KEY: "x",
  });
  assert.equal(blocked.approved_for_geocode_apply, false);
  assert.equal(blocked.route, "description_extraction");

  const mapbox = evaluateProviderReadiness({
    MAPBOX_ACCESS_TOKEN: "pk.test",
    MAPBOX_PERMANENT_GEOCODING: "1",
    GEOCODING_PROVIDER: "mapbox",
  });
  assert.equal(mapbox.mapbox_permanent_ready, true);
  assert.equal(mapbox.route, "geocode_apply");
});

test("IHG sample HTML extracts amenities + factual assembly", () => {
  if (!existsSync(FIXTURE) && existsSync(TMP)) {
    mkdirSync(dirname(FIXTURE), { recursive: true });
    writeFileSync(FIXTURE, readFileSync(TMP, "utf8").slice(0, 180000));
  }
  assert.ok(existsSync(FIXTURE), "missing fixtures/research-engine-v2/census-description-ihg-sample.html");
  const html = readFileSync(FIXTURE, "utf8");
  const pack = extractOfficialPageEnrichment(html, {
    family: "IHG",
    propertyName: "Holiday Inn Tuxtla Gutierrez",
    url: "https://www.ihg.com/holidayinn/hotels/us/en/tuxtla-gutierrez/tgzmx/hoteldetail",
  });
  assert.equal(pack.amenities.ok, true);
  assert.ok(pack.amenities.tags.length >= 2);
  assert.ok(pack.description);
  assert.ok(
    pack.description.method.includes("amenities") ||
      !/best price guarantee/i.test(pack.description.text)
  );
  const best = selectBestDescriptionHit([
    {
      text: "Official site of X. Read guest reviews and book your stay with our Best Price Guarantee.",
      method: "og",
      confidence: "Low",
      rejected: true,
    },
  ]);
  assert.equal(best, null);
});

console.log("All production-census-description-extractor tests passed.");
