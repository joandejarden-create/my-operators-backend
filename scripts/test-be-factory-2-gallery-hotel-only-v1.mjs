#!/usr/bin/env node
/**
 * Gate: Factory gallery cards must be hotel property photography only
 * (exterior / interior / rendering) — never people/lifestyle marketing.
 * Captions must include a scene label that matches the image cues.
 */
import assert from "assert";
import {
  loadGalleryCardsFromFixture,
} from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import {
  isPeopleOrLifestyleMarketingImage,
  isLikelyHotelPropertyPhotograph,
  galleryRoleToSceneLabel,
} from "../lib/brand-explorer/be-permanent-six-image-gallery-standard-v1.js";

const ROOT = process.cwd();

assert.ok(
  isPeopleOrLifestyleMarketingImage(
    "https://www.hilton.com/im/en/PTYHFHH/18472409/emotions-2.jpg"
  ),
  "emotions lifestyle rejected"
);
assert.ok(
  !isLikelyHotelPropertyPhotograph(
    "https://marriott-hotels.marriott.com/wp-content/uploads/sites/9/2025/12/mc-lobby-apec-kv-2024-15759-1024x630.jpg",
    { role: "public_space_lobby" }
  ),
  "APEC KV lifestyle lobby rejected"
);
assert.equal(
  galleryRoleToSceneLabel("property_setting", {
    imageUrl: "https://example.com/mhrs.1238136_2500x1875.jpg",
  }),
  null,
  "opaque property_setting is not Exterior"
);
assert.equal(
  galleryRoleToSceneLabel("exterior_arrival", {
    imageUrl: "https://cache.marriott.com/content/dam/marriott-renditions/YULLE/yulle-exterior-0246-hor-pano.jpg",
  }),
  "Exterior"
);

for (const [slug, brand] of [
  ["waldorf-astoria", "Waldorf Astoria Hotels & Resorts"],
  ["hampton-by-hilton", "Hampton by Hilton"],
  ["hyatt-regency", "Hyatt Regency"],
  ["courtyard-by-marriott", "Courtyard by Marriott"],
]) {
  const cards = loadGalleryCardsFromFixture(ROOT, slug, brand);
  assert.ok(cards.length >= 4, `${slug}: expected >=4 hotel photos, got ${cards.length}`);
  for (const c of cards) {
    assert.ok(!isPeopleOrLifestyleMarketingImage(c.imageUrl), `${slug}: people image slipped through ${c.imageUrl}`);
    assert.ok(
      isLikelyHotelPropertyPhotograph(c.imageUrl, { role: c.role }),
      `${slug}: not hotel photo ${c.imageUrl}`
    );
    assert.ok(
      /Exterior|Guest Room|Lobby|F&B|Pool|Wellness|Meetings|Amenity|Rendering/i.test(c.title),
      `${slug}: caption missing scene label: ${c.title}`
    );
    assert.ok(!/\bExterior\b/i.test(c.title) || /exterior|arrival|facade|aerial|cochere/i.test(c.imageUrl + c.title),
      `${slug}: Exterior caption without exterior cues: ${c.title} ${c.imageUrl}`);
  }
  console.log(`OK ${slug}: ${cards.length} hotel-only cards`);
}

console.log("test-be-factory-2-gallery-hotel-only-v1: PASS");
