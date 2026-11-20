#!/usr/bin/env node
import assert from "assert";
import {
  resolveOfficialBrandWebsite,
  harvestImagesFromBlocks,
  poolToGalleryCards,
  BE_FACTORY_2_0_HOTEL_IMAGE_DISCOVERY_V1,
} from "../lib/brand-explorer/factory-2/be-factory-2-hotel-image-discovery-v1.js";
import { isParentCompanyHomepage } from "../lib/brand-explorer/active-brand-website-corrections.js";

assert.strictEqual(BE_FACTORY_2_0_HOTEL_IMAGE_DISCOVERY_V1, "BE_FACTORY_2_0_HOTEL_IMAGE_DISCOVERY_V1");

const fairmont = resolveOfficialBrandWebsite({ brandName: "Fairmont", sources: { brandPage: "https://accor.com" } });
assert.ok(fairmont.url.includes("fairmont.com"), fairmont.url);
assert.strictEqual(fairmont.parentHomepage, false);

const parent = resolveOfficialBrandWebsite({
  brandName: "Unknown Brand XYZ",
  sources: { brandPage: "https://marriott.com" },
});
assert.strictEqual(isParentCompanyHomepage(parent.url, "", "Unknown Brand XYZ"), true);

const harvested = harvestImagesFromBlocks(
  [
    {
      slotKey: "materials.gallery.1",
      title: "Fairmont Banff Springs · Exterior",
      imageUrl: "https://cache.marriott.com/content/dam/marriott-digital/fairmont/banff.jpg",
      source: "https://fairmont.com",
    },
  ],
  "fairmont",
  "Fairmont"
);
// May be empty if same-brand affinity rejects opaque CDN path — still must not throw
assert.ok(Array.isArray(harvested));

const cards = poolToGalleryCards(
  {
    images: [
      {
        imageUrl: "https://example.com/hotel-exterior.jpg",
        title: "Test Hotel · Exterior | Fairmont",
        role: "Exterior",
        propertyName: "Test Hotel",
      },
    ],
  },
  "Fairmont",
  6
);
assert.strictEqual(cards.length, 1);

console.log("PASS be-factory-2-hotel-image-discovery-v1");
