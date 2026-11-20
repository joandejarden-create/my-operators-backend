#!/usr/bin/env node
import assert from "assert";
import {
  pickOpeningsHotelImages,
  buildOpeningsHotelExampleBlocks,
} from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import {
  isPeopleOrLifestyleMarketingImage,
  isLikelyHotelPropertyPhotograph,
} from "../lib/brand-explorer/be-permanent-six-image-gallery-standard-v1.js";

const ROOT = process.cwd();

for (const [slug, brand] of [
  ["sonesta-select", "Sonesta Select"],
  ["hampton-by-hilton", "Hampton by Hilton"],
  ["ritz-carlton", "The Ritz-Carlton"],
]) {
  const imgs = pickOpeningsHotelImages(ROOT, slug, brand, 3);
  assert.ok(imgs.length >= 3, `${slug}: need 3 openings hotel images`);
  for (const img of imgs) {
    assert.ok(!isPeopleOrLifestyleMarketingImage(img.imageUrl), `${slug}: people`);
    assert.ok(
      isLikelyHotelPropertyPhotograph(img.imageUrl, { role: img.scene }),
      `${slug}: not hotel ${img.imageUrl}`
    );
  }
  const blocks = buildOpeningsHotelExampleBlocks(ROOT, {
    slug,
    brandName: brand,
    parentCompany: "Parent",
    sources: {},
  });
  assert.ok(blocks.length >= 3, `${slug}: need 3 openings blocks`);
  for (const b of blocks) {
    assert.equal(b.slotKey, "footprint.openings");
    assert.ok(/^https?:\/\//i.test(b.imageUrl || ""), `${slug}: missing imageUrl`);
    assert.ok(/International Reference/i.test(b.title), `${slug}: title ${b.title}`);
  }
  console.log(`OK ${slug}: ${blocks.length} openings hotel example cards`);
}

console.log("test-be-factory-2-openings-hotel-images-v1: PASS");
