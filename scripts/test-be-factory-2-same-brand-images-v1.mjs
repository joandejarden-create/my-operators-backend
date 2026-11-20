#!/usr/bin/env node
/**
 * Gate: property-facing images must be same-brand (never sibling flag photos).
 * Waldorf Astoria must never receive Hilton Hotels & Resorts photography.
 */
import assert from "assert";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  pickScenarioHotelImages,
  loadGalleryCardsFromFixture,
  GALLERY_FIXTURE_BY_SLUG,
} from "../lib/brand-explorer/factory-2/be-factory-2-empty-brand-completion-slots-v1.js";
import {
  isSameBrandImage,
  isExactBrandGalleryFixture,
  assessSameBrandImageAffinity,
} from "../lib/brand-explorer/be-permanent-same-brand-image-affinity-v1.js";

const ROOT = process.cwd();

// 1) Fixture map is exact-brand only
for (const [slug, rel] of Object.entries(GALLERY_FIXTURE_BY_SLUG)) {
  assert.ok(
    isExactBrandGalleryFixture(slug, rel),
    `GALLERY_FIXTURE_BY_SLUG[${slug}] must be exact-brand fixture, got ${rel}`
  );
}
console.log(`OK exact-brand fixture map (${Object.keys(GALLERY_FIXTURE_BY_SLUG).length})`);

// 2) Hilton flagship image rejected for Waldorf
const hiltonPanama =
  "https://www.hilton.com/im/en/PTYHFHH/3368977/hilton-panama-exterior.jpg";
const reject = assessSameBrandImageAffinity("waldorf-astoria", hiltonPanama, {
  title: "Hilton Panama — Exterior",
  propertyName: "Hilton Panama",
});
assert.equal(reject.ok, false, "Waldorf must reject Hilton Panama exterior");
console.log("OK Waldorf rejects Hilton Hotels photography");

// 3) Waldorf Panama DAM accepted
const waldorfPanama = "https://www.hilton.com/im/en/PTYTPWA/10066365/building-2-.jpg";
assert.ok(
  isSameBrandImage("waldorf-astoria", waldorfPanama, {
    propertyName: "Waldorf Astoria Panama",
    propertyKey: "waldorf-astoria-panama",
  }),
  "Waldorf Panama DAM must pass"
);
console.log("OK Waldorf Panama DAM accepted");

// 4) pickScenarioHotelImages for Waldorf returns same-brand only
const imgs = pickScenarioHotelImages(ROOT, "waldorf-astoria", "Waldorf Astoria Hotels & Resorts", 3);
assert.ok(imgs.length >= 3, `Waldorf needs ≥3 scenario images, got ${imgs.length}`);
for (const img of imgs) {
  assert.ok(
    isSameBrandImage("waldorf-astoria", img.imageUrl, img),
    `cross-brand scenario image: ${img.imageUrl}`
  );
  assert.ok(!/ptyhfhh|hilton-panama|hilton%20hotels%20and%20resorts/i.test(img.imageUrl), img.imageUrl);
}
console.log(`OK Waldorf scenario images (${imgs.length}) are same-brand`);

// 5) Gallery cards same-brand
const gallery = loadGalleryCardsFromFixture(ROOT, "waldorf-astoria", "Waldorf Astoria Hotels & Resorts");
assert.ok(gallery.length >= 3, `Waldorf gallery need ≥3, got ${gallery.length}`);
for (const card of gallery) {
  assert.ok(isSameBrandImage("waldorf-astoria", card.imageUrl, card), card.imageUrl);
}
console.log(`OK Waldorf gallery (${gallery.length}) same-brand`);

// 6) Frozen payload (if present) has no Hilton flagship contamination on scenarios
const frozen = join(
  ROOT,
  "public/brand-explorer-phase2e-preview/waldorf-astoria-factory2-frozen-approval-payload-v1.json"
);
if (existsSync(frozen)) {
  const j = JSON.parse(readFileSync(frozen, "utf8"));
  const blocks = j.brandExplorer?.blocks || [];
  const bad = blocks.filter((b) => {
    const url = String(b.imageUrl || "");
    if (!url) return false;
    if (!/overview\.scenario\.|materials\.gallery\./i.test(b.slotKey || "")) return false;
    return !isSameBrandImage("waldorf-astoria", url, { title: b.title, body: b.body });
  });
  assert.equal(bad.length, 0, `frozen Waldorf still has cross-brand images: ${bad.map((b) => b.slotKey + "=" + b.imageUrl).join("; ")}`);
  console.log("OK frozen Waldorf payload same-brand");
}

console.log("test-be-factory-2-same-brand-images-v1: PASS");
