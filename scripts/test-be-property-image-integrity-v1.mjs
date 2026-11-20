#!/usr/bin/env node
/**
 * Property image integrity — reject publication thumbnails as hotel photos.
 * Unit + proposed-profile gates. Browser audit is separate evidence.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_PROPERTY_IMAGE_INTEGRITY_V1,
  PROPERTY_IMAGE_FAIL,
  isUnsuitablePropertyDisplayImage,
  filenameMarksPublicationGraphic,
  separateResearchSourceFromPropertyImage,
  applyPropertyImageIntegrityToBlocks,
  validatePropertyFacingImages,
  findVerifiedPropertyImageReplacement,
  VERIFIED_PROPERTY_IMAGE_REPLACEMENTS,
} from "../lib/brand-explorer/be-property-image-integrity-v1.js";
import { buildCumulativeProposedBrand } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixturePath = path.join(
  root,
  "fixtures/brand-explorer-property-image-integrity/royal-palm-travel-weekly-publication-graphic.json",
);
const preview = path.join(root, "public", "brand-explorer-phase2e-preview");

assert.equal(BE_PROPERTY_IMAGE_INTEGRITY_V1, "BE_PROPERTY_IMAGE_INTEGRITY_V1");
assert.ok(VERIFIED_PROPERTY_IMAGE_REPLACEMENTS.length >= 1);

// --- Reject rules ---
assert.equal(
  filenameMarksPublicationGraphic(
    "royal-palm-galapagos-curio-collection-by-hilton-santa-cruz-island-tw.jpg",
  ),
  true,
);
assert.equal(
  isUnsuitablePropertyDisplayImage("https://cdn.example.com/x.jpg", {
    imageFilename: "hotel-tw.jpg",
  }).code,
  PROPERTY_IMAGE_FAIL.FILENAME_PUBLICATION_MARKER,
);
assert.equal(
  isUnsuitablePropertyDisplayImage(
    "https://www.travelweekly.com/ext/resources/2022/07/share-card.jpg",
  ).code,
  PROPERTY_IMAGE_FAIL.TRADE_PRESS_HOST,
);
assert.equal(
  isUnsuitablePropertyDisplayImage(
    "https://www.travelweekly.com/ext/resources/2022/07/masthead.jpg",
  ).unsuitable,
  true,
);
assert.equal(
  isUnsuitablePropertyDisplayImage(
    "https://cdn.example.com/brands/Curio-Collection-by-Hilton.webp",
  ).code,
  PROPERTY_IMAGE_FAIL.LOGO,
);
assert.equal(
  isUnsuitablePropertyDisplayImage(
    "https://stories-editor.hilton.com/wp-content/uploads/2022/07/Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial_Courtesy-of-Royal-Palm-Galapagos-Curio-Collection-by-Hilton.jpg",
    { imageFilename: "Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial_Courtesy-of-Royal-Palm-Galapagos-Curio-Collection-by-Hilton.jpg" },
  ).unsuitable,
  false,
);

// Generic / misleading gallery caption must fail validation
const genericCaptionGate = validatePropertyFacingImages([
  {
    slotKey: "materials.gallery.2",
    title: "Property imagery",
    imageUrl: "https://cdn.example.com/hotels/ok.jpg",
    imageFilename: "ok.jpg",
  },
]);
assert.equal(genericCaptionGate.pass, false);
assert.ok(
  genericCaptionGate.failures.some((f) => f.code === PROPERTY_IMAGE_FAIL.GENERIC_CAPTION),
);

// --- Research vs display separation ---
const sep = separateResearchSourceFromPropertyImage({
  title: "Royal Palm Galapagos",
  body: "https://www.travelweekly.com/story\nhttps://www.hilton.com/en/hotels/gpsrpqq-royal-palm-galapagos/",
  imageUrl: "https://cdn.example.com/x-tw.jpg",
  imageFilename: "x-tw.jpg",
});
assert.ok(sep.researchSourceUrl && /travelweekly/i.test(sep.researchSourceUrl));
assert.equal(sep.propertyImageUrl, null);
assert.ok(sep.rejectedDisplayImageUrl);
assert.equal(sep.verificationStatus, "REJECTED_NON_PROPERTY_VISUAL");

// --- Fixture repair ---
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const before = validatePropertyFacingImages(fixture.blocks);
assert.equal(before.pass, false, "fixture must FAIL before integrity repair");
assert.ok(before.failureCount >= 2);

// --- Airtable CDN regression (filename stripped — founder popup defect) ---
const cdnOnly = applyPropertyImageIntegrityToBlocks([
  {
    slotKey: "footprint.openings",
    title: "Royal Palm Galapagos Curio Collection by Hilton — Santa Cruz Island",
    body: "https://www.hilton.com/en/hotels/gpsrpqq-royal-palm-galapagos/",
    imageUrl:
      "https://v5.airtableusercontent.com/v3/u/59/59/1795104000000/etxh9bS3cPqMIzlMsvuKaQ/hashTokenWithoutTwMarker",
    imageFilename: "",
  },
]);
assert.equal(
  cdnOnly.changedCount,
  1,
  "Airtable CDN Royal Palm without -tw filename must still receive verified Hilton aerial",
);
assert.ok(
  String(cdnOnly.blocks[0].imageUrl).includes("stories-editor.hilton.com"),
  "CDN regression must swap to Hilton Stories aerial",
);
assert.ok(
  findVerifiedPropertyImageReplacement({
    title: "Royal Palm Galapagos Curio Collection by Hilton",
  }),
);

const repaired = applyPropertyImageIntegrityToBlocks(fixture.blocks, {
  forceVerifiedReplacements: true,
});
assert.ok(repaired.changedCount >= 1);

const royal = repaired.blocks.find((b) => /royal\s*palm/i.test(b.title || ""));
assert.ok(royal);
assert.ok(
  String(royal.imageUrl).includes(fixture.expected.royalPalmReplacementUrlContains),
  `expected Hilton aerial, got ${royal.imageUrl}`,
);
assert.equal(royal.__propertyImageIntegrity.caption, fixture.expected.royalPalmCaption);
assert.ok(
  /travelweekly/i.test(royal.body),
  "Travel Weekly research citation must remain in body",
);
assert.ok(!/-tw\.jpg/i.test(royal.imageFilename || ""));

const logoCard = repaired.blocks.find((b) => /Unrelated Logo/i.test(b.title || ""));
assert.ok(logoCard);
assert.ok(!logoCard.imageUrl, "logo must not remain as display image");
assert.equal(logoCard.__propertyImageIntegrity.researchRequired, true);

const trade = repaired.blocks.find((b) => /Trade press/i.test(b.title || ""));
assert.ok(trade);
assert.ok(!trade.imageUrl);

// Non-property-facing slots must be left alone
const proof = repaired.blocks.find((b) => b.slotKey === "overview.proof.1");
assert.ok(proof.imageUrl && /travelweekly/i.test(proof.imageUrl));

const after = validatePropertyFacingImages(repaired.blocks);
assert.equal(
  after.pass,
  true,
  `after repair still failing:\n${JSON.stringify(after.failures, null, 2)}`,
);

// --- Proposed RIGHT Curio path ---
const current = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-gate1-renderer-parity-payload.json"), "utf8"),
);
const candidate = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-collection-phase2h-candidate.json"), "utf8"),
);

const leftOpenings = (current.brandExplorer.blocks || []).filter(
  (b) => b.slotKey === "footprint.openings" && /royal\s*palm/i.test(b.title || ""),
);
assert.ok(leftOpenings.length >= 1);
const leftRoyal = leftOpenings[0];
assert.ok(
  /-tw\.jpg/i.test(leftRoyal.imageFilename || ""),
  "LEFT source must still carry the Travel Weekly attachment filename (founder failure preserved on CURRENT)",
);

const { brand, assembly } = buildCumulativeProposedBrand(current, candidate);
assert.ok(assembly.propertyImageIntegrity);
assert.ok(assembly.propertyImageIntegrity.changedCount >= 1);

const rightRoyal = (brand.brandExplorer.blocks || []).find(
  (b) => b.slotKey === "footprint.openings" && /royal\s*palm/i.test(b.title || ""),
);
assert.ok(rightRoyal);
assert.ok(
  String(rightRoyal.imageUrl).includes("Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial"),
  `RIGHT must use Hilton Stories aerial, got ${rightRoyal.imageUrl}`,
);
assert.equal(
  rightRoyal.__propertyImageIntegrity.caption,
  "Royal Palm Galápagos | Hotel & Guest Villas",
);
assert.ok(!/-tw\.jpg/i.test(rightRoyal.imageFilename || ""));

// LEFT object untouched
assert.ok(/-tw\.jpg/i.test(leftRoyal.imageFilename || ""));

const rightValidation = validatePropertyFacingImages(brand.brandExplorer.blocks || []);
assert.equal(
  rightValidation.pass,
  true,
  JSON.stringify(rightValidation.failures.slice(0, 10), null, 2),
);

const replacement = findVerifiedPropertyImageReplacement(leftRoyal);
assert.ok(replacement);
assert.ok(/stories\.hilton\.com/i.test(replacement.sourceUrl));

// Contract pointer
const contract = JSON.parse(
  fs.readFileSync(
    path.join(root, "configs/brand-explorer-permanent-content-standards-contract-v1.json"),
    "utf8",
  ),
);
assert.equal(
  contract.fixedCollections.propertyFacingImages.permanentStandard,
  BE_PROPERTY_IMAGE_INTEGRITY_V1,
);
assert.equal(
  contract.fixedCollections.propertyFacingImages.articleThumbnailsAreNotPropertyPhotos,
  true,
);

const lessons = JSON.parse(
  fs.readFileSync(
    path.join(root, "configs/brand-explorer-founder-quality-lessons-registry-v1.json"),
    "utf8",
  ),
);
assert.ok(lessons.lessons.some((l) => l.lessonId === "BE-QL-PROPERTY-IMAGE-001"));

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_PROPERTY_IMAGE_INTEGRITY_V1,
      fixtureFailuresBefore: before.failureCount,
      integrityChangedOnProposed: assembly.propertyImageIntegrity.changedCount,
      leftFilename: leftRoyal.imageFilename,
      rightImageBasename: path.basename(String(rightRoyal.imageUrl).split("?")[0]),
      rightCaption: rightRoyal.__propertyImageIntegrity.caption,
      airtableWrites: 0,
      founderApproved: false,
      curioAscend: "NOT_APPROVED",
    },
    null,
    2,
  ),
);
