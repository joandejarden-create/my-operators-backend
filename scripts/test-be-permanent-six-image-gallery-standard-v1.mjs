#!/usr/bin/env node
/**
 * Permanent six-image hotel gallery standard — regression gates.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_PERMANENT_SIX_IMAGE_GALLERY_STANDARD_V1,
  isBrandLogoGalleryAsset,
  isGenericGalleryCaption,
  captionFromPropertyImageUrl,
  validateSixImageGallery,
  decideGallerySlotApply,
  repairGalleryBlocksInPlace,
} from "../lib/brand-explorer/be-permanent-six-image-gallery-standard-v1.js";
import { buildCumulativeProposedBrand, PROVENANCE } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";
import { isLikelyLogoUrl } from "../lib/brand-explorer/be-phase2f-content-presentation-contract-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const preview = path.join(root, "public", "brand-explorer-phase2e-preview");

const logo =
  "https://stories-editor.hilton.com/wp-content/uploads/2023/07/Curio-Collection-by-Hilton.webp";
const hale =
  "https://stories-editor.hilton.com/wp-content/uploads/2026/09/Hale-Hokuala-Kauaʻi-Curio-Collection-by-Hilton-Exterior-with-Pool-and-Ocean.jpg";
const morrow =
  "https://stories-editor.hilton.com/wp-content/uploads/2022/11/The-Morrow-Hotel-Curio-Collection-by-Hilton_1.jpg";

assert.equal(BE_PERMANENT_SIX_IMAGE_GALLERY_STANDARD_V1, "BE_PERMANENT_SIX_IMAGE_GALLERY_STANDARD_V1");
assert.ok(isBrandLogoGalleryAsset(logo), "brand-name-only webp is a logo");
assert.ok(isLikelyLogoUrl(logo), "isLikelyLogoUrl must catch Curio-Collection-by-Hilton.webp");
assert.ok(!isBrandLogoGalleryAsset(hale), "hotel exterior is not a logo");
assert.ok(isGenericGalleryCaption("Property imagery"));
assert.ok(isGenericGalleryCaption("Hotel image"));
assert.ok(isGenericGalleryCaption("Image 1"));
assert.ok(!isGenericGalleryCaption("Zemi Miches Punta Cana | Beachfront Pool & Resort Grounds"));

const haleCaption = captionFromPropertyImageUrl(hale);
assert.ok(/Hale Hokuala/i.test(haleCaption));
assert.ok(/Exterior with Pool and Ocean/i.test(haleCaption));
assert.ok(/\|/.test(haleCaption));

const morrowCaption = captionFromPropertyImageUrl(morrow);
assert.equal(morrowCaption, "The Morrow Hotel | Property Photography");

const badGallery = [1, 2, 3, 4, 5, 6].map((n) => ({
  slotKey: `materials.gallery.${n}`,
  title: "Property imagery",
  imageUrl: n === 1 ? logo : hale,
  body: n === 1 ? logo : hale,
}));
const bad = validateSixImageGallery(badGallery);
assert.equal(bad.pass, false);
assert.ok(bad.failures.some((f) => f.code === "brand_logo_as_gallery_image"));
assert.ok(bad.failures.some((f) => f.code === "generic_gallery_caption"));
assert.ok(bad.failures.some((f) => f.code === "duplicate_gallery_image"));

const goodUrls = [
  morrow,
  hale,
  "https://stories-editor.hilton.com/wp-content/uploads/2026/07/Hotel-Palacio-Bellas-Artes-San-Sebastian-Curio-Collection-by-Hilton-Lobby-Lobby-Bar.jpg",
  "https://stories-editor.hilton.com/wp-content/uploads/2026/05/The-Monarch-San-Antonio-Curio-Collection-by-Hilton-Lobby.jpg",
  "https://stories-editor.hilton.com/wp-content/uploads/2026/07/Elika-Cave-Suites-Cappadocia-Curio-Collection-by-Hilton-Street-View.jpg",
  "https://example.com/hotels/zemi-miches-curio-beachfront-pool.jpg",
];
const goodGallery = goodUrls.map((url, i) => {
  const caption = captionFromPropertyImageUrl(url) || `Zemi Miches | Beachfront Pool & Resort Grounds`;
  return {
    slotKey: `materials.gallery.${i + 1}`,
    title: caption,
    body: caption,
    imageUrl: url,
  };
});
const good = validateSixImageGallery(goodGallery);
assert.equal(good.pass, true, JSON.stringify(good.failures, null, 2));
assert.equal(good.assetRegister.length, 6);

// Assembler must retain CURRENT hotel photos over logo / generic candidate
const current = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-gate1-renderer-parity-payload.json"), "utf8"),
);
const candidate = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-collection-phase2h-candidate.json"), "utf8"),
);
const { brand, assembly } = buildCumulativeProposedBrand(current, candidate);
const gallery = (brand.brandExplorer.blocks || []).filter((b) =>
  /^materials\.gallery\.\d+$/.test(b.slotKey || ""),
);
assert.equal(gallery.length, 6, "RIGHT must keep six gallery slots");
for (const g of gallery) {
  assert.ok(g.imageUrl, `${g.slotKey} must have imageUrl`);
  assert.ok(!isBrandLogoGalleryAsset(g.imageUrl), `${g.slotKey} must not be a logo`);
  assert.ok(!isGenericGalleryCaption(g.title), `${g.slotKey} caption must not be generic: ${g.title}`);
  const prov = assembly.provenanceBySlot[g.slotKey];
  assert.ok(
    prov.provenance === PROVENANCE.RETAINED_FROM_CURRENT ||
      prov.draftStatus === "INCOMPLETE_DRAFT" ||
      prov.publicationAction === "retain_current",
    `${g.slotKey} should retain CURRENT when candidate fails gallery standard (${JSON.stringify(prov)})`,
  );
}

const rejectLogo = decideGallerySlotApply(
  "materials.gallery.1",
  {
    slotKey: "materials.gallery.1",
    title: "Zemi Miches Punta Cana | Beachfront Pool",
    imageUrl: "https://v5.airtableusercontent.com/v3/u/x/zemi.jpg",
  },
  { slotKey: "materials.gallery.1", title: "Property imagery", imageUrl: logo },
);
assert.equal(rejectLogo.applyCandidate, false);

const repaired = repairGalleryBlocksInPlace([
  { slotKey: "materials.gallery.1", title: "Property imagery", imageUrl: logo, body: logo },
  { slotKey: "materials.gallery.2", title: "Property imagery", imageUrl: hale, body: hale },
]);
assert.equal(repaired[0].imageUrl, null);
assert.ok(/Hale Hokuala/i.test(repaired[1].title));

const contract = JSON.parse(
  fs.readFileSync(
    path.join(root, "configs/brand-explorer-permanent-content-standards-contract-v1.json"),
    "utf8",
  ),
);
assert.equal(contract.fixedCollections.imageGallery.count, 6);
assert.equal(
  contract.fixedCollections.imageGallery.permanentStandard,
  BE_PERMANENT_SIX_IMAGE_GALLERY_STANDARD_V1,
);
assert.ok(contract.fixedCollections.imageGallery.logoRejection.brandNameOnlyBasenamesAreLogos);
assert.ok(contract.fixedCollections.imageGallery.browserValidateAllSixGalleryCards);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_PERMANENT_SIX_IMAGE_GALLERY_STANDARD_V1,
      rightGalleryTitles: gallery.map((g) => g.title),
      badFailureCodes: [...new Set(bad.failures.map((f) => f.code))],
      airtableWrites: 0,
      founderApproved: false,
      curioGalleryStatus: "NOT_APPROVED",
      ascendGalleryStatus: "NOT_APPROVED",
    },
    null,
    2,
  ),
);
