#!/usr/bin/env node
import "dotenv/config";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import {
  buildImageIdentity,
  evaluateImageUniqueness,
} from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";

function mockRes() {
  return {
    headers: {},
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

const res = mockRes();
await getBrandLibraryBrandById({ query: { brandId: "recdeh1NsP4gjrv80" }, headers: {} }, res);
const blocks = res.payload.brand.brandExplorer.blocks || [];
for (const b of blocks.filter((x) => /^materials\.gallery/.test(x.slotKey || ""))) {
  const id = buildImageIdentity(b.imageUrl || "", {});
  console.log(
    b.slotKey,
    Boolean(b.imageUrl),
    id.sourceImageId || id.duplicateGroupId,
    String(b.imageUrl || "").slice(0, 90)
  );
}
const uniq = evaluateImageUniqueness({
  brand: res.payload.brand,
  presentationRows: blocks,
  brandSlug: "bw-signature-collection",
});
console.log({
  pass: uniq.pass,
  gallerySlot: uniq.gallerySlotCount,
  galleryDistinct: uniq.galleryDistinctCount,
  dups: uniq.duplicates,
});
console.log(
  "gallery statuses",
  (uniq.gallery || []).map((g) => ({
    slot: g.slotKey,
    status: g.uniquenessStatus,
    src: g.sourceImageId,
  }))
);
