import "../load-env.js";
import { listRegistryAssetsForBrand } from "../lib/partner-intelligence/brand-asset-registry-workflow.js";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { WOODSPRING_PROPERTY_CATALOG } from "../lib/partner-intelligence/brand-explorer-woodspring-real-property-examples-writer.js";

const brandId = "recsOd51NzRPYsMko";
const req = { query: { brandId, refresh: "1" }, headers: {} };
const res = { setHeader() {}, status() { return this; }, json(p) { this.payload = p; } };
await getBrandLibraryBrandById(req, res);
const brand = res.payload?.brand;
const blocks = brand?.brandExplorer?.blocks || [];
const openings = blocks.filter((b) => b.slotKey === "footprint.openings");
const gallery = blocks.filter((b) => /^materials\.gallery\./.test(b.slotKey || ""));
console.log("OPENINGS");
for (const b of openings) {
  console.log(b.recordId, b.title, b.imageUrl?.slice(0, 80));
}
console.log("GALLERY");
for (const b of gallery) {
  console.log(b.slotKey, b.recordId, b.title, b.imageUrl ? "has image" : "no image");
}
const reg = await listRegistryAssetsForBrand(brandId);
for (const slot of ["footprint.openings", "materials.gallery.1", "materials.gallery.2", "materials.gallery.3"]) {
  const assets = reg.filter((a) => a.recommendedExplorerSlot === slot);
  console.log("REG", slot, assets.map((a) => ({ id: a.id, name: a.assetName?.slice(0, 50), src: a.sourceUrl?.slice(0, 90) })));
}
