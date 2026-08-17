import dotenv from "dotenv";
dotenv.config();
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { evaluateBrandPublicVisibility } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";
import { resolveBrandExplorerDisplayState } from "../lib/partner-intelligence/brand-explorer-display-state.js";

async function fetchBrand(id) {
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: id }, headers: {} }, res);
  return res.payload?.brand;
}

const brand = await fetchBrand("recwXZ5gVZ8ZH8ekA");
const be = brand?.brandExplorer || {};
const blocks = be.blocks || [];
const scenarios = blocks.filter((b) => /scenario/i.test(b.slotKey || ""));
const gallery = blocks.filter((b) => /gallery/i.test(b.slotKey || "") || b.section === "gallery");
const openings = blocks.filter((b) => /openings|property|examples/i.test(b.slotKey || ""));
const withImg = blocks.filter((b) => b.imageUrl || b.image?.url);
console.log({
  name: brand.name,
  blocks: blocks.length,
  scenarios: scenarios.length,
  gallery: gallery.length,
  openings: openings.length,
  withImg: withImg.length,
  shouldRender: brand.shouldRenderFullProfile,
  display: brand.brandExplorerDisplayState || be.displayState,
  activeProfileApproved: brand.activeProfileApproved ?? brand.governance?.activeProfileApproved,
  founderVR: brand.founderVisualReviewPass ?? brand.governance?.founderVisualReviewPass,
  companyValidated: brand.companyValidated ?? brand.governance?.companyValidated,
});

// Try resolve display with available inputs if exported shape allows
try {
  const pvql = await evaluateBrandPublicVisibility("bw-premier-collection");
  console.log(
    "PVQL",
    JSON.stringify(
      {
        display: pvql.publicDisplayState,
        full: pvql.publicFullProfile,
        render: pvql.shouldRenderFullProfile,
        lock: pvql.lockPass,
        failures: pvql.failures,
        recommended: pvql.recommendedAction,
        gatesFail: Object.entries(pvql.gateResults || {})
          .filter(([, g]) => g && g.pass === false)
          .map(([k]) => k),
      },
      null,
      2
    )
  );
} catch (e) {
  console.error("pvql err", e.message);
}
