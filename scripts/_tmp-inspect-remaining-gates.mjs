#!/usr/bin/env node
import "dotenv/config";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";

async function fetch(id) {
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

const preferred = await fetch("recwl5JOYxlChuCAr");
const html = renderBrandExplorerHtmlForTest(preferred, {
  brandSlug: "preferred-hotels-and-resorts",
});
console.log("guestPsychographics:", preferred.guestPsychographics);
console.log("keyBrandDifferentiators:", preferred.keyBrandDifferentiators);
console.log("brandPositioning:", preferred.brandPositioning);
console.log("brandValueProposition:", preferred.brandValueProposition);
const s = JSON.stringify(preferred);
console.log("conversion-friendly in brand?", /conversion-friendly/i.test(s));
console.log(s.match(/.{0,100}conversion-friendly.{0,100}/i)?.[0]);

const premier = await fetch("recwXZ5gVZ8ZH8ekA");
const h2 = renderBrandExplorerHtmlForTest(premier, { brandSlug: "bw-premier-collection" });
let idx = 0;
while ((idx = h2.indexOf("oe-dd--empty", idx)) >= 0) {
  console.log("empty-dd:", h2.slice(Math.max(0, idx - 180), idx + 80).replace(/\s+/g, " "));
  idx += 12;
}
