import { crawlMarriottCountrySitemaps } from "../lib/marriott-brand-directory-extract.js";

const c = await crawlMarriottCountrySitemaps({
  countrySlugs: ["turks-and-caicos-islands"],
  delayMs: 0,
});
console.log("Hotels:", c.hotels.length);
for (const h of c.hotels) {
  console.log(h.marshaCode, h.name, h.website);
}
