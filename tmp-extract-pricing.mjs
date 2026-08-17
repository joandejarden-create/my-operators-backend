import fs from "fs";
const h = fs.readFileSync("tmp-old-home-live.html", "utf8");
const i = h.indexOf('id="pricing"');
console.log("pricing idx", i);
if (i >= 0) {
  const end = h.indexOf("</section>", i);
  fs.writeFileSync("tmp-pricing-section.html", h.slice(Math.max(0, i - 8), (end > i ? end : i + 12000) + 10));
  console.log("wrote tmp-pricing-section.html", end - i);
}
const links = [...h.matchAll(/href="([^"]+)"/g)]
  .map((x) => x[1])
  .filter((u) => /signup|access|brand|operator|request|opportunity|for-brands/i.test(u));
console.log("cta-ish", [...new Set(links)].slice(0, 50));
console.log("pricing css", [...h.matchAll(/dealality-old-home-pricing[^"']+/g)].map((m) => m[0]));
console.log("footer js", [...h.matchAll(/old-home-footer[^"']+/g)].map((m) => m[0]));
