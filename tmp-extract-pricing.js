const fs = require("fs");
const h = fs.readFileSync("tmp-old-home-live.html", "utf8");
const i = h.indexOf('id="pricing"');
console.log("pricing idx", i);
if (i >= 0) {
  const end = h.indexOf("</section>", i);
  console.log(h.slice(i - 8, (end > i ? end : i + 12000) + 10));
}
const links = [...h.matchAll(/href="([^"]+)"/g)]
  .map((x) => x[1])
  .filter((u) => /signup|access|brand|operator|request|opportunity|for-brands/i.test(u));
console.log("cta-ish", [...new Set(links)].slice(0, 50));
const pricingCss = [...h.matchAll(/dealality-old-home-pricing[^"']+/g)].map((m) => m[0]);
console.log("pricing css", pricingCss);
const footerJs = [...h.matchAll(/old-home-footer[^"']+/g)].map((m) => m[0]);
console.log("footer js", footerJs);
