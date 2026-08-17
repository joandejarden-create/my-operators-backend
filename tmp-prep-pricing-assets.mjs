import fs from "fs";
const h = fs.readFileSync(
  "public/marketing/dealality-old-home-premium.html",
  "utf8"
);
const s = h.indexOf('<section id="pricing"');
const e = h.indexOf("</section>", s) + "</section>".length;
if (s < 0 || e < s) throw new Error("pricing section not found");
const html = h.slice(s, e);
fs.writeFileSync("tmp-pricing-insert.html", html);
console.log("chars", html.length);

const crypto = await import("crypto");
const css = fs.readFileSync(
  "public/marketing/dealality-old-home-pricing.v20260729b.css"
);
const js = fs.readFileSync("public/marketing/old-home-footer-oh.v20260729e.js");
console.log("css md5", crypto.createHash("md5").update(css).digest("hex"), css.length);
console.log("js md5", crypto.createHash("md5").update(js).digest("hex"), js.length);
