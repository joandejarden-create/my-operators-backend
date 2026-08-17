import fs from "fs";
import crypto from "crypto";

const t = fs.readFileSync("tmp-old-home-live.html", "utf8");
const i = t.indexOf('id="testimonials"');
console.log("---TESTIMONIALS---");
console.log(t.slice(i, i + 5000));

const headLink = [...t.matchAll(/freeform-head[^"']+|dealality-old-home-freeform-head[^"']+/g)].map((m) => m[0]);
console.log("---HEAD LINKS---", headLink.slice(0, 10));

const cssLinks = [...t.matchAll(/href="(https:\/\/cdn\.prod\.website-files\.com\/[^"]+freeform[^"]+)"/g)].map((m) => m[1]);
console.log("---CSS---", cssLinks);

const jsLinks = [...t.matchAll(/src="(https:\/\/cdn\.prod\.website-files\.com\/[^"]+testimonial[^"]+)"/g)].map((m) => m[1]);
console.log("---JS---", jsLinks);

for (const page of ["public/marketing/dealality-landing-v9.html", "public/marketing/dealality-landing-v7.html", "public/marketing/dealality-old-home-owner.html"]) {
  if (!fs.existsSync(page)) continue;
  const h = fs.readFileSync(page, "utf8");
  const founder = h.match(/founder[\s\S]{0,1200}/i);
  console.log("\n===", page, "===");
  console.log(founder ? founder[0].slice(0, 900) : "no founder");
  const imgs = [...h.matchAll(/src="([^"]*founder[^"]*)"/gi)].map((m) => m[1]);
  console.log("imgs", imgs);
}
