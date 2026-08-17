import fs from "fs";

const h = fs.readFileSync("tmp-old-home-live-features.html", "utf8");
const pats = [
  "Brand Explorer",
  "Operator Explorer",
  "Opportunity Review",
  "Deal Readiness",
  "Radar",
  "Smart Matching",
  "Market Intelligence",
  "pf-card",
  "dealality-old-home-platform-features",
  "how-we-do",
  "How we do it",
  "platform-features",
  "HtmlEmbed",
];
for (const p of pats) console.log(p, h.split(p).length - 1);
console.log("css", h.match(/dealality-old-home-platform-features[^\"']*/g));
console.log("cards", h.match(/id=\"pf-card[^\"]*\"/g));
const i = h.indexOf("platform-features");
console.log("snippet\n", h.slice(Math.max(0, i - 200), i + 3500));

// Find Features titles near Brand Explorer
const be = h.indexOf("Brand Explorer");
console.log("\n--- around Brand Explorer ---\n", h.slice(Math.max(0, be - 800), be + 1200));
