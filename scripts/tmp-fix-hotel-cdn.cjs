const fs = require("fs");
const hotel =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bde85c014ee4e80e65c24_deal-desk-coastal-hotel-480.jpg";
const path = "docs/old-home-problem-deal-desk-embed-phaseB2.html";
let html = fs.readFileSync(path, "utf8");
const beforeRel = html.includes("deal-desk-assets");
html = html.replace(
  /src="[^"]*coastal-hotel[^"]*"/g,
  `src="${hotel}"`
);
fs.writeFileSync(path, html);
console.log({
  beforeRel,
  afterHasCdn: html.includes(hotel),
  afterHasRel: html.includes("deal-desk-assets"),
});
