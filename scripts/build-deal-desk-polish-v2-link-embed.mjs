import fs from "fs";

const hotel =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bde85c014ee4e80e65c24_deal-desk-coastal-hotel-480.jpg";
const cssUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bdf56570ac7789fe12096_oh-deal-desk-polish-v2.css";

let html = fs.readFileSync(
  "public/marketing/old-home-problem-deal-desk.v1.html",
  "utf8"
);
html = html.replace(/src="data:image\/[^"]+"/g, `src="${hotel}"`);
html = html.replace(/src="[^"]*coastal-hotel[^"]*"/g, `src="${hotel}"`);

const out = `<link id="oh-deal-desk" rel="stylesheet" href="${cssUrl}">\n${html}`;
fs.writeFileSync("docs/old-home-problem-deal-desk-embed-polish-v2-link.html", out);

console.log(
  JSON.stringify({
    chars: out.length,
    hasStyleTag: out.includes("<style"),
    hasLink: out.includes(cssUrl),
    hasHotelCdn: out.includes("6a6bde85c014ee4e80e65c24"),
    hasAnimation: /@keyframes|animation\s*:/.test(out),
    hasScript: /<script/.test(out),
    hasStrip: out.includes("dpd-strip"),
  })
);
