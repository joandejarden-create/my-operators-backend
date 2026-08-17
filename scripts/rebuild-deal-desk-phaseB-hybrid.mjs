import fs from "fs";

const css =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6c48e126a07d130ad33932_oh-deal-desk-cinematic-v1-phaseB.css";
const hotel =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bde85c014ee4e80e65c24_deal-desk-coastal-hotel-480.jpg";
const js =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6c48e19cbfe5ed843b0866_old-home-problem-deal-desk.v1.js";

let html = fs.readFileSync(
  "public/marketing/old-home-problem-deal-desk.v1.html",
  "utf8"
);
html = html.replace(/src="[^"]*coastal-hotel[^"]*"/g, `src="${hotel}"`);
const out = `<style id="oh-deal-desk">@import url("${css}");</style>\n${html}`;
fs.writeFileSync(
  "docs/old-home-problem-deal-desk-embed-cinematic-v1-hybrid.html",
  out
);
fs.writeFileSync("docs/_mcp-phaseB-embed-code.json", JSON.stringify(out));
fs.writeFileSync(
  "docs/old-home-problem-deal-desk-phaseB-cdn.json",
  JSON.stringify(
    {
      css,
      js,
      pvl: "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6c48e226a07d130ad339c3_dealality-old-home-platform-video-launcher.v20260731a.js",
      hotel,
    },
    null,
    2
  )
);
console.log("embedChars", out.length);
