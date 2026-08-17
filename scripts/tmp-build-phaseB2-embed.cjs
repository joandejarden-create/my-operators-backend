const fs = require("fs");
const css =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6c5d6d19799fe674fa34ab_oh-deal-desk-cinematic-v1-phaseB2.css";
const html = fs.readFileSync(
  "public/marketing/old-home-problem-deal-desk.v1.html",
  "utf8"
);
const embed =
  `<style id="oh-deal-desk">@import url("${css}");</style>\n` + html;
fs.writeFileSync("docs/old-home-problem-deal-desk-embed-phaseB2.html", embed);
fs.writeFileSync(
  "docs/old-home-problem-deal-desk-embed-cinematic-v1-hybrid.html",
  `<style id="oh-deal-desk">@import url("${css}");</style>\n`
);
console.log("embed bytes", Buffer.byteLength(embed));
