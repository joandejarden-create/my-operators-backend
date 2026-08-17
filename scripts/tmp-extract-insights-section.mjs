import fs from "fs";

const html = fs.readFileSync("public/marketing/dealality-old-home-premium.html", "utf8");
const match = html.match(/<section id="insights"[\s\S]*?<\/section>/);
if (!match) {
  console.error("insights section not found");
  process.exit(1);
}
const out = match[0].replace(/\r\n/g, "\n").replace(/>\s+</g, "><").trim();
fs.writeFileSync("tmp-insights-section.html", out);
console.log(
  JSON.stringify({
    bytes: out.length,
    controlsAfter: out.indexOf("insights-controls") > out.indexOf("insights-grid"),
    hrefInsights: out.includes('href="#insights"'),
  })
);
