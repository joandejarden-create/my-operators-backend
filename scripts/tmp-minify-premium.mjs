import fs from "fs";

let html = fs.readFileSync("public/marketing/dealality-old-home-premium.html", "utf8");
let css = fs.readFileSync("public/marketing/dealality-old-home-premium.css", "utf8");
html = html.replace(/\r\n/g, "\n").replace(/>\s+</g, "><").trim();
css = css
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/\s+/g, " ")
  .replace(/\s*([{}:;,])\s*/g, "$1")
  .trim();
html = `<div class="dc-page--premium" id="dc-premium">${html}</div>`;
fs.writeFileSync(
  "tmp-premium-min.json",
  JSON.stringify({ html, css, htmlLen: html.length, cssLen: css.length })
);
console.log(html.length, css.length);
