#!/usr/bin/env node
/**
 * Combines markup + CSS + JS into a single Webflow Code Embed snippet,
 * and a local preview.html for Phase B review.
 *
 * Phase B keeps feature PNGs as external assets (CDN or relative).
 * Production Webflow update is deferred — this build estimates size only.
 */
const fs = require("fs");
const path = require("path");

const dir = __dirname;
let css = fs.readFileSync(path.join(dir, "many-futures.css"), "utf8");
let js = fs.readFileSync(path.join(dir, "many-futures.js"), "utf8");
let markup = fs.readFileSync(path.join(dir, "markup.html"), "utf8");

function minifyCss(input) {
  return input
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\s+/g, " ")
    .replace(/\s*([{}:;,>~+])\s*/g, "$1")
    .replace(/;}/g, "}")
    .trim();
}

function minifyJs(input) {
  return input
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|[^:])\/\/.*$/gm, "$1")
    .replace(/\s+/g, " ")
    .replace(/\s*([{}();,:?=+\-*/<>!&|])\s*/g, "$1")
    .replace(/;}/g, "}")
    .trim();
}

function minifyMarkup(input) {
  return input
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/>\s+</g, "><")
    .replace(/\s{2,}/g, " ")
    .trim();
}

/* Phase B: keep relative asset paths in local preview.
   Production CDN map is prepared but not uploaded in Phase B. */
const CDN_BASE =
  "https://cdn.jsdelivr.net/gh/OWNER/REPO@BRANCH/webflow-embeds/many-futures";

const FEATURE_FILES = [
  "brand-explorer-desktop.png",
  "brand-explorer-mobile.png",
  "operator-explorer-desktop.png",
  "operator-explorer-mobile.png",
  "operator-track-record-desktop.png",
  "operator-track-record-mobile.png",
  "fee-estimator-desktop.png",
  "fee-estimator-mobile.png",
  "radar-desktop.png",
  "radar-mobile.png",
  "opportunity-review-desktop.png",
  "opportunity-review-mobile.png",
  "deal-compare-desktop.png",
  "deal-compare-mobile.png",
  "smart-matching-desktop.png",
  "smart-matching-mobile.png",
  "deal-readiness-desktop.png",
  "deal-readiness-mobile.png",
  "clause-library-desktop.png",
  "clause-library-mobile.png",
  "financial-term-library-desktop.png",
  "financial-term-library-mobile.png",
  "submit-proposal-desktop.png",
  "submit-proposal-mobile.png",
];

let prodMarkup = markup;
prodMarkup = prodMarkup.split("assets/hotel-temp.jpg").join(`${CDN_BASE}/assets/hotel-temp.jpg`);
for (const file of FEATURE_FILES) {
  prodMarkup = prodMarkup
    .split(`assets/features/${file}`)
    .join(`${CDN_BASE}/assets/features/${file}`);
}

const cssOut = minifyCss(css);
const jsOut = minifyJs(js);
const markupOut = minifyMarkup(prodMarkup);

const embed = `<style>${cssOut}</style>${markupOut}<script>${jsOut}</script>`;
const out = path.join(dir, "..", "many-futures-section.html");
fs.writeFileSync(out, embed);

/* Short Webflow loader alternative when embed exceeds ~45–50KB safe inline size */
const loader = `<!-- Many Futures Phase B — CDN loader (Webflow HtmlEmbed) -->
<link rel="stylesheet" href="${CDN_BASE}/many-futures.css" />
<div id="mf-embed-host"></div>
<script>
(function(){
  var host=document.getElementById("mf-embed-host");
  if(!host) return;
  fetch("${CDN_BASE}/markup.html").then(function(r){return r.text();}).then(function(html){
    host.innerHTML=html;
    var s=document.createElement("script");
    s.src="${CDN_BASE}/many-futures.js";
    document.body.appendChild(s);
  });
})();
</script>
`;
fs.writeFileSync(path.join(dir, "cdn-loader.html"), loader);

const previewShell = fs.readFileSync(path.join(dir, "index.html"), "utf8");
const preview = previewShell
  .replace("<!-- MANY_FUTURES_MARKUP -->", markup)
  .replace(
    /<link rel="stylesheet" href="\.\/many-futures\.css" \/>/,
    `<style>\n${css}\n</style>`
  )
  .replace(
    /<script src="\.\/many-futures\.js"><\/script>/,
    `<script>\n${js}\n</script>`
  );

fs.writeFileSync(path.join(dir, "preview.html"), preview);

const readableSource = `<style>\n${css}\n</style>\n${markup}\n<script>\n${js}\n</script>`;
const inlineKb = embed.length / 1024;
const readableKb = readableSource.length / 1024;
const cssKb = cssOut.length / 1024;
const jsKb = jsOut.length / 1024;
const markupKb = markupOut.length / 1024;
const loaderKb = loader.length / 1024;

console.log("Wrote", out);
console.log("Wrote preview.html and cdn-loader.html");
console.log("Readable production source characters:", readableSource.length);
console.log("Readable production source KB:", readableKb.toFixed(1));
console.log("Minified inline Embed characters:", embed.length);
console.log("Minified inline Embed KB:", inlineKb.toFixed(1));
console.log("  CSS KB:", cssKb.toFixed(1));
console.log("  JS KB:", jsKb.toFixed(1));
console.log("  Markup KB:", markupKb.toFixed(1));
console.log("CDN loader KB (backup only):", loaderKb.toFixed(1));
console.log(
  "Phase C default:",
  inlineKb <= 45
    ? "Use full minified inline Embed (under 45 KB headroom target; Webflow ~50 KB limit)."
    : "Inline exceeds 45 KB headroom — minify further or seek approval before CDN loader."
);
