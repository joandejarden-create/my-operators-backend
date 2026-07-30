#!/usr/bin/env node
/**
 * Combines markup + CSS + JS into a single Webflow Code Embed snippet,
 * and local preview.html.
 *
 * Phase C: full minified inline Embed is the default deployment path.
 * CDN loader remains backup only.
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

/* Pin to commit that includes HTML UI CSS (ring/gaps/badges) + final assets. */
const CDN_SHA = "d2aee9c5201f671fde32d4b3ee1e810d7d58c9f0";
const CDN_BASE =
  `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@${CDN_SHA}/webflow-embeds/many-futures`;

const ASSET_REWRITES = [
  ["assets/hotel-final.jpg", `${CDN_BASE}/assets/hotel-final.jpg`],
  ["assets/hotel-final-640.webp", `${CDN_BASE}/assets/hotel-final-640.webp`],
  ["assets/hotel-final-960.webp", `${CDN_BASE}/assets/hotel-final-960.webp`],
  ["assets/hotel-final-1280.webp", `${CDN_BASE}/assets/hotel-final-1280.webp`],
];

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
];

for (const file of FEATURE_FILES) {
  ASSET_REWRITES.push([
    `assets/features/${file}`,
    `${CDN_BASE}/assets/features/${file}`,
  ]);
}

let prodMarkup = markup;
for (const [from, to] of ASSET_REWRITES) {
  prodMarkup = prodMarkup.split(from).join(to);
}

const cssOut = minifyCss(css);
const jsOut = minifyJs(js);
const markupOut = minifyMarkup(prodMarkup);

/* Full single-file inline (may exceed Webflow ~50KB after HTML UIs) */
const embedInline = `<style>${cssOut}</style>${markupOut}<script>${jsOut}</script>`;

/* Phase C production embed: linked CSS/JS + inline semantic HTML.
   Keeps initial DOM content available (a11y/SEO). Not the async CDN loader
   that fetches markup. Full style+JS inline exceeds Webflow ~50KB limit. */
const embed = `<link rel="stylesheet" href="${CDN_BASE}/many-futures.css" /><script src="${CDN_BASE}/many-futures.js" defer></script>${markupOut}`;

const out = path.join(dir, "..", "many-futures-section.html");
fs.writeFileSync(out, embed);
fs.writeFileSync(path.join(dir, "many-futures-section-full-inline.html"), embedInline);

const loader = `<!-- Many Futures — CDN loader backup (use only if inline Embed is rejected) -->
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
console.log("Wrote", out);
console.log("Wrote many-futures-section-full-inline.html (backup)");
console.log("Wrote preview.html and cdn-loader.html");
console.log("Readable production source characters:", readableSource.length);
console.log("Full style-inline Embed characters:", embedInline.length);
console.log("Phase C Embed (linked CSS/JS + inline HTML) characters:", embed.length);
console.log("Phase C Embed KB:", (embed.length / 1024).toFixed(1));
console.log("  CSS file:", cssOut.length, "JS:", jsOut.length, "Markup:", markupOut.length);
console.log(
  embed.length <= 50000
    ? "Phase C Embed within ~50KB Webflow Code Embed limit."
    : "Phase C Embed still exceeds ~50KB."
);
console.log(
  embedInline.length <= 50000
    ? "Full style-inline also within limit."
    : "Full style-inline exceeds ~50KB — linked CSS used for Phase C to preserve HTML UIs."
);
