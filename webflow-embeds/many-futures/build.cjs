#!/usr/bin/env node
/**
 * Combines markup + CSS + JS into a single Webflow Code Embed snippet.
 * Production output is minified. Source files stay readable.
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

const CDN = "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a";
/* Phase 3: curated crops + temporary hotel uploaded to Webflow CDN. */
const ASSETS = {
  "assets/hotel-temp.jpg": `${CDN}/6a6b9ecfbcdb4eea68e0a6f3_mf-hotel-temp.jpg`,
  "assets/crops/rebrand-desktop.png": `${CDN}/6a6b9ecf1bd127b9b4f752f2_mf-rebrand-desktop.png`,
  "assets/crops/rebrand-mobile.png": `${CDN}/6a6b9ecf433d91a90f75bfeb_mf-rebrand-mobile.png`,
  "assets/crops/new-operator-desktop.png": `${CDN}/6a6b9ecfa88451795cb12191_mf-new-operator-desktop.png`,
  "assets/crops/new-operator-mobile.png": `${CDN}/6a6b9ecfbcdb4eea68e0a716_mf-new-operator-mobile.png`,
  "assets/crops/soft-brand-desktop.png": `${CDN}/6a6b9f148786858d5f89eceb_mf-soft-brand-desktop.png`,
  "assets/crops/soft-brand-mobile.png": `${CDN}/6a6b9f145eaef921ad0f3c41_mf-soft-brand-mobile.png`,
  "assets/crops/independent-desktop.png": `${CDN}/6a6b9f145eaef921ad0f3c73_mf-independent-desktop.png`,
  "assets/crops/independent-mobile.png": `${CDN}/6a6b9f149e762a2c93e35d15_mf-independent-mobile.png`,
  "assets/crops/branded-residences-desktop.png": `${CDN}/6a6b9f148786858d5f89ed1e_mf-branded-residences-desktop.png`,
  "assets/crops/branded-residences-mobile.png": `${CDN}/6a6b9f150692bac596137240_mf-branded-residences-mobile.png`,
};

for (const [from, to] of Object.entries(ASSETS)) {
  markup = markup.split(from).join(to);
}

const cssOut = minifyCss(css);
const jsOut = minifyJs(js);
const markupOut = minifyMarkup(markup);

const embed = `<style>${cssOut}</style>${markupOut}<script>${jsOut}</script>`;

const out = path.join(dir, "..", "many-futures-section.html");
fs.writeFileSync(out, embed);

const previewShell = fs.readFileSync(path.join(dir, "index.html"), "utf8");
const preview = previewShell
  .replace(
    /<!-- MARKUP_START -->[\s\S]*?(?=<\/div>\s*<\/div>\s*<\/section>)/,
    markup + "\n        "
  )
  .replace(
    /<link rel="stylesheet" href="many-futures\.css" \/>/,
    `<style>\n${css}\n</style>`
  )
  .replace(
    /<script>[\s\S]*?fetch\("markup\.html"\)[\s\S]*?<\/script>/,
    `<script>\n${js}\n</script>`
  );

fs.writeFileSync(path.join(dir, "preview.html"), preview);

console.log("Wrote", out);
console.log("Production character count:", embed.length);
console.log("Approx KB:", (embed.length / 1024).toFixed(1));
console.log("CDN assets wired for Phase 3:", Object.keys(ASSETS).length);
