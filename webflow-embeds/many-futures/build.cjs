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
/* Pre-publish: v3 framed desktop crops + content-tight mobile presentations + temporary hotel. */
const ASSETS = {
  "assets/hotel-temp.jpg": `${CDN}/6a6b9ecfbcdb4eea68e0a6f3_mf-hotel-temp.jpg`,
  "assets/crops/rebrand-desktop.png": `${CDN}/6a6bc336fad2c4d6800459eb_mf-v3-rebrand-desktop.png`,
  "assets/crops/rebrand-mobile.png": `${CDN}/6a6bc33613510ee4c75caf80_mf-v3-rebrand-mobile.png`,
  "assets/crops/new-operator-desktop.png": `${CDN}/6a6bc336e234cfde45f7e884_mf-v3-new-operator-desktop.png`,
  "assets/crops/new-operator-mobile.png": `${CDN}/6a6bc336fad2c4d6800459d6_mf-v3-new-operator-mobile.png`,
  "assets/crops/soft-brand-desktop.png": `${CDN}/6a6bc336fad2c4d680045a06_mf-v3-soft-brand-desktop.png`,
  "assets/crops/soft-brand-mobile.png": `${CDN}/6a6bc351625bda5699df9c25_mf-v3-soft-brand-mobile.png`,
  "assets/crops/independent-desktop.png": `${CDN}/6a6bc351625bda5699df9ccc_mf-v3-independent-desktop.png`,
  "assets/crops/independent-mobile.png": `${CDN}/6a6bc351a7054be2bdf7b4d4_mf-v3-independent-mobile.png`,
  "assets/crops/branded-residences-desktop.png": `${CDN}/6a6bc351a7054be2bdf7b509_mf-v3-branded-residences-desktop.png`,
  "assets/crops/branded-residences-mobile.png": `${CDN}/6a6bc3528b323476364a4f35_mf-v3-branded-residences-mobile.png`,
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
console.log("CDN assets wired for pre-publish:", Object.keys(ASSETS).length);
