#!/usr/bin/env node
/**
 * Builds Webflow Code Embed + local preview.
 *
 * Production Embed: content-hashed CSS/JS on jsDelivr (commit SHA + file hash)
 * so HTML/CSS/JS cannot drift via cache. CDN markup loader remains backup-only.
 */
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

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

function shortHash(content) {
  return crypto.createHash("sha256").update(content).digest("hex").slice(0, 12);
}

const cssOut = minifyCss(css);
const jsOut = minifyJs(js);
const cssHash = shortHash(cssOut);
const jsHash = shortHash(jsOut);
const cssFile = `many-futures.${cssHash}.css`;
const jsFile = `many-futures.${jsHash}.js`;

const distDir = path.join(dir, "dist");
fs.mkdirSync(distDir, { recursive: true });
/* Content-addressed filenames — never overwrite a different payload at the same name. */
const cssPath = path.join(distDir, cssFile);
const jsPath = path.join(distDir, jsFile);
if (fs.existsSync(cssPath) && fs.readFileSync(cssPath, "utf8") !== cssOut) {
  throw new Error(`Hash collision or mutated asset: ${cssFile}`);
}
if (fs.existsSync(jsPath) && fs.readFileSync(jsPath, "utf8") !== jsOut) {
  throw new Error(`Hash collision or mutated asset: ${jsFile}`);
}
fs.writeFileSync(cssPath, cssOut);
fs.writeFileSync(jsPath, jsOut);

/* Pin CDN to commit SHA after push. Placeholder updated by release script / rebuild. */
const CDN_SHA =
  process.env.MF_CDN_SHA ||
  (fs.existsSync(path.join(dir, "CDN_SHA"))
    ? fs.readFileSync(path.join(dir, "CDN_SHA"), "utf8").trim()
    : "d2aee9c5201f671fde32d4b3ee1e810d7d58c9f0");

const CDN_BASE = `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@${CDN_SHA}/webflow-embeds/many-futures`;

const ASSET_REWRITES = [
  ["assets/hotel-final.jpg", `${CDN_BASE}/assets/hotel-final.jpg`],
  ["assets/hotel-final-640.webp", `${CDN_BASE}/assets/hotel-final-640.webp`],
  ["assets/hotel-final-960.webp", `${CDN_BASE}/assets/hotel-final-960.webp`],
  ["assets/hotel-final-1280.webp", `${CDN_BASE}/assets/hotel-final-1280.webp`],
];

const FEATURE_FILES = [
  "operator-track-record-desktop.png",
  "operator-track-record-mobile.png",
  "fee-estimator-desktop.png",
  "fee-estimator-desktop.webp",
  "fee-estimator-mobile.png",
  "fee-estimator-mobile.webp",
  "radar-desktop.png",
  "radar-desktop.webp",
  "radar-desktop-800.png",
  "radar-desktop-800.webp",
  "radar-mobile.png",
  "radar-mobile.webp",
  "smart-matching-desktop.png",
  "smart-matching-desktop.webp",
  "smart-matching-mobile.png",
  "smart-matching-mobile.webp",
  "smart-matching-operators-desktop.png",
  "smart-matching-operators-desktop.webp",
  "smart-matching-operators-mobile.png",
  "smart-matching-operators-mobile.webp",
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

const markupOut = minifyMarkup(prodMarkup);
const markupHash = shortHash(markupOut);
const bodyFile = `many-futures.${markupHash}.body.html`;
const bodyPath = path.join(distDir, bodyFile);
if (fs.existsSync(bodyPath) && fs.readFileSync(bodyPath, "utf8") !== markupOut) {
  throw new Error(`Hash collision or mutated asset: ${bodyFile}`);
}
fs.writeFileSync(bodyPath, markupOut);

const cssUrl = `${CDN_BASE}/dist/${cssFile}`;
const jsUrl = `${CDN_BASE}/dist/${jsFile}`;
const bodyUrl = `${CDN_BASE}/dist/${bodyFile}`;

/* Tiny critical CSS so content is not invisible before stylesheet arrives */
const critical =
  "<style>#dealality-many-futures{color:#e8ecf8;font-family:system-ui,sans-serif}#dealality-many-futures .mf-panel[hidden]{display:none!important}</style>";

const embedInline = `<style>${cssOut}</style>${markupOut}<script>${jsOut}</script>`;
const embedFull = `${critical}<link rel="stylesheet" href="${cssUrl}" /><script src="${jsUrl}" defer></script>${markupOut}`;

/* Webflow Code Embed soft limit ~50KB. Nine-state markup exceeds it — use hashed body fetch. */
const embedLoader = `${critical}<link rel="stylesheet" href="${cssUrl}" /><div id="mf-embed-host" aria-busy="true"></div><script>(function(){var h=document.getElementById("mf-embed-host");if(!h)return;fetch("${bodyUrl}").then(function(r){if(!r.ok)throw new Error("mf body");return r.text()}).then(function(html){h.outerHTML=html;var s=document.createElement("script");s.src="${jsUrl}";s.defer=true;document.body.appendChild(s)}).catch(function(){h.setAttribute("aria-busy","false");h.textContent="Many Futures interactive could not load. Refresh to try again."})})();</script>`;

const embed = embedFull.length <= 49500 ? embedFull : embedLoader;
const WEBFLOW_MODE = embedFull.length <= 49500 ? "inline-markup" : "cdn-body-loader";

const out = path.join(dir, "..", "many-futures-section.html");
fs.writeFileSync(out, embedFull);
fs.writeFileSync(path.join(dir, "webflow-embed.html"), embed);
fs.writeFileSync(path.join(dir, "many-futures-section-full-inline.html"), embedInline);

fs.writeFileSync(
  path.join(distDir, "MANIFEST.json"),
  JSON.stringify(
    {
      css: cssFile,
      js: jsFile,
      body: bodyFile,
      cssHash,
      jsHash,
      markupHash,
      webflowMode: WEBFLOW_MODE,
      builtAt: new Date().toISOString(),
    },
    null,
    2
  ) + "\n"
);

const loader = `<!-- Many Futures — CDN loader backup (use only if inline Embed is rejected) -->
<link rel="stylesheet" href="${cssUrl}" />
<div id="mf-embed-host"></div>
<script>
(function(){
  var host=document.getElementById("mf-embed-host");
  if(!host) return;
  fetch("${bodyUrl}").then(function(r){return r.text();}).then(function(html){
    host.outerHTML=html;
    var s=document.createElement("script");
    s.src="${jsUrl}";
    s.defer=true;
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

console.log("Wrote", out);
console.log("Webflow embed:", path.join(dir, "webflow-embed.html"), `(${WEBFLOW_MODE})`);
console.log("CSS URL:", cssUrl);
console.log("JS URL:", jsUrl);
console.log("Body URL:", bodyUrl);
console.log("CDN_SHA:", CDN_SHA);
console.log("Full embed characters:", embedFull.length, "KB:", (embedFull.length / 1024).toFixed(1));
console.log("Webflow embed characters:", embed.length, "KB:", (embed.length / 1024).toFixed(1));
console.log(
  embed.length <= 50000
    ? "Webflow embed within ~50KB Code Embed limit."
    : "Webflow embed still exceeds ~50KB."
);
console.log(
  embedInline.length <= 50000
    ? "Full style-inline also within limit."
    : "Full style-inline exceeds ~50KB — linked hashed CSS/JS used for Phase C."
);
