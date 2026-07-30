#!/usr/bin/env node
/**
 * Combines markup + CSS + JS into a single Webflow Code Embed snippet.
 * Local relative asset paths are rewritten to production CDN URLs.
 */
const fs = require("fs");
const path = require("path");

const dir = __dirname;
const css = fs.readFileSync(path.join(dir, "many-futures.css"), "utf8");
const js = fs.readFileSync(path.join(dir, "many-futures.js"), "utf8");
let markup = fs.readFileSync(path.join(dir, "markup.html"), "utf8");

const CDN = "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a";
const ASSETS = {
  "assets/hotel-temp.jpg":
    /* Phase 2 temporary — replace before Phase 3 with approved Webflow Asset URL */
    "assets/hotel-temp.jpg",
  "assets/brand-explorer.png":
    `${CDN}/6a6b68f279195823136433b7_dealality-old-home-feature-brand-explorer.v20260730l.png`,
  "assets/operator-explorer.png":
    `${CDN}/6a6b650591a1d82d913e3ea1_dealality-old-home-feature-operator-explorer.v20260730i.png`,
  "assets/fee-estimator.png":
    `${CDN}/6a6b368cc1507579a4d6b3b6_dealality-old-home-feature-fee-estimator.v20260730d.png`,
  "assets/radar.png":
    `${CDN}/6a6b368b0b3eb941d61d056d_dealality-old-home-feature-radar.v20260730d.png`,
  "assets/opportunity-review.png":
    `${CDN}/6a6b66e3b2a036492744c65a_dealality-old-home-feature-opportunity-review.v20260730k.png`,
};

for (const [from, to] of Object.entries(ASSETS)) {
  markup = markup.split(from).join(to);
}

const embed = `<!-- Dealality Many Futures Embed
  Source of truth: webflow-embeds/many-futures/
  Combined: webflow-embeds/many-futures-section.html
  Asset replace points: img[data-mf-asset="hotel|rebrand|new-operator|soft-brand|independent|branded-residences"]
-->
<style>
${css}
</style>
${markup}
<script>
${js}
</script>
`;

const out = path.join(dir, "..", "many-futures-section.html");
fs.writeFileSync(out, embed);
console.log("Wrote", out);
console.log("Character count:", embed.length);
console.log("Approx KB:", (embed.length / 1024).toFixed(1));
