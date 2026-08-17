import fs from "fs";

let head = fs.readFileSync("tmp-old-home-head-globe.txt", "utf8");
const cssLink =
  '<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a692ab8bb6beaf6339f55da_dealality-old-home-hero-globe-bg.v20260729.css">\n';
if (!head.includes("dealality-old-home-hero-globe-bg.v20260729.css")) {
  head = head.replace(
    '<link async rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/styles.min.css">',
    cssLink +
      '<link async rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/styles.min.css">'
  );
}
fs.writeFileSync("tmp-old-home-head-globe.txt", head);
console.log({
  hasGlobe: head.includes("Hero globe"),
  hasLink: head.includes("hero-globe-bg.v20260729.css"),
  hasHideBoth: head.includes("#hero-globe,#hero-signals{display:none"),
  len: head.length,
});
