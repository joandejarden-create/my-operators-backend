import fs from "fs";

let css = fs.readFileSync("public/marketing/dealality-old-home-premium.css", "utf8");
css = css
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/\s+/g, " ")
  .replace(/\s*([{}:;,])\s*/g, "$1")
  .trim();
const head = [
  '<link rel="preconnect" href="https://fonts.googleapis.com">',
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
  '<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">',
  `<style>${css}</style>`,
].join("");
fs.writeFileSync("tmp-premium-head-v2.html", head);
console.log(head.length);
