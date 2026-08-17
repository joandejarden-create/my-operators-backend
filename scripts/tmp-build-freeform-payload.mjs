import fs from "fs";

const foot = fs.readFileSync("tmp-premium-foot.html", "utf8");
const head = [
  '<link rel="preconnect" href="https://fonts.googleapis.com">',
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
  '<link href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap" rel="stylesheet">',
  '<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a686dac767d9ac601ff9cae_dealality-old-home-dark.v20260728j.css">',
  "<style>html,body,#dc-page{background:#080F25!important;height:auto!important;min-height:0!important;margin:0;padding:0}</style>",
].join("\n");

fs.writeFileSync("tmp-freeform-payload.json", JSON.stringify({ head, foot }));
console.log(JSON.stringify({ head: head.length, foot: foot.length }));
