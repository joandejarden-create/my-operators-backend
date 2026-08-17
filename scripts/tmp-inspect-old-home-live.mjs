import fs from "fs";

const html = fs.readFileSync("tmp-old-home-live.html", "utf8");
const idx = html.indexOf("dealality-old-home-dark");
console.log("css idx", idx);
console.log(html.slice(Math.max(0, idx - 250), idx + 160));

// Webflow often injects page custom code near end of head
const markers = [
  'id="w-node-',
  "dealality-old-home-dark",
  "Plus Jakarta Sans",
  "oh-sst-spin",
];
for (const m of markers) {
  console.log(m, html.includes(m));
}

// Find the freeform style block with fsw
const start = html.indexOf("/* Varko-style Form Subscribe Wrap */");
console.log("fsw style idx", start);
if (start > 0) {
  const styleStart = html.lastIndexOf("<style", start);
  const styleEnd = html.indexOf("</style>", start);
  console.log({ styleStart, styleEnd, len: styleEnd - styleStart });
}
