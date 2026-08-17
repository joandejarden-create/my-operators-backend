import fs from "fs";

const h = fs.readFileSync("tmp-old-home-live2.html", "utf8");
const links = [...h.matchAll(/href="([^"]*freeform[^"]*)"/g)].map((m) => m[1]);
const scripts = [...h.matchAll(/src="([^"]*(?:footer-oh|modules|benefits|oh-)[^"]*)"/g)].map((m) => m[1]);
console.log("LINKS", links);
console.log("SCRIPTS", scripts.slice(0, 30));

const i = h.indexOf('id="modules"');
console.log("modules idx", i);
if (i >= 0) {
  const chunk = h.slice(i, i + 15000);
  fs.writeFileSync("tmp-modules-chunk.html", chunk);
  console.log("--- CHUNK START ---");
  console.log(chunk.slice(0, 5000));
  console.log("--- CHUNK MID ---");
  // Find panel platform section
  const p2 = chunk.indexOf("modules-panel-platform");
  console.log("panel2 in chunk", p2);
  if (p2 >= 0) console.log(chunk.slice(Math.max(0, p2 - 200), p2 + 800));
}

// Extract attributes around panels
for (const id of [
  "modules-panel-outcomes",
  "modules-panel-platform",
  "modules-tab-outcomes",
  "modules-tab-platform",
  "modules-dot-1",
  "modules-dot-2",
  "modules-dots",
]) {
  const re = new RegExp(`id="${id}"[^>]{0,400}`, "g");
  const m = [...h.matchAll(re)];
  console.log("\nID", id, "matches", m.length);
  m.forEach((x) => console.log(x[0]));
}

// Head inline script snippet
const si = h.indexOf("mod-1-p");
console.log("\nmod-1-p idx", si);
if (si >= 0) console.log(h.slice(si - 200, si + 400));

// Footer script URL
const fi = h.indexOf("old-home-footer");
console.log("\nfooter idx", fi);
if (fi >= 0) console.log(h.slice(fi - 80, fi + 120));
