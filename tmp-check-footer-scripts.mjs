import fs from "fs";

const html = fs.readFileSync("tmp-old-home-verify.html", "utf8");

for (const needle of [
  "ohmodulestabfixw16",
  "oldhomebootguardw19",
  "modules-panel-platform",
  "aria-hidden=\"true\"",
  "benefits-tabs",
  "oldhomebenefitstabsv2",
]) {
  const idx = html.indexOf(needle);
  console.log(needle, idx);
}

// Show footer script region
const last = html.slice(-3500);
console.log("\n--- tail scripts ---");
console.log(
  [...last.matchAll(/<script[^>]+src="([^"]+)"/g)].map((m) => m[1]).join("\n")
);

// Confirm modules panel2 initial state in DOM
const m = html.match(
  /id="modules-panel-platform"[^>]{0,200}/
);
console.log("\npanel2 tag", m && m[0]);
