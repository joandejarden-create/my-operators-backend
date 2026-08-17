import fs from "fs";
const h = fs.readFileSync("tmp-old-home-live-features.html", "utf8");
const i = h.indexOf("platform-features.v20260730a");
console.log("idx", i);
console.log(h.slice(Math.max(0, i - 220), i + 280));
const m = h.match(/dealality-old-home-platform-features[^"']+/g);
console.log("assets", m);
const scripts = [...h.matchAll(/<script[^>]+src="([^"]+)"/g)]
  .map((x) => x[1])
  .filter((s) => /old-home|platform|oh|features/i.test(s));
console.log("scripts", scripts);
