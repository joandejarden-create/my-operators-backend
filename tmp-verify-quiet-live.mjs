import fs from "fs";

const r = await fetch("https://www.dealality.com/old-home", {
  headers: { "cache-control": "no-cache" },
});
const t = await r.text();
const out = {
  quietScript: t.includes("hero-signals-quiet.v20260729w18.js"),
  freeformHead: t.match(/freeform-head[^"'\\\s>]+/g) || [],
  hasWithDealality: /With Dealality/.test(t),
  hasWITH: /WITH DEALALITY/.test(t),
  hasOhHeroSignalsQuiet: t.includes("ohHeroSignalsQuiet"),
};
fs.writeFileSync("tmp-verify-quiet-live.json", JSON.stringify(out, null, 2));
console.log(JSON.stringify(out, null, 2));
