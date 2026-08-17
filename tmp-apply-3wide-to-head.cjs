const fs = require("fs");
const https = require("https");

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "Cache-Control": "no-cache", Pragma: "no-cache" } }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve(d));
      })
      .on("error", reject);
  });
}

const OLD = `#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex:0 0 360px!important;flex-shrink:0!important;width:360px!important;min-width:360px!important;max-width:360px!important;box-sizing:border-box!important}
@media(max-width:960px){#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex-basis:300px!important;width:300px!important;min-width:300px!important;max-width:300px!important}}
@media(max-width:640px){#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex-basis:min(100%,320px)!important;width:min(100%,320px)!important;min-width:min(100%,280px)!important;max-width:100%!important}}`;

const NEW = `#insights-grid,.oh-insights-grid{--ins-gap:2rem!important;--ins-visible:3!important;gap:var(--ins-gap)!important;scroll-snap-type:x mandatory!important}
#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{--ins-card-w:calc((100% - (var(--ins-gap) * (var(--ins-visible) - 1))) / var(--ins-visible));flex:0 0 var(--ins-card-w)!important;flex-shrink:0!important;width:var(--ins-card-w)!important;min-width:var(--ins-card-w)!important;max-width:var(--ins-card-w)!important;box-sizing:border-box!important;scroll-snap-align:start!important}
@media(max-width:960px){#insights-grid,.oh-insights-grid{--ins-visible:2!important;--ins-gap:1.25rem!important}}
@media(max-width:640px){#insights-grid,.oh-insights-grid{--ins-visible:1!important;--ins-gap:1rem!important}}`;

const headPath = process.argv[2];
if (!headPath) {
  console.error("usage: node tmp-apply-3wide-to-head.cjs <head-file>");
  process.exit(1);
}
let head = fs.readFileSync(headPath, "utf8");
if (!head.includes(OLD)) {
  console.error("360px card block not found in head file");
  process.exit(1);
}
head = head.replace(OLD, NEW);
head = head.replace(
  "/* Insights carousel — force overflow scroll across 6 cards */",
  "/* Insights carousel — exactly 3 wide on desktop; 6 cards scroll */"
);
fs.writeFileSync("tmp-old-home-head-3wide.txt", head);
console.log("wrote tmp-old-home-head-3wide.txt", head.length);
console.log("has calc", head.includes("--ins-card-w"));
console.log("still 360 card?", /flex:0 0 360px/.test(head));
