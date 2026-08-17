const fs = require("fs");

const headPath = "tmp-old-home-head.txt";
const src = process.argv[2] || "tmp-old-home-head-live.txt";

let head = fs.readFileSync(src, "utf8");

const oldBlock = `/* Insights carousel — force overflow scroll across 6 cards */
#insights-inner,.oh-insights-inner{min-width:0!important;max-width:1320px!important;width:100%!important;box-sizing:border-box!important}
#insights-carousel,.oh-insights-carousel{min-width:0!important;max-width:100%!important;width:100%!important;display:flex!important;flex-direction:column!important}
#insights-grid,.oh-insights-grid{display:flex!important;flex-wrap:nowrap!important;overflow-x:auto!important;overflow-y:hidden!important;width:100%!important;max-width:100%!important;min-width:0!important;box-sizing:border-box!important;-webkit-overflow-scrolling:touch!important}
#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex:0 0 360px!important;flex-shrink:0!important;width:360px!important;min-width:360px!important;max-width:360px!important;box-sizing:border-box!important}
@media(max-width:960px){#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex-basis:300px!important;width:300px!important;min-width:300px!important;max-width:300px!important}}
@media(max-width:640px){#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex-basis:min(100%,320px)!important;width:min(100%,320px)!important;min-width:min(100%,280px)!important;max-width:100%!important}}
#insights-prev.is-disabled,#insights-next.is-disabled,#insights-prev[aria-disabled="true"],#insights-next[aria-disabled="true"]{pointer-events:auto!important}`;

const newBlock = `/* Insights carousel — exactly 3 wide on desktop (calc), 6 cards scroll */
#insights-inner,.oh-insights-inner{min-width:0!important;max-width:1320px!important;width:100%!important;box-sizing:border-box!important}
#insights-carousel,.oh-insights-carousel{min-width:0!important;max-width:100%!important;width:100%!important;display:flex!important;flex-direction:column!important}
#insights-grid,.oh-insights-grid{--ins-gap:2rem!important;--ins-visible:3!important;display:flex!important;flex-wrap:nowrap!important;gap:var(--ins-gap)!important;overflow-x:auto!important;overflow-y:hidden!important;scroll-snap-type:x mandatory!important;width:100%!important;max-width:100%!important;min-width:0!important;box-sizing:border-box!important;-webkit-overflow-scrolling:touch!important}
#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{--ins-card-w:calc((100% - (var(--ins-gap) * (var(--ins-visible) - 1))) / var(--ins-visible));flex:0 0 var(--ins-card-w)!important;flex-shrink:0!important;width:var(--ins-card-w)!important;min-width:var(--ins-card-w)!important;max-width:var(--ins-card-w)!important;box-sizing:border-box!important;scroll-snap-align:start!important}
@media(max-width:960px){#insights-grid,.oh-insights-grid{--ins-visible:2!important;--ins-gap:1.25rem!important}}
@media(max-width:640px){#insights-grid,.oh-insights-grid{--ins-visible:1!important;--ins-gap:1rem!important}}
#insights-prev.is-disabled,#insights-next.is-disabled,#insights-prev[aria-disabled="true"],#insights-next[aria-disabled="true"]{pointer-events:auto!important}`;

if (!head.includes(oldBlock)) {
  console.error("old carousel block not found");
  // try softer match
  const start = head.indexOf("/* Insights carousel");
  const end = head.indexOf("#insights-prev.is-disabled");
  console.log("start", start, "end", end);
  if (start >= 0 && end > start) {
    const endClose = head.indexOf("</style>", end);
    const slice = head.slice(start, head.indexOf("\n", end + 120) + 1);
    console.log("FOUND SLICE LEN", slice.length);
    console.log(JSON.stringify(slice.slice(0, 200)));
    console.log("---");
    console.log(JSON.stringify(slice.slice(-200)));
  }
  process.exit(1);
}

head = head.replace(oldBlock, newBlock);
fs.writeFileSync(headPath, head);
console.log("wrote", headPath, head.length);
console.log("has calc", head.includes("--ins-card-w"));
console.log("has 360px card", /flex:0 0 360px/.test(head));
