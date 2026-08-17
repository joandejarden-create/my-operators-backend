const fs = require("fs");

// Rebuild head freeform: keep existing content, replace Insights carousel override block
const headPath = "tmp-old-home-head-current.txt";

// Fetch was via MCP - write from the get result we already have by reading a saved copy if present
// We'll construct patch by reading from a file we'll create from stdin args

const NEW_INSIGHTS_CSS = `/* Insights carousel — force overflow scroll across 6 cards */
#insights-inner,.oh-insights-inner{min-width:0!important;max-width:1320px!important;width:100%!important;box-sizing:border-box!important}
#insights-carousel,.oh-insights-carousel{min-width:0!important;max-width:100%!important;width:100%!important;display:flex!important;flex-direction:column!important}
#insights-grid,.oh-insights-grid{display:flex!important;flex-wrap:nowrap!important;overflow-x:auto!important;overflow-y:hidden!important;width:100%!important;max-width:100%!important;min-width:0!important;box-sizing:border-box!important;-webkit-overflow-scrolling:touch!important}
#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex:0 0 360px!important;flex-shrink:0!important;width:360px!important;min-width:360px!important;max-width:360px!important;box-sizing:border-box!important}
@media(max-width:960px){#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex-basis:300px!important;width:300px!important;min-width:300px!important;max-width:300px!important}}
@media(max-width:640px){#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex-basis:min(100%,320px)!important;width:min(100%,320px)!important;min-width:min(100%,280px)!important;max-width:100%!important}}
#insights-prev.is-disabled,#insights-next.is-disabled,#insights-prev[aria-disabled="true"],#insights-next[aria-disabled="true"]{pointer-events:auto!important}
`;

const OLD_BLOCK_START = "#insights-grid,.oh-insights-grid{display:flex!important;";

function patchHead(head) {
  // Remove previous carousel override at end of style if present
  const re =
    /\/\* Insights carousel[\s\S]*?#insights-prev\.is-disabled[\s\S]*?pointer-events:auto!important\}\n|#insights-grid,\.oh-insights-grid\{display:flex!important;flex-wrap:nowrap!important;overflow-x:auto!important;width:100%!important;max-width:100%!important\}\n#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,\.oh-ins-card\{flex:0 0 calc[\s\S]*?max-width:none!important\}\n#insights-prev\.is-disabled[\s\S]*?pointer-events:auto!important\}\n/;
  let next = head.replace(re, "");
  if (next === head) {
    // fallback: strip known last three rules
    next = head
      .replace(
        /#insights-grid,\.oh-insights-grid\{display:flex!important;flex-wrap:nowrap!important;overflow-x:auto!important;width:100%!important;max-width:100%!important\}\n/,
        ""
      )
      .replace(
        /#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,\.oh-ins-card\{flex:0 0 calc\(\(100% - \(var\(--ins-visible,3\) - 1\) \* var\(--ins-gap,2rem\)\) \/ var\(--ins-visible,3\)\)!important;width:calc\(\(100% - \(var\(--ins-visible,3\) - 1\) \* var\(--ins-gap,2rem\)\) \/ var\(--ins-visible,3\)\)!important;min-width:calc\(\(100% - \(var\(--ins-visible,3\) - 1\) \* var\(--ins-gap,2rem\)\) \/ var\(--ins-visible,3\)\)!important;max-width:none!important\}\n/,
        ""
      )
      .replace(
        /#insights-prev\.is-disabled,#insights-next\.is-disabled,#insights-prev\[aria-disabled=\\"true\\"\],#insights-next\[aria-disabled=\\"true\\"\]\{pointer-events:auto!important\}\n/,
        ""
      )
      .replace(
        /#insights-prev\.is-disabled,#insights-next\.is-disabled,#insights-prev\[aria-disabled="true"\],#insights-next\[aria-disabled="true"\]\{pointer-events:auto!important\}\n/,
        ""
      );
  }
  if (!next.includes("</style>")) throw new Error("no style close");
  next = next.replace("</style>", NEW_INSIGHTS_CSS + "</style>");
  return next;
}

module.exports = { patchHead, NEW_INSIGHTS_CSS };
