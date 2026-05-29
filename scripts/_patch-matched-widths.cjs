const fs = require("fs");
let html = fs.readFileSync("public/my-deals.html", "utf8");
const anchor = "        #matchedDealsTable th { white-space: nowrap; }\n        #matchedDealsTable .cell-call-to-action { min-width: 0; padding-left: 8px; }";
const insert = anchor + `
        /* Matched Brands — room for Chain Scale / Project Type / Target Opening Date (override global compact + ellipsis) */
        #matchedDealsTable {
            table-layout: auto;
        }
        #matchedDealsTable th[data-sort="hotelChainScale"],
        #matchedDealsTable th[data-sort="projectType"],
        #matchedDealsTable th[data-sort="targetOpeningDate"] {
            width: auto !important;
            max-width: none !important;
            white-space: nowrap;
        }
        #matchedDealsTable th[data-sort="targetOpeningDate"] > span {
            max-width: none;
            white-space: nowrap;
        }
        #matchedDealsTable tbody tr td:nth-child(4),
        #matchedDealsTable tbody tr td:nth-child(5),
        #matchedDealsTable tbody tr td:nth-child(6) {
            min-width: 0;
            overflow: visible;
            text-overflow: clip;
            white-space: normal;
            word-break: break-word;
            line-height: 1.35;
            vertical-align: top;
        }
        #matchedDealsTable tbody tr td:nth-child(4) {
            min-width: 7.25rem;
        }
        #matchedDealsTable tbody tr td:nth-child(5) {
            min-width: 8.5rem;
        }
        #matchedDealsTable tbody tr td:nth-child(6) {
            min-width: 6.75rem;
        }`;
if (html.includes("Matched Brands — room for Chain Scale")) {
  console.log("already patched");
  process.exit(0);
}
if (!html.includes(anchor)) throw new Error("anchor missing");
html = html.replace(anchor, insert);
fs.writeFileSync("public/my-deals.html", html);
console.log("patched");
