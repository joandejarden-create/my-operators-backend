const fs = require("fs");

let html = fs.readFileSync("public/my-deals.html", "utf8");
let patches = 0;

const cssBlock = `
        /* Matched Brands — readable Chain Scale / Project / Opening columns */
        #matchedDealsTable th.col-chain-scale,
        #matchedDealsTable td.col-chain-scale {
            width: 8.25rem;
            min-width: 8.25rem;
            max-width: 9.5rem;
        }
        #matchedDealsTable th.col-project-type,
        #matchedDealsTable td.col-project-type {
            width: 7rem;
            min-width: 7rem;
            max-width: 8.5rem;
        }
        #matchedDealsTable th.col-target-opening,
        #matchedDealsTable td.col-target-opening {
            width: 5.75rem;
            min-width: 5.75rem;
            max-width: 6.5rem;
        }
        #matchedDealsTable .deal-meta-cell {
            padding-left: 8px;
            padding-right: 8px;
            font-size: 12px;
            line-height: 1.35;
            color: var(--neutral--300);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        #matchedDealsTable th.col-chain-scale,
        #matchedDealsTable th.col-project-type,
        #matchedDealsTable th.col-target-opening {
            width: auto !important;
            max-width: none !important;
            padding-left: 8px;
            padding-right: 8px;
        }
        #matchedDealsTable:has(th[data-sort="targetOpeningDate"]) tbody tr td.col-chain-scale,
        #matchedDealsTable:has(th[data-sort="targetOpeningDate"]) tbody tr td.col-project-type,
        #matchedDealsTable:has(th[data-sort="targetOpeningDate"]) tbody tr td.col-target-opening {
            max-width: none;
        }`;
if (html.includes(cssBlock)) {
  html = html.replace(cssBlock, "");
  patches++;
}

const scriptLine = '    <script src="/js/my-deals-deal-meta-cells.js"></script>\n';
if (html.includes(scriptLine)) {
  html = html.replace(scriptLine, "");
  patches++;
}

const dash = "\u2014";
const parseNew =
  "            function parseTargetOpeningDate(displayStr) {\n                if (window.MyDealsDealMetaCells && typeof window.MyDealsDealMetaCells.parseTargetOpeningDate === 'function') {\n                    return window.MyDealsDealMetaCells.parseTargetOpeningDate(displayStr);\n                }\n                if (!displayStr || displayStr === '" +
  dash +
  "' || displayStr.trim() === '') return null;\n                const d = new Date(displayStr.trim());\n                return isNaN(d.getTime()) ? null : d;\n            }";
const parseOld =
  "            function parseTargetOpeningDate(displayStr) {\n                if (!displayStr || displayStr === '" +
  dash +
  "' || displayStr.trim() === '') return null;\n                const d = new Date(displayStr.trim());\n                return isNaN(d.getTime()) ? null : d;\n            }";
if (html.includes(parseNew)) {
  html = html.replace(parseNew, parseOld);
  patches++;
}

const renderNew =
  "                    var metaCells = (window.MyDealsDealMetaCells && typeof window.MyDealsDealMetaCells.buildCellsHtml === 'function')\n                        ? window.MyDealsDealMetaCells.buildCellsHtml(deal, escapeHtml)\n                        : null;\n                    var hotelChainScaleCell = metaCells ? metaCells.chainScale : ('<td class=\"deal-meta-cell col-chain-scale\">' + escapeHtml(deal.hotelChainScale || '" +
  dash +
  "') + '</td>');\n                    var projectTypeCell = metaCells ? metaCells.projectType : ('<td class=\"deal-meta-cell col-project-type\">' + escapeHtml(deal.projectType || '" +
  dash +
  "') + '</td>');\n                    var openingCell = metaCells ? metaCells.opening : ('<td class=\"deal-meta-cell col-target-opening\">' + escapeHtml(deal.targetOpeningDate || '" +
  dash +
  "') + '</td>');\n                    var dealTypeCell = escapeHtml(deal.dealType || '" +
  dash +
  "');\n                    var outreachCell = escapeHtml(deal.hasOutreachSetup ? 'Custom' : 'Default');";
const renderOld =
  "                    var hotelChainScaleCell = escapeHtml(deal.hotelChainScale || '" +
  dash +
  "');\n                    var projectTypeCell = escapeHtml(deal.projectType || '" +
  dash +
  "');\n                    var openingCell = escapeHtml(deal.targetOpeningDate || '" +
  dash +
  "');\n                    var dealTypeCell = escapeHtml(deal.dealType || '" +
  dash +
  "');\n                    var outreachCell = escapeHtml(deal.hasOutreachSetup ? 'Custom' : 'Default');";
if (html.includes(renderNew)) {
  html = html.replace(renderNew, renderOld);
  patches++;
}

const headerNew =
  '<th data-sort="hotelChainScale" class="col-chain-scale"><span style="display:inline-flex;align-items:center;">Chain Scale<span class="sort-indicator" title="From Location &amp; Property"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span></span></th>\n                                    <th data-sort="projectType" class="col-project-type" title="Project Type"><span style="display:inline-flex;align-items:center;">Project<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span></span></th>\n                                    <th data-sort="targetOpeningDate" class="col-target-opening" title="Target Opening Date"><span style="display:inline-flex;align-items:center;">Opening<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span></span></th>';
const headerOld =
  '<th data-sort="hotelChainScale" ><span style="display:inline-flex;align-items:center;">Chain Scale<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span></span></th>\n                                    <th data-sort="projectType" ><span style="display:inline-flex;align-items:center;">Project Type<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span></span></th>\n                                    <th data-sort="targetOpeningDate" ><span style="display:inline-flex;align-items:center;">Target Opening Date<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span></span></th>';
if (html.includes(headerNew)) {
  html = html.replace(headerNew, headerOld);
  patches++;
}

fs.writeFileSync("public/my-deals.html", html);
console.log("reverted patches", patches);

let api = fs.readFileSync("api/my-deals.js", "utf8");
if (api.includes("${m} ${y}") && !api.includes("${m}, ${y}")) {
  api = api.replace("${m} ${y}", "${m}, ${y}");
  fs.writeFileSync("api/my-deals.js", api);
  console.log("api formatDate reverted");
}

try {
  fs.unlinkSync("public/js/my-deals-deal-meta-cells.js");
  console.log("removed my-deals-deal-meta-cells.js");
} catch (e) {
  console.log("js file", e.message);
}
