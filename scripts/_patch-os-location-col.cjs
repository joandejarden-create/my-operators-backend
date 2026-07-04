const fs = require("fs");
const path = "public/css/operator-strategy-my-deals.css";
let css = fs.readFileSync(path, "utf8");

const oldBlock = `.operator-strategy-table.deals-table th,
.operator-strategy-table.deals-table td {
  font-size: 12px;
  vertical-align: top;
}

.operator-strategy-table .col-deal {
  width: 15%;
}
.operator-strategy-table .col-company {
  width: 14%;
}
.operator-strategy-table .col-location {
  width: 14%;
}`;

const newBlock = `.operator-strategy-table.deals-table {
  table-layout: auto;
}

.operator-strategy-table.deals-table th,
.operator-strategy-table.deals-table td {
  font-size: 12px;
  vertical-align: top;
}

.operator-strategy-table .col-deal {
  width: 15%;
}
.operator-strategy-table .col-company {
  width: 14%;
}
.operator-strategy-table .col-location {
  width: 8%;
  max-width: 8.5rem;
  min-width: 5.5rem;
}
.operator-strategy-table th.col-location,
.operator-strategy-table td.col-location {
  white-space: normal;
  overflow: visible;
  text-overflow: clip;
  word-break: break-word;
  overflow-wrap: break-word;
  line-height: 1.35;
}`;

if (css.includes("operator-strategy-table th.col-location")) {
  console.log("already patched");
} else if (!css.includes(oldBlock)) {
  console.error("block not found");
  process.exit(1);
} else {
  css = css.replace(oldBlock, newBlock);
}

const oldLoc = `.operator-strategy-location {
  color: var(--neutral--400);
  font-size: 12px;
  line-height: 1.4;
  display: block;
}`;

const newLoc = `.operator-strategy-location {
  color: var(--neutral--400);
  font-size: 12px;
  line-height: 1.35;
  display: block;
  white-space: normal;
  word-break: break-word;
  overflow-wrap: break-word;
}`;

if (!css.includes("operator-strategy-location {\n  color") || css.includes("overflow-wrap: break-word")) {
  // location span may already be patched if block was
} else {
  css = css.replace(oldLoc, newLoc);
}

if (!css.includes(".operator-strategy-location {\n  color: var(--neutral--400);\n  font-size: 12px;\n  line-height: 1.35")) {
  css = css.replace(oldLoc, newLoc);
}

fs.writeFileSync(path, css);
console.log("done");
