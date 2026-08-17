const fs = require("fs");
const c = fs.readFileSync("c:/Dev/deal-capture-proxy/tmp/many-futures.css", "utf8");
const re = /\.mf-hotel[a-zA-Z0-9_-]*\s*\{[^}]+\}/g;
const matches = c.match(re) || [];
console.log("count", matches.length);
matches.slice(0, 12).forEach((m, i) => {
  console.log("\n---", i, "---\n", m.slice(0, 600));
});
const idx = c.indexOf(".mf-hotel{");
console.log("\nidx", idx);
if (idx >= 0) console.log(c.slice(idx, idx + 800));
