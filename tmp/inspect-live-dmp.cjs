const fs = require("fs");
const h = fs.readFileSync("tmp/live-home-f21b.html", "utf8");
let idx = 0;
while ((idx = h.indexOf("manual-process.v20260801", idx)) !== -1) {
  console.log("---");
  console.log(h.slice(Math.max(0, idx - 140), idx + 200));
  idx++;
}
const m = h.match(/data-dmp-version="([^"]+)"/);
console.log("embed", m && m[1]);
console.log("has f20.css href", h.includes("v20260801f20.css"));
console.log("has f21.css href", h.includes("v20260801f21.css"));
