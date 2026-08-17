const fs = require("fs");
const h = fs.readFileSync("tmp/live-home-f21b.html", "utf8");
let idx = 0;
while ((idx = h.indexOf("manual-process.v20260801", idx)) !== -1) {
  console.log("---");
  console.log(h.slice(Math.max(0, idx - 140), idx + 200));
  idx++;
}
const m = h.match(/data-dmp-version="([^"]+)"/);
console.log("embed version", m && m[1]);
console.log("f20 stylesheet link", /f20\.css/.test(h));
console.log("f21 stylesheet link", /f21\.css/.test(h));
console.log("comment f20 only?", /dmp f20/.test(h), /Manual Process f21/.test(h));
