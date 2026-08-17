import fs from "fs";
const t = fs.readFileSync("tmp-verify-problem-live.html", "utf8");
const headEnd = t.indexOf("</head>");
console.log("headEnd", headEnd);
console.log(t.slice(0, Math.min(2500, headEnd)));
console.log("--- link count ---", (t.match(/<link /g) || []).length);
console.log(
  "all css",
  [...t.matchAll(/<link[^>]+href="([^"]+)"/g)].map((m) => m[1]).slice(0, 30)
);
