import fs from "fs";

const p = "public/marketing/old-home-manual-process.boot.v20260801f17.js";
const url =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d92c203751affa3687fb6_old-home-manual-process.v20260801f17.js";
let s = fs.readFileSync(p, "utf8");
s = s.replace(
  /var JS_URL[\s\S]*?;\r?\n/,
  `var JS_URL =\n    "${url}";\n`
);
fs.writeFileSync(p, s);
console.log(s.slice(0, 400));
