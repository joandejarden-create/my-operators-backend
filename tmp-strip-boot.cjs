const fs = require("fs");
let s = fs.readFileSync("tmp-globe-boot.js", "utf8");
s = s.replace(/^<script>\s*/i, "").replace(/\s*<\/script>\s*$/i, "");
fs.writeFileSync("tmp-globe-boot-inline.js", s);
console.log(s.length);
