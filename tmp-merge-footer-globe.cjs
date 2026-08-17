const fs = require("fs");
const footer = fs.readFileSync("tmp-footer-current.txt", "utf8");
const boot = fs.readFileSync("tmp-globe-boot.js", "utf8");
const out = footer.trimEnd() + "\n" + boot + "\n";
fs.writeFileSync("tmp-footer-with-globe.txt", out, "utf8");
console.log("footer", out.length);
