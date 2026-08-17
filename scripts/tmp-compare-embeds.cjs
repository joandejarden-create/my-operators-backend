const fs = require("fs");
const b2 = fs.readFileSync("docs/old-home-problem-deal-desk-embed-phaseB2.html", "utf8");
const strip = (s) => s.replace(/@import url\("[^"]+"\)/, '@import url("CSS")');
const b1Path = "docs/old-home-problem-deal-desk-embed-phaseB.html";
if (fs.existsSync(b1Path)) {
  const b1 = fs.readFileSync(b1Path, "utf8");
  console.log("html same?", strip(b1) === strip(b2));
  console.log("b1 css", (b1.match(/@import[^;]+/) || [])[0]);
}
console.log("b2 css", (b2.match(/@import[^;]+/) || [])[0]);
console.log("bytes", Buffer.byteLength(b2));
