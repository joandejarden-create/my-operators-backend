const fs = require("fs");
const html = fs.readFileSync("tmp-live-home.html", "utf8");
const page = (html.match(/data-wf-page="([^"]+)"/) || [])[1];
console.log("page", page);

const markers = ["Discuss Your Opportunity", "Get Started", "Platform", "footer"];
for (const m of markers) {
  const idxs = [];
  let i = html.indexOf(m);
  while (i >= 0 && idxs.length < 8) {
    idxs.push(i);
    i = html.indexOf(m, i + 1);
  }
  console.log(m, idxs);
}

const d = html.lastIndexOf("Discuss Your Opportunity");
if (d >= 0) {
  const start = Math.max(0, d - 1200);
  const chunk = html.slice(start, d + 900);
  fs.writeFileSync("tmp-home-footer-chunk.html", chunk);
  console.log("wrote chunk", chunk.length);
  console.log(chunk);
}
