const fs = require("fs");
const html = fs.readFileSync("public/marketing/dealality-old-home-premium.html", "utf8");
const start = html.indexOf('<div id="faq-list">');
const end = html.indexOf("</div>\n    <div id=\"faq-glow\"", start);
if (start < 0 || end < 0) throw new Error("faq-list not found");
let inner = html.slice(start, end + 6); // include closing </div> of faq-list? No - we want contents only
// extract inside faq-list
const openEnd = html.indexOf(">", start) + 1;
const closeStart = html.lastIndexOf("</div>", end);
inner = html.slice(openEnd, closeStart).trim();

// Split into chunks of ~2 FAQs each for WHTML size limits
const parts = [];
const re = /<details[\s\S]*?<\/details>(?:\s*<div id="faq-\d+-div"[\s\S]*?<\/div>)?/g;
let m;
while ((m = re.exec(inner))) parts.push(m[0].trim());
console.log("parts", parts.length);
parts.forEach((p, i) => {
  fs.writeFileSync(`tmp-faq-chunk-${i + 1}.html`, p);
  console.log(i + 1, p.slice(0, 60).replace(/\n/g, " "), "...", p.length);
});
fs.writeFileSync("tmp-faq-list-inner.html", inner);
console.log("total inner", inner.length);
