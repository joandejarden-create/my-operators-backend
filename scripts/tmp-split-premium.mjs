import fs from "fs";

const full = fs.readFileSync("tmp-premium-body.html", "utf8");
const inner = full.replace(/^<div class="dc-page--premium" id="dc-premium">/, "").replace(/<\/div>$/, "");

const parts = [];
const markers = [
  "</div></section><section class=\"dc-psec\" id=\"problem\">",
  "</div></section><section class=\"dc-psec dc-psec--alt\" id=\"how-it-works\">",
  "</div></section><section class=\"dc-psec\" id=\"product-proof\">",
  "</div></section><section class=\"dc-psec dc-psec--alt\" id=\"trust\">",
  "</div></section><section class=\"dc-psec dc-pcta\" id=\"cta\">",
  "</div></section><footer",
];

// Split into 2 chunks for MCP payload size
const splitAt = inner.indexOf('<section class="dc-psec" id="product-proof">');
const a = `<div class="dc-page--premium" id="dc-premium">${inner.slice(0, splitAt)}</div>`;
const b = `<div class="dc-page--premium-cont">${inner.slice(splitAt)}</div>`;
fs.writeFileSync("tmp-premium-part-a.html", a);
fs.writeFileSync("tmp-premium-part-b.html", b);
console.log(a.length, b.length);
