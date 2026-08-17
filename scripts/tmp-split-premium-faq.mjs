import fs from "fs";

const full = fs.readFileSync("tmp-premium-body.html", "utf8");
const inner = full.replace(/^<div id="dc-premium">/, "").replace(/<\/div>$/, "");
const splitAt = inner.indexOf('<section id="insights"');
if (splitAt < 0) throw new Error("insights marker missing");
const a = inner.slice(0, splitAt);
const b = inner.slice(splitAt);
fs.writeFileSync("tmp-premium-chunk-a.html", a);
fs.writeFileSync("tmp-premium-chunk-b.html", b);
console.log(JSON.stringify({ a: a.length, b: b.length, aHasNav: a.includes("id=\"nav\""), bHasFaq: b.includes("faq-badge-left"), bHasFooter: b.includes("id=\"footer\"") }));
