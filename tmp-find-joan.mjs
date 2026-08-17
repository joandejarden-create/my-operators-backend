import fs from "fs";

const r = await fetch("https://www.dealality.com/old-home");
const t = await r.text();
const imgs = [...t.matchAll(/https:\/\/cdn\.prod\.website-files\.com\/[^"'\\\s>]+\.(?:png|jpe?g|webp)/gi)].map((m) => m[0]);
console.log([...new Set(imgs)].join("\n"));
fs.writeFileSync("tmp-old-home-live.html", t);
const i = t.indexOf("testimonials");
console.log("---SLICE---");
console.log(t.slice(Math.max(0, i - 200), i + 3500));
