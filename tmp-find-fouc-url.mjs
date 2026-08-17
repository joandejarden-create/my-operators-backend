const t = await (await fetch("https://www.dealality.com/old-home?x=3")).text();
const m = [...t.matchAll(/https:\/\/cdn\.prod\.website-files\.com\/[^"'\\\s>]*fouc-gate[^"'\\\s>]*/gi)].map((x) => x[0]);
console.log([...new Set(m)]);
const idx = t.indexOf("fouc-gate");
console.log("context", t.slice(Math.max(0, idx - 120), idx + 180));
