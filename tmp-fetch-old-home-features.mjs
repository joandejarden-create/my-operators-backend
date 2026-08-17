const r = await fetch("https://www.dealality.com/old-home?t=" + Date.now());
const t = await r.text();
const start = t.indexOf('id="platform-features"');
const end = t.indexOf('id="modules"', start);
console.log(t.slice(Math.max(0, start - 40), end > 0 ? end : start + 4000));
const css = [...t.matchAll(/https:\/\/cdn\.prod\.website-files\.com\/[^"']+platform-features[^"']+/g)].map((m) => m[0]);
console.log("\nCSS:", [...new Set(css)]);
const head = [...t.matchAll(/dealality-old-home-freeform-head[^"'\\\s>]+/g)].map((m) => m[0]);
console.log("HEAD:", [...new Set(head)]);
