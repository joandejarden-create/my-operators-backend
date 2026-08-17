const r = await fetch("https://www.dealality.com/old-home?t=" + Date.now());
const t = await r.text();
const links = [...t.matchAll(/dealality-old-home-freeform-head[^"'\\\s>]+/g)].map((m) => m[0]);
console.log("freeform links:", [...new Set(links)].join("\n"));
const i = t.indexOf("hero-signals");
console.log("hero-signals idx", i);
if (i >= 0) console.log(t.slice(i, i + 900));
const j = t.indexOf("hs-pos-label");
if (j >= 0) console.log("label area:", t.slice(j - 80, j + 500));
