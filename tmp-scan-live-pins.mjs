const t = await (await fetch("https://www.dealality.com/old-home?x=2")).text();
const pick = (re) => [...new Set([...t.matchAll(re)].map((m) => m[0]))];
console.log("fouc", pick(/old-home-fouc-gate[^"'\\\s>]+/gi));
console.log("head", pick(/dealality-old-home-freeform-head[^"'\\\s>]+/gi));
console.log("quiet", pick(/dealality-old-home-hero-signals-quiet[^"'\\\s>]+/gi));
console.log("herofit", pick(/dealality-old-home-hero-fit[^"'\\\s>]+/gi));
