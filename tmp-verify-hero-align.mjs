const t = await (await fetch("https://www.dealality.com/old-home?v=" + Date.now())).text();
console.log("has embed style", t.includes("oh-hero-align-w17"));
console.log("has gutter calc", t.includes("padding-left:calc((100% - 1120px)"));
console.log(
  "freeform head",
  [...t.matchAll(/dealality-old-home-freeform-head[^"'\\\s>]+/g)].map((m) => m[0])
);
console.log("has 20vw in page html?", t.includes("20vw,24rem"));
