const url = `https://mvp-deal-capture.webflow.io/old-home?cb=${Date.now()}`;
const t = await (await fetch(url)).text();
const boots = [...t.matchAll(/old-home-boot-guard[^"'\\s]+/g)].map((m) => m[0]);
console.log(JSON.stringify({ boots: [...new Set(boots)], hasAj: t.includes("31aj"), hasAi: t.includes("31ai"), has31e: t.includes("section-type.v20260731e") }, null, 2));
