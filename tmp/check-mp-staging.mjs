const res = await fetch("https://mvp-deal-capture.webflow.io/old-home?cb=" + Date.now(), {
  cache: "no-store",
});
const t = await res.text();
const matches = [...t.matchAll(/old-home-manual-process[^"'\\\s]+/g)].map((x) => x[0]);
console.log([...new Set(matches)].join("\n"));
