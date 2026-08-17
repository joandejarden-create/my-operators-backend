const res = await fetch(
  "https://mvp-deal-capture.webflow.io/old-home?cb=" + Date.now(),
  { cache: "no-store" }
);
const t = await res.text();
const matches = [
  ...t.matchAll(/old-home-boot-guard[^"'\\\s]+|old-home-how-we-do-it[^"'\\\s]+|section-type[^"'\\\s]+/g),
].map((x) => x[0]);
console.log([...new Set(matches)].join("\n"));
