const res = await fetch("https://www.dealality.com/old-home", {
  headers: { "cache-control": "no-cache" },
});
const t = await res.text();
const pick = (re) => [...t.matchAll(re)].map((m) => m[0]);
const out = {
  quoteTiles: pick(/old-home-quote-tiles[^"'\\\s>]*/gi),
  freeform: pick(/dealality-old-home-freeform-head[^"'\\\s>]*/gi),
  problem: pick(/old-home-problem-v2[^"'\\\s>]*/gi),
  ohTtStyle: /id=["']oh-tt["']|#oh-tt/i.test(t),
  quoteCssLink: /quote-tiles\.v20260729/i.test(t),
};
console.log(JSON.stringify(out, null, 2));
