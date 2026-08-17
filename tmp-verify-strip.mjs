const t = await (await fetch("https://www.dealality.com/old-home?v=" + Date.now())).text();
const checks = [
  "freeform-head.v20260729w12.css",
  "With Dealality",
  "Without a shared process",
  "Fragmented information",
  "Slower decisions",
  "More options",
  "hero-signals-pos",
];
for (const c of checks) console.log(t.includes(c) ? "YES" : "NO ", c);
const i = t.indexOf('id="hero-signals"');
console.log(t.slice(i, i + 850));
